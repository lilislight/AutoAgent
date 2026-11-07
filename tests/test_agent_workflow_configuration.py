"""Versioned Agent Operator extensions and real Core policy enforcement."""

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
import tempfile
import unittest

from autoagent.agent import (
    AgentInput, AgentNodePolicy, AgentResult, AgentWorkflowExtensions,
    AgentWorkflowOptions, AgentWorkflowPolicies, ContextPolicy, CompactionInput, CompactionResult,
    CompleteRoundInput, Conversation,
    AgentModelSettings, AgentSessionSettings, LLMMessage, LLMRequest, LocalTokenPricing, OpenAIChatCompletionsProvider, OpenAIProviderConfig,
    RoundResult, TextContent, TokenPrice, agent_workflow, LLMHistoryCompactor, always_compact,
    complete_agent_round, finish_agent_round,
)
from autoagent.core import (
    BackoffPolicy, Operator, Recovery, ResourcePolicy, RetryPolicy, TimeoutPolicy,
    RuntimeTransitionError, UsagePolicy, WorkflowCompiler, WorkflowDefinitionSnapshot, WorkflowResourcePolicy,
)
from autoagent.host import AutoAgentHost, HostSettings
from tests import test_agent_providers as wire
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import ScriptedProvider, answer, settings


def trigger(value):
    return False


class AgentConfigurationTests(unittest.TestCase):
    def test_context_policy_preserves_graph_and_uses_ordinary_compaction(self):
        """Custom triggers retain ordinary Node boundaries and opt-in replay safety."""
        graph = WorkflowCompiler().compile_or_raise(agent_workflow(context_policy=ContextPolicy(trigger=trigger)))
        self.assertEqual((len(graph.nodes), len(graph.edges)), (11, 15))
        self.assertEqual(graph.node("prepare_context").recovery_mode.mode, "never")
        self.assertEqual(graph.node("build_request").recovery_mode.mode, "replay_safe")
        self.assertEqual(graph.node("compact_context").executable.id, "agent.compact_context")
        self.assertEqual(graph.node("compact_context").recovery_mode.mode, "never")
        self.assertEqual(graph.node("tool_execute").recovery_mode.mode, "never")
        self.assertEqual(graph.node("tool_execute_approved").recovery_mode.mode, "never")

    def test_core_policies_apply_independently_and_configuration_is_detached(self):
        """Node and Workflow policy objects are preserved without implicit Map or Retry changes."""
        selected = {
            "llm_call": AgentNodePolicy(timeout=TimeoutPolicy(30), retry=RetryPolicy(max_attempts=2),
                resource=ResourcePolicy(usage=UsagePolicy(max_total_tokens=100)),
                recovery_mode=Recovery(mode="replay_safe")),
            "tool_execute_approved": AgentNodePolicy(timeout=TimeoutPolicy(5)),
        }
        limits = WorkflowResourcePolicy(invocation=UsagePolicy(max_total_tokens=500),
            session=UsagePolicy(max_cost_usd="0.01"))
        policies = AgentWorkflowPolicies(nodes=selected, resource=limits)
        selected.clear()
        graph = WorkflowCompiler().compile_or_raise(agent_workflow(policies=policies))
        self.assertEqual(graph.resource, limits)
        llm = graph.node("llm_call")
        self.assertEqual((llm.timeout.seconds, llm.retry.max_attempts, llm.resource.usage.max_total_tokens), (30, 2, 100))
        self.assertEqual(llm.recovery_mode.mode, "replay_safe")
        self.assertIsNone(graph.node("tool_execute").timeout)
        self.assertEqual(graph.node("tool_execute_approved").timeout.seconds, 5)
        self.assertIsNone(graph.node("tool_execute_approved").retry)
        self.assertEqual(graph.node("tool_execute_approved").recovery_mode.mode, "never")
        with self.assertRaises(TypeError):
            policies.nodes["finish"] = AgentNodePolicy()

    def test_extension_versions_and_limits_change_durable_workflow_identity(self):
        """Code versions and policy changes produce distinct reproducible Workflow Revisions."""
        def snapshot(**kwargs):
            return WorkflowDefinitionSnapshot.from_workflow_ir(WorkflowCompiler().compile_or_raise(agent_workflow(**kwargs)))
        first = snapshot(context_policy=ContextPolicy(id="app.context", version="1", trigger=trigger))
        same = snapshot(context_policy=ContextPolicy(id="app.context", version="1", trigger=trigger))
        revised = snapshot(context_policy=ContextPolicy(id="app.context", version="2", trigger=trigger))
        bounded = snapshot(policies=AgentWorkflowPolicies(nodes={"llm_call": AgentNodePolicy(timeout=TimeoutPolicy(1))}))
        self.assertEqual(first.workflow_revision_id, same.workflow_revision_id)
        self.assertNotEqual(first.workflow_revision_id, revised.workflow_revision_id)
        self.assertNotEqual(bounded.workflow_revision_id, snapshot().workflow_revision_id)
        self.assertEqual(WorkflowDefinitionSnapshot.from_record(first.to_record()), first)

    def test_invalid_extensions_and_policy_targets_fail_before_execution(self):
        """Unknown Nodes, unversioned callbacks and incompatible stage contracts are rejected."""
        with self.assertRaises(TypeError):
            AgentWorkflowExtensions(prepare_context=trigger)
        with self.assertRaises(TypeError):
            AgentWorkflowExtensions(finish=Operator(trigger))
        with self.assertRaises(ValueError):
            AgentWorkflowPolicies(nodes={"typo": AgentNodePolicy()})
        with self.assertRaises(TypeError):
            AgentNodePolicy(timeout=1)
        with self.assertRaises(TypeError):
            AgentWorkflowPolicies(nodes={"llm_call": {"timeout": 1}})
        with self.assertRaises(TypeError):
            agent_workflow(extensions={})
        timed_wait = agent_workflow(policies=AgentWorkflowPolicies(nodes={
            "wait_approval": AgentNodePolicy(timeout=TimeoutPolicy(1))}))
        self.assertTrue(WorkflowCompiler().compile(timed_wait).ok)


class AgentConfigurationContracts:
    async def database_url(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        return "sqlite+aiosqlite:///" + str(Path(temporary.name) / "runtime.db")

    @asynccontextmanager
    async def host(self, provider, *, policies=None, extensions=None, pricing=None, **options):
        @asynccontextmanager
        async def lifespan(setup):
            async with AsyncExitStack() as stack:
                selected = await stack.enter_async_context(provider()) if callable(provider) and not hasattr(provider, "ainvoke") else provider
                setup.agent_context.resources.register_provider("offline", selected)
                setup.agent_context.set_defaults(settings())
                yield
        async with AutoAgentHost(workflows=[agent_workflow(policies=policies, extensions=extensions, **options)],
            lifespan=lifespan, environment={}, pricing=pricing,
            settings=HostSettings(database_url=await self.database_url())) as host:
            yield host

    async def run_agent(self, host, **kwargs):
        return await host.join(await host.execute("agent", AgentInput(message="Help"), **kwargs))

    async def test_context_callbacks_and_completion_extensions_run_in_real_nodes(self):
        """Policy compaction and completion extensions execute on every requested round."""
        seen = []
        async def compact(value: CompactionInput) -> CompactionResult:
            seen.append("compact")
            return CompactionResult(view=value.view)
        def complete(value: CompleteRoundInput) -> RoundResult:
            seen.append("complete")
            return complete_agent_round(value).model_copy(update={"continue_loop": value.previous_rounds == 0})
        def finish(value: RoundResult) -> AgentResult:
            seen.append("finish")
            return finish_agent_round(value)
        extensions = AgentWorkflowExtensions(
            complete_round=Operator(complete, id="app.complete", version="1"),
            finish=Operator(finish, id="app.finish", version="1"))
        provider = ScriptedProvider(answer("Draft"), answer("Reviewed"))
        async with self.host(provider, extensions=extensions,
            context_policy=ContextPolicy(trigger=always_compact, compact=compact),
            llm_options={"model": "per-call-model", "max_output_tokens": 77}) as host:
            result = await self.run_agent(host)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(result.output["rounds"], 2)
        self.assertEqual(seen, ["compact", "complete", "compact", "complete", "finish"])
        self.assertEqual([item.model for item in provider.requests], ["per-call-model"] * 2)
        self.assertEqual([item.max_output_tokens for item in provider.requests], [77] * 2)

    async def test_context_policy_controls_the_separate_compaction_model(self):
        """Default compaction uses its own generation model and output limit."""
        conversation = Conversation(items=(LLMMessage(role="user", content=(TextContent(text="Earlier goal"),)), answer().output[0]))
        provider = ScriptedProvider(answer("Summary"), answer())
        policy = ContextPolicy(context_window=4096, trigger=always_compact,
            compact=LLMHistoryCompactor(model="compaction-model", max_output_tokens=123))
        async with self.host(provider, context_policy=policy) as host:
            result = await self.run_agent(host, session_context={"agent_conversation": conversation.model_dump(mode="json")})
            self.assertEqual(result.status, "completed", result.error)
        self.assertEqual([item.model for item in provider.requests], ["compaction-model", "model-a"])
        self.assertEqual(provider.requests[0].max_output_tokens, 123)

    async def test_per_call_settings_need_no_private_routing_metadata(self):
        """Workflow generation options override an unset Session model through the fixed builder."""
        provider = ScriptedProvider(answer())
        async with self.host(provider, llm_options={"model": "application-model", "max_output_tokens": 42}) as host:
            await host.agent_context.configure("raw-request", AgentSessionSettings(model=AgentModelSettings(provider="offline")))
            result = await self.run_agent(host, session_id="raw-request")
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual((provider.requests[0].model, provider.requests[0].max_output_tokens), ("application-model", 42))
            self.assertTrue(provider.requests[0].routing["agent_managed_provider"])

    async def test_workflow_options_are_detached_from_application_mutation(self):
        """Mutating caller-owned nested options cannot change an already registered Agent."""
        options = AgentWorkflowOptions(llm_options={"max_output_tokens": 17})
        provider = ScriptedProvider(answer())
        async with self.host(provider, options=options) as host:
            options.llm_options["max_output_tokens"] = 999
            result = await self.run_agent(host)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(provider.requests[0].max_output_tokens, 17)

    async def test_custom_completion_cannot_rewrite_the_canonical_round_counter(self):
        """A completion extension cannot silently erase or duplicate committed rounds."""
        def complete(value: CompleteRoundInput) -> RoundResult:
            return complete_agent_round(value).model_copy(update={"rounds": 0})
        provider = ScriptedProvider(answer())
        async with self.host(provider, extensions=AgentWorkflowExtensions(
            complete_round=Operator(complete, id="app.bad.counter", version="1"))) as host:
            result = await self.run_agent(host)
            self.assertEqual(result.status, "failed")
            self.assertIn("round counter", result.error.message)
            self.assertEqual(len(provider.requests), 1)

    async def test_custom_completion_can_request_another_round_for_partial_output(self):
        """A completion strategy can continue after partial output without accepting it as final."""
        def complete(value: CompleteRoundInput) -> RoundResult:
            if value.response.finish_reason == "length":
                return RoundResult(response=value.response, results=value.results,
                    rounds=value.previous_rounds + 1, continue_loop=True)
            return complete_agent_round(value)
        provider = ScriptedProvider(answer("Partial").model_copy(update={"finish_reason": "length"}), answer("Complete"))
        async with self.host(provider, extensions=AgentWorkflowExtensions(
            complete_round=Operator(complete, id="app.check.completion", version="1"))) as host:
            result = await self.run_agent(host)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(result.output["rounds"], 2)
            self.assertEqual(len(provider.requests), 2)

    async def test_custom_continuation_cannot_bypass_the_round_limit(self):
        """The semantic round limit remains effective when a custom policy removes the Node start limit."""
        def complete(value: CompleteRoundInput) -> RoundResult:
            return complete_agent_round(value).model_copy(update={"continue_loop": True})
        provider = ScriptedProvider(answer(), answer())
        async with self.host(provider, max_rounds=2, policies=AgentWorkflowPolicies(nodes={
            "llm_call": AgentNodePolicy(resource=ResourcePolicy(max_executions_per_invocation=None))}),
            extensions=AgentWorkflowExtensions(complete_round=Operator(complete, id="app.continue", version="1"))) as host:
            result = await self.run_agent(host)
            self.assertEqual(result.status, "failed")
            self.assertIn("round limit", result.error.message)
        self.assertEqual(len(provider.requests), 2)

    async def test_explicit_retry_and_timeout_use_core_execution_boundaries(self):
        """A failed model attempt retries explicitly and a timeout cancels its own Provider attempt."""
        def fail(request):
            raise RuntimeError("Transient failure")
        provider = ScriptedProvider(fail, answer())
        retry = RetryPolicy(max_attempts=1, backoff=BackoffPolicy(initial_seconds=0, max_seconds=0, jitter="none"),
            retry_on=("RuntimeError",))
        async with self.host(provider, policies=AgentWorkflowPolicies(nodes={"llm_call": AgentNodePolicy(retry=retry)})) as host:
            result = await self.run_agent(host)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(len(provider.requests), 2)
        async def slow(request):
            await asyncio.sleep(1)
            return answer()
        provider = ScriptedProvider(slow)
        async with self.host(provider, policies=AgentWorkflowPolicies(nodes={
            "llm_call": AgentNodePolicy(timeout=TimeoutPolicy(.02))})) as host:
            result = await self.run_agent(host)
            self.assertEqual(result.status, "failed")
            self.assertEqual(result.error.type, "NodeTimeoutError")
            self.assertEqual(len(provider.requests), 1)

    async def test_custom_compaction_recovery_requires_explicit_replay_safety(self):
        """Recovery reruns a failed custom compactor only when its Node opts into replay."""
        for replay_safe in (False, True):
            attempts = []
            async def compact(value: CompactionInput) -> CompactionResult:
                attempts.append(1)
                if len(attempts) == 1:
                    raise RuntimeError("Compaction failed")
                return CompactionResult(view=value.view)
            policies = AgentWorkflowPolicies(nodes={"compact_context": AgentNodePolicy(
                recovery_mode=Recovery(mode="replay_safe"))}) if replay_safe else None
            async with self.host(ScriptedProvider(answer()), policies=policies,
                context_policy=ContextPolicy(trigger=always_compact, compact=compact)) as host:
                result = await self.run_agent(host)
                self.assertEqual(result.status, "failed")
                if replay_safe:
                    resumed = await host.recover(result.invocation_id)
                    self.assertEqual(resumed.status, "completed", resumed.error)
                    self.assertEqual(len(attempts), 2)
                else:
                    with self.assertRaises(RuntimeTransitionError) as caught:
                        await host.recover(result.invocation_id)
                    self.assertEqual(caught.exception.code, "RECOVERY_NOT_ALLOWED")
                    self.assertEqual(len(attempts), 1)

    @asynccontextmanager
    async def recorded_provider(self):
        captured = []
        async def handle(request):
            captured.append(request)
            return wire.httpx.Response(200, json=wire.chat_body(model="model-a"))
        @asynccontextmanager
        async def create():
            # SDK resources belong to Host's Runtime loop, not the test loop.
            sdk = wire.AsyncOpenAI(api_key="test", max_retries=0,
                http_client=wire.httpx.AsyncClient(transport=wire.httpx.MockTransport(handle)))
            sdk._platform = "Linux"
            try:
                async with OpenAIChatCompletionsProvider(OpenAIProviderConfig(provider_id="offline"), client=sdk) as provider:
                    yield provider
            finally:
                await sdk.close()
        yield create, captured

    @unittest.skipUnless(wire.AsyncOpenAI, "OpenAI SDK not installed")
    async def test_node_token_and_workflow_cost_limits_block_before_provider_calls(self):
        """Agent policy limits use the real Provider ledger and do not retry exhausted allowances."""
        def complete(value: CompleteRoundInput) -> RoundResult:
            return complete_agent_round(value).model_copy(update={"continue_loop": value.previous_rounds == 0})
        extension = AgentWorkflowExtensions(complete_round=Operator(complete, id="app.budget.loop", version="1"))
        pricing = LocalTokenPricing(overrides={"offline/model-a": TokenPrice("1", "1")}, use_litellm_catalog=False)
        cases = (AgentWorkflowPolicies(nodes={"llm_call": AgentNodePolicy(resource=ResourcePolicy(
            usage=UsagePolicy(max_total_tokens=5)), retry=RetryPolicy(max_attempts=3))}),
            AgentWorkflowPolicies(resource=WorkflowResourcePolicy(invocation=UsagePolicy(max_cost_usd="0.000005"))))
        for policies in cases:
            async with self.recorded_provider() as (provider, captured):
                async with self.host(provider, extensions=extension, policies=policies, pricing=pricing) as host:
                    result = await self.run_agent(host)
                    self.assertEqual(result.status, "failed")
                    self.assertIn("ResourceBudgetExceeded", str(result.error))
                    self.assertEqual(len(captured), 1)
                    summary = await host.token_usage(invocation_id=result.invocation_id, node_id="llm_call")
                    self.assertEqual((summary.call_count, summary.total_tokens), (1, 5))

    @unittest.skipUnless(wire.AsyncOpenAI, "OpenAI SDK not installed")
    async def test_compaction_and_main_generation_have_independent_node_budgets(self):
        """A permitted compaction call cannot spend the main model Node's separate allowance."""
        previous = Conversation(items=(LLMMessage(role="user", content=(TextContent(text="Earlier"),)), answer().output[0]))
        policies = AgentWorkflowPolicies(nodes={
            "compact_context": AgentNodePolicy(resource=ResourcePolicy(usage=UsagePolicy(max_total_tokens=5))),
            "llm_call": AgentNodePolicy(resource=ResourcePolicy(usage=UsagePolicy(max_total_tokens=0))),
        })
        async with self.recorded_provider() as (provider, captured):
            async with self.host(provider, policies=policies, context_policy=ContextPolicy(context_window=4096, trigger=always_compact)) as host:
                result = await self.run_agent(host, session_context={"agent_conversation": previous.model_dump(mode="json")})
                self.assertEqual(result.status, "failed")
                self.assertIn("ResourceBudgetExceeded", str(result.error))
                self.assertEqual(len(captured), 1)
                compression = await host.token_usage(invocation_id=result.invocation_id, node_id="compact_context")
                main = await host.token_usage(invocation_id=result.invocation_id, node_id="llm_call")
                self.assertEqual((compression.total_tokens, main.call_count), (5, 0))

    @unittest.skipUnless(wire.AsyncOpenAI, "OpenAI SDK not installed")
    async def test_workflow_session_budget_accumulates_across_agent_invocations(self):
        """A Session allowance blocks the next Agent turn after the first turn settles usage."""
        policies = AgentWorkflowPolicies(resource=WorkflowResourcePolicy(session=UsagePolicy(max_total_tokens=5)))
        async with self.recorded_provider() as (provider, captured):
            async with self.host(provider, policies=policies) as host:
                first = await self.run_agent(host)
                self.assertEqual(first.status, "completed", first.error)
                second = await self.run_agent(host, session_id=first.session_id)
                self.assertEqual(second.status, "failed")
                self.assertIn("ResourceBudgetExceeded", str(second.error))
                self.assertEqual(len(captured), 1)


class SQLiteAgentConfigurationTests(AgentConfigurationContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLAgentConfigurationTests(AgentConfigurationContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

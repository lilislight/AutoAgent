"""Context projection, feedback estimates, budget selection and custom compaction."""

from contextlib import asynccontextmanager
import math
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from autoagent.agent import (
    AgentExecutionSnapshot, AgentInput, AgentModelSettings, AgentSessionSettings,
    AgentWorkflowOptions, CompactionInput, CompactionResult, ContextLimitExceeded,
    ContextPolicy, ContextPreparation, ContextView, Conversation, HistorySegment,
    LLMHistoryCompactor, LLMToolCall, LLMToolDefinition, LLMToolResult,
    TextContent, TokenAnchor, TokenEstimate, UsagePlusCharacters, agent_runtime_context,
    agent_workflow, always_compact, build_agent_request, prepare_agent_context,
)
from autoagent.agent.context import restore
from autoagent.agent.workflow.compaction import DEFAULT_COMPACTION_PROMPT, validate_tool_history
from autoagent.agent.workflow.context import compact_agent_context, message
from autoagent.agent.workflow.policy import item_characters
from autoagent.host import AutoAgentHost, HostSettings
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import ScriptedProvider, answer, settings


class Service:
    def __init__(self, provider, *, compaction_model=None):
        self.provider, self.compaction_model, self.definitions = provider, compaction_model, ()
        self.state = AgentExecutionSnapshot(snapshot_id="stage-1", session_id="session", invocation_id="invocation",
            epoch=1, settings_version=0, settings=settings(), bindings={"provider": "1"}, reason="start", created_at_us=0)

    async def snapshot(self):
        return self.state

    def history_route(self, snapshot, model):
        return None

    def catalog(self, snapshot):
        return {item.name: SimpleNamespace(version="1", resource_id="test/tool/" + item.name)
            for item in self.definitions}, ()

    def toolset(self, snapshot, *, catalog):
        return SimpleNamespace(definitions=self.definitions, context=None)

    async def resolve_selection(self, request):
        from autoagent.agent import LLMSelection
        return LLMSelection(self.provider, request.model)


def preparation(conversation, *, fingerprint="fixed-1"):
    return ContextPreparation(conversation=conversation, fingerprint=fingerprint, snapshot_id="stage-1")


class ContextHistoryTests(unittest.TestCase):
    def test_repeated_replacement_and_append_preserve_original_items(self):
        """Successive summaries replace visible ranges while all original records survive."""
        original = tuple(message("user" if i % 2 == 0 else "assistant", f"item-{i}") for i in range(13))
        first = Conversation(items=original).replace(0, 6, (message("user", "S1"),))
        self.assertEqual(first.items, original)
        self.assertEqual(first.visible, (message("user", "S1"), *original[6:]))
        second = first.replace(0, 11, (message("user", "S2"),))
        self.assertEqual(second.items, original)
        self.assertEqual(second.visible, (message("user", "S2"), *original[11:]))
        appended = second.append(message("assistant", "new"))
        self.assertEqual(appended.view.generation, 2)
        self.assertEqual(appended.view.source_count, 14)
        self.assertEqual(restore(Conversation, appended).visible, appended.visible)
        with self.assertRaises(ValueError):
            second.replace(1, 11, (message("user", "invalid partial summary"),))

    def test_projection_rejects_gaps_and_out_of_range_references(self):
        """Persisted views cannot duplicate, omit or reference nonexistent original ranges."""
        with self.assertRaises(ValueError):
            ContextView(segments=(HistorySegment(start=1, end=2),))
        with self.assertRaises(ValueError):
            Conversation(items=(message("user", "only"),), view=ContextView(segments=(HistorySegment(start=0, end=2),)))

    def test_estimate_uses_input_anchor_and_only_new_content(self):
        """A valid anchor counts new visible output once and invalidates after rewriting."""
        original = Conversation(items=(message("user", "old"),), token_anchor=TokenAnchor(
            fingerprint="fixed-1", input_tokens=500, source_count=1, view_generation=0))
        added = message("assistant", "answer")
        conversation = original.append(added)
        estimator = UsagePlusCharacters()
        estimate = estimator(preparation(conversation))
        self.assertTrue(estimate.anchored)
        self.assertEqual(estimate.input_tokens, 500 + math.ceil(item_characters(added) / 4))
        self.assertFalse(estimator(preparation(conversation, fingerprint="changed-tools")).anchored)
        shortened = conversation.replace(0, 1, (message("user", "S"),))
        self.assertIsNone(shortened.token_anchor)
        self.assertFalse(estimator(preparation(shortened)).anchored)

    def test_absolute_thresholds_override_ratios_and_reserve_generation_space(self):
        """Small windows support explicit soft targets while output space limits admission."""
        policy = ContextPolicy(context_window=8192, compact_at_tokens=6500, target_tokens=5000)
        self.assertEqual(policy.budget({}).target_tokens, 5000)
        self.assertEqual(policy.budget({}).trigger_tokens, 6500)
        budget = policy.budget({"max_output_tokens": 4096})
        self.assertEqual((budget.input_limit, budget.trigger_tokens, budget.target_tokens), (4096, 4096, 4095))
        with self.assertRaises(ContextLimitExceeded):
            policy.budget({"max_output_tokens": 8192})
        with self.assertRaises(ValueError):
            ContextPolicy(context_window=100, compact_at_ratio=.4, target_ratio=.5)
        with self.assertRaises(ValueError):
            ContextPolicy(target_tokens=True)

    def test_tool_history_validation_preserves_pairs(self):
        """Clipping a result preserves its call ID; dropping only one side is rejected."""
        call = LLMToolCall(call_id="one", name="tool", arguments="{}")
        result = LLMToolResult(call_id="one", content=(TextContent(text="trimmed"),))
        validate_tool_history((call, result))
        for invalid in ((call,), (result,), (call, result, result)):
            with self.assertRaises(ValueError):
                validate_tool_history(invalid)


class ContextStrategyTests(unittest.IsolatedAsyncioTestCase):
    async def test_repeated_compaction_uses_current_summary_instead_of_original_prefix(self):
        """A second LLM summary sees S1 plus new history without reopening large original logs."""
        provider = ScriptedProvider(answer("S1 checkpoint"), answer("S2 checkpoint"))
        service = Service(provider)
        policy = ContextPolicy(context_window=8192, target_tokens=800, trigger=always_compact,
            compact=LLMHistoryCompactor(max_output_tokens=128))
        original = Conversation(items=(message("user", "ORIGINAL_OLD_LOG " * 120), message("assistant", "old answer"),
            message("user", "first current goal")))
        with agent_runtime_context(service):
            first = await compact_agent_context(await prepare_agent_context(original, AgentWorkflowOptions(), policy), policy)
            built = await build_agent_request(first, policy)
            self.assertEqual(first.preparation.conversation.items, original.items)
            later = Conversation(items=original.items, view=built.view).append(
                message("assistant", "NEW_PROGRESS " * 600), message("user", "next question"))
            second = await compact_agent_context(await prepare_agent_context(later, AgentWorkflowOptions(), policy), policy)
        second_prompt = str(provider.requests[1].input)
        self.assertIn("S1 checkpoint", second_prompt)
        self.assertIn("NEW_PROGRESS", second_prompt)
        self.assertNotIn("ORIGINAL_OLD_LOG", second_prompt)
        self.assertIn("S2 checkpoint", str(second.preparation.conversation.visible))
        self.assertEqual(second.preparation.conversation.items, later.items)
        self.assertEqual(provider.requests[0].routing["agent_role"], "compaction")
        self.assertIn("task/agent ID", DEFAULT_COMPACTION_PROMPT)
        self.assertIn("collected", DEFAULT_COMPACTION_PROMPT)

    async def test_compaction_falls_back_to_the_effective_main_call_model(self):
        """A per-call main model override is also the default compaction model."""
        provider = ScriptedProvider(answer("checkpoint"))
        policy = ContextPolicy(context_window=4096, trigger=always_compact,
            compact=LLMHistoryCompactor(max_output_tokens=128))
        original = Conversation(items=(message("user", "old"), message("assistant", "done"), message("user", "new")))
        with agent_runtime_context(Service(provider)):
            plan = await prepare_agent_context(original, AgentWorkflowOptions(llm_options={"model": "effective-main"}), policy)
            await compact_agent_context(plan, policy)
        self.assertEqual(provider.requests[0].model, "effective-main")

    async def test_resumed_preparation_rebinds_settings_but_preserves_code_overrides(self):
        """Explicit resume updates Session defaults without replacing Workflow model choices."""
        for overrides, expected in (({}, "resumed-main"), ({"model": "code-main"}, "code-main")):
            provider = ScriptedProvider(answer("checkpoint"))
            service = Service(provider)
            policy = ContextPolicy(context_window=4096, trigger=always_compact,
                compact=LLMHistoryCompactor(max_output_tokens=128))
            original = Conversation(items=(message("user", "old"), message("assistant", "done"), message("user", "new")))
            with agent_runtime_context(service):
                plan = await prepare_agent_context(original, AgentWorkflowOptions(llm_options=overrides), policy)
                service.state = service.state.model_copy(update={"snapshot_id": "stage-2", "settings": settings(model="resumed-main")})
                compacted = await compact_agent_context(plan, policy)
                built = await build_agent_request(compacted, policy)
            self.assertEqual(provider.requests[0].model, expected)
            self.assertEqual(built.request.model, expected)
            self.assertEqual(built.request.routing["agent_snapshot_id"], "stage-2")

    async def test_environment_model_is_frozen_until_an_explicit_new_stage(self):
        """Automatic restore retains prepared env defaults; explicit resume can adopt new ones."""
        provider = ScriptedProvider(answer("checkpoint-old"), answer("checkpoint-new"))
        service = Service(provider, compaction_model="env-old")
        policy = ContextPolicy(context_window=4096, trigger=always_compact,
            compact=LLMHistoryCompactor(max_output_tokens=128))
        original = Conversation(items=(message("user", "old"), message("assistant", "done"), message("user", "new")))
        with agent_runtime_context(service):
            plan = await prepare_agent_context(original, AgentWorkflowOptions(), policy)
            plan = restore(type(plan), plan)
            service.compaction_model = "env-new"
            await compact_agent_context(plan, policy)
            service.state = service.state.model_copy(update={"snapshot_id": "stage-2"})
            await compact_agent_context(plan, policy)
        self.assertEqual([request.model for request in provider.requests], ["env-old", "env-new"])

    async def test_custom_rule_compaction_needs_no_provider_call(self):
        """Always-triggered synchronous rules can replace complete Tool exchanges locally."""
        provider = ScriptedProvider()
        call = LLMToolCall(call_id="one", name="tool", arguments="{}")
        result = LLMToolResult(call_id="one", content=(TextContent(text="long result"),))
        conversation = Conversation(items=(message("user", "goal"), call, result, message("assistant", "done")))
        def compact(value):
            return CompactionResult(view=value.conversation.replace(1, 3, (message("user", "Tool checkpoint"),)).view)
        policy = ContextPolicy(trigger=always_compact, compact=compact)
        with agent_runtime_context(Service(provider)):
            plan = await prepare_agent_context(conversation, AgentWorkflowOptions(), policy)
            self.assertTrue(plan.needs_compaction)
            result = await compact_agent_context(plan, policy)
            built = await build_agent_request(result, policy)
        self.assertIn("Tool checkpoint", str(built.request.input))
        self.assertEqual(provider.requests, [])
        self.assertEqual(result.preparation.conversation.items, conversation.items)

    async def test_custom_estimator_and_trigger_drive_the_real_plan(self):
        """User callbacks receive fixed sources and their estimate determines branch activation."""
        observed = []
        def estimate(value):
            observed.append(value.fixed_input)
            return TokenEstimate(input_tokens=88)
        def trigger(value):
            return value.estimate.input_tokens == 88
        policy = ContextPolicy(estimate=estimate, trigger=trigger)
        with agent_runtime_context(Service(ScriptedProvider())):
            plan = await prepare_agent_context(Conversation(items=(message("user", "small"),)), AgentWorkflowOptions(), policy)
        self.assertTrue(plan.needs_compaction)
        self.assertEqual(plan.estimate.input_tokens, 88)
        self.assertEqual(observed[0][0].role, "system")

    async def test_fixed_sources_are_reused_after_catalog_changes(self):
        """Build uses the exact Tool catalog evaluated by Prepare, without a second live read."""
        service = Service(ScriptedProvider())
        service.definitions = (LLMToolDefinition(name="original", parameters={"type": "object"}),)
        with agent_runtime_context(service):
            plan = await prepare_agent_context(Conversation(items=(message("user", "question"),)), AgentWorkflowOptions(), ContextPolicy())
            service.definitions = (LLMToolDefinition(name="new", parameters={"type": "object"}),)
            result = await build_agent_request(plan, ContextPolicy())
        self.assertEqual([tool.name for tool in result.request.tools], ["original"])

    async def test_no_fixed_recent_count_can_force_an_oversized_tail(self):
        """Large recent completed exchanges may be summarized while the latest question survives."""
        provider = ScriptedProvider(answer("Short summary"))
        original = Conversation(items=(message("user", "earlier"), message("assistant", "large " * 1500),
            message("user", "latest")))
        policy = ContextPolicy(context_window=4096, target_tokens=600,
            compact=LLMHistoryCompactor(max_output_tokens=128))
        with agent_runtime_context(Service(provider)):
            plan = await prepare_agent_context(original, AgentWorkflowOptions(), policy)
            result = await compact_agent_context(plan, policy)
            request = await build_agent_request(result, policy)
        self.assertEqual(request.request.input[-1], original.items[-1])
        self.assertNotIn(original.items[1], request.request.input)
        self.assertLess(result.estimate.input_tokens, 600)

    async def test_invalid_compaction_never_changes_original_session_data(self):
        """Orphan Tool results and failed summaries are rejected before committing a view."""
        original = Conversation(items=(message("user", "old"), message("assistant", "done"), message("user", "new")))
        async def invalid(value):
            return CompactionResult(view=value.conversation.replace(0, 2, (
                LLMToolResult(call_id="missing", content=(TextContent(text="bad"),)),)).view)
        policy = ContextPolicy(trigger=always_compact, compact=invalid)
        with agent_runtime_context(Service(ScriptedProvider())):
            plan = await prepare_agent_context(original, AgentWorkflowOptions(), policy)
            with self.assertRaises(ValueError):
                await compact_agent_context(plan, policy)
        self.assertEqual(original.view.generation, 0)
        self.assertIsNone(original.view.segments[0].replacement)

    async def test_large_history_is_summarized_in_bounded_incremental_batches(self):
        """Internal batches feed their previous summary forward and all fit the configured window."""
        provider = ScriptedProvider(answer("checkpoint-one"), answer("checkpoint-two"), answer("checkpoint-three"))
        items = tuple(item for index in range(3) for item in (
            message("user", f"goal-{index}"), message("assistant", f"part-{index} " + "x" * 3000)))
        conversation = Conversation(items=(*items, message("user", "latest")))
        policy = ContextPolicy(context_window=2048, target_tokens=400,
            compact=LLMHistoryCompactor(max_output_tokens=128))
        with agent_runtime_context(Service(provider)):
            plan = await prepare_agent_context(conversation, AgentWorkflowOptions(), policy)
            result = await compact_agent_context(plan, policy)
        self.assertGreater(len(provider.requests), 1)
        self.assertIn("checkpoint-one", str(provider.requests[1].input))
        for request in provider.requests:
            size = UsagePlusCharacters()(preparation(Conversation(items=request.input))).input_tokens
            self.assertLessEqual(size + 128, 2048)
        self.assertEqual(result.preparation.conversation.items, conversation.items)
        self.assertEqual(result.preparation.conversation.visible[-1], conversation.items[-1])

    async def test_target_is_soft_when_fixed_sources_already_exceed_it(self):
        """A useful summary above the soft target is accepted when the full request fits."""
        provider = ScriptedProvider(answer("compact"))
        policy = ContextPolicy(context_window=4096, target_tokens=300, trigger=always_compact,
            compact=LLMHistoryCompactor(max_output_tokens=128))
        original = Conversation(items=(message("user", "old"), message("assistant", "old reply"), message("user", "current")))
        with agent_runtime_context(Service(provider)):
            plan = await prepare_agent_context(original, AgentWorkflowOptions(instructions="rules " * 400), policy)
            result = await compact_agent_context(plan, policy)
            await build_agent_request(result, policy)
        self.assertGreater(result.estimate.input_tokens, 300)
        self.assertEqual(len(provider.requests), 1)

    async def test_always_strategy_on_first_question_needs_no_summary_call(self):
        """Always processing a first question does not fail or waste a model call."""
        provider = ScriptedProvider()
        policy = ContextPolicy(context_window=4096, trigger=always_compact)
        original = Conversation(items=(message("user", "first question"),))
        with agent_runtime_context(Service(provider)):
            plan = await prepare_agent_context(original, AgentWorkflowOptions(), policy)
            result = await compact_agent_context(plan, policy)
            await build_agent_request(result, policy)
        self.assertEqual(provider.requests, [])
        self.assertEqual(result.preparation.conversation.visible, original.items)

    async def test_compaction_request_is_bounded_before_sending(self):
        """An indivisible oversized Tool exchange fails without sending a known oversized request."""
        provider = ScriptedProvider()
        conversation = Conversation(items=(message("user", "old"), message("assistant", "huge " * 5000),
            message("user", "new")))
        policy = ContextPolicy(context_window=2048, target_tokens=400, compact=LLMHistoryCompactor(max_output_tokens=128))
        with agent_runtime_context(Service(provider)):
            plan = await prepare_agent_context(conversation, AgentWorkflowOptions(), policy)
            with self.assertRaises(ContextLimitExceeded):
                await compact_agent_context(plan, policy)
        self.assertEqual(provider.requests, [])


class ContextHostContracts:
    async def database_url(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        return "sqlite+aiosqlite:///" + str(Path(temporary.name) / "runtime.db")

    async def test_compaction_model_priority_and_persistent_view_after_restart(self):
        """Code overrides env overrides main model, and restarted Sessions keep their compacted view."""
        url = await self.database_url()
        for configured, environment, expected in (("code-model", "env-model", "code-model"),
                (None, "env-model", "env-model"), (None, "", "model-a")):
            with self.subTest(model=expected):
                provider = ScriptedProvider(answer("PERSISTED_SUMMARY"), answer("first final"), answer("next final"))
                @asynccontextmanager
                async def lifespan(setup):
                    setup.agent_context.resources.register_provider("offline", provider)
                    setup.agent_context.set_defaults(settings())
                    yield
                # A low explicit threshold forces the first large history to compact;
                # the next turn's real usage anchor prevents an unnecessary second summary.
                policy = ContextPolicy(context_window=4096, compact_at_tokens=500, target_tokens=300,
                    compact=LLMHistoryCompactor(model=configured, max_output_tokens=128))
                workflow = agent_workflow(context_policy=policy, user_events=True)
                options = dict(workflows=[workflow], lifespan=lifespan, settings=HostSettings(database_url=url),
                    environment={"AUTOAGENT_COMPACTION_LLM_MODEL": environment})
                old = Conversation(items=(message("user", "ORIGINAL_LONG " * 200), message("assistant", "old answer")))
                async with AutoAgentHost(**options) as host:
                    first = await host.join(await host.execute("agent", AgentInput(message="now"),
                        session_context={"agent_conversation": old.model_dump(mode="json")}))
                    self.assertEqual(first.status, "completed", first.error)
                    self.assertEqual([item.model for item in provider.requests], [expected, "model-a"])
                    events = (await host._persistence.user_events(first.invocation_id)).items
                    compactions = [event for event in events if event["kind"] == "agent.compaction"]
                    self.assertEqual(len(compactions), 1)
                    self.assertEqual(compactions[0]["payload"]["state"], "completed")
                    self.assertEqual(compactions[0]["payload"]["node_id"], "build_request")
                async with AutoAgentHost(**options) as host:
                    second = await host.join(await host.execute("agent", AgentInput(message="next"), session_id=first.session_id))
                    self.assertEqual(second.status, "completed", second.error)
                self.assertEqual(len(provider.requests), 3)
                self.assertIn("PERSISTED_SUMMARY", str(provider.requests[-1].input))
                self.assertNotIn("ORIGINAL_LONG", str(provider.requests[-1].input))


class SQLiteContextHostTests(ContextHostContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLContextHostTests(ContextHostContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

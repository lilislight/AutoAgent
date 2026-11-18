"""Real Provider entry-point budgets, Runtime identities and Host restart."""

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from autoagent import (
    ConditionContext, Edge, InputMappingContext, Map, Node, ResourcePolicy, RetryPolicy, UsagePolicy,
    Workflow, WorkflowResourcePolicy,
)
from autoagent.core.app import AutoAgentApp
from autoagent.core.compiler import WorkflowCompiler
from autoagent.agent import (
    LLMRequest, LLMResponse, LocalTokenPricing, TokenPrice, OpenAIProviderConfig, OpenAIResponsesProvider,
    OpenAIChatCompletionsProvider, ProjectUsagePolicy, UsageWindow, WindowUsagePolicy,
)
from autoagent.host import AutoAgentHost, HostSettings
from tests import test_agent_providers as wire
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase


def restore_request(context: InputMappingContext) -> LLMRequest:
    from autoagent.core.runtime.values import thaw
    return LLMRequest.model_validate_json(json.dumps(thaw(context.invocation_input)))


def restore_requests(context: InputMappingContext) -> list[LLMRequest]:
    return [restore_request(context) for _ in range(3)]


def always(context: ConditionContext) -> bool:
    return True


def never(context: ConditionContext) -> bool:
    return False


class TokenAdmissionContracts:
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.url = await self.database_url(Path(self.temporary.name) / "usage.sqlite")
        self.settings = HostSettings(database_url=self.url)
        self.captured, self.holder = [], {}
        self.protocol, self.streaming, self.record_calls = "responses", False, True

    async def database_url(self, path):
        return "sqlite+aiosqlite:///" + str(path)

    @asynccontextmanager
    async def resources(self, setup):
        async def handle(request):
            self.captured.append(request)
            if self.protocol == "responses":
                response = wire.response_body()
                if self.streaming:
                    return wire.httpx.Response(200, content=wire.sse([{"type": "response.completed", "response": response}]),
                                               headers={"content-type": "text/event-stream"})
            else:
                response = wire.chat_body()
                if self.streaming:
                    chunks = [wire.chat_chunk({"content": "Answer"}), wire.chat_chunk({}, "stop"),
                              {"id":"chat_1", "model":"model", "object":"chat.completion.chunk", "created":1,
                               "choices":[], "usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}]
                    return wire.httpx.Response(200, content=wire.sse(chunks, done=True), headers={"content-type":"text/event-stream"})
            return wire.httpx.Response(200, json=response)
        sdk = wire.AsyncOpenAI(api_key="test", max_retries=0,
            http_client=wire.httpx.AsyncClient(transport=wire.httpx.MockTransport(handle)))
        sdk._platform = "Linux"
        cls = OpenAIResponsesProvider if self.protocol == "responses" else OpenAIChatCompletionsProvider
        try:
            async with cls(OpenAIProviderConfig(provider_id="test"), client=sdk, record_calls=self.record_calls) as provider:
                self.holder["provider"] = provider
                yield
        finally:
            await sdk.close()

    def workflow(self, *, resource=None, node_resource=None, twice=False, retry=None, version="1", name="research"):
        async def generate(value: LLMRequest) -> LLMResponse:
            provider = self.holder["provider"]
            async def once():
                if self.streaming:
                    chunks = [chunk async for chunk in provider.astream(value)]
                    return chunks[-1].response
                return await provider.ainvoke(value)
            result = await once()
            return await once() if twice else result
        return Workflow(name, version=version, resource=resource or WorkflowResourcePolicy(),
                        nodes=[Node("analyze", generate, input_mapping=restore_request,
                                    resource=node_resource or ResourcePolicy(), retry=retry)])

    def host(self, workflows, **options):
        return AutoAgentHost(workflows=workflows, settings=self.settings, lifespan=self.resources,
                             environment={}, root=self.temporary.name, **options)

    async def run_call(self, host, workflow="research", **options):
        return await host.join(await host.execute(workflow, wire.request().model_dump(mode="json"), **options))

    async def test_node_limits_check_every_call_inside_one_operator_without_retry(self):
        """Exhaustion inside an Operator prevents its second HTTP request and Core retries."""
        workflow = self.workflow(twice=True, retry=RetryPolicy(max_attempts=3),
                                 node_resource=ResourcePolicy(usage=UsagePolicy(max_total_tokens=4)))
        async with self.host([workflow]) as host:
            result = await self.run_call(host)
            self.assertEqual(result.status, "failed")
            self.assertIn("ResourceBudgetExceeded", str(result.error))
            usage = await host.token_usage(invocation_id=result.invocation_id, node_id="analyze")
            self.assertEqual((usage.call_count, usage.total_tokens), (1, 5))
        self.assertEqual(len(self.captured), 1)

    async def test_cost_budget_stops_provider_calls_even_when_optional_recording_is_disabled(self):
        """Settled USD charges enforce Session limits at the actual Provider boundary."""
        self.record_calls = False
        pricing = LocalTokenPricing(overrides={"test/model": TokenPrice("1", "3")}, use_litellm_catalog=False)
        workflow = self.workflow(resource=WorkflowResourcePolicy(session=UsagePolicy(max_cost_usd="0.000009")))
        for self.protocol in ("responses", "chat"):
            for self.streaming in (False, True):
                with self.subTest(protocol=self.protocol, streaming=self.streaming):
                    before = len(self.captured)
                    async with self.host([workflow], pricing=pricing) as host:
                        first = await self.run_call(host)
                        self.assertEqual(first.status, "completed", first.error)
                        second = await self.run_call(host, session_id=first.ref.session_id)
                        self.assertEqual(second.status, "failed")
                        self.assertIn("cost_usd budget exhausted", str(second.error))
                        summary = await host.token_usage(session_id=first.ref.session_id)
                        self.assertEqual((summary.call_count, summary.total_tokens, summary.cost_usd), (1, 5, "0.000009"))
                    self.assertEqual(len(self.captured), before + 1)

    async def test_invocation_limit_applies_to_separate_nodes(self):
        """Nodes share the Invocation allowance instead of each receiving its own copy."""
        first = self.workflow(resource=WorkflowResourcePolicy(invocation=UsagePolicy(max_total_tokens=5)))
        first.nodes.append(Node("finish", first.nodes[0].executable, input_mapping=restore_request))
        first.edges.append(Edge("analyze", "finish"))
        async with self.host([first]) as host:
            result = await self.run_call(host)
            self.assertEqual(result.status, "failed")
            self.assertEqual((await host.token_usage(invocation_id=result.invocation_id)).total_tokens, 5)
        self.assertEqual(len(self.captured), 1)

    async def test_session_allowance_survives_restart_and_revision_change(self):
        """Session history is reused after restart and across Workflow revisions."""
        resource = WorkflowResourcePolicy(session=UsagePolicy(max_total_tokens=5))
        async with self.host([self.workflow(resource=resource)]) as host:
            first = await self.run_call(host)
            self.assertEqual(first.status, "completed", first.error)
        async with self.host([self.workflow(resource=resource, version="2")]) as host:
            second = await self.run_call(host, session_id=first.ref.session_id)
            self.assertEqual(second.status, "failed")
            self.assertIn("session", str(second.error))
            self.assertEqual((await host.token_usage(session_id=first.ref.session_id)).total_tokens, 5)
        self.assertEqual(len(self.captured), 1)

    async def test_workflow_window_covers_all_revisions_but_not_other_workflows(self):
        """Window allowances group Workflow IDs across revisions independently of Sessions."""
        policy = ProjectUsagePolicy(workflows={"research": WindowUsagePolicy(UsageWindow("day"), UsagePolicy(max_total_tokens=5))})
        async with self.host([self.workflow()], usage_policy=policy) as host:
            self.assertEqual((await self.run_call(host)).status, "completed")
        async with self.host([self.workflow(version="2"), self.workflow(name="other")], usage_policy=policy) as host:
            blocked = await self.run_call(host)
            self.assertEqual(blocked.status, "failed")
            self.assertEqual((await self.run_call(host, "other")).status, "completed")
            self.assertEqual((await host.token_usage(workflow_id="research")).total_tokens, 5)
        self.assertEqual(len(self.captured), 2)

    async def test_project_allowance_cannot_be_bypassed_by_disabling_recording(self):
        """Mandatory metering protects the project even when optional observation is off."""
        self.record_calls = False
        policy = ProjectUsagePolicy(project=WindowUsagePolicy(UsageWindow("month"), UsagePolicy(max_total_tokens=5)))
        async with self.host([self.workflow(), self.workflow(name="other")], usage_policy=policy) as host:
            self.assertEqual((await self.run_call(host)).status, "completed")
            self.assertEqual((await self.run_call(host, "other")).status, "failed")
            self.assertEqual((await host.token_usage()).total_tokens, 5)
        self.assertEqual(len(self.captured), 1)

    async def test_both_stream_protocols_meter_one_terminal_usage(self):
        """Responses and Chat streams settle once and block the next request."""
        self.streaming = True
        self.protocol = "chat"
        workflow = self.workflow(twice=True, node_resource=ResourcePolicy(usage=UsagePolicy(max_total_tokens=5)))
        async with self.host([workflow]) as host:
            result = await self.run_call(host)
            self.assertEqual(result.status, "failed", result.error)
            self.assertEqual((await host.token_usage()).total_tokens, 5)
        self.assertEqual(len(self.captured), 1)

    async def test_responses_stream_checks_before_second_request(self):
        """Responses streaming uses the same admission and terminal settlement path."""
        self.streaming = True
        workflow = self.workflow(twice=True, node_resource=ResourcePolicy(usage=UsagePolicy(max_total_tokens=5)))
        async with self.host([workflow]) as host:
            result = await self.run_call(host)
            self.assertEqual(result.status, "failed", result.error)
            self.assertEqual((await host.token_usage()).total_tokens, 5)
        self.assertEqual(len(self.captured), 1)

    async def test_failed_admission_never_reaches_http_transport(self):
        """Database admission errors are non-retryable and cannot send a request."""
        workflow = self.workflow(retry=RetryPolicy(max_attempts=2),
                                 node_resource=ResourcePolicy(usage=UsagePolicy(max_total_tokens=5)))
        async with self.host([workflow]) as host:
            with patch.object(host._persistence, "admit_llm_call", AsyncMock(side_effect=RuntimeError("database offline"))):
                result = await self.run_call(host)
            self.assertEqual(result.status, "failed")
            self.assertIn("UsageAccountingError", str(result.error))
        self.assertEqual(len(self.captured), 0)

    async def test_failed_settlement_keeps_response_and_blocks_next_request(self):
        """An accounting failure cannot resend a successful call or erase its unknown liability."""
        workflow = self.workflow(resource=WorkflowResourcePolicy(session=UsagePolicy(max_total_tokens=100)))
        async with self.host([workflow]) as host:
            with patch.object(host._persistence, "record_llm_call", AsyncMock(side_effect=RuntimeError("write failed"))):
                first = await self.run_call(host)
            self.assertEqual(first.status, "completed", first.error)
            second = await self.run_call(host, session_id=first.ref.session_id)
            self.assertEqual(second.status, "failed")
            self.assertIn("UsageAccountingError", str(second.error))
            summary = await host.token_usage()
            self.assertEqual((summary.call_count, summary.unsettled_calls), (1, 1))
        self.assertEqual(len(self.captured), 1)

    async def test_map_units_share_node_allowance(self):
        """Sequential Map units consume one shared Node token allowance."""
        workflow = self.workflow(node_resource=ResourcePolicy(usage=UsagePolicy(max_total_tokens=10)))
        workflow.nodes[0].input_mapping = restore_requests
        workflow.nodes[0].map = Map(max_parallelism=1)
        async with self.host([workflow]) as host:
            result = await self.run_call(host)
            self.assertEqual(result.status, "failed", result.error)
            self.assertEqual((await host.token_usage(invocation_id=result.invocation_id, node_id="analyze")).total_tokens, 10)
        self.assertEqual(len(self.captured), 2)

    async def test_loop_occurrences_share_node_allowance(self):
        """A new loop occurrence cannot reset its Node token allowance."""
        workflow = self.workflow(node_resource=ResourcePolicy(usage=UsagePolicy(max_total_tokens=5)))
        def identity(value: LLMRequest) -> LLMRequest:
            return value
        def finish(value: LLMResponse) -> LLMResponse:
            return value
        workflow.nodes.insert(0, Node("entry", identity, input_mapping=restore_request))
        workflow.nodes.append(Node("finish", finish))
        workflow.edges = [Edge("entry", "analyze"), Edge("analyze", "analyze", always, id="back"),
                          Edge("analyze", "finish", never, id="exit")]
        async with self.host([workflow]) as host:
            result = await self.run_call(host)
            self.assertEqual(result.status, "failed", result.error)
            self.assertIn("ResourceBudgetExceeded", str(result.error))
        self.assertEqual(len(self.captured), 1)

    async def test_retry_keeps_tokens_spent_before_handler_failure(self):
        """Retries retain earlier real LLM usage even when the enclosing handler failed."""
        workflow = self.workflow(node_resource=ResourcePolicy(usage=UsagePolicy(max_total_tokens=10)),
                                 retry=RetryPolicy(max_attempts=5))
        async def fail_after_call(value: LLMRequest) -> LLMResponse:
            await self.holder["provider"].ainvoke(value)
            raise ValueError("handler failed after model response")
        workflow.nodes[0].executable = fail_after_call
        async with self.host([workflow]) as host:
            result = await self.run_call(host)
            self.assertEqual(result.status, "failed", result.error)
            self.assertIn("ResourceBudgetExceeded", str(result.error))
            self.assertEqual((await host.token_usage()).total_tokens, 10)
        self.assertEqual(len(self.captured), 2)

    async def test_embedding_calls_share_admission_without_inventing_output_tokens(self):
        """Embedding token limits stop the next request while absent output usage stays unknown."""
        from autoagent.agent import EmbeddingRequest, EmbeddingResponse, OpenAIEmbeddingProvider
        from tests.test_agent_embeddings import body
        @asynccontextmanager
        async def resources(setup):
            async def handle(request):
                self.captured.append(request)
                return wire.httpx.Response(200, json=body())
            sdk = wire.AsyncOpenAI(api_key="test", max_retries=0,
                http_client=wire.httpx.AsyncClient(transport=wire.httpx.MockTransport(handle)))
            sdk._platform = "Linux"
            try:
                async with OpenAIEmbeddingProvider(client=sdk, record_calls=False) as provider:
                    self.holder["provider"] = provider
                    yield
            finally:
                await sdk.close()
        self.resources = resources
        async def generate() -> EmbeddingResponse:
            request = EmbeddingRequest(model="embedding-model", input="text")
            await self.holder["provider"].ainvoke(request)
            return await self.holder["provider"].ainvoke(request)
        workflow = Workflow("embedding", nodes=[Node("embed", generate,
            resource=ResourcePolicy(usage=UsagePolicy(max_total_tokens=3)))])
        async with self.host([workflow]) as host:
            result = await host.join(await host.execute(workflow.id, None))
            self.assertEqual(result.status, "failed", result.error)
            self.assertIn("ResourceBudgetExceeded", str(result.error))
            summary = await host.token_usage(kind="embedding")
            self.assertEqual((summary.call_count, summary.input_tokens, summary.total_tokens), (1, 3, 3))
            self.assertIsNone(summary.output_tokens)
        self.assertEqual(len(self.captured), 1)


class SQLiteTokenAdmissionTests(TokenAdmissionContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLTokenAdmissionTests(TokenAdmissionContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self, path):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

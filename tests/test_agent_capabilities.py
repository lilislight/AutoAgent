"""Canonical LLM/Embedding contracts, Provider adapters and Host registration."""

import asyncio
import subprocess
import sys
import tempfile
import unittest
from contextlib import aclosing, asynccontextmanager, closing
from pathlib import Path

from typing_extensions import TypedDict

from autoagent import (
    Node, Operator, Stream, TimeoutPolicy, UserEventContext,
    UserEventSpec, ValueContract, Workflow,
)
from autoagent.core.app import AutoAgentApp
from autoagent.agent import (
    EMBEDDING, LLM_CALL, EmbeddingProvider, EmbeddingRequest, EmbeddingResponse,
    EmbeddingVector, LLMMessage, LLMProvider, LLMRequest, LLMResponse,
    LLMStreamChunk, LLMStreamReducer, TextContent, embedding_operator, llm_call_operator,
)
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from autoagent.agent.integration.llm import LLMStreamState
from autoagent import StreamContext
from autoagent.core.compiler import WorkflowCompiler


class TextAnswer(TypedDict):
    text: str


class TextReducer(LLMStreamReducer):
    def finish(self, context: StreamContext, state: LLMStreamState) -> TextAnswer:
        return {"text": super().finish(context, state).text}


class Value(TypedDict):
    value: int


def identity(value: Value) -> Value:
    return value


def request(**options):
    return LLMRequest(**{
        "model": "test-model",
        "input": (LLMMessage(role="user", content=(TextContent(text="Hello"),)),),
        **options,
    })


def response():
    return LLMResponse(model="test-model", output=(LLMMessage(role="assistant", content=(TextContent(text="Answer"),)),), finish_reason="stop")


def llm_events(context: UserEventContext) -> UserEventSpec | None:
    if context.phase == "chunk":
        if context.value["type"] == "delta":
            return UserEventSpec(kind="llm.delta", payload={"delta": context.value["delta"]})
        return None
    return UserEventSpec(kind="llm.completed", payload=context.value)


class DemoLLM(LLMProvider):
    def __init__(self):
        self.calls = []
        self.closed = False
        self.streams_closed = 0
        self.block_stream = False
        self.omit_terminal = False

    async def ainvoke(self, request: LLMRequest) -> LLMResponse:
        self.calls.append(("invoke", request))
        return response()

    async def astream(self, request: LLMRequest):
        self.calls.append(("stream", request))
        try:
            yield LLMStreamChunk(type="delta", item_key="message", kind="text", delta="Answer")
            if self.block_stream:
                await asyncio.Future()
            if not self.omit_terminal:
                yield LLMStreamChunk(type="completed", response=response())
        finally:
            self.streams_closed += 1

    async def aclose(self):
        self.closed = True


class DemoEmbedding(EmbeddingProvider):
    def __init__(self):
        self.calls = []
        self.closed = False

    async def ainvoke(self, request: EmbeddingRequest) -> EmbeddingResponse:
        self.calls.append(request)
        return EmbeddingResponse(model=request.model, data=tuple(
            EmbeddingVector(index=index, embedding=(.1, .2)) for index in range(request.input_count)))

    async def aclose(self):
        self.closed = True


class AgentCapabilityTests(unittest.TestCase):
    def test_contracts_use_canonical_models_and_one_llm_identity(self):
        """Ordinary and streaming implementations share one nominal Capability contract."""
        self.assertEqual(LLM_CALL.id, "llm.call")
        self.assertIs(LLM_CALL.contract.input.annotation, LLMRequest)
        self.assertIs(LLM_CALL.contract.stream_chunk.annotation, LLMStreamChunk)
        self.assertIsNone(LLM_CALL.contract.output)
        self.assertEqual(EMBEDDING.id, "embedding.call")
        self.assertIs(EMBEDDING.contract.input.annotation, EmbeddingRequest)
        self.assertIs(EMBEDDING.contract.output.annotation, EmbeddingResponse)
        self.assertIsNone(EMBEDDING.contract.stream_chunk)
        provider = DemoLLM()
        with closing(AutoAgentApp()) as app:
            app.register_capability(LLM_CALL)
            app.register_operator(llm_call_operator(provider), capability_id=LLM_CALL.id)
            with self.assertRaisesRegex(ValueError, "contract"):
                app.register_operator(Operator(provider.ainvoke), capability_id=LLM_CALL.id)

    def test_optional_sdks_are_not_imported_by_declarations(self):
        """Importing and declaring Capabilities works without installing Provider SDKs."""
        script = '''
import importlib.abc
import sys
class NoSDK(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, *args):
        if fullname.split('.')[0] in {'openai', 'litellm', 'mcp', 'httpx', 'yaml'}:
            raise AssertionError('Unexpected optional import: ' + fullname)
sys.meta_path.insert(0, NoSDK())
from autoagent.agent import LLM_CALL, EMBEDDING
assert LLM_CALL.id == 'llm.call'
assert EMBEDDING.id == 'embedding.call'
'''
        result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_adapter_options_reject_invalid_providers(self):
        """Bad Providers and resolver hooks fail before Operator registration."""
        for factory in (lambda: llm_call_operator(object()), lambda: llm_call_operator(resolver=object()),
                        lambda: llm_call_operator(DemoLLM(), streaming=True), lambda: embedding_operator(object())):
            with self.assertRaises(TypeError):
                factory()


class AgentCapabilityExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_default_reducer_does_not_mutate_node_and_explicit_override_is_allowed(self):
        """Compilation supplies the Capability default without changing authored Nodes or blocking custom outputs."""
        provider = DemoLLM()
        node = Node("call", LLM_CALL)
        workflow = Workflow("implicit", nodes=[node])
        explicit = Workflow("implicit", nodes=[Node("call", LLM_CALL, stream=Stream(LLMStreamReducer()))])
        compiler = WorkflowCompiler()
        self.assertEqual(compiler.compile_or_raise(workflow).workflow_revision_id,
                         compiler.compile_or_raise(explicit).workflow_revision_id)
        self.assertIsNone(node.stream)
        async with aclosing(AutoAgentApp()) as app:
            app.register_operator(llm_call_operator(provider), capability_id=LLM_CALL.id)
            custom = Workflow("custom", nodes=[Node("call", LLM_CALL, stream=Stream(TextReducer()))])
            result = await app.ainvoke(custom, request(streaming=True))
        self.assertEqual(result.status, "completed", result.error)
        self.assertEqual(result.output, {"text": "Answer"})

    async def test_one_mapper_emits_delta_and_final_response_without_duplicate_completion(self):
        """Explicit LLM mapping can ignore terminal chunks and emit the Node output exactly once."""
        events = []

        class Sink:
            async def append_user_event(self, event):
                events.append(event)

        provider = DemoLLM()
        workflow = Workflow("mapped-llm", nodes=[Node("call", LLM_CALL,
            user_events=llm_events)])
        async with aclosing(AutoAgentApp(user_event_sink=Sink())) as app:
            app.register_operator(llm_call_operator(provider), capability_id=LLM_CALL.id, default=True)
            result = await app.ainvoke(workflow, request(streaming=True))
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual([event.kind for event in events], ["llm.delta", "llm.completed"])
            payload = events[-1].to_record()["payload"]
            self.assertEqual(ValueContract.create(LLMResponse, location="event").restore(payload).text, "Answer")

    async def test_per_request_streaming_reduces_to_the_same_response(self):
        """One Operator serves both transport modes without changing the Workflow Revision."""
        events = []
        class Sink:
            async def append_user_event(self, event):
                events.append(event)
        provider = DemoLLM()
        workflow = Workflow("llm", nodes=[Node("call", LLM_CALL)])
        async with aclosing(AutoAgentApp(user_event_sink=Sink())) as app:
            app.register_operator(llm_call_operator(provider), capability_id=LLM_CALL.id, default=True)
            first = await app.ainvoke(workflow, request())
            self.assertEqual(first.status, "completed", first.error)
            self.assertEqual(events, [])
            events.clear()
            second = await app.ainvoke(workflow, request(streaming=True))
            self.assertEqual(second.status, "completed", second.error)
            self.assertEqual(first.ref.workflow_revision_id, second.ref.workflow_revision_id)
            self.assertEqual(first.output, second.output)
            self.assertEqual(ValueContract.create(LLMResponse, location="output").restore(second.output).text, "Answer")
            self.assertEqual(events, [])
        self.assertEqual([mode for mode, _ in provider.calls], ["invoke", "stream"])
        self.assertEqual(provider.streams_closed, 1)
        self.assertFalse(provider.closed)

    async def test_embedding_capability_handles_a_batch(self):
        """Embedding Nodes preserve the canonical request and restore typed vector responses."""
        provider = DemoEmbedding()
        payload = EmbeddingRequest(model="test-embedding", input=("first", "second"))
        async with aclosing(AutoAgentApp()) as app:
            app.register_operator(embedding_operator(provider), capability_id=EMBEDDING.id, default=True)
            result = await app.ainvoke(Workflow("embedding", nodes=[Node("call", EMBEDDING)]), payload)
        self.assertEqual(result.status, "completed", result.error)
        restored = ValueContract.create(EmbeddingResponse, location="output").restore(result.output)
        self.assertEqual(restored.vectors, ((.1, .2), (.1, .2)))
        self.assertEqual(provider.calls, [payload])
        self.assertFalse(provider.closed)

    async def test_timeout_closes_the_forwarded_provider_stream(self):
        """Core timeout closes the source iterator without taking ownership of its Provider."""
        provider = DemoLLM()
        provider.block_stream = True
        async with aclosing(AutoAgentApp()) as app:
            app.register_operator(llm_call_operator(provider), capability_id=LLM_CALL.id)
            result = await app.ainvoke(Workflow("timeout", nodes=[Node("call", LLM_CALL,
                timeout=TimeoutPolicy(.02))]), request(streaming=True))
        self.assertEqual(result.error.type, "NodeTimeoutError")
        self.assertEqual(provider.streams_closed, 1)
        self.assertFalse(provider.closed)

    async def test_missing_terminal_response_remains_a_node_failure(self):
        """The unified contract cannot silently succeed with a truncated Provider stream."""
        provider = DemoLLM()
        provider.omit_terminal = True
        async with aclosing(AutoAgentApp()) as app:
            app.register_operator(llm_call_operator(provider), capability_id=LLM_CALL.id)
            result = await app.ainvoke(Workflow("truncated", nodes=[Node("call", LLM_CALL)]), request(streaming=True))
        self.assertEqual(result.status, "failed")
        self.assertIn("terminal response", result.error.message)


class HostAgentCapabilityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database_url = "sqlite+aiosqlite:///" + str(Path(self.temp.name) / "runtime.db")

    async def asyncTearDown(self):
        self.temp.cleanup()

    def host(self, **kwargs):
        from autoagent.host import AutoAgentHost, HostSettings
        return AutoAgentHost(settings=HostSettings(database_url=self.database_url), environment={}, **kwargs)

    async def test_default_contracts_are_registered_before_project_lifespan(self):
        """An ordinary Host pre-binds both Agent contracts without a Provider or API key."""
        @asynccontextmanager
        async def resources(setup):
            with self.assertRaisesRegex(ValueError, "contract"):
                # Wrong contract must be rejected even with no Agent Node in a Workflow.
                host._runtime.app.register_operator(Operator(identity, id="wrong-llm"), capability_id=LLM_CALL.id)
            with self.assertRaisesRegex(ValueError, "contract"):
                host._runtime.app.register_operator(Operator(identity, id="wrong-embedding"), capability_id=EMBEDDING.id)
            yield
        host = self.host(workflows=[Workflow("plain", nodes=[Node("work", identity)])], lifespan=resources)
        async with host:
            self.assertEqual(len(host._runtime.app.operator_registry.for_capability(LLM_CALL.id)), 1)
            result = await host.join(await host.execute("plain", {"value": 1}))
            self.assertEqual(result.status, "completed", result.error)

    async def test_host_calls_both_capabilities_without_a_resolver(self):
        """Lifespan-bound Providers run through the default Core Capability selection path."""
        llm, embedding = DemoLLM(), DemoEmbedding()
        @asynccontextmanager
        async def resources(setup):
            setup.register_operator(llm_call_operator(llm), capability_id=LLM_CALL.id, default=True)
            setup.register_operator(embedding_operator(embedding), capability_id=EMBEDDING.id, default=True)
            try:
                yield
            finally:
                await llm.aclose()
                await embedding.aclose()
        workflows = [Workflow("llm", nodes=[Node("call", LLM_CALL)]),
                     Workflow("embedding", nodes=[Node("call", EMBEDDING)])]
        async with self.host(workflows=workflows, lifespan=resources) as host:
            generated = await host.join(await host.execute("llm", request(streaming=True)))
            embedded = await host.join(await host.execute("embedding", EmbeddingRequest(model="test-embedding", input="hello")))
            self.assertEqual(generated.status, "completed", generated.error)
            self.assertEqual(embedded.status, "completed", embedded.error)
            self.assertEqual(len(embedded.output["data"]), 1)
            self.assertEqual(host._runtime.setup.capability_resolvers, {})
        self.assertTrue(llm.closed)
        self.assertTrue(embedding.closed)

    async def test_unbound_capability_fails_without_creating_a_default_provider(self):
        """Registering declarations does not invent credentials or silently choose a vendor."""
        workflow = Workflow("unbound", nodes=[Node("call", EMBEDDING)])
        async with self.host(workflows=[workflow]) as host:
            result = await host.join(await host.execute(workflow.id, EmbeddingRequest(model="model", input="hello")))
            self.assertEqual(result.status, "failed")
            self.assertIn("no enabled Operator", result.error.message)


@unittest.skipUnless(POSTGRES_TEST_URL, "isolated PostgreSQL URL not configured")
class PostgreSQLHostAgentCapabilityTests(HostAgentCapabilityTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.database = PostgreSQLDatabase()
        self.database_url = await self.database.create()

    async def asyncTearDown(self):
        try:
            await self.database.close()
        finally:
            await super().asyncTearDown()

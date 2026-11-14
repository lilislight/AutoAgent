"""Portable Agent history and exact-route replay across model/provider switches."""

from contextlib import asynccontextmanager
import math
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from autoagent.agent import (
    AgentResources,
    AgentSessionSettings, AgentModelSelection,
    AgentInput, AgentModelSettings, AgentHostSettings, AgentWorkflowOptions,
    AssetSource, AudioContent, ChatCompletionsAdapter, ContextPolicy, ContextPreparation, Conversation, DataSource, ImageContent,
    FileContent, HistoryOrigin, HistoryProjectionError, HistoryRoute, LLMMessage,
    LLMNativeData, LLMProviderItem, LLMReasoning, LLMRequest, LLMStreamChunk,
    LLMToolCall, LLMToolResult, LiteLLMChatCompletionsAdapter, ProviderSource,
    RefusalContent, ResponsesAdapter, TextContent, TokenAnchor, UsagePlusCharacters,
    OpenAIProviderConfig, OpenAIResponsesProvider,
    agent_runtime_context, agent_workflow, project_history, build_agent_request, prepare_agent_context,
)
from autoagent.agent.context import restore
from autoagent.agent.workflow.compaction import serialize_history
from autoagent.agent.workflow.policy import item_characters
from autoagent.host import AutoAgentHost, HostSettings
from autoagent.agent.runtime.context import AgentRuntime
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase


def route(*, provider="a", version="1", model="model-a", protocol="openai.chat_completions"):
    return HistoryRoute(provider=provider, version=version, model=model,
        provider_id="connection-" + provider, protocol=protocol)


def message(text, *, native=None):
    return LLMMessage(role="assistant", content=(TextContent(text=text),), native=native)


def native(**payload):
    return LLMNativeData(provider_id="connection-a", protocol="openai.chat_completions", payload=payload)


class HistoryProjectionTests(unittest.TestCase):
    def test_same_route_replays_native_without_mutating_original_history(self):
        """Positive model/connection provenance preserves the original replay group exactly."""
        output = (LLMReasoning(text="choose", native=native(_group_id="g", _reasoning_content=True)),
            LLMToolCall(call_id="c", name="lookup", arguments="{}", native=native(_group_id="g")))
        conversation = Conversation().append(*output, origin=route()).append(LLMToolResult(call_id="c"))
        before = conversation.model_dump_json()
        self.assertEqual(project_history(conversation, route())[:2], output)
        self.assertEqual(conversation.model_dump_json(), before)
        self.assertEqual(len(conversation.origins), 1)
        self.assertEqual((conversation.origins[0].start, conversation.origins[0].end), (0, 2))

    def test_provider_model_protocol_and_connection_version_changes_use_portable_items(self):
        """Every replay affinity component is checked, even if a connection ID remains the same."""
        body = message("answer", native=native(_group_id="g"))
        call = LLMToolCall(call_id="c", name="lookup", arguments='{"city":"北京"}', native=native(_group_id="g"))
        result = LLMToolResult(call_id="c", structured_content={"weather": "sunny"}, is_error=True)
        history = Conversation().append(body, call, origin=route()).append(result)
        for target in (route(provider="b"), route(model="model-b"), route(version="2"),
                route(protocol="openai.responses"), route().model_copy(update={"connection": "different-endpoint"}), None):
            with self.subTest(target=target):
                projected = project_history(history, target)
                self.assertTrue(all(item.native is None and item.id is None for item in projected))
                self.assertEqual(projected[0].content, body.content)
                self.assertEqual((projected[1].call_id, projected[1].arguments), (call.call_id, call.arguments))
                self.assertEqual(projected[2].structured_content, result.structured_content)
                self.assertTrue(projected[2].is_error)
        self.assertEqual(history.items[0], body)

    def test_reasoning_ciphertext_and_provider_activity_have_explicit_portable_representations(self):
        """Readable reasoning and hosted activity survive without replaying opaque reasoning ciphertext."""
        reasoning = LLMReasoning(id="r", text="plan", summary=("goal",), encrypted_content="private-cipher")
        activity = LLMProviderItem(id="search", provider_type="web_search_call", native=native(
            type="web_search_call", id="search", action={"query": "weather", "url": "https://example.org/result"}))
        history = Conversation(items=(reasoning, activity))
        projected = project_history(history, route(provider="b", protocol="openai.responses"))
        text = str(projected)
        self.assertIn("plan", text)
        self.assertIn("goal", text)
        self.assertIn("https://example.org/result", text)
        self.assertNotIn("private-cipher", text)
        self.assertIn("private-cipher", history.model_dump_json())
        self.assertNotIn("private-cipher", serialize_history(history.items))
        self.assertIn("web_search_call", serialize_history(history.items))

    def test_both_protocols_accept_portable_tool_history_and_refusal(self):
        """Nested native fields, provider item IDs and refusal wire shapes cannot lock a new route."""
        output = (LLMReasoning(text="plan", native=native(_reasoning_content=True)),
            LLMToolCall(id="fc-old", call_id="call", name="lookup", arguments="{}", native=native()),
            LLMToolResult(call_id="call", content=(TextContent(text="result", native=native()),)),
            LLMMessage(id="msg-old", role="assistant", content=(RefusalContent(text="cannot", native=native()),), native=native()))
        history = Conversation().append(*output, origin=route())
        for adapter in (ChatCompletionsAdapter(), ResponsesAdapter(), LiteLLMChatCompletionsAdapter()):
            target = route(provider="b", protocol=adapter.protocol)
            request = LLMRequest(model="other", input=project_history(history, target))
            wire = adapter.encode_request(request, provider_id=target.provider_id)
            self.assertIn("result", str(wire))
            self.assertIn("cannot", str(wire))
            self.assertNotIn("fc-old", str(wire))

    def test_unproven_history_is_portable_and_empty_reasoning_creates_no_message(self):
        """Matching native labels alone never prove model affinity or justify an empty Message."""
        history = Conversation(items=(LLMReasoning(), message("text", native=native(_group_id="g"))))
        projected = project_history(history, route())
        self.assertEqual(len(projected), 1)
        self.assertIsNone(projected[0].native)
        self.assertIsNotNone(history.items[-1].native)

    def test_origin_ranges_survive_summary_replacement_append_and_restore(self):
        """Summary views preserve provenance for later raw replay without reopening summarized ranges."""
        history = Conversation().append(message("a", native=native()), origin=route()).append(
            message("b", native=native()), origin=route(model="b"))
        summary = history.replace(0, 1, (message("summary"),)).append(message("c"))
        restored = restore(Conversation, summary)
        self.assertEqual(restored.origins, history.origins)
        self.assertEqual(project_history(restored, route(model="b")), (message("summary"), history.items[1], message("c")))
        with self.assertRaises(ValueError):
            Conversation(items=history.items, origins=(HistoryOrigin(start=0, end=3, route=route()),))
        with self.assertRaises(ValueError):
            Conversation(items=history.items, origins=(HistoryOrigin(start=0, end=2, route=route()),
                HistoryOrigin(start=1, end=2, route=route())))

    def test_estimator_measures_projected_history_and_anchor_suffix(self):
        """Cold and incremental estimates use the same portable representation as the request."""
        history = Conversation().append(message("old"), origin=route())
        history = history.model_copy(update={"token_anchor": TokenAnchor(
            fingerprint="fixed", input_tokens=20, source_count=1, view_generation=0)})
        history = history.append(LLMReasoning(text="new plan", encrypted_content="cipher"), origin=route())
        preparation = ContextPreparation(conversation=history, fingerprint="fixed", snapshot_id="stage",
            replay_target=route(provider="b"))
        suffix = project_history(history, preparation.replay_target, start=1)
        estimator = UsagePlusCharacters()
        self.assertEqual(estimator(preparation).input_tokens, 20 + math.ceil(sum(map(item_characters, suffix)) / 4))
        cold = preparation.model_copy(update={"conversation": history.model_copy(update={"token_anchor": None})})
        self.assertEqual(estimator(cold).input_tokens, math.ceil(sum(map(item_characters, cold.model_input)) / 4))

    def test_portable_assets_remain_typed_and_foreign_provider_file_ids_need_resolution(self):
        """Do not silently replace a historical binary file with its provider-scoped identifier."""
        item = LLMMessage(role="user", content=(FileContent(source=AssetSource(uri="asset:document")),))
        self.assertEqual(project_history(Conversation(items=(item,)), route(provider="b")), (item,))
        foreign = item.model_copy(update={"content": (FileContent(source=ProviderSource(
            provider_id="connection-a", file_id="file-a")),)})
        with self.assertRaisesRegex(HistoryProjectionError, "AssetSource"):
            project_history(Conversation(items=(foreign,)), route(provider="b"))

    def test_historical_assistant_audio_becomes_readable_without_replaying_an_audio_id(self):
        """Switching models retains the transcript/reference while omitting old binary continuation state."""
        audio = AudioContent(source=DataSource(data="YXVkaW8=", mime_type="audio/wav"),
            transcript="spoken answer", native=native(id="audio-old", expires_at=1))
        history = Conversation().append(LLMMessage(role="assistant", content=(audio,)), origin=route())
        projected = project_history(history, route(model="other"))
        wire = ChatCompletionsAdapter().encode_request(LLMRequest(model="other", input=projected), provider_id="connection-a")
        self.assertIn("spoken answer", str(wire))
        self.assertIn("historical_media_reference", str(wire))
        self.assertNotIn("audio-old", str(wire))
        self.assertNotIn("YXVkaW8=", str(wire))
        self.assertEqual(history.items[0].content[0], audio)

    def test_chat_tool_media_uses_references_only_when_the_target_adapter_requires_text(self):
        """Responses/extended Chat retain portable Tool media while standard Chat receives an explicit reference."""
        call = LLMToolCall(call_id="c", name="lookup", arguments="{}")
        image = ImageContent(source=AssetSource(uri="asset:image", mime_type="image/png"))
        result = LLMToolResult(call_id="c", content=(image,))
        history = Conversation(items=(call, result))
        standard = project_history(history, route())
        wire = ChatCompletionsAdapter().encode_request(LLMRequest(model="model-a", input=standard), provider_id="connection-a")
        self.assertIn("asset:image", str(wire))
        self.assertIn("Reference only", str(wire))
        for target in (route(protocol="openai.responses"), route().model_copy(update={"tool_media": True})):
            self.assertEqual(project_history(history, target)[1].content, (image,))

    def test_route_fingerprint_changes_with_endpoint_or_adapter_without_retaining_secrets(self):
        """Restarting a default registration at another endpoint invalidates its old replay affinity."""
        snapshot = SimpleNamespace(bindings={"provider": "1"}, settings=AgentHostSettings(
            model=AgentModelSettings(provider="a", model="m")))
        routes = []
        for endpoint in ("http://one.example/v1", "http://two.example/v1"):
            context = AgentRuntime(AutoAgentHost(environment={}, root=Path("/project")).runtime_services)
            provider = OpenAIResponsesProvider(OpenAIProviderConfig(provider_id="connection-a", base_url=endpoint,
                api_key="DO_NOT_STORE", headers={"Authorization": "SECRET_HEADER"}))
            context.services.providers.register("a", provider)
            routes.append(context.history_route(snapshot, "m"))
        self.assertNotEqual(routes[0], routes[1])
        self.assertNotIn("DO_NOT_STORE", routes[0].model_dump_json())
        self.assertNotIn("SECRET_HEADER", routes[0].model_dump_json())

    def test_citation_annotations_remain_visible_in_the_new_protocol_input(self):
        """Protocol output annotations become explicit historical text rather than disappearing on input encoding."""
        original = LLMMessage(role="assistant", content=(TextContent(text="answer", annotations=(
            {"type": "url_citation", "url": "https://example.org/source"},)),), native=native())
        history = Conversation().append(original, origin=route())
        for adapter in (ChatCompletionsAdapter(), ResponsesAdapter()):
            target = route(provider="b", protocol=adapter.protocol)
            wire = adapter.encode_request(LLMRequest(model="m", input=project_history(history, target)), provider_id=target.provider_id)
            self.assertIn("https://example.org/source", str(wire))
        self.assertEqual(history.items[0], original)


class ResumedRequestTests(unittest.IsolatedAsyncioTestCase):
    async def test_reused_completed_response_keeps_its_actual_route_after_resume(self):
        """Rebinding a completed Call cannot attribute its native state or Usage to the resumed model."""
        from autoagent.agent import LLMResponse, LLMUsage
        from autoagent.agent.workflow.provenance import stamp_response
        from autoagent.core import OutputBindingContext, RuntimeHandle
        from tests.test_agent_context_policy import Service
        class Routes(Service):
            def history_route(self, snapshot, model):
                return route(model=model)
            async def execution_snapshot(self, invocation_id):
                return self.state
        service = Routes(None)
        service.state = service.state.model_copy(update={"settings": service.state.settings.model_copy(update={
            "model": service.state.settings.model.model_copy(update={"model": "model-a"})})})
        original_stage = service.state.snapshot_id
        with agent_runtime_context(service):
            prepared = await build_agent_request(await prepare_agent_context(Conversation(), AgentWorkflowOptions(), ContextPolicy()), ContextPolicy())
            response = LLMResponse(model="model-a", finish_reason="stop", output=(message("old result"),),
                usage=LLMUsage(input_tokens=123), provider_metadata={"vendor": "retained"})
            completed = stamp_response(response, route(), original_stage)
            service.state = service.state.model_copy(update={"snapshot_id": "resumed-stage", "settings":
                service.state.settings.model_copy(update={"model": service.state.settings.model.model_copy(update={"model": "model-b"})})})
            llm = next(node for node in agent_workflow().nodes if node.id == "llm_call")
            for output, expected in ((completed, route().model_dump(mode="json")), (response, None)):
                context = OutputBindingContext(invocation_context={"agent": {"prepared": prepared.model_dump(mode="json"),
                    "request_snapshot_id": original_stage}}, session_context={}, output=output.model_dump(mode="json"),
                    self_handle=RuntimeHandle(session_id="session", invocation_id="invocation", workflow_id="agent", workflow_revision_id="revision"))
                patch = await llm.output_binding(context)
                values = {operation.path[-1]: operation.value for operation in patch.invocation}
                history = restore(Conversation, patch.session[0].value)
                self.assertEqual(history.origins[0].route.model_dump(mode="json") if history.origins else None, expected)
                self.assertIsNone(history.token_anchor)
                self.assertEqual(history.items, response.output)
                self.assertEqual(restore(LLMResponse, values["response"]), response)

    async def test_resume_after_build_projects_original_sources_for_the_new_model(self):
        """A clean resumed stage can restore native data omitted by the previously built model's request."""
        from autoagent.core import InputMappingContext, OutputBindingContext, RuntimeHandle
        from tests.test_agent_context_policy import Service
        class Routes(Service):
            def history_route(self, snapshot, model):
                return route(model=model)
            async def execution_snapshot(self, invocation_id):
                return self.state
        service = Routes(None)
        service.state = service.state.model_copy(update={"settings": service.state.settings.model_copy(update={
            "model": service.state.settings.model.model_copy(update={"model": "model-b"})})})
        reasoning = LLMReasoning(text="original plan", native=native(_reasoning_content=True, _group_id="g"))
        history = Conversation().append(reasoning, origin=route())
        options = AgentWorkflowOptions(instructions="fixed instructions")
        policy = ContextPolicy(context_window=8192)
        with agent_runtime_context(service):
            prepared = await build_agent_request(await prepare_agent_context(history, options, policy), policy)
            self.assertFalse(any(isinstance(item, LLMReasoning) for item in prepared.request.input))
            service.state = service.state.model_copy(update={"snapshot_id": "stage-2", "settings":
                service.state.settings.model_copy(update={"model": service.state.settings.model.model_copy(update={"model": "model-a"})})})
            workflow = agent_workflow(options=options, context_policy=policy)
            llm = next(node for node in workflow.nodes if node.id == "llm_call")
            build = next(node for node in workflow.nodes if node.id == "build_request")
            binding = await build.output_binding(OutputBindingContext(invocation_context={},
                session_context={"agent_conversation": history.model_dump(mode="json")},
                output=prepared.model_dump(mode="json")))
            stages = {operation.path[-1]: operation.value for operation in binding.invocation}
            self.assertEqual(stages["request_snapshot_id"], "stage-1")
            context = InputMappingContext(invocation_context={"agent": {"rounds": 0, "request_snapshot_id": "stage-1"}},
                session_context={"agent_conversation": history.model_dump(mode="json")}, invocation_input={},
                incoming={"build_llm": prepared.model_dump(mode="json")}, self_handle=RuntimeHandle(
                    session_id="session", invocation_id="invocation", workflow_id="agent", workflow_revision_id="revision"))
            request = await llm.input_mapping(context)
        self.assertEqual(request.model, "model-a")
        self.assertEqual(request.input[-1], reasoning)
        self.assertEqual(request.input[0].content[0].text, "fixed instructions")
        self.assertEqual(request.routing["agent_snapshot_id"], "stage-2")


class CodecProvider:
    """No network: real wire encoding/decoding exercises every history boundary."""
    def __init__(self, provider_id, adapter, *, chat=False):
        self.provider_id, self.adapter, self.chat = provider_id, adapter, chat
        self.requests, self.wires, self.responses = [], [], []
        self.streams = 0

    @property
    def protocol(self):
        return self.adapter.protocol

    async def ainvoke(self, request):
        self.requests.append(request)
        self.wires.append(self.adapter.encode_request(request, provider_id=self.provider_id))
        count = len(self.requests)
        if self.chat:
            body = {"role": "assistant", "content": "chat final"}
            finish = "stop"
            if count == 1:
                body = {"role": "assistant", "content": None, "reasoning_content": "ORIGINAL_REASONING",
                    "tool_calls": [{"id": "weather-call", "type": "function", "function": {
                        "name": "lookup", "arguments": '{"city":"北京"}'}}]}
                finish = "tool_calls"
            raw = {"id": "chat-" + str(count), "model": request.model,
                "choices": [{"message": body, "finish_reason": finish}],
                "usage": {"prompt_tokens": 50, "completion_tokens": 10, "total_tokens": 60}}
        else:
            output = [{"type": "message", "id": "msg-" + str(count), "role": "assistant",
                "content": [{"type": "output_text", "text": "responses final"}]}]
            if count == 1:
                output.insert(0, {"type": "reasoning", "id": "reason-r1", "summary": [{
                    "type": "summary_text", "text": "RESPONSE_REASONING"}], "encrypted_content": "cipher-r1"})
            raw = {"id": "responses-" + str(count), "status": "completed", "model": request.model,
                "output": output, "usage": {"input_tokens": 50, "output_tokens": 10, "total_tokens": 60}}
        response = self.adapter.decode_response(raw, provider_id=self.provider_id, request=request)
        self.responses.append(response)
        return response

    async def astream(self, request):
        self.streams += 1
        yield LLMStreamChunk(type="completed", response=await self.ainvoke(request))


class HistorySwitchContracts:
    async def database_url(self, root):
        return "sqlite+aiosqlite:///" + str(Path(root) / "history.db")

    async def test_switch_models_providers_protocols_and_restart_without_losing_replay(self):
        """A restarted Session can switch routes and later replay its original reasoning on the original model."""
        with tempfile.TemporaryDirectory() as root:
            url = await self.database_url(root)
            for streaming in (False, True):
                with self.subTest(streaming=streaming):
                    chat = CodecProvider("connection-a", LiteLLMChatCompletionsAdapter(), chat=True)
                    responses = CodecProvider("connection-b", ResponsesAdapter())
                    def lookup(city: str) -> str:
                        return "weather for " + city
                    @asynccontextmanager
                    async def lifespan(setup):
                        setup.providers.register("a", chat)
                        setup.providers.register("b", responses)
                        setup.extensions["agent"].context.set_defaults(AgentHostSettings(model=AgentModelSettings(
                            provider="a", model="model-a", streaming=streaming)))
                        yield
                    options = dict(workflows=[agent_workflow(resources=AgentResources(tools=[lookup]))], root=root, lifespan=lifespan,
                        environment={}, settings=HostSettings(database_url=url))
                    async with AutoAgentHost(**options) as host:
                        first = await host.join(await host.execute("agent", AgentInput(message="weather")))
                        self.assertEqual(first.status, "completed", first.error)
                        self.assertEqual(chat.wires[1]["messages"][-2]["reasoning_content"], "ORIGINAL_REASONING")
                    async with AutoAgentHost(**options) as host:
                        async def turn(provider, model):
                            await host.extensions["agent"].context.configure(first.session_id, AgentSessionSettings(
                                model=AgentModelSelection(provider=provider, model=model)))
                            result = await host.join(await host.execute("agent", AgentInput(message="continue"), session_id=first.session_id))
                            self.assertEqual(result.status, "completed", result.error)
                        await turn("a", "model-b")
                        self.assertFalse(any("reasoning_content" in item for item in chat.wires[2]["messages"]))
                        self.assertIn("ORIGINAL_REASONING", str(chat.wires[2]))
                        await turn("b", "model-b")
                        self.assertIn("weather-call", str(responses.wires[0]))
                        self.assertIn("weather for 北京", str(responses.wires[0]))
                        await turn("b", "model-c")
                        self.assertNotIn("cipher-r1", str(responses.wires[1]))
                        self.assertIn("RESPONSE_REASONING", str(responses.wires[1]))
                        await turn("a", "model-a")
                        self.assertTrue(any(item.get("reasoning_content") == "ORIGINAL_REASONING"
                            for item in chat.wires[-1]["messages"]))
                        self.assertNotIn("cipher-r1", str(chat.wires[-1]))
                    self.assertEqual(chat.responses[0].output[0].text, "ORIGINAL_REASONING")
                    self.assertEqual(responses.responses[0].output[0].encrypted_content, "cipher-r1")
                    self.assertEqual(bool(chat.streams), streaming)


class SQLiteHistorySwitchTests(HistorySwitchContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLHistorySwitchTests(HistorySwitchContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self, root):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

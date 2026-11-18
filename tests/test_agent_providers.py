"""Real SDK request/response tests over an in-process HTTP transport."""

from __future__ import annotations

import asyncio
import base64
from contextlib import asynccontextmanager
import json
import unittest
from unittest.mock import patch

from pydantic import ValidationError

try:
    import httpx
    from openai import AsyncOpenAI
except ImportError:
    httpx = None
    AsyncOpenAI = None

from autoagent import Node, Stream, ValueContract, Workflow
from autoagent.core.app import AutoAgentApp
from autoagent.agent import (
    AssetSource, AudioContent, ChatCompletionsAdapter, DataSource, FileContent,
    ImageContent, LLMAudioOutput, LLMJsonSchemaFormat, LLMMessage, LLMModelProfile,
    LLMNamedToolChoice, LLMNativeData, LLMOutputOptions, LLMProviderError,
    LLMProviderItem, LLMProviderOptions, LLMReasoning, LLMRequest, LLMResponse,
    LLMStreamChunk, LLMStreamReducer, LLMStreamState, LLMToolCall,
    LLMToolDefinition, LLMToolResult, LLMUnsupportedRequest,
    OpenAIChatCompletionsProvider, OpenAIProviderConfig, OpenAIResponsesProvider,
    ProviderSource, RefusalContent, ResponsesAdapter, TextContent, URLSource, VideoContent,
)


def request(*parts, **options) -> LLMRequest:
    options.setdefault("model", "model")
    return LLMRequest(
        input=(LLMMessage(role="user", content=parts or (TextContent(text="Hello"),)),),
        **options,
    )


def output_message(text="Answer", **extra):
    return {
        "id": "msg_1", "type": "message", "role": "assistant", "status": "completed",
        "content": [{"type": "output_text", "text": text, "annotations": []}], **extra,
    }


def response_body(output=None, **extra):
    return {
        "id": "resp_1", "object": "response", "status": "completed", "model": "model",
        "output": output if output is not None else [output_message()],
        "usage": {"input_tokens": 3, "output_tokens": 2, "total_tokens": 5}, **extra,
    }


def chat_body(message=None, **extra):
    return {
        "id": "chat_1", "object": "chat.completion", "created": 1, "model": "model",
        "choices": [{"index": 0, "message": message or {"role": "assistant", "content": "Answer"}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5}, **extra,
    }


def chat_chunk(delta, finish=None):
    return {
        "id": "chat_1", "object": "chat.completion.chunk", "created": 1, "model": "model",
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
    }


def sse(events, *, done=False):
    data = "".join("data: " + json.dumps(event) + "\n\n" for event in events)
    return (data + ("data: [DONE]\n\n" if done else "")).encode()


class TrackingStream(httpx.AsyncByteStream if httpx is not None else object):
    def __init__(self, first, *, pause=False):
        self.first = first
        self.gate = asyncio.Event() if pause else None
        self.closed = False

    async def __aiter__(self):
        yield self.first
        if self.gate is not None:
            await self.gate.wait()

    async def aclose(self):
        self.closed = True


class StreamModelTests(unittest.TestCase):
    def test_chunks_have_portable_core_contracts(self):
        """Streaming observations and reducer state survive Core serialization."""
        values = (
            LLMStreamChunk(type="delta", item_key="item:0", kind="text", delta="a"),
            LLMStreamChunk(type="completed", response=LLMResponse(model="model", output=(), finish_reason="stop")),
            LLMStreamState(),
        )
        for value in values:
            with self.subTest(model=type(value).__name__):
                contract = ValueContract.create(type(value), location="stream")
                self.assertEqual(contract.restore(contract.to_record(value)), value)

    def test_chunk_and_reducer_reject_missing_or_duplicate_terminals(self):
        """Malformed observations cannot produce a successful reduced response."""
        with self.assertRaises(ValidationError):
            LLMStreamChunk(type="completed")
        with self.assertRaises(ValidationError):
            LLMStreamChunk(type="delta", item_key="item:0", kind="text", delta="a", response=LLMResponse(model="model", output=(), finish_reason="stop"))
        reducer = LLMStreamReducer()
        state = reducer.initial(None)
        with self.assertRaises(ValueError):
            reducer.finish(None, state)
        terminal = LLMStreamChunk(type="completed", response=LLMResponse(model="model", output=(), finish_reason="stop"))
        state = reducer.add(None, state, terminal)
        with self.assertRaises(ValueError):
            reducer.add(None, state, terminal)


@unittest.skipUnless(AsyncOpenAI is not None, "Install autoagent[agent] for SDK transport tests")
class ProviderSDKTests(unittest.IsolatedAsyncioTestCase):
    async def test_reasoning_effort_uses_the_selected_protocol_field(self):
        """A normalized effort reaches each real SDK request in its protocol's expected location."""
        for protocol in ("chat", "responses"):
            with self.subTest(protocol=protocol):
                provider, _, captured = self.provider(protocol)
                await provider.ainvoke(request(reasoning_effort="high"))
                body = captured[0][1]
                if protocol == "chat":
                    self.assertEqual(body["reasoning_effort"], "high")
                    self.assertNotIn("reasoning", body)
                else:
                    self.assertEqual(body["reasoning"], {"effort": "high"})
                    self.assertNotIn("reasoning_effort", body)

    def provider(self, protocol="responses", *, reply=None, handler=None, **kwargs):
        captured = []
        async def handle(req):
            captured.append((str(req.url), json.loads(req.content), req.headers))
            if handler is not None:
                return await handler(req)
            return httpx.Response(200, json=reply or (response_body() if protocol == "responses" else chat_body()), headers={"x-request-id": "request_1"})
        client = AsyncOpenAI(api_key="test", base_url="https://provider.test/v1", max_retries=0, http_client=httpx.AsyncClient(transport=httpx.MockTransport(handle)))
        # OS discovery is unrelated to HTTP/SSE semantics and creates a default
        # executor whose shutdown notification can stall in this sandbox.
        client._platform = "Linux"
        cls = OpenAIResponsesProvider if protocol == "responses" else OpenAIChatCompletionsProvider
        provider = cls(OpenAIProviderConfig(provider_id="test"), client=client, **kwargs)
        self.addAsyncCleanup(client.close)
        self.addAsyncCleanup(provider.aclose)
        return provider, client, captured

    async def test_responses_sdk_encodes_multimodal_input_tools_and_output_schema(self):
        """Inspect the real SDK's HTTP body for Responses field placement."""
        provider, _, captured = self.provider(reply=response_body(output=[output_message("{}")]))
        tool = LLMToolDefinition(name="lookup", parameters={"type": "object", "properties": {}}, strict=True)
        value = request(
            TextContent(text="Compare"),
            ImageContent(source=DataSource(data="YQ==", mime_type="image/png"), detail="high"),
            FileContent(source=ProviderSource(provider_id="test", file_id="file_1")),
            tools=(tool,), tool_choice=LLMNamedToolChoice(name="lookup"), max_output_tokens=10,
            output_options=LLMOutputOptions(text_format=LLMJsonSchemaFormat(name="Answer", json_schema={"type": "object", "properties": {}, "additionalProperties": False})),
            provider_options=LLMProviderOptions(protocol="openai.responses", values={"reasoning": {"effort": "low"}, "store": False}),
        )
        result = await provider.ainvoke(value)
        url, body, _ = captured[0]
        self.assertTrue(url.endswith("/responses"))
        self.assertEqual(body["input"][0]["content"][1]["image_url"], "data:image/png;base64,YQ==")
        self.assertEqual(body["input"][0]["content"][2]["file_id"], "file_1")
        self.assertEqual(body["tools"][0]["type"], "function")
        self.assertEqual(body["tool_choice"], {"type": "function", "name": "lookup"})
        self.assertEqual(body["text"]["format"]["schema"]["type"], "object")
        self.assertEqual(body["max_output_tokens"], 10)
        self.assertEqual(body["reasoning"]["effort"], "low")
        self.assertEqual(result.text, "{}")
        self.assertEqual(result.provider_metadata["_request_id"], "request_1")

    async def test_call_settings_do_not_leak_routing_or_unresolved_models_to_http(self):
        """Adapters encode only wire fields and reject a pending model before any HTTP request."""
        for protocol in ("chat", "responses"):
            with self.subTest(protocol=protocol):
                provider, _, captured = self.provider(protocol)
                for method in ("ainvoke", "astream"):
                    with self.assertRaisesRegex(LLMUnsupportedRequest, "Resolve a model"):
                        if method == "ainvoke":
                            await provider.ainvoke(request(model=None))
                        else:
                            _ = [chunk async for chunk in provider.astream(request(model=None))]
                self.assertEqual(captured, [])
                await provider.ainvoke(request(streaming=True, routing={"task": "local-only"}))
                body = captured[0][1]
                self.assertNotIn("routing", body)
                self.assertNotIn("streaming", body)
                self.assertFalse(body.get("stream", False))

    async def test_selected_provider_rejects_incompatible_history_before_http(self):
        """Selecting another Provider never silently strips protocol-specific options or native history."""
        from contextlib import aclosing
        from autoagent.agent import LLM_CALL, LLMSelection, llm_call_operator
        provider, _, captured = self.provider()
        @asynccontextmanager
        async def resources():
            async with provider:
                yield
        values = (
            request(model=None, provider_options=LLMProviderOptions(
                provider_id="other", protocol="openai.responses", values={})),
            request(model=None, provider_options=LLMProviderOptions(protocol="openai.chat_completions")),
            request(TextContent(text="Hello", native=LLMNativeData(
                provider_id="other", protocol="openai.responses", payload={"signature": "opaque"})), model=None),
        )
        async with aclosing(AutoAgentApp(runtime_lifespan=resources)) as app:
            app.register_operator(llm_call_operator(resolver=lambda value: LLMSelection(provider, "model")),
                                  capability_id=LLM_CALL.id)
            await app.aprepare()
            for value in values:
                result = await app.ainvoke(Workflow("mismatch", nodes=[Node("call", LLM_CALL)]), value)
                self.assertEqual(result.status, "failed")
                self.assertIn("another", result.error.message)
        self.assertEqual(captured, [])

    async def test_chat_sdk_encodes_audio_pdf_and_function_envelopes(self):
        """Chat Completions uses its own media and function parameter envelopes."""
        provider, _, captured = self.provider("chat")
        value = request(
            AudioContent(source=DataSource(data="YQ==", mime_type="audio/wav")),
            FileContent(source=DataSource(data="JVBERg==", mime_type="application/pdf"), filename="report.pdf"),
            tools=(LLMToolDefinition(name="lookup", parameters={"type": "object"}),),
            max_output_tokens=20, tool_choice=LLMNamedToolChoice(name="lookup"),
        )
        await provider.ainvoke(value)
        url, body, _ = captured[0]
        self.assertTrue(url.endswith("/chat/completions"))
        self.assertEqual(body["messages"][0]["content"][0]["input_audio"], {"data": "YQ==", "format": "wav"})
        self.assertEqual(body["messages"][0]["content"][1]["file"]["filename"], "report.pdf")
        self.assertEqual(body["tools"][0]["function"]["name"], "lookup")
        self.assertEqual(body["tool_choice"]["function"]["name"], "lookup")
        self.assertEqual(body["max_completion_tokens"], 20)

    async def test_response_items_replay_with_encrypted_reasoning_and_call_identity(self):
        """A second real SDK request retains encrypted reasoning and Tool correlation."""
        reasoning = {"id": "rs_1", "type": "reasoning", "summary": [{"type": "summary_text", "text": "Looking up"}], "encrypted_content": "opaque"}
        call = {"id": "fc_1", "type": "function_call", "call_id": "call_1", "name": "lookup", "arguments": "{"}
        provider, _, captured = self.provider(reply=response_body([reasoning, call]))
        result = await provider.ainvoke(request())
        self.assertEqual(result.tool_calls[0].arguments, "{")
        followup = LLMRequest(model="model", input=(*request().input, *result.output, LLMToolResult(call_id="call_1", structured_content={"found": True})))
        await provider.ainvoke(followup)
        body = captured[-1][1]
        self.assertEqual(body["input"][1]["encrypted_content"], "opaque")
        self.assertEqual(body["input"][2]["id"], "fc_1")
        self.assertEqual(body["input"][2]["call_id"], "call_1")
        self.assertEqual(body["input"][3]["call_id"], "call_1")
        modified = result.output[0].model_copy(update={"summary": ("Changed",)})
        with self.assertRaises(LLMUnsupportedRequest):
            await provider.ainvoke(LLMRequest(model="model", input=(modified,)))

    async def test_chat_replay_groups_calls_without_merging_other_assistant_turns(self):
        """Reconstruct Chat assistant Tool calls using their original response group."""
        message = {"role": "assistant", "content": "Checking", "tool_calls": [{"id": "call_1", "type": "function", "function": {"name": "lookup", "arguments": "{}"}}]}
        provider, _, captured = self.provider("chat", reply=chat_body(message))
        result = await provider.ainvoke(request())
        followup = LLMRequest(model="model", input=(*request().input, *result.output, LLMToolResult(call_id="call_1", content=(TextContent(text="Found"),))))
        await provider.ainvoke(followup)
        messages = captured[-1][1]["messages"]
        self.assertEqual(len(messages), 3)
        self.assertEqual(messages[1]["tool_calls"][0]["id"], "call_1")
        self.assertEqual(messages[2]["role"], "tool")

    async def test_asset_resolution_is_per_call_and_does_not_mutate_request(self):
        """Resolve repeated asset references once and keep the durable request intact."""
        resolved = []
        async def resolver(source):
            resolved.append(source.uri)
            return DataSource(data="YQ==", mime_type="image/png")
        provider, _, captured = self.provider(media_resolver=resolver)
        part = ImageContent(source=AssetSource(uri="asset://image"))
        value = request(part, part)
        await provider.ainvoke(value)
        self.assertEqual(resolved, ["asset://image"])
        self.assertIsInstance(value.input[0].content[0].source, AssetSource)
        self.assertEqual(captured[0][1]["input"][0]["content"][0]["image_url"], "data:image/png;base64,YQ==")

    async def test_mutated_media_resolver_sources_fail_before_http(self):
        """Borrowed resolver models are validated before replacing a media source."""
        async def resolver(source):
            return DataSource(data="YQ==", mime_type="image/png").model_copy(
                update={"data": "invalid base64!"})

        provider, _, captured = self.provider(media_resolver=resolver)
        value = request(ImageContent(source=AssetSource(uri="asset://image")))
        with self.assertRaises(LLMProviderError):
            await provider.ainvoke(value)
        with self.assertRaises(LLMProviderError):
            async for _ in provider.astream(value):
                pass
        self.assertEqual(captured, [])

    async def test_capability_and_protocol_errors_are_detected_before_http(self):
        """Reject unsupported media, native provenance and profile options locally."""
        provider, _, captured = self.provider(model_profiles={"model": LLMModelProfile(sampling=False)})
        for value in (
            request(temperature=0.5),
            request(VideoContent(source=URLSource(url="https://example.com/v.mp4"))),
            request(AudioContent(source=DataSource(data="YQ==", mime_type="audio/wav"))),
            request(ImageContent(source=ProviderSource(provider_id="other", file_id="file_1"))),
            request(TextContent(text="a", native=LLMNativeData(provider_id="other", protocol="openai.responses"))),
            request(provider_options=LLMProviderOptions(protocol="openai.chat_completions", values={})),
            request(provider_options=LLMProviderOptions(protocol="openai.responses", values={"background": True})),
        ):
            with self.subTest(value=value.input), self.assertRaises(LLMUnsupportedRequest):
                await provider.ainvoke(value)
        self.assertEqual(captured, [])

    async def test_chat_rejects_unrepresentable_file_and_tool_content(self):
        """Do not silently extract documents or flatten multimodal Tool results."""
        provider, _, captured = self.provider("chat")
        for value in (
            request(FileContent(source=URLSource(url="https://example.com/a.pdf"))),
            request(FileContent(source=DataSource(data="YQ==", mime_type="text/plain"))),
            request(ImageContent(source=URLSource(url="https://example.com/a.png"), detail="original")),
            LLMRequest(model="model", input=(LLMToolResult(call_id="call", content=(ImageContent(source=URLSource(url="https://example.com/a.png")),)),)),
            LLMRequest(model="model", input=(LLMReasoning(id="rs", encrypted_content="opaque"),)),
        ):
            with self.subTest(value=value.input), self.assertRaises(LLMUnsupportedRequest):
                await provider.ainvoke(value)
        self.assertEqual(captured, [])

    async def test_provider_parameter_variant_reuses_the_sdk_transport(self):
        """A compatible service can choose max_tokens and omit stream usage options."""
        provider, _, captured = self.provider("chat", adapter=ChatCompletionsAdapter(token_parameter="max_tokens", include_stream_usage=False))
        await provider.ainvoke(request(max_output_tokens=10))
        body = captured[0][1]
        self.assertEqual(body["max_tokens"], 10)
        self.assertNotIn("max_completion_tokens", body)
        self.assertNotIn("stream_options", provider.adapter.stream_parameters(body))

    async def test_custom_adapter_can_adjust_a_vendor_field_without_transport_changes(self):
        """Replacing one encoding hook preserves the shared SDK lifecycle and response."""
        class VendorAdapter(ResponsesAdapter):
            def encode_request(self, value, *, provider_id):
                params = super().encode_request(value, provider_id=provider_id)
                params.setdefault("extra_body", {})["vendor_option"] = "enabled"
                return params
        provider, _, captured = self.provider(adapter=VendorAdapter())
        self.assertEqual((await provider.ainvoke(request())).text, "Answer")
        self.assertEqual(captured[0][1]["vendor_option"], "enabled")

    async def test_sdk_errors_preserve_status_request_id_and_retryability(self):
        """Normalize rate limits without issuing hidden retries through the SDK."""
        async def error_handler(_req):
            return httpx.Response(429, json={"error": {"message": "Rate limited", "type": "rate_limit_error"}}, headers={"x-request-id": "request_error"})
        provider, _, captured = self.provider(handler=error_handler)
        with self.assertRaises(LLMProviderError) as raised:
            await provider.ainvoke(request())
        self.assertEqual(raised.exception.status_code, 429)
        self.assertEqual(raised.exception.request_id, "request_error")
        self.assertTrue(raised.exception.retryable)
        self.assertEqual(len(captured), 1)

    async def test_invalid_response_is_a_nonretryable_protocol_error(self):
        """Reject pending results and malformed single-candidate responses."""
        for protocol, body in (("responses", response_body(status="in_progress")), ("chat", chat_body(choices=[]))):
            provider, _, _ = self.provider(protocol, reply=body)
            with self.subTest(protocol=protocol), self.assertRaises(LLMProviderError) as raised:
                await provider.ainvoke(request())
            self.assertFalse(raised.exception.retryable)

    async def test_responses_sse_preserves_deltas_and_authoritative_final_output(self):
        """Decode real SDK SSE events and retain usage and final item identities."""
        events = [
            {"type": "response.output_text.delta", "output_index": 0, "content_index": 0, "delta": "An"},
            {"type": "response.output_text.delta", "output_index": 0, "content_index": 0, "delta": "swer"},
            {"type": "response.output_item.done", "output_index": 0, "item": output_message()},
            {"type": "response.completed", "response": response_body()},
        ]
        async def stream_handler(_req):
            return httpx.Response(200, content=sse(events), headers={"content-type": "text/event-stream", "x-request-id": "stream_request_1"})
        provider, _, captured = self.provider(handler=stream_handler)
        chunks = [chunk async for chunk in provider.astream(request())]
        self.assertEqual([chunk.type for chunk in chunks], ["delta", "delta", "item", "completed"])
        self.assertEqual("".join(chunk.delta for chunk in chunks if chunk.type == "delta"), "Answer")
        self.assertEqual(chunks[-1].response.text, "Answer")
        self.assertEqual(chunks[-1].response.usage.total_tokens, 5)
        self.assertEqual(chunks[-1].response.provider_metadata["_request_id"], "stream_request_1")
        self.assertTrue(captured[0][1]["stream"])

    async def test_chat_sse_keeps_tool_argument_fragments_and_late_usage(self):
        """Wait for usage-only chunks and preserve Tool arguments as raw strings."""
        events = [
            chat_chunk({"content": "Checking"}),
            chat_chunk({"tool_calls": [{"index": 0, "id": "call_1", "type": "function", "function": {"name": "lookup", "arguments": "{"}}]}),
            chat_chunk({"tool_calls": [{"index": 0, "function": {"arguments": "}"}}]}, "tool_calls"),
            {"id": "chat_1", "model": "model", "object": "chat.completion.chunk", "created": 1, "choices": [], "usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5}},
        ]
        async def stream_handler(_req):
            return httpx.Response(200, content=sse(events, done=True), headers={"content-type": "text/event-stream"})
        provider, _, captured = self.provider("chat", handler=stream_handler)
        chunks = [chunk async for chunk in provider.astream(request())]
        self.assertEqual(chunks[-1].response.tool_calls[0].arguments, "{}")
        self.assertEqual(chunks[-1].response.usage.total_tokens, 5)
        self.assertEqual([chunk.item_key for chunk in chunks if chunk.kind == "tool_arguments"], ["tool:0", "tool:0"])
        self.assertEqual(captured[0][1]["stream_options"], {"include_usage": True})

    async def test_truncated_streams_do_not_fabricate_completed_responses(self):
        """EOF without a protocol terminal marker is a failed call in both protocols."""
        for protocol, events in (("responses", [{"type": "response.output_text.delta", "output_index": 0, "delta": "Partial"}]), ("chat", [chat_chunk({"content": "Partial"})])):
            async def stream_handler(_req, events=events):
                return httpx.Response(200, content=sse(events), headers={"content-type": "text/event-stream"})
            provider, _, _ = self.provider(protocol, handler=stream_handler)
            chunks = []
            with self.subTest(protocol=protocol), self.assertRaises(LLMProviderError):
                async for chunk in provider.astream(request()):
                    chunks.append(chunk)
            self.assertNotIn("completed", [chunk.type for chunk in chunks])

    async def test_audio_response_retains_requested_format_and_continuation(self):
        """Decode SDK audio with the requested MIME type and replay its live ID."""
        message = {"role": "assistant", "content": None, "audio": {"id": "audio_1", "data": "YQ==", "expires_at": 4102444800, "transcript": "Hello"}}
        provider, _, captured = self.provider("chat", reply=chat_body(message))
        options = LLMOutputOptions(modalities=("text", "audio"), audio=LLMAudioOutput(voice="alloy", format="mp3"))
        result = await provider.ainvoke(request(output_options=options))
        self.assertEqual(result.media[0].source.mime_type, "audio/mpeg")
        await provider.ainvoke(LLMRequest(model="model", input=result.output, output_options=options))
        self.assertEqual(captured[-1][1]["messages"][0]["audio"], {"id": "audio_1"})

    async def test_streamed_audio_chunks_are_decoded_before_concatenation(self):
        """Combine independently padded base64 chunks as bytes, not encoded strings."""
        events = [
            chat_chunk({"audio": {"id": "audio_1", "data": "YQ==", "transcript": "A"}}),
            chat_chunk({"audio": {"data": "Yg==", "expires_at": 4102444800, "transcript": "B"}}, "stop"),
        ]
        async def stream_handler(_req):
            return httpx.Response(200, content=sse(events, done=True), headers={"content-type": "text/event-stream"})
        provider, _, _ = self.provider("chat", handler=stream_handler)
        options = LLMOutputOptions(modalities=("audio",), audio=LLMAudioOutput(voice="alloy", format="pcm16"))
        chunks = [chunk async for chunk in provider.astream(request(output_options=options))]
        audio = chunks[-1].response.media[0]
        self.assertEqual(base64.b64decode(audio.source.data), b"ab")
        self.assertEqual(audio.transcript, "AB")
        self.assertTrue(all(chunk.content_index is None for chunk in chunks))

    async def test_injected_client_is_not_closed_and_closed_provider_rejects_calls(self):
        """Provider shutdown leaves a caller-owned SDK client available."""
        provider, client, captured = self.provider()
        await provider.ainvoke(request())
        await provider.aclose()
        self.assertFalse(client.is_closed())
        with self.assertRaises(RuntimeError):
            await provider.ainvoke(request())
        self.assertEqual(len(captured), 1)

    async def test_owned_client_is_lazy_and_has_retries_disabled(self):
        """Create and close an owned SDK client only inside its asynchronous lifespan."""
        transport = httpx.MockTransport(lambda _req: httpx.Response(200, json=response_body()))
        actual = AsyncOpenAI(api_key="test", http_client=httpx.AsyncClient(transport=transport))
        actual._platform = "Linux"
        provider = OpenAIResponsesProvider(OpenAIProviderConfig(api_key="test"))
        with patch("openai.AsyncOpenAI", return_value=actual) as factory:
            factory.assert_not_called()
            async with provider:
                self.assertEqual((await provider.ainvoke(request())).text, "Answer")
            self.assertEqual(factory.call_args.kwargs["max_retries"], 0)
        self.assertTrue(actual.is_closed())

    async def test_provider_and_stream_reducer_execute_inside_core_lifespan(self):
        """Verify SDK call, stream reduction and shutdown on Core's owned event loop."""
        events = [{"type": "response.completed", "response": response_body()}]
        async def handler(_req):
            return httpx.Response(200, content=sse(events), headers={"content-type": "text/event-stream"})
        client = AsyncOpenAI(api_key="test", max_retries=0, http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
        client._platform = "Linux"
        provider = OpenAIResponsesProvider(OpenAIProviderConfig(provider_id="test"), client=client)
        @asynccontextmanager
        async def lifespan():
            async with provider:
                try:
                    yield
                finally:
                    await client.close()
        app = AutoAgentApp(runtime_lifespan=lifespan)
        try:
            await app.aprepare()
            result = await app.ainvoke(Workflow("provider-stream", nodes=[Node("llm", provider.astream, stream=Stream(LLMStreamReducer()))]), request())
            self.assertEqual(result.status, "completed")
            restored = ValueContract.create(LLMResponse, location="result").restore(result.output)
            self.assertEqual(restored.text, "Answer")
            self.assertIsNot(provider._loop, asyncio.get_running_loop())
        finally:
            await app.aclose()
        self.assertTrue(client.is_closed())

    async def test_early_stream_close_releases_http_response_and_admission(self):
        """A consumer closing after one delta releases the SDK's HTTP stream."""
        body = TrackingStream(sse([{"type": "response.output_text.delta", "output_index": 0, "delta": "A"}]), pause=True)
        async def handler(_req):
            return httpx.Response(200, stream=body, headers={"content-type": "text/event-stream"})
        provider, _, _ = self.provider(handler=handler)
        stream = provider.astream(request())
        self.assertEqual((await stream.__anext__()).delta, "A")
        with self.assertRaises(RuntimeError):
            await provider.aclose()
        await stream.aclose()
        self.assertTrue(body.closed)
        self.assertEqual(provider._active, 0)

    async def test_stream_cancellation_closes_transport_and_remains_cancellation(self):
        """Cancellation is not converted into a retryable provider failure."""
        body = TrackingStream(sse([{"type": "response.output_text.delta", "output_index": 0, "delta": "A"}]), pause=True)
        async def handler(_req):
            return httpx.Response(200, stream=body, headers={"content-type": "text/event-stream"})
        provider, _, _ = self.provider(handler=handler)
        stream = provider.astream(request())
        await stream.__anext__()
        pending = asyncio.create_task(stream.__anext__())
        await asyncio.sleep(0)
        pending.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await pending
        self.assertTrue(body.closed)
        self.assertEqual(provider._active, 0)

    async def test_profiles_are_selected_by_model_on_the_same_connection(self):
        """One provider can apply different declared capabilities to different models."""
        provider, _, captured = self.provider(model_profiles={
            "restricted": LLMModelProfile(sampling=False, tools=False),
        })
        with self.assertRaises(LLMUnsupportedRequest):
            await provider.ainvoke(request(model="restricted", temperature=0.5))
        await provider.ainvoke(request(model="ordinary", temperature=0.5))
        self.assertEqual(captured[0][1]["model"], "ordinary")
        self.assertEqual(len(captured), 1)

    async def test_filtered_chat_response_preserves_empty_output(self):
        """A valid filtering terminal can carry no assistant text or Tool calls."""
        body = chat_body({"role": "assistant", "content": None})
        body["choices"][0]["finish_reason"] = "content_filter"
        provider, _, _ = self.provider("chat", reply=body)
        result = await provider.ainvoke(request())
        self.assertEqual(result.finish_reason, "content_filter")
        self.assertEqual(result.output, ())

    async def test_incomplete_and_failed_responses_have_distinct_outcomes(self):
        """Return a truncated result but raise remote execution failures."""
        provider, _, _ = self.provider(reply=response_body(status="incomplete", incomplete_details={"reason": "max_output_tokens"}))
        self.assertEqual((await provider.ainvoke(request())).finish_reason, "length")
        failed, _, _ = self.provider(reply=response_body(status="failed", error={"code": "server_error", "message": "Remote execution failed"}))
        with self.assertRaisesRegex(LLMProviderError, "Remote execution failed"):
            await failed.ainvoke(request())

    async def test_chat_usage_and_native_message_extensions_are_retained(self):
        """Preserve cached/reasoning counts and provider fields even for calls-only output."""
        body = chat_body({"role": "assistant", "content": None, "reasoning_content": "vendor reasoning", "tool_calls": [{"id": "call_1", "type": "function", "function": {"name": "lookup", "arguments": "{}"}}]})
        body["usage"].update(prompt_tokens_details={"cached_tokens": 2}, completion_tokens_details={"reasoning_tokens": 1})
        provider, _, _ = self.provider("chat", reply=body)
        result = await provider.ainvoke(request())
        self.assertEqual(result.usage.cache_read_tokens, 2)
        self.assertEqual(result.usage.reasoning_tokens, 1)
        self.assertEqual(result.tool_calls[0].native.payload["_message_fields"]["reasoning_content"], "vendor reasoning")

    async def test_sdk_connection_error_has_retryable_classification(self):
        """Normalize transport timeouts without hiding an extra request attempt."""
        async def handler(req):
            raise httpx.ReadTimeout("Timed out", request=req)
        provider, _, captured = self.provider(handler=handler)
        with self.assertRaises(LLMProviderError) as raised:
            await provider.ainvoke(request())
        self.assertTrue(raised.exception.retryable)
        self.assertIsNone(raised.exception.status_code)
        self.assertEqual(len(captured), 1)

    async def test_responses_message_and_hosted_item_replay_preserve_wire_identity(self):
        """Replay output message IDs, citations and hosted activity through the real SDK."""
        message = output_message()
        annotation = {
            "type": "url_citation", "start_index": 0, "end_index": 6,
            "url": "https://example.com/source", "title": "Source",
        }
        message["content"][0]["annotations"] = [annotation]
        activity = {
            "id": "ws_1", "type": "web_search_call", "status": "completed",
            "action": {"type": "search", "query": "evidence"},
        }
        provider, _, captured = self.provider(reply=response_body([message, activity]))
        result = await provider.ainvoke(request())
        self.assertIsInstance(result.output[1], LLMProviderItem)
        self.assertEqual(result.tool_calls, ())
        await provider.ainvoke(LLMRequest(model="model", input=result.output))
        wire = captured[-1][1]["input"]
        self.assertEqual(wire[0]["id"], "msg_1")
        self.assertEqual(wire[0]["status"], "completed")
        self.assertEqual(wire[0]["content"][0]["annotations"][0], annotation)
        self.assertEqual(wire[1]["id"], "ws_1")
        self.assertEqual(wire[1]["type"], "web_search_call")

    async def test_sdk_strict_schema_compilation_is_recursive_and_does_not_mutate_models(self):
        """Apply the SDK's strict object requirements to nested output and Tool schemas."""
        schema = {
            "type": "object", "properties": {
                "nested": {"type": "object", "properties": {
                    "value": {"anyOf": [{"type": "string"}, {"type": "null"}], "default": None},
                }},
            },
        }
        output = LLMJsonSchemaFormat(name="Answer", json_schema=schema)
        tool = LLMToolDefinition(name="lookup", parameters=schema, strict=True)
        for protocol in ("responses", "chat"):
            reply = response_body(output=[output_message('{"nested":{"value":null}}')]) if protocol == "responses" else chat_body({"role":"assistant", "content":'{"nested":{"value":null}}'})
            provider, _, captured = self.provider(protocol, reply=reply)
            value = request(tools=(tool,), output_options=LLMOutputOptions(text_format=output))
            before = value.model_dump_json()
            await provider.ainvoke(value)
            body = captured[0][1]
            actual = body["text"]["format"]["schema"] if protocol == "responses" else body["response_format"]["json_schema"]["schema"]
            tool_schema = body["tools"][0]["parameters"] if protocol == "responses" else body["tools"][0]["function"]["parameters"]
            self.assertEqual(actual, tool_schema)
            self.assertFalse(actual["additionalProperties"])
            self.assertEqual(actual["required"], ["nested"])
            self.assertFalse(actual["properties"]["nested"]["additionalProperties"])
            self.assertEqual(actual["properties"]["nested"]["required"], ["value"])
            self.assertNotIn("default", actual["properties"]["nested"]["properties"]["value"])
            self.assertEqual(value.model_dump_json(), before)

    async def test_cross_loop_use_is_rejected_before_sdk_dispatch(self):
        """A provider bound to a caller loop cannot be reused on Core's other loop."""
        provider, _, captured = self.provider()
        await provider.ainvoke(request())
        app = AutoAgentApp()
        try:
            result = await app.ainvoke(Workflow("wrong-loop", nodes=[Node("llm", provider.ainvoke)]), request())
            self.assertEqual(result.status, "failed")
            self.assertEqual(result.error.type, "RuntimeError")
            self.assertIn("owning event loop", result.error.message)
        finally:
            await app.aclose()
        self.assertEqual(len(captured), 1)


if __name__ == "__main__":
    unittest.main()

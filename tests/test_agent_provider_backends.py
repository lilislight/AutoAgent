"""Shared contracts for optional LiteLLM and direct DeepSeek backends."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tomllib
import unittest
from unittest.mock import patch

from pydantic import ValidationError

from autoagent import AutoAgentApp, Node, Stream, ValueContract, Workflow
from autoagent.agent import (
    AudioContent, DeepSeekChatCompletionsAdapter,
    DeepSeekProvider, DeepSeekResponsesAdapter,
    EmbeddingProviderOptions, EmbeddingRequest, FileContent, ImageContent,
    LiteLLMChatCompletionsAdapter, LiteLLMEmbeddingProvider, LiteLLMProvider,
    LiteLLMProviderConfig, LLMJsonFormat, LLMJsonSchemaFormat, LLMMessage,
    LLMNativeData, LLMOutputOptions, LLMProviderError, LLMProviderOptions,
    LLMReasoning, LLMRequest, LLMResponse, LLMStreamReducer,
    LLMToolDefinition, LLMToolResult, LLMUnsupportedRequest, ProviderSource,
    TextContent, URLSource, VideoContent,
)
from tests.test_agent_providers import TrackingStream, chat_body, chat_chunk, request, response_body, sse


class Source:
    def __init__(self, values, *, wait=False):
        self.values, self.closed, self.wait = values, False, wait

    async def __aiter__(self):
        for value in self.values:
            yield value
        if self.wait:
            await asyncio.Event().wait()

    async def aclose(self):
        self.closed = True


class ResponsesSource:
    """LiteLLM Responses iterators expose their resource without a close method."""

    def __init__(self, source, *, bridged):
        self.source = source
        if bridged:
            self.litellm_custom_stream_wrapper = source
        else:
            self.response = source
            self.response.headers = {}

    def __aiter__(self):
        return self.source.__aiter__()


class SDK:
    def __init__(self, reply=None, source=None):
        self.reply = reply or chat_body()
        self.source, self.calls = source, []

    async def acompletion(self, **kwargs):
        self.calls.append(kwargs)
        return self.source if kwargs.get("stream") else self.reply

    aresponses = acompletion

    async def aembedding(self, **kwargs):
        self.calls.append(kwargs)
        return self.reply


def tool_definition(**kwargs):
    return LLMToolDefinition(name="lookup", parameters={"type": "object", "properties": {}}, **kwargs)


def options(**values):
    return LLMProviderOptions(protocol="openai.chat_completions", values=values)


class ProviderDefinitionTests(unittest.TestCase):
    def test_optional_sdk_is_not_imported_by_public_exports_or_construction(self):
        """The base package and direct backend work without importing either SDK."""
        script = """
import sys
class Block:
    def find_spec(self, fullname, *args):
        if fullname.split('.')[0] in {'litellm', 'openai'}:
            raise ImportError('SDK blocked')
sys.meta_path.insert(0, Block())
from autoagent.agent import LiteLLMProvider, LiteLLMEmbeddingProvider, DeepSeekProvider
LiteLLMProvider(); LiteLLMEmbeddingProvider(); DeepSeekProvider()
assert 'litellm' not in sys.modules and 'openai' not in sys.modules
"""
        result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
        project = tomllib.loads(Path("pyproject.toml").read_text())["project"]
        self.assertFalse(any(value.startswith("litellm") for value in project["dependencies"]))
        self.assertTrue(any(value.startswith("litellm") for value in project["optional-dependencies"]["litellm"]))

    def test_scoped_options_and_embedding_requests_survive_core_contracts(self):
        """Extension JSON persists without credentials or transport controls."""
        value = EmbeddingRequest(model="embed", input=("one", "two"), provider_options=EmbeddingProviderOptions(
            provider_id="cohere", values={"input_type": "search_query"}))
        contract = ValueContract.create(EmbeddingRequest, location="request")
        self.assertEqual(contract.restore(contract.to_record(value)), value)
        for field in ("api_key", "input", "dimensions", "client", "drop_params"):
            with self.subTest(field=field), self.assertRaises(ValidationError):
                EmbeddingProviderOptions(values={field: "forbidden"})


class DeepSeekAdapterTests(unittest.TestCase):
    def test_chat_reasoning_and_multiple_calls_replay_as_one_assistant_message(self):
        """Reasoning survives tool-only output and remains attached to both calls."""
        adapter = DeepSeekChatCompletionsAdapter()
        raw = chat_body({"role": "assistant", "content": None, "reasoning_content": "opaque", "tool_calls": [
            {"id": str(n), "type": "function", "function": {"name": "lookup", "arguments": "{}"}}
            for n in (1, 2)]})
        raw["choices"][0]["finish_reason"] = "tool_calls"
        output = adapter.decode_response(raw, provider_id="deepseek")
        self.assertIsInstance(output.output[0], LLMReasoning)
        self.assertEqual(len(output.tool_calls), 2)
        restored = LLMResponse.model_validate_json(output.model_dump_json())
        params = adapter.encode_request(LLMRequest(model="deepseek-flash", input=restored.output,
                                                   tools=(tool_definition(),)), provider_id="deepseek")
        self.assertEqual(len(params["messages"]), 1)
        self.assertEqual(params["messages"][0]["reasoning_content"], "opaque")
        self.assertEqual(len(params["messages"][0]["tool_calls"]), 2)
        self.assertEqual(params["messages"][0]["content"], "")

    def test_chat_strings_tokens_images_and_original_detail_follow_vendor_shape(self):
        """DeepSeek receives string instructions and its image file reference format."""
        adapter = DeepSeekChatCompletionsAdapter()
        value = LLMRequest(model="deepseek-flash", max_output_tokens=24, input=(
            LLMMessage(role="developer", content=(TextContent(text="Guide"),)),
            LLMMessage(role="user", content=(ImageContent(source=ProviderSource(provider_id="deepseek", file_id="file-api-1")),
                ImageContent(source=URLSource(url="https://example.org/a.png"), detail="original"))),))
        body = adapter.encode_request(value, provider_id="deepseek")
        self.assertEqual(body["max_tokens"], 24)
        self.assertEqual(body["messages"][0], {"role": "system", "content": "Guide"})
        self.assertEqual(body["messages"][1]["content"][0], {"type": "file", "file_id": "file-api-1"})
        self.assertEqual(body["messages"][1]["content"][1]["image_url"]["detail"], "original")

    def test_unsupported_modalities_formats_and_modes_fail_before_transport(self):
        """Capabilities cannot silently discard documents, forced tools or ignored options."""
        source = URLSource(url="https://example.org/media")
        requests = [
            request(FileContent(source=source)), request(VideoContent(source=source)),
            request(AudioContent(source=source)), request(ImageContent(source=source), model="deepseek-v4-pro"),
            request(temperature=.2), request(top_p=.2, model="deepseek-flash"),
            request(tools=(tool_definition(),), tool_choice="required"),
            request(provider_options=options(previous_response_id="old")),
            request(output_options=LLMOutputOptions(text_format=LLMJsonSchemaFormat(
                name="answer", json_schema={"type": "object"}))),
        ]
        for value in requests:
            with self.subTest(value=value), self.assertRaises(LLMUnsupportedRequest):
                DeepSeekChatCompletionsAdapter().encode_request(value, provider_id="deepseek")
        allowed = request(temperature=.2, provider_options=options(thinking={"type": "disabled"}))
        self.assertEqual(DeepSeekChatCompletionsAdapter().encode_request(allowed, provider_id="deepseek")["temperature"], .2)

    def test_beta_prefix_and_strict_schema_are_explicit(self):
        """Beta features require beta configuration and supported schema constraints."""
        value = request(tools=(tool_definition(strict=True),))
        with self.assertRaises(LLMUnsupportedRequest):
            DeepSeekChatCompletionsAdapter().encode_request(value, provider_id="deepseek")
        self.assertTrue(DeepSeekChatCompletionsAdapter(beta=True).encode_request(value, provider_id="deepseek")["tools"][0]["function"]["strict"])
        prefix = LLMMessage(role="assistant", content=(TextContent(text="Start"),), native=LLMNativeData(
            provider_id="deepseek", protocol="openai.chat_completions", payload={"prefix": True}))
        self.assertTrue(DeepSeekChatCompletionsAdapter(beta=True).encode_request(
            LLMRequest(model="deepseek-flash", input=(prefix,)), provider_id="deepseek")["messages"][0]["prefix"])

    def test_missing_thinking_history_is_rejected_instead_of_fabricated(self):
        """Tool histories must retain original reasoning rather than a dummy placeholder."""
        value = LLMRequest(model="deepseek-flash", tools=(tool_definition(),), input=(
            LLMMessage(role="assistant", content=(TextContent(text="Previous"),)),))
        with self.assertRaises(LLMUnsupportedRequest):
            DeepSeekChatCompletionsAdapter().encode_request(value, provider_id="deepseek")

    def test_schema_keyword_names_are_not_confused_with_constraints(self):
        """A Tool property named maxLength is valid while an actual constraint is rejected."""
        adapter = DeepSeekChatCompletionsAdapter(beta=True)
        schema = {"type": "object", "properties": {"maxLength": {"type": "string"}}}
        value = request(tools=(LLMToolDefinition(name="tool", strict=True, parameters=schema),))
        self.assertIn("maxLength", adapter.encode_request(value, provider_id="deepseek")["tools"][0]["function"]["parameters"]["properties"])
        schema["properties"]["maxLength"]["maxLength"] = 3
        value = request(tools=(LLMToolDefinition(name="tool", strict=True, parameters=schema),))
        with self.assertRaises(LLMUnsupportedRequest):
            adapter.encode_request(value, provider_id="deepseek")

    def test_responses_preserves_reasoning_images_and_rejects_stateful_options(self):
        """Native Responses uses portable items without implying server-side history."""
        adapter = DeepSeekResponsesAdapter()
        value = request(ImageContent(source=ProviderSource(provider_id="deepseek", file_id="file-api-1")),
            model="deepseek-flash", provider_options=LLMProviderOptions(
                protocol="openai.responses", values={"reasoning": {"effort": "max"}}))
        self.assertEqual(adapter.encode_request(value, provider_id="deepseek")["input"][0]["content"][0]["file_id"], "file-api-1")
        with self.assertRaises(LLMUnsupportedRequest):
            adapter.encode_request(request(provider_options=LLMProviderOptions(
                protocol="openai.responses", values={"store": True})), provider_id="deepseek")


class LiteLLMContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_optional_sdk_has_actionable_install_error(self):
        """Using the optional backend fails lazily with the correct extra name."""
        provider = LiteLLMProvider()
        with patch("autoagent.agent.providers.litellm.importlib.import_module", side_effect=ImportError("absent")):
            with self.assertRaisesRegex(ImportError, r"autoagent\[litellm\]"):
                await provider.ainvoke(request())
        await provider.aclose()

    async def test_common_conversion_routing_recording_and_scope(self):
        """Vendor selection remains configuration and calls use the shared recorder."""
        records = []
        class Recorder:
            async def record_llm_call(self, record):
                records.append(record)
        sdk = SDK()
        provider = LiteLLMProvider(LiteLLMProviderConfig(provider_id="one", model_prefix="vendor", api_key="secret"), sdk=sdk, recorder=Recorder())
        self.assertEqual((await provider.ainvoke(request(provider_options=LLMProviderOptions(
            protocol="openai.chat_completions", provider_id="one", values={"reasoning_effort": "high"})))).text, "Answer")
        self.assertEqual(sdk.calls[0]["model"], "vendor/model")
        self.assertEqual(sdk.calls[0]["reasoning_effort"], "high")
        self.assertEqual(sdk.calls[0]["num_retries"], 0)
        self.assertFalse(sdk.calls[0]["drop_params"])
        self.assertEqual([r.status for r in records], ["running", "succeeded"])
        with self.assertRaises(LLMUnsupportedRequest):
            await provider.ainvoke(request(provider_options=LLMProviderOptions(
                protocol="openai.chat_completions", provider_id="other")))
        self.assertEqual(len(sdk.calls), 1)
        await provider.aclose()
        with self.assertRaises(RuntimeError):
            await provider.ainvoke(request())

    async def test_reasoning_blocks_images_and_tool_only_output_keep_native_data(self):
        """SDK extensions become typed content and retain signatures for replay."""
        raw = chat_body({"role": "assistant", "content": "Visible", "reasoning_content": "thought",
            "thinking_blocks": [{"type": "thinking", "thinking": "thought", "signature": "signature"}],
            "images": [{"type": "image_url", "image_url": {"url": "data:image/png;base64,YQ=="}}]})
        sdk = SDK(raw)
        provider = LiteLLMProvider(sdk=sdk)
        value = await provider.ainvoke(request())
        self.assertEqual(value.output[0].text, "thought")
        self.assertEqual(len(value.media), 1)
        self.assertEqual(value.text, "Visible")
        replay = provider.adapter.encode_request(LLMRequest(model="model", input=value.output), provider_id="litellm")
        self.assertEqual(replay["messages"][0]["thinking_blocks"][0]["signature"], "signature")
        self.assertEqual(len(replay["messages"]), 1)
        await provider.aclose()

    async def test_stream_has_reasoning_final_usage_and_closes_source(self):
        """Streaming reduces reasoning/text and publishes one authoritative result."""
        source = Source([chat_chunk({"reasoning_content": "think"}), chat_chunk({"content": "answer"}),
                         chat_chunk({}, "stop"), {"choices": [], "usage": {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5}}])
        provider = LiteLLMProvider(sdk=SDK(source=source))
        chunks = [chunk async for chunk in provider.astream(request())]
        self.assertTrue(source.closed)
        self.assertEqual(chunks[0].kind, "reasoning")
        self.assertEqual(chunks[-1].response.text, "answer")
        self.assertEqual(chunks[-1].response.output[0].text, "think")
        self.assertEqual(chunks[-1].response.usage.total_tokens, 5)
        await provider.aclose()

    async def test_stream_cancellation_closes_source_and_records_cancellation(self):
        """Cancellation releases the active stream without converting it to success."""
        source = Source([chat_chunk({"content": "first"})], wait=True)
        provider = LiteLLMProvider(sdk=SDK(source=source))
        stream = provider.astream(request())
        await anext(stream)
        task = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(source.closed)
        await provider.aclose()

    async def test_streamed_signatures_and_tool_extensions_survive_replay(self):
        """Fragmented thinking signatures and Tool call metadata remain attached."""
        source = Source([
            chat_chunk({"thinking_blocks": [{"type": "thinking", "thinking": "first"}]}),
            chat_chunk({"thinking_blocks": [{"type": "thinking", "thinking": " second"}]}),
            chat_chunk({"thinking_blocks": [{"type": "thinking", "signature": "signed"}]}),
            chat_chunk({"tool_calls": [{"index": 0, "id": "c1", "type": "function",
                "function": {"name": "lookup", "arguments": "{}"},
                "provider_specific_fields": {"thought_signature": "opaque"}}]}),
            chat_chunk({}, "tool_calls")])
        provider = LiteLLMProvider(sdk=SDK(source=source))
        chunks = [c async for c in provider.astream(request())]
        response = chunks[-1].response
        self.assertEqual(response.output[0].text, "first second")
        replay = provider.adapter.encode_request(LLMRequest(model="model", input=response.output), provider_id="litellm")
        message = replay["messages"][0]
        self.assertEqual(message["thinking_blocks"], [{"type": "thinking", "thinking": "first second", "signature": "signed"}])
        self.assertEqual(message["tool_calls"][0]["provider_specific_fields"]["thought_signature"], "opaque")
        changed = response.output[0].model_copy(update={"text": "edited"})
        with self.assertRaises(LLMUnsupportedRequest):
            provider.adapter.encode_request(LLMRequest(model="model", input=(changed,)), provider_id="litellm")
        await provider.aclose()

    async def test_responses_and_structured_output_use_existing_contract(self):
        """Responses and JSON validation return the same LLMResponse type."""
        sdk = SDK(response_body())
        provider = LiteLLMProvider(protocol="openai.responses", sdk=sdk)
        result = await provider.ainvoke(request())
        self.assertEqual(result.text, "Answer")
        self.assertIn("input", sdk.calls[0])
        await provider.aclose()
        provider = LiteLLMProvider(sdk=SDK(chat_body({"role": "assistant", "content": '{"ok":true}'})))
        value = await provider.ainvoke(request(output_options=LLMOutputOptions(text_format=LLMJsonFormat())))
        self.assertEqual(value.parse(dict), {"ok": True})
        await provider.aclose()

    async def test_responses_iterators_close_their_owned_resource_on_completion(self):
        """Native HTTP and Chat bridge iterators release streams without owning global SDK pools."""
        for bridged in (False, True):
            with self.subTest(bridged=bridged):
                source = Source([{"type": "response.completed", "response": response_body()}])
                wrapped = ResponsesSource(source, bridged=bridged)
                provider = LiteLLMProvider(protocol="openai.responses", sdk=SDK(source=wrapped))
                chunks = [chunk async for chunk in provider.astream(request())]
                self.assertEqual(chunks[-1].response.text, "Answer")
                self.assertTrue(source.closed)
                self.assertEqual(provider._active, 0)
                await provider.aclose()

    async def test_responses_iterators_close_on_early_exit_and_cancellation(self):
        """Closing or cancelling a Responses consumer releases the nested per-call resource."""
        for bridged in (False, True):
            for cancel in (False, True):
                with self.subTest(bridged=bridged, cancel=cancel):
                    source = Source([{"type": "response.output_text.delta", "output_index": 0, "delta": "A"}], wait=True)
                    wrapped = ResponsesSource(source, bridged=bridged)
                    provider = LiteLLMProvider(protocol="openai.responses", sdk=SDK(source=wrapped))
                    stream = provider.astream(request())
                    self.assertEqual((await anext(stream)).delta, "A")
                    if cancel:
                        pending = asyncio.create_task(anext(stream))
                        await asyncio.sleep(0)
                        pending.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await pending
                    else:
                        await stream.aclose()
                    self.assertTrue(source.closed)
                    self.assertEqual(provider._active, 0)
                    await provider.aclose()

    async def test_responses_iterator_cleanup_preserves_the_original_failure(self):
        """A remote failure is retained while the nested Responses stream is still closed."""
        source = Source([{"type": "error", "message": "remote failure"}])
        wrapped = ResponsesSource(source, bridged=True)
        provider = LiteLLMProvider(protocol="openai.responses", sdk=SDK(source=wrapped))
        with self.assertRaisesRegex(LLMProviderError, "remote failure"):
            async for _chunk in provider.astream(request()):
                pass
        self.assertTrue(source.closed)
        await provider.aclose()

    async def test_embeddings_normalize_order_and_pass_api_options(self):
        """Embedding extensions reach LiteLLM while vectors retain input alignment."""
        sdk = SDK({"object": "list", "model": "embed", "data": [
            {"object": "embedding", "index": 1, "embedding": [.3, .4]},
            {"object": "embedding", "index": 0, "embedding": [.1, .2]}]})
        provider = LiteLLMEmbeddingProvider(LiteLLMProviderConfig(model_prefix="cohere"), sdk=sdk)
        value = await provider.ainvoke(EmbeddingRequest(model="embed", input=("a", "b"),
            provider_options=EmbeddingProviderOptions(values={"input_type": "search_query"})))
        self.assertEqual(value.vectors, ((.1, .2), (.3, .4)))
        self.assertEqual(sdk.calls[0]["input_type"], "search_query")
        self.assertEqual(sdk.calls[0]["model"], "cohere/embed")
        await provider.aclose()

    async def test_media_mapping_rejects_unrepresentable_video_ranges(self):
        """Generic conversion preserves media types and refuses implicit video cropping."""
        adapter = LiteLLMChatCompletionsAdapter()
        source = URLSource(url="https://example.org/media")
        for part, kind in ((FileContent(source=source), "file"), (VideoContent(source=source), "video_url")):
            self.assertEqual(adapter.encode_request(request(part), provider_id="litellm")["messages"][0]["content"][0]["type"], kind)
        with self.assertRaises(LLMUnsupportedRequest):
            adapter.encode_request(request(VideoContent(source=source, start_seconds=1)), provider_id="litellm")

    async def test_provider_node_and_stream_node_use_runtime_lifespan(self):
        """The backend composes with typed Nodes and the existing stream reducer."""
        source = Source([chat_chunk({"content": "answer"}), chat_chunk({}, "stop")])
        provider = LiteLLMProvider(sdk=SDK(source=source))
        @asynccontextmanager
        async def resources():
            async with provider:
                yield
        app = AutoAgentApp(runtime_lifespan=resources)
        try:
            await app.aprepare()
            direct = await app.ainvoke(Workflow("direct", nodes=[Node("llm", provider.ainvoke)]), request())
            self.assertEqual(direct.status, "completed", direct.error)
            streamed = await app.ainvoke(Workflow("stream", nodes=[Node("llm", provider.astream, stream=Stream(LLMStreamReducer()))]), request())
            self.assertEqual(streamed.status, "completed", streamed.error)
            self.assertEqual(streamed.output["output"][0]["content"][0]["text"], "answer")
        finally:
            await app.aclose()


@unittest.skipUnless(importlib.util.find_spec("openai"), "Install autoagent[agent] for SDK wire tests")
class DirectDeepSeekSDKTests(unittest.IsolatedAsyncioTestCase):
    async def test_owned_client_uses_deepseek_key_without_openai_fallback(self):
        """Direct credentials belong to DeepSeek and owned SDK retries stay disabled."""
        with patch.dict("os.environ", {"DEEPSEEK_API_KEY": "deepseek-test", "OPENAI_API_KEY": "other-test"}, clear=True):
            async with DeepSeekProvider() as provider:
                self.assertEqual(provider._get_client().api_key, "deepseek-test")
                self.assertEqual(provider._get_client().max_retries, 0)
        with patch.dict("os.environ", {"OPENAI_API_KEY": "other-test"}, clear=True):
            provider = DeepSeekProvider()
            with self.assertRaisesRegex(ValueError, "DEEPSEEK_API_KEY"):
                await provider.aprepare()
            await provider.aclose()

    async def test_direct_sdk_sends_vendor_options_and_replays_response(self):
        """The real SDK preserves DeepSeek thinking and its tool message grouping."""
        import httpx
        from openai import AsyncOpenAI
        bodies = []
        def handler(http_request):
            bodies.append(json.loads(http_request.content))
            return httpx.Response(200, json=chat_body({"role": "assistant", "content": "Answer", "reasoning_content": "thought"}))
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            async with AsyncOpenAI(api_key="offline", base_url="https://api.deepseek.com", http_client=http) as client:
                async with DeepSeekProvider(client=client) as provider:
                    first = await provider.ainvoke(request(model="deepseek-flash", provider_options=options(
                        thinking={"type": "enabled"}, reasoning_effort="max")))
                    await provider.ainvoke(LLMRequest(model="deepseek-flash", input=first.output, tools=(tool_definition(),)))
        self.assertEqual(bodies[0]["reasoning_effort"], "max")
        self.assertEqual(bodies[0]["thinking"], {"type": "enabled"})
        self.assertEqual(bodies[1]["messages"][0]["reasoning_content"], "thought")

    async def test_direct_stream_emits_reasoning_and_closes_http(self):
        """The direct SDK streams reasoning and Tool arguments through one contract."""
        import httpx
        from openai import AsyncOpenAI
        body = TrackingStream(sse([chat_chunk({"reasoning_content": "thought"}),
            chat_chunk({"tool_calls": [{"index": 0, "id": "c1", "type": "function", "function": {"name": "lookup", "arguments": "{"}}]}),
            chat_chunk({"tool_calls": [{"index": 0, "function": {"arguments": "}"}}]}),
            chat_chunk({}, "tool_calls")], done=True))
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200,
            stream=body, headers={"content-type": "text/event-stream"}))) as http:
            async with AsyncOpenAI(api_key="offline", base_url="https://api.deepseek.com", http_client=http) as client:
                async with DeepSeekProvider(client=client) as provider:
                    chunks = [c async for c in provider.astream(request(model="deepseek-flash"))]
        self.assertTrue(body.closed)
        self.assertEqual(chunks[0].kind, "reasoning")
        self.assertEqual(chunks[-1].response.tool_calls[0].arguments, "{}")


@unittest.skipUnless(importlib.util.find_spec("litellm"), "Install autoagent[litellm] for LiteLLM SDK tests")
class LiteLLMSDKTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.environment = patch.dict("os.environ", {"LITELLM_LOCAL_MODEL_COST_MAP": "True"})
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def isolated(self):
        """Keep optional SDK global pools/model maps outside Host memory-policy tests."""
        if os.environ.get("AUTOAGENT_LITELLM_SDK_TEST_PROCESS") == "1":
            return False
        environment = dict(os.environ, AUTOAGENT_LITELLM_SDK_TEST_PROCESS="1")
        result = subprocess.run([sys.executable, "-m", "unittest",
            "tests.test_agent_provider_backends.LiteLLMSDKTests." + self._testMethodName, "-v"],
            env=environment, capture_output=True, text=True, timeout=45)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return True

    async def test_actual_sdk_chat_reasoning_and_native_responses_share_models(self):
        """The real SDK reaches both APIs without leaking its response classes."""
        if self.isolated():
            return
        import httpx
        bodies = []
        async def send(client, http_request, **kwargs):
            body = json.loads(http_request.content)
            bodies.append((http_request.url.path, body))
            if http_request.url.path.endswith("/responses"):
                return httpx.Response(200, request=http_request, json=response_body(created_at=1, output=[
                    {"type": "reasoning", "id": "rs1", "summary": [], "content": [{"type": "reasoning_text", "text": "thought"}]},
                    {"type": "message", "id": "m1", "role": "assistant", "status": "completed",
                     "content": [{"type": "output_text", "text": "Answer", "annotations": []}]}]))
            if http_request.url.path.endswith("/chat/completions"):
                return httpx.Response(200, request=http_request, json=chat_body({"role": "assistant", "content": "Answer", "reasoning_content": "thought"}))
            raise AssertionError("Unexpected request " + str(http_request.url))
        with patch.object(httpx.AsyncClient, "send", send):
            for protocol in ("openai.chat_completions", "openai.responses"):
                async with LiteLLMProvider(LiteLLMProviderConfig(provider_id="test", model_prefix="openai",
                    base_url="https://api.deepseek.com", api_key="offline"), protocol=protocol) as provider:
                    response = await provider.ainvoke(request(model="deepseek-flash"))
                    self.assertIsInstance(response, LLMResponse)
                    self.assertEqual(response.output[0].text, "thought")
                    self.assertEqual(response.text, "Answer")
                    contract = ValueContract.create(LLMResponse, location="response")
                    self.assertEqual(contract.restore(contract.to_record(response)), response)
        self.assertTrue(bodies[-1][0].endswith("/responses"))

    async def test_actual_sdk_stream_and_embedding_normalization(self):
        """Real LiteLLM stream wrappers close and vectors decode into portable models."""
        if self.isolated():
            return
        import httpx
        body = TrackingStream(sse([chat_chunk({"reasoning_content": "thought"}),
                                   chat_chunk({"content": "Answer"}), chat_chunk({}, "stop")], done=True))
        async def send(client, http_request, **kwargs):
            if http_request.url.path.endswith("/embeddings"):
                return httpx.Response(200, request=http_request, json={"object": "list", "model": "embed", "data": [
                    {"object": "embedding", "index": 0, "embedding": [.1, .2]}]})
            if http_request.url.path.endswith("/chat/completions"):
                return httpx.Response(200, request=http_request, stream=body, headers={"content-type": "text/event-stream"})
            raise AssertionError("Unexpected request " + str(http_request.url))
        config = LiteLLMProviderConfig(model_prefix="openai", api_key="offline")
        with patch.object(httpx.AsyncClient, "send", send):
            async with LiteLLMProvider(config) as provider:
                chunks = [c async for c in provider.astream(request(model="gpt-4o"))]
                self.assertEqual(chunks[-1].response.text, "Answer")
                self.assertEqual(chunks[-1].response.output[0].text, "thought")
            async with LiteLLMEmbeddingProvider(config) as provider:
                response = await provider.ainvoke(EmbeddingRequest(model="text-embedding-3-small", input="hello"))
                self.assertEqual(response.vectors, ((.1, .2),))
        self.assertTrue(body.closed)

    async def test_actual_sdk_bridged_responses_stream_replays_tools_and_closes_http(self):
        """LM Studio's real Responses bridge completes both tool and answer turns without cleanup errors."""
        if self.isolated():
            return
        import httpx
        bodies, streams = [], []
        async def send(client, http_request, **kwargs):
            self.assertTrue(http_request.url.path.endswith("/chat/completions"))
            body = json.loads(http_request.content)
            bodies.append(body)
            if len(bodies) == 1:
                events = [chat_chunk({"reasoning_content": "thought"}),
                    chat_chunk({"tool_calls": [{"index": 0, "id": "call-1", "type": "function",
                        "function": {"name": "lookup", "arguments": "{}"}}]}),
                    chat_chunk({}, "tool_calls")]
            else:
                events = [chat_chunk({"content": "Answer"}), chat_chunk({}, "stop")]
            stream = TrackingStream(sse(events, done=True))
            streams.append(stream)
            return httpx.Response(200, request=http_request, stream=stream,
                headers={"content-type": "text/event-stream"})
        config = LiteLLMProviderConfig(model_prefix="lm_studio", base_url="http://lm-studio.test/v1", api_key="offline")
        with patch.object(httpx.AsyncClient, "send", send):
            async with LiteLLMProvider(config, protocol="openai.responses") as provider:
                value = request(model="google/gemma-4-e4b", tools=(tool_definition(),))
                chunks = [chunk async for chunk in provider.astream(value)]
                response = chunks[-1].response
                self.assertEqual(response.finish_reason, "tool_calls")
                self.assertEqual([item.type for item in response.output], ["reasoning", "tool_call"])
                self.assertEqual(response.tool_calls[0].call_id, "call-1")
                followup = value.model_copy(update={"input": (*value.input, *response.output,
                    LLMToolResult(call_id="call-1", content=(TextContent(text="Found"),)))})
                chunks = [chunk async for chunk in provider.astream(followup)]
                self.assertEqual(chunks[-1].response.text, "Answer")
                self.assertEqual(provider._active, 0)
        self.assertEqual(len(bodies), 2)
        self.assertTrue(all(stream.closed for stream in streams))
        self.assertEqual(bodies[0]["model"], "google/gemma-4-e4b")
        self.assertTrue(any(message.get("tool_call_id") == "call-1" for message in bodies[1]["messages"]))

    async def test_actual_sdk_responses_stream_early_close_releases_http(self):
        """Closing native and Chat-bridged Responses streams after a delta releases the actual HTTP body."""
        if self.isolated():
            return
        import httpx
        for route in ("openai", "lm_studio"):
            with self.subTest(route=route):
                if route == "lm_studio":
                    events = [chat_chunk({"content": "A"})]
                else:
                    events = [{"type": "response.output_text.delta", "output_index": 0,
                        "item_id": "msg_1", "content_index": 0, "sequence_number": 1, "delta": "A"}]
                body = TrackingStream(sse(events), pause=True)
                async def send(client, http_request, **kwargs):
                    return httpx.Response(200, request=http_request, stream=body,
                        headers={"content-type": "text/event-stream"})
                config = LiteLLMProviderConfig(model_prefix=route, base_url="http://local-model.test/v1", api_key="offline")
                with patch.object(httpx.AsyncClient, "send", send):
                    async with LiteLLMProvider(config, protocol="openai.responses") as provider:
                        stream = provider.astream(request(model="gpt-4o" if route == "openai" else "google/gemma-4-e4b"))
                        self.assertEqual((await anext(stream)).delta, "A")
                        await stream.aclose()
                        self.assertEqual(provider._active, 0)
                self.assertTrue(body.closed)

    async def test_actual_sdk_rate_limit_does_not_retry_or_fallback(self):
        """A failed SDK request retains status while Core owns retry decisions."""
        if self.isolated():
            return
        import httpx
        calls = []
        async def send(client, http_request, **kwargs):
            calls.append(http_request.url.path)
            return httpx.Response(429, request=http_request, headers={"x-request-id": "request-test"},
                                  json={"error": {"message": "Rate limited", "type": "rate_limit_error"}})
        with patch.object(httpx.AsyncClient, "send", send):
            async with LiteLLMProvider(LiteLLMProviderConfig(model_prefix="openai", api_key="offline")) as provider:
                with self.assertRaises(LLMProviderError) as failure:
                    await provider.ainvoke(request(model="gpt-4o"))
        self.assertEqual(failure.exception.status_code, 429)
        self.assertTrue(failure.exception.retryable)
        self.assertEqual(len(calls), 1)


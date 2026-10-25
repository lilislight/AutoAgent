"""Compact Provider output must retain the next request's protocol semantics."""

from copy import deepcopy
import unittest

from autoagent.agent import (
    ChatCompletionsAdapter, DeepSeekChatCompletionsAdapter, ImageContent,
    LiteLLMChatCompletionsAdapter, LLMMessage, LLMRequest, LLMResponse,
    LLMToolResult, LLMUnsupportedRequest, RefusalContent, ResponsesAdapter,
    TextContent,
)
from autoagent.agent.providers._common import usage
from tests.test_agent_providers import chat_body, chat_chunk, output_message, request, response_body


def calls():
    return [{"id": f"call-{index}", "type": "function", "function": {
        "name": "lookup", "arguments": '{"city":"' + city + '"}'}}
        for index, city in enumerate(("北京", "上海", "东京"))]


def restore(response):
    return LLMResponse.model_validate_json(response.model_dump_json())


class NativeReplayTests(unittest.TestCase):
    def test_tool_only_reasoning_is_stored_once_and_replayed_as_one_message(self):
        """Null and empty Chat bodies produce reasoning and calls without a blank Message."""
        for adapter in (LiteLLMChatCompletionsAdapter(), DeepSeekChatCompletionsAdapter()):
            for content in (None, ""):
                with self.subTest(adapter=type(adapter).__name__, content=content):
                    raw = chat_body({"role": "assistant", "content": content,
                        "reasoning_content": "one-source-of-reasoning", "tool_calls": calls(),
                        "function_call": None, "provider_specific_fields": {"refusal": None}})
                    original = deepcopy(raw)
                    response = restore(adapter.decode_response(raw, provider_id="local"))
                    self.assertEqual(raw, original)
                    self.assertEqual([item.type for item in response.output], ["reasoning", *["tool_call"] * 3])
                    self.assertEqual(response.model_dump_json().count("one-source-of-reasoning"), 1)
                    for call in response.tool_calls:
                        self.assertEqual(set(call.native.payload), {"_group_id"})
                    wire = adapter.encode_request(LLMRequest(model="model", input=response.output), provider_id="local")
                    self.assertEqual(len(wire["messages"]), 1)
                    self.assertEqual(wire["messages"][0]["reasoning_content"], "one-source-of-reasoning")
                    self.assertEqual(wire["messages"][0]["tool_calls"], calls())

    def test_thinking_blocks_retain_boundaries_empty_blocks_and_signatures(self):
        """Thinking text references preserve signed and redacted block sequences after JSON restore."""
        blocks = [
            {"type": "thinking", "thinking": "first ", "signature": "sig-1"},
            {"type": "thinking", "thinking": "", "signature": "sig-empty"},
            {"type": "redacted_thinking", "data": "opaque"},
            {"type": "thinking", "thinking": "北京", "signature": "sig-2"},
        ]
        adapter = LiteLLMChatCompletionsAdapter()
        for with_text in (False, True):
            message = {"role": "assistant", "content": "Visible", "thinking_blocks": blocks}
            if with_text:
                message["reasoning_content"] = "first 北京"
            response = restore(adapter.decode_response(chat_body(message), provider_id="local"))
            reasoning = response.output[0]
            self.assertEqual(reasoning.text, "first 北京")
            self.assertTrue(all("thinking" not in block for block in reasoning.native.payload["_chat_fields"]["thinking_blocks"]))
            wire = adapter.encode_request(LLMRequest(model="model", input=response.output), provider_id="local")
            self.assertEqual(wire["messages"][0]["thinking_blocks"], blocks)
            self.assertEqual("reasoning_content" in wire["messages"][0], with_text)
            changed = reasoning.model_copy(update={"text": "edited"})
            with self.assertRaises(LLMUnsupportedRequest):
                adapter.encode_request(LLMRequest(model="model", input=(changed,)), provider_id="local")

    def test_distinct_thinking_and_reasoning_fields_remain_distinct(self):
        """Deduplication does not replace a genuinely different provider reasoning representation."""
        adapter = LiteLLMChatCompletionsAdapter()
        blocks = [{"type": "thinking", "thinking": "different text", "signature": "sig"}]
        response = restore(adapter.decode_response(chat_body({"role": "assistant", "content": None,
            "reasoning_content": "canonical text", "thinking_blocks": blocks}), provider_id="local"))
        wire = adapter.encode_request(LLMRequest(model="model", input=response.output), provider_id="local")
        self.assertEqual(wire["messages"][0]["reasoning_content"], "canonical text")
        self.assertEqual(wire["messages"][0]["thinking_blocks"], blocks)

    def test_provider_fields_alone_do_not_create_an_empty_reasoning_item(self):
        """Message extensions have a single owner without manufacturing empty reasoning output."""
        adapter = LiteLLMChatCompletionsAdapter()
        fields = {"turn_token": "provider-turn-token"}
        response = restore(adapter.decode_response(chat_body({"role": "assistant", "content": "",
            "tool_calls": calls(), "provider_specific_fields": fields}), provider_id="local"))
        self.assertEqual([item.type for item in response.output], ["tool_call"] * 3)
        self.assertEqual(response.model_dump_json().count("provider-turn-token"), 1)
        wire = adapter.encode_request(LLMRequest(model="model", input=response.output), provider_id="local")
        self.assertEqual(wire["messages"][0]["provider_specific_fields"], fields)

    def test_reasoning_only_output_keeps_its_provider_replay_fields(self):
        """A reasoning-only turn owns its extensions when there is no Message or Tool Call."""
        adapter = LiteLLMChatCompletionsAdapter()
        fields = {"turn_token": "reasoning-token"}
        response = restore(adapter.decode_response(chat_body({"role": "assistant", "content": "",
            "reasoning_content": "reasoning-only-text", "provider_specific_fields": fields}), provider_id="local"))
        self.assertEqual([item.type for item in response.output], ["reasoning"])
        self.assertEqual(response.model_dump_json().count("reasoning-only-text"), 1)
        wire = adapter.encode_request(LLMRequest(model="model", input=response.output), provider_id="local")
        self.assertEqual(wire["messages"][0]["provider_specific_fields"], fields)

    def test_image_only_output_has_no_empty_text_or_duplicate_native_image(self):
        """Media-only responses keep real content and replayable message extensions."""
        adapter = LiteLLMChatCompletionsAdapter()
        for with_tools in (False, True):
            raw = chat_body({"role": "assistant", "content": "", "images": [
                {"image_url": {"url": "data:image/png;base64,YQ=="}}],
                "provider_specific_fields": {"turn_token": "image-token"},
                "tool_calls": calls() if with_tools else []})
            response = restore(adapter.decode_response(raw, provider_id="local"))
            message = response.output[0]
            self.assertIsInstance(message, LLMMessage)
            self.assertEqual(len(message.content), 1)
            self.assertIsInstance(message.content[0], ImageContent)
            self.assertEqual(response.model_dump_json().count("YQ=="), 1)
            wire = adapter.encode_request(LLMRequest(model="model", input=response.output), provider_id="local")
            self.assertEqual(len(wire["messages"]), 1)
            self.assertEqual(wire["messages"][0]["provider_specific_fields"], {"turn_token": "image-token"})

    def test_responses_reasoning_has_no_duplicate_text_summary_or_ciphertext(self):
        """Compact reasoning preserves content boundaries and extra fields when reconstructing Responses."""
        adapter = ResponsesAdapter()
        reasoning = {"type": "reasoning", "id": "rs1", "status": "completed",
            "content": [
                {"type": "reasoning_text", "text": "reasoning-first", "marker": "a"},
                {"type": "reasoning_text", "text": "北京", "marker": "b"},
            ],
            "summary": [{"type": "summary_text", "text": "unique-summary", "marker": "c"}],
            "encrypted_content": "unique-ciphertext"}
        original = deepcopy(reasoning)
        response = restore(adapter.decode_response(response_body([reasoning]), provider_id="local"))
        self.assertEqual(reasoning, original)
        for value in ("reasoning-first", "unique-summary", "unique-ciphertext"):
            self.assertEqual(response.model_dump_json().count(value), 1)
        wire = adapter.encode_request(LLMRequest(model="model", input=response.output), provider_id="local")
        self.assertEqual(wire["input"], [reasoning])
        for field, changed in (("text", "edited"), ("summary", ("edited",)), ("encrypted_content", "edited")):
            item = response.output[0].model_copy(update={field: changed})
            with self.assertRaises(LLMUnsupportedRequest):
                adapter.encode_request(LLMRequest(model="model", input=(item,)), provider_id="local")

    def test_responses_empty_content_and_absent_content_remain_distinct(self):
        """Removing duplicate raw reasoning preserves the empty versus absent content boundary."""
        adapter = ResponsesAdapter()
        for content in (None, [], [{"type": "reasoning_text", "text": ""}]):
            reasoning = {"type": "reasoning", "id": "rs1", "summary": []}
            if content is not None:
                reasoning["content"] = content
            response = restore(adapter.decode_response(response_body([reasoning]), provider_id="local"))
            wire = adapter.encode_request(LLMRequest(model="model", input=response.output), provider_id="local")
            self.assertEqual(wire["input"], [reasoning])

    def test_responses_skip_empty_messages_and_empty_text_parts(self):
        """Only meaningful standard content survives, including refusals and opaque content."""
        adapter = ResponsesAdapter()
        empty = output_message("")
        missing = {**empty, "content": []}
        mixed = output_message("Visible")
        mixed["content"].insert(0, {"type": "output_text", "text": "", "annotations": []})
        response = adapter.decode_response(response_body([empty, missing, mixed]), provider_id="local")
        self.assertEqual(len(response.output), 1)
        self.assertEqual(len(response.output[0].content), 1)
        self.assertEqual(response.text, "Visible")
        self.assertIsNone(response.output[0].content[0].native)
        refusal = {**empty, "content": [{"type": "refusal", "refusal": "Denied"}]}
        response = adapter.decode_response(response_body([refusal]), provider_id="local")
        self.assertIsInstance(response.output[0].content[0], RefusalContent)
        unknown = {**empty, "content": [{"type": "unknown_content", "data": "opaque"}]}
        response = adapter.decode_response(response_body([unknown]), provider_id="local")
        self.assertEqual(response.output[0].type, "provider_item")

    def test_standard_chat_keeps_message_extensions_once_for_multiple_calls(self):
        """Unmapped message-level data is retained once, independent of the number of calls."""
        adapter = ChatCompletionsAdapter()
        for content in (None, "Visible"):
            response = adapter.decode_response(chat_body({"role": "assistant", "content": content,
                "reasoning_content": "unmapped-extension", "tool_calls": calls()}), provider_id="local")
            self.assertEqual(response.model_dump_json().count("unmapped-extension"), 1)

    def test_reasoning_does_not_leak_between_response_groups(self):
        """Compact data reconstructs separate assistant turns around correlated tool observations."""
        adapter = LiteLLMChatCompletionsAdapter()
        first = adapter.decode_response(chat_body({"role": "assistant", "content": "",
            "reasoning_content": "first reasoning", "tool_calls": calls()}), provider_id="local")
        raw = chat_body({"role": "assistant", "content": "Answer", "reasoning_content": "second reasoning"})
        raw["id"] = "second-response"
        second = adapter.decode_response(raw, provider_id="local")
        inputs = (*request().input, *restore(first).output,
            *(LLMToolResult(call_id=call.call_id, content=(TextContent(text="Found"),)) for call in first.tool_calls),
            *restore(second).output)
        messages = adapter.encode_request(LLMRequest(model="model", input=inputs), provider_id="local")["messages"]
        assistants = [message for message in messages if message["role"] == "assistant"]
        self.assertEqual(len(assistants), 2)
        self.assertEqual([message["reasoning_content"] for message in assistants], ["first reasoning", "second reasoning"])
        self.assertEqual(len(assistants[0]["tool_calls"]), 3)
        self.assertNotIn("tool_calls", assistants[1])

    def test_usage_retains_only_unmapped_statistics_in_native(self):
        """Reported counts occur once while extra provider usage details remain available."""
        for chat in (False, True):
            incoming = "prompt_tokens" if chat else "input_tokens"
            outgoing = "completion_tokens" if chat else "output_tokens"
            raw = {incoming: 10, outgoing: 5, "total_tokens": 15,
                incoming + "_details": {"cached_tokens": 2, "audio_tokens": 0},
                outgoing + "_details": {"reasoning_tokens": 3, "accepted_prediction_tokens": 1, "unused": None}}
            original = deepcopy(raw)
            result = usage(raw, chat=chat)
            self.assertEqual(raw, original)
            self.assertEqual((result.input_tokens, result.output_tokens, result.total_tokens), (10, 5, 15))
            self.assertEqual((result.cache_read_tokens, result.reasoning_tokens), (2, 3))
            self.assertEqual(result.native, {incoming + "_details": {"audio_tokens": 0},
                outgoing + "_details": {"accepted_prediction_tokens": 1}})


class NativeReplayStreamTests(unittest.IsolatedAsyncioTestCase):
    async def test_chat_stream_and_nonstream_output_use_the_same_compact_data(self):
        """Empty text deltas cannot reintroduce blank Messages or duplicated reasoning."""
        adapter = LiteLLMChatCompletionsAdapter()
        reasoning = "streamed-reasoning"
        async def source():
            yield chat_chunk({"content": "", "reasoning_content": reasoning})
            yield chat_chunk({"tool_calls": [{**call, "index": index} for index, call in enumerate(calls())]})
            yield chat_chunk({}, "tool_calls")
        chunks = [chunk async for chunk in adapter.decode_stream(source(), request=request(), provider_id="local")]
        streamed = chunks[-1].response
        raw = chat_body({"role": "assistant", "content": "", "reasoning_content": reasoning, "tool_calls": calls()})
        raw["id"] = streamed.provider_response_id
        raw["choices"][0]["finish_reason"] = "tool_calls"
        regular = adapter.decode_response(raw, provider_id="local")
        self.assertEqual(streamed.output, regular.output)
        self.assertEqual(streamed.model_dump_json().count(reasoning), 1)
        self.assertFalse(any(chunk.type == "item" and isinstance(chunk.item, LLMMessage) for chunk in chunks))

    async def test_responses_stream_skips_empty_completed_message_items(self):
        """Item events and terminal Responses output both omit empty Messages."""
        adapter = ResponsesAdapter()
        empty = output_message("")
        visible = output_message("Answer")
        async def source():
            yield {"type": "response.output_item.done", "output_index": 0, "item": empty}
            yield {"type": "response.output_item.done", "output_index": 1, "item": visible}
            yield {"type": "response.completed", "response": response_body([empty, visible])}
        chunks = [chunk async for chunk in adapter.decode_stream(source(), request=request(), provider_id="local")]
        self.assertEqual([chunk.type for chunk in chunks], ["item", "completed"])
        self.assertEqual(chunks[0].item_key, "item:1")
        self.assertEqual(chunks[-1].response.text, "Answer")
        self.assertEqual(len(chunks[-1].response.output), 1)

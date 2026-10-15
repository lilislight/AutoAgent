"""Generated JSON validation and Provider terminal response boundaries."""

import json
import unittest

from pydantic import BaseModel, Field

from autoagent.agent import (
    LLMResponse, LLMMessage, TextContent, RefusalContent, LLMJsonSchemaFormat,
    LLMStructuredOutputError, LLMJsonFormat, LLMToolCall, LLMOutputOptions,
)
from tests import test_agent_providers as provider_tests
from tests.test_agent_providers import response_body, output_message, request, httpx


class Answer(BaseModel):
    count: int = Field(ge=0)


def response(text='{"count":2}', *, finish_reason="stop", content=None):
    return LLMResponse(model="test", finish_reason=finish_reason,
                       output=(LLMMessage(role="assistant", content=content or (TextContent(text=text),)),))


class StructuredOutputTests(unittest.TestCase):
    def test_parse_retains_python_types_and_constraints(self):
        """Parse complete JSON into the caller's constrained Python model."""
        self.assertEqual(response().parse(Answer).count, 2)
        for value in ('{"count":-1}', '{"count":"2"}'):
            with self.subTest(value=value), self.assertRaises(LLMStructuredOutputError) as caught:
                response(value).parse(Answer)
            self.assertEqual(caught.exception.kind, "schema_mismatch")
        self.assertEqual(response('{"count":"2"}').parse(Answer, strict=False).count, 2)

    def test_invalid_duplicate_and_nonfinite_json_are_rejected(self):
        """Reject malformed values before Pydantic can coerce or overwrite them."""
        for value in ('not-json', '{"count":1,"count":2}', '{"count":NaN}', '{"count":1e400}'):
            with self.subTest(value=value), self.assertRaises(LLMStructuredOutputError) as caught:
                response(value).parse(Answer)
            self.assertEqual(caught.exception.kind, "invalid_json")

    def test_refusal_and_incomplete_outputs_have_distinct_errors(self):
        """Refusal and truncation cannot be mistaken for a completed value."""
        for value, kind in ((response(content=(RefusalContent(text="No"),)), "refusal"),
                            (response(finish_reason="length"), "truncated"),
                            (response(""), "no_text"), (response(finish_reason="tool_calls"), "tool_calls")):
            with self.subTest(kind=kind), self.assertRaises(LLMStructuredOutputError) as caught:
                value.parse(Answer)
            self.assertEqual(caught.exception.kind, kind)

    def test_schema_refs_are_local_and_schema_is_not_mutated(self):
        """Validate local definitions without retrieving external schemas."""
        schema = LLMJsonSchemaFormat(name="Answer", json_schema={
            "type":"object", "properties":{"count":{"$ref":"#/$defs/count"}},
            "$defs":{"count":{"type":"integer", "minimum":0}}, "required":["count"],
        })
        before = schema.model_dump_json()
        self.assertEqual(response().validate_output(schema), {"count":2})
        self.assertEqual(before, schema.model_dump_json())
        external = LLMJsonSchemaFormat(name="External", json_schema={"type":"object", "$ref":"https://no-fetch.test/schema"})
        with self.assertRaises(LLMStructuredOutputError):
            response().validate_output(external)

    def test_json_object_format_rejects_scalar_output(self):
        """JSON object requests reject valid JSON of a different shape."""
        with self.assertRaises(LLMStructuredOutputError):
            response("[1,2]").validate_output(LLMJsonFormat())


class StructuredProviderTests(unittest.IsolatedAsyncioTestCase):
    provider = provider_tests.ProviderSDKTests.provider

    async def test_both_protocols_validate_structured_text_and_keep_error_kind(self):
        """Provider calls reject mismatched content without hiding its semantic error."""
        for protocol in ("responses", "chat"):
            from tests.test_agent_providers import chat_body
            reply = response_body(output=[output_message('{"count":"bad"}')]) if protocol == "responses" else chat_body({"role":"assistant", "content":'{"count":"bad"}'})
            provider, _, captured = self.provider(protocol, reply=reply)
            with self.assertRaises(LLMStructuredOutputError) as caught:
                await provider.ainvoke(request(output_options=LLMOutputOptions(text_format=LLMJsonSchemaFormat.from_type(Answer))))
            self.assertEqual(caught.exception.kind, "schema_mismatch")
            self.assertEqual(len(captured), 1)
            self.assertEqual(provider._active, 0)

    async def test_tool_calls_can_precede_the_structured_answer(self):
        """A Tool call response does not require the final answer's JSON text."""
        provider, _, _ = self.provider(reply=response_body(output=[{
            "type":"function_call", "call_id":"1", "name":"lookup", "arguments":"{}",
        }]))
        result = await provider.ainvoke(request(output_options=LLMOutputOptions(text_format=LLMJsonFormat())))
        self.assertEqual(result.finish_reason, "tool_calls")

    async def test_stream_validation_precedes_completed_chunk_and_closes_http(self):
        """Malformed terminal content never becomes a successful completed stream."""
        body = response_body(output=[output_message("not-json")])
        async def handle(req):
            event = {"type":"response.completed", "response":body}
            return httpx.Response(200, content=("data: " + json.dumps(event) + "\n\n").encode(), headers={"content-type":"text/event-stream"})
        provider, _, _ = self.provider(handler=handle)
        with self.assertRaises(LLMStructuredOutputError):
            async for chunk in provider.astream(request(output_options=LLMOutputOptions(text_format=LLMJsonFormat()))):
                self.assertNotEqual(chunk.type, "completed")
        self.assertEqual(provider._active, 0)

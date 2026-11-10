"""Deliverable answers and model observations exclude execution-only metadata."""

import json
from types import SimpleNamespace
import unittest

from autoagent.agent import (
    AgentResult, ImageContent, LLMMessage, LLMNativeData, LLMReasoning, LLMRequest,
    LLMResponse, LLMToolCall, LLMToolResult, LLMUsage, RefusalContent, RoundResult,
    TextContent, URLSource, finish_agent_round,
)
from autoagent.agent.providers.chat_completions import ChatCompletionsAdapter
from autoagent.agent.providers.responses import ResponsesAdapter
from autoagent.agent.workflow._events import _snapshot
from autoagent.agent.workflow.compaction import serialize_history
from autoagent.agent.workflow.policy import item_characters
from autoagent.agent.workflow.subagents import project_observation


class AgentPayloadTests(unittest.TestCase):
    def test_subagent_observations_preserve_arbitrary_business_results(self):
        """Empty lifecycle fields disappear without stripping keys from a custom Workflow's result."""
        business = {"usage": None, "native": "business data", "rounds": 2}
        raw = SimpleNamespace(handle=SimpleNamespace(invocation_id="child-1", workflow_id="custom"),
            status="completed", output=business, error=None, waits=())
        observed = project_observation(raw, "child")
        self.assertEqual(observed["result"], business)
        self.assertNotIn("error", observed)
        self.assertNotIn("waits", observed)

    def test_final_answer_preserves_order_media_citations_and_refusal(self):
        """Final deliverables retain user content without the final call's accounting or replay data."""
        native = LLMNativeData(provider_id="offline", protocol="openai.responses", payload={"replay": "internal"})
        citation = {"type": "url_citation", "url": "https://example.com/source"}
        response = LLMResponse(model="model", provider_response_id="response-1", finish_reason="stop",
            usage=LLMUsage(input_tokens=100, output_tokens=20, total_tokens=120),
            provider_metadata={"diagnostic": "internal"}, output=(
                LLMReasoning(text="internal reasoning", encrypted_content="ciphertext", native=native),
                LLMMessage(role="assistant", id="message-1", native=native, content=(
                    TextContent(text=" "), TextContent(text="Answer", annotations=(citation,), native=native),
                    ImageContent(source=URLSource(url="https://example.com/image.png"), native=native))),
                LLMMessage(role="assistant", content=(RefusalContent(text="No.", native=native),))))
        result = finish_agent_round(RoundResult(response=response, continue_loop=False, rounds=7))
        self.assertEqual(result.model_dump(mode="json"), {"content": [
            {"type": "text", "text": "Answer", "annotations": [citation]},
            {"type": "image", "source": {"type": "url", "url": "https://example.com/image.png"}},
            {"type": "refusal", "text": "No."}]})
        self.assertEqual(AgentResult.model_validate_json(result.model_dump_json()).text, "AnswerNo.")
        self.assertEqual(response.usage.total_tokens, 120)
        self.assertEqual(response.output[1].native, native)

    def test_protocol_and_compaction_inputs_keep_business_data_without_metadata(self):
        """Only Tool metadata is excluded; similarly named user JSON fields remain model-visible."""
        call = LLMToolCall(call_id="call-1", name="custom", arguments="{}")
        business = {"usage": 42, "model": "part-number", "native": "business value"}
        result = LLMToolResult(call_id=call.call_id, structured_content=business,
            tool_metadata={"diagnostic": "PRIVATE_LOG"})
        request = LLMRequest(model="model", input=(call, result))
        for adapter in (ChatCompletionsAdapter(), ResponsesAdapter()):
            wire = json.dumps(adapter.encode_request(request, provider_id="offline"))
            self.assertNotIn("PRIVATE_LOG", wire)
            self.assertIn("business value", wire)
            self.assertIn("part-number", wire)
        history = serialize_history((call, result))
        self.assertNotIn("PRIVATE_LOG", history)
        self.assertEqual(json.loads(history)[1]["structured_content"], business)
        self.assertEqual(item_characters(result), item_characters(result.model_copy(update={"tool_metadata": {}})))
        self.assertEqual(result.tool_metadata["diagnostic"], "PRIVATE_LOG")

    def test_tool_ui_snapshot_retains_diagnostics_without_changing_model_result(self):
        """Execution presentation retains timing and diffs while the model sees only observations."""
        call = LLMToolCall(call_id="call-1", name="write_file", arguments="{}")
        result = LLMToolResult(call_id=call.call_id, structured_content={"path": "a", "change": "created"},
            tool_metadata={"file": {"diff": "+hello", "diff_truncated": False, "private": "hidden"},
                "execution": {"wall_time_seconds": .1, "total_output_bytes": 5, "retained_start": 0}})
        view = _snapshot("response-1", call, result=result)
        self.assertEqual(view.result.structured_content["diff"], "+hello")
        self.assertEqual(view.result.structured_content["wall_time_seconds"], .1)
        self.assertNotIn("private", view.result.structured_content)
        self.assertEqual(result.structured_content, {"path": "a", "change": "created"})
        # Custom Tools may use these metadata namespaces for scalar values.
        scalar = result.model_copy(update={"tool_metadata": {"file": "custom"}})
        self.assertEqual(_snapshot("response-1", call, result=scalar).result.structured_content, result.structured_content)

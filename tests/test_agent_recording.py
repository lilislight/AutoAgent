"""Optional model-call recording without database coupling or RPC retries."""

import asyncio
import json
import unittest
from unittest.mock import patch

from pydantic import ValidationError

from autoagent.agent import (
    EmbeddingRequest, LLMCallContext, LLMCallRecord, LLMJsonFormat, LLMOutputOptions,
    LLMProviderError, LLMStructuredOutputError, llm_recording_context, runtime_llm_recorder,
)
from tests import test_agent_providers as providers
from tests import test_agent_embeddings as embeddings


class Recorder:
    def __init__(self):
        self.records = []

    async def record_llm_call(self, record):
        self.records.append(LLMCallRecord.model_validate(record))


@unittest.skipUnless(providers.AsyncOpenAI is not None, "Install autoagent[agent] for SDK transport tests")
class ProviderRecordingTests(unittest.IsolatedAsyncioTestCase):
    provider = providers.ProviderSDKTests.provider

    async def test_success_keeps_reported_usage_and_identifiers_without_payloads(self):
        """One successful Provider invocation has one stable start-to-terminal identity."""
        recorder = Recorder()
        provider, _, _ = self.provider(recorder=recorder)
        await provider.ainvoke(providers.request())
        start, end = recorder.records
        self.assertEqual((start.status, end.status), ("running", "succeeded"))
        self.assertEqual(start.llm_call_id, end.llm_call_id)
        self.assertEqual((end.input_tokens, end.output_tokens, end.total_tokens), (3, 2, 5))
        self.assertEqual(end.provider_request_id, "request_1")
        self.assertEqual(end.provider_response_id, "resp_1")
        self.assertTrue(end.usage_reported)
        self.assertIsNone(end.workflow_id)
        self.assertGreaterEqual(end.duration_ns, 0)
        self.assertNotIn("input", end.model_dump())
        self.assertNotIn("output", end.model_dump())

    async def test_without_recorder_no_call_records_are_constructed(self):
        """A standalone Provider does not allocate recording state or require storage."""
        provider, _, _ = self.provider()
        with patch("autoagent.agent.recording.LLMCallRecord", side_effect=AssertionError("Unexpected recording")):
            self.assertEqual((await provider.ainvoke(providers.request())).text, "Answer")

    async def test_chat_completion_uses_the_same_recording_contract(self):
        """Chat Completions retains actual protocol, token usage and provider identifiers."""
        recorder = Recorder()
        provider, _, _ = self.provider("chat", recorder=recorder)
        await provider.ainvoke(providers.request())
        row = recorder.records[-1]
        self.assertEqual(row.protocol, "openai.chat_completions")
        self.assertEqual(row.status, "succeeded")
        self.assertEqual(row.total_tokens, 5)
        self.assertEqual(row.provider_request_id, "request_1")

    async def test_billing_details_preserve_inclusive_cache_counts_without_payloads(self):
        """OpenAI-shaped cache extensions never add cached tokens to input a second time."""
        from autoagent.agent import LocalTokenPricing, TokenPrice, OpenAIProviderConfig
        recorder = Recorder()
        reply = providers.chat_body()
        reply["usage"] = {"prompt_tokens": 100, "completion_tokens": 2, "total_tokens": 102,
                          "cache_read_input_tokens": 60, "cache_creation_input_tokens": 10,
                          "prompt_tokens_details": {"cached_tokens": 60},
                          "unexpected_private_field": "not stored"}
        provider, _, captured = self.provider("chat", recorder=recorder, reply=reply)
        provider.config = OpenAIProviderConfig(provider_id="test", pricing_provider="vendor")
        await provider.ainvoke(providers.request())
        record = recorder.records[-1]
        pricing = LocalTokenPricing(overrides={"vendor/model": TokenPrice("1", "3", "0.2", "2")},
                                    use_litellm_catalog=False)
        self.assertEqual(pricing.calculate(record).units, 68_000_000)
        self.assertEqual(record.billing_details["pricing_provider"], "vendor")
        self.assertNotIn("unexpected_private_field", record.billing_details["usage"])
        self.assertNotIn("pricing_provider", captured[0][1])

    async def test_decode_failures_keep_the_known_remote_request_id(self):
        """A malformed response still records the request ID received in HTTP headers."""
        recorder = Recorder()
        provider, _, _ = self.provider(recorder=recorder, reply=providers.response_body(status="in_progress"))
        with self.assertRaises(LLMProviderError):
            await provider.ainvoke(providers.request())
        self.assertEqual(recorder.records[-1].status, "failed")
        self.assertEqual(recorder.records[-1].provider_request_id, "request_1")

    async def test_explicit_disable_skips_a_configured_runtime_recorder(self):
        """Recording can be disabled without changing model execution."""
        recorder = Recorder()
        provider, _, _ = self.provider(record_calls=False)
        with runtime_llm_recorder(recorder):
            await provider.ainvoke(providers.request())
        self.assertEqual(recorder.records, [])

    async def test_runtime_registration_reaches_tasks_created_before_lifespan(self):
        """Runtime-wide recording works when lifespan and invocation Tasks are siblings."""
        recorder = Recorder()
        provider, _, _ = self.provider()
        gate = asyncio.Event()
        async def invoke():
            await gate.wait()
            return await provider.ainvoke(providers.request())
        task = asyncio.create_task(invoke())
        with runtime_llm_recorder(recorder):
            gate.set()
            await task
        self.assertEqual(len(recorder.records), 2)
        await provider.ainvoke(providers.request())
        self.assertEqual(len(recorder.records), 2)

    async def test_concurrent_manual_contexts_cannot_mix_workflow_or_revision_ids(self):
        """Concurrent model calls retain independent standalone workflow associations."""
        recorder = Recorder()
        provider, _, _ = self.provider(recorder=recorder)
        async def invoke(identity):
            with llm_recording_context(LLMCallContext(workflow_id=identity, workflow_revision_id=identity + "-revision")):
                await provider.ainvoke(providers.request())
        await asyncio.gather(invoke("first"), invoke("second"))
        terminals = [record for record in recorder.records if record.status != "running"]
        self.assertEqual({(row.workflow_id, row.workflow_revision_id) for row in terminals},
                         {("first", "first-revision"), ("second", "second-revision")})
        self.assertEqual(len({row.llm_call_id for row in terminals}), 2)

    async def test_provider_error_preserves_exception_and_unknown_usage_without_retry(self):
        """Failures retain metadata while unreported token counts remain null."""
        recorder = Recorder()
        async def handle(request):
            return providers.httpx.Response(429, json={"error":{"message":"rate limited", "type":"rate_limit_error", "code":"rate_limit_exceeded"}}, headers={"x-request-id":"failed-request"})
        provider, _, calls = self.provider(recorder=recorder, handler=handle)
        with self.assertRaises(LLMProviderError):
            await provider.ainvoke(providers.request())
        end = recorder.records[-1]
        self.assertEqual(end.status, "failed")
        self.assertEqual(end.status_code, 429)
        self.assertEqual(end.provider_request_id, "failed-request")
        self.assertEqual(end.error_type, "RateLimitError")
        self.assertFalse(end.usage_reported)
        self.assertIsNone(end.total_tokens)
        self.assertEqual(len(calls), 1)

    async def test_local_structured_output_failure_retains_remote_usage(self):
        """Generated content validation failures retain tokens reported by the endpoint."""
        recorder = Recorder()
        provider, _, _ = self.provider(recorder=recorder)
        with self.assertRaises(LLMStructuredOutputError):
            await provider.ainvoke(providers.request(output_options=LLMOutputOptions(text_format=LLMJsonFormat())))
        end = recorder.records[-1]
        self.assertEqual(end.status, "failed")
        self.assertEqual(end.total_tokens, 5)
        self.assertEqual(end.error_code, "invalid_json")

    async def test_cancelled_call_records_terminal_cancellation_and_releases_admission(self):
        """Caller cancellation writes one cancelled terminal record before releasing resources."""
        recorder = Recorder()
        entered = asyncio.Event()
        async def handle(request):
            entered.set()
            await asyncio.Future()
        provider, _, _ = self.provider(recorder=recorder, handler=handle)
        task = asyncio.create_task(provider.ainvoke(providers.request()))
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(recorder.records[-1].status, "cancelled")
        self.assertIsNone(recorder.records[-1].total_tokens)
        self.assertEqual(provider._active, 0)

    async def test_cancelled_initial_write_never_dispatches_a_model_request(self):
        """Cancellation during initial recording does not leave a falsely running call or send RPCs."""
        entered = asyncio.Event()
        class BlockingRecorder(Recorder):
            async def record_llm_call(self, record):
                await super().record_llm_call(record)
                if record.status == "running":
                    entered.set()
                    await asyncio.Future()
        recorder = BlockingRecorder()
        provider, _, calls = self.provider(recorder=recorder)
        task = asyncio.create_task(provider.ainvoke(providers.request()))
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(recorder.records[-1].status, "cancelled")
        self.assertEqual(calls, [])
        self.assertEqual(provider._active, 0)

    async def test_storage_failure_does_not_turn_a_success_into_a_retryable_rpc_failure(self):
        """Optional recorder failures are observable without masking model success or repeating calls."""
        class Broken:
            async def record_llm_call(self, record):
                raise RuntimeError("Storage unavailable")
        provider, _, calls = self.provider(recorder=Broken())
        with self.assertLogs("autoagent.agent.recording", level="WARNING"):
            result = await provider.ainvoke(providers.request())
        self.assertEqual(result.text, "Answer")
        self.assertEqual(len(calls), 1)

    async def test_cancellation_during_terminal_write_settles_storage_without_repeating_rpc(self):
        """Cancellation waits for the terminal write and preserves known successful model usage."""
        entered, release = asyncio.Event(), asyncio.Event()
        class Delayed(Recorder):
            async def record_llm_call(self, record):
                if record.status != "running":
                    entered.set()
                    await release.wait()
                await super().record_llm_call(record)
        recorder = Delayed()
        provider, _, calls = self.provider(recorder=recorder)
        task = asyncio.create_task(provider.ainvoke(providers.request()))
        await entered.wait()
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(recorder.records[-1].status, "succeeded")
        self.assertEqual(recorder.records[-1].total_tokens, 5)
        self.assertEqual(len(calls), 1)
        self.assertEqual(provider._active, 0)

    async def test_stream_records_final_usage_once_and_early_close_as_cancellation(self):
        """Stream usage comes from its authoritative response and never from summing deltas."""
        recorder = Recorder()
        async def handle(request):
            events = [{"type":"response.output_text.delta", "delta":"Answer", "output_index":0, "content_index":0},
                      {"type":"response.completed", "response":providers.response_body()}]
            body = "".join("data: " + json.dumps(event) + "\n\n" for event in events)
            return providers.httpx.Response(200, content=body.encode(), headers={"content-type":"text/event-stream", "x-request-id":"stream-request"})
        provider, _, _ = self.provider(recorder=recorder, handler=handle)
        chunks = [chunk async for chunk in provider.astream(providers.request())]
        self.assertEqual(chunks[-1].type, "completed")
        self.assertEqual(recorder.records[-1].status, "succeeded")
        self.assertEqual(recorder.records[-1].total_tokens, 5)
        self.assertEqual(recorder.records[-1].provider_request_id, "stream-request")
        stream = provider.astream(providers.request())
        self.assertEqual((await anext(stream)).type, "delta")
        await stream.aclose()
        self.assertEqual(recorder.records[-1].status, "cancelled")
        self.assertEqual(recorder.records[-1].provider_request_id, "stream-request")
        self.assertIsNone(recorder.records[-1].total_tokens)
        self.assertEqual(provider._active, 0)

    async def test_close_after_terminal_chunk_keeps_completed_status(self):
        """Closing a stream after its completed event preserves reported terminal usage."""
        recorder = Recorder()
        async def handle(request):
            return providers.httpx.Response(200, content=("data: " + json.dumps({"type":"response.completed", "response":providers.response_body()}) + "\n\n").encode(), headers={"content-type":"text/event-stream"})
        provider, _, _ = self.provider(recorder=recorder, handler=handle)
        stream = provider.astream(providers.request())
        self.assertEqual((await anext(stream)).type, "completed")
        await stream.aclose()
        self.assertEqual(recorder.records[-1].status, "succeeded")


@unittest.skipUnless(providers.AsyncOpenAI is not None, "Install autoagent[agent] for SDK transport tests")
class EmbeddingRecordingTests(unittest.IsolatedAsyncioTestCase):
    provider = embeddings.EmbeddingSDKTests.provider

    async def test_embedding_records_usage_with_the_same_optional_recorder(self):
        """Embedding calls share storage semantics without inventing output-token measurements."""
        recorder = Recorder()
        provider, _, _ = self.provider(recorder=recorder)
        await provider.ainvoke(EmbeddingRequest(model="embedding-model", input="text"))
        end = recorder.records[-1]
        self.assertEqual(end.kind, "embedding")
        self.assertEqual(end.input_tokens, 3)
        self.assertIsNone(end.output_tokens)
        self.assertEqual(end.provider_request_id, "request-1")


class RecordContractTests(unittest.TestCase):
    def test_record_round_trips_through_core_and_rejects_invalid_lifecycle(self):
        """Model call metadata is portable, finite and explicit about lifecycle and missing usage."""
        from autoagent import ValueContract
        record = LLMCallRecord(llm_call_id="1", kind="generation", provider_id="test", protocol="test", requested_model="model", status="running", started_at_us=1)
        contract = ValueContract.create(LLMCallRecord, location="llm-call-record")
        self.assertEqual(contract.restore(contract.to_record(record)), record)
        for fields in ({"status":"succeeded"}, {"total_tokens":0}, {"duration_ns":1}):
            with self.subTest(fields=fields), self.assertRaises(ValidationError):
                LLMCallRecord.model_validate({**record.model_dump(mode="python"), **fields})

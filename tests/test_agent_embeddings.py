"""Embedding model boundaries and real SDK calls over a local HTTP transport."""

from __future__ import annotations

import asyncio
import base64
from contextlib import asynccontextmanager
import json
import struct
import unittest
from unittest.mock import patch

from pydantic import ValidationError

try:
    import httpx
    from openai import AsyncOpenAI
except ImportError:
    httpx = None
    AsyncOpenAI = None

from autoagent import AutoAgentApp, Node, ValueContract, Workflow
from autoagent.agent import (
    EmbeddingProviderError, EmbeddingRequest, EmbeddingResponse, EmbeddingUsage,
    EmbeddingVector, OpenAIEmbeddingAdapter, OpenAIEmbeddingProvider,
    OpenAIProviderConfig, OpenAIResponsesProvider, ProviderError,
)


def body(*vectors, usage=True):
    value = {
        "object": "list", "model": "embedding-model",
        "data": [
            {"object": "embedding", "index": index, "embedding": vector}
            for index, vector in enumerate(vectors or ([0.25, -0.5],))
        ],
    }
    if usage:
        value["usage"] = {"prompt_tokens": 3, "total_tokens": 3}
    return value


class EmbeddingModelTests(unittest.TestCase):
    def test_four_input_forms_round_trip_through_core(self):
        """Preserve text, text batches, tokens and token batches in Core contracts."""
        contract = ValueContract.create(EmbeddingRequest, location="embedding")
        for value, count in (("hello", 1), (("a", "b"), 2), ((1, 2), 1), (((1,), (2, 3)), 2)):
            with self.subTest(input=value):
                request = EmbeddingRequest(model="embedding-model", input=value)
                self.assertEqual(request.input_count, count)
                self.assertEqual(contract.restore(contract.to_record(request)), request)

    def test_invalid_inputs_and_parameters_are_rejected(self):
        """Reject empty or mixed batches, non-token numbers and invalid dimensions."""
        for value in ("", (), ("",), ((),), ("a", 1), (1, (2,)), (-1,), (True,), (1.5,)):
            with self.subTest(input=value), self.assertRaises(ValidationError):
                EmbeddingRequest(model="model", input=value)
        for options in ({"dimensions": 0}, {"dimensions": True}, {"encoding_format": "binary"}):
            with self.subTest(options=options), self.assertRaises(ValidationError):
                EmbeddingRequest(model="model", input="hello", **options)

    def test_response_vectors_and_usage_round_trip(self):
        """Keep normalized vectors, usage and request metadata serializable."""
        response = EmbeddingResponse(
            model="model", data=(EmbeddingVector(index=0, embedding=(0.25, -0.5)),),
            usage=EmbeddingUsage(input_tokens=3, total_tokens=3),
            provider_metadata={"_request_id": "request-1"},
        )
        contract = ValueContract.create(EmbeddingResponse, location="result")
        self.assertEqual(contract.restore(contract.to_record(response)), response)
        self.assertEqual(response.vectors, ((0.25, -0.5),))

    def test_vectors_must_be_finite_and_aligned(self):
        """Reject nonfinite, empty, duplicated or dimensionally inconsistent vectors."""
        for values in ((float("nan"),), (float("inf"),), (), (True,)):
            with self.subTest(vector=values), self.assertRaises(ValidationError):
                EmbeddingVector(index=0, embedding=values)
        for data in (
            (), (EmbeddingVector(index=1, embedding=(0.0,)),),
            (EmbeddingVector(index=0, embedding=(0.0,)), EmbeddingVector(index=0, embedding=(0.0,))),
            (EmbeddingVector(index=0, embedding=(0.0,)), EmbeddingVector(index=1, embedding=(0.0, 1.0))),
        ):
            with self.subTest(data=data), self.assertRaises(ValidationError):
                EmbeddingResponse(model="model", data=data)


@unittest.skipUnless(AsyncOpenAI is not None, "Install autoagent[agent] for SDK tests")
class EmbeddingSDKTests(unittest.IsolatedAsyncioTestCase):
    def provider(self, *, reply=None, handler=None, **options):
        captured = []

        async def handle(request):
            captured.append((str(request.url), json.loads(request.content)))
            if handler is not None:
                return await handler(request)
            return httpx.Response(200, json=body() if reply is None else reply, headers={"x-request-id": "request-1"})

        client = AsyncOpenAI(
            api_key="test", base_url="https://provider.test/v1", max_retries=0,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handle)),
        )
        # Keep unrelated OS discovery from creating a sandbox executor thread.
        client._platform = "Linux"
        provider = OpenAIEmbeddingProvider(OpenAIProviderConfig(provider_id="test"), client=client, **options)
        self.addAsyncCleanup(client.close)
        self.addAsyncCleanup(provider.aclose)
        return provider, client, captured

    async def test_sdk_wire_body_and_usage(self):
        """Send embeddings parameters to the real SDK endpoint and retain usage."""
        provider, _, captured = self.provider()
        response = await provider.ainvoke(EmbeddingRequest(
            model="embedding-model", input="hello", dimensions=2, user="user-1",
        ))
        self.assertEqual(captured, [("https://provider.test/v1/embeddings", {
            "model": "embedding-model", "input": "hello", "dimensions": 2,
            "encoding_format": "float", "user": "user-1",
        })])
        self.assertEqual(response.vectors, ((0.25, -0.5),))
        self.assertEqual(response.usage.input_tokens, 3)
        self.assertEqual(response.usage.total_tokens, 3)
        self.assertEqual(response.provider_metadata["_request_id"], "request-1")
        self.assertEqual(provider.protocol, "openai.embeddings")

    async def test_sdk_encodes_each_input_form(self):
        """Distinguish single token sequences from batches without tokenizing text."""
        for value, wire, count in (
            ("hello", "hello", 1), (("a", "b"), ["a", "b"], 2),
            ((1, 2), [1, 2], 1), (((1,), (2,)), [[1], [2]], 2),
        ):
            with self.subTest(input=value):
                provider, _, captured = self.provider(reply=body(*([[0.5, 1.0]] * count)))
                response = await provider.ainvoke(EmbeddingRequest(model="model", input=value))
                self.assertEqual(captured[0][1]["input"], wire)
                self.assertEqual(len(response.data), count)
                self.assertNotIn("dimensions", captured[0][1])

    async def test_base64_float32_decoding(self):
        """Normalize explicitly requested base64 vectors without NumPy or SDK coercion."""
        encoded = base64.b64encode(struct.pack("<ff", 0.25, -0.5)).decode()
        provider, _, captured = self.provider(reply=body(encoded))
        response = await provider.ainvoke(EmbeddingRequest(model="model", input="hi", encoding_format="base64"))
        self.assertEqual(captured[0][1]["encoding_format"], "base64")
        self.assertEqual(response.vectors, ((0.25, -0.5),))

    async def test_indices_restore_input_order(self):
        """Use returned indices to align a batch even if response items arrive reversed."""
        reply = body([0.0, 0.5], [1.0, 1.5])
        reply["data"].reverse()
        provider, _, _ = self.provider(reply=reply)
        response = await provider.ainvoke(EmbeddingRequest(model="model", input=("first", "second")))
        self.assertEqual(response.vectors, ((0.0, 0.5), (1.0, 1.5)))

    async def test_missing_usage_remains_unknown(self):
        """Do not invent token measurements for a compatible server omitting usage."""
        provider, _, _ = self.provider(reply=body(usage=False))
        response = await provider.ainvoke(EmbeddingRequest(model="model", input="hi"))
        self.assertIsNone(response.usage)

    async def test_malformed_responses_raise_protocol_errors(self):
        """Reject missing vectors, corrupt encoding and dimensions before publishing output."""
        bad_payloads = [
            {**body(), "data": []}, body("not-base64!"),
            body(base64.b64encode(b"abc").decode()), body(""),
            body(base64.b64encode(struct.pack("<f", float("nan"))).decode()),
            body([True, 0.5]), body([0.5]),
            {**body(), "object": "response"},
        ]
        out_of_range = body()
        out_of_range["data"][0]["index"] = 1
        bad_payloads.append(out_of_range)
        duplicate = body([0.5, 1.0], [0.5, 1.0])
        duplicate["data"][1]["index"] = 0
        for reply in bad_payloads:
            with self.subTest(reply=reply):
                provider, _, _ = self.provider(reply=reply)
                with self.assertRaises(EmbeddingProviderError) as caught:
                    await provider.ainvoke(EmbeddingRequest(model="model", input="hi", dimensions=2))
                self.assertFalse(caught.exception.retryable)
                self.assertEqual(caught.exception.request_id, "request-1")
                self.assertEqual(provider._active, 0)
        provider, _, _ = self.provider(reply=duplicate)
        with self.assertRaises(EmbeddingProviderError):
            await provider.ainvoke(EmbeddingRequest(model="model", input=("a", "b")))

    async def test_rate_limit_and_timeout_are_retryable_without_hidden_retries(self):
        """Retain endpoint identity and request IDs while making exactly one attempt."""
        async def rate_limit(_request):
            return httpx.Response(429, json={"error": {"message": "limited", "type": "rate_limit_error"}}, headers={"x-request-id": "limited-1"})

        provider, _, captured = self.provider(handler=rate_limit)
        with self.assertRaises(EmbeddingProviderError) as caught:
            await provider.ainvoke(EmbeddingRequest(model="model", input="hi"))
        self.assertIsInstance(caught.exception, ProviderError)
        self.assertEqual(caught.exception.status_code, 429)
        self.assertEqual(caught.exception.request_id, "limited-1")
        self.assertEqual(caught.exception.protocol, "openai.embeddings")
        self.assertEqual(caught.exception.provider_id, "test")
        self.assertTrue(caught.exception.retryable)
        self.assertEqual(len(captured), 1)

        async def timeout(request):
            raise httpx.ReadTimeout("timed out", request=request)

        provider, _, captured = self.provider(handler=timeout)
        with self.assertRaises(EmbeddingProviderError) as caught:
            await provider.ainvoke(EmbeddingRequest(model="model", input="hi"))
        self.assertTrue(caught.exception.retryable)
        self.assertEqual(len(captured), 1)

    async def test_cancellation_releases_active_call(self):
        """Preserve cancellation and prevent shutdown while an HTTP request is active."""
        entered = asyncio.Event()
        gate = asyncio.Event()

        async def pending(_request):
            entered.set()
            await gate.wait()
            return httpx.Response(200, json=body())

        provider, _, _ = self.provider(handler=pending)
        task = asyncio.create_task(provider.ainvoke(EmbeddingRequest(model="model", input="hi")))
        await entered.wait()
        with self.assertRaises(RuntimeError):
            await provider.aclose()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(provider._active, 0)
        await provider.aclose()

    async def test_owned_client_lifespan_and_borrowed_client_sharing(self):
        """Create owned clients lazily and leave a shared injected SDK client open."""
        borrowed, client, _ = self.provider()
        generator = OpenAIResponsesProvider(client=client)
        await borrowed.aclose()
        await generator.aclose()
        self.assertFalse(client.is_closed())

        actual = AsyncOpenAI(api_key="test", http_client=httpx.AsyncClient(transport=httpx.MockTransport(lambda _request: httpx.Response(200, json=body()))))
        actual._platform = "Linux"
        owned = OpenAIEmbeddingProvider(OpenAIProviderConfig(api_key="test"))
        with patch("openai.AsyncOpenAI", return_value=actual) as factory:
            factory.assert_not_called()
            async with owned:
                await owned.ainvoke(EmbeddingRequest(model="model", input="hi"))
            self.assertEqual(factory.call_args.kwargs["max_retries"], 0)
        self.assertTrue(actual.is_closed())
        await owned.aclose()
        with self.assertRaises(RuntimeError):
            await owned.ainvoke(EmbeddingRequest(model="model", input="hi"))

    async def test_adapter_extension_adds_vendor_fields(self):
        """Change one vendor's request encoding while reusing SDK transport."""
        class VendorAdapter(OpenAIEmbeddingAdapter):
            def encode_request(self, request, *, provider_id):
                return {**super().encode_request(request, provider_id=provider_id), "extra_body": {"input_type": "document"}}

        provider, _, captured = self.provider(adapter=VendorAdapter())
        await provider.ainvoke(EmbeddingRequest(model="model", input="hi"))
        self.assertEqual(captured[0][1]["input_type"], "document")

    async def test_embedding_node_uses_core_lifespan(self):
        """Run and close embedding resources on Core's loop without storing SDK objects."""
        provider, client, _ = self.provider()

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
            result = await app.ainvoke(
                Workflow("embedding", nodes=[Node("embed", provider.ainvoke)]),
                EmbeddingRequest(model="model", input="hi"),
            )
            self.assertEqual(result.status, "completed")
            restored = ValueContract.create(EmbeddingResponse, location="output").restore(result.output)
            self.assertEqual(restored.vectors, ((0.25, -0.5),))
            self.assertIsNot(provider._loop, asyncio.get_running_loop())
        finally:
            await app.aclose()
        self.assertTrue(client.is_closed())


if __name__ == "__main__":
    unittest.main()

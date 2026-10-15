"""OpenAI Embeddings protocol and SDK-backed Provider."""

from __future__ import annotations

import base64
import struct
from typing import TYPE_CHECKING

from ..models import (
    EmbeddingRequest, EmbeddingResponse, EmbeddingUsage, EmbeddingVector,
)
from ._common import raw_object
from ._openai_client import OpenAIClientOwner
from .base import EmbeddingProvider, EmbeddingProviderError
from .config import OpenAIProviderConfig
from ..recording import start_llm_call, finish_llm_call

if TYPE_CHECKING:
    from openai import AsyncOpenAI
    from ..recording import LLMCallRecorder


class OpenAIEmbeddingAdapter:
    """Stateless encoder/decoder; subclasses can handle vendor variations."""

    protocol = "openai.embeddings"

    def encode_request(self, request: EmbeddingRequest, *, provider_id: str) -> dict:
        # Passing an explicit format also prevents implicit SDK base64 decoding.
        return request.model_dump(mode="json", exclude_none=True)

    def decode_response(
        self, raw: dict, *, provider_id: str, request: EmbeddingRequest,
    ) -> EmbeddingResponse:
        if raw.get("object") != "list":
            raise ValueError("Expected an OpenAI Embeddings response list.")
        data = raw.get("data")
        if not isinstance(data, list) or len(data) != request.input_count:
            raise ValueError("Embedding response must contain one vector per input.")
        vectors = []
        for item in data:
            if not isinstance(item, dict) or item.get("object") != "embedding":
                raise ValueError("Expected an embedding item.")
            value = item.get("embedding")
            if isinstance(value, str):
                binary = base64.b64decode(value, validate=True)
                if not binary or len(binary) % 4:
                    raise ValueError("Base64 embeddings must contain complete float32 values.")
                vector = struct.unpack(f"<{len(binary) // 4}f", binary)
            elif isinstance(value, list):
                vector = tuple(value)
            else:
                raise ValueError("Expected a float array or base64 embedding.")
            if request.dimensions is not None and len(vector) != request.dimensions:
                raise ValueError("Embedding dimensions do not match the request.")
            vectors.append(EmbeddingVector(index=item.get("index"), embedding=vector))
        reported_usage = raw.get("usage")
        usage = None
        if reported_usage is not None:
            usage = EmbeddingUsage(
                input_tokens=reported_usage.get("prompt_tokens"),
                total_tokens=reported_usage.get("total_tokens"), native=reported_usage,
            )
        return EmbeddingResponse(
            model=raw.get("model"),
            data=tuple(sorted(vectors, key=lambda item: item.index)),
            usage=usage,
            provider_metadata={
                key: value for key, value in raw.items()
                if key not in {"model", "data", "usage"}
            },
        )


class OpenAIEmbeddingProvider(OpenAIClientOwner, EmbeddingProvider):
    """One bounded embeddings call with the same client ownership as LLM calls.

    Own clients are lazy and have SDK retries disabled. Injected clients remain
    caller-owned and may be shared with generation Providers on the same loop.
    """

    error_type = EmbeddingProviderError

    def __init__(
        self, config: OpenAIProviderConfig | None = None, *,
        adapter: OpenAIEmbeddingAdapter | None = None,
        client: AsyncOpenAI | None = None,
        recorder: LLMCallRecorder | None = None, record_calls: bool = True,
    ) -> None:
        super().__init__(config, client=client, recorder=recorder, record_calls=record_calls)
        self.adapter = adapter or OpenAIEmbeddingAdapter()
        if self.adapter.protocol != "openai.embeddings":
            raise ValueError("OpenAIEmbeddingProvider requires an Embeddings adapter.")

    @property
    def protocol(self) -> str:
        return self.adapter.protocol

    async def ainvoke(self, request: EmbeddingRequest) -> EmbeddingResponse:
        self._bind_loop()
        self._active += 1
        request_id = None
        recording = response = failure = None
        try:
            request = EmbeddingRequest.model_validate(request)
            recording = await start_llm_call(self, request, kind="embedding")
            params = self.adapter.encode_request(request, provider_id=self.provider_id)
            # The SDK's typed Embedding currently declares only list[float].
            # Read raw JSON to retain explicit base64 output and reject values
            # (such as bools) before SDK coercion can hide protocol errors.
            value = await self._get_client().embeddings.with_raw_response.create(**params)
            request_id = value.headers.get("x-request-id")
            raw = raw_object(value.http_response.json())
            if request_id is not None:
                raw["_request_id"] = request_id
            response = EmbeddingResponse.model_validate(self.adapter.decode_response(
                raw, provider_id=self.provider_id, request=request,
            ))
            return response
        except BaseException as error:
            failure = error if isinstance(error, (EmbeddingProviderError, ImportError)) or not isinstance(error, Exception) else self._error(error)
            if isinstance(failure, EmbeddingProviderError) and failure.request_id is None:
                failure.request_id = request_id
            if failure is error:
                raise
            raise failure from error
        finally:
            try:
                await finish_llm_call(recording, response=response, error=failure, request_id=request_id)
            finally:
                self._active -= 1


__all__ = ["OpenAIEmbeddingAdapter", "OpenAIEmbeddingProvider"]

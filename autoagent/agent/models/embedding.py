"""Serializable embedding requests and normalized floating point vectors."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field, JsonValue, model_validator

from ._base import Model, NonEmpty


EmbeddingText = Annotated[str, Field(min_length=1)]
EmbeddingToken = Annotated[int, Field(ge=0)]
EmbeddingTokens = Annotated[tuple[EmbeddingToken, ...], Field(min_length=1)]
EmbeddingInput = (
    EmbeddingText
    | Annotated[tuple[EmbeddingText, ...], Field(min_length=1)]
    | EmbeddingTokens
    | Annotated[tuple[EmbeddingTokens, ...], Field(min_length=1)]
)


class EmbeddingProviderOptions(Model):
    """JSON API options scoped to an embedding connection, never SDK controls."""

    protocol: Literal["openai.embeddings"] = "openai.embeddings"
    provider_id: NonEmpty | None = None
    values: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _reserved_fields(self):
        from .llm import _RESERVED_OPTIONS

        reserved = _RESERVED_OPTIONS | {"dimensions", "encoding_format", "user"}
        overlap = reserved.intersection(self.values)
        if overlap:
            raise ValueError("Embedding options cannot override normalized fields or SDK controls: "
                             + ", ".join(sorted(overlap)))
        return self


class EmbeddingRequest(Model):
    """One text/token sequence or a homogeneous batch, using OpenAI semantics.

    Token IDs belong to the explicitly selected model's tokenizer. Model token
    limits and supported dimensions remain the endpoint's responsibility.
    encoding_format selects the wire representation, not the response model.
    """

    model: NonEmpty
    input: EmbeddingInput
    dimensions: int | None = Field(default=None, gt=0)
    encoding_format: Literal["float", "base64"] = "float"
    user: str | None = None
    provider_options: EmbeddingProviderOptions | None = None

    @property
    def input_count(self) -> int:
        if isinstance(self.input, str) or isinstance(self.input[0], int):
            return 1
        return len(self.input)


class EmbeddingVector(Model):
    """Vector and its original input index, independent of wire encoding."""

    index: int = Field(ge=0)
    embedding: tuple[float, ...] = Field(min_length=1)


class EmbeddingUsage(Model):
    input_tokens: int | None = Field(default=None, ge=0)
    total_tokens: int | None = Field(default=None, ge=0)
    native: dict[str, JsonValue] = Field(default_factory=dict)


class EmbeddingResponse(Model):
    """Nonempty, equally sized vectors ordered by their original input index."""

    model: NonEmpty
    data: tuple[EmbeddingVector, ...] = Field(min_length=1)
    usage: EmbeddingUsage | None = None
    provider_metadata: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _aligned_vectors(self) -> EmbeddingResponse:
        if tuple(item.index for item in self.data) != tuple(range(len(self.data))):
            raise ValueError("Embedding indices must be contiguous and ordered from zero.")
        if len({len(item.embedding) for item in self.data}) != 1:
            raise ValueError("Embedding vectors must have the same dimensions.")
        return self

    @property
    def vectors(self) -> tuple[tuple[float, ...], ...]:
        return tuple(item.embedding for item in self.data)


__all__ = [
    "EmbeddingInput", "EmbeddingProviderOptions", "EmbeddingRequest", "EmbeddingResponse",
    "EmbeddingUsage", "EmbeddingVector",
]

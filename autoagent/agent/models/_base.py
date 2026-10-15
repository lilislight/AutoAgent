"""Shared validation for the Agent's portable data models."""

from __future__ import annotations

import math
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator


NonEmpty = Annotated[str, Field(min_length=1, pattern=r"\S")]
OpenAIProtocol = Literal["openai.responses", "openai.chat_completions"]


class Model(BaseModel):
    """Frozen fields with strict, finite, JSON-compatible values.

    JSON dictionaries remain ordinary dictionaries. Runtime contract boundaries
    validate them again and detach their contents before storing Runtime State.
    """

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        validate_default=True,
        revalidate_instances="always",
        allow_inf_nan=False,
    )

    @field_validator("*", mode="after")
    @classmethod
    def _finite_containers(cls, value: object) -> object:
        # JsonValue also accepts floats; prohibit NaN/Infinity inside extensions
        # and schemas rather than silently serializing them as null.
        _check_finite(value)
        return value


class StructuredContentModel(Model):
    """Retain the distinction between absent data and an explicit JSON null.

    The presence flag survives default-inclusive Runtime serialization; relying
    on Pydantic's fields_set alone would lose this distinction after restore.
    """

    structured_content: JsonValue | None = None
    structured_content_present: bool = False

    @model_validator(mode="after")
    def _consistent_presence(self):
        if "structured_content_present" not in self.model_fields_set:
            object.__setattr__(self, "structured_content_present", "structured_content" in self.model_fields_set)
            self.__pydantic_fields_set__.add("structured_content_present")
        if self.structured_content is not None and not self.structured_content_present:
            raise ValueError("Non-null structured content must be marked present.")
        return self


def _check_finite(value: object) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("JSON numbers must be finite.")
    if isinstance(value, dict):
        for item in value.values():
            _check_finite(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _check_finite(item)

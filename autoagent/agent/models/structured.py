"""Local structured output validation, independent of transport and retries."""

from __future__ import annotations

from copy import deepcopy
import json
from typing import TYPE_CHECKING, TypeVar

from pydantic import TypeAdapter, ValidationError

from ._base import _check_finite
from .content import RefusalContent
from .items import LLMMessage

if TYPE_CHECKING:
    from .llm import LLMResponse, LLMTextOutputFormat

T = TypeVar("T")


class LLMStructuredOutputError(ValueError):
    """A refused, incomplete or invalid generated value; no automatic repair."""

    def __init__(self, message: str, *, kind: str, response: LLMResponse):
        super().__init__(message)
        self.kind = kind
        self.response = response


def _failure(response, kind, message):
    return LLMStructuredOutputError(message, kind=kind, response=response)


def _object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"Duplicate JSON property: {key!r}.")
        value[key] = item
    return value


def response_json(response: LLMResponse):
    """Parse complete assistant text and reject duplicate/nonfinite JSON values."""
    from .llm import LLMResponse

    response = LLMResponse.model_validate(response)
    if any(isinstance(part, RefusalContent) for item in response.output
           if isinstance(item, LLMMessage) for part in item.content):
        raise _failure(response, "refusal", "The model refused the requested output.")
    if response.finish_reason != "stop":
        kind = "truncated" if response.finish_reason == "length" else response.finish_reason
        raise _failure(response, kind, "Structured output requires a complete text response.")
    if not response.text.strip():
        raise _failure(response, "no_text", "The response contains no structured text.")
    try:
        value = json.loads(response.text, object_pairs_hook=_object)
        _check_finite(value)
    except (ValueError, RecursionError) as error:
        raise _failure(response, "invalid_json", "The response is not valid finite JSON.") from error
    return value


def parse_response(response: LLMResponse, annotation: type[T] | object, *, strict: bool = True) -> T:
    """Compile a caller-owned annotation and validate generated JSON locally."""
    response_json(response)
    adapter = TypeAdapter(annotation)
    try:
        return adapter.validate_json(response.text, strict=strict)
    except ValidationError as error:
        raise _failure(response, "schema_mismatch", "The response does not match the requested type.") from error


def validate_response(response: LLMResponse, text_format: LLMTextOutputFormat):
    """Validate a declared JSON format without fetching external schema references."""
    from .llm import LLMJsonSchemaFormat, LLMTextFormat

    if isinstance(text_format, LLMTextFormat):
        return None
    value = response_json(response)
    if not isinstance(value, dict):
        raise _failure(response, "schema_mismatch", "Structured output must be a JSON object.")
    if isinstance(text_format, LLMJsonSchemaFormat):
        from jsonschema.validators import validator_for
        from referencing import Registry
        from referencing.exceptions import NoSuchResource

        def deny_remote(uri):
            raise NoSuchResource(ref=uri)

        schema = deepcopy(text_format.json_schema)
        validator_type = validator_for(schema)
        validator_type.check_schema(schema)
        try:
            validator_type(schema, registry=Registry(retrieve=deny_remote)).validate(value)
        except Exception as error:
            raise _failure(response, "schema_mismatch", "The response does not match its JSON Schema.") from error
    return value


__all__ = ["LLMStructuredOutputError", "parse_response", "validate_response"]

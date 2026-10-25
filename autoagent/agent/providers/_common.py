"""Shared protocol validation, media encoding and response metadata."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
import hashlib
import json
from typing import Annotated

from pydantic import JsonValue, TypeAdapter, WithJsonSchema

from ..models import (
    AssetSource, DataSource, LLMJsonSchemaFormat, LLMNativeData, LLMRequest,
    LLMUsage, ProviderSource, URLSource,
)
from .base import LLMProviderError, LLMUnsupportedRequest


def raw_object(value: object) -> dict:
    if isinstance(value, Mapping):
        return deepcopy(dict(value))
    dump = getattr(value, "model_dump", None)
    if not callable(dump):
        raise ValueError("SDK returned an unsupported response object.")
    return dump(mode="json")


def native_fields(value: dict, known: set[str], provider_id: str, protocol: str) -> LLMNativeData | None:
    payload = {key: deepcopy(field) for key, field in value.items() if key not in known and field is not None}
    return LLMNativeData(provider_id=provider_id, protocol=protocol, payload=payload) if payload else None


def replay_digest(value: object) -> str:
    """Detect changes to normalized replay content without retaining a second copy."""
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def check_native(native: LLMNativeData | None, provider_id: str, protocol: str) -> dict:
    if native is None:
        return {}
    if native.provider_id != provider_id or native.protocol != protocol:
        unsupported("Native data belongs to another provider or protocol.", provider_id, protocol)
    return deepcopy(native.payload)


def unsupported(message: str, provider_id: str, protocol: str) -> None:
    raise LLMUnsupportedRequest(message, provider_id=provider_id, protocol=protocol)


def source_value(source, provider_id: str, protocol: str) -> tuple[str, str]:
    if isinstance(source, URLSource):
        return "url", source.url
    if isinstance(source, DataSource):
        return "data", f"data:{source.mime_type};base64,{source.data}"
    if isinstance(source, ProviderSource):
        if source.provider_id != provider_id:
            unsupported("File reference belongs to another provider connection.", provider_id, protocol)
        return "id", source.file_id
    if isinstance(source, AssetSource):
        unsupported("AssetSource requires a media resolver.", provider_id, protocol)
    raise TypeError("Invalid media source.")


def api_schema(schema: dict, *, strict: bool | None) -> dict:
    """Use the SDK's strict schema compiler on a detached JSON Schema.

    The request stores schema data, not a Python annotation. WithJsonSchema
    lets the SDK consume that data through its TypeAdapter support. Keep this
    SDK helper dependency localized and covered by real wire-body tests.
    """
    schema = deepcopy(schema)
    if strict is True:
        from openai.lib._pydantic import to_strict_json_schema

        adapter = TypeAdapter(Annotated[JsonValue, WithJsonSchema(schema)])
        return to_strict_json_schema(adapter)
    return schema


def function_schema(tool) -> dict:
    result = tool.model_dump(exclude_none=True)
    result["parameters"] = api_schema(tool.parameters, strict=tool.strict)
    return result


def text_format(value) -> dict:
    if isinstance(value, LLMJsonSchemaFormat):
        return {"type": "json_schema", "name": value.name, "schema": api_schema(value.json_schema, strict=value.strict), "strict": value.strict}
    return {"type": value.type}


def common_parameters(request: LLMRequest, protocol: str, provider_id: str) -> dict:
    require_model(request, provider_id, protocol)
    result = {"model": request.model}
    for key in ("temperature", "top_p"):
        value = getattr(request, key)
        if value is not None:
            result[key] = value
    if request.provider_options is not None:
        if request.provider_options.provider_id not in {None, provider_id}:
            unsupported("Provider options target another connection.", provider_id, protocol)
        if request.provider_options.protocol != protocol:
            unsupported("Provider options target another protocol.", provider_id, protocol)
        values = deepcopy(request.provider_options.values)
        if values.get("background"):
            unsupported("Background jobs are not bounded LLM calls.", provider_id, protocol)
        result["extra_body"] = values
    return result


def require_model(request: LLMRequest, provider_id: str, protocol: str) -> None:
    if request.model is None:
        unsupported("Resolve a model before calling a Provider.", provider_id, protocol)


def usage(value, *, chat: bool) -> LLMUsage | None:
    if value is None:
        return None
    input_details = value.get("prompt_tokens_details" if chat else "input_tokens_details") or {}
    output_details = value.get("completion_tokens_details" if chat else "output_tokens_details") or {}
    native = deepcopy(value)
    for key in (("prompt_tokens", "completion_tokens", "total_tokens") if chat else
                ("input_tokens", "output_tokens", "total_tokens")):
        native.pop(key, None)
    for key, mapped in (("prompt_tokens_details" if chat else "input_tokens_details", "cached_tokens"),
                        ("completion_tokens_details" if chat else "output_tokens_details", "reasoning_tokens")):
        if isinstance(native.get(key), dict):
            native[key] = {name: count for name, count in native[key].items() if name != mapped and count is not None}
            if not native[key]:
                native.pop(key)
    native = {key: field for key, field in native.items() if field is not None}
    return LLMUsage(
        input_tokens=value.get("prompt_tokens" if chat else "input_tokens"),
        output_tokens=value.get("completion_tokens" if chat else "output_tokens"),
        total_tokens=value.get("total_tokens"),
        reasoning_tokens=output_details.get("reasoning_tokens"),
        cache_read_tokens=input_details.get("cached_tokens"), native=native,
    )


def remote_failure(raw: dict, provider_id: str, protocol: str) -> None:
    error = raw.get("error") or {}
    message = error.get("message", "Remote model execution failed.") if isinstance(error, dict) else str(error)
    raise LLMProviderError(
        message, provider_id=provider_id, protocol=protocol,
        retryable=None, request_id=raw.get("_request_id"),
    )

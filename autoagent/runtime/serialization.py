from __future__ import annotations

import json
import math
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


_TYPE_TAG = "__autoagent_type__"
_ARTIFACT_VIEW_TAG = "__autoagent_artifact__"


class RuntimeSerializationError(ValueError):
    """Raised when runtime data has no explicitly safe persistence encoding."""


class RuntimeDeserializationError(ValueError):
    """Raised when persisted data needs an unavailable registered type/codec."""


class ArtifactRef(BaseModel):
    """Serializable reference to data stored outside RuntimeStore JSON columns.

    Images, files, byte buffers, and large model payloads should be placed in an
    artifact store and represented in runtime input/output/context by this model.
    RuntimeStore persists the reference; observation UIs may render it without
    loading the artifact bytes.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    uri: str = Field(description="Stable artifact-store URI or key.")
    media_type: str | None = Field(default=None, description="Optional MIME type.")
    size_bytes: int | None = Field(default=None, ge=0)
    sha256: str | None = Field(default=None, description="Optional content digest.")
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("uri")
    @classmethod
    def validate_uri(cls, value: str) -> str:
        resolved = value.strip()
        if not resolved:
            raise ValueError("ArtifactRef uri cannot be empty.")
        return resolved


@dataclass(frozen=True)
class RuntimeCodec:
    """Explicit trusted codec used for one custom Python type."""

    type_id: str
    python_type: type[Any]
    encode: Callable[[Any], Any]
    decode: Callable[[Any], Any]


class RuntimeSerializer(ABC):
    """Persistence serializer shared by database Store and observation views."""

    @abstractmethod
    def dumps(self, value: Any) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def loads(self, payload: bytes | str) -> Any:
        raise NotImplementedError

    @abstractmethod
    def json_view(self, payload: bytes | str) -> Any:
        """Return a type-neutral JSON value suitable for APIs and tracing UIs."""

        raise NotImplementedError


class JsonRuntimeSerializer(RuntimeSerializer):
    """Safe JSON-plus serializer; it never imports types or executes pickle.

    Built-ins, UUID/time/Decimal, ArtifactRef, tuples/sets, registered custom
    codecs, and Pydantic models are supported. Pydantic classes seen by ``dumps``
    are registered in this serializer instance automatically. A fresh process
    must register those classes before ``loads`` so recovery cannot import and
    instantiate arbitrary persisted type names. ``json_view`` remains available
    without those registrations for observation endpoints.
    """

    def __init__(self, *, max_inline_bytes: int | None = 1_048_576) -> None:
        if max_inline_bytes is not None and max_inline_bytes <= 0:
            raise ValueError("max_inline_bytes must be positive or None.")
        self.max_inline_bytes = max_inline_bytes
        self._codecs_by_id: dict[str, RuntimeCodec] = {}
        self._codecs_by_type: dict[type[Any], RuntimeCodec] = {}
        self._models: dict[str, type[BaseModel]] = {}

    def register_codec(self, codec: RuntimeCodec) -> None:
        """Register one stable codec; duplicate type ids/types are rejected."""

        if not codec.type_id.strip():
            raise ValueError("RuntimeCodec type_id cannot be empty.")
        if codec.type_id in self._codecs_by_id:
            raise ValueError(f"Runtime codec already registered: {codec.type_id}")
        if codec.python_type in self._codecs_by_type:
            raise ValueError(
                f"Runtime codec already registered for: {codec.python_type.__name__}"
            )
        self._codecs_by_id[codec.type_id] = codec
        self._codecs_by_type[codec.python_type] = codec

    def register_pydantic_model(
        self,
        model_type: type[BaseModel],
        *,
        type_id: str | None = None,
    ) -> str:
        """Allow a persisted Pydantic value to recover to its original class."""

        resolved_id = type_id or _python_type_id(model_type)
        existing = self._models.get(resolved_id)
        if existing is not None and existing is not model_type:
            raise ValueError(f"Pydantic runtime type id already used: {resolved_id}")
        self._models[resolved_id] = model_type
        return resolved_id

    def dumps(self, value: Any) -> bytes:
        try:
            encoded = self._encode(value)
            payload = json.dumps(
                encoded,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            ).encode("utf-8")
        except RuntimeSerializationError:
            raise
        except (TypeError, ValueError) as exc:
            raise RuntimeSerializationError(str(exc)) from exc
        if self.max_inline_bytes is not None and len(payload) > self.max_inline_bytes:
            raise RuntimeSerializationError(
                "Runtime value exceeds max_inline_bytes; persist it externally and "
                "store an ArtifactRef instead."
            )
        return payload

    def loads(self, payload: bytes | str) -> Any:
        try:
            raw = payload.decode("utf-8") if isinstance(payload, bytes) else payload
            return self._decode(json.loads(raw))
        except RuntimeDeserializationError:
            raise
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            raise RuntimeDeserializationError(str(exc)) from exc

    def json_view(self, payload: bytes | str) -> Any:
        """Decode only transport tags; custom values remain their JSON payload."""

        try:
            raw = payload.decode("utf-8") if isinstance(payload, bytes) else payload
            return self._json_view(json.loads(raw))
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            raise RuntimeDeserializationError(str(exc)) from exc

    def _encode(self, value: Any) -> Any:
        if value is None or isinstance(value, (bool, int, str)):
            return value
        if isinstance(value, float):
            if not math.isfinite(value):
                raise RuntimeSerializationError("Non-finite floats are not supported.")
            return value
        if isinstance(value, UUID):
            return {_TYPE_TAG: "uuid", "value": str(value)}
        if isinstance(value, datetime):
            return {_TYPE_TAG: "datetime", "value": value.isoformat()}
        if isinstance(value, date):
            return {_TYPE_TAG: "date", "value": value.isoformat()}
        if isinstance(value, time):
            return {_TYPE_TAG: "time", "value": value.isoformat()}
        if isinstance(value, Decimal):
            return {_TYPE_TAG: "decimal", "value": str(value)}
        if isinstance(value, bytes):
            raise RuntimeSerializationError(
                "Raw bytes are not persisted inline; store an ArtifactRef."
            )
        if isinstance(value, ArtifactRef):
            return {
                _TYPE_TAG: "artifact",
                "value": self._encode(value.model_dump(mode="python")),
            }

        codec = self._codecs_by_type.get(type(value))
        if codec is not None:
            return {
                _TYPE_TAG: "codec",
                "type_id": codec.type_id,
                "value": self._encode(codec.encode(value)),
            }
        if isinstance(value, BaseModel):
            type_id = self.register_pydantic_model(type(value))
            return {
                _TYPE_TAG: "pydantic",
                "type_id": type_id,
                "value": self._encode(value.model_dump(mode="python")),
            }
        if isinstance(value, Mapping):
            if any(not isinstance(key, str) for key in value):
                raise RuntimeSerializationError("Runtime mapping keys must be strings.")
            encoded = {key: self._encode(item) for key, item in value.items()}
            if _TYPE_TAG in encoded:
                return {_TYPE_TAG: "mapping", "value": encoded}
            return encoded
        if isinstance(value, list):
            return [self._encode(item) for item in value]
        if isinstance(value, tuple):
            return {_TYPE_TAG: "tuple", "value": [self._encode(item) for item in value]}
        if isinstance(value, (set, frozenset)):
            items = [self._encode(item) for item in value]
            items.sort(
                key=lambda item: json.dumps(
                    item,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                )
            )
            return {_TYPE_TAG: "set", "value": items}
        raise RuntimeSerializationError(
            f"Unsupported runtime value: {type(value).__module__}.{type(value).__qualname__}"
        )

    def _decode(self, value: Any) -> Any:
        if isinstance(value, list):
            return [self._decode(item) for item in value]
        if not isinstance(value, dict):
            return value
        type_tag = value.get(_TYPE_TAG)
        if type_tag is None:
            return {key: self._decode(item) for key, item in value.items()}
        if type_tag == "mapping":
            return {key: self._decode(item) for key, item in value["value"].items()}
        if type_tag == "uuid":
            return UUID(value["value"])
        if type_tag == "datetime":
            return datetime.fromisoformat(value["value"])
        if type_tag == "date":
            return date.fromisoformat(value["value"])
        if type_tag == "time":
            return time.fromisoformat(value["value"])
        if type_tag == "decimal":
            return Decimal(value["value"])
        if type_tag == "artifact":
            return ArtifactRef.model_validate(self._decode(value["value"]))
        if type_tag == "tuple":
            return tuple(self._decode(item) for item in value["value"])
        if type_tag == "set":
            return set(self._decode(item) for item in value["value"])
        if type_tag == "codec":
            type_id = value["type_id"]
            codec = self._codecs_by_id.get(type_id)
            if codec is None:
                raise RuntimeDeserializationError(
                    f"Runtime codec is not registered: {type_id}"
                )
            return codec.decode(self._decode(value["value"]))
        if type_tag == "pydantic":
            type_id = value["type_id"]
            model_type = self._models.get(type_id)
            if model_type is None:
                raise RuntimeDeserializationError(
                    "Pydantic runtime type is not registered in this process: "
                    f"{type_id}"
                )
            return model_type.model_validate(self._decode(value["value"]))
        raise RuntimeDeserializationError(f"Unknown runtime type tag: {type_tag}")

    def _json_view(self, value: Any) -> Any:
        if isinstance(value, list):
            return [self._json_view(item) for item in value]
        if not isinstance(value, dict):
            return value
        type_tag = value.get(_TYPE_TAG)
        if type_tag is None:
            return {key: self._json_view(item) for key, item in value.items()}
        if type_tag == "artifact":
            return {_ARTIFACT_VIEW_TAG: self._json_view(value["value"])}
        if type_tag in {"mapping", "pydantic", "codec"}:
            return self._json_view(value["value"])
        if type_tag in {"tuple", "set"}:
            return [self._json_view(item) for item in value["value"]]
        if type_tag in {"uuid", "datetime", "date", "time", "decimal"}:
            return value["value"]
        raise RuntimeDeserializationError(f"Unknown runtime type tag: {type_tag}")


def _python_type_id(model_type: type[Any]) -> str:
    return f"{model_type.__module__}:{model_type.__qualname__}"

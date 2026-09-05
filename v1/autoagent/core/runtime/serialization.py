from __future__ import annotations

import json
import math
from abc import ABC, abstractmethod
from collections.abc import Mapping
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator


_TYPE_TAG = "__autoagent_type__"
_ARTIFACT_VIEW_TAG = "__autoagent_artifact__"


class RuntimeSerializationError(ValueError):
    """Raised when runtime data has no explicitly safe persistence encoding."""


class RuntimeDeserializationError(ValueError):
    """Raised when persisted runtime JSON is malformed."""


class ArtifactRef(BaseModel):
    """One immutable reference to framework-owned or user-owned artifact data.

    ``runtime_value`` refs are hydrated transparently during recovery. Explicit
    ``artifact`` refs remain references so user code and observation clients can
    choose when to load their content. Semantic kind and physical storage are
    separate because either kind may move to a remote artifact service later.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: UUID = Field(default_factory=uuid4)
    kind: Literal["runtime_value", "artifact"] = "artifact"
    storage: Literal["database", "external"] = "external"
    uri: str | None = Field(
        default=None,
        description="External URI; database artifacts are addressed by id.",
    )
    media_type: str | None = Field(default=None, description="Optional MIME type.")
    encoding: str | None = None
    size_bytes: int | None = Field(default=None, ge=0)
    sha256: str | None = Field(default=None, description="Optional content digest.")
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_storage(self) -> ArtifactRef:
        if self.storage == "external":
            if self.uri is None or not self.uri.strip():
                raise ValueError("External ArtifactRef requires a non-empty uri.")
        elif self.uri is not None and not self.uri.strip():
            raise ValueError("ArtifactRef uri cannot be empty.")
        if self.kind == "runtime_value" and self.storage != "database":
            raise ValueError("runtime_value ArtifactRef must use database storage.")
        return self


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
    """Safe JSON serializer for Workflow values and framework-owned scalar tags.

    User Pydantic values are stored as JSON objects, never as importable Python
    type ids. Executable reads restore concrete values from registered Workflow
    contracts; observation reads remain type-neutral.
    """

    def __init__(self, *, max_inline_bytes: int | None = None) -> None:
        if max_inline_bytes is not None and max_inline_bytes <= 0:
            raise ValueError("max_inline_bytes must be positive or None.")
        self.max_inline_bytes = max_inline_bytes

    def dumps(self, value: Any) -> bytes:
        return self._dumps(value, enforce_limit=True)

    def dumps_unchecked(self, value: Any) -> bytes:
        """Encode one value without applying the inline persistence limit.

        Database artifact externalization uses this to measure and store a
        candidate before the smaller ArtifactRef-bearing envelope is encoded.
        """

        return self._dumps(value, enforce_limit=False)

    def _dumps(self, value: Any, *, enforce_limit: bool) -> bytes:
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
        if (
            enforce_limit
            and self.max_inline_bytes is not None
            and len(payload) > self.max_inline_bytes
        ):
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

    def normalize_json_value(self, value: Any) -> Any:
        """Normalize a dynamic Workflow value without losing ArtifactRefs."""

        try:
            return self._dynamic_json_view(self._encode(value))
        except RuntimeSerializationError:
            raise
        except (TypeError, ValueError) as exc:
            raise RuntimeSerializationError(str(exc)) from exc

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
        if isinstance(value, Enum):
            return self._encode(value.value)
        if isinstance(value, bytes):
            raise RuntimeSerializationError(
                "Raw bytes are not persisted inline; store an ArtifactRef."
            )
        if isinstance(value, ArtifactRef):
            return {
                _TYPE_TAG: "artifact",
                "value": self._encode(value.model_dump(mode="python")),
            }

        if isinstance(value, BaseModel):
            return self._encode_model_fields(value)
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

    def _encode_model_fields(self, value: BaseModel) -> Any:
        """Encode a Pydantic value as type-neutral JSON field data."""

        if type(value).__pydantic_root_model__:
            return self._encode(value.root)
        return {
            _pydantic_persistence_key(name, field_info): self._encode(
                getattr(value, name)
            )
            for name, field_info in type(value).model_fields.items()
        }

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
        if type_tag == "mapping":
            return self._json_view(value["value"])
        if type_tag in {"tuple", "set"}:
            return [self._json_view(item) for item in value["value"]]
        if type_tag in {"uuid", "datetime", "date", "time", "decimal"}:
            return value["value"]
        raise RuntimeDeserializationError(f"Unknown runtime type tag: {type_tag}")

    def _dynamic_json_view(self, value: Any) -> Any:
        if isinstance(value, list):
            return [self._dynamic_json_view(item) for item in value]
        if not isinstance(value, dict):
            return value
        type_tag = value.get(_TYPE_TAG)
        if type_tag is None:
            return {
                key: self._dynamic_json_view(item)
                for key, item in value.items()
            }
        if type_tag == "artifact":
            return ArtifactRef.model_validate(self._decode(value["value"]))
        if type_tag == "mapping":
            return self._dynamic_json_view(value["value"])
        if type_tag in {"tuple", "set"}:
            return [self._dynamic_json_view(item) for item in value["value"]]
        if type_tag in {"uuid", "datetime", "date", "time", "decimal"}:
            return value["value"]
        raise RuntimeSerializationError(f"Unknown runtime type tag: {type_tag}")


_WORKFLOW_VALUE_SERIALIZER = JsonRuntimeSerializer()


def _pydantic_persistence_key(name: str, field_info: Any) -> str:
    validation_alias = field_info.validation_alias
    if isinstance(validation_alias, str):
        return validation_alias
    if isinstance(field_info.alias, str):
        return field_info.alias
    return name


def ensure_serializable_value(value: Any) -> Any:
    """Reject a typed Workflow value that cannot be persisted safely."""

    _WORKFLOW_VALUE_SERIALIZER.dumps_unchecked(value)
    return value


def normalize_json_value(value: Any) -> Any:
    """Return the canonical JSON value allowed at dynamic Workflow boundaries."""

    return _WORKFLOW_VALUE_SERIALIZER.normalize_json_value(value)

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from hashlib import sha256
from typing import Any
from uuid import UUID, uuid4

from autoagent.core.runtime.serialization import (
    ArtifactRef,
    JsonRuntimeSerializer,
)
from autoagent.core.runtime.time import utc_timestamp_ms


@dataclass(frozen=True)
class ArtifactPolicy:
    """Controls automatic persistence externalization of large runtime values."""

    enabled: bool = True
    inline_max_bytes: int = 8 * 1024

    def __post_init__(self) -> None:
        if self.inline_max_bytes < 1:
            raise ValueError("inline_max_bytes must be positive.")


@dataclass(frozen=True)
class EncodedArtifact:
    id: UUID
    owner_invocation_id: UUID
    kind: str
    storage: str
    uri: str | None
    media_type: str | None
    encoding: str | None
    size_bytes: int
    sha256: str
    payload: bytes
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at_ms: int = field(default_factory=utc_timestamp_ms)

    def ref(self) -> ArtifactRef:
        return ArtifactRef(
            id=self.id,
            kind="runtime_value",
            storage="database",
            media_type=self.media_type,
            encoding=self.encoding,
            size_bytes=self.size_bytes,
            sha256=self.sha256,
            metadata=self.metadata,
        )


class RuntimeArtifactEncoder:
    """Externalize large subtrees and deduplicate them within one Invocation."""

    def __init__(
        self,
        serializer: JsonRuntimeSerializer,
        policy: ArtifactPolicy,
    ) -> None:
        self.serializer = serializer
        self.policy = policy
        self._known: dict[tuple[UUID, str], ArtifactRef] = {}

    def externalize(
        self,
        value: Any,
        *,
        invocation_id: UUID,
        preserve_root: bool = False,
    ) -> tuple[Any, tuple[EncodedArtifact, ...]]:
        if not self.policy.enabled:
            return value, ()
        artifacts: dict[UUID, EncodedArtifact] = {}
        transformed = self._externalize_value(
            value,
            invocation_id=invocation_id,
            artifacts=artifacts,
            preserve_container=preserve_root,
        )
        return transformed, tuple(artifacts.values())

    def remember(self, ref: ArtifactRef, *, invocation_id: UUID) -> None:
        if ref.sha256 is not None and ref.kind == "runtime_value":
            self._known[(invocation_id, ref.sha256)] = ref

    def forget_invocation(self, invocation_id: UUID) -> None:
        self._known = {
            key: ref
            for key, ref in self._known.items()
            if key[0] != invocation_id
        }

    def _externalize_value(
        self,
        value: Any,
        *,
        invocation_id: UUID,
        artifacts: dict[UUID, EncodedArtifact],
        preserve_container: bool = False,
    ) -> Any:
        if isinstance(value, ArtifactRef):
            return value
        if isinstance(value, Mapping):
            candidate = {
                key: self._externalize_value(
                    item,
                    invocation_id=invocation_id,
                    artifacts=artifacts,
                )
                for key, item in value.items()
            }
            if preserve_container:
                return candidate
            return self._externalize_candidate(
                candidate,
                invocation_id=invocation_id,
                artifacts=artifacts,
            )
        if isinstance(value, list):
            candidate = [
                self._externalize_value(
                    item,
                    invocation_id=invocation_id,
                    artifacts=artifacts,
                )
                for item in value
            ]
            if preserve_container:
                return candidate
            return self._externalize_candidate(
                candidate,
                invocation_id=invocation_id,
                artifacts=artifacts,
            )
        if isinstance(value, tuple):
            candidate = tuple(
                self._externalize_value(
                    item,
                    invocation_id=invocation_id,
                    artifacts=artifacts,
                )
                for item in value
            )
            if preserve_container:
                return candidate
            return self._externalize_candidate(
                candidate,
                invocation_id=invocation_id,
                artifacts=artifacts,
            )
        return self._externalize_candidate(
            value,
            invocation_id=invocation_id,
            artifacts=artifacts,
        )

    def _externalize_candidate(
        self,
        value: Any,
        *,
        invocation_id: UUID,
        artifacts: dict[UUID, EncodedArtifact],
    ) -> Any:
        if _obviously_inline(value, self.policy.inline_max_bytes):
            return value
        encoded = self.serializer.dumps_unchecked(value)
        if len(encoded) <= self.policy.inline_max_bytes:
            return value
        digest = sha256(encoded).hexdigest()
        known = self._known.get((invocation_id, digest))
        if known is not None:
            return known
        artifact = EncodedArtifact(
            id=uuid4(),
            owner_invocation_id=invocation_id,
            kind="runtime_value",
            storage="database",
            uri=None,
            media_type="application/json",
            encoding="autoagent-json",
            size_bytes=len(encoded),
            sha256=digest,
            payload=encoded,
        )
        ref = artifact.ref()
        self._known[(invocation_id, digest)] = ref
        artifacts[artifact.id] = artifact
        return ref


def _obviously_inline(value: Any, limit: int) -> bool:
    """Cheap upper-bound check that avoids JSON encoding ordinary small data."""

    return _estimated_json_size(value, limit) <= limit


def _estimated_json_size(value: Any, limit: int) -> int:
    if value is None or isinstance(value, (bool, int, float)):
        return 32
    if isinstance(value, str):
        if value.isascii() and all(
            character not in {'"', "\\"} and ord(character) >= 32
            for character in value
        ):
            return min(limit + 1, len(value) + 2)
        if len(value) <= limit // 12:
            return len(value) * 12 + 2
        return limit + 1
    if isinstance(value, UUID):
        return 64
    if isinstance(value, ArtifactRef):
        return 768
    if isinstance(value, Mapping):
        total = 2
        for key, item in value.items():
            if not isinstance(key, str):
                return limit + 1
            total += _estimated_json_size(key, limit) + 2
            total += _estimated_json_size(item, limit)
            if total > limit:
                return limit + 1
        return total
    if isinstance(value, (list, tuple, set, frozenset)):
        total = 2
        for item in value:
            total += _estimated_json_size(item, limit) + 1
            if total > limit:
                return limit + 1
        return total
    return limit + 1

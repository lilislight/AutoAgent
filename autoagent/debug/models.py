from __future__ import annotations

from typing import Any, Generic, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, model_validator


DebugSourceKind = Literal["server", "database"]
BoundaryKind = Literal[
    "invocation",
    "node",
    "edge",
    "operator_call",
    "phase",
    "wait",
    "recovery",
]


class _DebugModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ValueSummary(_DebugModel):
    """Bounded description of one Runtime value.

    ``preview`` is deliberately optional. Large values remain addressable through
    ``detail_ref`` without copying their complete serialized representation into
    an Invocation Report.
    """

    type: str = Field(min_length=1)
    preview: Any | None = None
    shape: dict[str, Any] = Field(default_factory=dict)
    serialized_bytes: int | None = Field(default=None, ge=0)
    digest: str | None = None
    artifact_ref: str | None = None
    detail_ref: str | None = None
    redacted: bool = False


class ReportError(_DebugModel):
    """Bounded structured error recorded by the Runtime."""

    code: str = Field(min_length=1)
    message: str = Field(min_length=1)
    exception_type: str | None = None
    detail_ref: str | None = None


class PrimaryBoundary(_DebugModel):
    """Most precise recorded boundary useful as an investigation starting point."""

    kind: BoundaryKind
    subject_id: str = Field(min_length=1)
    sequence: int | None = Field(default=None, ge=1)
    node_id: str | None = None
    workflow_path: tuple[str, ...] = ()
    status: str | None = None
    message: str | None = None


class EvidenceWarning(_DebugModel):
    """Explicit limitation in the evidence behind a Report."""

    code: str = Field(min_length=1)
    message: str = Field(min_length=1)
    detail: dict[str, Any] = Field(default_factory=dict)


class InvocationReport(_DebugModel):
    """Compact, sequence-bounded diagnostic index for one Invocation."""

    schema_version: int = Field(default=1, ge=1)
    source: DebugSourceKind
    invocation_id: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    workflow_id: str = Field(min_length=1)
    workflow_revision_id: str = Field(min_length=1)
    workflow_version: str | int | None = None
    event_mode: Literal["minimal", "standard", "full"]
    execution_mode: str
    state: str
    entry_node_id: str | None = None
    created_at_ms: int = Field(ge=0)
    updated_at_ms: int = Field(ge=0)
    observed_sequence: int = Field(ge=0)
    durable_sequence: int = Field(ge=0)
    observed_user_event_sequence: int = Field(default=0, ge=0)
    durable_user_event_sequence: int = Field(default=0, ge=0)
    persistence_status: str
    user_event_persistence_status: str
    input: ValueSummary
    result: ValueSummary | None = None
    error: ReportError | None = None
    primary_boundary: PrimaryBoundary | None = None
    node_execution_count: int = Field(default=0, ge=0)
    edge_evaluation_count: int = Field(default=0, ge=0)
    operator_call_count: int = Field(default=0, ge=0)
    retry_count: int = Field(default=0, ge=0)
    fallback_count: int = Field(default=0, ge=0)
    timeout_count: int = Field(default=0, ge=0)
    wait_count: int = Field(default=0, ge=0)
    recovery_count: int = Field(default=0, ge=0)
    user_event_counts: dict[str, int] = Field(default_factory=dict)
    available_evidence: tuple[str, ...] = ()
    warnings: tuple[EvidenceWarning, ...] = ()

    @model_validator(mode="after")
    def validate_sequence_boundaries(self) -> InvocationReport:
        if self.durable_sequence > self.observed_sequence:
            raise ValueError("durable_sequence cannot exceed observed_sequence.")
        if self.durable_user_event_sequence > self.observed_user_event_sequence:
            raise ValueError(
                "durable_user_event_sequence cannot exceed "
                "observed_user_event_sequence."
            )
        if any(value < 0 for value in self.user_event_counts.values()):
            raise ValueError("UserEvent counts cannot be negative.")
        return self


T = TypeVar("T")


class DebugPage(_DebugModel, Generic[T]):
    """Stable keyset page fixed to one observed Runtime sequence."""

    through_sequence: int = Field(ge=0)
    items: tuple[T, ...] = ()
    next_cursor: str | None = None
    has_more: bool = False

    @model_validator(mode="after")
    def validate_cursor(self) -> DebugPage[T]:
        if self.has_more and self.next_cursor is None:
            raise ValueError("A page with more items requires next_cursor.")
        return self

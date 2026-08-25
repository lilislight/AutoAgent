"""Stable JSON response values for the read-only Tracing API."""

from __future__ import annotations

from typing import Any, Literal, TypeVar

from pydantic import BaseModel, ConfigDict

from autoagent.hosting import Page


TRACING_API_VERSION = 1
_JS_SAFE_INTEGER = (1 << 53) - 1


def tracing_record(value: dict[str, object]) -> dict[str, object]:
    """Make arbitrary integers lossless in JSON and stringify Runtime clocks."""

    converted = _json_safe_integers(value)
    assert isinstance(converted, dict)
    _stringify_timestamps(
        converted,
        "created_at_ns",
        "updated_at_ns",
        "ended_at_ns",
        "occurred_at_ns",
    )
    return converted


def tracing_state_record(value: dict[str, object]) -> dict[str, object]:
    """Make State JSON lossless while preserving safe user integer values."""

    converted = _json_safe_integers(value)
    assert isinstance(converted, dict)
    value = converted
    session = value.get("session")
    if isinstance(session, dict):
        _stringify_timestamps(session, "created_at_ns", "updated_at_ns")
    invocation = value.get("invocation")
    if not isinstance(invocation, dict):
        return value
    _stringify_timestamps(
        invocation,
        "created_at_ns",
        "started_at_ns",
        "completed_at_ns",
    )
    scheduler = invocation.get("scheduler")
    if not isinstance(scheduler, dict):
        return value
    for collection, fields in (
        ("occurrences", ("started_at_ns", "completed_at_ns")),
        ("operator_calls", ("started_at_ns", "completed_at_ns")),
        ("waits", ("created_at_ns", "resumed_at_ns")),
    ):
        records = scheduler.get(collection)
        if isinstance(records, dict):
            for record in records.values():
                if isinstance(record, dict):
                    _stringify_timestamps(record, *fields)
    return value


def _json_safe_integers(value: object) -> object:
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int):
        return str(value) if abs(value) > _JS_SAFE_INTEGER else value
    if isinstance(value, dict):
        return {key: _json_safe_integers(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe_integers(item) for item in value]
    return value


def _stringify_timestamps(record: dict[str, object], *fields: str) -> None:
    for field in fields:
        item = record.get(field)
        if isinstance(item, int) and not isinstance(item, bool):
            record[field] = str(item)


class _ResponseModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def to_record(self) -> dict[str, object]:
        return self.model_dump(mode="json")


class HealthResponse(_ResponseModel):
    """Tracing API health response."""

    status: str
    api_version: int = TRACING_API_VERSION


class WorkflowSummaryResponse(_ResponseModel):
    workflow_id: str
    workflow_version: str
    workflow_revision_id: str
    definition_hash: str
    created_at_ns: str


class WorkflowDefinitionResponse(_ResponseModel):
    schema_version: int
    workflow_id: str
    workflow_version: str
    workflow_revision_id: str
    definition_hash: str
    definition: dict[str, object]


class SessionSummaryResponse(_ResponseModel):
    session_id: str
    root_session_id: str
    current_invocation_id: str | None
    invocation_count: int
    workflow_id: str | None
    workflow_revision_id: str | None
    status: str | None
    parent_session_id: str | None
    parent_invocation_id: str | None
    creation_id: str | None
    unit_index: int | None
    created_at_ns: str
    updated_at_ns: str


class InvocationSummaryResponse(_ResponseModel):
    invocation_id: str
    session_id: str
    root_session_id: str
    workflow_id: str
    workflow_revision_id: str
    entry_node_id: str
    status: str
    parent_session_id: str | None
    parent_invocation_id: str | None
    creation_id: str | None
    unit_index: int | None
    first_event_sequence: int
    last_event_sequence: int
    created_at_ns: str
    updated_at_ns: str
    ended_at_ns: str | None


class ChildSessionSummaryResponse(_ResponseModel):
    session_id: str
    root_session_id: str
    parent_session_id: str
    parent_invocation_id: str
    creation_id: str
    unit_index: int
    parent_occurrence_id: str
    mode: Literal["await", "spawn"]
    planned_workflow_id: str
    planned_workflow_revision_id: str
    planned_invocation_id: str
    planned_event_sequence: int
    phase: Literal["planned", "opened", "accepted", "terminal"]
    current_invocation_id: str | None
    invocation_count: int
    workflow_id: str | None
    workflow_revision_id: str | None
    status: str
    created_at_ns: str | None
    updated_at_ns: str | None


class TraceEventResponse(_ResponseModel):
    schema_version: int
    id: str
    session_id: str
    trace_sequence: int
    kind: str
    occurred_at_ns: str
    invocation_id: str | None
    causation_id: str | None
    state_version: int | None
    subject_ids: dict[str, str]
    status: str | None
    error: dict[str, object] | None
    metrics: Any
    attributes: dict[str, object]


_ItemModel = TypeVar("_ItemModel", bound=_ResponseModel)


class _PageResponse(_ResponseModel):
    """Stable keyset page envelope shared by concrete item DTOs."""

    next_cursor: str | None = None
    has_more: bool = False

    @classmethod
    def _page_fields(
        cls,
        page: Page[dict[str, object]],
        item_model: type[_ItemModel],
    ) -> dict[str, object]:
        return {
            "items": [
                item_model.model_validate(tracing_record(item))
                for item in page.items
            ],
            "next_cursor": page.next_cursor,
            "has_more": page.next_cursor is not None,
        }


class WorkflowPageResponse(_PageResponse):
    items: list[WorkflowSummaryResponse]

    @classmethod
    def from_page(cls, page: Page[dict[str, object]]) -> "WorkflowPageResponse":
        return cls(**cls._page_fields(page, WorkflowSummaryResponse))


class SessionPageResponse(_PageResponse):
    items: list[SessionSummaryResponse]

    @classmethod
    def from_page(cls, page: Page[dict[str, object]]) -> "SessionPageResponse":
        return cls(**cls._page_fields(page, SessionSummaryResponse))


class InvocationPageResponse(_PageResponse):
    items: list[InvocationSummaryResponse]

    @classmethod
    def from_page(cls, page: Page[dict[str, object]]) -> "InvocationPageResponse":
        return cls(**cls._page_fields(page, InvocationSummaryResponse))


class ChildSessionPageResponse(_PageResponse):
    items: list[ChildSessionSummaryResponse]

    @classmethod
    def from_page(cls, page: Page[dict[str, object]]) -> "ChildSessionPageResponse":
        return cls(**cls._page_fields(page, ChildSessionSummaryResponse))


class InvocationStateResponse(_ResponseModel):
    """Reducer-built State at one Invocation boundary."""

    invocation_id: str
    session_id: str
    through_sequence: int
    state: dict[str, object]


class TracePageResponse(_ResponseModel):
    """Forward page or bounded tail of safe Trace Events."""

    items: list[TraceEventResponse]
    next_cursor: str | None
    resume_cursor: str | None
    resume_sequence: int
    has_more: bool
    has_earlier: bool


class StreamEndResponse(_ResponseModel):
    """Terminal SSE marker after all Trace Events are delivered."""

    invocation_id: str
    status: str
    resume_cursor: str | None
    resume_sequence: int

__all__ = [
    "ChildSessionPageResponse",
    "HealthResponse",
    "InvocationPageResponse",
    "InvocationStateResponse",
    "InvocationSummaryResponse",
    "SessionPageResponse",
    "StreamEndResponse",
    "TRACING_API_VERSION",
    "TracePageResponse",
    "WorkflowDefinitionResponse",
    "WorkflowPageResponse",
    "tracing_record",
    "tracing_state_record",
]

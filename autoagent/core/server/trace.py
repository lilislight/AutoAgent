from __future__ import annotations

import base64
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass
import json
from typing import Any
from uuid import UUID

from autoagent.core.app import AutoAgentApp
from autoagent.core.compiler import (
    WorkflowVersionSnapshot,
    workflow_revision_id,
)
from autoagent.core.runtime import (
    Invocation,
    RuntimeEvent,
    Session,
    UserEvent,
    apply_state_operations,
)

_TIMELINE_OPERATOR_CALL_LIMIT = 50


@dataclass(frozen=True)
class TracePage:
    items: list[dict[str, Any]]
    next_cursor: str | None
    has_more: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "items": self.items,
            "next_cursor": self.next_cursor,
            "has_more": self.has_more,
        }


def _node_operator_summary(
    execution: dict[str, Any] | None,
) -> dict[str, Any]:
    if execution is None:
        return {
            "operator_call_count": 0,
            "failed_operator_call_count": 0,
            "retry_count": 0,
            "fallback_count": 0,
            "timeout_count": 0,
            "parallel_call_count": 0,
            "streaming_call_count": 0,
            "stream_chunk_count": 0,
            "latest_operator_kind": None,
        }
    calls = execution.get("operator_calls", ())
    latest = calls[-1] if calls else None
    parallel_summary = execution.get("parallel_summary") or {}
    return {
        "operator_call_count": int(
            execution.get("operator_call_count", 0)
        ),
        "failed_operator_call_count": int(
            execution.get("failed_operator_call_count", 0)
        ),
        "retry_count": int(execution.get("retry_count", 0)),
        "fallback_count": int(execution.get("fallback_count", 0)),
        "timeout_count": int(execution.get("timeout_count", 0)),
        "parallel_call_count": int(parallel_summary.get("call_count", 0)),
        "streaming_call_count": int(execution.get("streaming_call_count", 0)),
        "stream_chunk_count": int(execution.get("stream_chunk_count", 0)),
        "latest_operator_kind": (
            latest.get("kind") if latest is not None else None
        ),
    }


class TraceProjectionReducer:
    """Pure graph/read-model reducer shared semantically with the UI."""

    schema_version = 5

    @classmethod
    def initial(cls, invocation_id: UUID | str) -> dict[str, Any]:
        return {
            "schema_version": cls.schema_version,
            "invocation_id": str(invocation_id),
            "through_sequence": 0,
            "invocation_state": "created",
            "nodes": {},
            "node_executions": {},
            "edges": {},
            "active_waits": {},
            "latest_phase": None,
        }

    @classmethod
    def apply(
        cls,
        projection: dict[str, Any],
        event: RuntimeEvent,
    ) -> dict[str, Any]:
        if event.sequence <= int(projection["through_sequence"]):
            return projection
        result = deepcopy(projection)
        result["through_sequence"] = event.sequence
        name = event.event_name
        payload = event.payload

        if name.startswith("invocation."):
            result["invocation_state"] = event.status or name.rsplit(".", 1)[-1]
            return result

        if name.startswith("node."):
            node_id = str(payload.get("node_id") or event.subject_id)
            execution_id = payload.get("node_execution_id")
            state = event.status or name.rsplit(".", 1)[-1]
            executions = result["node_executions"]
            if execution_id is not None:
                execution_id = str(execution_id)
                previous = executions.get(execution_id, {})
                operator_summary = payload.get("operator_summary") or {}
                executions[execution_id] = {
                    "execution_id": execution_id,
                    "node_id": node_id,
                    "sequence": int(
                        previous.get(
                            "sequence",
                            sum(
                                1
                                for value in executions.values()
                                if value.get("node_id") == node_id
                            )
                            + 1,
                        )
                    ),
                    "first_event_sequence": int(
                        previous.get("first_event_sequence", event.sequence)
                    ),
                    "state": state,
                    "error": payload.get("error"),
                    "started_at_ms": (
                        event.occurred_at_ms
                        if state == "running"
                        else previous.get("started_at_ms")
                    ),
                    "ended_at_ms": (
                        event.occurred_at_ms
                        if state
                        in {
                            "completed",
                            "failed",
                            "cancelled",
                            "interrupted",
                            "skipped",
                        }
                        else None
                    ),
                    "elapsed_ns": event.elapsed_ns,
                    "timing": dict(event.timing),
                    "operator_call_count": int(
                        operator_summary.get(
                            "attempt_count",
                            previous.get("operator_call_count", 0),
                        )
                    ),
                    "failed_operator_call_count": int(
                        (
                            int(operator_summary.get("failure_count", 0))
                            + int(operator_summary.get("interrupted_count", 0))
                        )
                        if operator_summary
                        else previous.get("failed_operator_call_count", 0)
                    ),
                    "retry_count": int(
                        operator_summary.get(
                            "retry_count", previous.get("retry_count", 0)
                        )
                    ),
                    "fallback_count": int(
                        operator_summary.get(
                            "fallback_count", previous.get("fallback_count", 0)
                        )
                    ),
                    "timeout_count": int(
                        operator_summary.get(
                            "timeout_count", previous.get("timeout_count", 0)
                        )
                    ),
                    "streaming_call_count": int(
                        operator_summary.get(
                            "streaming_call_count",
                            previous.get("streaming_call_count", 0),
                        )
                    ),
                    "stream_chunk_count": int(
                        operator_summary.get(
                            "stream_chunk_count",
                            previous.get("stream_chunk_count", 0),
                        )
                    ),
                    "parallel_summary": (
                        payload.get("parallel_summary")
                        if payload.get("parallel_summary") is not None
                        else previous.get("parallel_summary")
                    ),
                    "operator_calls": list(previous.get("operator_calls", ())),
                }
            node_executions = [
                value
                for value in executions.values()
                if value["node_id"] == node_id
            ]
            latest = max(
                node_executions,
                key=lambda value: (
                    int(value["sequence"]),
                    str(value["execution_id"]),
                ),
                default=None,
            )
            previous_node = result["nodes"].get(node_id, {})
            skipped_count = int(previous_node.get("skipped_count", 0)) + int(
                name == "node.skipped"
            )
            result["nodes"][node_id] = {
                "node_id": node_id,
                # A synthetic skipped Node Instance must not erase an earlier
                # real execution of the same static Node (notably in loops).
                "state": latest["state"] if latest is not None else state,
                "latest_occurrence_state": state,
                "latest_occurrence_sequence": event.sequence,
                "latest_skipped_instance_key": (
                    payload.get("node_instance_key")
                    if name == "node.skipped"
                    else previous_node.get("latest_skipped_instance_key")
                ),
                "latest_execution_id": (
                    latest["execution_id"] if latest is not None else None
                ),
                "execution_count": len(node_executions),
                "skipped_count": skipped_count,
                "latest_error": (
                    latest.get("error") if latest is not None else payload.get("error")
                ),
                "latest_elapsed_ns": (
                    latest.get("elapsed_ns") if latest is not None else event.elapsed_ns
                ),
                "latest_timing": (
                    dict(latest.get("timing", {}))
                    if latest is not None
                    else dict(event.timing)
                ),
                **_node_operator_summary(latest),
            }
            return result

        if name == "edge.evaluated":
            edge_id = str(payload.get("edge_id") or event.subject_id)
            previous = result["edges"].get(edge_id, {})
            selected = bool(payload.get("selected"))
            latest_state = str(
                payload.get("state") or event.status or "evaluated"
            )
            selected_count = int(previous.get("selected_count", 0)) + int(
                selected
            )
            skipped_count = int(previous.get("skipped_count", 0)) + int(
                latest_state == "skipped"
            )
            failed_count = int(previous.get("failed_count", 0)) + int(
                latest_state == "failed"
            )
            state = (
                "failed"
                if failed_count
                else "selected"
                if selected_count
                else latest_state
            )
            result["edges"][edge_id] = {
                "edge_id": edge_id,
                "state": state,
                "latest_state": latest_state,
                "latest_evaluation_sequence": event.sequence,
                "selected": selected_count > 0,
                "latest_selected": selected,
                "evaluation_count": int(previous.get("evaluation_count", 0)) + 1,
                "selected_count": selected_count,
                "skipped_count": skipped_count,
                "failed_count": failed_count,
                "source_execution_id": payload.get("source_execution_id"),
                "target_node_id": payload.get("target_node_id"),
                "reason": payload.get("reason"),
                "elapsed_ns": event.elapsed_ns,
            }
            return result

        if name == "operator_call.completed":
            call_id = str(payload.get("operator_call_id") or event.subject_id)
            state = str(payload.get("state") or event.status or "completed")
            execution_id = payload.get("node_execution_id")
            if execution_id is not None:
                execution = result["node_executions"].get(str(execution_id))
                if execution is not None:
                    error = payload.get("error")
                    reason = payload.get("reason")
                    call = {
                        "id": call_id,
                        "event_sequence": event.sequence,
                        "node_execution_id": str(execution_id),
                        "operator_id": str(payload.get("operator_id") or "operator"),
                        "kind": str(payload.get("kind") or "direct"),
                        "call_no": int(payload.get("call_no", 0)),
                        "unit_index": payload.get("unit_index"),
                        "unit_attempt_no": int(
                            payload.get("unit_attempt_no", 1)
                        ),
                        "reason": (
                            str(reason) if reason is not None else None
                        ),
                        "state": state,
                        "error": error,
                        "streaming": bool(payload.get("streaming", False)),
                        "stream_chunk_count": int(
                            payload.get("stream_chunk_count", 0)
                        ),
                        "started_at_ms": payload.get("started_at_ms"),
                        "occurred_at_ms": event.occurred_at_ms,
                        "elapsed_ns": event.elapsed_ns,
                        "timing": dict(event.timing),
                    }
                    calls = [
                        value
                        for value in execution.get("operator_calls", ())
                        if value.get("id") != call_id
                    ]
                    calls.append(call)
                    execution["operator_calls"] = calls[
                        -_TIMELINE_OPERATOR_CALL_LIMIT:
                    ]
                    execution["operator_call_count"] += 1
                    execution["failed_operator_call_count"] += int(
                        state != "completed"
                    )
                    execution["retry_count"] += int(reason == "retry")
                    execution["fallback_count"] += int(reason == "fallback")
                    execution["timeout_count"] += int(
                        isinstance(error, dict)
                        and error.get("code") == "OPERATOR_TIMEOUT"
                    )
                    execution["streaming_call_count"] += int(
                        bool(payload.get("streaming", False))
                    )
                    execution["stream_chunk_count"] += int(
                        payload.get("stream_chunk_count", 0)
                    )
                    node = result["nodes"].get(execution["node_id"])
                    if node is not None:
                        node.update(_node_operator_summary(execution))
            return result

        if name == "wait.created":
            waits = payload.get("waits", ())
            if waits:
                for wait in waits:
                    wait_key = str(wait["wait_key"])
                    result["active_waits"][wait_key] = {
                        **dict(wait),
                        "wait_key": wait_key,
                        "created_at_ms": event.occurred_at_ms,
                    }
            else:
                for wait_key in payload.get("wait_keys", ()):
                    result["active_waits"][str(wait_key)] = {
                        "wait_key": str(wait_key),
                        "created_at_ms": event.occurred_at_ms,
                    }
            result["invocation_state"] = "waiting"
            return result

        if name == "wait.resumed":
            wait_key = payload.get("wait_key")
            if wait_key is not None:
                result["active_waits"].pop(str(wait_key), None)
            return result

        if event.event_type == "phase":
            result["latest_phase"] = {
                "event_name": name,
                "subject_id": event.subject_id,
                "status": event.status,
                "occurred_at_ms": event.occurred_at_ms,
                "elapsed_ns": event.elapsed_ns,
            }
        if event.event_type == "recovery" and name.endswith("interrupted"):
            result["invocation_state"] = "interrupted"
        return result


class TraceService:
    """Read-only RuntimeStore projection/query surface for AutoAgentServer."""

    def __init__(self, agent: AutoAgentApp, *, cache_size: int = 128) -> None:
        self.agent = agent
        self.store = agent.runtime_store
        self._projection_cache: OrderedDict[
            tuple[UUID, int], dict[str, Any]
        ] = OrderedDict()
        self._cache_size = cache_size

    async def list_workflows(
        self,
        *,
        cursor: str | None,
        limit: int,
        registered_only: bool = False,
    ) -> dict[str, Any]:
        versions = await self._workflow_version_page(
            limit=limit,
            before=_decode_cursor(cursor),
            registered_only=registered_only,
        )
        values: list[dict[str, Any]] = []
        registered = self._registered_workflow_identities()
        for revision_id, snapshot, created_at_ms in versions:
            identity = (snapshot.workflow_id, snapshot.definition_hash)
            if registered_only and identity not in registered:
                continue
            value = {
                "workflow_id": snapshot.workflow_id,
                "workflow_version": snapshot.workflow_version,
                "revision_id": revision_id,
                "definition_hash": snapshot.definition_hash,
                "name": snapshot.definition.get("name"),
                "description": snapshot.definition.get("description"),
                "registered": identity in registered,
                "created_at_ms": created_at_ms,
                "updated_at_ms": created_at_ms,
            }
            values.append(value)
        values.sort(
            key=lambda item: (item["updated_at_ms"], item["revision_id"]),
            reverse=True,
        )
        return _paginate(
            values,
            cursor=cursor,
            limit=limit,
            key_fields=("updated_at_ms", "revision_id"),
        ).to_dict()

    async def list_workflow_versions(
        self,
        workflow_id: str,
        *,
        cursor: str | None,
        limit: int,
    ) -> dict[str, Any]:
        registered = self._registered_workflow_identities()
        values = [
            {
                "workflow_id": snapshot.workflow_id,
                "workflow_version": snapshot.workflow_version,
                "revision_id": revision_id,
                "definition_hash": snapshot.definition_hash,
                "name": snapshot.definition.get("name"),
                "description": snapshot.definition.get("description"),
                "registered": (
                    snapshot.workflow_id,
                    snapshot.definition_hash,
                ) in registered,
                "created_at_ms": created_at_ms,
                "updated_at_ms": created_at_ms,
            }
            for revision_id, snapshot, created_at_ms
            in await self._workflow_version_page(
                limit=limit,
                before=_decode_cursor(cursor),
                workflow_id=workflow_id,
            )
        ]
        values.sort(
            key=lambda item: (item["created_at_ms"], item["revision_id"]),
            reverse=True,
        )
        return _paginate(
            values,
            cursor=cursor,
            limit=limit,
            key_fields=("created_at_ms", "revision_id"),
        ).to_dict()

    async def workflow_graph(self, revision_id: str) -> dict[str, Any]:
        for candidate, snapshot, _ in self._memory_workflow_versions():
            if candidate == revision_id:
                return _graph_view(revision_id, snapshot)
        backend_loader = getattr(
            self.store.backend,
            "aload_trace_workflow_version",
            None,
        )
        if backend_loader is not None:
            value = await backend_loader(revision_id)
            if value is not None:
                candidate, snapshot, _ = value
                return _graph_view(candidate, snapshot)
        raise KeyError(f"Unknown Workflow revision: {revision_id}")

    async def list_sessions(
        self,
        workflow_revision_id: str,
        *,
        cursor: str | None,
        limit: int,
    ) -> dict[str, Any]:
        values: dict[str, dict[str, Any]] = {}
        memory_sessions = [
            session
            for session in self.store.sessions.values()
            if session.workflow_revision_id == workflow_revision_id
        ]
        backend_loader = getattr(self.store.backend, "alist_trace_sessions", None)
        if backend_loader is not None:
            for record in await backend_loader(
                workflow_revision_id=workflow_revision_id,
                limit=limit + len(memory_sessions) + 1,
                before=_decode_cursor(cursor),
            ):
                values[str(record["id"])] = dict(record)
        for session in memory_sessions:
            values[str(session.id)] = self._session_summary(session)
        ordered = sorted(
            values.values(),
            key=lambda item: (item["updated_at_ms"], item["id"]),
            reverse=True,
        )
        return _paginate(
            ordered,
            cursor=cursor,
            limit=limit,
            key_fields=("updated_at_ms", "id"),
        ).to_dict()

    async def list_invocations(
        self,
        session_id: UUID,
        *,
        cursor: str | None,
        limit: int,
    ) -> dict[str, Any]:
        values: dict[str, dict[str, Any]] = {}
        session = self.store.sessions.get(session_id)
        memory_invocations = (
            list(session.invocations)
            if session is not None
            else []
        )
        backend_loader = getattr(
            self.store.backend,
            "alist_trace_invocations",
            None,
        )
        if backend_loader is not None:
            for record in await backend_loader(
                session_id=session_id,
                limit=limit + len(memory_invocations) + 1,
                before=_decode_cursor(cursor),
            ):
                values[str(record["id"])] = dict(record)
        if session is not None:
            for invocation in memory_invocations:
                values[str(invocation.id)] = self._invocation_summary(
                    session,
                    invocation,
                )
        ordered = sorted(
            values.values(),
            key=lambda item: (item["created_at_ms"], item["id"]),
            reverse=True,
        )
        return _paginate(
            ordered,
            cursor=cursor,
            limit=limit,
            key_fields=("created_at_ms", "id"),
        ).to_dict()

    async def list_invocation_neighbors(
        self,
        session_id: UUID,
        *,
        anchor_invocation_id: UUID,
        direction: str,
        limit: int,
    ) -> dict[str, Any]:
        if direction not in {"older", "newer"}:
            raise ValueError("Invocation neighbor direction must be older or newer.")
        anchor = await self._invocation_detail(anchor_invocation_id)
        if str(anchor["session_id"]) != str(session_id):
            raise ValueError("Invocation anchor does not belong to this Session.")
        anchor_key = (int(anchor["created_at_ms"]), str(anchor["id"]))
        values: dict[str, dict[str, Any]] = {}
        session = self.store.sessions.get(session_id)
        memory_invocations = (
            list(session.invocations)
            if session is not None
            else []
        )
        backend_loader = getattr(
            self.store.backend,
            "alist_trace_invocations",
            None,
        )
        if backend_loader is not None:
            query = {
                "before": anchor_key if direction == "older" else None,
                "after": anchor_key if direction == "newer" else None,
            }
            for record in await backend_loader(
                session_id=session_id,
                limit=limit + len(memory_invocations) + 1,
                **query,
            ):
                values[str(record["id"])] = dict(record)
        if session is not None:
            for invocation in memory_invocations:
                record = self._invocation_summary(
                    session,
                    invocation,
                )
                key = (int(record["created_at_ms"]), str(record["id"]))
                if (
                    direction == "older" and key < anchor_key
                    or direction == "newer" and key > anchor_key
                ):
                    values[str(record["id"])] = record
        ordered = sorted(
            values.values(),
            key=lambda item: (item["created_at_ms"], item["id"]),
            reverse=direction == "older",
        )
        has_more = len(ordered) > limit
        selected = ordered[:limit]
        selected.sort(key=lambda item: (item["created_at_ms"], item["id"]))
        return {
            "items": selected,
            "has_more": has_more,
            "direction": direction,
            "anchor_invocation_id": str(anchor_invocation_id),
        }

    async def trace_bootstrap(
        self,
        invocation_id: UUID,
        *,
        tail_limit: int,
    ) -> dict[str, Any]:
        record = await self._invocation_detail(invocation_id)
        live_sequence = int(record["live_sequence"])
        latest_projection = await self.projection(
            invocation_id,
            through_sequence=live_sequence,
        )
        self._enrich_latest_projection_from_memory(
            latest_projection,
            invocation_id,
        )
        if record["event_mode"] == "minimal":
            latest_projection["invocation_state"] = record["state"]
        revision = await self.workflow_graph(record["workflow_revision_id"])
        return {
            "workflow": revision,
            "session": record.pop("session"),
            "invocation": record,
            "capabilities": {
                "has_events": record["event_mode"] != "minimal",
                "has_graph_trace": record["event_mode"] != "minimal",
                "has_internal_phases": record["event_mode"] == "full",
                "has_historical_runtime_state": record["event_mode"] == "full",
                "fork_available": False,
                "design_available": False,
            },
            "checkpoint": {
                "schema_version": TraceProjectionReducer.schema_version,
                "through_sequence": live_sequence,
                "projection": latest_projection,
            },
            "event_page": {
                "items": [],
                "first_sequence": None,
                "last_sequence": None,
                "has_earlier": False,
                "has_later": live_sequence > 0,
                "live_sequence": live_sequence,
                "invocation_state": record["state"],
            },
        }

    async def event_page(
        self,
        invocation_id: UUID,
        *,
        after_sequence: int,
        before_sequence: int | None,
        limit: int,
    ) -> dict[str, Any]:
        events = await self.store.alist_trace_runtime_events(
            invocation_id=invocation_id,
            after_sequence=after_sequence,
            before_sequence=before_sequence,
            limit=limit,
        )
        detail = await self._invocation_detail(invocation_id)
        live_sequence = int(detail["live_sequence"])
        first_sequence = events[0].sequence if events else None
        last_sequence = events[-1].sequence if events else after_sequence
        return {
            "items": [
                self.event_view(event, include_values=False)
                for event in events
            ],
            "first_sequence": first_sequence,
            "last_sequence": events[-1].sequence if events else None,
            "has_earlier": bool(
                (first_sequence is not None and first_sequence > 1)
                or (first_sequence is None and after_sequence > 0)
            ),
            "has_later": last_sequence < live_sequence,
            "live_sequence": live_sequence,
            "invocation_state": detail["state"],
        }

    async def event_detail(
        self,
        invocation_id: UUID,
        sequence: int,
    ) -> dict[str, Any]:
        events = await self.store.alist_trace_runtime_events(
            invocation_id=invocation_id,
            after_sequence=sequence - 1,
            limit=1,
        )
        if not events or events[0].sequence != sequence:
            raise KeyError(
                f"Unknown RuntimeEvent sequence: {invocation_id}/{sequence}"
            )
        return self.event_view(events[0], include_values=True)

    async def user_event_page(
        self,
        invocation_id: UUID,
        *,
        after_sequence: int,
        limit: int,
    ) -> dict[str, Any]:
        events = await self.store.alist_user_events(
            invocation_id=invocation_id,
            after_sequence=after_sequence,
            limit=limit,
        )
        live_sequence = await self.store.alatest_user_event_sequence(
            invocation_id
        )
        return {
            "items": [self.user_event_view(event) for event in events],
            "last_sequence": events[-1].sequence if events else None,
            "has_later": (
                bool(events) and events[-1].sequence < live_sequence
            ),
            "live_sequence": live_sequence,
        }

    async def invocation_detail(self, invocation_id: UUID) -> dict[str, Any]:
        record = await self._invocation_detail(invocation_id)
        record.pop("session", None)
        return record

    async def runtime_state(
        self,
        invocation_id: UUID,
        *,
        through_sequence: int | None,
    ) -> dict[str, Any]:
        detail = await self._invocation_detail(invocation_id)
        if detail["event_mode"] != "full":
            raise ValueError(
                "Historical Runtime state is available only for full tracing."
            )
        target_sequence = (
            int(detail["live_sequence"])
            if through_sequence is None
            else through_sequence
        )
        snapshot = await self.store.aload_trace_execution_snapshot(
            invocation_id,
            at_or_before_sequence=target_sequence,
        )
        if snapshot is None:
            raise KeyError(
                f"No execution snapshot for Invocation: {invocation_id}"
            )
        state = deepcopy(snapshot.state)
        cursor = snapshot.through_sequence
        while cursor < target_sequence:
            events = await self.store.alist_trace_runtime_events(
                invocation_id=invocation_id,
                after_sequence=cursor,
                before_sequence=target_sequence + 1,
                limit=min(1_000, target_sequence - cursor),
            )
            if not events:
                break
            for event in events:
                if event.sequence != cursor + 1:
                    raise ValueError(
                        "RuntimeEvent journal is not contiguous: "
                        f"expected sequence {cursor + 1}, "
                        f"got {event.sequence}."
                    )
                if event.operations is None:
                    raise ValueError(
                        f"Full RuntimeEvent {event.sequence} has no "
                        "state operations."
                    )
                state = apply_state_operations(
                    state,
                    event.operations,
                )
                cursor = event.sequence
        return {
            "invocation_id": str(invocation_id),
            "through_sequence": cursor,
            "session_context": deepcopy(
                state["session"]["context"]
            ),
            "invocation": deepcopy(state["invocation"]),
            "node_executions": deepcopy(
                state.get("node_executions", [])
            ),
        }

    async def projection(
        self,
        invocation_id: UUID,
        *,
        through_sequence: int,
    ) -> dict[str, Any]:
        key = (invocation_id, through_sequence)
        cached = self._projection_cache.get(key)
        if cached is not None:
            self._projection_cache.move_to_end(key)
            return deepcopy(cached)
        nearest_sequence = 0
        projection = TraceProjectionReducer.initial(invocation_id)
        for (candidate_id, sequence), value in reversed(
            self._projection_cache.items()
        ):
            if candidate_id == invocation_id and sequence <= through_sequence:
                nearest_sequence = sequence
                projection = deepcopy(value)
                break
        cursor = nearest_sequence
        while cursor < through_sequence:
            page = await self.store.alist_trace_runtime_events(
                invocation_id=invocation_id,
                after_sequence=cursor,
                limit=min(1_000, through_sequence - cursor),
            )
            if not page:
                break
            for event in page:
                if event.sequence > through_sequence:
                    break
                projection = TraceProjectionReducer.apply(projection, event)
                cursor = event.sequence
            if page[-1].sequence <= cursor and cursor >= through_sequence:
                break
            if cursor == nearest_sequence:
                break
            nearest_sequence = cursor
        self._projection_cache[key] = deepcopy(projection)
        self._projection_cache.move_to_end(key)
        while len(self._projection_cache) > self._cache_size:
            self._projection_cache.popitem(last=False)
        return projection

    def event_view(
        self,
        event: RuntimeEvent,
        *,
        include_values: bool,
    ) -> dict[str, Any]:
        return {
            "id": str(event.id),
            "invocation_id": str(event.invocation_id),
            "sequence": event.sequence,
            "schema_version": event.schema_version,
            "event_type": event.event_type,
            "event_name": event.event_name,
            "subject_type": event.subject_type,
            "subject_id": event.subject_id,
            "occurred_at_ms": event.occurred_at_ms,
            "elapsed_ns": event.elapsed_ns,
            "status": event.status,
            "timing": dict(event.timing),
            "payload": _json_value(self.store, event.payload),
            "has_input": event.input is not None,
            "has_output": event.output is not None,
            "has_operations": event.operations is not None,
            "input": (
                _json_value(self.store, event.input)
                if include_values and event.input is not None
                else None
            ),
            "output": (
                _json_value(self.store, event.output)
                if include_values and event.output is not None
                else None
            ),
            **(
                {
                    "operations": (
                        _json_value(
                            self.store,
                            [
                                operation.model_dump(mode="python")
                                for operation in event.operations
                            ],
                        )
                        if event.operations is not None
                        else None
                    )
                }
                if include_values
                else {}
            ),
        }

    def user_event_view(self, event: UserEvent) -> dict[str, Any]:
        return {
            "id": str(event.id),
            "invocation_id": str(event.invocation_id),
            "sequence": event.sequence,
            "schema_version": event.schema_version,
            "type": event.type,
            "data": deepcopy(event.data),
            "node_id": event.node_id,
            "workflow_path": list(event.workflow_path),
            "node_execution_id": str(event.node_execution_id),
            "operator_call_id": (
                str(event.operator_call_id)
                if event.operator_call_id is not None
                else None
            ),
            "occurred_at_ms": event.occurred_at_ms,
        }

    def _enrich_latest_projection_from_memory(
        self,
        projection: dict[str, Any],
        invocation_id: UUID,
    ) -> None:
        """Fill fields omitted by Events created before Projection schema v2.

        This is intentionally limited to the latest in-memory view. Historical
        replay remains Event-derived and never leaks a later Runtime value into
        an earlier cursor.
        """

        invocation = self.store.invocations.get(invocation_id)
        if invocation is None:
            return
        for node_execution in invocation.node_executions:
            projected = projection["node_executions"].get(
                str(node_execution.id)
            )
            if projected is None:
                continue
            if node_execution.error is not None:
                projected["error"] = node_execution.error.to_record()
            summary = node_execution.operator_summary
            projected["operator_call_count"] = summary.attempt_count
            projected["failed_operator_call_count"] = (
                summary.failure_count + summary.interrupted_count
            )
            projected["retry_count"] = summary.retry_count
            projected["fallback_count"] = summary.fallback_count
            projected["timeout_count"] = summary.timeout_count
            projected["streaming_call_count"] = summary.streaming_call_count
            projected["stream_chunk_count"] = summary.stream_chunk_count
            projected["parallel_summary"] = (
                node_execution.parallel_summary.to_record()
                if node_execution.parallel_summary is not None
                else None
            )
            node = projection["nodes"].get(node_execution.node_id)
            if node is not None:
                if node_execution.error is not None:
                    node["latest_error"] = node_execution.error.to_record()
                node.update(_node_operator_summary(projected))

    async def _invocation_detail(
        self,
        invocation_id: UUID,
    ) -> dict[str, Any]:
        invocation = self.store.invocations.get(invocation_id)
        if invocation is not None:
            session_id = self.store.invocation_sessions[invocation_id]
            session = self.store.sessions[session_id]
            return {
                **self._invocation_summary(session, invocation),
                "input": _json_value(self.store, invocation.input),
                "result": _json_value(self.store, invocation.result),
                "error": (
                    invocation.error.to_record()
                    if invocation.error is not None
                    else None
                ),
                "session": self._session_summary(session),
            }
        backend_loader = getattr(
            self.store.backend,
            "aload_trace_invocation",
            None,
        )
        if backend_loader is None:
            raise KeyError(f"Unknown Invocation: {invocation_id}")
        record = await backend_loader(invocation_id)
        if record is None:
            raise KeyError(f"Unknown Invocation: {invocation_id}")
        return dict(record)

    def _session_summary(self, session: Session) -> dict[str, Any]:
        current = session.get_current_invocation()
        return {
            "id": str(session.id),
            "workflow_id": session.workflow_id,
            "workflow_revision_id": session.workflow_revision_id,
            "session_key": session.session_key,
            "current_invocation_id": (
                str(session.current_invocation_id)
                if session.current_invocation_id is not None
                else None
            ),
            "current_invocation_state": (
                current.state if current is not None else None
            ),
            "invocation_count": len(session.invocations),
            "created_at_ms": session.created_at_ms,
            "updated_at_ms": session.updated_at_ms,
        }

    def _invocation_summary(
        self,
        session: Session,
        invocation: Invocation,
    ) -> dict[str, Any]:
        return {
            "id": str(invocation.id),
            "session_id": str(session.id),
            "workflow_id": invocation.workflow_id,
            "workflow_revision_id": invocation.workflow_revision_id,
            "workflow_version": invocation.workflow_version,
            "definition_hash": invocation.workflow_definition_hash,
            "entry_node_id": invocation.entry_node_id,
            "state": invocation.state,
            "execution_mode": invocation.execution_mode,
            "event_mode": invocation.event_mode,
            "live_sequence": invocation.event_sequence,
            "live_user_event_sequence": (
                self.store.latest_user_event_sequence(invocation.id)
            ),
            "durable_sequence": self.store.durable_sequence(invocation.id),
            "persistence_status": self.store.persistence_status(invocation.id),
            "user_event_persistence_status": (
                self.store.user_event_persistence_status(invocation.id)
            ),
            "created_at_ms": invocation.created_at_ms,
            "updated_at_ms": invocation.updated_at_ms,
        }

    async def _workflow_version_page(
        self,
        *,
        limit: int,
        before: tuple[int, str] | None,
        workflow_id: str | None = None,
        registered_only: bool = False,
    ) -> list[tuple[str, WorkflowVersionSnapshot, int]]:
        values: dict[
            tuple[str, str],
            tuple[str, WorkflowVersionSnapshot, int],
        ] = {}
        memory_versions = [
            value
            for value in self._memory_workflow_versions()
            if workflow_id is None or value[1].workflow_id == workflow_id
        ]
        registered = self._registered_workflow_identities()
        if registered_only:
            return [
                value
                for value in memory_versions
                if (value[1].workflow_id, value[1].definition_hash)
                in registered
            ]
        backend_loader = getattr(
            self.store.backend,
            "alist_trace_workflow_versions",
            None,
        )
        if backend_loader is not None:
            database_versions = await backend_loader(
                limit=limit + len(memory_versions) + 1,
                before=before,
                workflow_id=workflow_id,
            )
            for revision_id, snapshot, created_at_ms in database_versions:
                values[(snapshot.workflow_id, snapshot.definition_hash)] = (
                    revision_id,
                    snapshot,
                    created_at_ms,
                )
            registered_loader = getattr(
                self.store.backend,
                "aload_trace_workflow_versions",
                None,
            )
            if registered_loader is not None:
                revision_ids = tuple(
                    revision_id
                    for revision_id, snapshot, _ in memory_versions
                    if (
                        snapshot.workflow_id,
                        snapshot.definition_hash,
                    )
                    not in values
                )
                if revision_ids:
                    for revision_id, snapshot, created_at_ms in (
                        await registered_loader(revision_ids)
                    ):
                        values[
                            (snapshot.workflow_id, snapshot.definition_hash)
                        ] = (
                            revision_id,
                            snapshot,
                            created_at_ms,
                        )
        for revision_id, snapshot, created_at_ms in memory_versions:
            identity = (snapshot.workflow_id, snapshot.definition_hash)
            values.setdefault(
                identity,
                (
                    revision_id,
                    snapshot,
                    created_at_ms,
                ),
            )
        return list(values.values())

    def _memory_workflow_versions(
        self,
    ) -> list[tuple[str, WorkflowVersionSnapshot, int]]:
        values: dict[
            tuple[str, str],
            tuple[str, WorkflowVersionSnapshot, int],
        ] = {}
        for snapshot in self.store.workflow_versions.values():
            identity = (snapshot.workflow_id, snapshot.definition_hash)
            values[identity] = (
                workflow_revision_id(*identity),
                snapshot,
                0,
            )
        for entry in self.agent.workflow_registry.values():
            snapshot = entry.workflow_snapshot
            identity = (snapshot.workflow_id, snapshot.definition_hash)
            values.setdefault(
                identity,
                (
                    workflow_revision_id(*identity),
                    snapshot,
                    0,
                ),
            )
        return list(values.values())

    def _registered_workflow_identities(
        self,
    ) -> set[tuple[str, str]]:
        return {
            (
                entry.workflow_snapshot.workflow_id,
                entry.workflow_snapshot.definition_hash,
            )
            for entry in self.agent.workflow_registry.values()
        }


def _graph_view(
    revision_id: str,
    snapshot: WorkflowVersionSnapshot,
) -> dict[str, Any]:
    definition = snapshot.definition
    nodes = [dict(value) for value in definition.get("nodes", ())]
    edges = [dict(value) for value in definition.get("edges", ())]
    return {
        "workflow_id": snapshot.workflow_id,
        "workflow_version": snapshot.workflow_version,
        "revision_id": revision_id,
        "definition_hash": snapshot.definition_hash,
        "name": definition.get("name"),
        "description": definition.get("description"),
        "nodes": nodes,
        "edges": edges,
        "groups": _workflow_groups(nodes, edges),
        "entry_node_ids": list(definition.get("entry_node_ids", ())),
        "exit_node_ids": list(definition.get("exit_node_ids", ())),
        "loop_regions": list(definition.get("loop_regions", ())),
        "policy": definition.get("policy"),
    }


def _workflow_groups(
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    paths: set[tuple[str, ...]] = set()
    for node in nodes:
        path = tuple(str(value) for value in node.get("workflow_path", ()))
        paths.update(path[:depth] for depth in range(1, len(path) + 1))
    groups: list[dict[str, Any]] = []
    for path in sorted(paths, key=lambda value: (len(value), value)):
        group_id = "/".join(path)
        direct = [
            str(node["id"])
            for node in nodes
            if tuple(node.get("workflow_path", ())) == path
        ]
        descendants = [
            str(node["id"])
            for node in nodes
            if tuple(node.get("workflow_path", ()))[: len(path)] == path
        ]
        descendant_set = set(descendants)
        direct_edges = [
            str(edge["id"])
            for edge in edges
            if tuple(edge.get("workflow_path", ())) == path
        ]
        descendant_edges = [
            str(edge["id"])
            for edge in edges
            if tuple(edge.get("workflow_path", ()))[: len(path)] == path
        ]
        incoming_inside = {
            str(edge["to_node"])
            for edge in edges
            if str(edge["from_node"]) in descendant_set
            and str(edge["to_node"]) in descendant_set
        }
        outgoing_inside = {
            str(edge["from_node"])
            for edge in edges
            if str(edge["from_node"]) in descendant_set
            and str(edge["to_node"]) in descendant_set
        }
        boundary_entries = {
            str(edge["to_node"])
            for edge in edges
            if str(edge["from_node"]) not in descendant_set
            and str(edge["to_node"]) in descendant_set
        }
        boundary_exits = {
            str(edge["from_node"])
            for edge in edges
            if str(edge["from_node"]) in descendant_set
            and str(edge["to_node"]) not in descendant_set
        }
        groups.append(
            {
                "id": group_id,
                "parent_group_id": (
                    "/".join(path[:-1]) if len(path) > 1 else None
                ),
                "label": path[-1],
                "workflow_path": list(path),
                "node_ids": descendants,
                "direct_node_ids": direct,
                "edge_ids": descendant_edges,
                "direct_edge_ids": direct_edges,
                "entry_node_ids": sorted(
                    boundary_entries
                    | (descendant_set - incoming_inside)
                ),
                "exit_node_ids": sorted(
                    boundary_exits
                    | (descendant_set - outgoing_inside)
                ),
            }
        )
    return groups


def _json_value(store, value: Any) -> Any:
    if value is None:
        return None
    payload = store.serializer.dumps_unchecked(value)
    return store.serializer.json_view(payload)


def _paginate(
    values: list[dict[str, Any]],
    *,
    cursor: str | None,
    limit: int,
    key_fields: tuple[str, str],
) -> TracePage:
    anchor = _decode_cursor(cursor)
    candidates = (
        values
        if anchor is None
        else [
            value
            for value in values
            if _page_key(value, key_fields) < anchor
        ]
    )
    selected = candidates[:limit]
    has_more = len(candidates) > len(selected)
    return TracePage(
        items=selected,
        next_cursor=(
            _encode_cursor(_page_key(selected[-1], key_fields))
            if has_more and selected
            else None
        ),
        has_more=has_more,
    )


def _page_key(
    value: dict[str, Any],
    fields: tuple[str, str],
) -> tuple[int, str]:
    return int(value[fields[0]]), str(value[fields[1]])


def _encode_cursor(anchor: tuple[int, str]) -> str:
    raw = json.dumps(
        {"timestamp": anchor[0], "id": anchor[1]},
        separators=(",", ":"),
    ).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_cursor(cursor: str | None) -> tuple[int, str] | None:
    if cursor is None:
        return None
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        value = json.loads(base64.urlsafe_b64decode(padded))
        timestamp = int(value["timestamp"])
        identity = str(value["id"])
    except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        raise ValueError("Invalid pagination cursor.") from exc
    if timestamp < 0 or not identity:
        raise ValueError("Invalid pagination cursor.")
    return timestamp, identity

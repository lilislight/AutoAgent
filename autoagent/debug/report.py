from __future__ import annotations

from collections.abc import Mapping
import asyncio
from time import monotonic
from typing import Any
from uuid import UUID

from autoagent.core.runtime import Invocation, RuntimeStore, Session
from autoagent.debug.models import (
    DebugPage,
    DebugSourceKind,
    EvidenceWarning,
    InvocationReport,
    PrimaryBoundary,
    ReportError,
)
from autoagent.debug.cursor import decode_debug_cursor, encode_debug_cursor
from autoagent.debug.values import summarize_value


_STREAM_TYPES = frozenset(
    {"message_delta", "reasoning_delta", "tool_call_delta"}
)


class DebugQueryService:
    """Type-neutral, read-only access to Runtime debugging evidence."""

    def __init__(self, store: RuntimeStore, *, source: DebugSourceKind) -> None:
        self.store = store
        self.source = source

    async def report(self, invocation_id: UUID) -> InvocationReport:
        record, invocation = await self._invocation_record(invocation_id)
        observed_sequence = int(record.get("live_sequence", 0))
        observed_user_sequence = await self.store.alatest_user_event_sequence(
            invocation_id
        )
        in_memory = invocation is not None
        durable_sequence = (
            self.store.durable_sequence(invocation_id)
            if in_memory
            else int(record.get("durable_sequence", observed_sequence))
        )
        durable_user_sequence = (
            self.store.durable_user_event_sequence(invocation_id)
            if in_memory
            else observed_user_sequence
        )
        persistence_status = str(
            record.get(
                "persistence_status",
                "durable" if not in_memory else "memory_only",
            )
        )
        user_event_persistence_status = str(
            record.get(
                "user_event_persistence_status",
                "durable" if not in_memory else "memory_only",
            )
        )

        warnings: list[EvidenceWarning] = []
        node_records, snapshot_sequence = await self._node_records(
            invocation_id,
            invocation,
            observed_sequence=observed_sequence,
        )
        if (
            record["event_mode"] != "minimal"
            and snapshot_sequence < observed_sequence
            and not in_memory
        ):
            warnings.append(
                EvidenceWarning(
                    code="RECOVERY_STATE_BEHIND_JOURNAL",
                    message=(
                        "Execution aggregates are based on the latest durable "
                        "Recovery State and may omit the bounded Event tail."
                    ),
                    detail={
                        "recovery_sequence": snapshot_sequence,
                        "observed_sequence": observed_sequence,
                    },
                )
            )
        if persistence_status not in {"memory_only", "durable"}:
            warnings.append(
                EvidenceWarning(
                    code="RUNTIME_EVENTS_NOT_FULLY_DURABLE",
                    message="Some observed RuntimeEvents are not durable yet.",
                    detail={
                        "observed_sequence": observed_sequence,
                        "durable_sequence": durable_sequence,
                    },
                )
            )
        if user_event_persistence_status not in {"memory_only", "durable"}:
            warnings.append(
                EvidenceWarning(
                    code="USER_EVENTS_NOT_FULLY_DURABLE",
                    message="Some observed UserEvents are not durable yet.",
                    detail={
                        "observed_sequence": observed_user_sequence,
                        "durable_sequence": durable_user_sequence,
                    },
                )
            )

        counts = _execution_counts(node_records)
        raw_user_counts = await self.store.acount_user_event_types(
            invocation_id,
            through_sequence=observed_user_sequence,
        )
        error = _report_error(record.get("error"))
        invocation_record = (
            invocation.to_record(self.store.invocation_sessions[invocation_id])
            if invocation is not None
            else None
        )
        available = ["invocation", "values"]
        if record["event_mode"] != "minimal":
            available.extend(
                ("nodes", "edges", "operator_calls", "runtime_events")
            )
        if observed_user_sequence:
            available.append("user_events")
        if record["event_mode"] == "full":
            available.append("runtime_state")

        return InvocationReport(
            source=self.source,
            invocation_id=str(record["id"]),
            session_id=str(record["session_id"]),
            workflow_id=str(record["workflow_id"]),
            workflow_revision_id=str(record["workflow_revision_id"]),
            workflow_version=record.get("workflow_version"),
            event_mode=record["event_mode"],
            execution_mode=str(record.get("execution_mode", "normal")),
            state=str(record["state"]),
            entry_node_id=record.get("entry_node_id"),
            created_at_ms=int(record["created_at_ms"]),
            updated_at_ms=int(record["updated_at_ms"]),
            observed_sequence=observed_sequence,
            durable_sequence=durable_sequence,
            observed_user_event_sequence=observed_user_sequence,
            durable_user_event_sequence=durable_user_sequence,
            persistence_status=persistence_status,
            user_event_persistence_status=user_event_persistence_status,
            input=summarize_value(
                record.get("input"),
                detail_ref=f"invocation:{invocation_id}:input",
            ),
            result=(
                None
                if record.get("result") is None
                else summarize_value(
                    record["result"],
                    detail_ref=f"invocation:{invocation_id}:result",
                )
            ),
            error=error,
            primary_boundary=_primary_boundary(
                state=str(record["state"]),
                error=error,
                node_records=node_records,
                invocation_record=invocation_record,
                invocation_id=str(invocation_id),
            ),
            node_execution_count=counts["node_execution_count"],
            edge_evaluation_count=counts["edge_evaluation_count"],
            operator_call_count=counts["operator_call_count"],
            retry_count=counts["retry_count"],
            fallback_count=counts["fallback_count"],
            timeout_count=counts["timeout_count"],
            wait_count=counts["wait_count"],
            recovery_count=counts["recovery_count"],
            user_event_counts=_user_event_categories(raw_user_counts),
            available_evidence=tuple(available),
            warnings=tuple(warnings),
        )

    async def report_when_stable(
        self,
        invocation_id: UUID,
        *,
        timeout_ms: int = 10_000,
    ) -> InvocationReport:
        """Wait by Runtime notification for waiting/terminal, then report facts."""

        if timeout_ms < 0:
            raise ValueError("Report wait timeout cannot be negative.")
        loop = asyncio.get_running_loop()
        changed = asyncio.Event()

        def notify() -> None:
            loop.call_soon_threadsafe(changed.set)

        unsubscribe = self.store.subscribe_runtime_changes(invocation_id, notify)
        deadline = monotonic() + timeout_ms / 1_000
        try:
            while True:
                changed.clear()
                report = await self.report(invocation_id)
                if report.state not in {"created", "running"}:
                    return report
                remaining = deadline - monotonic()
                if remaining <= 0:
                    warning = EvidenceWarning(
                        code="INVOCATION_STILL_RUNNING",
                        message=(
                            "Invocation did not reach waiting or terminal state "
                            "within the Report observation window."
                        ),
                        detail={"timeout_ms": timeout_ms},
                    )
                    return report.model_copy(
                        update={"warnings": (*report.warnings, warning)}
                    )
                try:
                    await asyncio.wait_for(changed.wait(), timeout=remaining)
                except TimeoutError:
                    # Re-read once so a boundary reached at the deadline is not
                    # reported as active because of timer ordering.
                    changed.set()
        finally:
            unsubscribe()

    async def runtime_events(
        self,
        invocation_id: UUID,
        *,
        cursor: str | None = None,
        through_sequence: int | None = None,
        limit: int = 20,
    ) -> DebugPage[dict[str, Any]]:
        limit = _page_limit(limit)
        through_sequence, position = await self._page_boundary(
            invocation_id,
            query="runtime_events",
            cursor=cursor,
            through_sequence=through_sequence,
            filters={},
        )
        events = await self.store.alist_trace_runtime_events(
            invocation_id=invocation_id,
            after_sequence=position,
            before_sequence=through_sequence + 1,
            limit=limit + 1,
        )
        selected = events[:limit]
        has_more = len(events) > limit
        next_cursor = (
            encode_debug_cursor(
                invocation_id=str(invocation_id),
                query="runtime_events",
                through_sequence=through_sequence,
                position=selected[-1].sequence,
            )
            if has_more and selected
            else None
        )
        return DebugPage[dict[str, Any]](
            through_sequence=through_sequence,
            items=tuple(_runtime_event_summary(event) for event in selected),
            next_cursor=next_cursor,
            has_more=has_more,
        )

    async def runtime_event(
        self,
        invocation_id: UUID,
        sequence: int,
        *,
        through_sequence: int | None = None,
    ) -> dict[str, Any]:
        if sequence < 1:
            raise ValueError("RuntimeEvent sequence must be positive.")
        boundary = await self._observed_sequence(
            invocation_id,
            requested=through_sequence,
        )
        if sequence > boundary:
            raise KeyError(f"Unknown RuntimeEvent sequence: {sequence}")
        events = await self.store.alist_trace_runtime_events(
            invocation_id=invocation_id,
            after_sequence=sequence - 1,
            before_sequence=sequence + 1,
            limit=1,
        )
        if not events or events[0].sequence != sequence:
            raise KeyError(f"Unknown RuntimeEvent sequence: {sequence}")
        event = events[0]
        return {
            **_runtime_event_summary(event),
            "payload": summarize_value(
                event.payload,
                detail_ref=f"runtime_event:{event.id}:payload",
            ).model_dump(mode="json"),
            "input": (
                None
                if event.input is None
                else summarize_value(
                    event.input,
                    detail_ref=f"runtime_event:{event.id}:input",
                ).model_dump(mode="json")
            ),
            "output": (
                None
                if event.output is None
                else summarize_value(
                    event.output,
                    detail_ref=f"runtime_event:{event.id}:output",
                ).model_dump(mode="json")
            ),
            "operation_count": len(event.operations or ()),
        }

    async def user_events(
        self,
        invocation_id: UUID,
        *,
        cursor: str | None = None,
        through_sequence: int | None = None,
        limit: int = 20,
        include_stream_deltas: bool = False,
    ) -> DebugPage[dict[str, Any]]:
        limit = _page_limit(limit)
        filters = {"include_stream_deltas": include_stream_deltas}
        through_sequence, position = await self._user_page_boundary(
            invocation_id,
            cursor=cursor,
            through_sequence=through_sequence,
            filters=filters,
        )
        selected, has_more = await self._select_user_events(
            invocation_id,
            after_sequence=position,
            through_sequence=through_sequence,
            limit=limit,
            include_stream_deltas=include_stream_deltas,
        )
        next_cursor = (
            encode_debug_cursor(
                invocation_id=str(invocation_id),
                query="user_events",
                through_sequence=through_sequence,
                position=selected[-1].sequence,
                filters=filters,
            )
            if has_more and selected
            else None
        )
        return DebugPage[dict[str, Any]](
            through_sequence=through_sequence,
            items=tuple(_user_event_summary(event) for event in selected),
            next_cursor=next_cursor,
            has_more=has_more,
        )

    async def user_event(
        self,
        invocation_id: UUID,
        sequence: int,
    ) -> dict[str, Any]:
        if sequence < 1:
            raise ValueError("UserEvent sequence must be positive.")
        events = await self.store.alist_user_events(
            invocation_id=invocation_id,
            after_sequence=sequence - 1,
            limit=1,
        )
        if not events or events[0].sequence != sequence:
            raise KeyError(f"Unknown UserEvent sequence: {sequence}")
        event = events[0]
        return {
            **_user_event_summary(event),
            "data": summarize_value(
                event.data,
                detail_ref=f"user_event:{event.id}:data",
            ).model_dump(mode="json"),
        }

    async def _page_boundary(
        self,
        invocation_id: UUID,
        *,
        query: str,
        cursor: str | None,
        through_sequence: int | None,
        filters: dict[str, Any],
    ) -> tuple[int, int]:
        if cursor is not None:
            return decode_debug_cursor(
                cursor,
                invocation_id=str(invocation_id),
                query=query,
                through_sequence=through_sequence,
                filters=filters,
            )
        return (
            await self._observed_sequence(
                invocation_id,
                requested=through_sequence,
            ),
            0,
        )

    async def _user_page_boundary(
        self,
        invocation_id: UUID,
        *,
        cursor: str | None,
        through_sequence: int | None,
        filters: dict[str, Any],
    ) -> tuple[int, int]:
        if cursor is not None:
            return decode_debug_cursor(
                cursor,
                invocation_id=str(invocation_id),
                query="user_events",
                through_sequence=through_sequence,
                filters=filters,
            )
        latest = await self.store.alatest_user_event_sequence(invocation_id)
        if through_sequence is not None:
            if through_sequence < 0 or through_sequence > latest:
                raise ValueError("Invalid UserEvent observed sequence boundary.")
            latest = through_sequence
        return latest, 0

    async def _observed_sequence(
        self,
        invocation_id: UUID,
        *,
        requested: int | None,
    ) -> int:
        record, _ = await self._invocation_record(invocation_id)
        latest = int(record.get("live_sequence", 0))
        if requested is not None:
            if requested < 0 or requested > latest:
                raise ValueError("Invalid Runtime observed sequence boundary.")
            return requested
        return latest

    async def _select_user_events(
        self,
        invocation_id: UUID,
        *,
        after_sequence: int,
        through_sequence: int,
        limit: int,
        include_stream_deltas: bool,
    ) -> tuple[tuple[Any, ...], bool]:
        selected: list[Any] = []
        cursor = after_sequence
        while len(selected) <= limit and cursor < through_sequence:
            page = await self.store.alist_user_events(
                invocation_id=invocation_id,
                after_sequence=cursor,
                limit=min(500, max(32, (limit + 1) * 4)),
            )
            if not page:
                break
            for event in page:
                cursor = event.sequence
                if event.sequence > through_sequence:
                    break
                if include_stream_deltas or event.type not in _STREAM_TYPES:
                    selected.append(event)
                    if len(selected) > limit:
                        break
            if page[-1].sequence <= after_sequence or page[-1].sequence >= through_sequence:
                break
            after_sequence = page[-1].sequence
        return tuple(selected[:limit]), len(selected) > limit

    async def _invocation_record(
        self,
        invocation_id: UUID,
    ) -> tuple[dict[str, Any], Invocation | None]:
        invocation = self.store.invocations.get(invocation_id)
        if invocation is not None:
            session_id = self.store.invocation_sessions[invocation_id]
            session = self.store.sessions[session_id]
            return _memory_invocation_record(self.store, session, invocation), invocation
        loader = getattr(self.store.backend, "aload_trace_invocation", None)
        if loader is None:
            raise KeyError(f"Unknown Invocation: {invocation_id}")
        record = await loader(invocation_id)
        if record is None:
            raise KeyError(f"Unknown Invocation: {invocation_id}")
        return dict(record), None

    async def _node_records(
        self,
        invocation_id: UUID,
        invocation: Invocation | None,
        *,
        observed_sequence: int,
    ) -> tuple[tuple[dict[str, Any], ...], int]:
        if invocation is not None:
            return (
                tuple(
                    execution.to_record(invocation.id)
                    for execution in invocation.node_executions
                ),
                observed_sequence,
            )
        snapshot = await self.store.aload_trace_execution_snapshot(
            invocation_id,
            at_or_before_sequence=observed_sequence,
        )
        if snapshot is None:
            return (), 0
        return (
            tuple(dict(value) for value in snapshot.state.get("node_executions", ())),
            snapshot.through_sequence,
        )


def _memory_invocation_record(
    store: RuntimeStore,
    session: Session,
    invocation: Invocation,
) -> dict[str, Any]:
    return {
        "id": str(invocation.id),
        "session_id": str(session.id),
        "workflow_id": invocation.workflow_id,
        "workflow_revision_id": invocation.workflow_revision_id,
        "workflow_version": invocation.workflow_version,
        "entry_node_id": invocation.entry_node_id,
        "state": invocation.state,
        "execution_mode": invocation.execution_mode,
        "event_mode": invocation.event_mode,
        "live_sequence": invocation.event_sequence,
        "durable_sequence": store.durable_sequence(invocation.id),
        "persistence_status": store.persistence_status(invocation.id),
        "user_event_persistence_status": store.user_event_persistence_status(
            invocation.id
        ),
        "input": store.serializer.json_view(
            store.serializer.dumps_unchecked(invocation.input)
        ),
        "result": (
            None
            if invocation.result is None
            else store.serializer.json_view(
                store.serializer.dumps_unchecked(invocation.result)
            )
        ),
        "error": invocation.error.to_record() if invocation.error else None,
        "created_at_ms": invocation.created_at_ms,
        "updated_at_ms": invocation.updated_at_ms,
    }


def _execution_counts(records: tuple[dict[str, Any], ...]) -> dict[str, int]:
    counts = {
        "node_execution_count": len(records),
        "edge_evaluation_count": 0,
        "operator_call_count": 0,
        "retry_count": 0,
        "fallback_count": 0,
        "timeout_count": 0,
        "wait_count": 0,
        "recovery_count": 0,
    }
    for node in records:
        counts["edge_evaluation_count"] += len(node.get("edge_evaluations", ()))
        if node.get("state") == "waiting":
            counts["wait_count"] += 1
        if int(node.get("recovery_attempt", 0)) > 0:
            counts["recovery_count"] += 1
        for call in node.get("operator_executions", ()):
            counts["operator_call_count"] += 1
            if call.get("type") == "parallel":
                summary = call.get("summary") or {}
                counts["retry_count"] += int(summary.get("retry_count", 0))
                counts["fallback_count"] += int(summary.get("fallback_count", 0))
                counts["timeout_count"] += sum(
                    1
                    for sample in summary.get("failure_samples", ())
                    if (sample.get("error") or {}).get("code")
                    == "OPERATOR_TIMEOUT"
                )
            else:
                reason = call.get("reason")
                if reason == "retry":
                    counts["retry_count"] += 1
                elif reason == "fallback":
                    counts["fallback_count"] += 1
                if (call.get("error") or {}).get("code") == "OPERATOR_TIMEOUT":
                    counts["timeout_count"] += 1
    return counts


def _report_error(value: Any) -> ReportError | None:
    if not isinstance(value, Mapping):
        return None
    detail = value.get("detail")
    return ReportError(
        code=str(value.get("code") or "RUNTIME_ERROR"),
        message=str(value.get("message") or "Runtime execution failed."),
        exception_type=(
            str(detail["error_type"])
            if isinstance(detail, Mapping) and detail.get("error_type")
            else None
        ),
        detail_ref="invocation:error",
    )


def _primary_boundary(
    *,
    state: str,
    error: ReportError | None,
    node_records: tuple[dict[str, Any], ...],
    invocation_record: dict[str, Any] | None,
    invocation_id: str,
) -> PrimaryBoundary | None:
    if state == "waiting" and invocation_record is not None:
        waiting = (invocation_record.get("scheduler") or {}).get(
            "waiting_executions",
            {},
        )
        if waiting:
            wait_key, value = next(iter(waiting.items()))
            return PrimaryBoundary(
                kind="wait",
                subject_id=str(wait_key),
                node_id=str(value.get("node_id") or "") or None,
                status="waiting",
            )
    wanted = (
        {"failed", "interrupted", "cancelled"}
        if error is not None or state in {"failed", "interrupted", "cancelled"}
        else {"running"}
        if state in {"created", "running"}
        else set()
    )
    for node in reversed(node_records):
        if node.get("state") in wanted:
            return PrimaryBoundary(
                kind="node",
                subject_id=str(node["id"]),
                node_id=str(node.get("node_id") or "") or None,
                status=str(node.get("state")),
                message=(
                    str((node.get("error") or {}).get("message"))
                    if node.get("error")
                    else None
                ),
            )
    if error is not None:
        return PrimaryBoundary(
            kind="invocation",
            subject_id=invocation_id,
            status=state,
            message=error.message,
        )
    return None


def _user_event_categories(counts: Mapping[str, int]) -> dict[str, int]:
    categories: dict[str, int] = {}
    for event_type, count in counts.items():
        if event_type in _STREAM_TYPES:
            category = "stream_delta"
        elif event_type.startswith("message_"):
            category = "message"
        elif event_type.startswith("reasoning_"):
            category = "reasoning"
        elif event_type.startswith("tool_call_"):
            category = "tool_call"
        elif event_type == "tool_result":
            category = "tool_result"
        elif event_type.startswith("agent_"):
            category = "agent_output"
        else:
            category = "custom"
        categories[category] = categories.get(category, 0) + int(count)
    return categories


def _page_limit(limit: int) -> int:
    if not 1 <= limit <= 100:
        raise ValueError("Debug page limit must be between 1 and 100.")
    return limit


def _runtime_event_summary(event: Any) -> dict[str, Any]:
    return {
        "id": str(event.id),
        "sequence": event.sequence,
        "event_type": event.event_type,
        "event_name": event.event_name,
        "subject_type": event.subject_type,
        "subject_id": event.subject_id,
        "occurred_at_ms": event.occurred_at_ms,
        "elapsed_ns": event.elapsed_ns,
        "status": event.status,
        "timing": dict(event.timing),
        "has_input": event.input is not None,
        "has_output": event.output is not None,
        "has_operations": bool(event.operations),
    }


def _user_event_summary(event: Any) -> dict[str, Any]:
    return {
        "id": str(event.id),
        "sequence": event.sequence,
        "type": event.type,
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

from __future__ import annotations

from collections.abc import Mapping
import asyncio
from copy import deepcopy
from time import monotonic
from typing import Any
from uuid import UUID

from autoagent.core.runtime import (
    Invocation,
    RuntimeStore,
    Session,
    apply_state_operations,
)
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
        if record["event_mode"] == "minimal":
            node_records = ()
        elif not in_memory:
            try:
                node_records = tuple(
                    (await self._node_record_map(
                        invocation_id,
                        observed_sequence=observed_sequence,
                    )).values()
                )
            except (KeyError, ValueError) as exc:
                node_records, snapshot_sequence = await self._node_records(
                    invocation_id,
                    None,
                    observed_sequence=observed_sequence,
                )
                warnings.append(
                    EvidenceWarning(
                        code="EXECUTION_AGGREGATES_INCOMPLETE",
                        message=(
                            "Execution aggregates use the latest available "
                            "Recovery State because its Event tail could not "
                            "be applied completely."
                        ),
                        detail={
                            "recovery_sequence": snapshot_sequence,
                            "observed_sequence": observed_sequence,
                            "reason": str(exc),
                        },
                    )
                )
        else:
            node_records, _ = await self._node_records(
                invocation_id,
                invocation,
                observed_sequence=observed_sequence,
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
        selected, has_more = await self._select_runtime_events(
            invocation_id,
            after_sequence=position,
            through_sequence=through_sequence,
            limit=limit,
        )
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

    async def node_executions(
        self,
        invocation_id: UUID,
        *,
        cursor: str | None = None,
        through_sequence: int | None = None,
        limit: int = 20,
    ) -> DebugPage[dict[str, Any]]:
        """Page logical NodeExecution starts without embedding their values."""

        limit = _page_limit(limit)
        through_sequence, position = await self._page_boundary(
            invocation_id,
            query="node_executions",
            cursor=cursor,
            through_sequence=through_sequence,
            filters={},
        )
        selected, has_more = await self._select_runtime_events(
            invocation_id,
            after_sequence=position,
            through_sequence=through_sequence,
            limit=limit,
            event_names=frozenset({"node.running"}),
        )
        record_map = await self._node_record_map(
            invocation_id,
            observed_sequence=through_sequence,
        )
        items = tuple(
            _node_execution_summary(
                event,
                record_map.get(str(event.payload.get("node_execution_id"))),
            )
            for event in selected
        )
        return DebugPage[dict[str, Any]](
            through_sequence=through_sequence,
            items=items,
            next_cursor=(
                encode_debug_cursor(
                    invocation_id=str(invocation_id),
                    query="node_executions",
                    through_sequence=through_sequence,
                    position=selected[-1].sequence,
                )
                if has_more and selected
                else None
            ),
            has_more=has_more,
        )

    async def node_execution(
        self,
        invocation_id: UUID,
        node_execution_id: UUID,
        *,
        through_sequence: int | None = None,
    ) -> dict[str, Any]:
        """Return bounded detail for one logical NodeExecution."""

        boundary = await self._observed_sequence(
            invocation_id,
            requested=through_sequence,
        )
        record_map = await self._node_record_map(
            invocation_id,
            observed_sequence=boundary,
        )
        record = record_map.get(str(node_execution_id))
        if record is None:
            raise KeyError(f"Unknown NodeExecution: {node_execution_id}")
        start = await self.store.aget_trace_runtime_event_by_subject(
            invocation_id=invocation_id,
            subject_type="node",
            subject_id=str(node_execution_id),
            event_name="node.running",
            through_sequence=boundary,
        )
        return _node_execution_detail(
            invocation_id,
            boundary,
            record,
            start_sequence=start.sequence if start is not None else None,
        )

    async def edge_evaluations(
        self,
        invocation_id: UUID,
        *,
        cursor: str | None = None,
        through_sequence: int | None = None,
        limit: int = 20,
    ) -> DebugPage[dict[str, Any]]:
        """Page individual Edge evaluations in Runtime sequence order."""

        return await self._event_kind_page(
            invocation_id,
            query="edge_evaluations",
            event_name="edge.evaluated",
            summary=_edge_evaluation_summary,
            cursor=cursor,
            through_sequence=through_sequence,
            limit=limit,
        )

    async def edge_evaluation(
        self,
        invocation_id: UUID,
        edge_evaluation_id: str,
        *,
        through_sequence: int | None = None,
    ) -> dict[str, Any]:
        boundary = await self._observed_sequence(
            invocation_id,
            requested=through_sequence,
        )
        try:
            event_id = UUID(edge_evaluation_id)
        except ValueError as exc:
            raise KeyError(
                f"Unknown Edge evaluation: {edge_evaluation_id}"
            ) from exc
        event = await self.store.aget_trace_runtime_event_by_id(
            invocation_id=invocation_id,
            event_id=event_id,
            event_name="edge.evaluated",
            through_sequence=boundary,
        )
        if event is None:
            raise KeyError(f"Unknown Edge evaluation: {edge_evaluation_id}")
        return {
            **_edge_evaluation_summary(event),
            "through_sequence": boundary,
            "payload": summarize_value(
                event.payload,
                detail_ref=f"runtime_event:{event.id}:payload",
            ).model_dump(mode="json"),
            "operation_count": len(event.operations or ()),
        }

    async def operator_calls(
        self,
        invocation_id: UUID,
        *,
        cursor: str | None = None,
        through_sequence: int | None = None,
        limit: int = 20,
    ) -> DebugPage[dict[str, Any]]:
        """Page logical direct or parallel Operator Calls."""

        return await self._event_kind_page(
            invocation_id,
            query="operator_calls",
            event_name="operator_call.completed",
            summary=_operator_call_summary,
            cursor=cursor,
            through_sequence=through_sequence,
            limit=limit,
        )

    async def operator_call(
        self,
        invocation_id: UUID,
        operator_call_id: UUID,
        *,
        through_sequence: int | None = None,
    ) -> dict[str, Any]:
        boundary = await self._observed_sequence(
            invocation_id,
            requested=through_sequence,
        )
        event = await self.store.aget_trace_runtime_event_by_subject(
            invocation_id=invocation_id,
            subject_type="operator_call",
            subject_id=str(operator_call_id),
            event_name="operator_call.completed",
            through_sequence=boundary,
        )
        if event is None:
            raise KeyError(f"Unknown Operator Call: {operator_call_id}")
        return {
            **_operator_call_summary(event),
            "through_sequence": boundary,
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

    async def runtime_state(
        self,
        invocation_id: UUID,
        *,
        through_sequence: int | None = None,
        path: str | None = None,
    ) -> dict[str, Any]:
        """Summarize one Full-mode Runtime State boundary or JSON-pointer path."""

        record, _ = await self._invocation_record(invocation_id)
        if record["event_mode"] != "full":
            raise ValueError(
                "Historical Runtime state is available only in full mode."
            )
        boundary = await self._observed_sequence(
            invocation_id,
            requested=through_sequence,
        )
        state, rebuilt_sequence = await self._rebuild_runtime_state(
            invocation_id,
            through_sequence=boundary,
        )
        if rebuilt_sequence != boundary:
            raise ValueError(
                "Runtime state cannot be rebuilt through the requested "
                f"sequence: rebuilt {rebuilt_sequence}, requested {boundary}."
            )
        if path is not None:
            value = _resolve_json_pointer(state, path)
            return {
                "invocation_id": str(invocation_id),
                "through_sequence": boundary,
                "path": path,
                "value": summarize_value(
                    value,
                    detail_ref=(
                        f"runtime_state:{invocation_id}:{boundary}:{path}"
                    ),
                ).model_dump(mode="json"),
            }
        invocation_state = state.get("invocation", {})
        session_state = state.get("session", {})
        return {
            "invocation_id": str(invocation_id),
            "through_sequence": boundary,
            "path": None,
            "invocation_state": invocation_state.get("state"),
            "session_context": summarize_value(
                session_state.get("context"),
                detail_ref=(
                    f"runtime_state:{invocation_id}:{boundary}:"
                    "/session/context"
                ),
            ).model_dump(mode="json"),
            "invocation_context": summarize_value(
                invocation_state.get("context"),
                detail_ref=(
                    f"runtime_state:{invocation_id}:{boundary}:"
                    "/invocation/context"
                ),
            ).model_dump(mode="json"),
            "result": (
                None
                if invocation_state.get("result") is None
                else summarize_value(
                    invocation_state["result"],
                    detail_ref=(
                        f"runtime_state:{invocation_id}:{boundary}:"
                        "/invocation/result"
                    ),
                ).model_dump(mode="json")
            ),
            "node_execution_count": len(state.get("node_executions", ())),
        }

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

    async def _select_runtime_events(
        self,
        invocation_id: UUID,
        *,
        after_sequence: int,
        through_sequence: int,
        limit: int,
        event_names: frozenset[str] | None = None,
    ) -> tuple[tuple[Any, ...], bool]:
        """Scan bounded forward pages while retaining only requested facts."""

        selected: list[Any] = []
        scan_position = after_sequence
        while len(selected) <= limit and scan_position < through_sequence:
            page = await self.store.alist_trace_runtime_events(
                invocation_id=invocation_id,
                after_sequence=scan_position,
                limit=min(500, max(32, (limit + 1) * 4)),
                event_names=(
                    None
                    if event_names is None
                    else tuple(sorted(event_names))
                ),
            )
            if not page:
                break
            previous_position = scan_position
            for event in page:
                if event.sequence > through_sequence:
                    scan_position = through_sequence
                    break
                scan_position = event.sequence
                if event_names is None or event.event_name in event_names:
                    selected.append(event)
                    if len(selected) > limit:
                        break
            if scan_position <= previous_position or scan_position >= through_sequence:
                break
        return tuple(selected[:limit]), len(selected) > limit

    async def _event_kind_page(
        self,
        invocation_id: UUID,
        *,
        query: str,
        event_name: str,
        summary: Any,
        cursor: str | None,
        through_sequence: int | None,
        limit: int,
    ) -> DebugPage[dict[str, Any]]:
        limit = _page_limit(limit)
        boundary, position = await self._page_boundary(
            invocation_id,
            query=query,
            cursor=cursor,
            through_sequence=through_sequence,
            filters={},
        )
        selected, has_more = await self._select_runtime_events(
            invocation_id,
            after_sequence=position,
            through_sequence=boundary,
            limit=limit,
            event_names=frozenset({event_name}),
        )
        return DebugPage[dict[str, Any]](
            through_sequence=boundary,
            items=tuple(summary(event) for event in selected),
            next_cursor=(
                encode_debug_cursor(
                    invocation_id=str(invocation_id),
                    query=query,
                    through_sequence=boundary,
                    position=selected[-1].sequence,
                )
                if has_more and selected
                else None
            ),
            has_more=has_more,
        )

    async def _node_record_map(
        self,
        invocation_id: UUID,
        *,
        observed_sequence: int,
    ) -> dict[str, dict[str, Any]]:
        invocation = self.store.invocations.get(invocation_id)
        if invocation is not None:
            return {
                str(execution.id): execution.to_record(invocation.id)
                for execution in invocation.node_executions
            }
        record, _ = await self._invocation_record(invocation_id)
        if record["event_mode"] == "full":
            state, rebuilt_sequence = await self._rebuild_runtime_state(
                invocation_id,
                through_sequence=observed_sequence,
            )
            if rebuilt_sequence != observed_sequence:
                raise ValueError(
                    "NodeExecution state cannot be rebuilt through the "
                    f"requested sequence: rebuilt {rebuilt_sequence}, "
                    f"requested {observed_sequence}."
                )
            return {
                str(value["id"]): dict(value)
                for value in state.get("node_executions", ())
            }

        records, snapshot_sequence = await self._node_records(
            invocation_id,
            None,
            observed_sequence=observed_sequence,
        )
        values = {str(value["id"]): dict(value) for value in records}
        await self._apply_standard_node_tail(
            invocation_id,
            values,
            after_sequence=snapshot_sequence,
            through_sequence=observed_sequence,
        )
        return values

    async def _apply_standard_node_tail(
        self,
        invocation_id: UUID,
        values: dict[str, dict[str, Any]],
        *,
        after_sequence: int,
        through_sequence: int,
    ) -> None:
        cursor = after_sequence
        while cursor < through_sequence:
            page = await self.store.alist_trace_runtime_events(
                invocation_id=invocation_id,
                after_sequence=cursor,
                limit=500,
            )
            if not page:
                return
            previous = cursor
            for event in page:
                if event.sequence > through_sequence:
                    return
                cursor = event.sequence
                payload = event.payload
                execution_id = payload.get("node_execution_id")
                if event.event_name == "edge.evaluated":
                    execution_id = payload.get("source_execution_id")
                if execution_id is None:
                    continue
                key = str(execution_id)
                if event.event_name == "node.running":
                    values.setdefault(
                        key,
                        _standard_node_record_from_start(event),
                    )
                value = values.get(key)
                if value is None:
                    continue
                if event.event_name.startswith("node."):
                    value["state"] = str(
                        payload.get("state") or event.status or value["state"]
                    )
                    value["updated_at_ms"] = event.occurred_at_ms
                    if value["state"] != "running":
                        value["ended_at_ms"] = event.occurred_at_ms
                    if payload.get("error") is not None:
                        value["error"] = payload["error"]
                elif event.event_name == "operator_call.completed":
                    calls = value.setdefault("operator_executions", [])
                    call_id = str(payload.get("operator_call_id"))
                    if not any(str(call.get("id")) == call_id for call in calls):
                        calls.append(
                            {
                                "id": call_id,
                                "type": (
                                    "parallel" if payload.get("kind") else "direct"
                                ),
                                "state": payload.get("state") or event.status,
                                "reason": payload.get("reason"),
                                "summary": payload.get("summary"),
                                "error": payload.get("error"),
                            }
                        )
                elif event.event_name == "edge.evaluated":
                    edges = value.setdefault("edge_evaluations", [])
                    evaluation_id = _edge_evaluation_identity(event)
                    if not any(
                        str(edge.get("id")) == evaluation_id for edge in edges
                    ):
                        edges.append(
                            {
                                "id": evaluation_id,
                                "edge_id": payload.get("edge_id"),
                                "target_node_id": payload.get("target_node_id"),
                                "state": payload.get("state") or event.status,
                                "selected": bool(payload.get("selected", False)),
                                "reason": payload.get("reason"),
                                "elapsed_ns": event.elapsed_ns,
                                "updated_at_ms": event.occurred_at_ms,
                            }
                        )
            if cursor <= previous:
                return

    async def _rebuild_runtime_state(
        self,
        invocation_id: UUID,
        *,
        through_sequence: int,
    ) -> tuple[dict[str, Any], int]:
        snapshot = await self.store.aload_trace_execution_snapshot(
            invocation_id,
            at_or_before_sequence=through_sequence,
        )
        if snapshot is None:
            raise KeyError(
                f"No execution snapshot for Invocation: {invocation_id}"
            )
        state = deepcopy(snapshot.state)
        cursor = snapshot.through_sequence
        while cursor < through_sequence:
            page = await self.store.alist_trace_runtime_events(
                invocation_id=invocation_id,
                after_sequence=cursor,
                limit=min(500, through_sequence - cursor),
            )
            if not page:
                break
            previous = cursor
            for event in page:
                if event.sequence > through_sequence:
                    break
                if event.sequence != cursor + 1:
                    raise ValueError(
                        "RuntimeEvent journal is not contiguous: expected "
                        f"{cursor + 1}, got {event.sequence}."
                    )
                if event.operations is None:
                    raise ValueError(
                        f"Full RuntimeEvent {event.sequence} has no operations."
                    )
                state = apply_state_operations(state, event.operations)
                cursor = event.sequence
            if cursor <= previous:
                break
        return state, cursor

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


def _node_execution_summary(
    start_event: Any,
    record: Mapping[str, Any] | None,
) -> dict[str, Any]:
    payload = start_event.payload
    value = record or {}
    error = _report_error(value.get("error"))
    return {
        "node_execution_id": str(
            payload.get("node_execution_id") or start_event.subject_id
        ),
        "node_id": str(payload.get("node_id") or value.get("node_id") or ""),
        "start_sequence": start_event.sequence,
        "execution_sequence": value.get("sequence"),
        "state": str(value.get("state") or start_event.status or "running"),
        "started_at_ms": value.get("started_at_ms", start_event.occurred_at_ms),
        "ended_at_ms": value.get("ended_at_ms"),
        "duration_ns": (value.get("resource_usage") or {}).get("duration_ns"),
        "operator_call_count": len(value.get("operator_executions", ())),
        "edge_evaluation_count": len(value.get("edge_evaluations", ())),
        "recovery_attempt": int(value.get("recovery_attempt", 0)),
        "error": error.model_dump(mode="json") if error is not None else None,
    }


def _standard_node_record_from_start(event: Any) -> dict[str, Any]:
    payload = event.payload
    return {
        "id": str(payload.get("node_execution_id") or event.subject_id),
        "invocation_id": str(event.invocation_id),
        "node_id": str(payload.get("node_id") or ""),
        "sequence": 0,
        "state": str(payload.get("state") or event.status or "running"),
        "input": None,
        "output": None,
        "error": None,
        "recovery_of_execution_id": None,
        "recovery_attempt": 0,
        "incoming_activations": [],
        "execution_scope": [],
        "operator_executions": [],
        "edge_evaluations": [],
        "resource_usage": {},
        "started_at_ms": event.occurred_at_ms,
        "ended_at_ms": None,
        "created_at_ms": event.occurred_at_ms,
        "updated_at_ms": event.occurred_at_ms,
    }


def _node_execution_detail(
    invocation_id: UUID,
    through_sequence: int,
    record: Mapping[str, Any],
    *,
    start_sequence: int | None,
) -> dict[str, Any]:
    execution_id = str(record["id"])
    error = _report_error(record.get("error"))
    return {
        "invocation_id": str(invocation_id),
        "through_sequence": through_sequence,
        "node_execution_id": execution_id,
        "node_id": str(record["node_id"]),
        "start_sequence": start_sequence,
        "execution_sequence": int(record["sequence"]),
        "state": str(record["state"]),
        "started_at_ms": record.get("started_at_ms"),
        "ended_at_ms": record.get("ended_at_ms"),
        "created_at_ms": record.get("created_at_ms"),
        "updated_at_ms": record.get("updated_at_ms"),
        "recovery_attempt": int(record.get("recovery_attempt", 0)),
        "recovery_of_execution_id": record.get("recovery_of_execution_id"),
        "input": summarize_value(
            record.get("input"),
            detail_ref=f"node_execution:{execution_id}:input",
        ).model_dump(mode="json"),
        "output": summarize_value(
            record.get("output"),
            detail_ref=f"node_execution:{execution_id}:output",
        ).model_dump(mode="json"),
        "error": error.model_dump(mode="json") if error is not None else None,
        "resource_usage": dict(record.get("resource_usage") or {}),
        "operator_call_count": len(record.get("operator_executions", ())),
        "edge_evaluation_count": len(record.get("edge_evaluations", ())),
        "incoming_activation_count": len(record.get("incoming_activations", ())),
        "execution_scope": summarize_value(
            record.get("execution_scope", ()),
            detail_ref=f"node_execution:{execution_id}:execution_scope",
        ).model_dump(mode="json"),
    }


def _edge_evaluation_identity(event: Any) -> str:
    return str(event.id)


def _edge_evaluation_summary(event: Any) -> dict[str, Any]:
    payload = event.payload
    return {
        "edge_evaluation_id": _edge_evaluation_identity(event),
        "event_sequence": event.sequence,
        "edge_id": str(payload.get("edge_id") or event.subject_id),
        "source_node_id": payload.get("node_id"),
        "source_execution_id": payload.get("source_execution_id"),
        "target_node_id": payload.get("target_node_id"),
        "state": str(payload.get("state") or event.status or "evaluated"),
        "selected": bool(payload.get("selected", False)),
        "reason": payload.get("reason"),
        "occurred_at_ms": event.occurred_at_ms,
        "elapsed_ns": event.elapsed_ns,
    }


def _operator_call_summary(event: Any) -> dict[str, Any]:
    payload = event.payload
    error = _report_error(payload.get("error"))
    summary = payload.get("summary") or {}
    return {
        "operator_call_id": str(
            payload.get("operator_call_id") or event.subject_id
        ),
        "event_sequence": event.sequence,
        "node_id": payload.get("node_id"),
        "node_execution_id": payload.get("node_execution_id"),
        "operator_id": payload.get("operator_id"),
        "operator_ids": list(payload.get("operator_ids", ())),
        "kind": str(payload.get("kind") or "direct"),
        "reason": payload.get("reason"),
        "state": str(payload.get("state") or event.status or "completed"),
        "occurred_at_ms": event.occurred_at_ms,
        "elapsed_ns": event.elapsed_ns,
        "timing": dict(event.timing),
        "streaming": bool(payload.get("streaming", False)),
        "stream_chunk_count": int(payload.get("stream_chunk_count", 0)),
        "call_count": int(summary.get("call_count", 1)),
        "attempt_count": int(summary.get("attempt_count", 1)),
        "retry_count": int(summary.get("retry_count", 0)),
        "fallback_count": int(summary.get("fallback_count", 0)),
        "error": error.model_dump(mode="json") if error is not None else None,
        "has_input": event.input is not None,
        "has_output": event.output is not None,
    }


def _resolve_json_pointer(value: Any, path: str) -> Any:
    if path == "":
        return value
    if not path.startswith("/"):
        raise ValueError("Runtime State path must be a JSON pointer.")
    current = value
    for raw_segment in path[1:].split("/"):
        segment = raw_segment.replace("~1", "/").replace("~0", "~")
        if isinstance(current, Mapping):
            if segment not in current:
                raise KeyError(f"Unknown Runtime State path: {path}")
            current = current[segment]
            continue
        if isinstance(current, list):
            try:
                index = int(segment)
            except ValueError as exc:
                raise KeyError(f"Unknown Runtime State path: {path}") from exc
            if index < 0 or index >= len(current):
                raise KeyError(f"Unknown Runtime State path: {path}")
            current = current[index]
            continue
        raise KeyError(f"Unknown Runtime State path: {path}")
    return current


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

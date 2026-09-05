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
    InvocationComparison,
    InvocationDifference,
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

    async def compare(
        self,
        baseline_invocation_id: UUID,
        candidate_invocation_id: UUID,
    ) -> InvocationComparison:
        """Compare two stable projections without interpreting business value."""

        baseline = await self.report_when_stable(baseline_invocation_id)
        candidate = await self.report_when_stable(candidate_invocation_id)
        if baseline.event_mode != candidate.event_mode:
            raise ValueError(
                "Invocation Comparison requires matching Event modes: "
                f"baseline={baseline.event_mode}, candidate={candidate.event_mode}."
            )
        if baseline.event_mode == "minimal":
            raise ValueError(
                "Invocation Comparison requires Standard or Full mode; "
                "Minimal mode has no graph execution evidence."
            )
        evidence_mode = baseline.event_mode
        input_equal = _summary_equal(baseline.input, candidate.input)
        entry_node_equal = baseline.entry_node_id == candidate.entry_node_id
        workflow_equal = baseline.workflow_id == candidate.workflow_id
        differences: list[InvocationDifference] = []

        if not input_equal:
            differences.append(
                InvocationDifference(
                    category="request",
                    key="input",
                    baseline=_summary_identity(baseline.input),
                    candidate=_summary_identity(candidate.input),
                )
            )
        if not entry_node_equal:
            differences.append(
                InvocationDifference(
                    category="request",
                    key="entry_node_id",
                    baseline=baseline.entry_node_id,
                    candidate=candidate.entry_node_id,
                )
            )
        if baseline.state != candidate.state:
            differences.append(
                InvocationDifference(
                    category="outcome",
                    key="state",
                    baseline=baseline.state,
                    candidate=candidate.state,
                )
            )
        if not _summary_equal(baseline.result, candidate.result):
            differences.append(
                InvocationDifference(
                    category="outcome",
                    key="result",
                    baseline=_summary_identity(baseline.result),
                    candidate=_summary_identity(candidate.result),
                )
            )
        if not _error_equal(baseline.error, candidate.error):
            differences.append(
                InvocationDifference(
                    category="outcome",
                    key="error",
                    baseline=_error_identity(baseline.error),
                    candidate=_error_identity(candidate.error),
                )
            )

        if workflow_equal and evidence_mode != "minimal":
            baseline_nodes = await self._node_record_map(
                baseline_invocation_id,
                observed_sequence=baseline.observed_sequence,
            )
            candidate_nodes = await self._node_record_map(
                candidate_invocation_id,
                observed_sequence=candidate.observed_sequence,
            )
            differences.extend(
                _execution_differences(
                    baseline_nodes,
                    candidate_nodes,
                    include_values=evidence_mode == "full",
                )
            )
        for event_type in sorted(
            set(baseline.user_event_counts) | set(candidate.user_event_counts)
        ):
            baseline_count = baseline.user_event_counts.get(event_type, 0)
            candidate_count = candidate.user_event_counts.get(event_type, 0)
            if baseline_count != candidate_count:
                differences.append(
                    InvocationDifference(
                        category="user_event",
                        key=event_type,
                        baseline=baseline_count,
                        candidate=candidate_count,
                    )
                )

        warnings = [*baseline.warnings, *candidate.warnings]
        if not workflow_equal:
            warnings.append(
                EvidenceWarning(
                    code="WORKFLOW_IDS_DIFFER",
                    message=(
                        "Invocations belong to different Workflows; graph "
                        "execution evidence was not aligned."
                    ),
                    detail={
                        "baseline_workflow_id": baseline.workflow_id,
                        "candidate_workflow_id": candidate.workflow_id,
                    },
                )
            )
        if workflow_equal and (not input_equal or not entry_node_equal):
            warnings.append(
                EvidenceWarning(
                    code="REQUESTS_DIFFER",
                    message=(
                        "The Invocations did not start from the same input and "
                        "entry boundary, so differences are not attributable "
                        "only to the Workflow revision."
                    ),
                )
            )

        status = (
            "incompatible"
            if not workflow_equal
            else "partially_comparable"
            if not input_equal or not entry_node_equal or warnings
            else "comparable"
        )
        visible = tuple(differences[:12])
        return InvocationComparison(
            source=self.source,
            status=status,
            baseline_invocation_id=str(baseline_invocation_id),
            candidate_invocation_id=str(candidate_invocation_id),
            workflow_id=baseline.workflow_id if workflow_equal else None,
            baseline_workflow_revision_id=baseline.workflow_revision_id,
            candidate_workflow_revision_id=candidate.workflow_revision_id,
            evidence_mode=evidence_mode,
            baseline_observed_sequence=baseline.observed_sequence,
            candidate_observed_sequence=candidate.observed_sequence,
            input_equal=input_equal,
            entry_node_equal=entry_node_equal,
            state_equal=baseline.state == candidate.state,
            result_equal=_summary_equal(baseline.result, candidate.result),
            error_equal=_error_equal(baseline.error, candidate.error),
            baseline_state=baseline.state,
            candidate_state=candidate.state,
            baseline_result=baseline.result,
            candidate_result=candidate.result,
            baseline_error=baseline.error,
            candidate_error=candidate.error,
            baseline_duration_ms=max(
                0, baseline.updated_at_ms - baseline.created_at_ms
            ),
            candidate_duration_ms=max(
                0, candidate.updated_at_ms - candidate.created_at_ms
            ),
            node_execution_delta=(
                candidate.node_execution_count - baseline.node_execution_count
            ),
            edge_evaluation_delta=(
                candidate.edge_evaluation_count - baseline.edge_evaluation_count
            ),
            operator_call_delta=(
                candidate.operator_call_count - baseline.operator_call_count
            ),
            retry_delta=candidate.retry_count - baseline.retry_count,
            fallback_delta=candidate.fallback_count - baseline.fallback_count,
            timeout_delta=candidate.timeout_count - baseline.timeout_count,
            difference_count=len(differences),
            differences=visible,
            differences_truncated=len(visible) < len(differences),
            warnings=tuple(_unique_warnings(warnings)),
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
            event_names=frozenset({"edge.evaluated"}),
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
        node_execution_id: UUID | None = None,
        cursor: str | None = None,
        through_sequence: int | None = None,
        limit: int = 20,
    ) -> DebugPage[dict[str, Any]]:
        """Page actual Operator Calls, optionally within one NodeExecution."""

        return await self._event_kind_page(
            invocation_id,
            query="operator_calls",
            event_names=frozenset(
                {
                    "operator_call.completed",
                    "operator_call.failed",
                    "operator_call.cancelled",
                    "operator_call.interrupted",
                }
            ),
            summary=_operator_call_summary,
            node_execution_id=node_execution_id,
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
        node_execution_id: UUID | None = None,
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
                node_execution_id=node_execution_id,
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
        event_names: frozenset[str],
        summary: Any,
        node_execution_id: UUID | None = None,
        cursor: str | None,
        through_sequence: int | None,
        limit: int,
    ) -> DebugPage[dict[str, Any]]:
        limit = _page_limit(limit)
        filters = {
            "node_execution_id": (
                str(node_execution_id)
                if node_execution_id is not None
                else None
            )
        }
        boundary, position = await self._page_boundary(
            invocation_id,
            query=query,
            cursor=cursor,
            through_sequence=through_sequence,
            filters=filters,
        )
        selected, has_more = await self._select_runtime_events(
            invocation_id,
            after_sequence=position,
            through_sequence=boundary,
            limit=limit,
            event_names=event_names,
            node_execution_id=node_execution_id,
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
                    filters=filters,
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
                    if payload.get("operator_summary") is not None:
                        value["operator_summary"] = payload["operator_summary"]
                    if payload.get("parallel_summary") is not None:
                        value["parallel_summary"] = payload["parallel_summary"]
                elif event.event_name.startswith("operator_call."):
                    summary = value.setdefault("operator_summary", {})
                    summary["attempt_count"] = int(
                        summary.get("attempt_count", 0)
                    ) + 1
                    state = payload.get("state") or event.status
                    summary["success_count"] = int(
                        summary.get("success_count", 0)
                    ) + int(state == "completed")
                    summary["failure_count"] = int(
                        summary.get("failure_count", 0)
                    ) + int(state == "failed")
                    summary["cancelled_count"] = int(
                        summary.get("cancelled_count", 0)
                    ) + int(state == "cancelled")
                    summary["interrupted_count"] = int(
                        summary.get("interrupted_count", 0)
                    ) + int(state == "interrupted")
                    reason = payload.get("reason")
                    summary["retry_count"] = int(
                        summary.get("retry_count", 0)
                    ) + int(reason == "retry")
                    summary["fallback_count"] = int(
                        summary.get("fallback_count", 0)
                    ) + int(reason == "fallback")
                    summary["timeout_count"] = int(
                        summary.get("timeout_count", 0)
                    ) + int(
                        (payload.get("error") or {}).get("code")
                        == "OPERATOR_TIMEOUT"
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


def _summary_equal(left: Any | None, right: Any | None) -> bool:
    if left is None or right is None:
        return left is right
    return left.type == right.type and left.digest == right.digest


def _summary_identity(value: Any | None) -> dict[str, Any] | None:
    if value is None:
        return None
    return {
        "type": value.type,
        "digest": value.digest,
        "shape": value.shape,
        "serialized_bytes": value.serialized_bytes,
    }


def _error_equal(left: Any | None, right: Any | None) -> bool:
    return _error_identity(left) == _error_identity(right)


def _error_identity(value: Any | None) -> dict[str, Any] | None:
    if value is None:
        return None
    return {
        "code": value.code,
        "message": value.message,
        "exception_type": value.exception_type,
    }


def _node_semantic_key(record: Mapping[str, Any]) -> str:
    scope = "/".join(
        f"{item.get('loop_region_id')}:{item.get('iteration')}"
        for item in record.get("execution_scope", ())
    )
    value = str(record.get("node_id") or "<unknown>")
    if scope:
        value = f"{value}@{scope}"
    recovery_attempt = int(record.get("recovery_attempt", 0))
    return value if recovery_attempt == 0 else f"{value}#recovery:{recovery_attempt}"


def _semantic_node_map(
    values: Mapping[str, Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    for record in values.values():
        key = _node_semantic_key(record)
        if key in result:
            # A duplicate should not normally exist, but preserving both makes
            # comparison deterministic without guessing which execution owns
            # the semantic coordinate.
            sequence = int(record.get("sequence", 0))
            key = f"{key}#execution:{sequence}"
        result[key] = record
    return result


def _execution_differences(
    baseline_values: Mapping[str, Mapping[str, Any]],
    candidate_values: Mapping[str, Mapping[str, Any]],
    *,
    include_values: bool,
) -> list[InvocationDifference]:
    baseline = _semantic_node_map(baseline_values)
    candidate = _semantic_node_map(candidate_values)
    differences: list[InvocationDifference] = []
    for key in sorted(set(baseline) | set(candidate)):
        left = baseline.get(key)
        right = candidate.get(key)
        left_ref = (
            None if left is None else f"node_execution:{left.get('id')}"
        )
        right_ref = (
            None if right is None else f"node_execution:{right.get('id')}"
        )
        if left is None or right is None:
            differences.append(
                InvocationDifference(
                    category="node",
                    key=key,
                    baseline=None if left is None else _node_identity(left),
                    candidate=None if right is None else _node_identity(right),
                    baseline_ref=left_ref,
                    candidate_ref=right_ref,
                )
            )
            continue
        left_identity = _node_identity(left, include_values=include_values)
        right_identity = _node_identity(right, include_values=include_values)
        if left_identity != right_identity:
            differences.append(
                InvocationDifference(
                    category="node",
                    key=key,
                    baseline=left_identity,
                    candidate=right_identity,
                    baseline_ref=left_ref,
                    candidate_ref=right_ref,
                )
            )
        differences.extend(_edge_differences(key, left, right))
        differences.extend(_operator_differences(key, left, right))
    return differences


def _node_identity(
    record: Mapping[str, Any],
    *,
    include_values: bool = False,
) -> dict[str, Any]:
    error = record.get("error") or {}
    value: dict[str, Any] = {
        "state": record.get("state"),
        "error_code": error.get("code"),
        "recovery_attempt": int(record.get("recovery_attempt", 0)),
    }
    if include_values:
        value["output"] = _summary_identity(
            summarize_value(record.get("output"))
        )
    return value


def _edge_differences(
    node_key: str,
    baseline: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> list[InvocationDifference]:
    left = {
        str(item.get("edge_id")): item
        for item in baseline.get("edge_evaluations", ())
    }
    right = {
        str(item.get("edge_id")): item
        for item in candidate.get("edge_evaluations", ())
    }
    values: list[InvocationDifference] = []
    for edge_id in sorted(set(left) | set(right)):
        left_value = left.get(edge_id)
        right_value = right.get(edge_id)
        left_identity = _edge_identity(left_value)
        right_identity = _edge_identity(right_value)
        if left_identity != right_identity:
            values.append(
                InvocationDifference(
                    category="edge",
                    key=f"{node_key}:{edge_id}",
                    baseline=left_identity,
                    candidate=right_identity,
                    baseline_ref=(
                        None
                        if left_value is None
                        else f"edge_evaluation:{left_value.get('id')}"
                    ),
                    candidate_ref=(
                        None
                        if right_value is None
                        else f"edge_evaluation:{right_value.get('id')}"
                    ),
                )
            )
    return values


def _edge_identity(value: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    return {
        "state": value.get("state"),
        "selected": bool(value.get("selected", False)),
        "target_node_id": value.get("target_node_id"),
        "reason": value.get("reason"),
    }


def _operator_differences(
    node_key: str,
    baseline: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> list[InvocationDifference]:
    left_identity = _operator_identity(baseline)
    right_identity = _operator_identity(candidate)
    if left_identity == right_identity:
        return []
    return [
        InvocationDifference(
            category="operator_call",
            key=f"{node_key}:summary",
            baseline=left_identity,
            candidate=right_identity,
        )
    ]


def _operator_identity(value: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    summary = value.get("operator_summary") or {}
    parallel = value.get("parallel_summary") or {}
    return {
        "state": value.get("state"),
        "kind": parallel.get("kind"),
        "call_count": parallel.get("call_count"),
        "attempt_count": summary.get("attempt_count"),
        "retry_count": summary.get("retry_count"),
        "fallback_count": summary.get("fallback_count"),
        "failure_count": summary.get("failure_count"),
        "cancelled_count": summary.get("cancelled_count"),
    }


def _unique_warnings(values: list[EvidenceWarning]) -> list[EvidenceWarning]:
    selected: list[EvidenceWarning] = []
    seen: set[tuple[str, str]] = set()
    for value in values:
        key = (value.code, value.message)
        if key not in seen:
            seen.add(key)
            selected.append(value)
    return selected


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
        summary = node.get("operator_summary") or {}
        counts["operator_call_count"] += int(summary.get("attempt_count", 0))
        counts["retry_count"] += int(summary.get("retry_count", 0))
        counts["fallback_count"] += int(summary.get("fallback_count", 0))
        counts["timeout_count"] += int(summary.get("timeout_count", 0))
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
        "operator_call_count": int(
            (value.get("operator_summary") or {}).get("attempt_count", 0)
        ),
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
        "operator_summary": {},
        "parallel_summary": None,
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
        "operator_call_count": int(
            (record.get("operator_summary") or {}).get("attempt_count", 0)
        ),
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
    return {
        "operator_call_id": str(
            payload.get("operator_call_id") or event.subject_id
        ),
        "event_sequence": event.sequence,
        "node_id": payload.get("node_id"),
        "node_execution_id": payload.get("node_execution_id"),
        "operator_id": payload.get("operator_id"),
        "kind": str(payload.get("kind") or "direct"),
        "call_no": int(payload.get("call_no", 0)),
        "unit_index": payload.get("unit_index"),
        "unit_attempt_no": int(payload.get("unit_attempt_no", 1)),
        "reason": payload.get("reason"),
        "state": str(payload.get("state") or event.status or "completed"),
        "started_at_ms": payload.get("started_at_ms"),
        "occurred_at_ms": event.occurred_at_ms,
        "elapsed_ns": event.elapsed_ns,
        "timing": dict(event.timing),
        "streaming": bool(payload.get("streaming", False)),
        "stream_chunk_count": int(payload.get("stream_chunk_count", 0)),
        "call_count": 1,
        "attempt_count": 1,
        "retry_count": int(payload.get("reason") == "retry"),
        "fallback_count": int(payload.get("reason") == "fallback"),
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

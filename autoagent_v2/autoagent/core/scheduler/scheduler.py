"""Pure DAG scheduling planner over Workflow IR and Runtime State."""

from __future__ import annotations

from collections import deque

from ..errors import RuntimeTransitionError
from ..runtime.events import (
    NodeOccurrenceCompleted,
    NodeOccurrenceFailed,
    NodeOccurrenceStarted,
    RuntimeErrorInfo,
    SchedulerInitialized,
)
from ..runtime.scheduling import (
    Activation,
    EdgeResolution,
    OccurrencePlan,
    SchedulerDelta,
    occurrence_key,
    resolution_key,
)
from ..runtime.state import RuntimeState
from ..workflow import EdgeIR, WorkflowIR
from ._routing import validate_edge_selection


class DAGScheduler:
    """Plan root-scope transitions for acyclic Workflows."""

    def initialize(
        self, workflow: WorkflowIR, state: RuntimeState
    ) -> SchedulerInitialized:
        invocation = _running_invocation(state)
        if workflow.loop_regions:
            raise RuntimeTransitionError(
                "DAG_SCHEDULER_LOOP_UNSUPPORTED",
                "Loop Workflow requires the scope-aware Scheduler.",
            )
        if invocation.scheduler.initialized:
            raise RuntimeTransitionError(
                "SCHEDULER_ALREADY_INITIALIZED", "Scheduler is already initialized."
            )
        entry = invocation.entry_node_id
        if entry not in workflow.entry_node_ids:
            raise RuntimeTransitionError(
                "INVOCATION_ENTRY_INVALID",
                f"Node {entry!r} is not a Workflow Entry.",
            )

        entry_plan = OccurrencePlan(occurrence_key(entry), entry, ())
        skipped_entries = tuple(
            OccurrencePlan(occurrence_key(node_id), node_id, ())
            for node_id in workflow.entry_node_ids
            if node_id != entry
        )
        initial_resolutions = tuple(
            self._skipped_outgoing(workflow, plan)
            for plan in skipped_entries
        )
        flattened = tuple(item for group in initial_resolutions for item in group)
        propagated = self._propagate(
            workflow,
            state,
            flattened,
            known_plans=(entry_plan, *skipped_entries),
        )
        return SchedulerInitialized(
            SchedulerDelta(
                resolutions=propagated.resolutions,
                ready=(entry_plan, *propagated.ready),
                skipped=(*skipped_entries, *propagated.skipped),
            )
        )

    def start(self, occurrence_id: str) -> NodeOccurrenceStarted:
        return NodeOccurrenceStarted(occurrence_id)

    def complete(
        self,
        workflow: WorkflowIR,
        state: RuntimeState,
        occurrence_id: str,
        output: object,
        *,
        selected_edge_ids: set[str] | frozenset[str] = frozenset(),
    ) -> NodeOccurrenceCompleted:
        occurrence = _running_occurrence(state, occurrence_id)
        resolutions = self._resolve_outgoing(
            workflow,
            occurrence.id,
            occurrence.node_id,
            "complete",
            selected_edge_ids,
        )
        delta = self._propagate(workflow, state, resolutions)
        return NodeOccurrenceCompleted(occurrence_id, output, delta)  # type: ignore[arg-type]

    def fail(
        self,
        workflow: WorkflowIR,
        state: RuntimeState,
        occurrence_id: str,
        error: RuntimeErrorInfo,
        *,
        selected_edge_ids: set[str] | frozenset[str] = frozenset(),
    ) -> NodeOccurrenceFailed:
        occurrence = _running_occurrence(state, occurrence_id)
        resolutions = self._resolve_outgoing(
            workflow,
            occurrence.id,
            occurrence.node_id,
            "error",
            selected_edge_ids,
        )
        delta = self._propagate(workflow, state, resolutions)
        return NodeOccurrenceFailed(occurrence_id, error, delta)

    def _resolve_outgoing(
        self,
        workflow: WorkflowIR,
        source_occurrence_id: str,
        source_node_id: str,
        source_status: str,
        selected_edge_ids: set[str] | frozenset[str],
    ) -> tuple[EdgeResolution, ...]:
        outgoing = workflow.outgoing(source_node_id)
        validate_edge_selection(outgoing, source_status, selected_edge_ids)
        return tuple(
            self._resolution(edge, source_occurrence_id, edge.id in selected_edge_ids)
            for edge in outgoing
        )

    def _skipped_outgoing(
        self, workflow: WorkflowIR, plan: OccurrencePlan
    ) -> tuple[EdgeResolution, ...]:
        return tuple(
            self._resolution(edge, plan.id, False)
            for edge in workflow.outgoing(plan.node_id)
        )

    @staticmethod
    def _resolution(
        edge: EdgeIR, source_occurrence_id: str, selected: bool
    ) -> EdgeResolution:
        activation = (
            Activation(edge.id, source_occurrence_id, edge.target)
            if selected
            else None
        )
        return EdgeResolution(edge.id, edge.target, (), selected, activation)

    def _propagate(
        self,
        workflow: WorkflowIR,
        state: RuntimeState,
        initial: tuple[EdgeResolution, ...],
        *,
        known_plans: tuple[OccurrencePlan, ...] = (),
    ) -> SchedulerDelta:
        invocation = _running_invocation(state)
        existing_occurrences = set(invocation.scheduler.occurrences)
        planned = {item.id for item in known_plans}
        resolutions = dict(invocation.scheduler.resolutions)
        new_resolutions: list[EdgeResolution] = []
        ready: list[OccurrencePlan] = []
        skipped: list[OccurrencePlan] = []
        consumed: list[str] = []
        targets: deque[str] = deque()

        def add_resolution(item: EdgeResolution) -> None:
            if item.id in resolutions:
                raise RuntimeTransitionError(
                    "EDGE_RESOLUTION_DUPLICATE",
                    f"Edge Resolution {item.id!r} already exists.",
                )
            resolutions[item.id] = item
            new_resolutions.append(item)
            targets.append(item.target_node_id)

        for item in initial:
            add_resolution(item)

        while targets:
            target = targets.popleft()
            target_occurrence = occurrence_key(target)
            if target_occurrence in existing_occurrences or target_occurrence in planned:
                continue
            incoming = workflow.incoming(target)
            values = [resolutions.get(resolution_key(edge.id)) for edge in incoming]
            if any(item is None for item in values):
                continue
            plan = OccurrencePlan(
                target_occurrence,
                target,
                (),
                tuple(resolution_key(edge.id) for edge in incoming),
            )
            planned.add(plan.id)
            for edge in incoming:
                key = resolution_key(edge.id)
                resolutions.pop(key, None)
                consumed.append(key)
            if any(item.selected for item in values if item is not None):
                ready.append(plan)
                continue
            skipped.append(plan)
            for item in self._skipped_outgoing(workflow, plan):
                add_resolution(item)

        return SchedulerDelta(
            resolutions=tuple(new_resolutions),
            consumed_resolution_ids=tuple(consumed),
            ready=tuple(ready),
            skipped=tuple(skipped),
        )


def _running_invocation(state: RuntimeState):
    invocation = state.invocation
    if invocation is None or invocation.status != "running":
        raise RuntimeTransitionError(
            "INVOCATION_NOT_RUNNING", "Scheduler requires a running Invocation."
        )
    return invocation


def _running_occurrence(state: RuntimeState, occurrence_id: str):
    invocation = _running_invocation(state)
    occurrence = invocation.scheduler.occurrences.get(occurrence_id)
    if occurrence is None or occurrence.status != "running":
        raise RuntimeTransitionError(
            "NODE_OCCURRENCE_NOT_RUNNING",
            f"Node Occurrence {occurrence_id!r} is not running.",
        )
    return occurrence

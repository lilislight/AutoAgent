"""Pure scope-aware Scheduler for DAGs containing natural Loop regions."""

from __future__ import annotations

from collections import deque

from ..errors import LoopControlError, RuntimeTransitionError
from ..runtime.events import (
    NodeOccurrenceCompleted,
    NodeOccurrenceFailed,
    RuntimeErrorInfo,
    SchedulerInitialized,
)
from ..runtime.scheduling import (
    Activation,
    EdgeResolution,
    ExecutionScope,
    LoopBoundaryResolution,
    LoopIteration,
    OccurrencePlan,
    SchedulerDelta,
    boundary_key,
    boundary_resolution_key,
    occurrence_key,
    resolution_key,
)
from ..runtime.state import RuntimeState
from ..workflow import EdgeIR, LoopRegionIR, WorkflowIR
from ._routing import validate_edge_selection
from .scheduler import DAGScheduler, _running_invocation, _running_occurrence


class Scheduler(DAGScheduler):
    """Plan deterministic DAG and Loop transitions without mutable cursor state."""

    def initialize(
        self, workflow: WorkflowIR, state: RuntimeState
    ) -> SchedulerInitialized:
        if not workflow.loop_regions:
            return super().initialize(workflow, state)
        invocation = _running_invocation(state)
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
        planner = _LoopPlanner(workflow, state)
        planner.plan_ready(entry, ())
        for node_id in workflow.entry_node_ids:
            if node_id != entry:
                planner.plan_skipped(node_id, ())
        planner.propagate()
        return SchedulerInitialized(planner.delta())

    def complete(
        self,
        workflow: WorkflowIR,
        state: RuntimeState,
        occurrence_id: str,
        output: object,
        *,
        selected_edge_ids: set[str] | frozenset[str] = frozenset(),
    ) -> NodeOccurrenceCompleted:
        if not workflow.loop_regions:
            return super().complete(
                workflow,
                state,
                occurrence_id,
                output,
                selected_edge_ids=selected_edge_ids,
            )
        occurrence = _running_occurrence(state, occurrence_id)
        planner = _LoopPlanner(workflow, state, terminal_occurrence_id=occurrence_id)
        planner.resolve_outgoing(
            occurrence.id,
            occurrence.node_id,
            occurrence.scope,
            "complete",
            selected_edge_ids,
        )
        planner.propagate()
        return NodeOccurrenceCompleted(occurrence_id, output, planner.delta())  # type: ignore[arg-type]

    def fail(
        self,
        workflow: WorkflowIR,
        state: RuntimeState,
        occurrence_id: str,
        error: RuntimeErrorInfo,
        *,
        selected_edge_ids: set[str] | frozenset[str] = frozenset(),
    ) -> NodeOccurrenceFailed:
        if not workflow.loop_regions:
            return super().fail(
                workflow,
                state,
                occurrence_id,
                error,
                selected_edge_ids=selected_edge_ids,
            )
        occurrence = _running_occurrence(state, occurrence_id)
        planner = _LoopPlanner(workflow, state, terminal_occurrence_id=occurrence_id)
        planner.resolve_outgoing(
            occurrence.id,
            occurrence.node_id,
            occurrence.scope,
            "error",
            selected_edge_ids,
        )
        planner.propagate()
        return NodeOccurrenceFailed(occurrence_id, error, planner.delta())

class _LoopPlanner:
    """Mutable transition-local projection; only its immutable delta is emitted."""

    def __init__(
        self,
        workflow: WorkflowIR,
        state: RuntimeState,
        *,
        terminal_occurrence_id: str | None = None,
    ) -> None:
        self.workflow = workflow
        self.scheduler = _running_invocation(state).scheduler
        self.terminal_occurrence_id = terminal_occurrence_id
        self.terminal_failed = False
        self.resolutions = dict(self.scheduler.resolutions)
        self.boundaries = dict(self.scheduler.boundary_resolutions)
        self.known_occurrences = set(self.scheduler.occurrences)
        self.new_resolutions: list[EdgeResolution] = []
        self.new_boundaries: list[LoopBoundaryResolution] = []
        self.closed_boundaries: list[str] = []
        self.consumed_resolutions: list[str] = []
        self.ready: list[OccurrencePlan] = []
        self.skipped: list[OccurrencePlan] = []
        self.revived: list[OccurrencePlan] = []
        self.targets: deque[tuple[str, ExecutionScope]] = deque()

    def delta(self) -> SchedulerDelta:
        return SchedulerDelta(
            resolutions=tuple(self.new_resolutions),
            boundary_resolutions=tuple(self.new_boundaries),
            closed_boundaries=tuple(self.closed_boundaries),
            consumed_resolution_ids=tuple(self.consumed_resolutions),
            ready=tuple(self.ready),
            skipped=tuple(self.skipped),
            revived=tuple(self.revived),
        )

    def plan_ready(
        self,
        node_id: str,
        scope: ExecutionScope,
        resolution_ids: tuple[str, ...] = (),
    ) -> None:
        plan = OccurrencePlan(
            occurrence_key(node_id, scope), node_id, scope, resolution_ids
        )
        if plan.id in self.known_occurrences:
            return
        self.known_occurrences.add(plan.id)
        self.ready.append(plan)

    def plan_skipped(self, node_id: str, scope: ExecutionScope) -> None:
        plan = OccurrencePlan(occurrence_key(node_id, scope), node_id, scope)
        if plan.id in self.known_occurrences:
            return
        self.known_occurrences.add(plan.id)
        self.skipped.append(plan)
        self._skip_outgoing(node_id, scope)

    def resolve_outgoing(
        self,
        source_occurrence_id: str,
        source_node_id: str,
        source_scope: ExecutionScope,
        source_status: str,
        selected_edge_ids: set[str] | frozenset[str],
    ) -> None:
        if source_status == "error":
            self.terminal_failed = True
        outgoing = self.workflow.outgoing(source_node_id)
        validate_edge_selection(outgoing, source_status, selected_edge_ids)
        self._validate_control(source_node_id, source_scope, selected_edge_ids)
        active_ids = {frame.loop_region_id for frame in source_scope}
        entered: dict[str, ExecutionScope] = {}
        for edge in outgoing:
            if edge.id not in selected_edge_ids:
                continue
            target_scope = _target_scope(self.workflow, edge, source_scope)
            for frame in target_scope:
                if frame.loop_region_id in active_ids:
                    continue
                loop_scope = self._region_scope(target_scope, frame.loop_region_id)
                assert loop_scope is not None
                entered[frame.loop_region_id] = loop_scope

        seeded: set[str] = set()
        for region_id, loop_scope in entered.items():
            region = self.workflow.loop(region_id)
            if source_node_id != region.header_node_id:
                continue
            for edge in outgoing:
                if edge.id not in region.exit_edge_ids:
                    continue
                self._add_boundary(
                    region,
                    edge,
                    loop_scope,
                    loop_scope,
                    edge.id in selected_edge_ids,
                    source_occurrence_id,
                )
                seeded.add(edge.id)

        for edge in outgoing:
            if edge.id in seeded and edge.id not in selected_edge_ids:
                target_scope = _target_scope(self.workflow, edge, source_scope)
                if not (
                    {frame.loop_region_id for frame in target_scope} & active_ids
                ) and not self._active_boundary_regions(edge, source_scope):
                    continue
            self._resolve_edge(
                edge,
                source_scope,
                edge.id in selected_edge_ids,
                source_occurrence_id,
            )

    def _validate_control(
        self,
        source_node_id: str,
        source_scope: ExecutionScope,
        selected_edge_ids: set[str] | frozenset[str],
    ) -> None:
        selected = [
            edge
            for edge in self.workflow.outgoing(source_node_id)
            if edge.id in selected_edge_ids
        ]
        active_ids = {frame.loop_region_id for frame in source_scope}
        containing = tuple(
            region
            for region in self.workflow.containing_loops(source_node_id)
            if region.id in active_ids or region.header_node_id == source_node_id
        )
        for region in containing:
            inside = [edge.id for edge in selected if edge.target in region.node_ids]
            outside = [edge.id for edge in selected if edge.target not in region.node_ids]
            if inside and outside:
                raise LoopControlError(
                    (
                        "LOOP_BACK_EXIT_CONFLICT"
                        if region.back_edge_id in inside
                        else "LOOP_CONTROL_CONFLICT"
                    ),
                    f"Node {source_node_id!r} selected Edges that both continue "
                    f"and exit Loop {region.id!r}: "
                    + ", ".join((*inside, *outside)),
                )

    def _resolve_edge(
        self,
        edge: EdgeIR,
        source_scope: ExecutionScope,
        selected: bool,
        source_occurrence_id: str | None,
    ) -> None:
        activation = (
            Activation(edge.id, source_occurrence_id, edge.target)
            if selected and source_occurrence_id is not None
            else None
        )
        boundaries = self._active_boundary_regions(edge, source_scope)
        for region in boundaries:
            loop_scope = self._region_scope(source_scope, region.id)
            if loop_scope is None:
                raise LoopControlError(
                    "LOOP_CONTROL_CONFLICT",
                    f"Boundary Edge {edge.id!r} has no active Loop scope.",
                )
            self._add_boundary(
                region,
                edge,
                loop_scope,
                source_scope,
                selected,
                source_occurrence_id,
            )
        if boundaries:
            return
        target_scope = _target_scope(self.workflow, edge, source_scope)
        self._add_resolution(edge, target_scope, selected, activation)

    def _add_boundary(
        self,
        region: LoopRegionIR,
        edge: EdgeIR,
        loop_scope: ExecutionScope,
        source_scope: ExecutionScope,
        selected: bool,
        source_occurrence_id: str | None,
    ) -> None:
        activation = (
            Activation(edge.id, source_occurrence_id, edge.target)
            if selected and source_occurrence_id is not None
            else None
        )
        item = LoopBoundaryResolution(
            region.id,
            loop_scope,
            edge.id,
            source_scope,
            edge.target,
            selected,
            activation,
        )
        existing = self.boundaries.get(item.id)
        if existing is not None and existing != item:
            raise LoopControlError(
                "LOOP_CONTROL_CONFLICT",
                f"Loop boundary Resolution {item.id!r} resolved twice.",
            )
        if existing is None:
            self.boundaries[item.id] = item
            self.new_boundaries.append(item)

    def _add_resolution(
        self,
        edge: EdgeIR,
        target_scope: ExecutionScope,
        selected: bool,
        activation: Activation | None,
    ) -> None:
        item = EdgeResolution(edge.id, edge.target, target_scope, selected, activation)
        existing = self.resolutions.get(item.id)
        if existing is not None:
            if existing != item:
                raise LoopControlError(
                    "LOOP_CONTROL_CONFLICT",
                    f"Edge Resolution {item.id!r} resolved twice.",
                )
            return
        self.resolutions[item.id] = item
        self.new_resolutions.append(item)
        self.targets.append((edge.target, target_scope))

    def _skip_outgoing(self, node_id: str, scope: ExecutionScope) -> None:
        for edge in self.workflow.outgoing(node_id):
            self._resolve_edge(edge, scope, False, None)

    def propagate(self) -> None:
        progressed = True
        while progressed:
            progressed = False
            while self.targets:
                node_id, scope = self.targets.popleft()
                if self._try_target(node_id, scope):
                    progressed = True
            if self._finalize_boundaries():
                progressed = True

    def _try_target(self, node_id: str, scope: ExecutionScope) -> bool:
        occurrence_id = occurrence_key(node_id, scope)
        expected = self._expected_incoming(node_id, scope)
        values = [self.resolutions.get(resolution_key(edge_id, scope)) for edge_id in expected]
        if not values or any(value is None for value in values):
            return False
        selected = any(
            value is not None and value.selected for value in values
        )
        resolution_ids = tuple(
            resolution_key(edge_id, scope) for edge_id in expected
        )
        if occurrence_id in self.known_occurrences:
            existing = self.scheduler.occurrences.get(occurrence_id)
            if existing is None or existing.status != "skipped" or not selected:
                return False
            self.revived.append(
                OccurrencePlan(occurrence_id, node_id, scope, resolution_ids)
            )
        else:
            if selected:
                self.plan_ready(node_id, scope, resolution_ids)
            else:
                self.plan_skipped(node_id, scope)
        for edge_id in expected:
            key = resolution_key(edge_id, scope)
            self.resolutions.pop(key, None)
            self.consumed_resolutions.append(key)
        return True

    def _finalize_boundaries(self) -> bool:
        progressed = False
        groups = sorted(
            {
                (item.loop_region_id, item.loop_scope)
                for item in self.boundaries.values()
            },
            key=lambda value: (len(value[1]), value[0]),
            reverse=True,
        )
        for region_id, loop_scope in groups:
            key = boundary_key(region_id, loop_scope)
            if key in self.closed_boundaries:
                continue
            region = self.workflow.loop(region_id)
            if self._scope_has_active_work(region, loop_scope):
                continue
            edge_ids = (region.back_edge_id, *region.exit_edge_ids)
            values = [
                self.boundaries.get(
                    boundary_resolution_key(region.id, edge_id, loop_scope)
                )
                for edge_id in edge_ids
            ]
            if any(value is None for value in values):
                continue
            self._finalize_boundary(region, loop_scope, tuple(values))  # type: ignore[arg-type]
            progressed = True
        return progressed

    def _finalize_boundary(
        self,
        region: LoopRegionIR,
        loop_scope: ExecutionScope,
        values: tuple[LoopBoundaryResolution, ...],
    ) -> None:
        selected_back = values[0].selected
        selected_exits = [item for item in values[1:] if item.selected]
        if selected_back and selected_exits:
            raise LoopControlError(
                "LOOP_BACK_EXIT_CONFLICT",
                f"Loop {region.id!r} selected both Back and Exit Edges.",
            )
        key = boundary_key(region.id, loop_scope)
        self.closed_boundaries.append(key)
        for item in values:
            self.boundaries.pop(item.id, None)

        if selected_back:
            value = values[0]
            edge = self.workflow.edge(value.edge_id)
            target_scope = _target_scope(self.workflow, edge, value.source_scope)
            self._add_resolution(edge, target_scope, True, value.activation)
            return

        if selected_exits:
            close_sets = {
                self._active_exit_set(
                    self.workflow.edge(item.edge_id), item.source_scope
                )
                for item in selected_exits
            }
            if len(close_sets) != 1:
                raise LoopControlError(
                    "LOOP_CONTROL_CONFLICT",
                    f"Loop {region.id!r} selected Exits that close different scopes.",
                )
            for item in values[1:]:
                edge = self.workflow.edge(item.edge_id)
                has_active_ancestor = any(
                    owner.id != region.id
                    and set(region.node_ids) < set(owner.node_ids)
                    and self._region_scope(item.source_scope, owner.id) is not None
                    for owner in self.workflow.exit_loops(edge.id)
                )
                if has_active_ancestor:
                    continue
                target_scope = _target_scope(self.workflow, edge, item.source_scope)
                self._add_resolution(
                    edge, target_scope, item.selected, item.activation
                )
            return

        if self.terminal_failed or self._scope_has_recorded_failure(
            region, loop_scope
        ) or not self._scope_was_activated(region, loop_scope):
            # An unreachable Loop, or one interrupted by an unhandled Node
            # failure, resolves its Exit boundaries as unselected.  LOOP_NO_ROUTE
            # is reserved for a successfully executed iteration whose control
            # logic selected neither Back nor Exit.
            for item in values[1:]:
                edge = self.workflow.edge(item.edge_id)
                target_scope = _target_scope(self.workflow, edge, item.source_scope)
                self._add_resolution(edge, target_scope, False, None)
            return

        raise LoopControlError(
            "LOOP_NO_ROUTE",
            f"Loop {region.id!r} stabilized without Back or Exit.",
        )

    def _scope_has_recorded_failure(
        self, region: LoopRegionIR, loop_scope: ExecutionScope
    ) -> bool:
        return any(
            occurrence.status == "failed"
            and occurrence.node_id in region.node_ids
            and self._region_scope(occurrence.scope, region.id) == loop_scope
            for occurrence in self.scheduler.occurrences.values()
        )

    def _scope_was_activated(
        self, region: LoopRegionIR, loop_scope: ExecutionScope
    ) -> bool:
        for occurrence_id, occurrence in self.scheduler.occurrences.items():
            if occurrence.node_id not in region.node_ids:
                continue
            if self._region_scope(occurrence.scope, region.id) != loop_scope:
                continue
            if occurrence.status != "skipped":
                return True
            if occurrence_id == self.terminal_occurrence_id:
                return True
        return any(
            plan.node_id in region.node_ids
            and self._region_scope(plan.scope, region.id) == loop_scope
            for plan in self.ready
        )

    def _scope_has_active_work(
        self, region: LoopRegionIR, loop_scope: ExecutionScope
    ) -> bool:
        for occurrence_id, occurrence in self.scheduler.occurrences.items():
            if occurrence_id == self.terminal_occurrence_id:
                continue
            if occurrence.status not in {"ready", "running"}:
                continue
            if occurrence.node_id not in region.node_ids:
                continue
            if self._region_scope(occurrence.scope, region.id) == loop_scope:
                return True
        for plan in self.ready:
            if plan.node_id not in region.node_ids:
                continue
            if self._region_scope(plan.scope, region.id) == loop_scope:
                return True
        return False

    def _active_boundary_regions(
        self, edge: EdgeIR, source_scope: ExecutionScope
    ) -> tuple[LoopRegionIR, ...]:
        active = {frame.loop_region_id for frame in source_scope}
        values: list[LoopRegionIR] = []
        back = self.workflow.back_loop(edge.id)
        if back is not None and back.id in active:
            values.append(back)
        for region in self.workflow.exit_loops(edge.id):
            if region.id in active and region not in values:
                values.append(region)
        return tuple(sorted(values, key=lambda item: len(item.node_ids)))

    def _expected_incoming(
        self, node_id: str, scope: ExecutionScope
    ) -> tuple[str, ...]:
        header_frames = [
            (frame, self.workflow.loop(frame.loop_region_id))
            for frame in scope
            if self.workflow.loop(frame.loop_region_id).header_node_id == node_id
        ]
        repeated = [item for item in header_frames if item[0].iteration > 1]
        if repeated:
            return (repeated[-1][1].back_edge_id,)
        if header_frames:
            return header_frames[0][1].entry_edge_ids
        header_regions = [
            region
            for region in self.workflow.loop_regions
            if region.header_node_id == node_id
        ]
        if header_regions:
            back_ids = {region.back_edge_id for region in header_regions}
            return tuple(
                edge.id
                for edge in self.workflow.incoming(node_id)
                if edge.id not in back_ids
            )
        return tuple(edge.id for edge in self.workflow.incoming(node_id))

    def _active_exit_set(
        self, edge: EdgeIR, source_scope: ExecutionScope
    ) -> tuple[str, ...]:
        return tuple(
            frame.loop_region_id
            for frame in source_scope
            if edge.target not in self.workflow.loop(frame.loop_region_id).node_ids
        )

    @staticmethod
    def _region_scope(
        scope: ExecutionScope, region_id: str
    ) -> ExecutionScope | None:
        index = _scope_index(scope, region_id)
        return scope[: index + 1] if index is not None else None


def _scope_index(scope: ExecutionScope, region_id: str) -> int | None:
    return next(
        (
            index
            for index, frame in enumerate(scope)
            if frame.loop_region_id == region_id
        ),
        None,
    )


def _target_scope(
    workflow: WorkflowIR, edge: EdgeIR, source_scope: ExecutionScope
) -> ExecutionScope:
    back = workflow.back_loop(edge.id)
    if back is not None:
        index = _scope_index(source_scope, back.id)
        if index is None:
            raise LoopControlError(
                "LOOP_CONTROL_CONFLICT",
                f"Back Edge {edge.id!r} has no active Loop scope.",
            )
        frame = source_scope[index]
        return (
            *source_scope[:index],
            LoopIteration(back.id, frame.iteration + 1),
        )

    target_regions = workflow.containing_loops(edge.target)
    target_ids = {region.id for region in target_regions}
    retained = tuple(
        frame for frame in source_scope if frame.loop_region_id in target_ids
    )
    active = {frame.loop_region_id for frame in retained}
    candidates = tuple(
        region
        for region in target_regions
        if region.id not in active
        and (
            edge.id in region.entry_edge_ids
            or (
                edge.source == region.header_node_id
                and edge.target != region.header_node_id
            )
        )
    )
    entered = _entry_chain(candidates)
    return (*retained, *(LoopIteration(region.id, 1) for region in entered))


def _entry_chain(
    regions: tuple[LoopRegionIR, ...],
) -> tuple[LoopRegionIR, ...]:
    by_parent: dict[str | None, list[LoopRegionIR]] = {}
    ids = {region.id for region in regions}
    for region in regions:
        parent = (
            region.parent_loop_region_id
            if region.parent_loop_region_id in ids
            else None
        )
        by_parent.setdefault(parent, []).append(region)
    result: list[LoopRegionIR] = []
    parent: str | None = None
    while len(by_parent.get(parent, ())) == 1:
        current = by_parent[parent][0]
        result.append(current)
        parent = current.id
    return tuple(result)

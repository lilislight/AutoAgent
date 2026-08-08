"""Scope-aware Scheduler implementing the normative V2 graph contract."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from uuid import UUID

from ..errors import LoopControlError
from ..workflow import EdgeIR, LoopRegionIR, WorkflowIR
from .models import (
    EdgeActivation,
    EdgeResolution,
    ExecutionScope,
    LoopIteration,
    NodeExecutionRequest,
    occurrence_key,
    scope_key,
)


def edge_occurrence_key(edge_id: str, target_scope: ExecutionScope) -> str:
    return f"{edge_id}@{scope_key(target_scope)}"


def boundary_edge_key(
    region_id: str, edge_id: str, region_scope: ExecutionScope
) -> str:
    return f"boundary:{region_id}:{edge_id}@{scope_key(region_scope)}"


@dataclass(frozen=True, slots=True)
class SkippedOccurrence:
    node_id: str
    scope: ExecutionScope


class Scheduler:
    """Maintain the current scoped graph cursor without Runtime history."""

    def __init__(self, workflow: WorkflowIR) -> None:
        self.workflow = workflow
        self.ready: deque[NodeExecutionRequest] = deque()
        self.resolutions: dict[str, EdgeResolution] = {}
        self.scheduled: set[str] = set()
        self.skipped: set[str] = set()
        self._scheduled_requests: dict[str, NodeExecutionRequest] = {}
        self._pending_boundaries: set[tuple[str, ExecutionScope]] = set()

    def initialize(self) -> None:
        if self.ready or self.scheduled:
            return
        for node_id in self.workflow.entry_node_ids:
            self._enqueue(node_id, (), ())

    def drain_ready(self) -> tuple[NodeExecutionRequest, ...]:
        values = tuple(self.ready)
        self.ready.clear()
        return values

    def restore(
        self,
        *,
        ready: tuple[NodeExecutionRequest, ...],
        resolutions: tuple[tuple[str, EdgeResolution], ...],
        scheduled: tuple[str, ...],
        skipped: tuple[str, ...],
    ) -> None:
        self.ready = deque(ready)
        self.resolutions = dict(resolutions)
        self.scheduled = set(scheduled)
        self.skipped = set(skipped)
        self._scheduled_requests = {item.occurrence: item for item in ready}
        self._pending_boundaries = set()
        for key, resolution in self.resolutions.items():
            for region in self.workflow.exit_loops(resolution.edge_id):
                region_scope = self._region_scope(resolution.target_scope, region.id)
                if region_scope is not None and key == boundary_edge_key(
                    region.id, resolution.edge_id, region_scope
                ):
                    self._pending_boundaries.add((region.id, region_scope))
            back = self.workflow.back_loop(resolution.edge_id)
            if back is not None:
                region_scope = self._region_scope(resolution.target_scope, back.id)
                if region_scope is not None and key == boundary_edge_key(
                    back.id, resolution.edge_id, region_scope
                ):
                    self._pending_boundaries.add((back.id, region_scope))

    def track_active(self, request: NodeExecutionRequest) -> None:
        """Restore a Wait-owned request not present in the ready queue."""

        self.scheduled.add(request.occurrence)
        self._scheduled_requests[request.occurrence] = request

    def resolve_outgoing(
        self,
        request: NodeExecutionRequest,
        source_execution_id: UUID,
        decisions: dict[str, bool],
    ) -> tuple[SkippedOccurrence, ...]:
        outgoing = self.workflow.outgoing(request.node_id)
        if set(decisions) != {edge.id for edge in outgoing}:
            raise ValueError("Decisions must resolve every outgoing Edge exactly once.")
        self._validate_outgoing(request, decisions)
        self._complete_request(request)
        skipped: list[SkippedOccurrence] = []
        target_scopes = {
            edge.id: self._target_scope(edge, request.scope) for edge in outgoing
        }
        active_ids = {frame.loop_region_id for frame in request.scope}
        entered_regions: dict[str, ExecutionScope] = {}
        for edge in outgoing:
            if not decisions[edge.id]:
                continue
            target_scope = target_scopes[edge.id]
            for frame in target_scope:
                if frame.loop_region_id in active_ids:
                    continue
                region_scope = self._region_scope(target_scope, frame.loop_region_id)
                assert region_scope is not None
                entered_regions[frame.loop_region_id] = region_scope

        # A shared Header may execute before one of its mutually-exclusive
        # sibling scopes exists. Once its selected Body Edge chooses a sibling,
        # the Header's other Exit decisions belong to that new iteration. They
        # must not be written as permanent root-scope skips, because a later
        # iteration may choose those targets.
        seeded_boundary_edges: set[str] = set()
        for region_id, region_scope in entered_regions.items():
            region = self.workflow.loop(region_id)
            if request.node_id != region.header_node_id:
                continue
            for edge in outgoing:
                if edge.id not in region.exit_edge_ids:
                    continue
                selected = decisions[edge.id]
                activation = (
                    EdgeActivation(edge.id, request.node_id, source_execution_id)
                    if selected
                    else None
                )
                self._resolve_boundary(
                    region, edge, region_scope, selected, activation
                )
                seeded_boundary_edges.add(edge.id)

        for edge in outgoing:
            selected = decisions[edge.id]
            activation = (
                EdgeActivation(edge.id, request.node_id, source_execution_id)
                if selected
                else None
            )
            boundaries = self._active_boundary_regions(edge, request.scope)
            for region in boundaries:
                self._resolve_boundary(
                    region, edge, request.scope, selected, activation
                )
            if boundaries:
                # A Back/Exit target is committed only when all active work for
                # its owning Loop boundary has stabilized.
                continue
            target_scope = target_scopes[edge.id]
            if edge.id in seeded_boundary_edges and not selected:
                target_active_ids = {
                    frame.loop_region_id for frame in target_scope
                }
                if not (target_active_ids & active_ids):
                    # This false choice belongs only to the newly-entered
                    # sibling/nested boundary. It does not create a skipped
                    # occurrence in a Loop that is not active yet.
                    continue
            self._resolve(edge, target_scope, selected, activation)
            skipped.extend(self._try_resolve_target(edge.target, target_scope))
        skipped.extend(self._finalize_pending_boundaries())
        return tuple(skipped)

    def skip_outgoing(
        self, node_id: str, scope: ExecutionScope
    ) -> tuple[SkippedOccurrence, ...]:
        request = self._scheduled_requests.get(occurrence_key(node_id, scope))
        if request is None:
            request = NodeExecutionRequest(node_id=node_id, scope=scope)
        self._complete_request(request)
        skipped: list[SkippedOccurrence] = []
        for edge in self.workflow.outgoing(node_id):
            boundaries = self._active_boundary_regions(edge, scope)
            for region in boundaries:
                self._resolve_boundary(region, edge, scope, False, None)
            if boundaries:
                continue
            target_scope = self._target_scope(edge, scope)
            self._resolve(edge, target_scope, False, None)
            skipped.extend(self._try_resolve_target(edge.target, target_scope))
        skipped.extend(self._finalize_pending_boundaries())
        return tuple(skipped)

    def _validate_outgoing(
        self, request: NodeExecutionRequest, decisions: dict[str, bool]
    ) -> None:
        selected = [
            edge
            for edge in self.workflow.outgoing(request.node_id)
            if decisions[edge.id]
        ]
        containing = self.workflow.containing_loops(request.node_id)
        for region in containing:
            members = set(region.node_ids)
            inside = [edge.id for edge in selected if edge.target in members]
            outside = [edge.id for edge in selected if edge.target not in members]
            if inside and outside:
                raise LoopControlError(
                    (
                        "LOOP_BACK_EXIT_CONFLICT"
                        if region.back_edge_id in inside
                        else "LOOP_CONTROL_CONFLICT"
                    ),
                    f"Node {request.node_id!r} selected Edges that both continue "
                    f"and exit Loop {region.id!r}: "
                    + ", ".join((*inside, *outside)),
                )
        exit_sets = {
            tuple(
                sorted(
                    region.id
                    for region in containing
                    if edge.target not in region.node_ids
                )
            )
            for edge in selected
            if any(edge.target not in region.node_ids for region in containing)
        }
        if len(exit_sets) > 1:
            raise LoopControlError(
                "LOOP_CONTROL_CONFLICT",
                f"Node {request.node_id!r} selected Exit Edges that close "
                "different Loop scope sets.",
            )

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

    def _resolve_boundary(
        self,
        region: LoopRegionIR,
        edge: EdgeIR,
        source_scope: ExecutionScope,
        selected: bool,
        activation: EdgeActivation | None,
    ) -> None:
        region_scope = self._region_scope(source_scope, region.id)
        if region_scope is None:
            raise LoopControlError(
                "LOOP_CONTROL_CONFLICT",
                f"Boundary Edge {edge.id!r} has no active Loop {region.id!r} scope.",
            )
        key = boundary_edge_key(region.id, edge.id, region_scope)
        value = EdgeResolution(edge.id, source_scope, selected, activation)
        existing = self.resolutions.get(key)
        if existing is not None and existing != value:
            raise LoopControlError(
                "LOOP_CONTROL_CONFLICT",
                f"Boundary Edge occurrence {key!r} resolved twice.",
            )
        self.resolutions[key] = value
        self._pending_boundaries.add((region.id, region_scope))

    def _finalize_pending_boundaries(self) -> list[SkippedOccurrence]:
        skipped: list[SkippedOccurrence] = []
        progressed = True
        while progressed:
            progressed = False
            for region_id, region_scope in tuple(self._pending_boundaries):
                region = self.workflow.loop(region_id)
                if self._scope_has_active_work(region, region_scope):
                    continue
                edge_ids = (region.back_edge_id, *region.exit_edge_ids)
                values = [
                    self.resolutions.get(
                        boundary_edge_key(region.id, edge_id, region_scope)
                    )
                    for edge_id in edge_ids
                ]
                if any(value is None for value in values):
                    continue
                self._pending_boundaries.remove((region_id, region_scope))
                skipped.extend(
                    self._finalize_boundary(region, region_scope, edge_ids, values)
                )
                progressed = True
        return skipped

    def _finalize_boundary(
        self,
        region: LoopRegionIR,
        region_scope: ExecutionScope,
        edge_ids: tuple[str, ...],
        values: list[EdgeResolution | None],
    ) -> list[SkippedOccurrence]:
        resolved = [value for value in values if value is not None]
        selected_back = resolved[0].selected
        selected_exits = [value for value in resolved[1:] if value.selected]
        if selected_back and selected_exits:
            raise LoopControlError(
                "LOOP_BACK_EXIT_CONFLICT",
                f"Loop {region.id!r} selected both its Back Edge and Exit Edge(s).",
            )
        for edge_id in edge_ids:
            self.resolutions.pop(
                boundary_edge_key(region.id, edge_id, region_scope), None
            )

        if selected_back:
            edge = self._edge(region.back_edge_id)
            value = resolved[0]
            target_scope = self._target_scope(edge, value.target_scope)
            self._resolve(edge, target_scope, True, value.activation)
            return self._try_resolve_target(edge.target, target_scope)

        if selected_exits:
            close_sets = {
                self._active_exit_set(self._edge(value.edge_id), value.target_scope)
                for value in selected_exits
            }
            if len(close_sets) != 1:
                raise LoopControlError(
                    "LOOP_CONTROL_CONFLICT",
                    f"Loop {region.id!r} selected Exit Edges that close different "
                    "active Loop scope sets.",
                )
            skipped: list[SkippedOccurrence] = []
            for value in resolved[1:]:
                edge = self._edge(value.edge_id)
                selected = value.selected
                active_ancestor_boundary = any(
                    owner.id != region.id
                    and set(region.node_ids) < set(owner.node_ids)
                    and self._region_scope(value.target_scope, owner.id) is not None
                    for owner in self.workflow.exit_loops(edge.id)
                )
                if active_ancestor_boundary:
                    continue
                target_scope = self._target_scope(edge, value.target_scope)
                self._resolve(edge, target_scope, selected, value.activation)
                skipped.extend(self._try_resolve_target(edge.target, target_scope))
            return skipped

        raise LoopControlError(
            "LOOP_NO_ROUTE",
            f"Loop {region.id!r} reached a stable boundary without Back, Exit, or Wait.",
        )

    def _active_exit_set(
        self, edge: EdgeIR, source_scope: ExecutionScope
    ) -> tuple[str, ...]:
        return tuple(
            frame.loop_region_id
            for frame in source_scope
            if edge.target not in self.workflow.loop(frame.loop_region_id).node_ids
        )

    def _scope_has_active_work(
        self, region: LoopRegionIR, region_scope: ExecutionScope
    ) -> bool:
        for request in self._scheduled_requests.values():
            if request.node_id not in region.node_ids:
                continue
            candidate = self._region_scope(request.scope, region.id)
            if candidate == region_scope:
                return True
        return False

    def _try_resolve_target(
        self, node_id: str, target_scope: ExecutionScope
    ) -> list[SkippedOccurrence]:
        occurrence = occurrence_key(node_id, target_scope)
        if occurrence in self.scheduled:
            return []
        expected = self._expected_incoming(node_id, target_scope)
        resolutions = [
            self.resolutions.get(edge_occurrence_key(edge_id, target_scope))
            for edge_id in expected
        ]
        if occurrence in self.skipped:
            # A Loop Exit can resolve a target in an outer/root scope more than
            # once across iterations or sibling transitions. An earlier
            # all-skipped occurrence must not permanently suppress a later
            # selected activation for that same structural scope.
            if not any(
                value is not None and value.selected for value in resolutions
            ):
                return []
            self.skipped.remove(occurrence)
        if not resolutions or any(value is None for value in resolutions):
            return []
        selected = [value.activation for value in resolutions if value.selected]
        for edge_id in expected:
            self.resolutions.pop(edge_occurrence_key(edge_id, target_scope), None)
        if selected:
            self._enqueue(
                node_id,
                target_scope,
                tuple(value for value in selected if value is not None),
            )
            return []
        self.skipped.add(occurrence)
        result = [SkippedOccurrence(node_id, target_scope)]
        result.extend(self.skip_outgoing(node_id, target_scope))
        return result

    def _expected_incoming(
        self, node_id: str, scope: ExecutionScope
    ) -> tuple[str, ...]:
        header_frames = [
            (index, frame, self.workflow.loop(frame.loop_region_id))
            for index, frame in enumerate(scope)
            if self.workflow.loop(frame.loop_region_id).header_node_id == node_id
        ]
        repeated = [item for item in header_frames if item[1].iteration > 1]
        if repeated:
            # A shared Header is activated by the deepest Loop whose iteration
            # advanced. Ancestor iterations can already be > 1 while an Inner
            # Back advances only the child scope.
            _, _, region = repeated[-1]
            return (region.back_edge_id,)
        if header_frames:
            _, _, outermost = header_frames[0]
            return outermost.entry_edge_ids
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

    def _target_scope(
        self, edge: EdgeIR, source_scope: ExecutionScope
    ) -> ExecutionScope:
        back = self.workflow.back_loop(edge.id)
        if back is not None:
            index = self._scope_index(source_scope, back.id)
            if index is None:
                raise LoopControlError(
                    "LOOP_CONTROL_CONFLICT",
                    f"Back Edge {edge.id!r} has no active Loop scope.",
                )
            frame = source_scope[index]
            base: ExecutionScope = (
                *source_scope[:index],
                LoopIteration(back.id, frame.iteration + 1),
            )
            # A Back activates only its owning Loop's shared Header. Child or
            # sibling scopes begin later from the Header's selected outgoing
            # Edge, even when this Back is structurally an Entry to them.
            return base

        target_regions = self.workflow.containing_loops(edge.target)
        target_ids = {region.id for region in target_regions}
        retained = tuple(
            frame for frame in source_scope if frame.loop_region_id in target_ids
        )
        active = {frame.loop_region_id for frame in retained}
        candidates = [
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
        ]
        entered = self._entry_chain(tuple(candidates))
        return (*retained, *(LoopIteration(region.id, 1) for region in entered))

    def _entry_chain(
        self, regions: tuple[LoopRegionIR, ...]
    ) -> tuple[LoopRegionIR, ...]:
        if not regions:
            return ()
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
        while True:
            candidates = by_parent.get(parent, [])
            if len(candidates) != 1:
                break
            current = candidates[0]
            result.append(current)
            parent = current.id
        return tuple(result)

    def _resolve(
        self,
        edge: EdgeIR,
        target_scope: ExecutionScope,
        selected: bool,
        activation: EdgeActivation | None,
    ) -> None:
        key = edge_occurrence_key(edge.id, target_scope)
        value = EdgeResolution(edge.id, target_scope, selected, activation)
        existing = self.resolutions.get(key)
        if existing is not None and existing != value:
            raise LoopControlError(
                "LOOP_CONTROL_CONFLICT",
                f"Edge occurrence {key!r} resolved twice.",
            )
        self.resolutions[key] = value

    def _enqueue(
        self,
        node_id: str,
        scope: ExecutionScope,
        activations: tuple[EdgeActivation, ...],
    ) -> None:
        occurrence = occurrence_key(node_id, scope)
        if occurrence in self.scheduled:
            return
        request = NodeExecutionRequest(node_id, scope, activations)
        self.scheduled.add(occurrence)
        self._scheduled_requests[occurrence] = request
        self.ready.append(request)

    def _complete_request(self, request: NodeExecutionRequest) -> None:
        self.scheduled.discard(request.occurrence)
        self._scheduled_requests.pop(request.occurrence, None)

    def _edge(self, edge_id: str) -> EdgeIR:
        return self.workflow.edge(edge_id)

    def _region_scope(
        self, scope: ExecutionScope, region_id: str
    ) -> ExecutionScope | None:
        index = self._scope_index(scope, region_id)
        return scope[: index + 1] if index is not None else None

    @staticmethod
    def _scope_index(scope: ExecutionScope, region_id: str) -> int | None:
        return next(
            (
                index
                for index, frame in enumerate(scope)
                if frame.loop_region_id == region_id
            ),
            None,
        )

from __future__ import annotations

from copy import deepcopy
from collections.abc import Iterable

from autoagent.core.compiler import EdgeIR, LoopRegionIR, WorkflowIR
from autoagent.core.runtime import (
    ConditionContext,
    Invocation,
    NodeExecution,
    RuntimeErrorInfo,
    Session,
)
from autoagent.core.runtime.hooks import invoke_hook_async
from autoagent.core.runtime.scheduler import (
    EdgeActivation,
    EdgeResolution,
    ExecutionScope,
    LoopIteration,
    NodeExecutionRequest,
    NodeExecutionTransition,
    edge_occurrence_key,
    execution_scope_key,
    node_instance_key,
)


class Scheduler:
    """Advance scoped node instances with ordinary fan-out and complete fan-in.

    A static edge resolves once per target execution scope, not once per
    Invocation. Natural-loop back and exit edges first resolve at the boundary
    of one loop iteration. After the whole acyclic iteration body quiesces, the
    boundary either creates the next header scope or resolves exits into the
    parent scope.
    """

    def initialize(self, *, workflow_ir: WorkflowIR, invocation: Invocation) -> None:
        cursor = invocation.scheduler
        if cursor.entry_paths_initialized:
            return
        cursor.entry_paths_initialized = True

        # Invocation creates the initial request before WorkflowIR is available.
        # Attach the natural-loop scope now that the compiled graph is known.
        initial_scope = self._initial_scope(workflow_ir, invocation.entry_node_id)
        existing = cursor.drain_ready()
        if existing:
            cursor.enqueue_ready(
                invocation.entry_node_id,
                activations=existing[0].activations,
                execution_scope=initial_scope,
            )
        else:
            cursor.enqueue_ready(
                invocation.entry_node_id,
                execution_scope=initial_scope,
            )
        cursor.scheduled_node_instances.add(
            node_instance_key(invocation.entry_node_id, initial_scope)
        )
        for frame in initial_scope:
            cursor.entered_loop_instances.add(
                self._loop_instance_key(initial_scope, frame.loop_region_id)
            )

        for entry_node_id in workflow_ir.entry_node_ids:
            if entry_node_id == invocation.entry_node_id:
                continue
            self._skip_node_instance(
                workflow_ir=workflow_ir,
                invocation=invocation,
                node_id=entry_node_id,
                scope=self._initial_scope(workflow_ir, entry_node_id),
            )

    async def next(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
        transitions: Iterable[NodeExecutionTransition],
    ) -> None:
        for transition in transitions:
            if invocation.state == "failed":
                return
            if transition.state == "completed":
                await self._advance_completed(
                    workflow_ir=workflow_ir,
                    session=session,
                    invocation=invocation,
                    transition=transition,
                )
            elif transition.state == "failed":
                execution = invocation.get_node_execution(transition.node_execution_id)
                error = (
                    execution.error
                    if execution is not None and execution.error is not None
                    else RuntimeErrorInfo(
                        code="NODE_EXECUTION_FAILED",
                        message=f"Node failed: {transition.node_id}",
                        detail={
                            "node_id": transition.node_id,
                            "node_execution_id": str(transition.node_execution_id),
                        },
                    )
                )
                if workflow_ir.policy.failure.mode == "fail_fast":
                    invocation.mark_failed(error)
                    continue
                invocation.defer_terminal(error, state="failed")
                if execution is not None:
                    self._advance_blocked_execution(
                        workflow_ir=workflow_ir,
                        invocation=invocation,
                        execution=execution,
                        reason=error.code,
                    )

    def skip_ready_request(
        self,
        *,
        workflow_ir: WorkflowIR,
        invocation: Invocation,
        request: NodeExecutionRequest,
    ) -> None:
        """Resolve a recovery-blocked node as a skipped branch."""

        targets = self._skip_node_instance(
            workflow_ir=workflow_ir,
            invocation=invocation,
            node_id=request.node_id,
            scope=request.execution_scope,
        )
        self._resolve_node_instances(
            workflow_ir=workflow_ir,
            invocation=invocation,
            targets=targets,
        )

    def _advance_blocked_execution(
        self,
        *,
        workflow_ir: WorkflowIR,
        invocation: Invocation,
        execution: NodeExecution,
        reason: str,
    ) -> None:
        targets: list[tuple[str, ExecutionScope]] = []
        boundaries: set[tuple[str, ExecutionScope]] = set()
        for edge_id in workflow_ir.graph.outgoing_edges.get(execution.node_id, ()):
            edge = workflow_ir.edges[edge_id]
            execution.add_edge_evaluation(
                edge_id=edge.id,
                target_node_id=edge.to_node,
                state="skipped",
                selected=False,
                reason=reason,
            )
            boundary = self._boundary_owner(
                workflow_ir,
                edge,
                execution.execution_scope,
            )
            if boundary is not None:
                loop_scope = self._loop_scope(
                    execution.execution_scope,
                    boundary.id,
                )
                invocation.scheduler.resolve_loop_boundary(
                    loop_region_id=boundary.id,
                    loop_scope=loop_scope,
                    edge_id=edge.id,
                    state="skipped",
                )
                boundaries.add((boundary.id, loop_scope))
            else:
                target_scope = self._target_scope(
                    workflow_ir,
                    edge,
                    execution.execution_scope,
                )
                invocation.scheduler.resolve_edge(
                    edge.id,
                    state="skipped",
                    scope=target_scope,
                )
                targets.append((edge.to_node, target_scope))
        self._resolve_node_instances(
            workflow_ir=workflow_ir,
            invocation=invocation,
            targets=targets,
        )
        for loop_region_id, loop_scope in boundaries:
            self._finalize_loop_boundary(
                workflow_ir=workflow_ir,
                invocation=invocation,
                loop_region=workflow_ir.graph.loop_regions[loop_region_id],
                loop_scope=loop_scope,
            )

    async def _advance_completed(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
        transition: NodeExecutionTransition,
    ) -> None:
        source_execution = invocation.get_node_execution(transition.node_execution_id)
        if source_execution is None:
            invocation.mark_failed(
                RuntimeErrorInfo(
                    code="SCHEDULER_UNKNOWN_EXECUTION",
                    message="Transition references an unknown node execution.",
                    detail={"node_execution_id": str(transition.node_execution_id)},
                )
            )
            return

        affected: list[tuple[str, ExecutionScope]] = []
        affected_boundaries: set[tuple[str, ExecutionScope]] = set()
        for edge_id in workflow_ir.graph.outgoing_edges.get(
            source_execution.node_id, ()
        ):
            edge = workflow_ir.edges[edge_id]
            selected, reason = await self._evaluate_edge(
                edge=edge,
                session=session,
                invocation=invocation,
                source_output=source_execution.output,
            )
            if selected is None:
                source_execution.add_edge_evaluation(
                    edge_id=edge.id,
                    target_node_id=edge.to_node,
                    state="failed",
                    selected=False,
                    reason=reason,
                )
                invocation.mark_failed(
                    RuntimeErrorInfo(
                        code="EDGE_CONDITION_FAILED",
                        message=f"Edge condition failed: {edge.id}",
                        detail={"edge_id": edge.id, "reason": reason},
                    )
                )
                return
            source_execution.add_edge_evaluation(
                edge_id=edge.id,
                target_node_id=edge.to_node,
                state="selected" if selected else "skipped",
                selected=selected,
                reason=reason,
            )
            activation = (
                EdgeActivation(
                    edge_id=edge.id,
                    source_node_id=edge.from_node,
                    source_execution_id=source_execution.id,
                )
                if selected
                else None
            )
            boundary = self._boundary_owner(
                workflow_ir, edge, source_execution.execution_scope
            )
            if boundary is not None:
                loop_scope = self._loop_scope(
                    source_execution.execution_scope, boundary.id
                )
                invocation.scheduler.resolve_loop_boundary(
                    loop_region_id=boundary.id,
                    loop_scope=loop_scope,
                    edge_id=edge.id,
                    state="selected" if selected else "skipped",
                    activation=activation,
                )
                affected_boundaries.add((boundary.id, loop_scope))
            else:
                target_scope = self._target_scope(
                    workflow_ir, edge, source_execution.execution_scope
                )
                invocation.scheduler.resolve_edge(
                    edge.id,
                    state="selected" if selected else "skipped",
                    activation=activation,
                    scope=target_scope,
                )
                affected.append((edge.to_node, target_scope))

        self._resolve_node_instances(
            workflow_ir=workflow_ir,
            invocation=invocation,
            targets=affected,
        )
        for loop_region_id, loop_scope in affected_boundaries:
            self._finalize_loop_boundary(
                workflow_ir=workflow_ir,
                invocation=invocation,
                loop_region=workflow_ir.graph.loop_regions[loop_region_id],
                loop_scope=loop_scope,
            )

    async def _evaluate_edge(
        self,
        *,
        edge: EdgeIR,
        session: Session,
        invocation: Invocation,
        source_output: object,
    ) -> tuple[bool | None, str | None]:
        if edge.condition is None:
            return True, None
        if isinstance(edge.condition, str):
            return False, "String edge conditions are not supported at runtime yet."
        if not callable(edge.condition):
            return False, "Edge condition is not callable."
        context = ConditionContext(
            invocation_input=invocation.input,
            invocation_context=deepcopy(invocation.context),
            session_context=deepcopy(session.context),
            outputs=invocation.outputs.scoped(edge.scope_node_ids),
            edge_id=edge.local_id or edge.id,
            source_node_id=edge.local_from_node or edge.from_node,
            target_node_id=edge.local_to_node or edge.to_node,
            source_output=source_output,
        )
        try:
            return bool(await invoke_hook_async(edge.condition, context)), None
        except Exception as exc:
            return None, f"{type(exc).__name__}: {exc}"

    def _resolve_node_instances(
        self,
        *,
        workflow_ir: WorkflowIR,
        invocation: Invocation,
        targets: Iterable[tuple[str, ExecutionScope]],
    ) -> None:
        pending = self._ordered_targets(workflow_ir, targets)
        while pending and invocation.state != "failed":
            node_id, scope = pending.pop(0)
            instance_key = node_instance_key(node_id, scope)
            if (
                instance_key in invocation.scheduler.scheduled_node_instances
                or instance_key in invocation.scheduler.skipped_node_instances
            ):
                continue
            incoming_edge_ids = self._incoming_edges_for_instance(
                workflow_ir, node_id, scope
            )
            resolutions = [
                invocation.scheduler.edge_resolutions.get(
                    edge_occurrence_key(edge_id, scope)
                )
                for edge_id in incoming_edge_ids
            ]
            if not incoming_edge_ids or any(item is None for item in resolutions):
                continue
            activations = tuple(
                item.activation
                for item in resolutions
                if item is not None
                and item.state == "selected"
                and item.activation is not None
            )
            if activations:
                invocation.scheduler.scheduled_node_instances.add(instance_key)
                invocation.scheduler.enqueue_ready(
                    node_id,
                    activations=activations,
                    execution_scope=scope,
                )
                for frame in scope:
                    invocation.scheduler.entered_loop_instances.add(
                        self._loop_instance_key(scope, frame.loop_region_id)
                    )
                continue

            header_region = self._header_region(workflow_ir, node_id)
            if header_region is not None and self._frame(scope, header_region.id).iteration == 0:
                pending.extend(
                    self._skip_inactive_loop(
                        workflow_ir=workflow_ir,
                        invocation=invocation,
                        loop_region=header_region,
                        loop_scope=scope[: header_region.depth + 1],
                    )
                )
            else:
                pending.extend(
                    self._skip_node_instance(
                        workflow_ir=workflow_ir,
                        invocation=invocation,
                        node_id=node_id,
                        scope=scope,
                    )
                )

    def _skip_node_instance(
        self,
        *,
        workflow_ir: WorkflowIR,
        invocation: Invocation,
        node_id: str,
        scope: ExecutionScope,
    ) -> list[tuple[str, ExecutionScope]]:
        instance_key = node_instance_key(node_id, scope)
        if instance_key in invocation.scheduler.skipped_node_instances:
            return []
        invocation.scheduler.skipped_node_instances.add(instance_key)
        targets: list[tuple[str, ExecutionScope]] = []
        boundaries: set[tuple[str, ExecutionScope]] = set()
        for edge_id in workflow_ir.graph.outgoing_edges.get(node_id, ()):
            edge = workflow_ir.edges[edge_id]
            boundary = self._boundary_owner(workflow_ir, edge, scope)
            if boundary is not None:
                loop_scope = self._loop_scope(scope, boundary.id)
                invocation.scheduler.resolve_loop_boundary(
                    loop_region_id=boundary.id,
                    loop_scope=loop_scope,
                    edge_id=edge.id,
                    state="skipped",
                )
                boundaries.add((boundary.id, loop_scope))
            else:
                target_scope = self._target_scope(workflow_ir, edge, scope)
                invocation.scheduler.resolve_edge(
                    edge.id, state="skipped", scope=target_scope
                )
                targets.append((edge.to_node, target_scope))
        for loop_region_id, loop_scope in boundaries:
            self._finalize_loop_boundary(
                workflow_ir=workflow_ir,
                invocation=invocation,
                loop_region=workflow_ir.graph.loop_regions[loop_region_id],
                loop_scope=loop_scope,
            )
        return targets

    def _skip_inactive_loop(
        self,
        *,
        workflow_ir: WorkflowIR,
        invocation: Invocation,
        loop_region: LoopRegionIR,
        loop_scope: ExecutionScope,
    ) -> list[tuple[str, ExecutionScope]]:
        targets: list[tuple[str, ExecutionScope]] = []
        for node_id in loop_region.node_ids:
            node_scope = self._scope_for_node(workflow_ir, node_id, loop_scope)
            invocation.scheduler.skipped_node_instances.add(
                node_instance_key(node_id, node_scope)
            )
        invocation.scheduler.exited_loop_instances.add(
            self._loop_instance_key(loop_scope, loop_region.id)
        )
        for edge_id in loop_region.exit_edge_ids:
            edge = workflow_ir.edges[edge_id]
            targets.extend(
                self._propagate_finalized_boundary_edge(
                    workflow_ir=workflow_ir,
                    invocation=invocation,
                    loop_scope=loop_scope,
                    edge=edge,
                    resolution=EdgeResolution(
                        edge_id=edge.id,
                        state="skipped",
                        scope=loop_scope,
                    ),
                )
            )
        return targets

    def _finalize_loop_boundary(
        self,
        *,
        workflow_ir: WorkflowIR,
        invocation: Invocation,
        loop_region: LoopRegionIR,
        loop_scope: ExecutionScope,
    ) -> None:
        boundary_edge_ids = loop_region.back_edge_ids + loop_region.exit_edge_ids
        resolutions: list[EdgeResolution] = []
        for edge_id in boundary_edge_ids:
            key = f"{loop_region.id}@{execution_scope_key(loop_scope)}:{edge_id}"
            resolution = invocation.scheduler.loop_boundary_resolutions.get(key)
            if resolution is None:
                return
            resolutions.append(resolution)

        selected_back = [
            item
            for item in resolutions
            if item.edge_id in loop_region.back_edge_ids and item.state == "selected"
        ]
        selected_exit = [
            item
            for item in resolutions
            if item.edge_id in loop_region.exit_edge_ids and item.state == "selected"
        ]
        if selected_back and selected_exit:
            invocation.mark_failed(
                RuntimeErrorInfo(
                    code="LOOP_CONTINUE_EXIT_CONFLICT",
                    message=(
                        "One loop iteration cannot select both a back edge and "
                        "an exit edge."
                    ),
                    detail={
                        "loop_region_id": loop_region.id,
                        "loop_scope": execution_scope_key(loop_scope),
                        "back_edge_ids": [item.edge_id for item in selected_back],
                        "exit_edge_ids": [item.edge_id for item in selected_exit],
                    },
                )
            )
            return

        if selected_back:
            current = self._frame(loop_scope, loop_region.id)
            next_scope = loop_scope[:-1] + (
                LoopIteration(loop_region.id, current.iteration + 1),
            )
            for edge_id in loop_region.back_edge_ids:
                resolution = next(
                    item for item in resolutions if item.edge_id == edge_id
                )
                invocation.scheduler.resolve_edge(
                    edge_id,
                    state=resolution.state,
                    activation=resolution.activation,
                    scope=next_scope,
                )
            self._resolve_node_instances(
                workflow_ir=workflow_ir,
                invocation=invocation,
                targets=((loop_region.header_node_id, next_scope),),
            )
            return

        if selected_exit:
            invocation.scheduler.exited_loop_instances.add(
                self._loop_instance_key(loop_scope, loop_region.id)
            )
            targets: list[tuple[str, ExecutionScope]] = []
            for edge_id in loop_region.exit_edge_ids:
                edge = workflow_ir.edges[edge_id]
                resolution = next(
                    item for item in resolutions if item.edge_id == edge_id
                )
                targets.extend(
                    self._propagate_finalized_boundary_edge(
                        workflow_ir=workflow_ir,
                        invocation=invocation,
                        loop_scope=loop_scope,
                        edge=edge,
                        resolution=resolution,
                    )
                )
            self._resolve_node_instances(
                workflow_ir=workflow_ir,
                invocation=invocation,
                targets=targets,
            )
            return

        invocation.mark_failed(
            RuntimeErrorInfo(
                code="LOOP_DEAD_END",
                message="Loop iteration selected neither a back edge nor an exit edge.",
                detail={
                    "loop_region_id": loop_region.id,
                    "loop_scope": execution_scope_key(loop_scope),
                },
            )
        )

    def _propagate_finalized_boundary_edge(
        self,
        *,
        workflow_ir: WorkflowIR,
        invocation: Invocation,
        loop_scope: ExecutionScope,
        edge: EdgeIR,
        resolution: EdgeResolution,
    ) -> list[tuple[str, ExecutionScope]]:
        """Move a finalized exit through enclosing loop boundaries if needed."""

        outer_scope = loop_scope[:-1]
        for frame in reversed(outer_scope):
            outer = workflow_ir.graph.loop_regions[frame.loop_region_id]
            if edge.id not in outer.back_edge_ids and edge.id not in outer.exit_edge_ids:
                continue
            owner_scope = self._loop_scope(outer_scope, outer.id)
            invocation.scheduler.resolve_loop_boundary(
                loop_region_id=outer.id,
                loop_scope=owner_scope,
                edge_id=edge.id,
                state=resolution.state,
                activation=resolution.activation,
            )
            self._finalize_loop_boundary(
                workflow_ir=workflow_ir,
                invocation=invocation,
                loop_region=outer,
                loop_scope=owner_scope,
            )
            return []

        target_scope = self._target_scope(workflow_ir, edge, loop_scope)
        invocation.scheduler.resolve_edge(
            edge.id,
            state=resolution.state,
            activation=resolution.activation,
            scope=target_scope,
        )
        return [(edge.to_node, target_scope)]

    def _incoming_edges_for_instance(
        self,
        workflow_ir: WorkflowIR,
        node_id: str,
        scope: ExecutionScope,
    ) -> tuple[str, ...]:
        region = self._header_region(workflow_ir, node_id)
        if region is None:
            return workflow_ir.graph.incoming_edges.get(node_id, ())
        frame = self._frame(scope, region.id)
        return (
            region.external_entry_edge_ids
            if frame.iteration == 0
            else region.back_edge_ids
        )

    def _boundary_owner(
        self,
        workflow_ir: WorkflowIR,
        edge: EdgeIR,
        source_scope: ExecutionScope,
    ) -> LoopRegionIR | None:
        for frame in reversed(source_scope):
            region = workflow_ir.graph.loop_regions[frame.loop_region_id]
            if edge.id in region.back_edge_ids or edge.id in region.exit_edge_ids:
                return region
        return None

    def _target_scope(
        self,
        workflow_ir: WorkflowIR,
        edge: EdgeIR,
        source_scope: ExecutionScope,
    ) -> ExecutionScope:
        target_stack = workflow_ir.graph.node_loop_stacks.get(edge.to_node, ())
        source_ids = tuple(frame.loop_region_id for frame in source_scope)
        common = 0
        while (
            common < len(source_ids)
            and common < len(target_stack)
            and source_ids[common] == target_stack[common]
        ):
            common += 1
        target_scope = source_scope[:common]
        for loop_region_id in target_stack[common:]:
            target_scope += (LoopIteration(loop_region_id, 0),)
        return target_scope

    def _initial_scope(self, workflow_ir: WorkflowIR, node_id: str) -> ExecutionScope:
        return tuple(
            LoopIteration(loop_region_id, 0)
            for loop_region_id in workflow_ir.graph.node_loop_stacks.get(node_id, ())
        )

    def _scope_for_node(
        self,
        workflow_ir: WorkflowIR,
        node_id: str,
        available_scope: ExecutionScope,
    ) -> ExecutionScope:
        depth = len(workflow_ir.graph.node_loop_stacks.get(node_id, ()))
        scope = available_scope[:depth]
        for loop_region_id in workflow_ir.graph.node_loop_stacks.get(node_id, ())[len(scope):]:
            scope += (LoopIteration(loop_region_id, 0),)
        return scope

    def _header_region(
        self, workflow_ir: WorkflowIR, node_id: str
    ) -> LoopRegionIR | None:
        for loop_region_id in reversed(
            workflow_ir.graph.node_loop_stacks.get(node_id, ())
        ):
            region = workflow_ir.graph.loop_regions[loop_region_id]
            if region.header_node_id == node_id:
                return region
        return None

    def _loop_scope(
        self, scope: ExecutionScope, loop_region_id: str
    ) -> ExecutionScope:
        for index, frame in enumerate(scope):
            if frame.loop_region_id == loop_region_id:
                return scope[: index + 1]
        raise KeyError(f"Execution scope does not contain loop: {loop_region_id}")

    def _frame(self, scope: ExecutionScope, loop_region_id: str) -> LoopIteration:
        for frame in scope:
            if frame.loop_region_id == loop_region_id:
                return frame
        raise KeyError(f"Execution scope does not contain loop: {loop_region_id}")

    def _loop_instance_key(
        self, scope: ExecutionScope, loop_region_id: str
    ) -> str:
        return f"{loop_region_id}@{execution_scope_key(self._loop_scope(scope, loop_region_id))}"

    def _ordered_targets(
        self,
        workflow_ir: WorkflowIR,
        targets: Iterable[tuple[str, ExecutionScope]],
    ) -> list[tuple[str, ExecutionScope]]:
        unique = {(node_id, scope) for node_id, scope in targets}
        node_order = {node_id: index for index, node_id in enumerate(workflow_ir.nodes)}
        return sorted(
            unique,
            key=lambda item: (node_order[item[0]], execution_scope_key(item[1])),
        )

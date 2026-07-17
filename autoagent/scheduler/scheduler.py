from __future__ import annotations

from collections.abc import Iterable

from autoagent.compiler import EdgeIR, LoopRegionIR, WorkflowIR
from autoagent.runtime import (
    ConditionContext,
    Invocation,
    NodeExecution,
    RuntimeErrorInfo,
    Session,
)
from autoagent.runtime.scheduler import EdgeActivation, NodeExecutionTransition
from autoagent.runtime.hooks import invoke_hook_async


class Scheduler:
    """Advance one invocation from stable node transitions to ready requests.

    Scheduler never sees running nodes and never executes operators. It consumes
    NodeExecutionTransition objects that were produced after a NodeExecution
    reached a stable state: completed, failed, waiting, interrupted, skipped, or
    cancelled. For completed nodes it evaluates outgoing edges and appends
    NodeExecutionRequest objects to Invocation.scheduler.ready_queue.

    State writes:
      - writes EdgeEvaluation records onto the completed source NodeExecution.
      - writes ready requests into SchedulerContext.
      - resolves acyclic edges once as selected/skipped and schedules a target
        only after every direct incoming edge has resolved.
      - advances loop-region edges as repeatable activations, enforcing one
        selected outgoing edge per completed loop execution.
    """

    def initialize(self, *, workflow_ir: WorkflowIR, invocation: Invocation) -> None:
        """Initialize entry selection exactly once for an Invocation.

        An invocation starts from one selected entry. Every other workflow entry
        is treated as a skipped path, including its outgoing edge resolutions.
        This lets downstream complete fan-in distinguish "not selected for this
        invocation" from "still pending" and prevents multi-entry joins from
        waiting forever.
        """

        cursor = invocation.scheduler
        if cursor.entry_paths_initialized:
            return
        cursor.entry_paths_initialized = True
        cursor.scheduled_node_ids.add(invocation.entry_node_id)

        affected_targets: set[str] = set()
        for entry_node_id in workflow_ir.entry_node_ids:
            if entry_node_id == invocation.entry_node_id:
                continue

            loop_region_id = workflow_ir.graph.node_loop_regions.get(entry_node_id)
            if loop_region_id is not None:
                loop_region = workflow_ir.graph.loop_regions[loop_region_id]
                cursor.skipped_node_ids.update(loop_region.node_ids)
                cursor.exited_loop_region_ids.add(loop_region.id)
                outgoing_edge_ids = loop_region.exit_edge_ids
            else:
                cursor.skipped_node_ids.add(entry_node_id)
                outgoing_edge_ids = workflow_ir.graph.outgoing_edges.get(
                    entry_node_id, ()
                )

            for edge_id in outgoing_edge_ids:
                if edge_id in cursor.edge_resolutions:
                    continue
                edge = workflow_ir.edges[edge_id]
                cursor.resolve_edge(edge_id, state="skipped")
                affected_targets.add(edge.to_node)

        if affected_targets:
            self._resolve_targets(
                workflow_ir=workflow_ir,
                invocation=invocation,
                target_node_ids=affected_targets,
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
                invocation.mark_failed(error)
            elif transition.state in {"waiting", "interrupted", "skipped", "cancelled"}:
                continue

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

        outgoing_edge_ids = workflow_ir.graph.outgoing_edges.get(transition.node_id, ())
        if not outgoing_edge_ids:
            return

        loop_region_id = workflow_ir.graph.node_loop_regions.get(transition.node_id)
        if loop_region_id is not None:
            await self._advance_loop_completed(
                workflow_ir=workflow_ir,
                session=session,
                invocation=invocation,
                source_execution=source_execution,
                loop_region=workflow_ir.graph.loop_regions[loop_region_id],
            )
            return

        affected_targets: set[str] = set()
        for edge_id in outgoing_edge_ids:
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
            invocation.scheduler.resolve_edge(
                edge.id,
                state="selected" if selected else "skipped",
                activation=activation,
            )
            affected_targets.add(edge.to_node)

        self._resolve_targets(
            workflow_ir=workflow_ir,
            invocation=invocation,
            target_node_ids=affected_targets,
        )

    async def _advance_loop_completed(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
        source_execution: NodeExecution,
        loop_region: LoopRegionIR,
    ) -> None:
        invocation.scheduler.entered_loop_region_ids.add(loop_region.id)
        selected_edges: list[EdgeIR] = []
        outgoing_edge_ids = workflow_ir.graph.outgoing_edges.get(
            source_execution.node_id, ()
        )

        for edge_id in outgoing_edge_ids:
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
            if selected:
                selected_edges.append(edge)

        if len(selected_edges) != 1:
            invocation.mark_failed(
                RuntimeErrorInfo(
                    code=(
                        "LOOP_NO_EDGE_ACTIVATED"
                        if not selected_edges
                        else "LOOP_MULTIPLE_EDGES_ACTIVATED"
                    ),
                    message=(
                        "A loop execution must activate exactly one internal or "
                        "exit edge."
                    ),
                    detail={
                        "loop_region_id": loop_region.id,
                        "node_id": source_execution.node_id,
                        "selected_edge_ids": [edge.id for edge in selected_edges],
                    },
                )
            )
            return

        selected_edge = selected_edges[0]
        activation = EdgeActivation(
            edge_id=selected_edge.id,
            source_node_id=selected_edge.from_node,
            source_execution_id=source_execution.id,
        )
        if selected_edge.to_node in loop_region.node_ids:
            invocation.scheduler.enqueue_ready(
                selected_edge.to_node,
                activations=(activation,),
            )
            return

        invocation.scheduler.exited_loop_region_ids.add(loop_region.id)
        affected_targets: set[str] = set()
        for edge_id in loop_region.exit_edge_ids:
            edge = workflow_ir.edges[edge_id]
            is_selected = edge_id == selected_edge.id
            invocation.scheduler.resolve_edge(
                edge_id,
                state="selected" if is_selected else "skipped",
                activation=activation if is_selected else None,
            )
            affected_targets.add(edge.to_node)

        self._resolve_targets(
            workflow_ir=workflow_ir,
            invocation=invocation,
            target_node_ids=affected_targets,
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
            invocation_context=invocation.context,
            session_context=session.context,
            outputs=invocation.outputs,
            edge_id=edge.id,
            source_node_id=edge.from_node,
            target_node_id=edge.to_node,
            source_output=source_output,
        )
        try:
            return bool(await invoke_hook_async(edge.condition, context)), None
        except Exception as exc:
            return None, f"{type(exc).__name__}: {exc}"

    def _resolve_targets(
        self,
        *,
        workflow_ir: WorkflowIR,
        invocation: Invocation,
        target_node_ids: set[str],
    ) -> None:
        """Resolve complete fan-in and recursively propagate skipped paths."""

        pending_targets = list(target_node_ids)
        while pending_targets and invocation.state != "failed":
            target_node_id = pending_targets.pop()
            if (
                target_node_id in invocation.scheduler.scheduled_node_ids
                or target_node_id in invocation.scheduler.skipped_node_ids
            ):
                continue

            loop_region_id = workflow_ir.graph.node_loop_regions.get(target_node_id)
            loop_region = (
                workflow_ir.graph.loop_regions[loop_region_id]
                if loop_region_id is not None
                else None
            )
            if loop_region is not None:
                if target_node_id != loop_region.entry_node_id:
                    invocation.mark_failed(
                        RuntimeErrorInfo(
                            code="LOOP_EXTERNAL_ENTRY_INVALID",
                            message="An external edge reached a non-entry loop node.",
                            detail={
                                "loop_region_id": loop_region.id,
                                "node_id": target_node_id,
                            },
                        )
                    )
                    return
                incoming_edge_ids = loop_region.entry_edge_ids
            else:
                incoming_edge_ids = workflow_ir.graph.incoming_edges.get(
                    target_node_id, ()
                )

            resolutions = [
                invocation.scheduler.edge_resolutions.get(edge_id)
                for edge_id in incoming_edge_ids
            ]
            if any(resolution is None for resolution in resolutions):
                continue

            activations = tuple(
                resolution.activation
                for resolution in resolutions
                if resolution is not None
                and resolution.state == "selected"
                and resolution.activation is not None
            )
            if activations:
                invocation.scheduler.scheduled_node_ids.add(target_node_id)
                if loop_region is not None:
                    invocation.scheduler.entered_loop_region_ids.add(loop_region.id)
                invocation.scheduler.enqueue_ready(
                    target_node_id,
                    activations=activations,
                )
                continue

            if loop_region is not None:
                invocation.scheduler.skipped_node_ids.update(loop_region.node_ids)
                invocation.scheduler.exited_loop_region_ids.add(loop_region.id)
                outgoing_to_resolve = loop_region.exit_edge_ids
            else:
                invocation.scheduler.skipped_node_ids.add(target_node_id)
                outgoing_to_resolve = workflow_ir.graph.outgoing_edges.get(
                    target_node_id, ()
                )

            for edge_id in outgoing_to_resolve:
                if edge_id in invocation.scheduler.edge_resolutions:
                    continue
                edge = workflow_ir.edges[edge_id]
                invocation.scheduler.resolve_edge(edge_id, state="skipped")
                pending_targets.append(edge.to_node)

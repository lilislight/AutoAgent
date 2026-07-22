from __future__ import annotations

import asyncio
from copy import deepcopy
from typing import Any
from uuid import UUID

from autoagent.core.compiler import NodeIR, WorkflowIR, WorkflowVersionSnapshot
from autoagent.core.executor.node_executor import (
    NodeExecutionJob,
    NodeExecutor,
    OperatorCallCheckpoint,
)
from autoagent.core.executor.result import NodeExecutionResult
from autoagent.core.runtime import (
    InputMappingContext,
    IncomingOutput,
    Invocation,
    NodeExecution,
    OperatorCall,
    OutputBindingContext,
    RuntimeErrorInfo,
    RuntimeStore,
    Session,
)
from autoagent.core.runtime.scheduler import NodeExecutionRequest, node_instance_key
from autoagent.core.runtime.hooks import invoke_hook_async, run_sync
from autoagent.core.scheduler import Scheduler


_MISSING = object()


class WorkflowExecutor:
    """Main control loop for one workflow invocation.

    WorkflowExecutor is the only object that connects scheduler decisions,
    node execution, runtime state mutation, and persistence. It is deliberately
    the layer that knows about running NodeExecutions; Scheduler does not.

    Loop order:
      1. Drain stable transitions and let Scheduler produce ready requests.
      2. Drain ready requests, create NodeExecutions, and submit them to
         NodeExecutor without waiting for the whole batch to finish.
      3. If no immediate work exists but nodes are running, await the first
         completed NodeExecution Task.
      4. Apply results on this main control path and persist.
      5. Return only when invocation is completed, failed, or externally waiting.
    """

    def __init__(
        self,
        *,
        scheduler: Scheduler | None = None,
        node_executor: NodeExecutor | None = None,
        runtime_store: RuntimeStore,
    ) -> None:
        self.scheduler = scheduler or Scheduler()
        self.node_executor = node_executor or NodeExecutor()
        self.runtime_store = runtime_store

    def invoke(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
    ) -> Invocation:
        """Synchronous adapter over the async-first execution loop."""

        return run_sync(
            self.ainvoke(
                workflow_ir=workflow_ir,
                session=session,
                invocation=invocation,
            ),
            api_name="invoke",
            async_api_name="ainvoke",
        )

    async def ainvoke(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
    ) -> Invocation:
        self.scheduler.initialize(workflow_ir=workflow_ir, invocation=invocation)
        await self.runtime_store.acheckpoint_invocation(session, invocation)
        try:
            return await self._drive(
                workflow_ir=workflow_ir,
                session=session,
                invocation=invocation,
            )
        except asyncio.CancelledError:
            await self._cancel_invocation(session=session, invocation=invocation)
            raise

    async def arecover(
        self,
        *,
        workflow_ir: WorkflowIR,
        workflow_snapshot: WorkflowVersionSnapshot,
        session: Session,
        invocation: Invocation,
    ) -> Invocation:
        """Automatically continue or terminate one process-interrupted Invocation.

        Recovery never restores Python stacks. A created Invocation restarts its
        ordinary control loop. A running NodeExecution is replayed as a new
        historical NodeExecution only when the current Workflow hash matches,
        every possible selected Operator manifest matches the stored snapshot,
        every Operator permits recovery, and RetryPolicy still has a whole-node
        attempt available. Any failed check interrupts the old Invocation; no
        user-facing manual recovery option is created.
        """

        if invocation.state not in {"created", "running"}:
            return invocation
        if invocation.workflow_definition_hash != workflow_ir.definition_hash:
            await self._interrupt_recovery(
                session,
                invocation,
                code="WORKFLOW_DEFINITION_CHANGED",
                message="Current Workflow definition does not match interrupted work.",
            )
            return invocation
        if (
            invocation.workflow_operator_manifest_hash
            != workflow_snapshot.operator_manifest_hash
        ):
            await self._interrupt_recovery(
                session,
                invocation,
                code="OPERATOR_MANIFEST_CHANGED",
                message=(
                    "Current Operator manifests do not match the interrupted "
                    "Invocation."
                ),
            )
            return invocation
        if invocation.state == "created":
            return await self.ainvoke(
                workflow_ir=workflow_ir,
                session=session,
                invocation=invocation,
            )

        active = [
            execution
            for execution in invocation.node_executions
            if execution.state in {"created", "ready", "running"}
        ]
        if not active:
            invocation.mark_running()
            return await self._drive(
                workflow_ir=workflow_ir,
                session=session,
                invocation=invocation,
            )

        manifest_by_id = {
            manifest.operator_id: manifest
            for manifest in workflow_snapshot.operator_manifests
        }
        jobs: list[NodeExecutionJob] = []
        changed_execution_ids: list[UUID] = []
        for interrupted in active:
            node_ir = workflow_ir.nodes.get(interrupted.node_id)
            reason = self._recovery_rejection_reason(
                node_ir=node_ir,
                interrupted=interrupted,
                manifest_by_id=manifest_by_id,
            )
            if reason is not None:
                await self._interrupt_recovery(
                    session,
                    invocation,
                    code="NODE_RECOVERY_REJECTED",
                    message=reason,
                )
                return invocation

        for interrupted in active:
            node_ir = workflow_ir.nodes[interrupted.node_id]
            changed_execution_ids.append(interrupted.id)
            interrupted.mark_interrupted(
                RuntimeErrorInfo(
                    code="WORKER_LOST",
                    message="Process ended while this NodeExecution was running.",
                )
            )
            request = NodeExecutionRequest(
                node_id=interrupted.node_id,
                activations=interrupted.incoming_activations,
                execution_scope=interrupted.execution_scope,
            )
            map_policy = self._map_policy_for_request(workflow_ir, request)
            replacement = invocation.create_node_execution(
                interrupted.node_id,
                input=deepcopy(interrupted.input),
                idempotency_key=(
                    interrupted.idempotency_key or str(interrupted.id)
                ),
                recovery_of_execution_id=interrupted.id,
                recovery_attempt=interrupted.recovery_attempt + 1,
                incoming_activations=interrupted.incoming_activations,
                execution_scope=interrupted.execution_scope,
            )
            invocation.mark_node_running(replacement.id, input=replacement.input)
            changed_execution_ids.append(replacement.id)
            jobs.append(
                NodeExecutionJob(
                    node_ir=node_ir,
                    node_execution=replacement,
                    input=replacement.input,
                    max_operator_calls=self._remaining_operator_calls(
                        invocation=invocation,
                        node_ir=node_ir,
                    ),
                    map_policy=map_policy,
                    concurrency_key=f"{workflow_ir.workflow_id}:{node_ir.id}",
                    recovery=True,
                    operator_call_checkpoint=self._operator_call_checkpoint(
                        session=session,
                        invocation=invocation,
                        node_execution=replacement,
                    ),
                )
            )

        invocation.mark_running()
        await self.runtime_store.acheckpoint_invocation(
            session,
            invocation,
            node_execution_ids=tuple(changed_execution_ids),
        )
        self.node_executor.submit_batch(jobs, mailbox=invocation.execution_mailbox)
        return await self._drive(
            workflow_ir=workflow_ir,
            session=session,
            invocation=invocation,
        )

    def _recovery_rejection_reason(
        self,
        *,
        node_ir: NodeIR | None,
        interrupted: NodeExecution,
        manifest_by_id: dict[str, Any],
    ) -> str | None:
        if node_ir is None:
            return f"Interrupted node is absent from current Workflow: {interrupted.node_id}"
        retry = node_ir.policy.retry if node_ir.policy is not None else None
        max_attempts = retry.max_attempts if retry is not None else 1
        if interrupted.recovery_attempt + 1 >= max_attempts:
            return (
                f"Node {interrupted.node_id} exhausted RetryPolicy.max_attempts "
                "for whole-node crash replay."
            )
        selection = node_ir.policy.selection if node_ir.policy is not None else None
        try:
            operators = self.node_executor.operator_resolver.resolve_candidates(
                node_ir.capability,
                selection,
            )
        except Exception as exc:
            return f"Node {interrupted.node_id} Operator resolution failed: {exc}"
        for operator in operators:
            current = operator.manifest
            persisted = manifest_by_id.get(operator.id)
            if persisted is None or persisted.manifest_hash != current.manifest_hash:
                return (
                    f"Operator manifest changed or was not persisted: {operator.id}"
                )
            if current.recovery_mode == "never":
                return f"Operator does not permit automatic replay: {operator.id}"
        for call in interrupted.operator_calls:
            persisted_call = call.operator_manifest
            current = next(
                (operator.manifest for operator in operators if operator.id == call.operator_id),
                None,
            )
            if (
                persisted_call is None
                or current is None
                or persisted_call.manifest_hash != current.manifest_hash
            ):
                return f"Interrupted OperatorCall is not recovery compatible: {call.operator_id}"
        return None

    async def _interrupt_recovery(
        self,
        session: Session,
        invocation: Invocation,
        *,
        code: str,
        message: str,
    ) -> None:
        error = RuntimeErrorInfo(code=code, message=message)
        changed_execution_ids = tuple(
            execution.id
            for execution in invocation.node_executions
            if execution.state in {"created", "ready", "running"}
        )
        invocation.interrupt_active_node_executions(error)
        await self.runtime_store.acheckpoint_invocation(
            session,
            invocation,
            node_execution_ids=changed_execution_ids,
        )

    async def _drive(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
    ) -> Invocation:
        """Advance one Invocation until it reaches a stable public state."""

        while True:
            transitions = invocation.scheduler.drain_transitions()
            if transitions:
                await self.scheduler.next(
                    workflow_ir=workflow_ir,
                    session=session,
                    invocation=invocation,
                    transitions=transitions,
                )
                await self.runtime_store.acheckpoint_invocation(
                    session,
                    invocation,
                    node_execution_ids=tuple(
                        transition.node_execution_id for transition in transitions
                    ),
                )
                if invocation.state == "failed":
                    abandoned = await self._abandon_active_work(invocation)
                    await self.runtime_store.acheckpoint_invocation(
                        session,
                        invocation,
                        node_execution_ids=abandoned,
                    )
                    return invocation
                continue

            ready_requests = invocation.scheduler.drain_ready()
            if ready_requests:
                changed_execution_ids, jobs = await self._submit_ready_requests(
                    workflow_ir=workflow_ir,
                    session=session,
                    invocation=invocation,
                    ready_requests=ready_requests,
                )
                await self.runtime_store.acheckpoint_invocation(
                    session,
                    invocation,
                    node_execution_ids=changed_execution_ids,
                )
                if jobs:
                    self.node_executor.submit_batch(
                        jobs,
                        mailbox=invocation.execution_mailbox,
                    )
                if invocation.state == "failed":
                    abandoned = await self._abandon_active_work(invocation)
                    await self.runtime_store.acheckpoint_invocation(
                        session,
                        invocation,
                        node_execution_ids=abandoned,
                    )
                    return invocation
                continue

            if self.node_executor.has_running(invocation.execution_mailbox):
                results = await self.node_executor.wait_next_completed(
                    invocation.execution_mailbox
                )
                changed_execution_ids = []
                for result in results:
                    changed_execution_id = await self._apply_result(
                        workflow_ir=workflow_ir,
                        session=session,
                        invocation=invocation,
                        result=result,
                    )
                    if changed_execution_id is not None:
                        changed_execution_ids.append(changed_execution_id)
                await self.runtime_store.acheckpoint_invocation(
                    session,
                    invocation,
                    node_execution_ids=tuple(changed_execution_ids),
                )
                continue

            if invocation.scheduler.waiting_executions:
                invocation.mark_waiting()
                await self.runtime_store.acheckpoint_invocation(session, invocation)
                return invocation

            if self._is_completed(workflow_ir=workflow_ir, invocation=invocation):
                invocation.mark_completed(
                    result=self._build_invocation_result(
                        workflow_ir=workflow_ir,
                        invocation=invocation,
                    )
                )
                await self.runtime_store.acheckpoint_invocation(session, invocation)
                return invocation

            invocation.mark_failed(
                RuntimeErrorInfo(
                    code="WORKFLOW_DEAD_END",
                    message="Workflow has no ready, running, waiting, or completed exit node.",
                )
            )
            await self.runtime_store.acheckpoint_invocation(session, invocation)
            return invocation

    async def _cancel_invocation(
        self,
        *,
        session: Session,
        invocation: Invocation,
    ) -> None:
        error = RuntimeErrorInfo(
            code="INVOCATION_CANCELLED",
            message="Invocation was cancelled by its caller.",
        )
        changed_execution_ids = tuple(
            execution.id
            for execution in invocation.node_executions
            if execution.state in {"created", "ready", "running"}
        )
        invocation.cancel_active_node_executions(error)
        invocation.mark_cancelled()
        await self.node_executor.abandon(invocation.execution_mailbox)
        await self.runtime_store.acheckpoint_invocation(
            session,
            invocation,
            node_execution_ids=changed_execution_ids,
        )

    def resume(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
        wait_key: str,
        output: Any = _MISSING,
    ) -> Invocation:
        """Synchronous adapter over aresume()."""

        return run_sync(
            self.aresume(
                workflow_ir=workflow_ir,
                session=session,
                invocation=invocation,
                wait_key=wait_key,
                output=output,
            ),
            api_name="resume",
            async_api_name="aresume",
        )

    async def aresume(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
        wait_key: str,
        output: Any = _MISSING,
    ) -> Invocation:
        try:
            return await self._aresume_impl(
                workflow_ir=workflow_ir,
                session=session,
                invocation=invocation,
                wait_key=wait_key,
                output=output,
            )
        except asyncio.CancelledError:
            await self._cancel_invocation(session=session, invocation=invocation)
            raise

    async def _aresume_impl(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
        wait_key: str,
        output: Any = _MISSING,
    ) -> Invocation:
        """Resume one persisted wait and continue the same Invocation.

        The external signal completes the existing waiting NodeExecution and
        exposes its transition to Scheduler. No second NodeExecution record is
        created.
        """

        waiting = invocation.scheduler.waiting_executions.get(wait_key)
        if waiting is None:
            raise KeyError(f"Unknown wait key: {wait_key}")
        node_execution = invocation.get_node_execution(waiting.node_execution_id)
        if node_execution is None:
            raise KeyError(f"Unknown waiting NodeExecution: {waiting.node_execution_id}")

        invocation.scheduler.remove_waiting_execution(wait_key)
        final_output = node_execution.output if output is _MISSING else output
        await self._complete_resumed_execution(
            workflow_ir=workflow_ir,
            session=session,
            invocation=invocation,
            node_execution=node_execution,
            output=final_output,
        )

        if not invocation.scheduler.waiting_executions:
            invocation.mark_running()
        await self.runtime_store.acheckpoint_invocation(
            session,
            invocation,
            node_execution_ids=(node_execution.id,),
        )
        return await self._drive(
            workflow_ir=workflow_ir,
            session=session,
            invocation=invocation,
        )

    async def _submit_ready_requests(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
        ready_requests: list[NodeExecutionRequest],
    ) -> tuple[tuple[UUID, ...], list[NodeExecutionJob]]:
        jobs: list[NodeExecutionJob] = []
        changed_execution_ids: list[UUID] = []
        for request in ready_requests:
            node_ir = workflow_ir.nodes.get(request.node_id)
            if node_ir is None:
                invocation.mark_failed(
                    RuntimeErrorInfo(
                        code="UNKNOWN_NODE",
                        message=f"Ready request references unknown node: {request.node_id}",
                    )
                )
                return tuple(changed_execution_ids), jobs

            resource_error = self._check_node_execution_resource(
                invocation=invocation,
                node_ir=node_ir,
            )
            if resource_error is not None:
                invocation.mark_failed(resource_error)
                return tuple(changed_execution_ids), jobs

            try:
                map_policy = self._map_policy_for_request(workflow_ir, request)
                node_input = await self._build_node_input(
                    workflow_ir=workflow_ir,
                    session=session,
                    invocation=invocation,
                    node_ir=node_ir,
                    request=request,
                    map_policy=map_policy,
                )
            except Exception as exc:
                # Mapping is a workflow data-shaping phase, not an OperatorCall.
                # Finalize the node here so retry and operator fallback cannot run.
                node_execution = invocation.create_node_execution(
                    node_ir.id,
                    incoming_activations=request.activations,
                    execution_scope=request.execution_scope,
                )
                changed_execution_ids.append(node_execution.id)
                invocation.scheduler.scheduled_node_instances.add(
                    node_instance_key(node_ir.id, request.execution_scope)
                )
                invocation.mark_node_failed(
                    node_execution.id,
                    RuntimeErrorInfo(
                        code="INPUT_MAPPING_FAILED",
                        message=str(exc),
                        detail={
                            "node_id": node_ir.id,
                            "error_type": type(exc).__name__,
                        },
                    ),
                )
                continue
            node_execution = invocation.create_node_execution(
                node_ir.id,
                input=node_input,
                incoming_activations=request.activations,
                execution_scope=request.execution_scope,
            )
            changed_execution_ids.append(node_execution.id)
            if node_execution.idempotency_key is None:
                node_execution.idempotency_key = str(node_execution.id)
            invocation.scheduler.scheduled_node_instances.add(
                node_instance_key(node_ir.id, request.execution_scope)
            )
            invocation.mark_node_running(node_execution.id, input=node_input)

            resource_error = self._check_operator_call_resource(
                invocation=invocation,
                node_ir=node_ir,
            )
            if resource_error is not None:
                invocation.mark_node_failed(node_execution.id, resource_error)
                continue

            jobs.append(
                NodeExecutionJob(
                    node_ir=node_ir,
                    node_execution=node_execution,
                    input=node_input,
                    max_operator_calls=self._remaining_operator_calls(
                        invocation=invocation,
                        node_ir=node_ir,
                    ),
                    map_policy=map_policy,
                    concurrency_key=f"{workflow_ir.workflow_id}:{node_ir.id}",
                    operator_call_checkpoint=self._operator_call_checkpoint(
                        session=session,
                        invocation=invocation,
                        node_execution=node_execution,
                    ),
                )
            )

        return tuple(changed_execution_ids), jobs

    async def _apply_result(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
        result: NodeExecutionResult,
    ) -> UUID | None:
        node_execution = invocation.get_node_execution(result.node_execution_id)
        if node_execution is None:
            invocation.mark_failed(
                RuntimeErrorInfo(
                    code="UNKNOWN_NODE_EXECUTION_RESULT",
                    message="NodeExecutor returned a result for an unknown NodeExecution.",
                    detail={"node_execution_id": str(result.node_execution_id)},
                )
            )
            return None

        existing_call_ids = {call.id for call in node_execution.operator_calls}
        for call_result in result.operator_calls:
            if call_result.id not in existing_call_ids:
                node_execution.operator_calls.append(call_result.to_operator_call())
                existing_call_ids.add(call_result.id)
            node_execution.resource_usage.add(
                duration_ms=call_result.resource_usage.duration_ms
            )
        node_execution.operator_calls.sort(key=lambda call: call.call_no)

        runtime_error = self._check_runtime_resource_after_result(
            workflow_ir=workflow_ir,
            invocation=invocation,
            node_execution=node_execution,
        )
        if runtime_error is not None:
            invocation.mark_node_failed(node_execution.id, runtime_error)
            return node_execution.id

        if result.state == "completed":
            try:
                await self._run_output_binding(
                    workflow_ir=workflow_ir,
                    session=session,
                    invocation=invocation,
                    node_execution=node_execution,
                    output=result.output,
                )
            except Exception as exc:
                # Binding runs after successful operator execution. Its failure
                # preserves permitted context writes and finalizes the node
                # without retrying or selecting another operator.
                invocation.mark_node_failed(
                    node_execution.id,
                    RuntimeErrorInfo(
                        code="OUTPUT_BINDING_FAILED",
                        message=str(exc),
                        detail={
                            "node_id": node_execution.node_id,
                            "error_type": type(exc).__name__,
                        },
                    ),
                )
                session.mark_context_updated()
            else:
                invocation.mark_node_completed(node_execution.id, result.output)
                session.mark_context_updated()
        elif result.state == "waiting":
            try:
                invocation.mark_node_waiting(
                    node_execution.id,
                    wait_key=result.wait_key or str(node_execution.id),
                    wait_type=result.wait_type,
                    payload=result.wait_payload,
                    pending_output=result.output,
                )
            except ValueError as exc:
                invocation.mark_node_failed(
                    node_execution.id,
                    RuntimeErrorInfo(
                        code="WAIT_KEY_CONFLICT",
                        message=str(exc),
                        detail={"node_id": node_execution.node_id},
                    ),
                )
        else:
            invocation.mark_node_failed(
                node_execution.id,
                result.error or RuntimeErrorInfo(
                    code="NODE_EXECUTION_FAILED",
                    message="Node execution failed.",
                ),
            )
        return node_execution.id

    def _operator_call_checkpoint(
        self,
        *,
        session: Session,
        invocation: Invocation,
        node_execution: NodeExecution,
    ) -> OperatorCallCheckpoint:
        """Bind a worker call checkpoint to its persisted runtime hierarchy."""

        async def checkpoint(call: OperatorCall) -> None:
            await self.runtime_store.acheckpoint_operator_call(
                session_id=session.id,
                invocation_id=invocation.id,
                node_execution_id=node_execution.id,
                node_id=node_execution.node_id,
                call=call,
            )

        return checkpoint

    async def _build_node_input(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
        node_ir: NodeIR,
        request: NodeExecutionRequest,
        map_policy: Any | None,
    ) -> Any:
        incoming = self._build_incoming_outputs(
            invocation=invocation,
            request=request,
        )
        scoped_incoming = self._scope_incoming_outputs(
            workflow_ir=workflow_ir,
            node_ir=node_ir,
            incoming=incoming,
        )
        if map_policy is not None:
            if len(incoming) != 1:
                raise ValueError("MapPolicy requires exactly one incoming activation.")
            return incoming[0].value
        if callable(node_ir.input_plan):
            return await invoke_hook_async(
                node_ir.input_plan,
                InputMappingContext(
                    invocation_input=invocation.input,
                    invocation_context=invocation.context,
                    session_context=session.context,
                    outputs=invocation.outputs.scoped(node_ir.scope_node_ids),
                    node_id=node_ir.local_id or node_ir.id,
                    incoming=scoped_incoming,
                ),
            )

        incoming_edge_ids = workflow_ir.graph.incoming_edges.get(node_ir.id, ())
        if not incoming_edge_ids:
            return dict(invocation.input)

        if len(incoming) == 1:
            return incoming[0].value

        values: dict[str, Any] = {}
        for item in scoped_incoming:
            values[item.source_node_id] = item.value
        return values

    def _scope_incoming_outputs(
        self,
        *,
        workflow_ir: WorkflowIR,
        node_ir: NodeIR,
        incoming: tuple[IncomingOutput, ...],
    ) -> tuple[IncomingOutput, ...]:
        """Expose local ids for activations authored in the same child scope."""

        scoped: list[IncomingOutput] = []
        for item in incoming:
            edge = workflow_ir.edges.get(item.edge_id)
            same_scope = edge is not None and edge.workflow_path == node_ir.workflow_path
            edge_id = item.edge_id
            source_node_id = item.source_node_id
            if same_scope:
                edge_id = edge.local_id or edge.id
                source_node_id = edge.local_from_node or item.source_node_id
            scoped.append(
                IncomingOutput(
                    edge_id=edge_id,
                    source_node_id=source_node_id,
                    source_execution_id=item.source_execution_id,
                    value=item.value,
                )
            )
        return tuple(scoped)

    def _build_incoming_outputs(
        self,
        *,
        invocation: Invocation,
        request: NodeExecutionRequest,
    ) -> tuple[IncomingOutput, ...]:
        incoming: list[IncomingOutput] = []
        for activation in request.activations:
            source = invocation.get_node_execution(activation.source_execution_id)
            if source is None:
                raise KeyError(
                    "Ready request references unknown source execution: "
                    f"{activation.source_execution_id}"
                )
            incoming.append(
                IncomingOutput(
                    edge_id=activation.edge_id,
                    source_node_id=activation.source_node_id,
                    source_execution_id=activation.source_execution_id,
                    value=source.output,
                )
            )
        return tuple(incoming)

    async def _run_output_binding(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
        node_execution: NodeExecution,
        output: Any,
    ) -> None:
        node_ir = workflow_ir.nodes[node_execution.node_id]
        if not callable(node_ir.output_binding):
            return
        await invoke_hook_async(
            node_ir.output_binding,
            OutputBindingContext(
                invocation_input=invocation.input,
                invocation_context=invocation.context,
                session_context=session.context,
                outputs=invocation.outputs.scoped(node_ir.scope_node_ids),
                node_id=node_ir.local_id or node_ir.id,
                output=output,
            ),
        )

    async def _complete_resumed_execution(
        self,
        *,
        workflow_ir: WorkflowIR,
        session: Session,
        invocation: Invocation,
        node_execution: NodeExecution,
        output: Any,
    ) -> None:
        try:
            await self._run_output_binding(
                workflow_ir=workflow_ir,
                session=session,
                invocation=invocation,
                node_execution=node_execution,
                output=output,
            )
        except Exception as exc:
            # Resumed completion follows the same non-retryable binding contract.
            invocation.mark_node_failed(
                node_execution.id,
                RuntimeErrorInfo(
                    code="OUTPUT_BINDING_FAILED",
                    message=str(exc),
                    detail={
                        "node_id": node_execution.node_id,
                        "error_type": type(exc).__name__,
                    },
                ),
            )
            session.mark_context_updated()
        else:
            invocation.mark_node_completed(node_execution.id, output)
            session.mark_context_updated()

    def _map_policy_for_request(
        self,
        workflow_ir: WorkflowIR,
        request: NodeExecutionRequest,
    ) -> Any | None:
        policies = []
        for activation in request.activations:
            edge = workflow_ir.edges[activation.edge_id]
            if edge.policy is not None and edge.policy.map is not None:
                policies.append(edge.policy.map)
        if len(policies) > 1:
            raise ValueError("A NodeExecution cannot be triggered by multiple MapPolicy edges.")
        return policies[0] if policies else None

    def _check_node_execution_resource(
        self,
        *,
        invocation: Invocation,
        node_ir: NodeIR,
    ) -> RuntimeErrorInfo | None:
        resource = node_ir.policy.resource if node_ir.policy is not None else None
        if resource is None or resource.max_node_executions_per_invocation is None:
            return None
        actual = invocation.count_node_executions(node_ir.id)
        limit = resource.max_node_executions_per_invocation
        if actual >= limit:
            return RuntimeErrorInfo(
                code="RESOURCE_LIMIT_EXCEEDED",
                message="Node execution limit exceeded.",
                detail={
                    "scope": "invocation",
                    "resource": "node_executions",
                    "node_id": node_ir.id,
                    "limit": limit,
                    "actual": actual + 1,
                },
            )
        return None

    async def _abandon_active_work(self, invocation: Invocation) -> tuple[UUID, ...]:
        error = RuntimeErrorInfo(
            code="INVOCATION_FAILED_FAST",
            message="Node execution was cancelled after fail-fast invocation failure.",
        )
        changed_execution_ids = tuple(
            execution.id
            for execution in invocation.node_executions
            if execution.state in {"created", "ready", "running"}
        )
        invocation.cancel_active_node_executions(error)
        await self.node_executor.abandon(invocation.execution_mailbox)
        return changed_execution_ids

    def _check_operator_call_resource(
        self,
        *,
        invocation: Invocation,
        node_ir: NodeIR,
    ) -> RuntimeErrorInfo | None:
        resource = node_ir.policy.resource if node_ir.policy is not None else None
        if resource is None or resource.max_operator_calls_per_invocation is None:
            return None
        actual = invocation.count_operator_calls(node_ir.id)
        limit = resource.max_operator_calls_per_invocation
        if actual >= limit:
            return RuntimeErrorInfo(
                code="RESOURCE_LIMIT_EXCEEDED",
                message="Operator call limit exceeded.",
                detail={
                    "scope": "invocation",
                    "resource": "operator_calls",
                    "node_id": node_ir.id,
                    "limit": limit,
                    "actual": actual + 1,
                },
            )
        return None

    def _remaining_operator_calls(
        self,
        *,
        invocation: Invocation,
        node_ir: NodeIR,
    ) -> int | None:
        """Return the remaining fallback-call budget for this node submission."""

        resource = node_ir.policy.resource if node_ir.policy is not None else None
        if resource is None or resource.max_operator_calls_per_invocation is None:
            return None
        return max(
            0,
            resource.max_operator_calls_per_invocation
            - invocation.count_operator_calls(node_ir.id),
        )

    def _check_runtime_resource_after_result(
        self,
        *,
        workflow_ir: WorkflowIR,
        invocation: Invocation,
        node_execution: NodeExecution,
    ) -> RuntimeErrorInfo | None:
        node_ir = workflow_ir.nodes[node_execution.node_id]
        resource = node_ir.policy.resource if node_ir.policy is not None else None
        if resource is None or resource.max_runtime_ms_per_invocation is None:
            return None
        actual = invocation.sum_node_runtime_ms(node_ir.id)
        limit = resource.max_runtime_ms_per_invocation
        if actual > limit:
            return RuntimeErrorInfo(
                code="RESOURCE_LIMIT_EXCEEDED",
                message="Runtime limit exceeded.",
                detail={
                    "scope": "invocation",
                    "resource": "runtime_ms",
                    "node_id": node_ir.id,
                    "limit": limit,
                    "actual": actual,
                },
            )
        return None

    def _is_completed(self, *, workflow_ir: WorkflowIR, invocation: Invocation) -> bool:
        return any(
            execution.state == "completed" and execution.node_id in workflow_ir.exit_node_ids
            for execution in invocation.node_executions
        )

    def _build_invocation_result(
        self,
        *,
        workflow_ir: WorkflowIR,
        invocation: Invocation,
    ) -> dict[str, Any]:
        outputs: dict[str, Any] = {}
        for node_id in workflow_ir.exit_node_ids:
            execution = invocation.latest_node_execution(node_id)
            if execution is not None and execution.state == "completed":
                outputs[node_id] = deepcopy(execution.output)
        if len(outputs) == 1:
            return {"output": next(iter(outputs.values()))}
        return {"outputs": outputs}

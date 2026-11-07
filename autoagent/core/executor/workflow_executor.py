"""Control loop for one Workflow Invocation.

This module owns scheduling and Runtime transition orchestration. It never
compiles Workflows and exposes no public application API.
"""

from __future__ import annotations

from ..commands.waits import CommandSuspended
from ..commands import Wait
from ..runtime._context_index import context_previews
from contextlib import AbstractAsyncContextManager, nullcontext

import asyncio
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import replace
from typing import Literal
from uuid import uuid4

from ..errors import RuntimeInfrastructureError, RuntimeTransitionError
from ..operators import Operator, ValueContract
from ..operators.context import OperatorCallContext, operator_call_context
from ..workflow import UserEventContext, UserEventSpec
from ..runtime.clocks import unix_time_us
from ..commands import CommandIR, RuntimeHandle
from ..commands.models import runtime_handles
from ..runtime import OperatorCallFailed
from ..runtime import (
    InputMapped, CapabilityResolved, Aggregated, OutputBound, RoutingResolved,
    NodeFaulted, NodeStarted, NodeCompleted, NodeFailed,
    ChildInvocationPhaseChanged, ChildInvocationPlanned, ChildUnitSpec,
    InvocationCompleted, InvocationFailed, InvocationStarted, RuntimeErrorInfo,
    RuntimeState, RuntimeRepository, SessionOpened, RuntimeEvent, TaskRuntime,
    UserEvent, thaw, freeze,
)
from ..workflow import (
    AggregationContext,
    Capability,
    ErrorInfo,
    NodeIR,
    WorkflowIR,
)
from .node_executor import NodeExecutor, UserCallableCancelledError


_USER_EVENT_CONTRACT = ValueContract.create(UserEventSpec, location="User Event")
_log = logging.getLogger(__name__)


CapabilityResolver = Callable[[Capability, object], Operator | Awaitable[Operator]]
EmitRuntimeEvent = Callable[
    [str, str | None, object], Awaitable[RuntimeEvent | None]
]
EmitUserEvent = Callable[
    ..., Awaitable[UserEvent]
]
StartChild = Callable[
    [WorkflowIR, str, str, asyncio.Semaphore | None, asyncio.Event | None],
    asyncio.Task[None],
]
EnsureChildDurable = Callable[[str], Awaitable[None]]


class WorkflowExecutor:
    """Drive exactly one Invocation through Scheduler and NodeExecutor."""

    def __init__(
        self,
        *,
        repository: RuntimeRepository,
        node_executor: NodeExecutor,
        task_runtime: TaskRuntime,
        operator_registry,
        emit: EmitRuntimeEvent,
        emit_user: EmitUserEvent,
        start_child: StartChild,
        ensure_child_durable: EnsureChildDurable,
        capability_resolvers: Mapping[str, CapabilityResolver] | None = None,
        suspend_command=None,
        clock_us=unix_time_us,
        status_runtime=None,
        send_signal_runtime=None,
        receive_signal_runtime=None,
        has_signal_runtime=None,
        release_signal=None,
        cancel_runtime=None,
        resume_runtime=None,
        release_resume=None,
        runtime_identity=None,
        workflow_lookup=None,
        node_admission=None,
        dispatch_blocked=None,
        child_admission: Callable[[str, str], AbstractAsyncContextManager[None]] | None = None,
        flush_user_events: Callable[[set[str]], Awaitable[None]] | None = None,
    ) -> None:
        self._suspend_command = suspend_command
        self._clock_us = clock_us
        self._status_runtime = status_runtime
        self._send_signal_runtime = send_signal_runtime
        self._receive_signal_runtime = receive_signal_runtime
        self._has_signal_runtime = has_signal_runtime
        self._release_signal = release_signal
        self._cancel_runtime = cancel_runtime
        self._resume_runtime = resume_runtime
        self._release_resume = release_resume
        self._runtime_identity = runtime_identity
        self._workflow_lookup = workflow_lookup
        self._node_admission = node_admission or (lambda _: nullcontext(True))
        self._dispatch_blocked = dispatch_blocked or (lambda _: False)
        self._repository = repository
        self._node_executor = node_executor
        self._tasks = task_runtime
        self._operator_registry = operator_registry
        self._emit = emit
        self._emit_user = emit_user
        self._flush_user_events = flush_user_events
        self._start_child = start_child
        self._ensure_child_durable = ensure_child_durable
        self._child_admission = child_admission or (lambda *_: nullcontext())
        self._capability_resolvers = capability_resolvers if capability_resolvers is not None else {}
        self._completion_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    def _occurrence_calls(self, session_id, occurrence_id):
        invocation = self._repository.state(session_id).invocation
        ids = self._repository.execution_index(session_id).calls_by_occurrence.get(occurrence_id, ())
        actual = tuple(invocation.scheduler.operator_calls[key] for key in ids)
        workspace = invocation.scheduler.occurrences[occurrence_id].execution
        from ..runtime.state import OperatorCallState
        reused = tuple(OperatorCallState(item['id'], occurrence_id, item['operator_id'], int(index),
            'completed', None, item['output'], started_at_us=0, completed_at_us=0)
            for index, item in workspace.reused_calls.items())
        return (*reused, *actual)

    def _occurrence_waits(self, session_id, occurrence_id):
        invocation = self._repository.state(session_id).invocation
        ids = self._repository.execution_index(session_id).waits_by_occurrence.get(occurrence_id, ())
        return tuple(invocation.scheduler.waits[key] for key in ids)

    async def drive(self, workflow: WorkflowIR, session_id: str) -> None:
        """Run until terminal state or a stable external Wait boundary."""

        tasks: dict[str, asyncio.Task[None]] = {}
        wake_task: asyncio.Task[bool] | None = None
        wake = self._tasks.wake_event(session_id)
        try:
            while True:
                state = self._repository.state(session_id)
                invocation = state.invocation
                if invocation is None or invocation.terminal or invocation.status == "settling":
                    return
                index = self._repository.execution_index(session_id)
                dispatchable = tuple(dict.fromkeys((*invocation.scheduler.ready, *index.running)))
                for occurrence_id in dispatchable:
                    async with self._node_admission(session_id) as admitted:
                        if not admitted:
                            break
                        if occurrence_id in tasks:
                            continue
                        occurrence = invocation.scheduler.occurrences[occurrence_id]
                        node = workflow.node(occurrence.node_id)
                        limit = node.resource.max_executions_per_invocation
                        current = self._repository.state(session_id).invocation
                        if (occurrence.started_sequence is None and limit is not None
                                and current.scheduler.node_executions.get(node.id, 0) >= limit):
                            await self._emit(session_id, invocation.id, InvocationFailed(RuntimeErrorInfo(
                                "NodeExecutionLimitExceeded",
                                f"Node {node.id!r} exceeded its limit of {limit} executions in this Invocation.",
                            )))
                            return
                        if occurrence.started_sequence is None:
                            incoming = self._incoming_values(current, occurrence_id, detach=False)
                            raw_input = {"invocation_input": current.input
                                if node.input_mapping is not None or not incoming else None, "incoming": incoming}
                            await self._emit(session_id, invocation.id, NodeStarted(occurrence_id, raw_input))
                            incoming = raw_input = None
                        tasks[occurrence_id] = asyncio.create_task(
                            self._execute_occurrence(workflow, session_id, occurrence_id)
                        )
                if not tasks:
                    await self._finish_if_quiescent(workflow, session_id)
                    # A Child completion or resume may make an occurrence ready
                    # while _finish_if_quiescent is awaiting an emitted boundary.
                    # Re-read durable State before this Task returns.  There is
                    # deliberately no await between this check and return, so a
                    # later producer either sees this Task live and wakes it, or
                    # sees it done and starts a replacement drive.
                    current = self._repository.state(session_id).invocation
                    if current is not None and not current.terminal:
                        if not self._dispatch_blocked(session_id) and (current.scheduler.ready or self._repository.execution_index(session_id).running):
                            wake.clear()
                            continue
                    return
                # The drive waits for Tasks, not for an old graph snapshot.
                state = invocation = occurrence = None
                wake_task = asyncio.create_task(wake.wait())
                done, _pending = await asyncio.wait(
                    (*tasks.values(), wake_task),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if wake_task in done:
                    wake.clear()
                else:
                    wake_task.cancel()
                    await asyncio.gather(wake_task, return_exceptions=True)
                wake_task = None
                for occurrence_id, task in tuple(tasks.items()):
                    if task in done:
                        result = (await asyncio.gather(task, return_exceptions=True))[0]
                        del tasks[occurrence_id]
                        if isinstance(result, RuntimeInfrastructureError):
                            raise result
                        if isinstance(result, asyncio.CancelledError):
                            raise RuntimeError(
                                "Node occurrence task was cancelled outside "
                                "the Invocation drive."
                            )
                error = self._fail_fast_error(workflow, session_id)
                if error is not None:
                    current = self._repository.state(session_id).invocation
                    if current is not None and not current.terminal:
                        await self._emit(
                            session_id,
                            current.id,
                            InvocationFailed(error),
                        )
                    return
        except asyncio.CancelledError:
            raise
        except RuntimeInfrastructureError:
            raise
        except BaseException as error:
            state = self._repository.state(session_id)
            invocation = state.invocation
            if invocation is not None and not invocation.terminal:
                await self._emit(
                    session_id,
                    invocation.id,
                    InvocationFailed(
                        RuntimeErrorInfo(
                            type(error).__name__,
                            str(error) or type(error).__name__,
                        )
                    ),
                )
        finally:
            if wake_task is not None and not wake_task.done():
                wake_task.cancel()
                await asyncio.gather(wake_task, return_exceptions=True)
            for task in tasks.values():
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks.values(), return_exceptions=True)
            self._tasks.release_wake_event(session_id)
            self._completion_locks.pop(session_id, None)

    async def _execute_occurrence(self, workflow: WorkflowIR, session_id: str, occurrence_id: str) -> None:
        token = runtime_handles.set(self._runtime_identity(session_id) if self._runtime_identity else (None, None))
        try:
            invocation = _active_invocation(self._repository.state(session_id))
            occurrence = invocation.scheduler.occurrences[occurrence_id]
            from ..commands.invoker import CommandInvoker, command_invoker_context
            with command_invoker_context(CommandInvoker(self, session_id, occurrence_id)), operator_call_context(OperatorCallContext(
                workflow_id=workflow.workflow_id, workflow_revision_id=workflow.workflow_revision_id,
                workflow_version=workflow.workflow_version, session_id=session_id,
                invocation_id=invocation.id, occurrence_id=occurrence_id, node_id=occurrence.node_id,
                attempt=occurrence.recovery_attempts + 1,
                node_usage=workflow.node(occurrence.node_id).resource.usage,
                invocation_usage=workflow.resource.invocation,
                session_usage=workflow.resource.session,
            )):
                await self._execute_occurrence_body(workflow, session_id, occurrence_id)
        finally:
            runtime_handles.reset(token)

    async def _execute_occurrence_body(self, workflow, session_id, occurrence_id):
        state = self._repository.state(session_id)
        invocation = _active_invocation(state)
        invocation_id = invocation.id
        invocation_context, session_context = invocation.context, state.session.context
        occurrence = invocation.scheduler.occurrences[occurrence_id]
        node = workflow.node(occurrence.node_id)
        metrics = None
        phase = "input_mapping"
        async def emit(payload):
            return await self._emit(session_id, invocation_id, payload)
        def current_execution():
            return self._repository.state(session_id).invocation.scheduler.occurrences[occurrence_id].execution
        try:
            execution = current_execution()
            if execution.fault is not None:
                await self._finish_node_error(workflow, node, session_id, occurrence_id, execution.fault)
                return
            latest_calls = {call.unit_index: call for call in self._occurrence_calls(session_id, occurrence_id)}
            failed_call = next((call for call in latest_calls.values()
                if call.status == "failed" and call.retry_at_us is None
                and call.error is not None and call.error.type != "CancelledError"), None)
            if failed_call is not None:
                error = replace(failed_call.error, phase="operator")
                await emit(NodeFaulted(occurrence_id, "operator", error))
                await self._finish_node_error(workflow, node, session_id, occurrence_id, error)
                return
            if "aggregated" in execution.completed_stages:
                output = execution.aggregate_output
            else:
                child_plan = self._child_plan(invocation, occurrence_id) if isinstance(node.executable, CommandIR) else None
                if "input_mapped" in execution.completed_stages:
                    mapped = self._restore_mapped(node, thaw(execution.mapped_input))
                elif child_plan is not None:
                    values = [thaw(unit.input) for unit in child_plan.units]
                    mapped = self._restore_mapped(node, values if node.map is not None else values[0])
                else:
                    saved = self._repository.state(session_id).invocation.scheduler.occurrences[occurrence_id].raw_input
                    incoming = thaw(saved["incoming"])
                    invocation_input = thaw(saved["invocation_input"])
                    mapped, duration = await self._node_executor.timed(self._node_executor.map_input, node,
                        invocation_input=invocation_input, incoming=incoming,
                        invocation_context=invocation.context, session_context=state.session.context)
                    if node.input_mapping is not None:
                        await emit(InputMapped(occurrence_id, self._record_mapped(node, mapped), duration))
                # The validated mapped value owns the execution input. Do not
                # keep the pre-validation thawed graph input throughout a Call.
                incoming = invocation_input = None
                # Keep Context snapshots without retaining the old Scheduler.
                state = invocation = occurrence = None
                phase = "capability_resolution"
                executable_node = node
                if isinstance(node.executable, Capability):
                    async def resolve_unit(index, item):
                        binding = current_execution().capability_bindings.get(str(index))
                        if binding is not None:
                            candidates = self._operator_registry.for_capability(node.executable.id)
                            operator = next((entry.operator for entry in candidates if entry.operator.id == binding["operator_id"]), None)
                            if operator is None:
                                raise RuntimeError("Recovered Operator is unavailable.")
                            return replace(node, executable=operator)
                        parent = OperatorCallContext.current()
                        with operator_call_context(replace(parent, unit_index=index)):
                            selected, duration = await self._node_executor.timed(self._resolve_executable, node, item)
                        await emit(CapabilityResolved(occurrence_id, node.executable.id, selected.executable.id, duration, index))
                        return selected
                phase = "operator"
                if isinstance(node.executable, (CommandIR, Wait)):
                    output = await self._execute_command(node, occurrence_id, mapped, session_id)
                else:
                    async def emit_chunk(chunk):
                        call = OperatorCallContext.current()
                        await self._emit_mapped_user_events(node, session_id=session_id,
                            invocation_id=invocation_id, occurrence_id=occurrence_id,
                            phase="chunk", value=chunk, invocation_context=invocation_context,
                            session_context=session_context, call=call)
                    calls = self._occurrence_calls(session_id, occurrence_id)
                    completed = {item.unit_index: item for item in calls
                        if item.occurrence_id == occurrence_id and item.status == "completed"}
                    execution_options = {}
                    if isinstance(node.executable, Capability):
                        execution_options['resolve_unit'] = resolve_unit
                    execution_options['previous_calls'] = {item.unit_index: item for item in calls}
                    if type(self._node_executor) is NodeExecutor and (node.map is None or node.map.aggregate is None):
                        # This path forwards accepted immutable Call records below;
                        # no user aggregator consumes the transient Python outputs.
                        execution_options['_retain_outputs'] = False
                    result = await self._node_executor.execute(executable_node, occurrence_id, mapped,
                        invocation_context=invocation_context, session_context=session_context,
                        on_call_event=emit, on_stream_chunk=emit_chunk if node.user_events is not None else None,
                        completed_calls=completed, aggregate=False,
                        **execution_options)
                    output, metrics = result.output, result.metrics
                    if node.map is not None and node.map.aggregate is not None:
                        phase = "aggregation"
                        from ..workflow import AggregationContext
                        aggregate_context = AggregationContext(invocation_context=invocation_context,
                            session_context=session_context, inputs=tuple(mapped), outputs=tuple(output))
                        output, duration = await self._node_executor.timed(self._node_executor.call_hook,
                            node.map.aggregate, aggregate_context)
                        record = node.output_contract.to_record(output) if node.output_contract is not None else output
                        accepted = await emit(Aggregated(occurrence_id, record, duration))
                        output = accepted.payload.output
                    else:
                        # The accepted Call outputs have already passed their
                        # contracts. Propagate those owned values, not another
                        # record conversion of the transient executor result.
                        calls = self._occurrence_calls(session_id, occurrence_id)
                        accepted = {item.unit_index: item.output for item in calls
                            if item.occurrence_id == occurrence_id and item.status == "completed"}
                        if node.map is None:
                            output = accepted[0]
                        else:
                            output = freeze(tuple(accepted[index] for index in range(len(mapped))))
            # Intermediate raw results are no longer needed once final output
            # exists. In particular, a small Map aggregate must not pin its rows
            # through later binding, routing or user-event hooks.
            incoming = invocation_input = mapped = calls = completed = result = saved = latest_calls = None
            aggregate_context = accepted = record = resumed = execution = None
            state = invocation = occurrence = child_plan = values = None
            invocation_context = session_context = None
            # Context preview, routing and final commit remain serialized per
            # Session so other node completions cannot invalidate the preview.
            async with self._completion_locks[session_id]:
                latest = self._repository.state(session_id)
                current = _active_invocation(latest)
                execution = current_execution()
                phase = "output_binding"
                if "output_bound" in execution.completed_stages:
                    patch = execution.pending_context_patch
                else:
                    patch, duration = await self._node_executor.timed(self._node_executor.bind_output, node, output,
                        invocation_context=current.context, session_context=latest.session.context)
                    if node.output_binding is not None:
                        accepted = await emit(OutputBound(occurrence_id, patch, duration))
                        patch = accepted.payload.patch
                with context_previews() if patch.session or patch.invocation else nullcontext():
                    candidate_session, candidate_invocation = self._repository.preview_context_patch(session_id, occurrence_id, patch)
                    phase = "condition"
                    execution = current_execution()
                    if "routing_resolved" not in execution.completed_stages:
                        conditions, duration = await self._node_executor.evaluate_conditions(workflow.outgoing(node.id),
                            source_status="complete", source_node_id=node.id, output=output, error=None,
                            invocation_context=candidate_invocation, session_context=candidate_session)
                        if conditions:
                            await emit(RoutingResolved(occurrence_id, "complete", conditions, duration))
                    phase = "validation"
                    await emit(NodeCompleted(occurrence_id, output,
                        metrics={"call_count": metrics.call_count, "peak_parallelism": metrics.peak_parallelism} if metrics else None))
            latest = current = execution = patch = accepted = None
            await self._emit_mapped_user_events(node, session_id=session_id, invocation_id=invocation_id,
                occurrence_id=occurrence_id, phase="output", value=output, invocation_context=candidate_invocation,
                session_context=candidate_session)
        except CommandSuspended:
            return
        except (asyncio.CancelledError, RuntimeInfrastructureError):
            raise
        except BaseException as exc:
            latest = self._repository.state(session_id)
            current = latest.invocation
            if current is None or current.terminal or current.scheduler.occurrences[occurrence_id].status == "completed":
                return
            error = replace(_runtime_error(exc), phase=phase)
            if isinstance(node.executable, (CommandIR, Wait)):
                for call in self._occurrence_calls(session_id, occurrence_id):
                    if call.status == 'running':
                        await emit(OperatorCallFailed(call.id, error))
            await emit(NodeFaulted(occurrence_id, phase, error))
            await self._finish_node_error(workflow, node, session_id, occurrence_id, error)

    async def _finish_node_error(self, workflow, node, session_id, occurrence_id, error):
        try:
            async with self._completion_locks[session_id]:
                state = self._repository.state(session_id)
                inv = _active_invocation(state)
                execution = inv.scheduler.occurrences[occurrence_id].execution
                if execution.routing_source_status != "error":
                    conditions, duration = await self._node_executor.evaluate_conditions(workflow.outgoing(node.id),
                        source_status="error", source_node_id=node.id, output=None,
                        error=ErrorInfo(error.type, error.message), invocation_context=inv.context, session_context=state.session.context)
                    if conditions:
                        await self._emit(session_id, inv.id, RoutingResolved(occurrence_id, "error", conditions, duration))
                await self._emit(session_id, inv.id, NodeFailed(occurrence_id, error))
        except (asyncio.CancelledError, RuntimeInfrastructureError):
            raise
        except BaseException as exc:
            inv = self._repository.state(session_id).invocation
            if inv is not None and not inv.terminal:
                await self._emit(session_id, inv.id, InvocationFailed(_runtime_error(exc)))

    @staticmethod
    def _record_mapped(node, value):
        if node.input_contract is None:
            return value
        if node.map is not None:
            return [node.input_contract.to_record(item) for item in value]
        return node.input_contract.to_record(value)

    @staticmethod
    def _restore_mapped(node, value):
        if node.input_contract is None:
            return value
        if node.map is not None:
            return [node.input_contract.restore(item) for item in value]
        return node.input_contract.restore(value)

    async def _execute_command(self, node, occurrence_id, mapped, session_id):
        from .commands import CommandExecutor
        return await CommandExecutor(self, node, occurrence_id, mapped, session_id).run()

    async def _command_output(self, node, occurrence_id, inputs, outputs, session_id):
        if node.map is not None and node.map.aggregate is not None:
            state = self._repository.state(session_id)
            output, duration = await self._node_executor.timed(self._node_executor.call_hook,
                node.map.aggregate, AggregationContext(invocation_context=state.invocation.context,
                    session_context=state.session.context, inputs=tuple(inputs), outputs=tuple(outputs)))
            record = node.output_contract.to_record(output)
            accepted = await self._emit(session_id, state.invocation.id, Aggregated(occurrence_id, record, duration))
            return accepted.payload.output
        from .commands import output_contract
        contract = output_contract(node)
        return freeze([contract.to_record(value) for value in outputs] if node.map is not None
            else contract.to_record(outputs[0]))

    async def _plan_children(self, node, occurrence_id, value, state, *, creation_id=None):
        """Plan every Child unit once and reuse that plan after recovery/waits."""

        command = node.executable
        child = command.workflow
        entry_node_id = command.entry_node_id
        if node.map is None:
            inputs = [value]
        else:
            if not isinstance(value, list):
                raise TypeError("Map Node input must be a list of Child Workflow inputs.")
            inputs = value
        input_records = (
            [node.input_contract.to_record(item) for item in inputs]
            if node.input_contract is not None
            else inputs
        )
        if not input_records:
            return None

        parent_session = state.session
        parent = state.invocation
        assert parent_session is not None and parent is not None
        plan = parent.child_plans.get(creation_id) if creation_id is not None else self._child_plan(parent, occurrence_id)
        if plan is None:
            creation_id = creation_id or f"{parent.id}:{occurrence_id}:child"
            await self._emit(
                parent_session.id,
                parent.id,
                ChildInvocationPlanned(
                    creation_id=creation_id,
                    parent_occurrence_id=occurrence_id,
                    entry_node_id=entry_node_id,
                    workflow_id=child.workflow_id,
                    workflow_revision_id=child.workflow_revision_id,
                    units=tuple(
                        ChildUnitSpec(
                            unit_index=index,
                            child_session_id=str(uuid4()),
                            child_invocation_id=str(uuid4()),
                            input=item,
                        )
                        for index, item in enumerate(input_records)
                    ),
                ),
            )
            parent = _active_invocation(self._repository.state(parent_session.id))
            plan = parent.child_plans.get(creation_id)
            assert plan is not None
        if plan.entry_node_id != entry_node_id:
            raise RuntimeError("Child plan entry does not match the Command.")
        self._validate_child_plan(node, child, plan, input_records)

        return plan

    async def _ensure_child_unit(
        self,
        child: WorkflowIR,
        parent_session_id: str,
        parent_invocation_id: str,
        creation_id: str,
        unit_index: int,
        capacity: asyncio.Semaphore | None,
    ) -> asyncio.Task[None] | None:
        async with self._child_admission(parent_session_id, parent_invocation_id):
            parent = _active_invocation(self._repository.state(parent_session_id))
            plan = parent.child_plans[creation_id]
            unit = plan.units[unit_index]
            child_state = self._repository.state(unit.session_id)
            if unit.phase == "planned":
                await self._open_compiled(
                    child,
                    thaw(unit.input),
                    unit.session_id,
                    unit.invocation_id,
                    entry_node_id=plan.entry_node_id,
                )
                await self._emit(
                    parent_session_id,
                    parent_invocation_id,
                    ChildInvocationPhaseChanged(creation_id, unit_index, "opened"),
                )
                parent = _active_invocation(self._repository.state(parent_session_id))
                unit = parent.child_plans[creation_id].units[unit_index]
                child_state = self._repository.state(unit.session_id)
        self._validate_child_state(child, unit, child_state)
        if child_state.invocation.entry_node_id != plan.entry_node_id:
            raise RuntimeTransitionError("CHILD_STATE_MISMATCH", "Child entry differs from its durable plan.")
        child_invocation = child_state.invocation
        assert child_invocation is not None
        live = self._tasks.task(unit.session_id)

        if unit.phase == "opened" and (
            live is not None
            or child_invocation.status == "waiting"
            or child_invocation.terminal
        ):
            # Recovery may restart a spawn Child, or let it reach waiting,
            # before the parent Node resumes.  A complete Child Invocation is
            # already durably admitted; reconcile the parent marker before
            # choosing the live/waiting/terminal branch below.
            await self._emit(
                parent_session_id,
                parent_invocation_id,
                ChildInvocationPhaseChanged(
                    creation_id, unit_index, "accepted"
                ),
            )
            parent = _active_invocation(
                self._repository.state(parent_session_id)
            )
            unit = parent.child_plans[creation_id].units[unit_index]
            child_invocation = self._repository.state(unit.session_id).invocation
            assert child_invocation is not None

        if child_invocation.terminal:
            # A checkpoint may be captured after the Child State advanced but
            # before its parent phase marker did.  Reconstruct every durable
            # phase in order; the Reducer intentionally rejects skipped phases.
            if unit.phase == "opened":
                await self._emit(
                    parent_session_id,
                    parent_invocation_id,
                    ChildInvocationPhaseChanged(
                        creation_id, unit_index, "accepted"
                    ),
                )
                parent = _active_invocation(
                    self._repository.state(parent_session_id)
                )
                unit = parent.child_plans[creation_id].units[unit_index]
            if live is not None:
                # Join the Child's short settle tail before deciding whether
                # it already wrote the terminal parent marker.
                await live
                parent = _active_invocation(
                    self._repository.state(parent_session_id)
                )
                unit = parent.child_plans[creation_id].units[unit_index]
            if unit.phase == "accepted":
                await self._ensure_child_durable(unit.session_id)
                await self._emit(
                    parent_session_id,
                    parent_invocation_id,
                    ChildInvocationPhaseChanged(creation_id, unit_index, "terminal"),
                )
            return None
        if live is not None:
            return live
        if child_invocation.status == "waiting":
            return None

        gate: asyncio.Event | None = None
        task = None
        try:
            async with self._child_admission(parent_session_id, parent_invocation_id):
                # Cancellation may have completed between opening and acceptance.
                parent = _active_invocation(self._repository.state(parent_session_id))
                unit = parent.child_plans[creation_id].units[unit_index]
                child_invocation = self._repository.state(unit.session_id).invocation
                if child_invocation is None or child_invocation.terminal:
                    return None
                live = self._tasks.task(unit.session_id)
                if live is not None:
                    return live
                if unit.phase == "opened":
                    gate = asyncio.Event()
                task = self._start_child(
                    child, unit.session_id, unit.invocation_id, capacity, gate,
                )
                if gate is not None:
                    await self._emit(
                        parent_session_id, parent_invocation_id,
                        ChildInvocationPhaseChanged(creation_id, unit_index, "accepted"),
                    )
                    gate.set()
                return task
        except BaseException:
            # A cancelled drive can publish a settle tail; join outside admission.
            if task is not None:
                task.cancel()
                if gate is not None:
                    gate.set()
                await asyncio.gather(task, return_exceptions=True)
            raise

    @staticmethod
    def _child_plan(invocation, occurrence_id: str):
        return next(
            (
                plan
                for plan in invocation.child_plans.values()
                if plan.parent_occurrence_id == occurrence_id
            ),
            None,
        )

    @staticmethod
    def _validate_child_plan(
        node: NodeIR, child: WorkflowIR, plan, inputs: list[object]
    ) -> None:
        if (
            plan.workflow_id != child.workflow_id
            or plan.workflow_revision_id != child.workflow_revision_id
            or len(plan.units) != len(inputs)
            or any(thaw(unit.input) != item for unit, item in zip(plan.units, inputs))
        ):
            raise RuntimeTransitionError(
                "CHILD_PLAN_MISMATCH",
                "Recovered Child plan does not match this Node execution.",
            )

    @staticmethod
    def _validate_child_state(child: WorkflowIR, unit, state: RuntimeState) -> None:
        invocation = state.invocation
        if (
            state.session is None
            or state.session.id != unit.session_id
            or invocation is None
            or invocation.id != unit.invocation_id
            or invocation.workflow_id != child.workflow_id
            or invocation.workflow_revision_id != child.workflow_revision_id
        ):
            raise RuntimeTransitionError(
                "CHILD_STATE_MISMATCH",
                "Child Runtime State does not match its durable plan.",
            )

    @staticmethod
    def _child_handle(child: WorkflowIR, unit) -> RuntimeHandle:
        return RuntimeHandle(
            session_id=unit.session_id,
            invocation_id=unit.invocation_id,
            workflow_id=child.workflow_id,
            workflow_revision_id=child.workflow_revision_id,
        )

    async def _resolve_executable(self, node: NodeIR, value: object) -> NodeIR:
        if not isinstance(node.executable, Capability):
            return node
        registrations = self._operator_registry.for_capability(node.executable.id)
        candidates = tuple(item.operator for item in registrations)
        if not candidates:
            raise RuntimeError(
                f"Capability {node.executable.id!r} has no enabled Operator."
            )
        resolver = self._capability_resolvers.get(node.executable.id)
        if resolver is None:
            default = self._operator_registry.default_for_capability(
                node.executable.id
            )
            if default is not None:
                selected_operator = default
            elif len(candidates) == 1:
                selected_operator = candidates[0]
            else:
                priorities = {
                    item.operator.id: item.priority for item in registrations
                }
                highest = max(priorities[operator.id] for operator in candidates)
                preferred = tuple(
                    operator
                    for operator in candidates
                    if priorities[operator.id] == highest
                )
                if len(preferred) != 1:
                    raise RuntimeError(
                        f"Capability {node.executable.id!r} requires a CapabilityResolver."
                    )
                selected_operator = preferred[0]
        else:
            selected_operator = await self._node_executor.call_hook(
                resolver, node.executable, value
            )
            registered_operator = next(
                (
                    candidate
                    for candidate in candidates
                    if candidate is selected_operator
                ),
                None,
            )
            if registered_operator is None:
                raise RuntimeError(
                    "CapabilityResolver returned an Operator outside the Capability."
                )
            selected_operator = registered_operator
        return replace(node, executable=selected_operator)

    async def _emit_mapped_user_events(
        self,
        node: NodeIR,
        *,
        session_id: str,
        invocation_id: str,
        occurrence_id: str,
        phase: Literal["chunk", "output", "waiting", "wait_resolved", "error"],
        value: object,
        invocation_context: object,
        session_context: object,
        call: OperatorCallContext | None = None,
        handles=None,
    ) -> None:
        if node.user_events is None:
            return
        try:
            if phase in {"chunk", "output"} and self._flush_user_events is not None:
                await self._flush_user_events({invocation_id})
            context = UserEventContext(
                invocation_context=freeze(invocation_context if isinstance(invocation_context, Mapping) else {}),
                session_context=freeze(session_context if isinstance(session_context, Mapping) else {}),
                phase=phase, value=freeze(value), session_id=session_id,
                invocation_id=invocation_id, node_id=node.id, occurrence_id=occurrence_id,
                operator_id=call.operator_id if call is not None else None,
                operator_call_id=call.operator_call_id if call is not None else None,
                unit_index=call.unit_index if call is not None else None,
                **({"self_handle": handles[0], "owner_handle": handles[1]} if handles is not None else {}),
            )
            spec = await self._node_executor.call_hook(node.user_events, context)
            if spec is None:
                return
            if not isinstance(spec, UserEventSpec):
                raise TypeError("User Event Mapper must return UserEventSpec or None.")
            record = _USER_EVENT_CONTRACT.to_record(spec)
            await self._emit_user(session_id, invocation_id, record["kind"], record["payload"], occurrence_id,
                                  persist=record["persist"])
        except asyncio.CancelledError:
            raise
        except Exception as error:
            _log.warning("User Event delivery failed: node=%s occurrence=%s phase=%s error=%s: %s",
                        node.id, occurrence_id, phase, type(error).__name__, str(error))
            _log.debug("User Event failure details", exc_info=True)

    async def _open_compiled(
        self,
        workflow: WorkflowIR,
        value: object,
        session_id: str,
        invocation_id: str,
        entry_node_id: str,
    ) -> None:
        """Idempotently establish a complete Child admission boundary."""

        state = self._repository.state(session_id)
        if state.session is None:
            await self._emit(session_id, None, SessionOpened({}))
            state = self._repository.state(session_id)
        invocation = state.invocation
        if invocation is None:
            await self._emit(
                session_id,
                invocation_id,
                InvocationStarted(
                    workflow.workflow_id,
                    workflow.workflow_revision_id,
                    entry_node_id,
                    value,
                ),
            )
            state = self._repository.state(session_id)
            invocation = state.invocation
        assert invocation is not None
        if (
            invocation.id != invocation_id
            or invocation.workflow_id != workflow.workflow_id
            or invocation.workflow_revision_id != workflow.workflow_revision_id
            or invocation.entry_node_id != (entry_node_id)
            or thaw(invocation.input) != value
        ):
            raise RuntimeTransitionError(
                "CHILD_STATE_MISMATCH",
                "Existing Child admission state belongs to another Invocation.",
            )
        # Parent phases and the Child execution gate cannot advance until
        # the complete Child admission Event is durably accepted by Host.
        await self._ensure_child_durable(session_id)

    async def _finish_if_quiescent(
        self, workflow: WorkflowIR, session_id: str
    ) -> None:
        state = self._repository.state(session_id)
        invocation = _active_invocation(state)
        scheduler = invocation.scheduler
        index = self._repository.execution_index(session_id)
        if any(index.occurrence_counts.get(status, 0) for status in ("ready", "running", "waiting")):
            return
        if index.waiting_count:
            return
        unhandled = [
            item
            for item in scheduler.occurrences.values()
            if item.status == "failed"
            and not self._failure_handled(workflow, scheduler, item.id)
        ]
        if unhandled:
            error = unhandled[0].error or RuntimeErrorInfo(
                "NodeFailed", "Node failed."
            )
            await self._emit(session_id, invocation.id, InvocationFailed(error))
            return
        exits = [
            item
            for item in scheduler.occurrences.values()
            if item.node_id in workflow.exit_node_ids and item.status == "completed"
        ]
        if not exits:
            await self._emit(
                session_id,
                invocation.id,
                InvocationFailed(
                    RuntimeErrorInfo("WorkflowNoOutput", "No Exit completed.")
                ),
            )
            return
        output = (
            exits[0].output
            if len(workflow.exit_node_ids) == 1
            else {item.node_id: item.output for item in exits}
        )
        await self._emit(
            session_id, invocation.id, InvocationCompleted(output)
        )

    def _fail_fast_error(
        self, workflow: WorkflowIR, session_id: str
    ) -> RuntimeErrorInfo | None:
        if workflow.failure_mode != "fail_fast":
            return None
        invocation = self._repository.state(session_id).invocation
        if invocation is None:
            return None
        if not self._repository.execution_index(session_id).occurrence_counts.get("failed", 0):
            return None
        return next(
            (
                item.error or RuntimeErrorInfo("NodeFailed", "Node failed.")
                for item in invocation.scheduler.occurrences.values()
                if item.status == "failed"
                and not self._failure_handled(
                    workflow, invocation.scheduler, item.id
                )
            ),
            None,
        )

    def recovery_preflight_error(
        self,
        workflow: WorkflowIR,
        state: RuntimeState,
    ) -> RuntimeErrorInfo | None:
        """Return a terminal failure already decided by persisted Scheduler State."""

        invocation = state.invocation
        if invocation is None:
            return RuntimeErrorInfo("RecoveryStateInvalid", "Invocation is missing.")
        scheduler = invocation.scheduler
        if not scheduler.initialized:
            return None
        unhandled = next(
            (
                item.error or RuntimeErrorInfo("NodeFailed", "Node failed.")
                for item in scheduler.occurrences.values()
                if item.status == "failed"
                and not self._failure_handled(workflow, scheduler, item.id)
            ),
            None,
        )
        if workflow.failure_mode == "fail_fast" and unhandled is not None:
            return unhandled
        if any(
            item.status in {"ready", "running", "waiting"}
            for item in scheduler.occurrences.values()
        ) or any(item.status == "waiting" for item in scheduler.waits.values()):
            return None
        if unhandled is not None:
            return unhandled
        if not any(
            item.node_id in workflow.exit_node_ids and item.status == "completed"
            for item in scheduler.occurrences.values()
        ):
            return RuntimeErrorInfo("WorkflowNoOutput", "No Exit completed.")
        return None

    @staticmethod
    def _failure_handled(workflow: WorkflowIR, scheduler, occurrence_id: str) -> bool:
        error_edge_ids = {
            edge.id
            for edge in workflow.outgoing(
                scheduler.occurrences[occurrence_id].node_id
            )
            if edge.on == "error"
        }
        return any(
            activation.edge_id in error_edge_ids
            and activation.source_occurrence_id == occurrence_id
            for occurrence in scheduler.occurrences.values()
            for activation in occurrence.activations
        ) or any(
            item.selected
            and item.edge_id in error_edge_ids
            and item.activation is not None
            and item.activation.source_occurrence_id == occurrence_id
            for item in (
                *scheduler.resolutions.values(),
                *scheduler.boundary_resolutions.values(),
            )
        )

    @staticmethod
    def _incoming_values(invocation, occurrence_id: str, *, detach=True) -> dict[str, object]:
        occurrence = invocation.scheduler.occurrences[occurrence_id]
        materialize = thaw if detach else lambda value: value
        result: dict[str, object] = {}
        for activation in occurrence.activations:
            source = invocation.scheduler.occurrences[
                activation.source_occurrence_id
            ]
            if source.status == "completed":
                result[activation.edge_id] = materialize(source.output)
            elif source.error is not None:
                latest_calls = {call.unit_index: call for call in invocation.scheduler.operator_calls.values()
                    if call.occurrence_id == source.id}
                failed_unit = next((call.unit_index for call in latest_calls.values()
                    if call.status == "failed"
                    and call.error is not None and source.error.phase == "operator"
                    and call.error.type == source.error.type and call.error.message == source.error.message), None)
                result[activation.edge_id] = {
                    "node_id": source.node_id, "occurrence_id": source.id,
                    "error": {"type": source.error.type, "message": source.error.message,
                        "phase": source.error.phase, "unit_index": failed_unit},
                    "input": materialize(source.raw_input),
                }
            else:
                result[activation.edge_id] = None
        return result


def _active_invocation(state: RuntimeState):
    invocation = state.invocation
    if invocation is None or invocation.status not in {"running", "waiting"}:
        raise RuntimeTransitionError(
            "INVOCATION_NOT_RUNNING", "Invocation is not running."
        )
    return invocation


def _runtime_error(error: BaseException) -> RuntimeErrorInfo:
    from .commands import CommandCallError
    if isinstance(error, CommandCallError):
        return error.error
    if isinstance(error, UserCallableCancelledError):
        return RuntimeErrorInfo("CancelledError", str(error))
    return RuntimeErrorInfo(
        type(error).__name__, str(error) or type(error).__name__
    )


__all__ = ["CapabilityResolver", "WorkflowExecutor"]

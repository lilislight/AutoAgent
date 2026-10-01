"""Control loop for one Workflow Invocation.

This module owns scheduling and Runtime transition orchestration. It never
compiles Workflows and exposes no public application API.
"""

from __future__ import annotations

from ..commands import SelectResult, SignalCase, TimerCase
from ..commands.waits import CommandSuspended
from ..commands import Wait
from ..runtime._context_index import context_previews
from contextlib import AbstractAsyncContextManager, nullcontext

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import replace
from uuid import uuid4

from ..runtime._execution_index import ExecutionIndex
from ..errors import RuntimeInfrastructureError, RuntimeTransitionError
from ..operators import Operator
from ..commands import SignalReceipt, SignalBatch, SelfHandle, OwnerHandle
from ..commands.signals import SendSignalRequest, ReceiveSignalRequest
from ..workflow import InputMappingContext
from ..commands import CancelReceipt, TimerRequest, TimerResult
from ..runtime.clocks import unix_time_us
from ..commands import CommandIR, RuntimeHandle, RuntimeObservation, ResumeReceipt
from ..commands.models import runtime_handles
from ..runtime import OperatorCallStarted, OperatorCallCompleted, OperatorCallFailed
from ..runtime import (
    InputMapped,
    CapabilityResolved,
    Aggregated,
    OutputBound,
    RoutingResolved,
    NodeFaulted,
    NodeStarted,
    NodeCompleted,
    NodeFailed,
    ChildInvocationPhaseChanged,
    ChildInvocationPlanned,
    ChildUnitSpec,
    InvocationCompleted,
    InvocationFailed,
    InvocationStarted,
    WaitRequested,
    RuntimeErrorInfo,
    RuntimeState,
    SessionOpened,
    RuntimeEvent,
    TaskRuntime,
    UserEvent,
    thaw,
    freeze,
)
from ..workflow import (
    AggregationContext,
    Capability,
    ErrorInfo,
    NodeIR,
    OutputBindingContext,
    WorkflowIR,
)
from .node_executor import NodeExecutor, UserCallableCancelledError


CapabilityResolver = Callable[[Capability, object], Operator | Awaitable[Operator]]
EmitRuntimeEvent = Callable[
    [str, str | None, object], Awaitable[RuntimeEvent | None]
]
EmitUserEvent = Callable[
    [str, str, str, object, str | None], Awaitable[UserEvent]
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
        repository,
        scheduler,
        node_executor: NodeExecutor,
        task_runtime: TaskRuntime,
        operator_registry,
        emit: EmitRuntimeEvent,
        emit_user: EmitUserEvent,
        start_child: StartChild,
        ensure_child_durable: EnsureChildDurable,
        max_node_executions_per_invocation: int,
        capability_resolver: CapabilityResolver | None = None,
        suspend_command=None,
        clock_us=unix_time_us,
        status_runtime=None,
        send_signal_runtime=None,
        receive_signal_runtime=None,
        release_signal=None,
        cancel_runtime=None,
        resume_runtime=None,
        release_resume=None,
        runtime_identity=None,
        child_admission: Callable[[str, str], AbstractAsyncContextManager[None]] | None = None,
    ) -> None:
        self._suspend_command = suspend_command
        self._clock_us = clock_us
        self._status_runtime = status_runtime
        self._send_signal_runtime = send_signal_runtime
        self._receive_signal_runtime = receive_signal_runtime
        self._release_signal = release_signal
        self._cancel_runtime = cancel_runtime
        self._resume_runtime = resume_runtime
        self._release_resume = release_resume
        self._runtime_identity = runtime_identity
        self._repository = repository
        self._scheduler = scheduler
        self._node_executor = node_executor
        self._tasks = task_runtime
        self._operator_registry = operator_registry
        self._emit = emit
        self._emit_user = emit_user
        self._start_child = start_child
        self._ensure_child_durable = ensure_child_durable
        self._child_admission = child_admission or (lambda *_: nullcontext())
        self._max_node_executions = max_node_executions_per_invocation
        self._capability_resolver = capability_resolver
        self._completion_locks: dict[str, asyncio.Lock] = {}

    def _execution_index(self, session_id):
        lookup = getattr(self._repository, "execution_index", None)
        return lookup(session_id) if lookup is not None else ExecutionIndex(self._repository.state(session_id))

    def _occurrence_calls(self, session_id, occurrence_id):
        invocation = self._repository.state(session_id).invocation
        ids = self._execution_index(session_id).calls_by_occurrence.get(occurrence_id, ())
        return tuple(invocation.scheduler.operator_calls[key] for key in ids)

    def _occurrence_waits(self, session_id, occurrence_id):
        invocation = self._repository.state(session_id).invocation
        ids = self._execution_index(session_id).waits_by_occurrence.get(occurrence_id, ())
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
                index = self._execution_index(session_id)
                execution_count = index.started_count
                dispatchable = tuple(dict.fromkeys((*invocation.scheduler.ready, *index.running)))
                for occurrence_id in dispatchable:
                    if occurrence_id in tasks:
                        continue
                    occurrence = invocation.scheduler.occurrences[occurrence_id]
                    node = workflow.node(occurrence.node_id)
                    if occurrence.started_sequence is None and execution_count >= self._max_node_executions:
                        await self._emit(
                            session_id,
                            invocation.id,
                            InvocationFailed(
                                RuntimeErrorInfo(
                                    "InvocationExecutionLimitExceeded",
                                    "Invocation exceeded its Node execution limit.",
                                )
                            ),
                        )
                        return
                    if occurrence.started_sequence is None:
                        await self._emit(session_id, invocation.id, NodeStarted(occurrence_id))
                        execution_count += 1
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
                        if current.scheduler.ready or self._execution_index(session_id).running:
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
            failed_call = next((call for call in self._occurrence_calls(session_id, occurrence_id)
                if call.occurrence_id == occurrence_id and call.status == "failed"
                and call.error is not None and call.error.type != "CancelledError"), None)
            if failed_call is not None:
                await emit(NodeFaulted(occurrence_id, "operator", failed_call.error))
                await self._finish_node_error(workflow, node, session_id, occurrence_id, failed_call.error)
                return
            resumed = next((item for item in self._occurrence_waits(session_id, occurrence_id)
                if item.occurrence_id == occurrence_id and item.status == "resumed"), None)
            if isinstance(node.executable, Wait) and resumed is not None:
                output = node.executable.output_contract.restore(thaw(resumed.response))
                if node.output_contract is not None:
                    output = node.output_contract.to_record(output)
                call = next((c for c in self._occurrence_calls(session_id, occurrence_id) if c.id == resumed.id), None)
                if call is not None and call.status == "running":
                    await emit(OperatorCallCompleted(call.id, output))
            elif "aggregated" in execution.completed_stages:
                output = execution.aggregate_output
            else:
                child_plan = self._child_plan(invocation, occurrence_id) if isinstance(node.executable, CommandIR) else None
                if "input_mapped" in execution.completed_stages:
                    mapped = self._restore_mapped(node, thaw(execution.mapped_input))
                elif child_plan is not None:
                    values = [thaw(unit.input) for unit in child_plan.units]
                    mapped = self._restore_mapped(node, values if node.map is not None else values[0])
                else:
                    incoming = self._incoming_values(invocation, occurrence_id)
                    invocation_input = (
                        thaw(invocation.input) if node.input_mapping is not None or not incoming else None
                    )
                    mapped, duration = await self._node_executor.timed(self._node_executor.map_input, node,
                        invocation_input=invocation_input, incoming=incoming,
                        invocation_context=invocation.context, session_context=state.session.context)
                    if node.input_mapping is not None:
                        await emit(InputMapped(occurrence_id, self._record_mapped(node, mapped), duration))
                # The validated mapped value owns the execution input. Do not
                # keep the pre-validation thawed graph input throughout a Call.
                incoming = invocation_input = None
                if isinstance(node.executable, Wait):
                    calls = self._occurrence_calls(session_id, occurrence_id)
                    call_id = calls[0].id if calls else str(uuid4())
                    request = node.executable.input_contract.to_record(mapped)
                    if not calls:
                        await emit(OperatorCallStarted(call_id, occurrence_id, node.executable.id, 0, request))
                    await emit(WaitRequested(occurrence_id, call_id, request))
                    return
                # Keep Context snapshots without retaining the old Scheduler.
                state = invocation = occurrence = None
                phase = "capability_resolution"
                executable_node = node
                if isinstance(node.executable, Capability):
                    execution = current_execution()
                    if "capability_resolved" in execution.completed_stages:
                        candidates = self._operator_registry.for_capability(node.executable.id)
                        operator = next((item.operator for item in candidates if item.operator.id == execution.resolved_operator_id), None)
                        if operator is None:
                            raise RuntimeError("Recovered Operator is unavailable.")
                        executable_node = replace(node, executable=operator)
                    else:
                        executable_node, duration = await self._node_executor.timed(self._resolve_executable, node, mapped)
                        await emit(CapabilityResolved(occurrence_id, node.executable.id, executable_node.executable.id, duration))
                phase = "operator"
                if isinstance(node.executable, CommandIR):
                    output = await self._execute_command(node, occurrence_id, mapped, session_id)
                else:
                    async def emit_chunk(chunk):
                        try:
                            await self._emit_user(session_id, invocation_id, "stream.chunk", chunk, occurrence_id)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            return
                    calls = self._occurrence_calls(session_id, occurrence_id)
                    completed = {item.unit_index: item for item in calls
                        if item.occurrence_id == occurrence_id and item.status == "completed"}
                    execution_options = {}
                    if type(self._node_executor) is NodeExecutor and (node.map is None or node.map.aggregate is None):
                        # This path forwards accepted immutable Call records below;
                        # no user aggregator consumes the transient Python outputs.
                        execution_options['_retain_outputs'] = False
                    result = await self._node_executor.execute(executable_node, occurrence_id, mapped,
                        invocation_context=invocation_context, session_context=session_context,
                        on_call_event=emit, on_stream_chunk=emit_chunk, completed_calls=completed, aggregate=False,
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
            incoming = invocation_input = mapped = calls = completed = result = None
            aggregate_context = accepted = record = resumed = execution = None
            state = invocation = occurrence = child_plan = values = None
            invocation_context = session_context = None
            # Context preview, routing and final commit remain serialized per
            # Session so other node completions cannot invalidate the preview.
            async with self._completion_locks.setdefault(session_id, asyncio.Lock()):
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
                occurrence_id=occurrence_id, output=output, invocation_context=candidate_invocation,
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
            error = _runtime_error(exc)
            if isinstance(node.executable, (CommandIR, Wait)):
                for call in self._occurrence_calls(session_id, occurrence_id):
                    if call.status == 'running':
                        await emit(OperatorCallFailed(call.id, error))
            await emit(NodeFaulted(occurrence_id, phase, error))
            await self._finish_node_error(workflow, node, session_id, occurrence_id, error)

    async def _finish_node_error(self, workflow, node, session_id, occurrence_id, error):
        try:
            async with self._completion_locks.setdefault(session_id, asyncio.Lock()):
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
        command = node.executable
        invocation_id = self._repository.state(session_id).invocation.id
        async def emit(payload):
            return await self._emit(session_id, invocation_id, payload)
        inputs = list(mapped) if node.map is not None else [mapped]
        calls = {c.unit_index: c for c in self._occurrence_calls(session_id, occurrence_id)}
        for index, value in enumerate(inputs):
            if index not in calls:
                if command.id == 'system_command:timer' and value.delay_us is not None:
                    value = TimerRequest(deadline_at_us=self._clock_us() + value.delay_us)
                record = node.input_contract.to_record(value) if node.input_contract is not None else value
                if command.id == 'system_command:send_signal':
                    state = self._repository.state(session_id)
                    handle = command.handle
                    if isinstance(handle, (SelfHandle, OwnerHandle)):
                        identities = self._runtime_identity(session_id)
                        handle = identities[0 if isinstance(handle, SelfHandle) else 1]
                    elif callable(handle):
                        handle = await self._node_executor.call_hook(handle, InputMappingContext(
                            invocation_context=state.invocation.context, session_context=state.session.context,
                            invocation_input=thaw(state.invocation.input),
                            incoming=self._incoming_values(state.invocation, occurrence_id)))
                    record = SendSignalRequest(handle=RuntimeHandle.model_validate(handle),
                        endpoint=command.endpoint, payload=record).model_dump(mode='python')
                elif command.id in {'system_command:receive_signal', 'system_command:await_signal'}:
                    record = ReceiveSignalRequest(endpoint=command.endpoint, limit=command.limit).model_dump(mode='python')
                if command.id == 'system_command:select':
                    inv = self._repository.state(session_id).invocation
                    ctx = InputMappingContext(invocation_input=thaw(inv.input), incoming=self._incoming_values(inv, occurrence_id),
                        invocation_context=inv.context, session_context=self._repository.state(session_id).session.context)
                    cases = []
                    for name, case in command.cases.items():
                        if isinstance(case, SignalCase):
                            kind, condition = 'signal', {'endpoint': case.endpoint, 'limit': case.limit}
                        elif isinstance(case, TimerCase):
                            kind, condition = 'timer', {'deadline_at_us': case.deadline_at_us if case.deadline_at_us is not None else self._clock_us() + case.delay_us}
                        else:
                            handle = await self._node_executor.call_hook(case.handle, ctx) if callable(case.handle) else case.handle
                            after = await self._node_executor.call_hook(case.after, ctx) if callable(case.after) else case.after
                            kind, condition = 'child', {'handle': RuntimeHandle.model_validate(handle).model_dump(), 'after': after}
                        cases.append({'name': name, 'kind': kind, 'condition': condition})
                    record = {'cases': cases, 'select': True}
                await emit(OperatorCallStarted(str(uuid4()), occurrence_id, command.id, index, record))
        calls = {c.unit_index: c for c in self._occurrence_calls(session_id, occurrence_id)}
        if command.workflow is not None and any(c.status != 'completed' for c in calls.values()):
            handles = await self._create_children(node, occurrence_id, mapped,
                self._repository.state(session_id))
        else:
            handles = inputs
        if command.id in {'system_command:send_signal', 'system_command:receive_signal'}:
            request_type = SendSignalRequest if command.id == 'system_command:send_signal' else ReceiveSignalRequest
            handles = [request_type.model_validate(thaw(calls[index].input)) for index in range(len(inputs))]
        outputs = []
        for index, handle in enumerate(handles):
            call = calls[index]
            if call.status == 'completed':
                typ = (RuntimeHandle if command.id == 'system_command:spawn' else
                    ResumeReceipt if command.id == 'system_command:resume' else
                    CancelReceipt if command.id == 'system_command:cancel' else
                    TimerResult if command.id == 'system_command:timer' else
                    SignalReceipt if command.id == 'system_command:send_signal' else
                    SignalBatch if command.id in {'system_command:receive_signal', 'system_command:await_signal'} else
                    SelectResult if command.id == 'system_command:select' else RuntimeObservation)
                result = typ.model_validate(thaw(call.output))
            else:
                try:
                    if command.id in {'system_command:await', 'system_command:await_any', 'system_command:timer', 'system_command:await_signal', 'system_command:select'}:
                        if command.id == 'system_command:await':
                            kind, condition = 'child', {'handle': RuntimeHandle.model_validate(handle).model_dump(), 'after': None}
                        elif command.id == 'system_command:await_any':
                            kind, condition = 'any', {'select': False, 'cases': [
                                {'name': str(i), 'kind': 'child', 'condition': {'handle': h.model_dump(), 'after': None}}
                                for i, h in enumerate(handle.handles)]}
                        elif command.id == 'system_command:timer':
                            kind, condition = 'timer', {'deadline_at_us': call.input['deadline_at_us']}
                        elif command.id == 'system_command:await_signal':
                            kind, condition = 'signal', thaw(call.input)
                        else:
                            kind, condition = 'any', thaw(call.input)
                        await self._suspend_command(session_id, invocation_id, call.id, kind, condition)
                        raise CommandSuspended()
                    if command.id == 'system_command:resume':
                        result = await self._resume_runtime(session_id, invocation_id, call.id, handle)
                    elif command.id == 'system_command:cancel':
                        result = await self._cancel_runtime(session_id, invocation_id, call.id, handle)
                    elif command.id == 'system_command:send_signal':
                        result = await self._send_signal_runtime(session_id, invocation_id, call.id, handle)
                    elif command.id == 'system_command:receive_signal':
                        result = await self._receive_signal_runtime(session_id, invocation_id, call.id, handle)
                    elif command.id == 'system_command:status':
                        result = await self._status_runtime(session_id, handle)
                    else:
                        if command.id != 'system_command:spawn':
                            raise RuntimeError('Unsupported System Command.')
                        result = handle
                except (CommandSuspended, asyncio.CancelledError, RuntimeInfrastructureError):
                    raise
                except Exception as exc:
                    await emit(OperatorCallFailed(call.id, _runtime_error(exc)))
                    raise
                if command.id != 'system_command:receive_signal':
                    await emit(OperatorCallCompleted(call.id, result.model_dump(mode='python')))
            if command.id == 'system_command:send_signal':
                await self._release_signal(session_id, invocation_id, call.id, handle.handle.session_id)
            if command.id == 'system_command:resume':
                await self._release_resume(session_id, invocation_id, call.id, handle.handle.session_id)
            outputs.append(result)
        if node.map is not None and node.map.aggregate is not None:
            state = self._repository.state(session_id)
            output, duration = await self._node_executor.timed(self._node_executor.call_hook,
                node.map.aggregate, AggregationContext(invocation_context=state.invocation.context,
                    session_context=state.session.context, inputs=tuple(inputs), outputs=tuple(outputs)))
            record = node.output_contract.to_record(output)
            accepted = await emit(Aggregated(occurrence_id, record, duration))
            return accepted.payload.output
        return freeze([x.model_dump(mode='python') for x in outputs] if node.map is not None
            else outputs[0].model_dump(mode='python'))

    async def _create_children(
        self,
        node: NodeIR,
        occurrence_id: str,
        value: object,
        state: RuntimeState,
    ) -> list[RuntimeHandle]:
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
            return []

        parent_session = state.session
        parent = state.invocation
        assert parent_session is not None and parent is not None
        plan = self._child_plan(parent, occurrence_id)
        if plan is None:
            creation_id = f"{parent.id}:{occurrence_id}:child"
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
            plan = self._child_plan(parent, occurrence_id)
            assert plan is not None
        if plan.entry_node_id != entry_node_id:
            raise RuntimeError("Child plan entry does not match the Command.")
        self._validate_child_plan(node, child, plan, input_records)

        limit = min(
            len(plan.units),
            node.map.max_parallelism
            if node.map is not None and node.map.max_parallelism is not None
            else self._node_executor.max_operator_concurrency,
            self._node_executor.max_operator_concurrency,
        )
        capacity = asyncio.Semaphore(limit)
        for unit in plan.units:
            await self._ensure_child_unit(
                child,
                parent_session.id,
                parent.id,
                plan.creation_id,
                unit.unit_index,
                capacity,
            )
        return [self._child_handle(child, unit) for unit in plan.units]

    async def _ensure_child_unit(
        self,
        child: WorkflowIR,
        parent_session_id: str,
        parent_invocation_id: str,
        creation_id: str,
        unit_index: int,
        capacity: asyncio.Semaphore,
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
        if self._capability_resolver is None:
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
                self._capability_resolver, node.executable, value
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
        output: object,
        invocation_context: object,
        session_context: object,
    ) -> None:
        if not node.user_events:
            return
        context = OutputBindingContext(
            invocation_context=(
                invocation_context if isinstance(invocation_context, Mapping) else {}
            ),
            session_context=(
                session_context if isinstance(session_context, Mapping) else {}
            ),
            output=thaw(output),
        )
        for mapping in node.user_events:
            try:
                payload = await self._node_executor.call_hook(mapping.mapper, context)
                payload = mapping.output_contract.to_record(payload)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                try:
                    await self._emit_user(
                        session_id,
                        invocation_id,
                        "user_event.mapping_failed",
                        {
                            "mapping_kind": mapping.kind,
                            "error_type": type(error).__name__,
                            "message": str(error) or type(error).__name__,
                        },
                        occurrence_id,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass
                continue
            try:
                await self._emit_user(
                    session_id,
                    invocation_id,
                    mapping.kind,
                    payload,
                    occurrence_id,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

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
        grouped = state.session is None
        try:
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
        except BaseException:
            raise

    async def _finish_if_quiescent(
        self, workflow: WorkflowIR, session_id: str
    ) -> None:
        state = self._repository.state(session_id)
        invocation = _active_invocation(state)
        scheduler = invocation.scheduler
        index = self._execution_index(session_id)
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
        if not self._execution_index(session_id).occurrence_counts.get("failed", 0):
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
    def _incoming_values(invocation, occurrence_id: str) -> dict[str, object]:
        occurrence = invocation.scheduler.occurrences[occurrence_id]
        result: dict[str, object] = {}
        for activation in occurrence.activations:
            source = invocation.scheduler.occurrences[
                activation.source_occurrence_id
            ]
            result[activation.edge_id] = (
                thaw(source.output)
                if source.status == "completed"
                else {"type": source.error.type, "message": source.error.message}
                if source.error is not None
                else None
            )
        return result


def _active_invocation(state: RuntimeState):
    invocation = state.invocation
    if invocation is None or invocation.status not in {"running", "waiting"}:
        raise RuntimeTransitionError(
            "INVOCATION_NOT_RUNNING", "Invocation is not running."
        )
    return invocation


def _runtime_error(error: BaseException) -> RuntimeErrorInfo:
    if isinstance(error, UserCallableCancelledError):
        return RuntimeErrorInfo("CancelledError", str(error))
    return RuntimeErrorInfo(
        type(error).__name__, str(error) or type(error).__name__
    )


__all__ = ["CapabilityResolver", "WorkflowExecutor"]

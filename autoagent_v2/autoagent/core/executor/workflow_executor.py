"""Control loop for one Workflow Invocation.

This module owns scheduling and Runtime transition orchestration. It never
compiles Workflows and exposes no public application API.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import replace
from typing import cast
from uuid import uuid4

from ..errors import RuntimeInfrastructureError, RuntimeTransitionError
from ..operators import Operator, Wait
from ..runtime import (
    ChildAwaitSuspended,
    ChildInvocationPlan,
    ChildInvocationPhaseChanged,
    ChildInvocationPlanned,
    ChildUnitSpec,
    InvocationCancelled,
    InvocationCompleted,
    InvocationFailed,
    InvocationOpened,
    InvocationState,
    InvocationStarted,
    InvocationWaiting,
    NodeOccurrenceWaiting,
    RuntimeErrorInfo,
    RuntimeState,
    SessionOpened,
    StateReducer,
    StateTransition,
    TaskRuntime,
    UserEvent,
    thaw,
)
from ..workflow import (
    AggregationContext,
    Capability,
    ChildInvocationHandle,
    ErrorInfo,
    NodeIR,
    OutputBindingContext,
    WorkflowIR,
)
from .node_executor import NodeExecutor, UserCallableCancelledError
from .result import ExecutionMetrics, NodeExecutionResult


CapabilityResolver = Callable[[Capability, object], Operator | Awaitable[Operator]]
EmitRuntimeEvent = Callable[
    [str, str | None, object, str | None], Awaitable[StateTransition]
]
EmitUserEvent = Callable[
    [str, str, str, object, str | None], Awaitable[UserEvent]
]
StartChild = Callable[
    [WorkflowIR, str, str, asyncio.Semaphore | None, asyncio.Event | None],
    asyncio.Task[None],
]
BeginChildAdmission = Callable[[str], None]
AbortChildAdmission = Callable[[str], None]
EnsureChildDurable = Callable[[str], Awaitable[None]]


class _ChildAwaitPending(Exception):
    """Internal control signal: durable Child wait replaced this coroutine."""


class WorkflowExecutor:
    """Drive exactly one Invocation through Scheduler and NodeExecutor."""

    def __init__(
        self,
        *,
        journal,
        scheduler,
        node_executor: NodeExecutor,
        task_runtime: TaskRuntime,
        operator_registry,
        emit: EmitRuntimeEvent,
        emit_user: EmitUserEvent,
        start_child: StartChild,
        begin_child_admission: BeginChildAdmission,
        abort_child_admission: AbortChildAdmission,
        ensure_child_durable: EnsureChildDurable,
        max_node_executions_per_invocation: int,
        capability_resolver: CapabilityResolver | None = None,
    ) -> None:
        self._journal = journal
        self._scheduler = scheduler
        self._node_executor = node_executor
        self._tasks = task_runtime
        self._operator_registry = operator_registry
        self._emit = emit
        self._emit_user = emit_user
        self._start_child = start_child
        self._begin_child_admission = begin_child_admission
        self._abort_child_admission = abort_child_admission
        self._ensure_child_durable = ensure_child_durable
        self._max_node_executions = max_node_executions_per_invocation
        self._capability_resolver = capability_resolver
        self._completion_locks: dict[str, asyncio.Lock] = {}

    async def drive(self, workflow: WorkflowIR, session_id: str) -> None:
        """Run until terminal state or a stable external Wait boundary."""

        tasks: dict[str, asyncio.Task[None]] = {}
        wake_task: asyncio.Task[bool] | None = None
        wake = self._tasks.wake_event(session_id)
        try:
            while True:
                state = self._journal.state(session_id)
                invocation = state.invocation
                if invocation is None or invocation.terminal:
                    return
                execution_count = sum(
                    item.started_state_version is not None
                    for item in invocation.scheduler.occurrences.values()
                )
                for occurrence_id in invocation.scheduler.ready:
                    if occurrence_id in tasks:
                        continue
                    occurrence = invocation.scheduler.occurrences[occurrence_id]
                    node = workflow.node(occurrence.node_id)
                    if execution_count >= self._max_node_executions:
                        await self._emit(
                            session_id,
                            invocation.id,
                            InvocationFailed(
                                RuntimeErrorInfo(
                                    "InvocationExecutionLimitExceeded",
                                    "Invocation exceeded its Node execution limit.",
                                )
                            ),
                            None,
                        )
                        return
                    await self._emit(
                        session_id,
                        invocation.id,
                        self._scheduler.start(occurrence_id),
                        None,
                    )
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
                    current = self._journal.state(session_id).invocation
                    if current is not None and not current.terminal:
                        if current.scheduler.ready:
                            wake.clear()
                            continue
                    return
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
                    current = self._journal.state(session_id).invocation
                    if current is not None and not current.terminal:
                        await self._emit(
                            session_id,
                            current.id,
                            InvocationFailed(error),
                            None,
                        )
                    return
        except _ChildAwaitPending:
            return
        except asyncio.CancelledError:
            raise
        except RuntimeInfrastructureError:
            raise
        except BaseException as error:
            state = self._journal.state(session_id)
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
                    None,
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

    async def _execute_occurrence(
        self, workflow: WorkflowIR, session_id: str, occurrence_id: str
    ) -> None:
        state = self._journal.state(session_id)
        invocation = _active_invocation(state)
        occurrence = invocation.scheduler.occurrences[occurrence_id]
        node = workflow.node(occurrence.node_id)
        metrics: ExecutionMetrics | None = None
        try:
            resumed = next(
                (
                    item
                    for item in invocation.scheduler.waits.values()
                    if item.occurrence_id == occurrence_id and item.status == "resumed"
                ),
                None,
            )
            if isinstance(node.executable, Wait) and resumed is not None:
                output = node.executable.output_contract.restore(
                    thaw(resumed.response)
                )
            else:
                child_plan = (
                    self._child_plan(invocation, occurrence_id)
                    if isinstance(node.executable, WorkflowIR)
                    else None
                )
                if child_plan is None:
                    mapped = await self._node_executor.map_input(
                        node,
                        invocation_input=thaw(invocation.input),
                        incoming=self._incoming_values(invocation, occurrence_id),
                        invocation_context=invocation.context,
                        session_context=state.session.context,  # type: ignore[union-attr]
                    )
                else:
                    saved_inputs = [thaw(unit.input) for unit in child_plan.units]
                    if node.input_contract is not None:
                        saved_inputs = [
                            node.input_contract.restore(item)
                            for item in saved_inputs
                        ]
                    mapped = saved_inputs if node.map is not None else saved_inputs[0]
                if isinstance(node.executable, Wait):
                    request = node.executable.input_contract.to_record(mapped)
                    await self._emit(
                        session_id,
                        invocation.id,
                        NodeOccurrenceWaiting(occurrence_id, str(uuid4()), request),
                        None,
                    )
                    return
                output, metrics = await self._execute_node_value(
                    workflow, node, occurrence_id, mapped, state
                )
            if node.output_contract is not None:
                output = node.output_contract.to_record(output)
            completion_lock = self._completion_locks.setdefault(
                session_id, asyncio.Lock()
            )
            async with completion_lock:
                latest = self._journal.state(session_id)
                current = _active_invocation(latest)
                session_context = latest.session.context  # type: ignore[union-attr]
                invocation_context = current.context
                patch = await self._node_executor.bind_output(
                    node,
                    output,
                    invocation_context=invocation_context,
                    session_context=session_context,
                )
                candidate_session_context, candidate_invocation_context = (
                    StateReducer().preview_context_patch(
                        latest, occurrence_id, patch
                    )
                )
                selected = await self._node_executor.select_edges(
                    workflow.outgoing(node.id),
                    source_status="complete",
                    source_node_id=node.id,
                    output=output,
                    error=None,
                    invocation_context=candidate_invocation_context,
                    session_context=candidate_session_context,
                )
                payload = self._scheduler.complete(
                    workflow,
                    latest,
                    occurrence_id,
                    output,
                    selected_edge_ids=selected,
                )
                event = await self._emit(
                    session_id,
                    current.id,
                    replace(
                        payload,
                        patch=patch,
                        metrics=(
                            {
                                "duration_ns": metrics.duration_ns,
                                "call_count": metrics.call_count,
                                "peak_parallelism": metrics.peak_parallelism,
                            }
                            if metrics is not None
                            else None
                        ),
                    ),
                    None,
                )
            await self._emit_mapped_user_events(
                node,
                session_id=session_id,
                invocation_id=current.id,
                occurrence_id=occurrence_id,
                output=output,
                invocation_context=candidate_invocation_context,
                session_context=candidate_session_context,
                causation_id=event.id,
            )
        except _ChildAwaitPending:
            return
        except asyncio.CancelledError:
            raise
        except RuntimeInfrastructureError:
            raise
        except BaseException as exc:
            latest = self._journal.state(session_id)
            current = latest.invocation
            if current is None or current.terminal:
                return
            current_occurrence = current.scheduler.occurrences.get(occurrence_id)
            if current_occurrence is not None and current_occurrence.status == "completed":
                return
            error = _runtime_error(exc)
            try:
                completion_lock = self._completion_locks.setdefault(
                    session_id, asyncio.Lock()
                )
                async with completion_lock:
                    latest = self._journal.state(session_id)
                    current = _active_invocation(latest)
                    session_context = latest.session.context  # type: ignore[union-attr]
                    invocation_context = current.context
                    selected = await self._node_executor.select_edges(
                        workflow.outgoing(node.id),
                        source_status="error",
                        source_node_id=node.id,
                        output=None,
                        error=ErrorInfo(error.type, error.message),
                        invocation_context=invocation_context,
                        session_context=session_context,
                    )
                    payload = self._scheduler.fail(
                        workflow,
                        latest,
                        occurrence_id,
                        error,
                        selected_edge_ids=selected,
                    )
                    await self._emit(
                        session_id, current.id, payload, None
                    )
            except asyncio.CancelledError:
                raise
            except RuntimeInfrastructureError:
                raise
            except BaseException as transition_error:
                final_error = _runtime_error(transition_error)
                await self._emit(
                    session_id, current.id, InvocationFailed(final_error), None
                )

    async def _execute_node_value(
        self,
        workflow: WorkflowIR,
        node: NodeIR,
        occurrence_id: str,
        value: object,
        state: RuntimeState,
    ) -> tuple[object, ExecutionMetrics | None]:
        if isinstance(node.executable, WorkflowIR):
            return await self._execute_child_node(node, occurrence_id, value, state)

        executable_node = await self._resolve_executable(node, value)

        async def emit_call(payload) -> None:
            session_id = state.session.id  # type: ignore[union-attr]
            current = _active_invocation(self._journal.state(session_id))
            await self._emit(session_id, current.id, payload, None)

        async def emit_chunk(chunk: object) -> None:
            session_id = state.session.id  # type: ignore[union-attr]
            current = _active_invocation(self._journal.state(session_id))
            try:
                await self._emit_user(
                    session_id, current.id, "stream.chunk", chunk, occurrence_id
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                # User Events are observations, not canonical Runtime State.
                return

        result = await self._node_executor.execute(
            executable_node,
            occurrence_id,
            value,
            invocation_context=state.invocation.context,  # type: ignore[union-attr]
            session_context=state.session.context,  # type: ignore[union-attr]
            on_call_event=emit_call,
            on_stream_chunk=emit_chunk,
        )
        return result.output, result.metrics

    async def _execute_child_node(
        self,
        node: NodeIR,
        occurrence_id: str,
        value: object,
        state: RuntimeState,
    ) -> tuple[object, None]:
        """Plan every Child unit once and reuse that plan after recovery/waits."""

        child = cast(WorkflowIR, node.executable)
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
            return await self._aggregate_child_outputs(node, inputs, [], state)

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
                    mode=node.execution_mode,
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
                None,
            )
            parent = _active_invocation(self._journal.state(parent_session.id))
            plan = self._child_plan(parent, occurrence_id)
            assert plan is not None
        self._validate_child_plan(node, child, plan, input_records)

        limit = min(
            len(plan.units),
            node.map.max_parallelism
            if node.map is not None and node.map.max_parallelism is not None
            else self._node_executor.max_operator_concurrency,
            self._node_executor.max_operator_concurrency,
        )
        capacity = asyncio.Semaphore(limit)
        tasks: list[asyncio.Task[None]] = []
        for unit in plan.units:
            task = await self._ensure_child_unit(
                child,
                parent_session.id,
                parent.id,
                plan.creation_id,
                unit.unit_index,
                capacity,
            )
            if task is not None:
                tasks.append(task)

        if node.execution_mode == "spawn":
            handles = [self._child_handle(child, unit) for unit in plan.units]
            if node.map is None:
                return handles[0], None
            return await self._aggregate_child_outputs(
                node, inputs, cast(list[object], handles), state
            )

        failed = await self._wait_for_child_tasks_or_failure(plan, tasks)
        if failed is not None:
            message = (
                failed.error.message
                if failed.error is not None
                else "Child Invocation did not complete successfully."
            )
            await self.converge_failed_child_plan(
                parent_session.id,
                parent.id,
                plan.creation_id,
            )
            raise RuntimeError(message)
        parent = _active_invocation(self._journal.state(parent_session.id))
        plan = self._child_plan(parent, occurrence_id)
        assert plan is not None
        child_states = [
            self._journal.state(unit.session_id).invocation for unit in plan.units
        ]
        if any(item is None for item in child_states):
            raise RuntimeError("Child Invocation state is missing.")
        if any(
            item.status in {"created", "running", "waiting"}
            for item in child_states  # type: ignore[union-attr]
        ):
            occurrence = parent.scheduler.occurrences[occurrence_id]
            if occurrence.status == "running":
                await self._emit(
                    parent_session.id,
                    parent.id,
                    ChildAwaitSuspended(plan.creation_id, occurrence_id),
                    None,
                )
            raise _ChildAwaitPending()
        failed = next(
            (item for item in child_states if item.status != "completed"),  # type: ignore[union-attr]
            None,
        )
        if failed is not None:
            await self.converge_failed_child_plan(
                parent_session.id,
                parent.id,
                plan.creation_id,
            )
            raise RuntimeError(
                failed.error.message if failed.error is not None else "Child failed."
            )
        child_output_contract = child.node(child.exit_node_ids[0]).output_contract
        outputs = [thaw(item.output) for item in child_states]  # type: ignore[union-attr]
        if child_output_contract is not None:
            outputs = [child_output_contract.restore(item) for item in outputs]
        return await self._aggregate_child_outputs(node, inputs, outputs, state)

    async def _wait_for_child_tasks_or_failure(
        self,
        plan: ChildInvocationPlan,
        tasks: list[asyncio.Task[None]],
    ) -> InvocationState | None:
        """Observe Child completion incrementally and return the first failure."""

        failed = self._first_unsuccessful_child(plan)
        if failed is not None:
            return failed
        pending = set(tasks)
        while pending:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )
            results = await asyncio.gather(*done, return_exceptions=True)
            infrastructure_error = next(
                (
                    item
                    for item in results
                    if isinstance(item, RuntimeInfrastructureError)
                ),
                None,
            )
            if infrastructure_error is not None:
                raise infrastructure_error
            failed = self._first_unsuccessful_child(plan)
            if failed is not None:
                return failed
        return None

    def _first_unsuccessful_child(
        self, plan: ChildInvocationPlan
    ) -> InvocationState | None:
        for unit in plan.units:
            invocation = self._journal.state(unit.session_id).invocation
            if invocation is not None and invocation.status in {"failed", "cancelled"}:
                return invocation
        return None

    async def converge_failed_child_plan(
        self,
        parent_session_id: str,
        parent_invocation_id: str,
        creation_id: str,
    ) -> None:
        """Cancel non-terminal units, await physical tasks, then close plan phases."""

        parent = _active_invocation(self._journal.state(parent_session_id))
        plan = parent.child_plans[creation_id]
        live_tasks: list[asyncio.Task[None]] = []
        for unit in plan.units:
            child = self._journal.state(unit.session_id).invocation
            if child is None:
                raise RuntimeError("Child Invocation state is missing.")
            if child.terminal:
                continue
            task = self._tasks.task(unit.session_id)
            if task is not None:
                live_tasks.append(task)
            try:
                await self._emit(
                    unit.session_id,
                    unit.invocation_id,
                    InvocationCancelled("Sibling Child Invocation failed."),
                    None,
                )
            except RuntimeTransitionError:
                current = self._journal.state(unit.session_id).invocation
                if current is None or not current.terminal:
                    raise

        for task in live_tasks:
            if not task.done():
                task.cancel()
        if live_tasks:
            await asyncio.gather(*live_tasks, return_exceptions=True)

        for unit_index in range(len(plan.units)):
            parent = _active_invocation(self._journal.state(parent_session_id))
            current_plan = parent.child_plans[creation_id]
            unit = current_plan.units[unit_index]
            if unit.phase == "terminal":
                continue
            child = self._journal.state(unit.session_id).invocation
            if child is None or not child.terminal:
                raise RuntimeError(
                    "Child Invocation did not converge to a terminal state."
                )
            await self._ensure_child_durable(unit.session_id)
            try:
                await self._emit(
                    parent_session_id,
                    parent_invocation_id,
                    ChildInvocationPhaseChanged(creation_id, unit_index, "terminal"),
                    None,
                )
            except RuntimeTransitionError:
                current = _active_invocation(
                    self._journal.state(parent_session_id)
                )
                if current.child_plans[creation_id].units[unit_index].phase != "terminal":
                    raise

    async def _aggregate_child_outputs(
        self,
        node: NodeIR,
        inputs: list[object],
        outputs: list[object],
        state: RuntimeState,
    ) -> tuple[object, None]:
        if node.map is None:
            return outputs[0], None
        if node.map.aggregate is None:
            return outputs, None
        parent = state.invocation
        session = state.session
        assert parent is not None and session is not None
        result = await self._node_executor.call_hook(
            node.map.aggregate,
            AggregationContext(
                invocation_context=parent.context,
                session_context=session.context,
                inputs=tuple(inputs),
                outputs=tuple(outputs),
            ),
        )
        return result, None

    async def _ensure_child_unit(
        self,
        child: WorkflowIR,
        parent_session_id: str,
        parent_invocation_id: str,
        creation_id: str,
        unit_index: int,
        capacity: asyncio.Semaphore,
    ) -> asyncio.Task[None] | None:
        parent = _active_invocation(self._journal.state(parent_session_id))
        plan = parent.child_plans[creation_id]
        unit = plan.units[unit_index]
        child_state = self._journal.state(unit.session_id)
        if unit.phase == "planned":
            await self._open_compiled(
                child,
                thaw(unit.input),
                unit.session_id,
                unit.invocation_id,
            )
            await self._emit(
                parent_session_id,
                parent_invocation_id,
                ChildInvocationPhaseChanged(creation_id, unit_index, "opened"),
                None,
            )
            parent = _active_invocation(self._journal.state(parent_session_id))
            unit = parent.child_plans[creation_id].units[unit_index]
            child_state = self._journal.state(unit.session_id)
        self._validate_child_state(child, unit, child_state)
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
                None,
            )
            parent = _active_invocation(
                self._journal.state(parent_session_id)
            )
            unit = parent.child_plans[creation_id].units[unit_index]
            child_invocation = self._journal.state(unit.session_id).invocation
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
                    None,
                )
                parent = _active_invocation(
                    self._journal.state(parent_session_id)
                )
                unit = parent.child_plans[creation_id].units[unit_index]
            if live is not None:
                # Join the Child's short settle tail before deciding whether
                # it already wrote the terminal parent marker.
                await live
                parent = _active_invocation(
                    self._journal.state(parent_session_id)
                )
                unit = parent.child_plans[creation_id].units[unit_index]
            if unit.phase == "accepted":
                await self._ensure_child_durable(unit.session_id)
                await self._emit(
                    parent_session_id,
                    parent_invocation_id,
                    ChildInvocationPhaseChanged(creation_id, unit_index, "terminal"),
                    None,
                )
            return None
        if live is not None:
            return live
        if child_invocation.status == "waiting":
            return None

        gate: asyncio.Event | None = None
        if unit.phase == "opened":
            gate = asyncio.Event()
        task = self._start_child(
            child,
            unit.session_id,
            unit.invocation_id,
            capacity,
            gate,
        )
        if gate is not None:
            try:
                await self._emit(
                    parent_session_id,
                    parent_invocation_id,
                    ChildInvocationPhaseChanged(creation_id, unit_index, "accepted"),
                    None,
                )
            except BaseException:
                task.cancel()
                gate.set()
                await asyncio.gather(task, return_exceptions=True)
                raise
            gate.set()
        return task

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
            plan.mode != node.execution_mode
            or plan.workflow_id != child.workflow_id
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
    def _child_handle(child: WorkflowIR, unit) -> ChildInvocationHandle:
        return cast(
            ChildInvocationHandle,
            {
                "session_id": unit.session_id,
                "invocation_id": unit.invocation_id,
                "workflow_id": child.workflow_id,
                "workflow_revision_id": child.workflow_revision_id,
            },
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
        causation_id: str,
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
            output=output,
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
                            "causation_id": causation_id,
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

    async def invoke_compiled(
        self,
        workflow: WorkflowIR,
        value: object,
        session_id: str,
        invocation_id: str,
    ) -> None:
        await self._open_compiled(workflow, value, session_id, invocation_id)
        await self.drive(workflow, session_id)

    async def _open_compiled(
        self,
        workflow: WorkflowIR,
        value: object,
        session_id: str,
        invocation_id: str,
    ) -> None:
        """Idempotently establish a complete Child admission boundary."""

        state = self._journal.state(session_id)
        grouped = state.session is None
        if grouped:
            self._begin_child_admission(session_id)
        try:
            if state.session is None:
                await self._emit(session_id, None, SessionOpened({}), None)
                state = self._journal.state(session_id)
            invocation = state.invocation
            if invocation is None:
                await self._emit(
                    session_id,
                    invocation_id,
                    InvocationOpened(
                        workflow.workflow_id,
                        workflow.workflow_revision_id,
                        _single_entry(workflow),
                        value,
                    ),
                    None,
                )
                state = self._journal.state(session_id)
                invocation = state.invocation
            assert invocation is not None
            if (
                invocation.id != invocation_id
                or invocation.workflow_id != workflow.workflow_id
                or invocation.workflow_revision_id != workflow.workflow_revision_id
                or thaw(invocation.input) != value
            ):
                raise RuntimeTransitionError(
                    "CHILD_STATE_MISMATCH",
                    "Existing Child admission state belongs to another Invocation.",
                )
            causation_id: str | None = None
            if invocation.status == "created":
                started = await self._emit(
                    session_id, invocation_id, InvocationStarted(), None
                )
                causation_id = started.id
                invocation = self._journal.state(session_id).invocation
                assert invocation is not None
            if invocation.status == "running" and not invocation.scheduler.initialized:
                await self._emit(
                    session_id,
                    invocation_id,
                    self._scheduler.initialize(
                        workflow, self._journal.state(session_id)
                    ),
                    causation_id,
                )
            # Parent phases and the Child execution gate cannot advance until
            # the complete Child admission Event is durably accepted by Host.
            await self._ensure_child_durable(session_id)
        except BaseException:
            if grouped:
                self._abort_child_admission(session_id)
            raise

    async def _finish_if_quiescent(
        self, workflow: WorkflowIR, session_id: str
    ) -> None:
        state = self._journal.state(session_id)
        invocation = _active_invocation(state)
        scheduler = invocation.scheduler
        if any(
            item.status in {"ready", "running"}
            for item in scheduler.occurrences.values()
        ):
            return
        if any(item.status == "waiting" for item in scheduler.occurrences.values()):
            if invocation.status == "running":
                await self._emit(
                    session_id, invocation.id, InvocationWaiting(), None
                )
            return
        if any(item.status == "waiting" for item in scheduler.waits.values()):
            if invocation.status == "running":
                await self._emit(
                    session_id, invocation.id, InvocationWaiting(), None
                )
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
            await self._emit(session_id, invocation.id, InvocationFailed(error), None)
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
                None,
            )
            return
        output = (
            thaw(exits[0].output)
            if len(workflow.exit_node_ids) == 1
            else {item.node_id: thaw(item.output) for item in exits}
        )
        await self._emit(
            session_id, invocation.id, InvocationCompleted(output), None
        )

    def _fail_fast_error(
        self, workflow: WorkflowIR, session_id: str
    ) -> RuntimeErrorInfo | None:
        if workflow.failure_mode != "fail_fast":
            return None
        invocation = self._journal.state(session_id).invocation
        if invocation is None:
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


def _single_entry(workflow: WorkflowIR) -> str:
    if len(workflow.entry_node_ids) != 1:
        raise RuntimeTransitionError(
            "INVOCATION_ENTRY_REQUIRED",
            "Workflow with multiple Entries requires entry_node_id.",
        )
    return workflow.entry_node_ids[0]


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

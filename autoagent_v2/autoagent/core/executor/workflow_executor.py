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

from ..errors import RuntimeTransitionError
from ..operators import Operator, Wait
from ..runtime import (
    InvocationCancelled,
    ChildInvocationLinked,
    InvocationCompleted,
    InvocationFailed,
    InvocationOpened,
    InvocationStarted,
    InvocationWaiting,
    NodeOccurrenceWaiting,
    RuntimeErrorInfo,
    RuntimeEvent,
    RuntimeState,
    SessionOpened,
    StateReducer,
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
from .node_executor import NodeExecutor
from .result import ExecutionMetrics, NodeExecutionResult


CapabilityResolver = Callable[[Capability, object], Operator | Awaitable[Operator]]
EmitRuntimeEvent = Callable[
    [str, str | None, object, str | None], Awaitable[RuntimeEvent]
]
EmitUserEvent = Callable[
    [str, str, str, object, str | None], Awaitable[UserEvent]
]


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
        self._max_node_executions = max_node_executions_per_invocation
        self._capability_resolver = capability_resolver
        self._node_concurrency: dict[tuple[str, str], asyncio.Semaphore] = {}

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
                for occurrence_id in invocation.scheduler.ready:
                    if occurrence_id in tasks:
                        continue
                    occurrence = invocation.scheduler.occurrences[occurrence_id]
                    node = workflow.node(occurrence.node_id)
                    count = sum(
                        item.node_id == occurrence.node_id
                        and item.started_state_version is not None
                        for item in invocation.scheduler.occurrences.values()
                    )
                    limit = (
                        node.max_occurrences_per_invocation
                        or self._max_node_executions
                    )
                    if count >= limit:
                        await self._emit(
                            session_id,
                            invocation.id,
                            InvocationFailed(
                                RuntimeErrorInfo(
                                    "NodeExecutionLimitExceeded",
                                    f"Node {occurrence.node_id!r} exceeded execution limit.",
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
                    tasks[occurrence_id] = asyncio.create_task(
                        self._execute_occurrence(workflow, session_id, occurrence_id)
                    )
                if not tasks:
                    break
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
                        await asyncio.gather(task, return_exceptions=True)
                        del tasks[occurrence_id]
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
            await self._finish_if_quiescent(workflow, session_id)
        except asyncio.CancelledError:
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
                output = node.executable.output_contract.validate(thaw(resumed.response))
            else:
                mapped = await self._node_executor.map_input(
                    node,
                    invocation_input=thaw(invocation.input),
                    incoming=self._incoming_values(invocation, occurrence_id),
                    invocation_context=invocation.context,
                    session_context=state.session.context,  # type: ignore[union-attr]
                )
                if isinstance(node.executable, Wait):
                    request = node.executable.input_contract.validate(mapped)
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
            latest = self._journal.state(session_id)
            current = _active_invocation(latest)
            patch = await self._node_executor.bind_output(
                node,
                output,
                invocation_context=current.context,
                session_context=latest.session.context,  # type: ignore[union-attr]
            )
            candidate_session_context, candidate_invocation_context = (
                StateReducer().preview_context_patch(latest, occurrence_id, patch)
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
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            latest = self._journal.state(session_id)
            current = latest.invocation
            if current is None or current.terminal:
                return
            error = RuntimeErrorInfo(type(exc).__name__, str(exc) or type(exc).__name__)
            try:
                selected = await self._node_executor.select_edges(
                    workflow.outgoing(node.id),
                    source_status="error",
                    source_node_id=node.id,
                    output=None,
                    error=ErrorInfo(error.type, error.message),
                    invocation_context=current.context,
                    session_context=latest.session.context,  # type: ignore[union-attr]
                )
                payload = self._scheduler.fail(
                    workflow,
                    latest,
                    occurrence_id,
                    error,
                    selected_edge_ids=selected,
                )
                await self._emit(session_id, current.id, payload, None)
            except asyncio.CancelledError:
                raise
            except BaseException as transition_error:
                final_error = RuntimeErrorInfo(
                    type(transition_error).__name__,
                    str(transition_error) or type(transition_error).__name__,
                )
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
            await self._emit_user(
                session_id, current.id, "stream.chunk", chunk, occurrence_id
            )

        policy = executable_node.operator_policy
        prior_calls = 0
        prior_runtime_ns = 0
        if policy is not None:
            invocation = state.invocation
            assert invocation is not None
            occurrences = invocation.scheduler.occurrences
            matching_occurrence_ids = {
                item.id for item in occurrences.values() if item.node_id == node.id
            }
            prior_calls = sum(
                1
                for call in invocation.scheduler.operator_calls.values()
                if call.occurrence_id in matching_occurrence_ids
            )
            prior_runtime_ns = sum(
                int(item.metrics.get("duration_ns", 0))  # type: ignore[union-attr]
                for item in occurrences.values()
                if item.node_id == node.id and isinstance(item.metrics, Mapping)
            )
        remaining_calls = (
            policy.max_operator_calls_per_invocation - prior_calls
            if policy is not None
            and policy.max_operator_calls_per_invocation is not None
            else None
        )

        async def execute_operator() -> NodeExecutionResult:
            execution = self._node_executor.execute(
                    executable_node,
                    occurrence_id,
                    value,
                    invocation_context=state.invocation.context,  # type: ignore[union-attr]
                    session_context=state.session.context,  # type: ignore[union-attr]
                    on_call_event=emit_call,
                    on_stream_chunk=emit_chunk,
                    max_calls=remaining_calls,
                )
            if (
                policy is None
                or policy.max_runtime_ms_per_invocation is None
            ):
                return await execution
            remaining_ns = (
                policy.max_runtime_ms_per_invocation * 1_000_000
                - prior_runtime_ns
            )
            if remaining_ns <= 0:
                execution.close()
                raise TimeoutError(
                    "Operator runtime limit exceeded for this Invocation."
                )
            try:
                return await asyncio.wait_for(
                    execution, remaining_ns / 1_000_000_000
                )
            except TimeoutError as error:
                raise TimeoutError(
                    "Operator runtime limit exceeded for this Invocation."
                ) from error

        if policy is not None and policy.max_concurrency is not None:
            key = (workflow.workflow_revision_id, node.id)
            semaphore = self._node_concurrency.setdefault(
                key, asyncio.Semaphore(policy.max_concurrency)
            )
            async with semaphore:
                result = await execute_operator()
        else:
            result = await execute_operator()
        return result.output, result.metrics

    async def _execute_child_node(
        self,
        node: NodeIR,
        occurrence_id: str,
        value: object,
        state: RuntimeState,
    ) -> tuple[object, None]:
        """Apply the same one-occurrence Map boundary to Child Workflows."""

        if node.map is None:
            return await self._execute_child_workflow(
                node, occurrence_id, value, state
            )
        if not isinstance(value, list):
            raise TypeError("Map Node input must be a list of Child Workflow inputs.")

        limit = min(
            len(value) or 1,
            node.map.max_parallelism
            or self._node_executor.max_operator_concurrency,
            self._node_executor.max_operator_concurrency,
        )
        child_capacity = asyncio.Semaphore(limit)
        results: list[tuple[object, None] | None] = [None] * len(value)
        next_index = 0
        index_lock = asyncio.Lock()

        async def worker() -> None:
            nonlocal next_index
            while True:
                async with index_lock:
                    if next_index >= len(value):
                        return
                    index = next_index
                    next_index += 1
                results[index] = await self._execute_child_workflow(
                    node,
                    occurrence_id,
                    value[index],
                    state,
                    child_capacity=child_capacity,
                )

        tasks = tuple(asyncio.create_task(worker()) for _ in range(limit))
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            raise

        outputs = [item[0] for item in results if item is not None]
        if node.map.aggregate is None:
            return outputs, None
        parent = state.invocation
        parent_session = state.session
        assert parent is not None and parent_session is not None
        aggregated = await self._node_executor.call_hook(
            node.map.aggregate,
            AggregationContext(
                invocation_context=parent.context,
                session_context=parent_session.context,
                inputs=tuple(value),
                outputs=tuple(outputs),
            ),
        )
        return aggregated, None

    async def _execute_child_workflow(
        self,
        node: NodeIR,
        occurrence_id: str,
        value: object,
        state: RuntimeState,
        *,
        child_capacity: asyncio.Semaphore | None = None,
    ) -> tuple[object, None]:
        child = cast(WorkflowIR, node.executable)
        child_session = str(uuid4())
        child_invocation = str(uuid4())
        handle: ChildInvocationHandle = {
            "session_id": child_session,
            "invocation_id": child_invocation,
            "workflow_id": child.workflow_id,
            "workflow_revision_id": child.workflow_revision_id,
        }
        parent = state.invocation
        assert parent is not None
        parent_session = state.session
        assert parent_session is not None
        await self._emit(
            parent_session.id,
            parent.id,
            ChildInvocationLinked(
                occurrence_id,
                child_session,
                child_invocation,
                child.workflow_id,
                child.workflow_revision_id,
            ),
            None,
        )

        await self._open_compiled(child, value, child_session, child_invocation)

        async def drive_child() -> None:
            if child_capacity is None:
                await self.drive(child, child_session)
                return
            async with child_capacity:
                await self.drive(child, child_session)

        task = asyncio.create_task(drive_child())
        self._tasks.track(child_session, child_invocation, task)
        if node.execution_mode == "spawn":
            return handle, None
        try:
            await task
            while True:
                child_state = self._journal.state(child_session).invocation
                if child_state is None:
                    raise RuntimeError("Child Invocation state is missing.")
                if child_state.status not in {"running", "waiting"}:
                    break
                await self._tasks.wait_update(child_invocation)
        except asyncio.CancelledError:
            child_state = self._journal.state(child_session).invocation
            if child_state is not None and not child_state.terminal:
                await self._emit(
                    child_session,
                    child_invocation,
                    InvocationCancelled("Parent stopped while awaiting Child."),
                    None,
                )
                child_task = self._tasks.task(child_session)
                if child_task is not None:
                    child_task.cancel()
            raise
        finally:
            self._tasks.release_update_event(child_invocation)
        child_state = self._journal.state(child_session).invocation
        assert child_state is not None
        if child_state.status != "completed":
            raise RuntimeError(
                child_state.error.message if child_state.error else "Child failed."
            )
        return thaw(child_state.output), None

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
            if selected_operator not in candidates:
                raise RuntimeError(
                    "CapabilityResolver returned an Operator outside the Capability."
                )
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
                continue
            await self._emit_user(
                session_id,
                invocation_id,
                mapping.kind,
                payload,
                occurrence_id,
            )

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
        """Create a complete durable Child Invocation start boundary."""

        await self._emit(
            session_id,
            None,
            SessionOpened(workflow.workflow_id, {}),
            None,
        )
        opened = await self._emit(
            session_id,
            invocation_id,
            InvocationOpened(
                workflow.workflow_revision_id, _single_entry(workflow), value
            ),
            None,
        )
        started = await self._emit(
            session_id,
            invocation_id,
            InvocationStarted(),
            opened.id,
        )
        await self._emit(
            session_id,
            invocation_id,
            self._scheduler.initialize(workflow, self._journal.state(session_id)),
            started.id,
        )

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
            for item in scheduler.resolutions.values()
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


__all__ = ["CapabilityResolver", "WorkflowExecutor"]

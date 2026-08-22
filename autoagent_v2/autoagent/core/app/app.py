"""Public facade and component assembly for the standalone V2 Core."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, replace
from typing import Literal, cast
from uuid import uuid4

from ..compiler import WorkflowCompiler, WorkflowDefinitionSnapshot
from ..errors import RuntimeTransitionError
from ..executor import CapabilityResolver, NodeExecutor, WorkflowExecutor
from ..operators import Operator, OperatorRegistry, Wait
from ..runtime import (
    InMemoryEventJournal,
    InMemoryUserEventJournal,
    InvocationCancelled,
    InvocationFailed,
    InvocationOpened,
    InvocationRecoveryRequested,
    InvocationStarted,
    RuntimeErrorInfo,
    RuntimeEvent,
    RuntimeState,
    SessionOpened,
    StateReducer,
    TaskRuntime,
    UserEvent,
    WaitResumed,
    WaitState,
    thaw,
)
from ..scheduler import Scheduler
from ..workflow import Capability, ChildInvocationHandle, Workflow, WorkflowIR
from .ports import (
    Clock,
    NodeExecutorPort,
    OperatorRegistryPort,
    RuntimeJournalPort,
    SchedulerPort,
    UserEventJournalPort,
)
from .runtime_loop import RuntimeLoop
from .stream import AttachedStream, InvocationStream, is_stream_end


@dataclass(frozen=True, slots=True)
class InvocationResult:
    session_id: str
    invocation_id: str
    status: Literal["running", "waiting", "completed", "failed", "cancelled"]
    output: object = None
    error: RuntimeErrorInfo | None = None
    waits: tuple[WaitState, ...] = ()
    events: tuple[RuntimeEvent, ...] = ()
    user_events: tuple[UserEvent, ...] = ()
    next_event_cursor: int = 0
    next_user_event_cursor: int = 0


StreamItem = RuntimeEvent | UserEvent | InvocationResult


class AutoAgentApp:
    """Compile, invoke, recover and observe V2 Core Workflows.

    Execution algorithms live in WorkflowExecutor and NodeExecutor. The App
    owns only registries, public lifecycle methods and component assembly.
    """

    def __init__(
        self,
        *,
        max_operator_concurrency: int = 32,
        max_node_executions_per_invocation: int = 1_000,
        capability_resolver: CapabilityResolver | None = None,
        runtime_journal: RuntimeJournalPort | None = None,
        user_event_journal: UserEventJournalPort | None = None,
        scheduler: SchedulerPort | None = None,
        node_executor: NodeExecutorPort | None = None,
        clock_ns: Clock | None = None,
        operator_registry: OperatorRegistryPort | None = None,
    ) -> None:
        if (
            not isinstance(max_operator_concurrency, int)
            or isinstance(max_operator_concurrency, bool)
            or max_operator_concurrency < 1
        ):
            raise ValueError("max_operator_concurrency must be positive.")
        if (
            not isinstance(max_node_executions_per_invocation, int)
            or isinstance(max_node_executions_per_invocation, bool)
            or max_node_executions_per_invocation < 1
        ):
            raise ValueError("max_node_executions_per_invocation must be positive.")
        self._compiler = WorkflowCompiler()
        self._workflows: dict[str, WorkflowIR] = {}
        self._workflow_definition_snapshots: dict[
            str, WorkflowDefinitionSnapshot
        ] = {}
        self._latest_workflow_revision: dict[str, str] = {}
        self._journal = runtime_journal or InMemoryEventJournal()
        self._user_event_journal = user_event_journal or InMemoryUserEventJournal()
        self._scheduler = scheduler or Scheduler()
        if (
            node_executor is not None
            and node_executor.max_operator_concurrency
            > max_operator_concurrency
        ):
            raise ValueError(
                "Injected NodeExecutor concurrency cannot exceed the App limit."
            )
        self._node_executor = node_executor or NodeExecutor(
            max_operator_concurrency=max_operator_concurrency
        )
        self._clock_ns = clock_ns or time.time_ns
        self._operator_registry = operator_registry or OperatorRegistry()
        self._task_runtime = TaskRuntime()
        self._workflow_executor = WorkflowExecutor(
            journal=self._journal,
            scheduler=self._scheduler,
            node_executor=self._node_executor,  # type: ignore[arg-type]
            task_runtime=self._task_runtime,
            operator_registry=self._operator_registry,
            emit=self._emit,
            emit_user=self._emit_user,
            max_node_executions_per_invocation=(
                max_node_executions_per_invocation
            ),
            capability_resolver=capability_resolver,
        )
        self._closed = False
        self._runtime_loop = RuntimeLoop()
        self._attached_streams: dict[str, AttachedStream] = {}
        self._attached_stream_tasks: dict[str, asyncio.Task[None]] = {}

    # ------------------------------------------------------------------
    # Definition and implementation registries

    def register_workflow(self, workflow: Workflow) -> WorkflowIR:
        self._ensure_open()
        result = self._compiler.compile(workflow)
        compiled = result.require_workflow_ir()
        self._register_ir(compiled)
        return compiled

    def workflow_definition_snapshot(
        self, workflow_id_or_revision_id: str
    ) -> WorkflowDefinitionSnapshot:
        """Return the portable snapshot for one registered Workflow revision."""

        self._ensure_open()
        revision_id = self._latest_workflow_revision.get(
            workflow_id_or_revision_id, workflow_id_or_revision_id
        )
        snapshot = self._workflow_definition_snapshots.get(revision_id)
        if snapshot is None:
            raise RuntimeTransitionError(
                "WORKFLOW_NOT_REGISTERED",
                f"Workflow {workflow_id_or_revision_id!r} is not registered.",
            )
        return snapshot

    @property
    def operator_registry(self) -> OperatorRegistryPort:
        return self._operator_registry

    def register_capability(self, capability: Capability) -> Capability:
        self._ensure_open()
        self._operator_registry.bind_capability(capability)
        return capability

    def register_operator(
        self,
        operator: Operator | Callable[..., object],
        *,
        capability_id: str,
        operator_id: str | None = None,
        priority: int = 0,
        enabled: bool = True,
        default: bool = False,
    ) -> Operator:
        self._ensure_open()
        compiled = (
            operator
            if isinstance(operator, Operator)
            else Operator(operator, id=operator_id)
        )
        return self._operator_registry.register(
            compiled,
            capability_id=capability_id,
            priority=priority,
            enabled=enabled,
            default=default,
        )

    def set_operator_enabled(self, operator_id: str, enabled: bool) -> None:
        self._ensure_open()
        self._operator_registry.set_enabled(operator_id, enabled)

    # ------------------------------------------------------------------
    # Invocation public API

    def invoke(
        self,
        workflow: Workflow | str,
        value: object,
        *,
        session_id: str | None = None,
        session_context: dict[str, object] | None = None,
        entry_node_id: str | None = None,
    ) -> InvocationResult:
        return self._run(
            self._invoke(
                workflow,
                value,
                session_id=session_id,
                session_context=session_context,
                entry_node_id=entry_node_id,
            )
        )

    async def ainvoke(
        self,
        workflow: Workflow | str,
        value: object,
        *,
        session_id: str | None = None,
        session_context: dict[str, object] | None = None,
        entry_node_id: str | None = None,
    ) -> InvocationResult:
        return await self._await(
            self._submit(
                self._invoke(
                    workflow,
                    value,
                    session_id=session_id,
                    session_context=session_context,
                    entry_node_id=entry_node_id,
                )
            )
        )

    def submit_invoke(
        self,
        workflow: Workflow | str,
        value: object,
        *,
        session_id: str | None = None,
        session_context: dict[str, object] | None = None,
        entry_node_id: str | None = None,
    ) -> InvocationResult:
        return self._run(
            self._invoke(
                workflow,
                value,
                session_id=session_id,
                session_context=session_context,
                entry_node_id=entry_node_id,
                wait_for_boundary=False,
            )
        )

    async def asubmit_invoke(
        self,
        workflow: Workflow | str,
        value: object,
        *,
        session_id: str | None = None,
        session_context: dict[str, object] | None = None,
        entry_node_id: str | None = None,
    ) -> InvocationResult:
        return await self._await(
            self._submit(
                self._invoke(
                    workflow,
                    value,
                    session_id=session_id,
                    session_context=session_context,
                    entry_node_id=entry_node_id,
                    wait_for_boundary=False,
                )
            )
        )

    def stream(
        self,
        workflow: Workflow | str,
        value: object,
        *,
        session_id: str | None = None,
        session_context: dict[str, object] | None = None,
        entry_node_id: str | None = None,
    ) -> InvocationStream[StreamItem]:
        """Run one Invocation through a strict synchronous Event stream."""

        session, channel = self._run(
            self._start_attached_stream(
                workflow,
                value,
                session_id=session_id,
                session_context=session_context,
                entry_node_id=entry_node_id,
            )
        )

        def close() -> None:
            if not self._closed:
                self._run(self._abandon_attached_stream(session, channel))

        return InvocationStream(
            receive=lambda: self._run(channel.receive()),
            close=close,
        )

    async def astream(
        self,
        workflow: Workflow | str,
        value: object,
        *,
        session_id: str | None = None,
        session_context: dict[str, object] | None = None,
        entry_node_id: str | None = None,
    ) -> AsyncIterator[StreamItem]:
        """Run one Invocation through a strict caller-driven Event stream."""

        session, channel = await self._await(
            self._submit(
                self._start_attached_stream(
                    workflow,
                    value,
                    session_id=session_id,
                    session_context=session_context,
                    entry_node_id=entry_node_id,
                )
            )
        )
        try:
            while True:
                item = await self._await(self._submit(channel.receive()))
                if is_stream_end(item):
                    return
                yield cast(StreamItem, item)
        finally:
            if not self._closed:
                await self._await(
                    self._submit(self._abandon_attached_stream(session, channel))
                )

    def resume(
        self, session_id: str, wait_id: str, response: object
    ) -> InvocationResult:
        return self._run(self._resume(session_id, wait_id, response))

    async def aresume(
        self, session_id: str, wait_id: str, response: object
    ) -> InvocationResult:
        return await self._await(
            self._submit(self._resume(session_id, wait_id, response))
        )

    def submit_resume(
        self, session_id: str, wait_id: str, response: object
    ) -> InvocationResult:
        return self._run(
            self._resume(
                session_id, wait_id, response, wait_for_boundary=False
            )
        )

    async def asubmit_resume(
        self, session_id: str, wait_id: str, response: object
    ) -> InvocationResult:
        return await self._await(
            self._submit(
                self._resume(
                    session_id, wait_id, response, wait_for_boundary=False
                )
            )
        )

    def cancel(
        self, session_id: str, reason: str | None = None
    ) -> InvocationResult:
        return self._run(self._cancel(session_id, reason))

    async def acancel(
        self, session_id: str, reason: str | None = None
    ) -> InvocationResult:
        return await self._await(self._submit(self._cancel(session_id, reason)))

    def recover(self, session_id: str) -> InvocationResult:
        return self._run(self._recover(session_id))

    async def arecover(self, session_id: str) -> InvocationResult:
        return await self._await(self._submit(self._recover(session_id)))

    def recover_events(
        self, workflow: Workflow | str, events: tuple[RuntimeEvent, ...]
    ) -> InvocationResult:
        return self._run(self._recover_events(workflow, events))

    def wait(
        self,
        session_id: str,
        timeout: float | None = None,
        *,
        event_cursor: int = 0,
        user_event_cursor: int = 0,
    ) -> InvocationResult:
        return self._run(
            self._wait(
                session_id,
                timeout,
                event_cursor=event_cursor,
                user_event_cursor=user_event_cursor,
            )
        )

    async def await_result(
        self,
        session_id: str,
        timeout: float | None = None,
        *,
        event_cursor: int = 0,
        user_event_cursor: int = 0,
    ) -> InvocationResult:
        return await self._await(
            self._submit(
                self._wait(
                    session_id,
                    timeout,
                    event_cursor=event_cursor,
                    user_event_cursor=user_event_cursor,
                )
            )
        )

    # ------------------------------------------------------------------
    # Observation and child API

    def child_status(self, handle: ChildInvocationHandle) -> InvocationResult:
        return self._run(self._child_status(handle))

    async def achild_status(
        self, handle: ChildInvocationHandle
    ) -> InvocationResult:
        return await self._await(self._submit(self._child_status(handle)))

    def child_handles(
        self, parent_invocation_id: str
    ) -> tuple[ChildInvocationHandle, ...]:
        return self._run(self._list_child_handles(parent_invocation_id))

    async def achild_handles(
        self, parent_invocation_id: str
    ) -> tuple[ChildInvocationHandle, ...]:
        return await self._await(
            self._submit(self._list_child_handles(parent_invocation_id))
        )

    def wait_child(
        self, handle: ChildInvocationHandle, timeout: float | None = None
    ) -> InvocationResult:
        return self._run(self._wait_child(handle, timeout))

    async def await_child(
        self, handle: ChildInvocationHandle, timeout: float | None = None
    ) -> InvocationResult:
        return await self._await(self._submit(self._wait_child(handle, timeout)))

    def cancel_child(
        self, handle: ChildInvocationHandle, reason: str | None = None
    ) -> InvocationResult:
        return self._run(self._cancel_child(handle, reason))

    async def acancel_child(
        self, handle: ChildInvocationHandle, reason: str | None = None
    ) -> InvocationResult:
        return await self._await(
            self._submit(self._cancel_child(handle, reason))
        )

    # ------------------------------------------------------------------
    # Lifecycle

    def close(self) -> None:
        if self._closed:
            return
        self._run(self._close())
        self._runtime_loop.close()
        self._node_executor.close()
        self._closed = True

    async def aclose(self) -> None:
        if self._closed:
            return
        await self._await(self._submit(self._close()))
        self._runtime_loop.close()
        self._node_executor.close()
        self._closed = True

    # ------------------------------------------------------------------
    # Runtime-loop operations

    async def _start_attached_stream(
        self,
        workflow: Workflow | str,
        value: object,
        *,
        session_id: str | None,
        session_context: dict[str, object] | None,
        entry_node_id: str | None,
    ) -> tuple[str, AttachedStream]:
        session = session_id or str(uuid4())
        if session in self._attached_streams:
            raise RuntimeTransitionError(
                "INVOCATION_STREAM_ATTACHED",
                "Session already has an attached Invocation stream.",
            )
        channel = AttachedStream()
        self._attached_streams[session] = channel

        async def run() -> None:
            error: BaseException | None = None
            try:
                result = await self._invoke(
                    workflow,
                    value,
                    session_id=session,
                    session_context=session_context,
                    entry_node_id=entry_node_id,
                )
                await channel.publish(
                    replace(result, events=(), user_events=())
                )
            except asyncio.CancelledError:
                raise
            except BaseException as caught:
                error = caught
            finally:
                await channel.finish(error)
                if self._attached_streams.get(session) is channel:
                    self._attached_streams.pop(session, None)
                self._attached_stream_tasks.pop(session, None)

        task = asyncio.create_task(run())
        self._attached_stream_tasks[session] = task
        return session, channel

    async def _abandon_attached_stream(
        self, session_id: str, channel: AttachedStream
    ) -> None:
        if self._attached_streams.get(session_id) is channel:
            self._attached_streams.pop(session_id, None)
        channel.abandon()
        task = self._attached_stream_tasks.get(session_id)
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def _invoke(
        self,
        workflow: Workflow | str,
        value: object,
        *,
        session_id: str | None,
        session_context: dict[str, object] | None,
        entry_node_id: str | None,
        wait_for_boundary: bool = True,
    ) -> InvocationResult:
        compiled = self._resolve_workflow(workflow)
        session = session_id or str(uuid4())
        state = self._journal.state(session)
        event_cursor = state.sequence
        if state.session is None:
            await self._emit(
                session,
                None,
                SessionOpened(compiled.workflow_id, session_context or {}),
            )
        elif state.session.workflow_id != compiled.workflow_id:
            raise RuntimeTransitionError(
                "SESSION_WORKFLOW_MISMATCH",
                "Session belongs to another Workflow.",
            )
        elif session_context is not None:
            raise RuntimeTransitionError(
                "SESSION_CONTEXT_ALREADY_OPEN",
                "session_context can only be provided when opening a new Session.",
            )
        elif state.invocation is not None and not state.invocation.terminal:
            raise RuntimeTransitionError(
                "SESSION_INVOCATION_ACTIVE",
                "Session already has an active Invocation.",
            )
        invocation_id = str(uuid4())
        entry = entry_node_id or _single_entry(compiled)
        opened = await self._emit(
            session,
            invocation_id,
            InvocationOpened(compiled.workflow_revision_id, entry, value),
        )
        started = await self._emit(
            session, invocation_id, InvocationStarted(), opened.id
        )
        await self._emit(
            session,
            invocation_id,
            self._scheduler.initialize(compiled, self._journal.state(session)),
            started.id,
        )
        task = self._start_drive(compiled, session, invocation_id)
        if wait_for_boundary:
            try:
                await task
            except asyncio.CancelledError:
                current = self._journal.state(session).invocation
                if current is not None and not current.terminal:
                    await self._emit(
                        session,
                        invocation_id,
                        InvocationCancelled("Invocation caller cancelled."),
                    )
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                raise
        return self._result(session, event_cursor, 0)

    async def _resume(
        self,
        session_id: str,
        wait_id: str,
        response: object,
        *,
        wait_for_boundary: bool = True,
    ) -> InvocationResult:
        state = self._journal.state(session_id)
        invocation = _active_invocation(state)
        event_cursor = state.sequence
        user_cursor = self._last_user_sequence(invocation.id)
        wait_state = invocation.scheduler.waits.get(wait_id)
        if wait_state is None or wait_state.status != "waiting":
            raise RuntimeTransitionError(
                "WAIT_NOT_WAITING", f"Wait {wait_id!r} is not waiting."
            )
        workflow = self._workflow_for_state(state)
        occurrence = invocation.scheduler.occurrences[wait_state.occurrence_id]
        node = workflow.node(occurrence.node_id)
        if not isinstance(node.executable, Wait):
            raise RuntimeTransitionError(
                "WAIT_NODE_INVALID", "Wait belongs to a non-Wait Node."
            )
        validated = node.executable.output_contract.validate(response)
        await self._emit(
            session_id,
            invocation.id,
            WaitResumed(wait_id, validated),
        )
        task = self._task_runtime.task(session_id)
        if task is None:
            task = self._start_drive(workflow, session_id, invocation.id)
        else:
            self._task_runtime.wake(session_id)
        if wait_for_boundary:
            await task
        return self._result(session_id, event_cursor, user_cursor)

    async def _cancel(
        self, session_id: str, reason: str | None
    ) -> InvocationResult:
        state = self._journal.state(session_id)
        invocation = _active_invocation(state)
        event_cursor = state.sequence
        user_cursor = self._last_user_sequence(invocation.id)
        await self._emit(
            session_id, invocation.id, InvocationCancelled(reason)
        )
        self._task_runtime.signal_update(invocation.id)
        task = self._task_runtime.task(session_id)
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        return self._result(session_id, event_cursor, user_cursor)

    async def _recover(self, session_id: str) -> InvocationResult:
        state = self._journal.state(session_id)
        invocation = _active_invocation(state)
        workflow = self._workflow_for_state(state)
        if self._task_runtime.is_live(session_id):
            raise RuntimeTransitionError(
                "INVOCATION_STILL_LIVE",
                "Recovery cannot run while this App owns the Invocation task.",
            )
        event_cursor = state.sequence
        user_cursor = self._last_user_sequence(invocation.id)
        recovery_error = self._recovery_error(workflow, state)
        if recovery_error is not None:
            await self._emit(
                session_id, invocation.id, InvocationFailed(recovery_error)
            )
            return self._result(session_id, event_cursor, user_cursor)
        await self._emit(
            session_id, invocation.id, InvocationRecoveryRequested()
        )
        task = self._start_drive(workflow, session_id, invocation.id)
        await task
        return self._result(session_id, event_cursor, user_cursor)

    async def _recover_events(
        self, workflow: Workflow | str, events: tuple[RuntimeEvent, ...]
    ) -> InvocationResult:
        if not events:
            raise ValueError("Recovery requires at least one Runtime Event.")
        if any(event.from_state_version is None for event in events):
            raise RuntimeTransitionError(
                "RECOVERY_EVENT_UNSEALED",
                "Recovery requires persisted Runtime Events with State Operation Batches.",
            )
        session_id = events[0].session_id
        if self._journal.state(session_id).session is not None:
            raise RuntimeTransitionError(
                "RECOVERY_SESSION_EXISTS",
                "Recovery target Session already exists.",
            )
        state = StateReducer().reduce(events)
        invocation = state.invocation
        if invocation is None:
            raise RuntimeTransitionError(
                "RECOVERY_INVOCATION_MISSING",
                "Runtime Event prefix has no Invocation.",
            )
        compiled = self._resolve_workflow(
            workflow, required_revision_id=invocation.workflow_revision_id
        )
        if state.session is None or state.session.workflow_id != compiled.workflow_id:
            raise RuntimeTransitionError(
                "RECOVERY_WORKFLOW_MISMATCH",
                "Runtime Events belong to another Workflow.",
            )
        if invocation.workflow_revision_id != compiled.workflow_revision_id:
            raise RuntimeTransitionError(
                "RECOVERY_REVISION_MISMATCH",
                "Runtime Events use another Workflow Revision.",
            )
        self._journal.append_many(events)
        event_cursor = state.sequence
        user_cursor = self._last_user_sequence(invocation.id)
        if invocation.terminal:
            return self._result(session_id, 0, 0)
        if invocation.status == "created":
            await self._emit(session_id, invocation.id, InvocationStarted())
            state = self._journal.state(session_id)
            invocation = _active_invocation(state)
        if not invocation.scheduler.initialized:
            await self._emit(
                session_id,
                invocation.id,
                self._scheduler.initialize(
                    compiled, self._journal.state(session_id)
                ),
            )
        else:
            recovery_error = self._recovery_error(
                compiled, self._journal.state(session_id)
            )
            if recovery_error is not None:
                await self._emit(
                    session_id,
                    invocation.id,
                    InvocationFailed(recovery_error),
                )
                return self._result(session_id, event_cursor, user_cursor)
            await self._emit(
                session_id, invocation.id, InvocationRecoveryRequested()
            )
        task = self._start_drive(
            compiled, session_id, invocation.id
        )
        await task
        return self._result(session_id, event_cursor, user_cursor)

    @staticmethod
    def _recovery_error(
        workflow: WorkflowIR, state: RuntimeState
    ) -> RuntimeErrorInfo | None:
        invocation = state.invocation
        if invocation is None:
            return RuntimeErrorInfo(
                "RecoveryStateInvalid", "Invocation is missing."
            )
        for occurrence in invocation.scheduler.occurrences.values():
            if occurrence.status != "running":
                continue
            recovery = workflow.node(occurrence.node_id).recovery_mode
            if recovery.mode == "never":
                return RuntimeErrorInfo(
                    "RecoveryNotAllowed",
                    f"Node {occurrence.node_id!r} does not permit crash replay.",
                )
            if occurrence.recovery_attempts >= recovery.max_attempts:
                return RuntimeErrorInfo(
                    "RecoveryAttemptsExceeded",
                    f"Node {occurrence.node_id!r} exhausted crash recovery attempts.",
                )
        return None

    async def _wait(
        self,
        session_id: str,
        timeout: float | None,
        *,
        event_cursor: int,
        user_event_cursor: int,
    ) -> InvocationResult:
        state = self._journal.state(session_id)
        if state.invocation is None:
            raise RuntimeTransitionError(
                "INVOCATION_UNKNOWN", "Invocation does not exist."
            )
        task = self._task_runtime.task(session_id)
        if task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout)
            except TimeoutError as error:
                raise TimeoutError(
                    "Invocation did not reach a boundary in time."
                ) from error
        return self._result(
            session_id, event_cursor, user_event_cursor
        )

    async def _list_child_handles(
        self, parent_invocation_id: str
    ) -> tuple[ChildInvocationHandle, ...]:
        return tuple(
            cast(
                ChildInvocationHandle,
                {
                    "session_id": item.session_id,
                    "invocation_id": item.invocation_id,
                    "workflow_id": item.workflow_id,
                    "workflow_revision_id": item.workflow_revision_id,
                },
            )
            for item in self._journal.child_links(parent_invocation_id)
        )

    async def _child_status(
        self, handle: ChildInvocationHandle
    ) -> InvocationResult:
        session_id, _invocation_id = self._validate_child_handle(handle)
        return self._result(session_id, 0, 0)

    async def _wait_child(
        self, handle: ChildInvocationHandle, timeout: float | None
    ) -> InvocationResult:
        session_id, _invocation_id = self._validate_child_handle(handle)
        task = self._task_runtime.task(session_id)
        if task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout)
            except TimeoutError as error:
                raise TimeoutError(
                    "Child Invocation did not reach a boundary in time."
                ) from error
        return self._result(session_id, 0, 0)

    async def _cancel_child(
        self, handle: ChildInvocationHandle, reason: str | None
    ) -> InvocationResult:
        session_id, _invocation_id = self._validate_child_handle(handle)
        return await self._cancel(session_id, reason)

    async def _close(self) -> None:
        for channel in tuple(self._attached_streams.values()):
            channel.abandon()
        self._attached_streams.clear()
        for session_id in self._task_runtime.active_sessions():
            state = self._journal.state(session_id)
            invocation = state.invocation
            if invocation is not None and not invocation.terminal:
                await self._emit(
                    session_id,
                    invocation.id,
                    InvocationCancelled("AutoAgentApp closed."),
                )
        await self._task_runtime.cancel_all()

    # ------------------------------------------------------------------
    # Small facade helpers

    def _start_drive(
        self,
        workflow: WorkflowIR,
        session_id: str,
        invocation_id: str,
    ) -> asyncio.Task[None]:
        task = asyncio.create_task(
            self._workflow_executor.drive(workflow, session_id)
        )
        self._task_runtime.track(session_id, invocation_id, task)
        return task

    async def _emit(
        self,
        session_id: str,
        invocation_id: str | None,
        payload: object,
        causation_id: str | None = None,
    ) -> RuntimeEvent:
        channel = self._attached_streams.get(session_id)

        def commit() -> RuntimeEvent:
            state = self._journal.state(session_id)
            previous_sequence = state.sequence
            event = RuntimeEvent(
                session_id=session_id,
                invocation_id=invocation_id,
                sequence=state.sequence + 1,
                occurred_at_ns=self._clock_ns(),
                payload=payload,  # type: ignore[arg-type]
                causation_id=(
                    causation_id
                    if causation_id is not None
                    else state.last_event_id
                ),
            )
            self._journal.append(event)
            if channel is None:
                return event
            self._journal.flush(session_id)
            return next(
                item
                for item in reversed(self._journal.events(session_id))
                if item.sequence > previous_sequence
            )

        if channel is None:
            return commit()
        return cast(RuntimeEvent, await channel.publish_created(commit))

    async def _emit_user(
        self,
        session_id: str,
        invocation_id: str,
        kind: str,
        payload: object,
        occurrence_id: str | None = None,
    ) -> UserEvent:
        def commit() -> UserEvent:
            return self._user_event_journal.emit(
                session_id=session_id,
                invocation_id=invocation_id,
                kind=kind,
                payload=payload,
                occurrence_id=occurrence_id,
                occurred_at_ns=self._clock_ns(),
            )

        channel = self._attached_streams.get(session_id)
        if channel is None:
            return commit()
        return cast(UserEvent, await channel.publish_created(commit))

    def _result(
        self,
        session_id: str,
        event_cursor: int,
        user_event_cursor: int,
    ) -> InvocationResult:
        self._journal.flush(session_id)
        state = self._journal.state(session_id)
        invocation = state.invocation
        assert invocation is not None
        all_user_events = self._user_event_journal.events(invocation.id)
        latest_user_sequence = (
            all_user_events[-1].sequence if all_user_events else 0
        )
        _validate_cursor("event_cursor", event_cursor, state.sequence)
        _validate_cursor(
            "user_event_cursor", user_event_cursor, latest_user_sequence
        )
        runtime_events = tuple(
            event
            for event in self._journal.events(session_id)
            if event.sequence > event_cursor
        )
        user_events = tuple(
            event
            for event in all_user_events
            if event.sequence > user_event_cursor
        )
        return InvocationResult(
            session_id=session_id,
            invocation_id=invocation.id,
            status=invocation.status,
            output=thaw(invocation.output),
            error=invocation.error,
            waits=tuple(invocation.scheduler.waits.values()),
            events=runtime_events,
            user_events=user_events,
            next_event_cursor=state.sequence,
            next_user_event_cursor=latest_user_sequence,
        )

    def _last_user_sequence(self, invocation_id: str) -> int:
        events = self._user_event_journal.events(invocation_id)
        return events[-1].sequence if events else 0

    def _validate_child_handle(
        self, handle: ChildInvocationHandle
    ) -> tuple[str, str]:
        if not isinstance(handle, Mapping):
            raise TypeError("Child Invocation Handle must be a mapping.")
        required = {
            "session_id",
            "invocation_id",
            "workflow_id",
            "workflow_revision_id",
        }
        if set(handle) != required or any(
            not isinstance(handle.get(key), str) or not handle[key]
            for key in required
        ):
            raise RuntimeTransitionError(
                "CHILD_HANDLE_INVALID",
                "Child Invocation Handle is incomplete.",
            )
        invocation_id = handle["invocation_id"]
        link = self._journal.child_link(invocation_id)
        if link is None or {
            "session_id": link.session_id,
            "invocation_id": link.invocation_id,
            "workflow_id": link.workflow_id,
            "workflow_revision_id": link.workflow_revision_id,
        } != dict(handle):
            raise RuntimeTransitionError(
                "CHILD_HANDLE_UNKNOWN",
                "Child Invocation Handle is not present in Runtime Event history.",
            )
        state = self._journal.state(link.session_id)
        if (
            state.session is None
            or state.invocation is None
            or state.session.workflow_id != link.workflow_id
            or state.invocation.id != invocation_id
            or state.invocation.workflow_revision_id
            != link.workflow_revision_id
        ):
            raise RuntimeTransitionError(
                "CHILD_HANDLE_UNKNOWN",
                "Child Invocation Handle does not identify current child state.",
            )
        return link.session_id, invocation_id

    def _register_ir(self, workflow: WorkflowIR) -> None:
        existing = self._workflows.get(workflow.workflow_revision_id)
        if (
            existing is not None
            and existing.definition_hash != workflow.definition_hash
        ):
            raise RuntimeTransitionError(
                "WORKFLOW_REVISION_CONFLICT",
                f"Workflow Revision {workflow.workflow_revision_id!r} was reused.",
            )
        self._workflows[workflow.workflow_revision_id] = workflow
        self._workflow_definition_snapshots[workflow.workflow_revision_id] = (
            WorkflowDefinitionSnapshot.from_workflow_ir(workflow)
        )
        self._latest_workflow_revision[workflow.workflow_id] = (
            workflow.workflow_revision_id
        )
        for node in workflow.nodes:
            if isinstance(node.executable, WorkflowIR):
                self._register_ir(node.executable)
            elif isinstance(node.executable, Capability):
                self._operator_registry.bind_capability(node.executable)

    def _workflow_for_state(self, state: RuntimeState) -> WorkflowIR:
        if state.session is None:
            raise RuntimeTransitionError(
                "SESSION_UNKNOWN", "Session does not exist."
            )
        invocation = state.invocation
        revision_id = (
            invocation.workflow_revision_id
            if invocation is not None
            else self._latest_workflow_revision.get(state.session.workflow_id)
        )
        workflow = self._workflows.get(revision_id or "")
        if workflow is None:
            raise RuntimeTransitionError(
                "WORKFLOW_NOT_REGISTERED", "Workflow is not registered."
            )
        return workflow

    def _resolve_workflow(
        self,
        value: Workflow | str,
        *,
        required_revision_id: str | None = None,
    ) -> WorkflowIR:
        if isinstance(value, Workflow):
            workflow = self.register_workflow(value)
            if (
                required_revision_id is not None
                and workflow.workflow_revision_id != required_revision_id
            ):
                raise RuntimeTransitionError(
                    "RECOVERY_REVISION_MISMATCH",
                    "Workflow object does not match the required Revision.",
                )
            return workflow
        if required_revision_id is not None:
            exact = self._workflows.get(required_revision_id)
            if exact is not None and value in {
                exact.workflow_id,
                exact.workflow_revision_id,
            }:
                return exact
        workflow = self._workflows.get(value)
        if workflow is None:
            revision_id = self._latest_workflow_revision.get(value)
            workflow = self._workflows.get(revision_id or "")
        if workflow is None:
            raise RuntimeTransitionError(
                "WORKFLOW_NOT_REGISTERED",
                f"Workflow {value!r} is not registered.",
            )
        return workflow

    def _submit(self, coroutine):
        self._ensure_open()
        return self._runtime_loop.submit(coroutine)

    async def _await(self, future):
        return await self._runtime_loop.wait(future)

    def _run(self, coroutine):
        self._ensure_open()
        return self._runtime_loop.run(coroutine)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("AutoAgentApp is closed.")


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


def _validate_cursor(name: str, value: int, maximum: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer.")
    if value > maximum:
        raise ValueError(f"{name} cannot be ahead of the current Event stream.")


__all__ = [
    "AutoAgentApp",
    "CapabilityResolver",
    "InvocationResult",
    "InvocationStream",
    "StreamItem",
]

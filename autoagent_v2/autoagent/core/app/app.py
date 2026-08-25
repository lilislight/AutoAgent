"""Public facade and process-local assembly for the standalone V2 Core."""

from __future__ import annotations

import asyncio
import math
import threading
import time
from collections.abc import AsyncIterator, Callable, Mapping
from concurrent.futures import CancelledError as FutureCancelledError, Future
from dataclasses import replace
from typing import cast
from uuid import uuid4

from ..compiler import WorkflowCompiler, WorkflowDefinitionSnapshot
from ..errors import RuntimeInfrastructureError, RuntimeTransitionError
from ..executor import CapabilityResolver, NodeExecutor, WorkflowExecutor
from ..hosting import RuntimeEventSink
from ..operators import Operator, OperatorRegistry, Wait
from ..runtime import (
    ChildAwaitReady,
    ChildAwaitSuspended,
    ChildInvocationPhaseChanged,
    ChildInvocationPlanned,
    InMemoryEventJournal,
    InMemoryUserEventJournal,
    InvocationCancelled,
    InvocationCompleted,
    InvocationFailed,
    InvocationOpened,
    InvocationRecoveryRequested,
    InvocationStarted,
    InvocationWaiting,
    NodeOccurrenceCompleted,
    NodeOccurrenceFailed,
    NodeOccurrenceStarted,
    NodeOccurrenceWaiting,
    RuntimeCheckpointBundle,
    RuntimeErrorInfo,
    RuntimeEvent,
    RuntimeState,
    SchedulerInitialized,
    SessionOpened,
    StateTransition,
    TaskRuntime,
    TraceEvent,
    UserEvent,
    WaitResumed,
    project_trace_event,
    thaw,
)
from ..scheduler import Scheduler
from ..workflow import Capability, ChildInvocationHandle, Workflow, WorkflowIR
from .models import (
    AppCheckpoint,
    CheckpointLoadResult,
    InvocationRef,
    InvocationResult,
    InvocationStatus,
    InvocationSubmission,
    InvocationUpdate,
    InvocationWait,
    StreamItem,
    _validate_graph_claims,
)
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


_CHECKPOINT_PAYLOADS = (
    SchedulerInitialized,
    # Node start is the write-ahead recovery boundary for every user hook and
    # Operator side effect that follows it.  Per-unit Operator Call events do
    # not create additional checkpoints because Map is one atomic occurrence:
    # recovery replays the complete Node according to recovery_mode.
    NodeOccurrenceStarted,
    NodeOccurrenceWaiting,
    WaitResumed,
    InvocationRecoveryRequested,
    ChildInvocationPlanned,
    ChildInvocationPhaseChanged,
    ChildAwaitSuspended,
    ChildAwaitReady,
    NodeOccurrenceCompleted,
    NodeOccurrenceFailed,
    InvocationWaiting,
    InvocationCompleted,
    InvocationFailed,
    InvocationCancelled,
)


class AutoAgentApp:
    """Compile and execute Workflows while retaining only current Runtime State."""

    def __init__(
        self,
        *,
        max_operator_concurrency: int = 32,
        max_node_executions_per_invocation: int = 1_000,
        capability_resolver: CapabilityResolver | None = None,
        runtime_journal: RuntimeJournalPort | None = None,
        runtime_event_sink: RuntimeEventSink | None = None,
        user_event_journal: UserEventJournalPort | None = None,
        scheduler: SchedulerPort | None = None,
        node_executor: NodeExecutorPort | None = None,
        clock_ns: Clock | None = None,
        operator_registry: OperatorRegistryPort | None = None,
    ) -> None:
        _positive_integer(max_operator_concurrency, "max_operator_concurrency")
        _positive_integer(
            max_node_executions_per_invocation,
            "max_node_executions_per_invocation",
        )
        self._compiler = WorkflowCompiler()
        self._workflows: dict[str, WorkflowIR] = {}
        self._workflow_definition_snapshots: dict[
            str, WorkflowDefinitionSnapshot
        ] = {}
        self._latest_workflow_revision: dict[str, str] = {}
        self._journal = runtime_journal or InMemoryEventJournal()
        self._runtime_event_sink = runtime_event_sink
        self._user_event_journal = user_event_journal or InMemoryUserEventJournal()
        self._scheduler = scheduler or Scheduler()
        if (
            node_executor is not None
            and node_executor.max_operator_concurrency > max_operator_concurrency
        ):
            raise ValueError(
                "Injected NodeExecutor concurrency cannot exceed the App limit."
            )
        self._owns_node_executor = node_executor is None
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
            start_child=self._start_drive,
            begin_child_admission=self._begin_child_admission,
            abort_child_admission=self._abort_child_admission,
            ensure_child_durable=self._ensure_child_durable,
            max_node_executions_per_invocation=max_node_executions_per_invocation,
            capability_resolver=capability_resolver,
        )
        self._runtime_loop = RuntimeLoop()
        self._attached_streams: dict[str, AttachedStream] = {}
        self._attached_stream_tasks: dict[str, asyncio.Task[None]] = {}
        self._observations: dict[str, list[TraceEvent | UserEvent]] = {}
        self._trace_sequences: dict[str, int] = {}
        self._last_transition_ids: dict[str, str] = {}
        self._runtime_locks: dict[str, asyncio.Lock] = {}
        self._child_capacities: dict[tuple[str, str], asyncio.Semaphore] = {}
        # Durable ownership remains in parent Runtime State.  This transient
        # reverse index makes the hot Child -> Root lookup proportional to
        # graph depth instead of rescanning every plan in the App.
        self._child_owners: dict[str, tuple[str, str, int, str]] = {}
        self._rebuild_child_owners()
        # A Session may replace a terminal Invocation only after every public
        # call that is delivering that Invocation's stable boundary has
        # returned.  Without this transient lease, a concurrent new invoke can
        # replace the State between InvocationCompleted and _result().
        self._result_leases: dict[str, int] = {}
        self._close_lock = threading.Lock()
        self._close_future: Future[AppCheckpoint] | None = None
        self._closing = False
        self._closed = False
        self._closed_checkpoint: AppCheckpoint | None = None

    # ------------------------------------------------------------------
    # Definition and implementation registries

    def register_workflow(self, workflow: Workflow) -> WorkflowIR:
        with self._close_lock:
            self._ensure_open()
            return self._compile_and_register(workflow)

    def _compile_and_register(self, workflow: Workflow) -> WorkflowIR:
        compiled = self._compiler.compile(workflow).require_workflow_ir()
        self._register_ir(compiled)
        return compiled

    def workflow_definition_snapshot(
        self, workflow_id_or_revision_id: str
    ) -> WorkflowDefinitionSnapshot:
        with self._close_lock:
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
        with self._close_lock:
            self._ensure_open()
            self._operator_registry.bind_capabilities((capability,))
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
        with self._close_lock:
            self._ensure_open()
            compiled = operator if isinstance(operator, Operator) else Operator(
                operator, id=operator_id
            )
            return self._operator_registry.register(
                compiled,
                capability_id=capability_id,
                priority=priority,
                enabled=enabled,
                default=default,
            )

    def set_operator_enabled(self, operator_id: str, enabled: bool) -> None:
        with self._close_lock:
            self._ensure_open()
            self._operator_registry.set_enabled(operator_id, enabled)

    # ------------------------------------------------------------------
    # Invocation API

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
                wait_for_boundary=True,
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
                    wait_for_boundary=True,
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
    ) -> InvocationSubmission:
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
    ) -> InvocationSubmission:
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
            self._close_attached_stream(session, channel)

        def receive() -> object:
            if self._closed or self._closing:
                raise StopIteration
            try:
                return self._run(channel.receive())
            except RuntimeError:
                if self._closed or self._closing:
                    raise StopIteration from None
                raise

        return InvocationStream(receive=receive, close=close)

    async def astream(
        self,
        workflow: Workflow | str,
        value: object,
        *,
        session_id: str | None = None,
        session_context: dict[str, object] | None = None,
        entry_node_id: str | None = None,
    ) -> AsyncIterator[StreamItem]:
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
                try:
                    item = await self._await(self._submit(channel.receive()))
                except RuntimeError:
                    if self._closed or self._closing:
                        return
                    raise
                if is_stream_end(item):
                    return
                yield cast(StreamItem, item)
        finally:
            await self._aclose_attached_stream(session, channel)

    def wait(self, ref: InvocationRef, timeout: float | None = None) -> InvocationResult:
        return self._run(self._wait(ref, timeout))

    async def await_result(
        self, ref: InvocationRef, timeout: float | None = None
    ) -> InvocationResult:
        return await self._await(self._submit(self._wait(ref, timeout)))

    def resume(
        self, ref: InvocationRef, wait_id: str, response: object
    ) -> InvocationResult:
        return self._run(
            self._resume(ref, wait_id, response, wait_for_boundary=True)
        )

    async def aresume(
        self, ref: InvocationRef, wait_id: str, response: object
    ) -> InvocationResult:
        return await self._await(
            self._submit(
                self._resume(ref, wait_id, response, wait_for_boundary=True)
            )
        )

    def submit_resume(
        self, ref: InvocationRef, wait_id: str, response: object
    ) -> InvocationSubmission:
        return self._run(
            self._resume(ref, wait_id, response, wait_for_boundary=False)
        )

    async def asubmit_resume(
        self, ref: InvocationRef, wait_id: str, response: object
    ) -> InvocationSubmission:
        return await self._await(
            self._submit(
                self._resume(ref, wait_id, response, wait_for_boundary=False)
            )
        )

    def cancel(
        self, ref: InvocationRef, reason: str | None = None
    ) -> InvocationResult:
        return self._run(self._cancel(ref, reason))

    async def acancel(
        self, ref: InvocationRef, reason: str | None = None
    ) -> InvocationResult:
        return await self._await(self._submit(self._cancel(ref, reason)))

    def recover(self, ref: InvocationRef) -> InvocationResult:
        return self._run(self._recover(ref))

    async def arecover(self, ref: InvocationRef) -> InvocationResult:
        return await self._await(self._submit(self._recover(ref)))

    # ------------------------------------------------------------------
    # Checkpoint loading and Child observation

    def load_checkpoint(
        self, checkpoint: RuntimeCheckpointBundle | AppCheckpoint
    ) -> CheckpointLoadResult:
        return self._run(self._load_checkpoint(checkpoint))

    async def aload_checkpoint(
        self, checkpoint: RuntimeCheckpointBundle | AppCheckpoint
    ) -> CheckpointLoadResult:
        return await self._await(self._submit(self._load_checkpoint(checkpoint)))

    def child_handles(
        self, parent: InvocationRef
    ) -> tuple[ChildInvocationHandle, ...]:
        return self._run(self._list_child_handles(parent))

    async def achild_handles(
        self, parent: InvocationRef
    ) -> tuple[ChildInvocationHandle, ...]:
        return await self._await(self._submit(self._list_child_handles(parent)))

    def child_status(self, handle: ChildInvocationHandle) -> InvocationResult:
        return self._run(self._child_status(handle))

    async def achild_status(
        self, handle: ChildInvocationHandle
    ) -> InvocationResult:
        return await self._await(self._submit(self._child_status(handle)))

    def wait_child(
        self, handle: ChildInvocationHandle, timeout: float | None = None
    ) -> InvocationResult:
        return self._run(self._wait_child(handle, timeout))

    async def await_child(
        self, handle: ChildInvocationHandle, timeout: float | None = None
    ) -> InvocationResult:
        return await self._await(
            self._submit(self._wait_child(handle, timeout))
        )

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

    def close(self, timeout: float | None = 30.0) -> AppCheckpoint:
        _close_timeout(timeout)
        # A timeout detaches this caller; it never cancels the shared close
        # operation after quiescence may already have changed transient state.
        return self._begin_close().result(timeout=timeout)

    async def aclose(self, timeout: float | None = 30.0) -> AppCheckpoint:
        _close_timeout(timeout)
        # Closing belongs to the App, not to any one caller.  Shielding the
        # cross-thread waiter lets a cancelled caller detach without aborting
        # the one shared close operation used by every concurrent caller.
        waiter = asyncio.shield(
            self._runtime_loop.wait(self._begin_close())
        )
        return (
            await waiter
            if timeout is None
            else await asyncio.wait_for(waiter, timeout)
        )

    def _begin_close(self) -> Future[AppCheckpoint]:
        """Start exactly one process-local close operation and share its result."""

        with self._close_lock:
            if self._closed_checkpoint is not None:
                completed: Future[AppCheckpoint] = Future()
                completed.set_result(self._closed_checkpoint)
                return completed
            if self._closed:
                completed = Future()
                completed.set_result(AppCheckpoint())
                return completed
            if self._close_future is not None:
                return self._close_future
            future: Future[AppCheckpoint] = Future()
            self._close_future = future
            self._closing = True
            threading.Thread(
                target=self._complete_close,
                args=(future,),
                name="autoagent-close",
                daemon=True,
            ).start()
            return future

    def _complete_close(
        self,
        future: Future[AppCheckpoint],
    ) -> None:
        """Coordinate Runtime-loop quiescence outside every caller loop."""

        try:
            # A preloaded RuntimeJournal is valid even when this App never
            # starts an Invocation.  Always enter the RuntimeLoop so close
            # captures that retained root/child graph instead of returning an
            # empty checkpoint merely because the loop is still lazy.
            checkpoint = self._runtime_loop.run(self._close())
            self._runtime_loop.close()
            if self._owns_node_executor:
                self._node_executor.close()
        except BaseException as error:
            with self._close_lock:
                self._closing = False
                if self._close_future is future:
                    self._close_future = None
            future.set_exception(error)
            return
        with self._close_lock:
            self._closed = True
            self._closing = False
            self._closed_checkpoint = checkpoint
        future.set_result(checkpoint)

    # ------------------------------------------------------------------
    # Runtime operations

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
                    wait_for_boundary=True,
                    attached_channel=channel,
                )
                await channel.publish_terminal(result)
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
        wait_for_boundary: bool,
        attached_channel: AttachedStream | None = None,
    ) -> InvocationResult | InvocationSubmission:
        compiled = self._resolve_workflow(workflow)
        session_id = session_id or str(uuid4())
        current_channel = self._attached_streams.get(session_id)
        if current_channel is not None and current_channel is not attached_channel:
            raise RuntimeTransitionError(
                "INVOCATION_STREAM_ATTACHED",
                "Session already has an attached Invocation stream.",
            )
        if self._parent_plan(session_id) is not None:
            raise RuntimeTransitionError(
                "SESSION_OWNED_BY_CHILD",
                "A Child Session can only be controlled through its parent Handle.",
            )
        entry = entry_node_id or _single_entry(compiled)
        if entry not in compiled.entry_node_ids:
            raise RuntimeTransitionError(
                "INVOCATION_ENTRY_INVALID",
                f"Node {entry!r} is not an Entry of Workflow {compiled.workflow_id!r}.",
            )
        entry_node = compiled.node(entry)
        if entry_node.input_mapping is None and entry_node.input_contract is not None:
            # Public values are strict Python-domain objects.  Runtime State
            # owns only their canonical durable representation; execution
            # restores the domain value before calling the Operator.
            value = entry_node.input_contract.to_record(value)
        state = self._journal.state(session_id)
        old_child_sessions: tuple[str, ...] = ()
        retired_invocation_ids: tuple[str, ...] = ()
        if state.session is None:
            pass
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
        elif state.invocation is not None:
            old_child_sessions = self._descendant_sessions(session_id)
            active_children = [
                child_id
                for child_id in old_child_sessions
                if (
                    self._journal.state(child_id).invocation is not None
                    and not self._journal.state(child_id).invocation.terminal
                )
            ]
            if active_children:
                raise RuntimeTransitionError(
                    "SESSION_CHILDREN_ACTIVE",
                    "A Session cannot replace its Invocation while spawned Children are active.",
                )
            retired_invocation_ids = (
                state.invocation.id,
                *(
                    child.id
                    for child_session_id in old_child_sessions
                    if (
                        child := self._journal.state(child_session_id).invocation
                    ) is not None
                ),
            )

        if self._result_leases.get(session_id, 0):
            raise RuntimeTransitionError(
                "SESSION_RESULT_PENDING",
                "Session cannot replace its Invocation before the current "
                "public result boundary has been delivered.",
            )
        self._acquire_result_lease(session_id)

        invocation_id: str | None = None
        grouped_root_admission = state.session is None
        try:
            if state.invocation is not None:
                # A terminal graph may still own captured Child Events whose
                # Host append failed after the public parent boundary (notably
                # for spawned Children).  Confirm every old Event before root
                # replacement; otherwise retiring Child State would silently
                # drain the only recoverable copy of unacknowledged progress.
                previous_graph = (session_id, *old_child_sessions)
                for previous_session_id in previous_graph:
                    self._journal.flush(previous_session_id)
                await self._export_runtime_events(previous_graph)
            self._reset_observations(session_id)
            if state.session is None:
                self._journal.begin_event_group(session_id)
                await self._emit(session_id, None, SessionOpened(session_context or {}))
            invocation_id = str(uuid4())
            try:
                opened = await self._emit(
                    session_id,
                    invocation_id,
                    InvocationOpened(
                        compiled.workflow_id,
                        compiled.workflow_revision_id,
                        entry,
                        value,
                    ),
                )
            finally:
                current = self._journal.state(session_id).invocation
                if current is not None and current.id == invocation_id:
                    # State application precedes Host Event export.  Once the
                    # new root Invocation is installed, its superseded Child
                    # graph must be retired even if the sink rejects export.
                    # The unacknowledged root Event remains in the Journal for
                    # an idempotent Host retry; obsolete Child State must not
                    # become a second independent Root at close.
                    self._retire_invocation_graph(
                        session_id,
                        old_child_sessions,
                        retired_invocation_ids,
                    )
            started = await self._emit(
                session_id, invocation_id, InvocationStarted(), opened.id
            )
            await self._emit(
                session_id,
                invocation_id,
                self._scheduler.initialize(compiled, self._journal.state(session_id)),
                started.id,
            )
            ref = InvocationRef(session_id, invocation_id)
            task = self._start_drive(compiled, session_id, invocation_id, None, None)
            if not wait_for_boundary:
                checkpoint = await self._capture_checkpoint(session_id)
                traces, users = self._drain_observations(session_id)
                return InvocationSubmission(ref, checkpoint, traces, users)
            await task
            return await self._result(ref)
        except asyncio.CancelledError:
            current = self._journal.state(session_id).invocation
            if (
                not self._public_caller_is_cancelling()
                and invocation_id is not None
                and current is not None
                and current.id == invocation_id
                and current.terminal
            ):
                # A control operation first commits the terminal State and
                # then cancels the physical drive.  Cancellation of the
                # awaited drive is not cancellation of this public caller;
                # return the stable terminal boundary (and let an attached
                # stream publish its mandatory final Result).
                return await self._result(
                    InvocationRef(session_id, invocation_id)
                )
            if not self._closing:
                if (
                    invocation_id is not None
                    and current is not None
                    and current.id == invocation_id
                    and not current.terminal
                ):
                    await self._cancel_graph(
                        InvocationRef(session_id, invocation_id),
                        "Invocation caller cancelled.",
                    )
                elif current is None:
                    self._abort_admission(session_id)
                    await self._discard_incomplete_session(session_id)
            raise
        except BaseException:
            if grouped_root_admission:
                self._abort_admission(session_id)
            raise
        finally:
            self._release_result_lease(session_id)

    async def _resume(
        self,
        ref: InvocationRef,
        wait_id: str,
        response: object,
        *,
        wait_for_boundary: bool,
    ) -> InvocationResult | InvocationSubmission:
        state = self._state_for_ref(ref, active=True)
        root = self._root_session_id(ref.session_id)
        self._ensure_attached_result_boundary_delivered(root)
        self._acquire_result_lease(root)
        try:
            invocation = state.invocation
            assert invocation is not None
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
            validated = node.executable.output_contract.to_record(response)
            await self._emit(
                ref.session_id, ref.invocation_id, WaitResumed(wait_id, validated)
            )
            task = self._task_runtime.task(ref.session_id)
            if task is None:
                task = self._start_drive(
                    workflow, ref.session_id, ref.invocation_id, None, None
                )
            else:
                self._task_runtime.wake(ref.session_id)
            if not wait_for_boundary:
                checkpoint = await self._capture_checkpoint(root)
                traces, users = self._drain_observations(root)
                return InvocationSubmission(ref, checkpoint, traces, users)
            await task
            return await self._result(ref)
        except asyncio.CancelledError:
            if self._drive_cancelled_at_terminal_boundary(ref):
                return await self._result(ref)
            if not self._closing:
                await self._finish_caller_cancellation(ref)
            raise
        finally:
            self._release_result_lease(root)

    async def _wait(
        self, ref: InvocationRef, timeout: float | None
    ) -> InvocationResult:
        self._state_for_ref(ref)
        root = self._root_session_id(ref.session_id)
        self._acquire_result_lease(root)
        try:
            task = self._task_runtime.task(ref.session_id)
            if task is not None:
                try:
                    await asyncio.wait_for(asyncio.shield(task), timeout)
                except TimeoutError as error:
                    raise TimeoutError(
                        "Invocation did not reach a stable boundary in time."
                    ) from error
                except asyncio.CancelledError:
                    if not self._drive_cancelled_at_terminal_boundary(ref):
                        raise
            current = self._state_for_ref(ref).invocation
            assert current is not None
            if current.status in {"created", "running"}:
                raise RuntimeTransitionError(
                    "INVOCATION_NOT_LIVE",
                    "Invocation is runnable but this App owns no live task; "
                    "load its checkpoint and call recover().",
                )
            return await self._result(ref)
        finally:
            self._release_result_lease(root)

    async def _cancel(
        self, ref: InvocationRef, reason: str | None
    ) -> InvocationResult:
        self._state_for_ref(ref, active=True)
        root = self._root_session_id(ref.session_id)
        self._acquire_result_lease(root)
        try:
            operation = asyncio.create_task(self._cancel_graph(ref, reason))
            try:
                await asyncio.shield(operation)
            except asyncio.CancelledError:
                await asyncio.shield(operation)
                raise
            return await self._result(ref)
        finally:
            self._release_result_lease(root)

    async def _cancel_graph(self, ref: InvocationRef, reason: str | None) -> None:
        target = ref.session_id
        session_ids = (target, *self._descendant_sessions(target))
        root_state = self._journal.state(target)
        root_invocation = root_state.invocation
        if root_invocation is not None and not root_invocation.terminal:
            await self._emit(
                target, root_invocation.id, InvocationCancelled(reason)
            )
        for session_id in session_ids[1:]:
            invocation = self._journal.state(session_id).invocation
            if invocation is not None and not invocation.terminal:
                await self._emit(
                    session_id,
                    invocation.id,
                    InvocationCancelled(reason or "Parent Invocation cancelled."),
                )
        tasks = [
            self._task_runtime.task(session_id) for session_id in session_ids
        ]
        for task in tasks:
            if task is not None:
                task.cancel()
        if any(task is not None for task in tasks):
            await asyncio.gather(
                *(task for task in tasks if task is not None),
                return_exceptions=True,
            )
        for session_id in reversed(session_ids[1:]):
            invocation = self._journal.state(session_id).invocation
            if invocation is not None and invocation.terminal:
                await self._settle_child(session_id, invocation.id)
        target_invocation = self._journal.state(target).invocation
        if target_invocation is not None and target_invocation.terminal:
            await self._settle_child(target, target_invocation.id)

    async def _recover(self, ref: InvocationRef) -> InvocationResult:
        self._state_for_ref(ref)
        root = self._root_session_id(ref.session_id)
        target_path = {ref.session_id}
        current_session_id = ref.session_id
        while current_session_id != root:
            current = self._journal.state(current_session_id).invocation
            parent = self._parent_plan(
                current_session_id,
                current.id if current is not None else None,
            )
            if parent is None:
                raise RuntimeTransitionError(
                    "CHILD_HANDLE_UNKNOWN",
                    "Recovery target is not owned by the current Root graph.",
                )
            current_session_id = parent[0]
            target_path.add(current_session_id)
        self._acquire_result_lease(root)
        try:
            if any(
                self._task_runtime.is_live(session_id)
                for session_id in (root, *self._descendant_sessions(root))
            ):
                raise RuntimeTransitionError(
                    "INVOCATION_STILL_LIVE",
                    "Recovery cannot run while this App owns a graph task.",
                )
            await self._recover_session(
                root,
                set(),
                wait_for_boundary=True,
                target_path=frozenset(target_path),
            )
            return await self._result(ref)
        except asyncio.CancelledError:
            if self._drive_cancelled_at_terminal_boundary(ref):
                return await self._result(ref)
            if not self._closing:
                await self._finish_caller_cancellation(ref)
            raise
        finally:
            self._release_result_lease(root)

    async def _finish_caller_cancellation(self, ref: InvocationRef) -> None:
        current = self._journal.state(ref.session_id).invocation
        if current is None or current.id != ref.invocation_id or current.terminal:
            return
        operation = asyncio.create_task(
            self._cancel_graph(ref, "Invocation caller cancelled.")
        )
        try:
            await asyncio.shield(operation)
        except asyncio.CancelledError:
            await asyncio.shield(operation)
            raise

    async def _recover_session(
        self,
        session_id: str,
        seen: set[str],
        *,
        wait_for_boundary: bool,
        target_path: frozenset[str],
    ) -> None:
        """Restore one graph branch while preserving spawn detachment."""

        if session_id in seen:
            return
        seen.add(session_id)
        state = self._journal.state(session_id)
        invocation = state.invocation
        if invocation is None:
            return
        await self._accept_recovered_child_invocations(session_id, invocation.id)
        state = self._journal.state(session_id)
        invocation = state.invocation
        assert invocation is not None
        if invocation.status in {"failed", "cancelled"}:
            # A persisted graph may end exactly after the ancestor terminal
            # transition and before its live drive cancelled descendants.  A
            # failed/cancelled ancestor owns no resumable business work: close
            # every active descendant instead of replaying it.  Completed
            # ancestors are different because detached spawn Children remain
            # valid work and are recovered below.
            await self._cancel_descendants(
                session_id,
                invocation.cancel_reason
                or "Ancestor Invocation did not complete successfully.",
            )
            await self._settle_child(session_id, invocation.id)
            return
        workflow = self._workflow_for_state(state)
        preflight_error = self._workflow_executor.recovery_preflight_error(
            workflow,
            state,
        )
        if preflight_error is None and invocation.status == "running" and any(
            item.status == "running"
            for item in invocation.scheduler.occurrences.values()
        ):
            preflight_error = self._recovery_error(workflow, state)
        if preflight_error is not None:
            # Terminal Scheduler decisions and recovery permission are ancestor
            # preconditions.  Replaying a Child first could create another
            # external side effect after the parent graph was already doomed.
            await self._emit(
                session_id,
                invocation.id,
                InvocationFailed(preflight_error),
            )
            await self._cancel_descendants(
                session_id,
                "Ancestor Invocation cannot continue during crash recovery.",
            )
            await self._settle_child(session_id, invocation.id)
            return
        child_sessions = tuple(
            (unit.session_id, plan.mode)
            for plan in invocation.child_plans.values()
            for unit in plan.units
            if self._journal.state(unit.session_id).invocation is not None
        )
        if child_sessions:
            recovered = await asyncio.gather(
                *(
                    self._recover_session(
                        child,
                        seen,
                        wait_for_boundary=(
                            wait_for_boundary
                            and (mode == "await" or child in target_path)
                        ),
                        target_path=target_path,
                    )
                    for child, mode in child_sessions
                ),
                return_exceptions=True,
            )
            for (child_session_id, _mode), outcome in zip(
                child_sessions, recovered, strict=True
            ):
                if not isinstance(outcome, BaseException):
                    continue
                child = self._journal.state(child_session_id).invocation
                if isinstance(outcome, asyncio.CancelledError) and (
                    child is not None and child.terminal
                ):
                    # Await fail-fast may deliberately cancel a sibling drive
                    # while the graph recovery coordinator is awaiting it.
                    continue
                raise outcome
        state = self._journal.state(session_id)
        invocation = state.invocation
        assert invocation is not None
        if invocation.terminal:
            await self._settle_child(session_id, invocation.id)
            return
        if invocation.status == "created":
            await self._emit(session_id, invocation.id, InvocationStarted())
            invocation = self._journal.state(session_id).invocation
            assert invocation is not None
        if not invocation.scheduler.initialized:
            await self._emit(
                session_id,
                invocation.id,
                self._scheduler.initialize(workflow, self._journal.state(session_id)),
            )
            invocation = self._journal.state(session_id).invocation
            assert invocation is not None
        elif invocation.status == "running" and any(
            item.status == "running"
            for item in invocation.scheduler.occurrences.values()
        ):
            await self._emit(
                session_id, invocation.id, InvocationRecoveryRequested()
            )
            invocation = self._journal.state(session_id).invocation
            assert invocation is not None
        if invocation.status == "running":
            task = self._task_runtime.task(session_id) or self._start_drive(
                workflow, session_id, invocation.id, None, None
            )
            if wait_for_boundary:
                await task
        await self._settle_child(session_id, invocation.id)

    async def _accept_recovered_child_invocations(
        self, parent_session_id: str, parent_invocation_id: str
    ) -> None:
        """Close every opened Child admission before recovery drives it."""

        parent = self._journal.state(parent_session_id).invocation
        assert parent is not None and parent.id == parent_invocation_id
        candidates = tuple(
            (creation_id, unit.unit_index, unit.session_id)
            for creation_id, plan in parent.child_plans.items()
            for unit in plan.units
            if unit.phase == "opened"
        )
        for creation_id, unit_index, child_session_id in candidates:
            current = self._journal.state(parent_session_id).invocation
            assert current is not None and current.id == parent_invocation_id
            plan = current.child_plans.get(creation_id)
            if plan is None or unit_index >= len(plan.units):
                continue
            unit = plan.units[unit_index]
            if unit.phase != "opened":
                continue
            child = self._journal.state(child_session_id).invocation
            if child is None:
                continue
            await self._emit(
                parent_session_id,
                parent_invocation_id,
                ChildInvocationPhaseChanged(
                    creation_id, unit_index, "accepted"
                ),
            )

    @staticmethod
    def _recovery_error(
        workflow: WorkflowIR, state: RuntimeState
    ) -> RuntimeErrorInfo | None:
        invocation = state.invocation
        if invocation is None:
            return RuntimeErrorInfo("RecoveryStateInvalid", "Invocation is missing.")
        planned_occurrences = {
            plan.parent_occurrence_id for plan in invocation.child_plans.values()
        }
        for occurrence in invocation.scheduler.occurrences.values():
            if occurrence.status != "running":
                continue
            node = workflow.node(occurrence.node_id)
            if occurrence.id in planned_occurrences and not (
                (node.map is not None and node.map.aggregate is not None)
                or node.output_binding is not None
                or any(
                    edge.condition is not None
                    for edge in workflow.outgoing(node.id)
                )
            ):
                # The durable Child plan makes framework-only admission and
                # awaiting idempotent: inputs and Child identities are reused.
                # User post-processing Hooks are not covered by that plan and
                # must still obey the Node's explicit replay policy.
                continue
            recovery = node.recovery_mode
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

    async def _load_checkpoint(
        self, checkpoint: RuntimeCheckpointBundle | AppCheckpoint
    ) -> CheckpointLoadResult:
        bundles = (
            checkpoint.roots
            if isinstance(checkpoint, AppCheckpoint)
            else (checkpoint,)
            if isinstance(checkpoint, RuntimeCheckpointBundle)
            else None
        )
        if bundles is None:
            raise TypeError("checkpoint must be RuntimeCheckpointBundle or AppCheckpoint.")
        if not bundles:
            return CheckpointLoadResult((), ())
        all_states: dict[str, RuntimeState] = {}
        for bundle in bundles:
            overlap = set(all_states).intersection(bundle.states)
            if overlap:
                raise RuntimeTransitionError(
                    "CHECKPOINT_GRAPH_OVERLAP",
                    "Checkpoint Runtime graphs overlap.",
                )
            all_states.update(bundle.states)
        existing_graphs: dict[str, dict[str, RuntimeState]] = {}
        for session_id in self._journal.session_ids():
            root_session_id = self._root_session_id(session_id)
            existing_graphs.setdefault(root_session_id, {})[session_id] = (
                self._journal.state(session_id)
            )
        try:
            _validate_graph_claims(
                (
                    *(
                        (root_session_id, states)
                        for root_session_id, states in existing_graphs.items()
                    ),
                    *((bundle.root_session_id, bundle.states) for bundle in bundles),
                )
            )
        except ValueError as error:
            raise RuntimeTransitionError(
                "CHECKPOINT_GRAPH_CONFLICT",
                str(error),
            ) from error
        for state in all_states.values():
            invocation = state.invocation
            if invocation is None:
                raise RuntimeTransitionError(
                    "CHECKPOINT_INVOCATION_MISSING",
                    "Every checkpoint State must contain an Invocation.",
                )
            workflow = self._workflows.get(invocation.workflow_revision_id)
            if workflow is None or workflow.workflow_id != invocation.workflow_id:
                raise RuntimeTransitionError(
                    "WORKFLOW_NOT_REGISTERED",
                    "Every checkpoint Workflow Revision must be registered exactly.",
                )
        self._validate_checkpoint_child_semantics(bundles)
        self._validate_checkpoint_ownership(bundles)
        if any(
            self._task_runtime.is_live(session_id)
            or session_id in self._attached_streams
            for session_id in all_states
        ) or any(
            self._result_leases.get(bundle.root_session_id, 0)
            for bundle in bundles
        ):
            raise RuntimeTransitionError(
                "CHECKPOINT_SESSION_LIVE",
                "Checkpoint loading cannot replace a live Runtime graph.",
            )
        self._journal.install_states(all_states)
        self._rebuild_child_owners()
        for bundle in bundles:
            self._reset_observations(bundle.root_session_id)
        for session_id, state in all_states.items():
            if state.last_event_id is not None:
                self._last_transition_ids[session_id] = state.last_event_id
        refs = tuple(
            InvocationRef(session_id, state.invocation.id)  # type: ignore[union-attr]
            for session_id, state in sorted(all_states.items())
        )
        roots = tuple(
            InvocationRef(
                bundle.root_session_id,
                bundle.states[bundle.root_session_id].invocation.id,  # type: ignore[union-attr]
            )
            for bundle in bundles
        )
        return CheckpointLoadResult(roots, refs)

    def _validate_checkpoint_child_semantics(
        self, bundles: tuple[RuntimeCheckpointBundle, ...]
    ) -> None:
        """Validate every cross-Session Child claim against its parent IR.

        RuntimeCheckpointBundle validates a self-contained ownership graph, but
        only the App has the registered Workflow definitions needed to prove
        that each plan belongs to the referenced parent Node.  This check runs
        before any State is installed, so an invalid graph cannot execute a
        Child while its parent relationship is still untrusted.
        """

        for bundle in bundles:
            for parent_state in bundle.states.values():
                parent = parent_state.invocation
                assert parent is not None
                workflow = self._workflows[parent.workflow_revision_id]
                planned_occurrences: set[str] = set()
                for plan in parent.child_plans.values():
                    if plan.parent_occurrence_id in planned_occurrences:
                        raise RuntimeTransitionError(
                            "CHECKPOINT_CHILD_SEMANTICS_INVALID",
                            "A parent Node Occurrence cannot own multiple Child plans.",
                        )
                    planned_occurrences.add(plan.parent_occurrence_id)
                    occurrence = parent.scheduler.occurrences.get(
                        plan.parent_occurrence_id
                    )
                    if occurrence is None:
                        raise RuntimeTransitionError(
                            "CHECKPOINT_CHILD_SEMANTICS_INVALID",
                            "Child plan references an unknown parent Node Occurrence.",
                        )
                    try:
                        node = workflow.node(occurrence.node_id)
                    except KeyError as error:
                        raise RuntimeTransitionError(
                            "CHECKPOINT_CHILD_SEMANTICS_INVALID",
                            "Child plan references a Node outside its parent Workflow.",
                        ) from error
                    child = node.executable
                    if not isinstance(child, WorkflowIR):
                        raise RuntimeTransitionError(
                            "CHECKPOINT_CHILD_SEMANTICS_INVALID",
                            "Child plan parent is not a Child Workflow Node.",
                        )
                    if (
                        plan.mode != node.execution_mode
                        or plan.workflow_id != child.workflow_id
                        or plan.workflow_revision_id != child.workflow_revision_id
                    ):
                        raise RuntimeTransitionError(
                            "CHECKPOINT_CHILD_SEMANTICS_INVALID",
                            "Child plan does not match its parent Node definition.",
                        )
                    if node.map is None and len(plan.units) != 1:
                        raise RuntimeTransitionError(
                            "CHECKPOINT_CHILD_SEMANTICS_INVALID",
                            "A non-Map Child Workflow Node requires exactly one unit.",
                        )
                    if occurrence.status in {"ready", "skipped"}:
                        raise RuntimeTransitionError(
                            "CHECKPOINT_CHILD_SEMANTICS_INVALID",
                            "A Child plan cannot precede its parent Node execution.",
                        )
                    if occurrence.status == "completed":
                        allowed_completed_phases = (
                            {"terminal"}
                            if plan.mode == "await"
                            else {"opened", "accepted", "terminal"}
                        )
                        if any(
                            unit.phase not in allowed_completed_phases
                            for unit in plan.units
                        ):
                            raise RuntimeTransitionError(
                                "CHECKPOINT_CHILD_SEMANTICS_INVALID",
                                "A completed Child Workflow Node has an open plan phase.",
                            )
                    for unit in plan.units:
                        child_state = bundle.states.get(unit.session_id)
                        if child_state is None:
                            # Planning is a durable write-ahead boundary.  The
                            # Child Session may not exist while its parent Node
                            # is still running, or after that unfinished Node
                            # was abandoned by failure/cancellation.  A
                            # completed parent/occurrence cannot legitimately
                            # retain a Handle to a missing Child State.
                            if unit.phase == "planned" and (
                                (
                                    parent.status == "running"
                                    and occurrence.status == "running"
                                )
                                or (
                                    parent.status in {"failed", "cancelled"}
                                    and occurrence.status in {"failed", "cancelled"}
                                )
                            ):
                                continue
                            raise RuntimeTransitionError(
                                "CHECKPOINT_CHILD_SEMANTICS_INVALID",
                                "Child Runtime State is missing outside its planning boundary.",
                            )
                        child_invocation = child_state.invocation
                        assert child_invocation is not None
                        if child_invocation.input != unit.input:
                            raise RuntimeTransitionError(
                                "CHECKPOINT_CHILD_SEMANTICS_INVALID",
                                "Child Invocation input does not match its parent plan.",
                            )

    def _validate_checkpoint_ownership(
        self, bundles: tuple[RuntimeCheckpointBundle, ...]
    ) -> None:
        """Reject partial overlap with any graph already owned by this App.

        Reinstalling one identical complete Root graph is idempotent.  Sharing
        even an equal Child State between two different Roots is not: parent
        lookup and clean-shutdown checkpoint construction require exclusive
        graph ownership.
        """

        existing_ids = set(self._journal.session_ids())
        for bundle in bundles:
            incoming_ids = set(bundle.states)
            overlap = existing_ids.intersection(incoming_ids)
            if not overlap:
                continue
            if bundle.root_session_id not in existing_ids:
                raise RuntimeTransitionError(
                    "CHECKPOINT_GRAPH_CONFLICT",
                    "Checkpoint graph partially overlaps an existing Runtime graph.",
                )
            current_root = self._root_session_id(bundle.root_session_id)
            current_ids = {
                current_root,
                *(
                    session_id
                    for session_id in self._descendant_sessions(current_root)
                    if session_id in existing_ids
                ),
            }
            if (
                current_root != bundle.root_session_id
                or current_ids != incoming_ids
                or any(
                    self._journal.state(session_id) != bundle.states[session_id]
                    for session_id in incoming_ids
                )
            ):
                raise RuntimeTransitionError(
                    "CHECKPOINT_GRAPH_CONFLICT",
                    "Checkpoint graph conflicts with existing Runtime ownership.",
                )

    async def _list_child_handles(
        self, parent: InvocationRef
    ) -> tuple[ChildInvocationHandle, ...]:
        parent_state = self._state_for_ref(parent)
        invocation = parent_state.invocation
        assert invocation is not None
        handles: list[ChildInvocationHandle] = []
        for unit in (
            unit
            for plan in invocation.child_plans.values()
            for unit in plan.units
        ):
            if self._journal.event_group_active(unit.session_id):
                # Admission traces are observable one transition at a time,
                # while their canonical Event and checkpoint membership are
                # all-or-nothing.  Do not publish a Handle whose own Result
                # could not yet carry a self-contained recovery checkpoint.
                continue
            child = self._journal.state(unit.session_id).invocation
            if child is None or child.id != unit.invocation_id:
                continue
            handles.append(
                cast(
                    ChildInvocationHandle,
                    {
                        "session_id": unit.session_id,
                        "invocation_id": unit.invocation_id,
                        "workflow_id": child.workflow_id,
                        "workflow_revision_id": child.workflow_revision_id,
                    },
                )
            )
        return tuple(handles)

    async def _child_status(
        self, handle: ChildInvocationHandle
    ) -> InvocationResult:
        ref = self._child_ref(handle)
        root = self._root_session_id(ref.session_id)
        self._acquire_result_lease(root)
        try:
            return await self._result(ref)
        finally:
            self._release_result_lease(root)

    async def _wait_child(
        self,
        handle: ChildInvocationHandle,
        timeout: float | None,
    ) -> InvocationResult:
        return await self._wait(self._child_ref(handle), timeout)

    async def _cancel_child(
        self,
        handle: ChildInvocationHandle,
        reason: str | None,
    ) -> InvocationResult:
        return await self._cancel(self._child_ref(handle), reason)

    async def _close(self) -> AppCheckpoint:
        return await self._close_operation()

    async def _close_operation(self) -> AppCheckpoint:
        """Quiesce every transient owner and capture all current Root graphs."""

        for channel in tuple(self._attached_streams.values()):
            channel.abandon()
        self._attached_streams.clear()
        stream_tasks = tuple(
            task for task in self._attached_stream_tasks.values() if not task.done()
        )
        for task in stream_tasks:
            task.cancel()
        if stream_tasks:
            await asyncio.gather(*stream_tasks, return_exceptions=True)
        await self._task_runtime.cancel_all()
        await self._discard_incomplete_sessions()
        roots = self._root_session_ids()
        checkpoints = tuple(
            [await self._capture_checkpoint(root) for root in roots]
        )
        return AppCheckpoint(checkpoints)

    # ------------------------------------------------------------------
    # Task, Event, Trace and Checkpoint coordination

    def _start_drive(
        self,
        workflow: WorkflowIR,
        session_id: str,
        invocation_id: str,
        capacity: asyncio.Semaphore | None,
        gate: asyncio.Event | None,
    ) -> asyncio.Task[None]:
        existing = self._task_runtime.task(session_id)
        if existing is not None:
            return existing

        capacity = self._child_capacity(session_id, capacity)

        async def run() -> None:
            if gate is not None:
                await gate.wait()
            if capacity is None:
                await self._workflow_executor.drive(workflow, session_id)
            else:
                async with capacity:
                    await self._workflow_executor.drive(workflow, session_id)
            current = self._journal.state(session_id).invocation
            if current is not None and current.status in {"failed", "cancelled"}:
                await self._cancel_descendants(
                    session_id,
                    "Parent Invocation did not complete successfully.",
                )
            await self._settle_child(session_id, invocation_id)

        task = asyncio.create_task(run())
        self._task_runtime.track(session_id, task)
        return task

    async def _settle_child(self, session_id: str, invocation_id: str) -> None:
        parent_info = self._parent_plan(session_id, invocation_id)
        if parent_info is None:
            return
        parent_session_id, creation_id, unit_index = parent_info
        child_invocation = self._journal.state(session_id).invocation
        if child_invocation is None or not child_invocation.terminal:
            return
        parent = self._journal.state(parent_session_id).invocation
        if parent is None:
            return
        plan = parent.child_plans.get(creation_id)
        if plan is None:
            return
        unit = plan.units[unit_index]
        if unit.phase == "accepted":
            await self._ensure_child_durable(session_id)
            await self._emit(
                parent_session_id,
                parent.id,
                ChildInvocationPhaseChanged(creation_id, unit_index, "terminal"),
            )
            parent = self._journal.state(parent_session_id).invocation
            assert parent is not None
            plan = parent.child_plans[creation_id]
        if plan.mode != "await":
            return
        occurrence = parent.scheduler.occurrences.get(plan.parent_occurrence_id)
        if occurrence is None or occurrence.status != "waiting":
            return
        child_states = tuple(
            self._journal.state(item.session_id).invocation for item in plan.units
        )
        if any(
            child is not None and child.status in {"failed", "cancelled"}
            for child in child_states
        ):
            # Once an awaited parent has suspended, a terminal failure is
            # enough to decide the whole Map.  Converge every remaining unit
            # before waking the parent so its failure boundary is recoverable
            # and no sibling remains indefinitely waiting.
            await self._workflow_executor.converge_failed_child_plan(
                parent_session_id,
                parent.id,
                creation_id,
            )
            parent = self._journal.state(parent_session_id).invocation
            assert parent is not None
            plan = parent.child_plans[creation_id]
        if any(item.phase != "terminal" for item in plan.units):
            return
        await self._emit(
            parent_session_id,
            parent.id,
            ChildAwaitReady(creation_id, plan.parent_occurrence_id),
        )
        parent_state = self._journal.state(parent_session_id)
        parent = parent_state.invocation
        if parent is not None and not parent.terminal and not self._closing:
            workflow = self._workflow_for_state(parent_state)
            if self._task_runtime.task(parent_session_id) is not None:
                self._task_runtime.wake(parent_session_id)
            else:
                self._start_drive(
                    workflow, parent_session_id, parent.id, None, None
                )

    async def _cancel_descendants(
        self, root_session_id: str, reason: str
    ) -> None:
        descendants = self._descendant_sessions(root_session_id)
        if not descendants:
            return
        tasks = [self._task_runtime.task(session_id) for session_id in descendants]
        for task in tasks:
            if task is not None:
                task.cancel()
        if any(task is not None for task in tasks):
            await asyncio.gather(
                *(task for task in tasks if task is not None),
                return_exceptions=True,
            )
        for session_id in reversed(descendants):
            invocation = self._journal.state(session_id).invocation
            if invocation is not None and not invocation.terminal:
                await self._emit(
                    session_id,
                    invocation.id,
                    InvocationCancelled(reason),
                )
            invocation = self._journal.state(session_id).invocation
            if invocation is not None and invocation.terminal:
                await self._settle_child(session_id, invocation.id)

    def _child_capacity(
        self,
        child_session_id: str,
        supplied: asyncio.Semaphore | None,
    ) -> asyncio.Semaphore | None:
        parent_info = self._parent_plan(child_session_id)
        if parent_info is None:
            return supplied
        parent_session_id, creation_id, _unit_index = parent_info
        key = (parent_session_id, creation_id)
        existing = self._child_capacities.get(key)
        if existing is not None:
            return existing
        if supplied is not None:
            self._child_capacities[key] = supplied
            return supplied
        parent_state = self._journal.state(parent_session_id)
        parent = parent_state.invocation
        if parent is None:
            return None
        plan = parent.child_plans.get(creation_id)
        if plan is None:
            return None
        occurrence = parent.scheduler.occurrences.get(plan.parent_occurrence_id)
        if occurrence is None:
            return None
        node = self._workflow_for_state(parent_state).node(occurrence.node_id)
        limit = min(
            len(plan.units),
            node.map.max_parallelism
            if node.map is not None and node.map.max_parallelism is not None
            else self._node_executor.max_operator_concurrency,
            self._node_executor.max_operator_concurrency,
        )
        capacity = asyncio.Semaphore(limit)
        self._child_capacities[key] = capacity
        return capacity

    async def _emit(
        self,
        session_id: str,
        invocation_id: str | None,
        payload: object,
        causation_id: str | None = None,
    ) -> StateTransition:
        root = self._root_session_id(session_id)
        channel = self._attached_streams.get(root)
        created: StateTransition | None = None

        async def commit() -> InvocationUpdate:
            nonlocal created
            async with self._runtime_lock(root):
                transition = StateTransition(
                    session_id=session_id,
                    invocation_id=invocation_id,
                    payload=payload,  # type: ignore[arg-type]
                    causation_id=(
                        causation_id
                        if causation_id is not None
                        else self._last_transition_ids.get(session_id)
                    ),
                    occurred_at_ns=self._clock_ns(),
                )
                self._journal.apply_transition(transition)
                if self._journal.event_group_active(session_id) and (
                    isinstance(payload, SchedulerInitialized)
                    or (
                        isinstance(payload, InvocationOpened)
                        and self._parent_plan(session_id) is None
                    )
                ):
                    # A new Root atomically opens Session + Invocation; a Child
                    # atomically opens through Scheduler initialization.  The
                    # transitions remain individually observable as Trace, but
                    # a Host can never acknowledge an unrecoverable prefix.
                    self._journal.commit_event_group(session_id)
                if isinstance(payload, ChildInvocationPlanned):
                    for unit in payload.units:
                        self._child_owners[unit.child_session_id] = (
                            session_id,
                            payload.creation_id,
                            unit.unit_index,
                            unit.child_invocation_id,
                        )
                created = transition
                self._last_transition_ids[session_id] = transition.id
                live = self._journal.state(session_id)
                trace = replace(
                    project_trace_event(
                        session_id,
                        self._next_trace_sequence(root),
                        transition,
                    ),
                    state_version=live.state_version,
                )
                await self._export_runtime_events((session_id,))
                checkpoint = (
                    await self._capture_checkpoint_locked(root)
                    if channel is not None
                    and isinstance(payload, _CHECKPOINT_PAYLOADS)
                    and self._checkpoint_boundary_safe(root)
                    else None
                )
                update = InvocationUpdate(trace, checkpoint)
                if channel is None or not channel.attached:
                    self._observations.setdefault(root, []).append(trace)
                return update

        if channel is None:
            await commit()
        else:
            await channel.publish_async_created(commit)
        assert created is not None
        return created

    def _begin_child_admission(self, session_id: str) -> None:
        self._journal.begin_event_group(session_id)

    def _abort_child_admission(self, session_id: str) -> None:
        self._abort_admission(session_id)

    def _abort_admission(self, session_id: str) -> None:
        if self._journal.abort_event_group(session_id):
            # A grouped admission starts from an empty Session State, so every
            # transition causation id created inside that group is gone too.
            self._last_transition_ids.pop(session_id, None)

    async def _ensure_child_durable(self, session_id: str) -> None:
        """Persist Child progress before a dependent parent phase can advance."""

        root = self._root_session_id(session_id)
        async with self._runtime_lock(root):
            if self._journal.event_group_active(session_id):
                raise RuntimeTransitionError(
                    "CHILD_ADMISSION_INCOMPLETE",
                    "Child admission did not reach Scheduler initialization.",
                )
            # Retrying after a sink failure reaches this barrier with the
            # complete Event still owned by the Journal.  Exporting here is
            # therefore both the normal durability fence and the retry path.
            self._journal.flush(session_id)
            await self._export_runtime_events((session_id,))

    async def _emit_user(
        self,
        session_id: str,
        invocation_id: str,
        kind: str,
        payload: object,
        occurrence_id: str | None = None,
    ) -> UserEvent:
        root = self._root_session_id(session_id)
        channel = self._attached_streams.get(root)
        created: UserEvent | None = None

        async def commit() -> InvocationUpdate:
            nonlocal created
            created = self._user_event_journal.emit(
                session_id=session_id,
                invocation_id=invocation_id,
                kind=kind,
                payload=payload,
                occurrence_id=occurrence_id,
                occurred_at_ns=self._clock_ns(),
            )
            self._user_event_journal.drain(invocation_id)
            if channel is None or not channel.attached:
                self._observations.setdefault(root, []).append(created)
            return InvocationUpdate(created)

        if channel is None:
            await commit()
        else:
            await channel.publish_async_created(commit)
        assert created is not None
        return created

    async def _capture_checkpoint(
        self, root_session_id: str
    ) -> RuntimeCheckpointBundle:
        async with self._runtime_lock(root_session_id):
            return await self._capture_checkpoint_locked(root_session_id)

    async def _capture_checkpoint_locked(
        self, root_session_id: str
    ) -> RuntimeCheckpointBundle:
        checkpoint = self._journal.capture_checkpoint(
            root_session_id, captured_at_ns=self._clock_ns()
        )
        await self._export_runtime_events(tuple(checkpoint.states))
        return checkpoint

    def _runtime_lock(self, root_session_id: str) -> asyncio.Lock:
        return self._runtime_locks.setdefault(root_session_id, asyncio.Lock())

    def _acquire_result_lease(self, root_session_id: str) -> None:
        self._result_leases[root_session_id] = (
            self._result_leases.get(root_session_id, 0) + 1
        )

    def _ensure_attached_result_boundary_delivered(
        self, root_session_id: str
    ) -> None:
        channel = self._attached_streams.get(root_session_id)
        if channel is not None and channel.attached:
            raise RuntimeTransitionError(
                "INVOCATION_RESULT_PENDING",
                "Invocation cannot resume before its current public result "
                "boundary is delivered.",
            )

    @staticmethod
    def _public_caller_is_cancelling() -> bool:
        current = asyncio.current_task()
        return current is not None and current.cancelling() > 0

    def _drive_cancelled_at_terminal_boundary(self, ref: InvocationRef) -> bool:
        """Distinguish control-plane drive cancellation from caller cancellation."""

        if self._public_caller_is_cancelling():
            return False
        current = self._journal.state(ref.session_id).invocation
        return (
            current is not None
            and current.id == ref.invocation_id
            and current.terminal
        )

    def _release_result_lease(self, root_session_id: str) -> None:
        remaining = self._result_leases[root_session_id] - 1
        if remaining:
            self._result_leases[root_session_id] = remaining
        else:
            self._result_leases.pop(root_session_id, None)

    async def _export_runtime_events(self, session_ids: tuple[str, ...]) -> None:
        for session_id in dict.fromkeys(session_ids):
            events = self._journal.events(session_id)
            if not events:
                continue
            if self._runtime_event_sink is not None:
                try:
                    for event in events:
                        await self._runtime_event_sink.append(event)
                except asyncio.CancelledError:
                    raise
                except BaseException as error:
                    raise RuntimeInfrastructureError(
                        "Runtime Event sink rejected canonical progress."
                    ) from error
            self._journal.drain_events(session_id)

    async def _result(self, ref: InvocationRef) -> InvocationResult:
        root = self._root_session_id(ref.session_id)
        async with self._runtime_lock(root):
            # Result data, observation ownership and its recovery boundary are
            # one point-in-time projection.  Reading State before joining this
            # lock can pair an old Invocation with a newer Checkpoint when
            # multiple Event publishers are already queued ahead of Result.
            state = self._state_for_ref(ref)
            invocation = state.invocation
            assert invocation is not None
            checkpoint = await self._capture_checkpoint_locked(root)
            traces, users = self._drain_observations(root)
            waits = tuple(
                InvocationWait(item.id, thaw(item.request))
                for item in invocation.scheduler.waits.values()
                if item.status == "waiting"
            )
            return InvocationResult(
                ref=ref,
                status=cast(InvocationStatus, invocation.status),
                checkpoint=checkpoint,
                output=thaw(invocation.output),
                error=invocation.error,
                waits=waits,
                trace_events=traces,
                user_events=users,
            )

    # ------------------------------------------------------------------
    # State graph helpers

    def _retire_invocation_graph(
        self,
        root_session_id: str,
        child_session_ids: tuple[str, ...],
        invocation_ids: tuple[str, ...],
    ) -> None:
        """Release the superseded Invocation graph after root replacement."""

        if child_session_ids:
            self._journal.discard_states(child_session_ids)
            for child_session_id in child_session_ids:
                self._child_owners.pop(child_session_id, None)
                self._last_transition_ids.pop(child_session_id, None)
                self._runtime_locks.pop(child_session_id, None)
            retired_sessions = {root_session_id, *child_session_ids}
            self._child_capacities = {
                key: capacity
                for key, capacity in self._child_capacities.items()
                if key[0] not in retired_sessions
            }
        for invocation_id in invocation_ids:
            self._user_event_journal.discard(invocation_id)

    def _state_for_ref(
        self, ref: InvocationRef, *, active: bool = False
    ) -> RuntimeState:
        if not isinstance(ref, InvocationRef):
            raise TypeError("Control operations require an InvocationRef.")
        state = self._journal.state(ref.session_id)
        invocation = state.invocation
        if invocation is None or invocation.id != ref.invocation_id:
            raise RuntimeTransitionError(
                "INVOCATION_REF_STALE",
                "InvocationRef does not identify the Session's current Invocation.",
            )
        if active and invocation.status not in {"running", "waiting"}:
            raise RuntimeTransitionError(
                "INVOCATION_NOT_RUNNING", "Invocation is not running."
            )
        return state

    def _parent_plan(
        self, child_session_id: str, child_invocation_id: str | None = None
    ) -> tuple[str, str, int] | None:
        owner = self._child_owners.get(child_session_id)
        if owner is None:
            return None
        parent_session_id, creation_id, unit_index, owned_invocation_id = owner
        if (
            child_invocation_id is not None
            and child_invocation_id != owned_invocation_id
        ):
            return None
        parent = self._journal.state(parent_session_id).invocation
        plan = parent.child_plans.get(creation_id) if parent is not None else None
        if plan is None or unit_index >= len(plan.units):
            return None
        unit = plan.units[unit_index]
        if (
            unit.session_id != child_session_id
            or unit.invocation_id != owned_invocation_id
        ):
            return None
        return parent_session_id, creation_id, unit_index

    def _rebuild_child_owners(self) -> None:
        owners: dict[str, tuple[str, str, int, str]] = {}
        for parent_session_id in self._journal.session_ids():
            parent = self._journal.state(parent_session_id).invocation
            if parent is None:
                continue
            for creation_id, plan in parent.child_plans.items():
                for unit in plan.units:
                    previous = owners.get(unit.session_id)
                    owner = (
                        parent_session_id,
                        creation_id,
                        unit.unit_index,
                        unit.invocation_id,
                    )
                    if previous is not None and previous != owner:
                        raise RuntimeTransitionError(
                            "CHILD_GRAPH_MULTIPLE_PARENTS",
                            "One Child Session cannot belong to multiple parent plans.",
                        )
                    owners[unit.session_id] = owner
        self._child_owners = owners

    def _root_session_id(self, session_id: str) -> str:
        current = session_id
        visited: set[str] = set()
        while current not in visited:
            visited.add(current)
            state = self._journal.state(current)
            invocation = state.invocation
            parent = self._parent_plan(
                current, invocation.id if invocation is not None else None
            )
            if parent is None:
                return current
            current = parent[0]
        raise RuntimeTransitionError(
            "CHILD_GRAPH_CYCLE", "Runtime Child graph contains a cycle."
        )

    def _descendant_sessions(self, root_session_id: str) -> tuple[str, ...]:
        result: list[str] = []
        pending = [root_session_id]
        seen = {root_session_id}
        while pending:
            session_id = pending.pop()
            invocation = self._journal.state(session_id).invocation
            if invocation is None:
                continue
            for plan in invocation.child_plans.values():
                for unit in plan.units:
                    if unit.session_id in seen:
                        continue
                    seen.add(unit.session_id)
                    result.append(unit.session_id)
                    pending.append(unit.session_id)
        return tuple(result)

    def _root_session_ids(self) -> tuple[str, ...]:
        sessions = set(self._journal.session_ids())
        children: set[str] = set()
        for session_id in sessions:
            invocation = self._journal.state(session_id).invocation
            if invocation is None:
                continue
            children.update(
                unit.session_id
                for plan in invocation.child_plans.values()
                for unit in plan.units
                if unit.session_id in sessions
            )
        return tuple(sorted(sessions - children))

    async def _discard_incomplete_sessions(self) -> None:
        incomplete = tuple(
            session_id
            for session_id in self._journal.session_ids()
            if self._journal.state(session_id).invocation is None
        )
        if not incomplete:
            return
        for session_id in incomplete:
            await self._discard_incomplete_session(session_id)

    async def _discard_incomplete_session(self, session_id: str) -> None:
        self._abort_admission(session_id)
        state = self._journal.state(session_id)
        if state.session is None or state.invocation is not None:
            return
        self._journal.flush(session_id)
        await self._export_runtime_events((session_id,))
        self._journal.discard_states((session_id,))
        self._child_owners.pop(session_id, None)
        self._last_transition_ids.pop(session_id, None)
        self._runtime_locks.pop(session_id, None)
        self._observations.pop(session_id, None)
        self._trace_sequences.pop(session_id, None)

    def _checkpoint_boundary_safe(self, root_session_id: str) -> bool:
        root = self._journal.state(root_session_id).invocation
        if root is None or root.status not in {"failed", "cancelled"}:
            return True
        return all(
            (invocation := self._journal.state(session_id).invocation) is not None
            and invocation.terminal
            for session_id in self._descendant_sessions(root_session_id)
        )

    def _child_ref(self, handle: ChildInvocationHandle) -> InvocationRef:
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
                "CHILD_HANDLE_INVALID", "Child Invocation Handle is incomplete."
            )
        ref = InvocationRef(handle["session_id"], handle["invocation_id"])
        if self._journal.event_group_active(ref.session_id):
            raise RuntimeTransitionError(
                "CHILD_ADMISSION_INCOMPLETE",
                "Child Invocation admission has not reached its recoverable boundary.",
            )
        state = self._state_for_ref(ref)
        invocation = state.invocation
        assert invocation is not None
        if (
            self._parent_plan(ref.session_id, ref.invocation_id) is None
            or invocation.workflow_id != handle["workflow_id"]
            or invocation.workflow_revision_id != handle["workflow_revision_id"]
        ):
            raise RuntimeTransitionError(
                "CHILD_HANDLE_UNKNOWN",
                "Child Invocation Handle is not owned by the current Runtime graph.",
            )
        return ref

    def _workflow_for_state(self, state: RuntimeState) -> WorkflowIR:
        invocation = state.invocation
        if state.session is None or invocation is None:
            raise RuntimeTransitionError(
                "INVOCATION_UNKNOWN", "Runtime State has no Invocation."
            )
        workflow = self._workflows.get(invocation.workflow_revision_id)
        if workflow is None or workflow.workflow_id != invocation.workflow_id:
            raise RuntimeTransitionError(
                "WORKFLOW_NOT_REGISTERED", "Workflow Revision is not registered."
            )
        return workflow

    def _register_ir(self, workflow: WorkflowIR) -> None:
        closure: list[WorkflowIR] = []
        pending = [workflow]
        seen: dict[str, str] = {}
        while pending:
            current = pending.pop()
            previous_hash = seen.get(current.workflow_revision_id)
            if previous_hash is not None:
                if previous_hash != current.definition_hash:
                    raise RuntimeTransitionError(
                        "WORKFLOW_REVISION_CONFLICT",
                        f"Workflow Revision {current.workflow_revision_id!r} was reused.",
                    )
                continue
            seen[current.workflow_revision_id] = current.definition_hash
            closure.append(current)
            pending.extend(
                cast(WorkflowIR, node.executable)
                for node in reversed(current.nodes)
                if isinstance(node.executable, WorkflowIR)
            )

        snapshots: dict[str, WorkflowDefinitionSnapshot] = {}
        capabilities: list[Capability] = []
        staged: dict[str, WorkflowIR] = {}
        for current in closure:
            existing = self._workflows.get(current.workflow_revision_id)
            pending_existing = staged.get(current.workflow_revision_id)
            for candidate in (existing, pending_existing):
                if (
                    candidate is not None
                    and candidate.definition_hash != current.definition_hash
                ):
                    raise RuntimeTransitionError(
                        "WORKFLOW_REVISION_CONFLICT",
                        f"Workflow Revision {current.workflow_revision_id!r} was reused.",
                    )
            staged[current.workflow_revision_id] = current
            snapshots[current.workflow_revision_id] = (
                WorkflowDefinitionSnapshot.from_workflow_ir(current)
            )
            capabilities.extend(
                cast(Capability, node.executable)
                for node in current.nodes
                if isinstance(node.executable, Capability)
            )

        # Capability validation and binding own one Registry transaction.  No
        # definition dictionaries change until the complete IR closure and
        # every nominal implementation contract have passed validation.
        self._operator_registry.bind_capabilities(tuple(capabilities))
        for current in closure:
            self._workflows[current.workflow_revision_id] = current
            self._workflow_definition_snapshots[current.workflow_revision_id] = (
                snapshots[current.workflow_revision_id]
            )
            self._latest_workflow_revision[current.workflow_id] = (
                current.workflow_revision_id
            )

    def _resolve_workflow(self, value: Workflow | str) -> WorkflowIR:
        if isinstance(value, Workflow):
            # The public call was already admitted atomically with App close;
            # compiling here must not be rejected merely because close started
            # after that admission point.
            return self._compile_and_register(value)
        workflow = self._workflows.get(value)
        if workflow is None:
            revision_id = self._latest_workflow_revision.get(value)
            workflow = self._workflows.get(revision_id or "")
        if workflow is None:
            raise RuntimeTransitionError(
                "WORKFLOW_NOT_REGISTERED", f"Workflow {value!r} is not registered."
            )
        return workflow

    def _next_trace_sequence(self, root_session_id: str) -> int:
        sequence = self._trace_sequences.get(root_session_id, 0) + 1
        self._trace_sequences[root_session_id] = sequence
        return sequence

    def _reset_observations(self, root_session_id: str) -> None:
        self._observations[root_session_id] = []
        self._trace_sequences[root_session_id] = 0

    def _drain_observations(
        self, root_session_id: str
    ) -> tuple[tuple[TraceEvent, ...], tuple[UserEvent, ...]]:
        values = tuple(self._observations.get(root_session_id, ()))
        self._observations[root_session_id] = []
        return (
            tuple(item for item in values if isinstance(item, TraceEvent)),
            tuple(item for item in values if isinstance(item, UserEvent)),
        )

    def _submit(self, coroutine):
        try:
            # Admission and the transition to ``closing`` are one linearized
            # operation.  A call is therefore either queued before close or
            # rejected; it cannot be lost between the two lifecycle owners.
            with self._close_lock:
                self._ensure_open()
                return self._runtime_loop.submit(coroutine)
        except BaseException:
            coroutine.close()
            raise

    def _close_attached_stream(
        self, session_id: str, channel: AttachedStream
    ) -> None:
        coroutine = self._abandon_attached_stream(session_id, channel)
        try:
            future = self._runtime_loop.submit(coroutine)
        except RuntimeError:
            coroutine.close()
            return
        try:
            future.result()
        except FutureCancelledError:
            if not (self._closing or self._closed):
                raise

    async def _aclose_attached_stream(
        self, session_id: str, channel: AttachedStream
    ) -> None:
        coroutine = self._abandon_attached_stream(session_id, channel)
        try:
            future = self._runtime_loop.submit(coroutine)
        except RuntimeError:
            coroutine.close()
            return
        try:
            await self._runtime_loop.wait(future)
        except FutureCancelledError:
            if not (self._closing or self._closed):
                raise

    async def _await(self, future):
        return await self._runtime_loop.wait(future)

    def _run(self, coroutine):
        return self._submit(coroutine).result()

    def _ensure_open(self) -> None:
        if self._closed or self._closing:
            raise RuntimeError("AutoAgentApp is closed or closing.")


def _single_entry(workflow: WorkflowIR) -> str:
    if len(workflow.entry_node_ids) != 1:
        raise RuntimeTransitionError(
            "INVOCATION_ENTRY_REQUIRED",
            "Workflow with multiple Entries requires entry_node_id.",
        )
    return workflow.entry_node_ids[0]


def _positive_integer(value: object, name: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(f"{name} must be positive.")


def _close_timeout(value: float | None) -> None:
    if value is not None and (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValueError("close timeout must be a non-negative number or None.")


__all__ = [
    "AppCheckpoint",
    "AutoAgentApp",
    "CapabilityResolver",
    "CheckpointLoadResult",
    "InvocationRef",
    "InvocationResult",
    "InvocationStream",
    "InvocationSubmission",
    "InvocationUpdate",
    "InvocationWait",
    "StreamItem",
]

"""Public facade and process-local assembly for the standalone V2 Core."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import math
import threading
import warnings
from collections.abc import AsyncIterator, Callable, Mapping
from concurrent.futures import CancelledError as FutureCancelledError, Future
from dataclasses import replace
from typing import cast, get_args
from uuid import uuid4

from ..compiler import WorkflowCompiler, WorkflowDefinitionSnapshot
from ..errors import RuntimeTransitionError
from ..executor import CapabilityResolver, NodeExecutor, WorkflowExecutor
from ..hosting import RuntimeEventSink, UserEventSink
from ..operators import Operator, OperatorRegistry, Wait
from ..runtime.clocks import unix_time_us
from ..runtime import (
    ChildAwaitReady,
    ChildInvocationPhaseChanged,
    ChildInvocationPlanned,
    RuntimeRepository,
    InMemoryUserEventJournal,
    InvocationCancelled,
    InvocationFailed,
    RecoveryApplied,
    InvocationState,
    InvocationStarted,
    NodeCompleted,
    NodeFailed,
    SessionCheckpoint,
    RuntimeErrorInfo,
    RuntimeEvent,
    RuntimeState,
    SessionOpened,
    TaskRuntime,
    UserEvent,
    WaitResumed,
    thaw,
)
from ..scheduler import Scheduler
from ..workflow import Capability, InvocationRef, Workflow, WorkflowIR
from .models import (
    AppCheckpoint,
    CheckpointLoadResult,
    InvocationResult,
    InvocationStatus,
    InvocationSubmission,
    InvocationUpdate,
    InvocationWait,
    StreamItem,
)
from .ports import (
    Clock,
    NodeExecutorPort,
    OperatorRegistryPort,
    RuntimeRepositoryPort,
    SchedulerPort,
    UserEventJournalPort,
)
from .runtime_loop import RuntimeLoop
from ._graph_gate import GraphGate
from .stream import AttachedStream, InvocationStream, is_stream_end


class AutoAgentApp:
    """Compile and execute Workflows while retaining only current Runtime State."""

    def __init__(
        self,
        *,
        max_operator_concurrency: int = 32,
        max_node_executions_per_invocation: int = 1_000,
        capability_resolver: CapabilityResolver | None = None,
        runtime_repository: RuntimeRepositoryPort | None = None,
        runtime_event_sink: RuntimeEventSink | None = None,
        user_event_sink: UserEventSink | None = None,
        user_event_journal: UserEventJournalPort | None = None,
        scheduler: SchedulerPort | None = None,
        node_executor: NodeExecutorPort | None = None,
        clock_us: Clock | None = None,
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
        self._repository = runtime_repository or RuntimeRepository(sink=runtime_event_sink)
        if runtime_repository is not None and runtime_event_sink is not None:
            if not isinstance(runtime_repository, RuntimeRepository):
                raise TypeError("Configure the sink on the custom RuntimeRepository.")
            runtime_repository.sink = runtime_event_sink
        self._runtime_event_sink = runtime_event_sink
        self._user_event_sink = user_event_sink
        self._user_event_sink_errors: dict[str, BaseException] = {}
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
        self._clock_us = clock_us or unix_time_us
        self._operator_registry = operator_registry or OperatorRegistry()
        self._task_runtime = TaskRuntime()
        self._workflow_executor = WorkflowExecutor(
            journal=self._repository,
            scheduler=self._scheduler,
            node_executor=self._node_executor,  # type: ignore[arg-type]
            task_runtime=self._task_runtime,
            operator_registry=self._operator_registry,
            emit=self._emit_executor_event,
            child_admission=self._child_admission,
            emit_user=self._emit_user,
            start_child=self._start_drive,
            ensure_child_durable=self._ensure_child_durable,
            max_node_executions_per_invocation=max_node_executions_per_invocation,
            capability_resolver=capability_resolver,
        )
        self._runtime_loop = RuntimeLoop()
        self._attached_streams: dict[str, AttachedStream] = {}
        self._attached_stream_tasks: dict[str, asyncio.Task[None]] = {}
        self._admission_retirements: dict[str, tuple[str, tuple[str, ...], tuple[str, ...]]] = {}
        self._graph_gates: dict[str, GraphGate] = {}
        self._session_transition_locks: dict[str, asyncio.Lock] = {}
        self._recovering: dict[str, set[str]] = {}
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
        self._close_future: Future[AppCheckpoint | None] | None = None
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

    @property
    def user_event_sink_error(self) -> BaseException | None:
        """Return the first independent User Event delivery failure, if any."""

        return next(iter(self._user_event_sink_errors.values()), None)

    @property
    def user_event_sink_errors(self) -> Mapping[str, BaseException]:
        """Return failed User Event streams keyed by Invocation id."""

        return dict(self._user_event_sink_errors)

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

    def stream_resume(
        self,
        ref: InvocationRef,
        wait_id: str,
        response: object,
    ) -> InvocationStream[StreamItem]:
        root, channel = self._run(
            self._start_attached_resume_stream(ref, wait_id, response)
        )

        def close() -> None:
            self._close_attached_stream(root, channel)

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

    async def astream_resume(
        self,
        ref: InvocationRef,
        wait_id: str,
        response: object,
    ) -> AsyncIterator[StreamItem]:
        root, channel = await self._await(
            self._submit(self._start_attached_resume_stream(ref, wait_id, response))
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
            await self._aclose_attached_stream(root, channel)

    def status(
        self, ref: InvocationRef
    ) -> InvocationResult:
        """Read the current state of an exact Invocation without waiting."""

        return self._run(self._status(ref))

    async def astatus(
        self, ref: InvocationRef
    ) -> InvocationResult:
        """Asynchronously read an exact Invocation without waiting."""

        return await self._await(self._submit(self._status(ref)))

    def join(
        self,
        ref: InvocationRef,
        timeout: float | None = None,
    ) -> InvocationResult:
        """Join a submitted Invocation at its next stable boundary.

        A timeout raises ``TimeoutError`` without cancelling the Invocation.
        """

        return self._run(self._join(ref, timeout))

    async def ajoin(
        self,
        ref: InvocationRef,
        timeout: float | None = None,
    ) -> InvocationResult:
        """Asynchronously join without cancelling the Invocation on timeout."""

        return await self._await(self._submit(self._join(ref, timeout)))

    def resume(
        self,
        ref: InvocationRef,
        wait_id: str,
        response: object,
    ) -> InvocationResult:
        return self._run(
            self._resume(ref, wait_id, response, wait_for_boundary=True)
        )

    async def aresume(
        self,
        ref: InvocationRef,
        wait_id: str,
        response: object,
    ) -> InvocationResult:
        return await self._await(
            self._submit(
                self._resume(ref, wait_id, response, wait_for_boundary=True)
            )
        )

    def submit_resume(
        self,
        ref: InvocationRef,
        wait_id: str,
        response: object,
    ) -> InvocationSubmission:
        return self._run(
            self._resume(ref, wait_id, response, wait_for_boundary=False)
        )

    async def asubmit_resume(
        self,
        ref: InvocationRef,
        wait_id: str,
        response: object,
    ) -> InvocationSubmission:
        return await self._await(
            self._submit(
                self._resume(ref, wait_id, response, wait_for_boundary=False)
            )
        )

    def cancel(
        self,
        ref: InvocationRef,
        reason: str | None = None,
    ) -> InvocationResult:
        return self._run(self._cancel(ref, reason))

    async def acancel(
        self,
        ref: InvocationRef,
        reason: str | None = None,
    ) -> InvocationResult:
        return await self._await(self._submit(self._cancel(ref, reason)))

    def recover(
        self, ref: InvocationRef
    ) -> InvocationResult:
        return self._run(self._recover(ref))

    async def arecover(
        self, ref: InvocationRef
    ) -> InvocationResult:
        return await self._await(self._submit(self._recover(ref)))

    # ------------------------------------------------------------------
    # Checkpoints and Child relationships

    def load_checkpoint(
        self, checkpoint: SessionCheckpoint | AppCheckpoint
    ) -> CheckpointLoadResult:
        return self._run(self._load_checkpoint(checkpoint))

    async def aload_checkpoint(
        self, checkpoint: SessionCheckpoint | AppCheckpoint
    ) -> CheckpointLoadResult:
        return await self._await(self._submit(self._load_checkpoint(checkpoint)))

    def child_invocations(
        self, parent: InvocationRef
    ) -> tuple[InvocationRef, ...]:
        return self._run(self._list_child_invocations(parent))

    async def achild_invocations(
        self, parent: InvocationRef
    ) -> tuple[InvocationRef, ...]:
        return await self._await(self._submit(self._list_child_invocations(parent)))

    def resident_invocations(self) -> tuple[InvocationRef, ...]:
        """Return a stable snapshot of Invocations currently resident in Core."""

        return self._run(self._resident_invocations())

    async def aresident_invocations(self) -> tuple[InvocationRef, ...]:
        """Asynchronously return Invocations currently resident in Core."""

        return await self._await(self._submit(self._resident_invocations()))

    def unload_session(
        self, ref: InvocationRef, *, capture_checkpoint: bool = False
    ) -> SessionCheckpoint | None:
        """Release one quiescent Session, optionally returning its checkpoint."""

        return self._run(self._unload_session(ref, capture_checkpoint=capture_checkpoint))

    async def aunload_session(
        self, ref: InvocationRef, *, capture_checkpoint: bool = False
    ) -> SessionCheckpoint | None:
        """Asynchronously release a Session, optionally returning its checkpoint."""

        return await self._await(self._submit(
            self._unload_session(ref, capture_checkpoint=capture_checkpoint)))

    # ------------------------------------------------------------------
    # Lifecycle

    def close(
        self, timeout: float | None = 30.0, *, capture_checkpoint: bool = False
    ) -> AppCheckpoint | None:
        """Close once; the first caller selects optional checkpoint capture.

        Concurrent and repeated callers receive the same result, including None.
        Capture cannot be enabled retroactively after a default close.
        """
        _close_timeout(timeout)
        # A timeout detaches this caller; it never cancels the shared close
        # operation after quiescence may already have changed transient state.
        return self._begin_close(capture_checkpoint=capture_checkpoint).result(timeout=timeout)

    async def aclose(
        self, timeout: float | None = 30.0, *, capture_checkpoint: bool = False
    ) -> AppCheckpoint | None:
        """Asynchronous close with the same first-caller capture policy as close."""
        _close_timeout(timeout)
        # Closing belongs to the App, not to any one caller.  Shielding the
        # cross-thread waiter lets a cancelled caller detach without aborting
        # the one shared close operation used by every concurrent caller.
        waiter = asyncio.shield(
            self._runtime_loop.wait(self._begin_close(capture_checkpoint=capture_checkpoint))
        )
        return (
            await waiter
            if timeout is None
            else await asyncio.wait_for(waiter, timeout)
        )

    def _begin_close(self, *, capture_checkpoint: bool = False) -> Future[AppCheckpoint | None]:
        """Start exactly one process-local close operation and share its result."""

        with self._close_lock:
            if self._closed:
                completed: Future[AppCheckpoint | None] = Future()
                completed.set_result(self._closed_checkpoint)
                return completed
            if self._close_future is not None:
                return self._close_future
            future: Future[AppCheckpoint | None] = Future()
            self._close_future = future
            self._closing = True
            threading.Thread(
                target=self._complete_close,
                args=(future, capture_checkpoint),
                name="autoagent-close",
                daemon=True,
            ).start()
            return future

    def _complete_close(
        self,
        future: Future[AppCheckpoint | None],
        capture_checkpoint: bool,
    ) -> None:
        """Coordinate Runtime-loop quiescence outside every caller loop."""

        try:
            # Quiesce and settle even a preloaded repository; capture is optional.
            checkpoint = self._runtime_loop.run(
                self._close(capture_checkpoint=capture_checkpoint))
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

    async def _start_attached_resume_stream(
        self,
        ref: InvocationRef,
        wait_id: str,
        response: object,
    ) -> tuple[str, AttachedStream]:
        ref = self._control_ref(ref)
        session_id = ref.session_id
        if session_id in self._attached_streams:
            raise RuntimeTransitionError(
                "INVOCATION_STREAM_ATTACHED",
                "Invocation already has an attached stream.",
            )
        channel = AttachedStream()
        self._attached_streams[session_id] = channel

        async def run() -> None:
            error: BaseException | None = None
            try:
                result = await self._resume(
                    ref,
                    wait_id,
                    response,
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
                if self._attached_streams.get(session_id) is channel:
                    self._attached_streams.pop(session_id, None)
                self._attached_stream_tasks.pop(session_id, None)

        task = asyncio.create_task(run())
        self._attached_stream_tasks[session_id] = task
        return session_id, channel

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
        if session_id in self._recovering:
            raise RuntimeTransitionError('INVOCATION_STILL_LIVE', 'Graph recovery is in progress.')
        current_channel = self._attached_streams.get(session_id)
        if current_channel is not None and current_channel is not attached_channel:
            raise RuntimeTransitionError(
                "INVOCATION_STREAM_ATTACHED",
                "Session already has an attached Invocation stream.",
            )
        if self._parent_plan(session_id) is not None:
            raise RuntimeTransitionError(
                "SESSION_OWNED_BY_CHILD",
                "A Child Session cannot be replaced through root invocation admission.",
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
        # Preserve prompt admission rejection while an old Child is awaiting ACK.
        initial = self._repository.state(session_id)
        if initial.session is not None and session_context is not None:
            raise RuntimeTransitionError(
                "SESSION_CONTEXT_ALREADY_OPEN",
                "session_context can only be provided when opening a new Session.")
        current = initial.invocation
        if current is not None and not current.terminal:
            raise RuntimeTransitionError(
                "SESSION_INVOCATION_ACTIVE", "Session already has an active Invocation.")
        if current is not None and current.terminal and any(
            self._task_runtime.is_live(child_id)
            for child_id in self._descendant_sessions(session_id)
        ):
            raise RuntimeTransitionError(
                "SESSION_CHILDREN_ACTIVE",
                "A Session cannot replace its Invocation while Children are settling.",
            )
        if self._result_leases.get(session_id, 0):
            raise RuntimeTransitionError(
                "SESSION_RESULT_PENDING", "Session still owns an undelivered result.")
        invocation_id: str | None = None
        leased = False
        try:
            async with self._graph_gate(session_id):
                if session_id in self._recovering:
                    raise RuntimeTransitionError(
                        "INVOCATION_STILL_LIVE", "Graph recovery is in progress.")
                state = self._repository.state(session_id)
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
                    for child_id in old_child_sessions:
                        if not self._task_runtime.is_live(child_id):
                            await self._settle_runtime_commits((child_id,))
                    active_children = [
                        child_id
                        for child_id in old_child_sessions
                        if (
                            (child := self._repository.state(child_id).invocation) is None
                            or not child.terminal
                            or self._task_runtime.is_live(child_id)
                        )
                    ]
                    if active_children:
                        raise RuntimeTransitionError(
                            "SESSION_CHILDREN_ACTIVE",
                            "A Session cannot replace its Invocation while spawned "
                            "Children are active or still settling.",
                        )
                    retired_invocation_ids = (
                        state.invocation.id,
                        *(
                            child.id
                            for child_session_id in old_child_sessions
                            if (
                                child := self._repository.state(child_session_id).invocation
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
                leased = True

                if state.invocation is not None:
                    # A terminal graph may still own captured Child Events whose
                    # Host append failed after the public parent boundary (notably
                    # for spawned Children).  Confirm every old Event before root
                    # replacement; otherwise retiring Child State would silently
                    # drain the only recoverable copy of unacknowledged progress.
                    previous_graph = (session_id, *old_child_sessions)
                    await self._settle_runtime_commits(previous_graph)
                    # A Child task whose terminal Event export failed is no longer
                    # live, but its parent phase can still be ``accepted``.  Once
                    # the retry above makes every terminal Child durable, converge
                    # those orphaned settle tails from leaves to Root.  Live tasks
                    # were rejected before this point so this cannot race their own
                    # ``_settle_child`` calls.
                    for child_session_id in reversed(old_child_sessions):
                        child = self._repository.state(child_session_id).invocation
                        if child is not None and child.terminal:
                            await self._settle_child(child_session_id, child.id)
                    if not self._child_graph_settled(
                        session_id,
                        old_child_sessions,
                    ):
                        raise RuntimeTransitionError(
                            "SESSION_CHILDREN_ACTIVE",
                            "A Session cannot replace its Invocation before every "
                            "Child plan reaches its terminal phase.",
                        )
                if state.session is None:
                    await self._emit(session_id, None, SessionOpened(session_context or {}))
                invocation_id = str(uuid4())
                if retired_invocation_ids:
                    self._admission_retirements[session_id] = (
                        invocation_id, old_child_sessions, retired_invocation_ids)
                try:
                    await self._emit(
                        session_id,
                        invocation_id,
                        InvocationStarted(
                            compiled.workflow_id,
                            compiled.workflow_revision_id,
                            entry,
                            value,
                        ),
                    )
                finally:
                    current = self._repository.state(session_id).invocation
                    if current is not None and current.id == invocation_id:
                        self._finish_admission_retirement(session_id)
                ref = InvocationRef(
                    session_id=session_id,
                    invocation_id=invocation_id,
                    workflow_id=compiled.workflow_id,
                    workflow_revision_id=compiled.workflow_revision_id,
                )
                task = self._start_drive(compiled, session_id, invocation_id, None, None)
            if not wait_for_boundary:
                return InvocationSubmission(ref)
            await task
            return await self._result(ref)
        except asyncio.CancelledError:
            current = self._repository.state(session_id).invocation
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
                    InvocationRef(
                        session_id=session_id,
                        invocation_id=invocation_id,
                        workflow_id=current.workflow_id,
                        workflow_revision_id=current.workflow_revision_id,
                    )
                )
            if not self._closing:
                if (
                    invocation_id is not None
                    and current is not None
                    and current.id == invocation_id
                    and not current.terminal
                ):
                    await self._cancel_graph(
                        InvocationRef(
                            session_id=session_id,
                            invocation_id=invocation_id,
                            workflow_id=current.workflow_id,
                            workflow_revision_id=current.workflow_revision_id,
                        ),
                        "Invocation caller cancelled.",
                    )
                elif current is None:
                    await self._discard_incomplete_session(session_id)
            raise
        except BaseException:
            raise
        finally:
            if leased:
                self._release_result_lease(session_id)

    async def _resume(
        self,
        ref: InvocationRef,
        wait_id: str,
        response: object,
        *,
        wait_for_boundary: bool,
        attached_channel: AttachedStream | None = None,
    ) -> InvocationResult | InvocationSubmission:
        ref = self._control_ref(ref)
        state = self._state_for_ref(ref, active=True)
        root = self._root_session_id(ref.session_id)
        if root in self._recovering and ref.session_id not in self._recovering[root]:
            raise RuntimeTransitionError(
                "INVOCATION_STILL_LIVE", "Session recovery admission is in progress.")
        current_channel = self._attached_streams.get(ref.session_id)
        if attached_channel is None:
            self._ensure_attached_result_boundary_delivered(ref.session_id)
        elif current_channel is not attached_channel:
            raise RuntimeTransitionError(
                "INVOCATION_STREAM_ATTACHED",
                "Runtime graph already has an attached Invocation stream.",
            )
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
                return InvocationSubmission(ref)
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

    async def _status(
        self, ref: InvocationRef
    ) -> InvocationResult:
        ref = self._control_ref(ref)
        self._state_for_ref(ref)
        root = self._root_session_id(ref.session_id)
        self._acquire_result_lease(root)
        try:
            return await self._result(ref)
        finally:
            self._release_result_lease(root)

    async def _join(
        self,
        ref: InvocationRef,
        timeout: float | None,
    ) -> InvocationResult:
        ref = self._control_ref(ref)
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
        self,
        ref: InvocationRef,
        reason: str | None,
    ) -> InvocationResult:
        ref = self._control_ref(ref)
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
        root = self._root_session_id(ref.session_id)
        async with self._graph_gate(root):
            target = ref.session_id
            session_ids = (target, *self._descendant_sessions(target))
            await self._settle_runtime_commits(session_ids)
            root_state = self._state_for_ref(ref)
            root_invocation = root_state.invocation
            if root_invocation is not None and not root_invocation.terminal:
                await self._emit(
                    target, root_invocation.id, InvocationCancelled(reason)
                )
            for session_id in session_ids[1:]:
                invocation = self._repository.state(session_id).invocation
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
            invocation = self._repository.state(session_id).invocation
            if invocation is not None and invocation.terminal:
                await self._settle_child(session_id, invocation.id)
        target_invocation = self._repository.state(target).invocation
        if target_invocation is not None and target_invocation.terminal:
            await self._settle_child(target, target_invocation.id)

    async def _recover(self, ref):
        ref = self._control_ref(ref)
        root = self._root_session_id(ref.session_id)
        gate = self._graph_gates.get(root)
        if root in self._recovering or (gate is not None and not gate.idle):
            raise RuntimeTransitionError(
                "INVOCATION_STILL_LIVE", "Graph admission or recovery is in progress.")
        self._recovering[root] = set()
        try:
            return await self._recover_reserved(ref)
        finally:
            self._recovering.pop(root, None)

    async def _recover_reserved(
        self, ref: InvocationRef
    ) -> InvocationResult:
        ref = self._control_ref(ref)
        self._state_for_ref(ref)
        root = self._root_session_id(ref.session_id)
        target_path = {ref.session_id}
        current_session_id = ref.session_id
        while current_session_id != root:
            current = self._repository.state(current_session_id).invocation
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
        current = self._repository.state(ref.session_id).invocation
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
        state = self._repository.state(session_id)
        invocation = state.invocation
        if invocation is None:
            return
        await self._accept_recovered_child_invocations(session_id, invocation.id)
        state = self._repository.state(session_id)
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
            if self._repository.state(unit.session_id).invocation is not None
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
                child = self._repository.state(child_session_id).invocation
                if isinstance(outcome, asyncio.CancelledError) and (
                    child is not None and child.terminal
                ):
                    # Await fail-fast may deliberately cancel a sibling drive
                    # while the graph recovery coordinator is awaiting it.
                    continue
                raise outcome
        state = self._repository.state(session_id)
        invocation = state.invocation
        assert invocation is not None
        if invocation.terminal:
            await self._settle_child(session_id, invocation.id)
            return
        admitted = self._recovering.get(self._root_session_id(session_id))
        if admitted is not None:
            # The handshake/preflight is complete. External Waits may resume
            # while recovery is still awaiting other running graph branches.
            admitted.add(session_id)
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

        parent = self._repository.state(parent_session_id).invocation
        assert parent is not None and parent.id == parent_invocation_id
        candidates = tuple(
            (creation_id, unit.unit_index, unit.session_id)
            for creation_id, plan in parent.child_plans.items()
            for unit in plan.units
            if unit.phase == "opened"
        )
        for creation_id, unit_index, child_session_id in candidates:
            current = self._repository.state(parent_session_id).invocation
            assert current is not None and current.id == parent_invocation_id
            plan = current.child_plans.get(creation_id)
            if plan is None or unit_index >= len(plan.units):
                continue
            unit = plan.units[unit_index]
            if unit.phase != "opened":
                continue
            child = self._repository.state(child_session_id).invocation
            if child is None:
                continue
            await self._emit_child_transition(
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
        for occurrence in invocation.scheduler.occurrences.values():
            if occurrence.status != "running":
                continue
            node = workflow.node(occurrence.node_id)
            if occurrence.execution.fault is not None or any(
                call.occurrence_id == occurrence.id and call.status == "failed"
                and call.error is not None and call.error.type != "CancelledError"
                for call in invocation.scheduler.operator_calls.values()
            ):
                continue
            unknown_calls = [call for call in invocation.scheduler.operator_calls.values()
                if call.occurrence_id == occurrence.id and (call.status in {"running", "lost"}
                    or (call.status == "failed" and call.error is not None and call.error.type == "CancelledError"))]
            stages = occurrence.execution.completed_stages
            child_hooks_pending = isinstance(node.executable, WorkflowIR) and (
                (node.map is not None and node.map.aggregate is not None and "aggregated" not in stages)
                or (node.output_binding is not None and "output_bound" not in stages)
                or (any(edge.condition is not None for edge in workflow.outgoing(node.id)) and "routing_resolved" not in stages)
            )
            if not unknown_calls and not child_hooks_pending:
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

    async def _resident_invocations(self) -> tuple[InvocationRef, ...]:
        refs: list[InvocationRef] = []
        for session_id in sorted(self._repository.session_ids()):
            invocation = self._repository.state(session_id).invocation
            if invocation is not None:
                refs.append(self._ref_for_invocation(session_id, invocation))
        return tuple(refs)

    async def _unload_session(
        self, ref: InvocationRef, *, capture_checkpoint: bool = False
    ) -> SessionCheckpoint | None:
        ref = self._control_ref(ref)
        state = self._state_for_ref(ref)
        root = self._root_session_id(ref.session_id)
        async with self._graph_gate(root):
            state = self._state_for_ref(ref)
            related = self._resident_related_sessions(ref.session_id)
            allowed = {"waiting", "completed", "failed", "cancelled"}
            for session_id in related:
                invocation = self._repository.state(session_id).invocation
                if (
                    invocation is None
                    or invocation.status not in allowed
                    or self._task_runtime.is_live(session_id)
                ):
                    raise RuntimeTransitionError(
                        "RELATED_INVOCATION_NOT_UNLOADABLE",
                        "Every resident parent and Child Invocation must be "
                        "waiting or terminal before one Session can unload.",
                    )
            related_roots = {
                self._root_session_id(session_id) for session_id in related
            }
            if any(session_id in self._attached_streams for session_id in related) or any(
                self._result_leases.get(root_id, 0) for root_id in related_roots
            ):
                raise RuntimeTransitionError(
                    "INVOCATION_RESULT_PENDING",
                    "An attached stream or active result reader must finish before "
                    "unloading a Session.",
                )
            if capture_checkpoint:
                checkpoint = await self._capture_checkpoint_locked(ref.session_id)
            else:
                await self._settle_runtime_commits((ref.session_id,))
                checkpoint = None
            invocation = state.invocation
            assert invocation is not None
            self._repository.discard_states((ref.session_id,))
            self._task_runtime.release_wake_event(ref.session_id)
            self._retire_session_coordination(ref.session_id)
            self._result_leases.pop(ref.session_id, None)
            self._user_event_journal.discard(invocation.id)
            self._user_event_sink_errors.pop(invocation.id, None)
            self._child_capacities = {
                key: value
                for key, value in self._child_capacities.items()
                if key[0] != ref.session_id
            }
            resident = set(self._repository.session_ids())
            self._child_owners = {
                child: owner
                for child, owner in self._child_owners.items()
                if child in resident or owner[0] in resident
            }
            return checkpoint

    async def _load_checkpoint(
        self, checkpoint: SessionCheckpoint | AppCheckpoint
    ) -> CheckpointLoadResult:
        checkpoints = (
            checkpoint.sessions
            if isinstance(checkpoint, AppCheckpoint)
            else (checkpoint,)
            if isinstance(checkpoint, SessionCheckpoint)
            else None
        )
        if checkpoints is None:
            raise TypeError("checkpoint must be SessionCheckpoint or AppCheckpoint.")
        if not checkpoints:
            return CheckpointLoadResult(())
        all_states = {item.session_id: item.state for item in checkpoints}
        if len(all_states) != len(checkpoints):
            raise RuntimeTransitionError(
                "CHECKPOINT_SESSION_DUPLICATE",
                "A Session can appear only once in one checkpoint load.",
            )
        existing_ids = set(self._repository.session_ids())
        for session_id, state in all_states.items():
            if session_id in existing_ids and self._repository.state(session_id) != state:
                raise RuntimeTransitionError(
                    "CHECKPOINT_SESSION_CONFLICT",
                    "Checkpoint conflicts with the current Runtime Session.",
                )
        combined = {
            session_id: self._repository.state(session_id)
            for session_id in existing_ids
        }
        combined.update(all_states)
        self._validate_checkpoint_relationships(combined)
        affected_roots = {
            self._root_session_id(session_id)
            if session_id in existing_ids or session_id in self._child_owners
            else session_id
            for session_id in all_states
        }
        if any(
            self._task_runtime.is_live(session_id)
            or session_id in self._attached_streams
            for session_id in all_states
        ) or any(
            self._result_leases.get(root, 0)
            or root in self._recovering
            or (root in self._graph_gates and not self._graph_gates[root].idle)
            for root in affected_roots
        ):
            raise RuntimeTransitionError(
                "CHECKPOINT_SESSION_LIVE",
                "Checkpoint loading cannot replace a live Runtime Session.",
            )
        self._repository.install_states(all_states)
        self._rebuild_child_owners()
        refs = tuple(
            InvocationRef(
                session_id=session_id,
                invocation_id=state.invocation.id,
                workflow_id=state.invocation.workflow_id,
                workflow_revision_id=state.invocation.workflow_revision_id,
            )
            for session_id, state in sorted(all_states.items())
        )
        return CheckpointLoadResult(refs)

    @staticmethod
    def _validate_checkpoint_relationships(
        states: Mapping[str, RuntimeState],
    ) -> None:
        """Validate durable parent claims without requiring Workflow code."""

        claims: dict[str, tuple[str, str, int, str, str, str]] = {}
        for parent_session_id, state in states.items():
            parent = state.invocation
            if parent is None:
                continue
            for creation_id, plan in parent.child_plans.items():
                for unit in plan.units:
                    claim = (
                        parent_session_id,
                        creation_id,
                        unit.unit_index,
                        unit.invocation_id,
                        plan.workflow_id,
                        plan.workflow_revision_id,
                    )
                    previous = claims.get(unit.session_id)
                    if previous is not None and previous != claim:
                        raise RuntimeTransitionError(
                            "CHECKPOINT_CHILD_OWNERSHIP_CONFLICT",
                            "One Child Session is claimed by multiple parents.",
                        )
                    claims[unit.session_id] = claim
                    child_state = states.get(unit.session_id)
                    child = (
                        child_state.invocation if child_state is not None else None
                    )
                    if child is not None and (
                        child.id != unit.invocation_id
                        or child.workflow_id != plan.workflow_id
                        or child.workflow_revision_id != plan.workflow_revision_id
                        or child.input != unit.input
                    ):
                        raise RuntimeTransitionError(
                            "CHECKPOINT_CHILD_IDENTITY_MISMATCH",
                            "Child Session checkpoint does not match its parent plan.",
                        )

    async def _list_child_invocations(
        self, parent: InvocationRef
    ) -> tuple[InvocationRef, ...]:
        parent = self._control_ref(parent)
        parent_state = self._state_for_ref(parent)
        invocation = parent_state.invocation
        assert invocation is not None
        handles: list[InvocationRef] = []
        for unit in (
            unit
            for plan in invocation.child_plans.values()
            for unit in plan.units
        ):
            child = self._repository.state(unit.session_id).invocation
            if child is None or child.id != unit.invocation_id:
                continue
            handles.append(
                InvocationRef(
                    session_id=unit.session_id,
                    invocation_id=unit.invocation_id,
                    workflow_id=child.workflow_id,
                    workflow_revision_id=child.workflow_revision_id,
                )
            )
        return tuple(handles)

    async def _close(self, *, capture_checkpoint: bool = False) -> AppCheckpoint | None:
        return await self._close_operation(capture_checkpoint=capture_checkpoint)

    async def _close_operation(self, *, capture_checkpoint: bool = False) -> AppCheckpoint | None:
        """Quiesce owners and settle Events, optionally capturing resident Sessions."""

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
        for session_id in self._repository.session_ids():
            settled = await self._repository.settle(session_id)
            if isinstance(settled, RuntimeEvent):
                self._publish_ownership(settled)
            self._finish_admission_retirement(session_id)
        await self._discard_incomplete_sessions()
        if not capture_checkpoint:
            return None
        session_ids = self._repository.session_ids()
        checkpoints = tuple(
            [await self._capture_checkpoint(session_id) for session_id in session_ids]
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
            current = self._repository.state(session_id).invocation
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
        child_invocation = self._repository.state(session_id).invocation
        if child_invocation is None or not child_invocation.terminal:
            return
        parent = self._repository.state(parent_session_id).invocation
        if parent is None:
            return
        plan = parent.child_plans.get(creation_id)
        if plan is None:
            return
        unit = plan.units[unit_index]
        if unit.phase == "accepted":
            await self._ensure_child_durable(session_id)
            await self._emit_child_transition(
                parent_session_id,
                parent.id,
                ChildInvocationPhaseChanged(creation_id, unit_index, "terminal"),
            )
            parent = self._repository.state(parent_session_id).invocation
            assert parent is not None
            plan = parent.child_plans[creation_id]
        if plan.mode != "await":
            return
        occurrence = parent.scheduler.occurrences.get(plan.parent_occurrence_id)
        if occurrence is None or occurrence.status != "waiting":
            return
        if self._repository.has_failed_child(plan):
            # Once an awaited parent has suspended, a terminal failure is
            # enough to decide the whole Map.  Converge every remaining unit
            # before waking the parent so its failure boundary is recoverable
            # and no sibling remains indefinitely waiting.
            await self._workflow_executor.converge_failed_child_plan(
                parent_session_id,
                parent.id,
                creation_id,
            )
            parent = self._repository.state(parent_session_id).invocation
            assert parent is not None
            plan = parent.child_plans[creation_id]
        if self._repository.execution_index(parent_session_id).child_remaining[creation_id]:
            return
        await self._emit_child_transition(
            parent_session_id,
            parent.id,
            ChildAwaitReady(creation_id, plan.parent_occurrence_id),
        )
        parent_state = self._repository.state(parent_session_id)
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
        root = self._root_session_id(root_session_id)
        async with self._graph_gate(root):
            descendants = self._descendant_sessions(root_session_id)
            if not descendants:
                return
            await self._settle_runtime_commits(descendants)
            for session_id in descendants:
                invocation = self._repository.state(session_id).invocation
                if invocation is not None and not invocation.terminal:
                    await self._emit(session_id, invocation.id, InvocationCancelled(reason))
            tasks = [self._task_runtime.task(session_id) for session_id in descendants]
            for task in tasks:
                if task is not None:
                    task.cancel()
        # Drive finalizers may publish parent markers; never join under the barrier.
        if any(task is not None for task in tasks):
            await asyncio.gather(
                *(task for task in tasks if task is not None), return_exceptions=True,
            )
        for session_id in reversed(descendants):
            invocation = self._repository.state(session_id).invocation
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
        parent_state = self._repository.state(parent_session_id)
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
    ) -> RuntimeEvent:
        root = self._root_session_id(session_id)
        async with self._graph_gate(root).shared():
            async with self._session_transition_lock(session_id):
                return await self._emit_locked(session_id, invocation_id, payload)

    async def _emit_locked(self, session_id, invocation_id, payload):
        """Plan against ACKed State while holding graph admission and the Session lane."""
        settled = await self._repository.settle(session_id)
        if isinstance(settled, RuntimeEvent):
            self._publish_ownership(settled)
            self._finish_admission_retirement(session_id)
            if settled.payload == payload and settled.invocation_id == invocation_id:
                return settled
        state = self._repository.state(session_id)
        graph_delta = None
        if isinstance(payload, RecoveryApplied):
            payload = RecoveryApplied(
                tuple(item.id for item in state.invocation.scheduler.occurrences.values() if item.status == "running"),
                tuple(item.id for item in state.invocation.scheduler.operator_calls.values() if item.status == "running"),
            )
        if isinstance(payload, InvocationStarted):
            workflow = self._workflows[payload.workflow_revision_id]
            initial = InvocationState(invocation_id, payload.workflow_id, payload.workflow_revision_id,
                payload.entry_node_id, "running", payload.input, {}, started_at_us=0)
            graph_delta = self._scheduler.initialize(workflow, replace(state, invocation=initial))
        elif isinstance(payload, (NodeCompleted, NodeFailed)):
            workflow = self._workflow_for_state(state)
            occurrence = state.invocation.scheduler.occurrences[payload.occurrence_id]
            source_status = "complete" if isinstance(payload, NodeCompleted) else "error"
            selected = frozenset(
                [item.edge_id for item in occurrence.execution.routing if item.selected
                 and occurrence.execution.routing_source_status == source_status]
                + [edge.id for edge in workflow.outgoing(occurrence.node_id)
                   if edge.on == source_status and edge.condition is None])
            planning_options = {}
            if type(self._scheduler) is Scheduler and hasattr(self._repository, 'execution_index'):
                planning_options['_execution_index'] = self._repository.execution_index(session_id)
            if isinstance(payload, NodeCompleted):
                graph_delta = self._scheduler.complete(workflow, state, payload.occurrence_id,
                    payload.output, selected_edge_ids=selected, **planning_options)
            else:
                graph_delta = self._scheduler.fail(workflow, state, payload.occurrence_id,
                    payload.error, selected_edge_ids=selected, **planning_options)
        transition = await self._repository.commit(
            session_id=session_id, invocation_id=invocation_id, payload=payload,
            occurred_at_us=self._clock_us(), scheduler_delta=graph_delta,
            **({'output_node_ids': workflow.exit_node_ids}
               if isinstance(payload, (NodeCompleted, NodeFailed)) else {}),
        )
        self._publish_ownership(transition)
        await self._settle_runtime_commits((session_id,))
        return transition

    def _publish_ownership(self, event):
        if isinstance(event.payload, ChildInvocationPlanned):
            for unit in event.payload.units:
                self._child_owners[unit.child_session_id] = (
                    event.session_id, event.payload.creation_id, unit.unit_index, unit.child_invocation_id)

    async def _emit_executor_event(self, session_id, invocation_id, payload):
        if isinstance(payload, (ChildInvocationPhaseChanged, ChildAwaitReady)):
            return await self._emit_child_transition(session_id, invocation_id, payload)
        return await self._emit(session_id, invocation_id, payload)

    async def _emit_child_transition(self, session_id, invocation_id, payload):
        """Recheck idempotent internal phase requests inside the parent Session lane."""
        root = self._root_session_id(session_id)
        async with self._graph_gate(root).shared():
            async with self._session_transition_lock(session_id):
                settled = await self._repository.settle(session_id)
                if isinstance(settled, RuntimeEvent):
                    self._publish_ownership(settled)
                parent = self._repository.state(session_id).invocation
                if parent is None or parent.id != invocation_id:
                    return None
                plan = parent.child_plans.get(payload.creation_id)
                if plan is None:
                    return None
                if isinstance(payload, ChildInvocationPhaseChanged):
                    ranks = {'planned': 0, 'opened': 1, 'accepted': 2, 'terminal': 3}
                    if ranks[plan.units[payload.unit_index].phase] >= ranks[payload.phase]:
                        return None
                else:
                    occurrence = parent.scheduler.occurrences.get(plan.parent_occurrence_id)
                    if parent.terminal or occurrence is None or occurrence.status != 'waiting':
                        return None
                    if any(unit.phase != 'terminal' for unit in plan.units):
                        return None
                return await self._emit_locked(session_id, invocation_id, payload)

    @asynccontextmanager
    async def _child_admission(self, parent_session_id, parent_invocation_id):
        """Keep opening and task acceptance on one side of an exclusive graph cut."""
        root = self._root_session_id(parent_session_id)
        async with self._graph_gate(root).shared():
            parent = self._repository.state(parent_session_id).invocation
            if parent is None or parent.id != parent_invocation_id or parent.status in {'failed', 'cancelled'}:
                raise asyncio.CancelledError()
            yield

    async def _ensure_child_durable(self, session_id: str) -> None:
        root = self._root_session_id(session_id)
        async with self._graph_gate(root).shared():
            async with self._session_transition_lock(session_id):
                await self._settle_runtime_commits((session_id,))

    async def _emit_user(
        self,
        session_id: str,
        invocation_id: str,
        kind: str,
        payload: object,
        occurrence_id: str | None = None,
    ) -> UserEvent:
        channel = self._attached_streams.get(session_id)
        created: UserEvent | None = None

        async def commit() -> InvocationUpdate:
            nonlocal created
            created = self._user_event_journal.emit(
                session_id=session_id,
                invocation_id=invocation_id,
                kind=kind,
                payload=payload,
                occurrence_id=occurrence_id,
                occurred_at_us=self._clock_us(),
            )
            sink = self._user_event_sink
            if (
                sink is not None
                and invocation_id not in self._user_event_sink_errors
            ):
                try:
                    await sink.append_user_event(created)
                except asyncio.CancelledError:
                    raise
                except BaseException as error:
                    # User Events do not change canonical Runtime State.
                    # Keep Workflow progress valid, but stop the stream at the
                    # first rejected sequence so later persistence cannot mask
                    # a User Event sequence gap.
                    self._user_event_sink_errors[invocation_id] = error
                    warnings.warn(
                        "User Event sink rejected delivery; canonical "
                        "Workflow execution continues but persisted User Events "
                        "are incomplete.",
                        RuntimeWarning,
                        stacklevel=2,
                    )
            self._user_event_journal.drain(invocation_id)
            return InvocationUpdate(created)

        if channel is None:
            await commit()
        else:
            await channel.publish_async_created(commit)
        assert created is not None
        return created

    async def _capture_checkpoint(
        self, session_id: str
    ) -> SessionCheckpoint:
        root = self._root_session_id(session_id)
        async with self._graph_gate(root):
            return await self._capture_checkpoint_locked(session_id)

    async def _capture_checkpoint_locked(
        self, session_id: str
    ) -> SessionCheckpoint:
        await self._settle_runtime_commits((session_id,))
        checkpoint = self._repository.capture_checkpoint(
            session_id, captured_at_us=self._clock_us()
        )
        await self._settle_runtime_commits((checkpoint.session_id,))
        return checkpoint

    def _graph_gate(self, root_session_id: str) -> GraphGate:
        gate = self._graph_gates.get(root_session_id)
        if gate is None:
            gate = self._graph_gates[root_session_id] = GraphGate()
        return gate

    def _session_transition_lock(self, session_id):
        lock = self._session_transition_locks.get(session_id)
        if lock is None:
            lock = self._session_transition_locks[session_id] = asyncio.Lock()
        return lock

    def _retire_session_coordination(self, session_id):
        gate = self._graph_gates.get(session_id)
        if gate is not None:
            def discard():
                if self._graph_gates.get(session_id) is gate:
                    self._graph_gates.pop(session_id, None)
            gate.retire(discard)
        lock = self._session_transition_locks.get(session_id)
        if lock is not None and not lock.locked():
            self._session_transition_locks.pop(session_id, None)

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
        current = self._repository.state(ref.session_id).invocation
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

    async def _settle_runtime_commits(self, session_ids: tuple[str, ...]) -> None:
        for session_id in dict.fromkeys(session_ids):
            settled = await self._repository.settle(session_id)
            if isinstance(settled, RuntimeEvent):
                self._publish_ownership(settled)
            self._finish_admission_retirement(session_id)

    async def _result(self, ref: InvocationRef) -> InvocationResult:
        root = self._root_session_id(ref.session_id)
        async with self._graph_gate(root).shared(), self._session_transition_lock(ref.session_id):
            # Order this result behind publishers in its own Session.
            state = self._state_for_ref(ref)
            invocation = state.invocation
            assert invocation is not None
            waits = tuple(
                InvocationWait(item.id, thaw(item.request))
                for item in invocation.scheduler.waits.values()
                if item.status == "waiting"
            )
            output = thaw(invocation.output)
            workflow = self._workflows.get(invocation.workflow_revision_id)
            if (
                invocation.status == "completed"
                and workflow is not None
                and len(workflow.exit_node_ids) == 1
            ):
                contract = workflow.node(workflow.exit_node_ids[0]).output_contract
                if (
                    contract is not None
                    and _contains_invocation_ref(contract.annotation)
                ):
                    output = contract.restore(output)
            return InvocationResult(
                ref=ref,
                status=cast(InvocationStatus, invocation.status),
                output=output,
                error=invocation.error,
                waits=waits,
            )

    # ------------------------------------------------------------------
    # State graph helpers

    def _finish_admission_retirement(self, session_id: str) -> None:
        retirement = self._admission_retirements.get(session_id)
        if retirement is None:
            return
        invocation_id, children, old_invocations = retirement
        current = self._repository.state(session_id).invocation
        if current is not None and current.id == invocation_id:
            self._retire_invocation_graph(session_id, children, old_invocations)
            del self._admission_retirements[session_id]

    def _retire_invocation_graph(
        self,
        root_session_id: str,
        child_session_ids: tuple[str, ...],
        invocation_ids: tuple[str, ...],
    ) -> None:
        """Release the superseded Invocation graph after root replacement."""

        if child_session_ids:
            self._repository.discard_states(child_session_ids)
            for child_session_id in child_session_ids:
                self._child_owners.pop(child_session_id, None)
                self._retire_session_coordination(child_session_id)
            retired_sessions = {root_session_id, *child_session_ids}
            self._child_capacities = {
                key: capacity
                for key, capacity in self._child_capacities.items()
                if key[0] not in retired_sessions
            }
        for invocation_id in invocation_ids:
            self._user_event_journal.discard(invocation_id)

    def _control_ref(
        self, ref: InvocationRef
    ) -> InvocationRef:
        if not isinstance(ref, InvocationRef):
            raise TypeError("Control operations require an InvocationRef.")
        return ref

    def _state_for_ref(
        self, ref: InvocationRef, *, active: bool = False
    ) -> RuntimeState:
        if not isinstance(ref, InvocationRef):
            raise TypeError("Control operations require an InvocationRef.")
        state = self._repository.state(ref.session_id)
        invocation = state.invocation
        if (
            invocation is None
            or invocation.id != ref.invocation_id
            or invocation.workflow_id != ref.workflow_id
            or invocation.workflow_revision_id != ref.workflow_revision_id
        ):
            raise RuntimeTransitionError(
                "INVOCATION_REF_STALE",
                "InvocationRef does not identify the Session's current Invocation.",
            )
        if active and invocation.status not in {"running", "waiting"}:
            raise RuntimeTransitionError(
                "INVOCATION_NOT_RUNNING", "Invocation is not running."
            )
        return state

    @staticmethod
    def _ref_for_invocation(
        session_id: str, invocation: InvocationState
    ) -> InvocationRef:
        return InvocationRef(
            session_id=session_id,
            invocation_id=invocation.id,
            workflow_id=invocation.workflow_id,
            workflow_revision_id=invocation.workflow_revision_id,
        )

    def _resident_related_sessions(self, session_id: str) -> tuple[str, ...]:
        """Return the resident component connected by durable Child ownership."""

        resident = set(self._repository.session_ids())
        adjacency: dict[str, set[str]] = {}
        for child_session_id, owner in self._child_owners.items():
            parent_session_id = owner[0]
            adjacency.setdefault(parent_session_id, set()).add(child_session_id)
            adjacency.setdefault(child_session_id, set()).add(parent_session_id)
        found: list[str] = []
        pending = [session_id]
        seen: set[str] = set()
        while pending:
            current = pending.pop()
            if current in seen:
                continue
            seen.add(current)
            if current in resident:
                found.append(current)
            pending.extend(adjacency.get(current, ()))
        return tuple(sorted(found))

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
        parent = self._repository.state(parent_session_id).invocation
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
        for parent_session_id in self._repository.session_ids():
            parent = self._repository.state(parent_session_id).invocation
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
            state = self._repository.state(current)
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
            invocation = self._repository.state(session_id).invocation
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

    def _child_graph_settled(
        self,
        root_session_id: str,
        descendant_session_ids: tuple[str, ...],
    ) -> bool:
        """Return whether a terminal graph has no unfinished Child plan phase."""

        for session_id in (root_session_id, *descendant_session_ids):
            invocation = self._repository.state(session_id).invocation
            if invocation is None:
                return False
            if session_id != root_session_id and not invocation.terminal:
                return False
            if any(
                unit.phase != "terminal"
                for plan in invocation.child_plans.values()
                for unit in plan.units
            ):
                return False
        return True

    def _root_session_ids(self) -> tuple[str, ...]:
        sessions = set(self._repository.session_ids())
        children: set[str] = set()
        for session_id in sessions:
            invocation = self._repository.state(session_id).invocation
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
            for session_id in self._repository.session_ids()
            if self._repository.state(session_id).invocation is None
        )
        if not incomplete:
            return
        for session_id in incomplete:
            await self._discard_incomplete_session(session_id)

    async def _discard_incomplete_session(self, session_id: str) -> None:
        state = self._repository.state(session_id)
        if state.session is None or state.invocation is not None:
            return
        await self._settle_runtime_commits((session_id,))
        self._repository.discard_states((session_id,))
        self._child_owners.pop(session_id, None)
        self._retire_session_coordination(session_id)

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


def _contains_invocation_ref(annotation: object) -> bool:
    if annotation is InvocationRef:
        return True
    return any(_contains_invocation_ref(item) for item in get_args(annotation))


def _close_timeout(value: float | None) -> None:
    if value is None:
        return
    try:
        finite = math.isfinite(value)
    except (TypeError, OverflowError):
        finite = False
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not finite
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

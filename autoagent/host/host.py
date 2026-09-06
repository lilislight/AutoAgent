"""Lifecycle assembly for a configured V2 Workflow project."""

from __future__ import annotations

import asyncio
import contextvars
import math
import threading
import warnings
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from concurrent.futures import Future as ThreadFuture
from contextlib import contextmanager
from pathlib import Path
from types import MappingProxyType
from typing import Iterator, TypeVar, cast

from autoagent.core.app import (
    AppCheckpoint,
    AutoAgentApp,
    CheckpointLoadResult,
    InvocationRef,
    InvocationResult,
    InvocationStream,
    InvocationSubmission,
    StreamItem,
)
from autoagent.core.compiler import WorkflowCompiler, WorkflowDefinitionSnapshot
from autoagent.core.operators import Operator
from autoagent.core.runtime import SessionCheckpoint
from autoagent.core.workflow import (
    Capability,
    Workflow,
    WorkflowIR,
)
from autoagent.hosting import SQLiteRuntimeStore
from autoagent.hosting._worker import await_thread_future, run_in_daemon

from .environment import load_project_environment
from .errors import HostOperationError
from .loader import LoadedProject, ProjectLoader
from .manifest import resolve_manifest_path
from .settings import HostSettings, _host_settings_from_environment
from .sinks import (
    HostRuntimeEventSink,
    RecoverySource,
    WorkflowDefinitionSink,
    create_runtime_event_sink,
)


_T = TypeVar("_T")


class AutoAgentHost:
    """Own one configured App, its project definitions, and its Event sink.

    ``from_project`` is the normal construction path.  The explicit constructor
    exists so embedders can assemble already-validated components without
    introducing another global registry.
    """

    def __init__(
        self,
        *,
        project: LoadedProject,
        settings: HostSettings,
        app: AutoAgentApp,
        event_sink: HostRuntimeEventSink | None,
        environment: Mapping[str, str] | None = None,
    ) -> None:
        self.project = project
        self.settings = settings
        self.app = app
        self.event_sink = event_sink
        self.environment: Mapping[str, str] = MappingProxyType(
            dict(environment or {})
        )
        self._lifecycle = threading.Lock()
        self._definition_lock = threading.Lock()
        self._closing = False
        self._closed = False
        self._active_restores = 0
        self._closed_checkpoint: AppCheckpoint | None = None
        self._close_future: ThreadFuture[AppCheckpoint] | None = None
        self._restore_drain_future: ThreadFuture[None] | None = None

    @classmethod
    def from_project(
        cls,
        path: str | Path | None = None,
        *,
        env_file: str | Path | None = None,
        use_env_file: bool = True,
        environ: Mapping[str, str] | None = None,
        loader: ProjectLoader | None = None,
    ) -> AutoAgentHost:
        """Load, configure, and register every Workflow as one Host unit."""

        if _in_async_context():
            raise HostOperationError(
                "HOST_SYNC_API_IN_ASYNC_CONTEXT",
                "Use await AutoAgentHost.afrom_project(...) from an "
                "asynchronous context.",
            )

        manifest_path = resolve_manifest_path(path)
        environment = load_project_environment(
            manifest_path.parent,
            env_file=env_file,
            use_env_file=use_env_file,
            environ=environ,
        )
        settings = _host_settings_from_environment(
            manifest_path.parent,
            environment,
        )
        project = (loader or ProjectLoader()).load(
            manifest_path,
            environment=environment,
        )
        sink = create_runtime_event_sink(settings)
        app: AutoAgentApp | None = None
        try:
            app = AutoAgentApp(
                max_operator_concurrency=settings.max_operator_concurrency,
                max_node_executions_per_invocation=(
                    settings.max_node_executions_per_invocation
                ),
                runtime_event_sink=sink,
                user_event_sink=sink,
            )
            workflows = _register_project_workflows(app, project)
            if isinstance(sink, WorkflowDefinitionSink):
                for revision_id in sorted(workflows):
                    sink.save_workflow(
                        app.workflow_definition_snapshot(revision_id)
                    )
        except BaseException as error:
            _cleanup_failed_startup(app, sink, error)
            raise
        return cls(
            project=project,
            settings=settings,
            app=app,
            event_sink=sink,
            environment=environment,
        )

    @classmethod
    async def afrom_project(
        cls,
        path: str | Path | None = None,
        *,
        env_file: str | Path | None = None,
        use_env_file: bool = True,
        environ: Mapping[str, str] | None = None,
        loader: ProjectLoader | None = None,
    ) -> AutoAgentHost:
        """Assemble one Host without blocking the caller's event loop.

        Project imports, SQLite initialization, and remote Workflow definition
        publication all run on a dedicated daemon thread.  If the caller is
        cancelled during startup, a successfully constructed Host is closed on
        another daemon thread as soon as construction finishes.
        """

        future: ThreadFuture[AutoAgentHost] = ThreadFuture()
        context = contextvars.copy_context()

        def construct() -> None:
            try:
                host = cls.from_project(
                    path,
                    env_file=env_file,
                    use_env_file=use_env_file,
                    environ=environ,
                    loader=loader,
                )
            except BaseException as error:
                if not future.done():
                    future.set_exception(error)
            else:
                if not future.done():
                    future.set_result(host)

        thread = threading.Thread(
            target=lambda: context.run(construct),
            name="autoagent-host-start",
            daemon=True,
        )
        thread.start()
        try:
            return await await_thread_future(future, cancel_future=False)
        except asyncio.CancelledError:
            future.add_done_callback(_close_cancelled_host_start)
            raise

    @property
    def runtime_store(self) -> SQLiteRuntimeStore | None:
        """Return the query/recovery Store when SQLite mode is configured."""

        return (
            self.event_sink
            if isinstance(self.event_sink, SQLiteRuntimeStore)
            else None
        )

    @property
    def recovery_source(self) -> RecoverySource | None:
        """Return the configured recovery capability, when one is available."""

        sink = self.event_sink
        return sink if isinstance(sink, RecoverySource) else None

    def workflow_definition_snapshot(
        self,
        workflow_id_or_revision_id: str,
    ) -> WorkflowDefinitionSnapshot:
        """Return a registered portable Workflow definition."""

        self._ensure_open()
        return self.app.workflow_definition_snapshot(workflow_id_or_revision_id)

    @property
    def user_event_sink_errors(self) -> Mapping[str, BaseException]:
        """Return independent User Event delivery failures by Invocation id."""

        self._ensure_open()
        return self.app.user_event_sink_errors

    def register_workflow(self, workflow: Workflow) -> WorkflowIR:
        """Compile, publish, and register one additional Workflow closure."""

        if _in_async_context():
            raise HostOperationError(
                "HOST_SYNC_API_IN_ASYNC_CONTEXT",
                "Use await host.aregister_workflow(...) from an asynchronous "
                "context.",
            )
        return self._register_workflow(workflow)

    async def aregister_workflow(self, workflow: Workflow) -> WorkflowIR:
        """Register a Workflow without blocking the caller's event loop."""

        return await run_in_daemon(
            lambda: self._register_workflow(workflow),
            name="autoagent-host-register-workflow",
        )

    def register_capability(self, capability: Capability) -> Capability:
        """Bind one additional Capability contract to the Host App."""

        self._ensure_open()
        return self.app.register_capability(capability)

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
        """Register one runtime implementation for a project Capability."""

        self._ensure_open()
        return self.app.register_operator(
            operator,
            capability_id=capability_id,
            operator_id=operator_id,
            priority=priority,
            enabled=enabled,
            default=default,
        )

    def set_operator_enabled(self, operator_id: str, enabled: bool) -> None:
        """Enable or disable one registered Capability implementation."""

        self._ensure_open()
        self.app.set_operator_enabled(operator_id, enabled)

    def invoke(
        self,
        workflow_id: str,
        value: object,
        *,
        session_id: str | None = None,
        session_context: dict[str, object] | None = None,
        entry_node_id: str | None = None,
    ) -> InvocationResult:
        """Invoke one registered project Workflow to its next stable boundary."""

        self._prepare_workflow_call(workflow_id)
        return self.app.invoke(
            workflow_id,
            value,
            session_id=session_id,
            session_context=session_context,
            entry_node_id=entry_node_id,
        )

    async def ainvoke(
        self,
        workflow_id: str,
        value: object,
        *,
        session_id: str | None = None,
        session_context: dict[str, object] | None = None,
        entry_node_id: str | None = None,
    ) -> InvocationResult:
        """Asynchronously invoke one registered project Workflow."""

        self._prepare_workflow_call(workflow_id)
        return await self.app.ainvoke(
            workflow_id,
            value,
            session_id=session_id,
            session_context=session_context,
            entry_node_id=entry_node_id,
        )

    def submit_invoke(
        self,
        workflow_id: str,
        value: object,
        *,
        session_id: str | None = None,
        session_context: dict[str, object] | None = None,
        entry_node_id: str | None = None,
    ) -> InvocationSubmission:
        """Reliably admit one project Workflow and return without awaiting it."""

        self._prepare_workflow_call(workflow_id)
        return self.app.submit_invoke(
            workflow_id,
            value,
            session_id=session_id,
            session_context=session_context,
            entry_node_id=entry_node_id,
        )

    async def asubmit_invoke(
        self,
        workflow_id: str,
        value: object,
        *,
        session_id: str | None = None,
        session_context: dict[str, object] | None = None,
        entry_node_id: str | None = None,
    ) -> InvocationSubmission:
        """Asynchronously admit one project Workflow without awaiting it."""

        self._prepare_workflow_call(workflow_id)
        return await self.app.asubmit_invoke(
            workflow_id,
            value,
            session_id=session_id,
            session_context=session_context,
            entry_node_id=entry_node_id,
        )

    def stream(
        self,
        workflow_id: str,
        value: object,
        *,
        session_id: str | None = None,
        session_context: dict[str, object] | None = None,
        entry_node_id: str | None = None,
    ) -> InvocationStream[StreamItem]:
        """Return Core's synchronous, caller-backpressured execution stream."""

        self._prepare_workflow_call(workflow_id)
        return self.app.stream(
            workflow_id,
            value,
            session_id=session_id,
            session_context=session_context,
            entry_node_id=entry_node_id,
        )

    async def astream(
        self,
        workflow_id: str,
        value: object,
        *,
        session_id: str | None = None,
        session_context: dict[str, object] | None = None,
        entry_node_id: str | None = None,
    ) -> AsyncIterator[StreamItem]:
        """Yield Core's asynchronous, caller-backpressured execution stream."""

        self._prepare_workflow_call(workflow_id)
        async for item in self.app.astream(
            workflow_id,
            value,
            session_id=session_id,
            session_context=session_context,
            entry_node_id=entry_node_id,
        ):
            yield item

    def stream_resume(
        self,
        ref: InvocationRef,
        wait_id: str,
        response: object,
    ) -> InvocationStream[StreamItem]:
        """Resume a durable Wait and stream Core observations."""

        self._ensure_open()
        return self.app.stream_resume(ref, wait_id, response)

    async def astream_resume(
        self,
        ref: InvocationRef,
        wait_id: str,
        response: object,
    ) -> AsyncIterator[StreamItem]:
        """Asynchronously resume a Wait and stream Core observations."""

        self._ensure_open()
        async for item in self.app.astream_resume(ref, wait_id, response):
            yield item

    def status(
        self, ref: InvocationRef
    ) -> InvocationResult:
        """Read one exact Invocation without waiting."""

        self._ensure_open()
        return self.app.status(ref)

    async def astatus(
        self, ref: InvocationRef
    ) -> InvocationResult:
        """Asynchronously read one exact Invocation without waiting."""

        self._ensure_open()
        return await self.app.astatus(ref)

    def join(
        self,
        ref: InvocationRef,
        timeout: float | None = None,
    ) -> InvocationResult:
        """Join one submitted Invocation at its next stable boundary."""

        self._ensure_open()
        return self.app.join(ref, timeout)

    async def ajoin(
        self,
        ref: InvocationRef,
        timeout: float | None = None,
    ) -> InvocationResult:
        """Asynchronously join one submitted Invocation."""

        self._ensure_open()
        return await self.app.ajoin(ref, timeout)

    def resume(
        self,
        ref: InvocationRef,
        wait_id: str,
        response: object,
    ) -> InvocationResult:
        """Resume one durable Wait and await its next stable boundary."""

        self._ensure_open()
        return self.app.resume(ref, wait_id, response)

    async def aresume(
        self,
        ref: InvocationRef,
        wait_id: str,
        response: object,
    ) -> InvocationResult:
        """Asynchronously resume one durable Wait."""

        self._ensure_open()
        return await self.app.aresume(ref, wait_id, response)

    def submit_resume(
        self,
        ref: InvocationRef,
        wait_id: str,
        response: object,
    ) -> InvocationSubmission:
        """Admit one Wait response without awaiting the next boundary."""

        self._ensure_open()
        return self.app.submit_resume(ref, wait_id, response)

    async def asubmit_resume(
        self,
        ref: InvocationRef,
        wait_id: str,
        response: object,
    ) -> InvocationSubmission:
        """Asynchronously admit one Wait response."""

        self._ensure_open()
        return await self.app.asubmit_resume(ref, wait_id, response)

    def cancel(
        self,
        ref: InvocationRef,
        reason: str | None = None,
    ) -> InvocationResult:
        """Cancel one active Invocation."""

        self._ensure_open()
        return self.app.cancel(ref, reason)

    async def acancel(
        self,
        ref: InvocationRef,
        reason: str | None = None,
    ) -> InvocationResult:
        """Asynchronously cancel one active Invocation."""

        self._ensure_open()
        return await self.app.acancel(ref, reason)

    def load_checkpoint(
        self,
        checkpoint: SessionCheckpoint | AppCheckpoint,
    ) -> CheckpointLoadResult:
        """Load caller-supplied Runtime Session checkpoints."""

        self._ensure_open()
        return self.app.load_checkpoint(checkpoint)

    async def aload_checkpoint(
        self,
        checkpoint: SessionCheckpoint | AppCheckpoint,
    ) -> CheckpointLoadResult:
        """Asynchronously load Runtime Session checkpoints."""

        self._ensure_open()
        return await self.app.aload_checkpoint(checkpoint)

    def child_invocations(
        self,
        parent: InvocationRef,
    ) -> tuple[InvocationRef, ...]:
        """Return every durable Child InvocationRef owned by a parent."""

        self._ensure_open()
        return self.app.child_invocations(parent)

    async def achild_invocations(
        self,
        parent: InvocationRef,
    ) -> tuple[InvocationRef, ...]:
        """Asynchronously return a parent's durable Child InvocationRefs."""

        self._ensure_open()
        return await self.app.achild_invocations(parent)

    def restore_session(self, session_id: str) -> CheckpointLoadResult:
        """Rebuild one SQLite Runtime Session and load it into Core."""

        with self._restore_lease():
            store = self._restore_store()
            checkpoint = _run_store_call(
                lambda: _rebuild_session_checkpoint(store, session_id),
                async_method="arestore_session",
            )
            return self.app.load_checkpoint(checkpoint)

    async def arestore_session(
        self, session_id: str
    ) -> CheckpointLoadResult:
        """Asynchronously rebuild and load one SQLite Runtime Session."""

        with self._restore_lease():
            store = self._restore_store()
            checkpoint = await _rebuild_session_checkpoint(store, session_id)
            return await self.app.aload_checkpoint(checkpoint)

    def recover(
        self, ref: InvocationRef
    ) -> InvocationResult:
        """Recover one exact Invocation already loaded into this Host."""

        self._ensure_open()
        return self.app.recover(ref)

    async def arecover(
        self, ref: InvocationRef
    ) -> InvocationResult:
        """Asynchronously recover one exact loaded Invocation."""

        self._ensure_open()
        return await self.app.arecover(ref)

    def close(self, timeout: float | None = 30.0) -> AppCheckpoint:
        """Wait up to ``timeout`` for the one Host-owned close coordinator."""

        _close_timeout(timeout)
        with self._lifecycle:
            if _in_async_context() and self._active_restores:
                raise HostOperationError(
                    "HOST_SYNC_CLOSE_IN_ASYNC_CONTEXT",
                    "Use aclose() while an asynchronous restore is active.",
                )
        return self._begin_close().result(timeout=timeout)

    async def aclose(self, timeout: float | None = 30.0) -> AppCheckpoint:
        """Wait asynchronously while shutdown continues after timeout/cancel."""

        _close_timeout(timeout)
        waiter = asyncio.shield(
            await_thread_future(self._begin_close(), cancel_future=False)
        )
        return (
            await waiter
            if timeout is None
            else await asyncio.wait_for(waiter, timeout)
        )

    def __enter__(self) -> AutoAgentHost:
        return self

    def __exit__(
        self,
        _error_type: type[BaseException] | None,
        error: BaseException | None,
        _traceback: object,
    ) -> None:
        try:
            self.close()
        except BaseException as close_error:
            if error is None:
                raise
            error.add_note(f"AutoAgentHost close failed: {close_error}")

    async def __aenter__(self) -> AutoAgentHost:
        return self

    async def __aexit__(
        self,
        _error_type: type[BaseException] | None,
        error: BaseException | None,
        _traceback: object,
    ) -> None:
        try:
            await self.aclose()
        except BaseException as close_error:
            if error is None:
                raise
            error.add_note(f"AutoAgentHost close failed: {close_error}")

    def _prepare_workflow_call(self, workflow_id: str) -> None:
        self._ensure_open()
        _identity(workflow_id)

    def _register_workflow(self, workflow: Workflow) -> WorkflowIR:
        if not isinstance(workflow, Workflow):
            raise TypeError("workflow must be a Workflow.")
        with self._definition_lock:
            self._ensure_open()
            compiled = WorkflowCompiler().compile(workflow).require_workflow_ir()
            closure = _workflow_ir_closure(compiled)
            sink = self.event_sink
            if isinstance(sink, WorkflowDefinitionSink):
                for revision_id in sorted(closure):
                    sink.save_workflow(
                        WorkflowDefinitionSnapshot.from_workflow_ir(
                            closure[revision_id]
                        )
                    )
            # Publishing may block.  Recheck lifecycle before exposing the
            # executable closure to the App.
            self._ensure_open()
            registered = self.app.register_workflow(workflow)
            if (
                registered.workflow_revision_id != compiled.workflow_revision_id
                or registered.definition_hash != compiled.definition_hash
            ):
                raise HostOperationError(
                    "HOST_WORKFLOW_CHANGED_DURING_REGISTRATION",
                    "Workflow definition changed while it was being registered.",
                )
            return registered

    def _ensure_open(self) -> None:
        with self._lifecycle:
            if self._closed:
                raise HostOperationError("HOST_CLOSED", "AutoAgentHost is closed.")
            if self._closing:
                raise HostOperationError(
                    "HOST_CLOSING", "AutoAgentHost is closing."
                )

    def _begin_close(
        self,
    ) -> ThreadFuture[AppCheckpoint]:
        with self._lifecycle:
            if self._closed:
                assert self._closed_checkpoint is not None
                assert self._close_future is not None
                return self._close_future
            if self._close_future is not None:
                return self._close_future
            future: ThreadFuture[AppCheckpoint] = ThreadFuture()
            restores_drained: ThreadFuture[None] | None = None
            if self._active_restores:
                restores_drained = ThreadFuture()
            self._close_future = future
            self._restore_drain_future = restores_drained
            self._closing = True
            thread = threading.Thread(
                target=self._complete_close,
                args=(future, restores_drained),
                name="autoagent-host-close",
                daemon=True,
            )
            try:
                thread.start()
            except BaseException as error:
                self._close_future = None
                self._restore_drain_future = None
                self._closing = False
                future.set_exception(error)
                raise
            return future

    def _complete_close(
        self,
        future: ThreadFuture[AppCheckpoint],
        restores_drained: ThreadFuture[None] | None,
    ) -> None:
        try:
            if restores_drained is not None:
                restores_drained.result()
            checkpoint = self.app.close(timeout=None)
        except BaseException as error:
            self._finish_close_error(future, error)
            return
        try:
            if self.event_sink is not None:
                self.event_sink.close()
        except BaseException as error:
            failure = HostOperationError(
                "HOST_SINK_CLOSE_FAILED",
                "The Runtime Event sink failed while the Host was closing.",
            )
            failure.__cause__ = error
            self._finish_close_terminal_error(future, checkpoint, failure)
            return
        self._finish_close_success(future, checkpoint)

    def _finish_close_success(
        self,
        future: ThreadFuture[AppCheckpoint],
        checkpoint: AppCheckpoint,
    ) -> None:
        with self._lifecycle:
            self._closed_checkpoint = checkpoint
            self._closed = True
            self._closing = False
            self._restore_drain_future = None
        if not future.done():
            future.set_result(checkpoint)

    def _finish_close_error(
        self,
        future: ThreadFuture[AppCheckpoint],
        error: BaseException,
    ) -> None:
        with self._lifecycle:
            if self._close_future is future:
                self._close_future = None
            self._closing = False
            self._restore_drain_future = None
        if not future.done():
            future.set_exception(error)

    def _finish_close_terminal_error(
        self,
        future: ThreadFuture[AppCheckpoint],
        checkpoint: AppCheckpoint,
        error: BaseException,
    ) -> None:
        """Keep Host terminal when Core closed but sink cleanup reported failure."""

        with self._lifecycle:
            self._closed_checkpoint = checkpoint
            self._closed = True
            self._closing = False
            self._restore_drain_future = None
        error.add_note("AutoAgentApp closed successfully; the Host is terminal.")
        if not future.done():
            future.set_exception(error)

    @contextmanager
    def _restore_lease(self) -> Iterator[None]:
        with self._lifecycle:
            if self._closed:
                raise HostOperationError("HOST_CLOSED", "AutoAgentHost is closed.")
            if self._closing:
                raise HostOperationError(
                    "HOST_CLOSING", "AutoAgentHost is closing."
                )
            self._active_restores += 1
        try:
            yield
        finally:
            with self._lifecycle:
                self._active_restores -= 1
                if (
                    self._active_restores == 0
                    and self._restore_drain_future is not None
                    and not self._restore_drain_future.done()
                ):
                    self._restore_drain_future.set_result(None)

    def _restore_store(self) -> RecoverySource:
        store = self.recovery_source
        if store is None:
            raise HostOperationError(
                "HOST_RESTORE_UNAVAILABLE",
                "Session restore requires AUTOAGENT_RUNTIME_EVENT_SINK=sqlite.",
            )
        return store


def _close_cancelled_host_start(
    future: ThreadFuture[AutoAgentHost],
) -> None:
    """Close a Host whose asynchronous construction outlived its caller."""

    try:
        host = future.result()
    except BaseException:
        return

    def close() -> None:
        for _attempt in range(3):
            try:
                host.close(timeout=None)
            except BaseException:
                # App-close failures leave Host retryable.  Startup has not
                # exposed the App, so a short bounded retry is safe and avoids
                # leaking a Host on a transient lifecycle failure.
                continue
            return
        warnings.warn(
            "A Host abandoned by cancelled asynchronous startup could not be "
            "closed after three attempts; owned resources may remain active.",
            RuntimeWarning,
            stacklevel=2,
        )

    threading.Thread(
        target=close,
        name="autoagent-host-cancelled-start-close",
        daemon=True,
    ).start()


def _register_project_workflows(
    app: AutoAgentApp,
    project: LoadedProject,
) -> dict[str, WorkflowIR]:
    registered: dict[str, WorkflowIR] = {}
    for loaded in project.workflows:
        root = app.register_workflow(loaded.workflow)
        registered.update(_workflow_ir_closure(root))
    return registered


def _workflow_ir_closure(root: WorkflowIR) -> dict[str, WorkflowIR]:
    """Collect one compiled root and every independently executable Child IR."""

    workflows: dict[str, WorkflowIR] = {}
    pending = [root]
    while pending:
        current = pending.pop()
        existing = workflows.get(current.workflow_revision_id)
        if existing is not None:
            if existing.definition_hash != current.definition_hash:
                raise HostOperationError(
                    "HOST_WORKFLOW_REVISION_CONFLICT",
                    f"Workflow Revision {current.workflow_revision_id!r} was reused.",
                )
            continue
        workflows[current.workflow_revision_id] = current
        pending.extend(
            cast(WorkflowIR, node.executable)
            for node in reversed(current.nodes)
            if isinstance(node.executable, WorkflowIR)
        )
    return workflows


def _cleanup_failed_startup(
    app: AutoAgentApp | None,
    sink: HostRuntimeEventSink | None,
    original: BaseException,
) -> None:
    for label, close in (
        ("App", None if app is None else app.close),
        ("Runtime Event sink", None if sink is None else sink.close),
    ):
        if close is None:
            continue
        try:
            close()
        except BaseException as cleanup_error:
            original.add_note(f"{label} cleanup failed: {cleanup_error}")


async def _rebuild_session_checkpoint(
    store: RecoverySource,
    session_id: str,
) -> SessionCheckpoint:
    session_id = _identity(session_id)
    try:
        return await store.rebuild_checkpoint(session_id)
    except KeyError as error:
        raise HostOperationError(
            "HOST_SESSION_NOT_FOUND",
            f"Session {session_id!r} was not found.",
        ) from error


def _run_store_call(
    call: Callable[[], Awaitable[_T]],
    *,
    async_method: str,
) -> _T:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(call())
    raise HostOperationError(
        "HOST_SYNC_API_IN_ASYNC_CONTEXT",
        f"Use {async_method}() from an asynchronous context.",
    )


def _identity(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("identity cannot be empty.")
    return value


def _in_async_context() -> bool:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


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
        or value > threading.TIMEOUT_MAX
    ):
        raise ValueError(
            "timeout must be a non-negative finite number no greater than "
            "the platform timeout limit, or None."
        )


__all__ = ["AutoAgentHost"]

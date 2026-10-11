"""Root graph pause requests and admission at resumable Node boundaries."""
import asyncio
import math
from contextlib import asynccontextmanager

from ..errors import RuntimeTransitionError
from ..runtime.events import GraphPauseRequested, GraphPaused, GraphResumed
from ..runtime.state import ChildResult, validate_pause_boundary
from ..runtime.signals import validate_signal_workflow
from ..workflow.models import InvocationRef
from .models import InvocationResult


class PauseRuntime:
    def _graph_is_paused(self, sid):
        root = self._root_session_id(sid)
        inv = self._repository.state(root).invocation
        return root in self._pause_fences or (inv is not None and inv.control_state != "active")

    @asynccontextmanager
    async def _node_admission(self, sid):
        # The check, NodeStarted ACK and Task creation share admission. A pause
        # writer cannot cut between committing a start and owning its Task.
        async with self._graph_gate(self._root_session_id(sid)).shared():
            yield not self._graph_is_paused(sid)

    def pause(self, ref: InvocationRef, timeout: float | None = None) -> InvocationResult:
        """Pause the complete Root graph without cancelling in-flight Nodes."""
        return self._run(self._pause(ref, timeout))

    async def apause(self, ref: InvocationRef, timeout: float | None = None) -> InvocationResult:
        """Await pause; timeout or caller cancellation leaves the request active."""
        return await self._await(self._submit(self._pause(ref, timeout)))

    async def _pause(self, ref, timeout):
        if timeout is not None and (isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout < 0):
            raise ValueError("Pause timeout must be a non-negative number or None.")
        ref = self._control_ref(ref)
        self._state_for_ref(ref)
        root = ref.session_id
        self._acquire_result_lease(root)
        try:
            operation = self._pause_operations.get(root)
            if operation is None:
                operation = asyncio.create_task(self._pause_graph(ref))
                self._pause_operations[root] = operation
                def finished(task):
                    if self._pause_operations.get(root) is task:
                        self._pause_operations.pop(root, None)
                    if not task.cancelled():
                        task.exception()
                    self._notify_boundary(root)
                operation.add_done_callback(finished)
            try:
                await asyncio.wait_for(asyncio.shield(operation), timeout)
            except TimeoutError as error:
                raise RuntimeTransitionError("PAUSE_TIMEOUT", "Pause remains requested; an in-flight Node has not reached its boundary.") from error
            return await self._result(ref)
        finally:
            self._release_result_lease(root)

    async def _pause_graph(self, ref):
        root = ref.session_id
        self._graph_controls[root] = self._graph_controls.get(root, 0) + 1
        try:
            async with self._graph_gate(root):
                inv = self._state_for_ref(ref).invocation
                if root in self._recovering:
                    raise RuntimeTransitionError("INVOCATION_STILL_LIVE", "Graph recovery is in progress.")
                await self._settle_runtime_commits((root, *self._descendant_sessions(root)))
                inv = self._state_for_ref(ref).invocation
                if inv.terminal or inv.stopping or inv.control_state == "paused":
                    return
                # An append with a lost ACK must retain admission closure.
                # Child dispatch cannot join a parent append that may itself
                # be waiting for that Child to finish.
                self._pause_fences.add(root)
                if inv.control_state == "active":
                    await self._emit(root, inv.id, GraphPauseRequested())
                timer = self._wait_timers.pop(root, None)
                if timer is not None:
                    timer.cancel()
                for sid in (root, *self._descendant_sessions(root)):
                    if self._task_runtime.is_live(sid):
                        self._task_runtime.wake(sid)
            event = self._boundary_events.setdefault(root, asyncio.Event())
            while True:
                event.clear()
                async with self._graph_gate(root):
                    sessions = (root, *self._descendant_sessions(root))
                    for sid in sessions:
                        if sid in self._drive_errors:
                            raise self._drive_errors[sid]
                    live = any(self._task_runtime.is_live(sid) for sid in sessions)
                    if not live and self._graph_controls.get(root) == 1:
                        await self._settle_runtime_commits(sessions)
                        inv = self._state_for_ref(ref).invocation
                        if inv.terminal or inv.stopping:
                            return
                        self._validate_clean_pause(sessions)
                        await self._emit(root, inv.id, GraphPaused())
                        return
                # Never retain exclusive admission while waiting for Node
                # completion, child admission or cancellation finalizers.
                await event.wait()
        finally:
            self._graph_controls[root] -= 1
            if not self._graph_controls[root]:
                self._graph_controls.pop(root)
            self._notify_boundary(root)

    def _validate_clean_pause(self, sessions):
        for sid in sessions:
            inv = self._repository.state(sid).invocation
            if inv is None or isinstance(inv, ChildResult) or inv.terminal:
                continue
            try:
                validate_pause_boundary(inv)
            except ValueError as error:
                raise RuntimeTransitionError("GRAPH_NOT_PAUSABLE", "Unowned interrupted work requires crash recovery before pause.") from error

    async def _continue_paused_graph(self, root):
        # Validate the entire graph before enabling any child or resident wake.
        async with self._graph_gate(root):
            sessions = (root, *self._descendant_sessions(root))
            await self._settle_runtime_commits(sessions)
            self._validate_clean_pause(sessions)
            for sid in sessions:
                inv = self._repository.state(sid).invocation
                if inv is None or inv.terminal:
                    continue
                workflow = self._workflow_for_state(self._repository.state(sid))
                validate_signal_workflow(inv, workflow)
                for wait in inv.scheduler.waits.values():
                    if wait.status == "waiting" and wait.kind != "external":
                        self._validate_wait_target(sid, wait.kind, wait.request)
            inv = self._repository.state(root).invocation
            if inv.control_state != "active":
                await self._emit(root, inv.id, GraphResumed())


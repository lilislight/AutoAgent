"""Host-owned process identities; no PID-based reconnection or implicit replay."""

import asyncio
from dataclasses import dataclass, field
import threading
import time
from uuid import uuid4

from ..tools import ToolBusinessError
from .local import settle


@dataclass
class ManagedProcess:
    id: str
    session_id: str
    invocation_id: str | None
    scope: str
    command: tuple[str, ...]
    spawn: asyncio.Task
    handle: object = None
    cwd: str = ""
    started: float = field(default_factory=time.monotonic)

    async def stop(self):
        try:
            handle = self.handle or await asyncio.shield(self.spawn)
        except Exception:
            return
        self.handle = handle
        await handle.terminate()


class ProcessManager:
    """Live records survive Core unload, but never outlive their owning Host.

    A manager belongs to one async loop. Bounded records/logs are runtime-only;
    committed Tool results continue to use the ordinary event/history storage.
    """

    def __init__(self, *, max_running=64, max_records=128):
        if type(max_running) is not int or type(max_records) is not int or min(max_running, max_records) <= 0:
            raise ValueError("Process manager limits must be positive integers.")
        self.max_running, self.max_records = max_running, max_records
        self._records = {}
        self._guard = threading.RLock()
        self._loop = None
        self._closed = False

    def _check_loop(self):
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            raise RuntimeError("A ProcessManager belongs to one event loop.")

    @staticmethod
    def running(record):
        if record.handle is not None:
            return record.handle.state == "running"
        return not record.spawn.done() or not record.spawn.cancelled() and record.spawn.exception() is None

    def get(self, identity, session_id, scope):
        with self._guard:
            record = self._records.get(identity)
            if record is None or record.session_id != session_id or record.scope != scope:
                raise ToolBusinessError("Process is unavailable in this Session and Workspace.", code="process_unavailable")
            return record

    def records(self, session_id=None, scope=None):
        with self._guard:
            return tuple(record for record in self._records.values()
                if (session_id is None or record.session_id == session_id) and (scope is None or record.scope == scope))

    async def start(self, backend, spec, *, session_id, invocation_id, scope, session_limit, prepare=None):
        self._check_loop()
        with self._guard:
            if self._closed:
                raise ToolBusinessError("Execution service is closing.", code="execution_closed")
            running = [r for r in self._records.values() if self.running(r)]
            own = [r for r in running if r.session_id == session_id]
            if len(running) >= self.max_running or len(own) >= session_limit:
                usage = f"Session {len(own)}/{session_limit}; Host {len(running)}/{self.max_running}"
                handles = ", ".join(r.id for r in own[:8] if r.scope == scope)
                raise ToolBusinessError(f"Command was not started: running process limit reached ({usage}). "
                    + (f"Wait for or terminate existing handles: {handles}." if handles else
                       "Wait for capacity; use manage_process list to inspect this Workspace's handles."),
                    code="process_limit_exceeded")
            while len(self._records) >= self.max_records:
                finished = next((r for r in self._records.values() if not self.running(r)), None)
                if finished is None:
                    raise ToolBusinessError("Process record limit reached.", code="process_limit_exceeded")
                del self._records[finished.id]
            async def spawn():
                # Reserve capacity before creating logs/directories or starting
                # the backend. The task owns preparation through cancellation.
                prepared = prepare(spec) if prepare is not None else spec
                return await backend.spawn(prepared)
            record = ManagedProcess("proc_" + uuid4().hex, session_id, invocation_id, scope,
                spec.argv, asyncio.create_task(spawn()), cwd=spec.cwd)
            self._records[record.id] = record
        try:
            record.handle = await asyncio.shield(record.spawn)
            if self._closed:
                await record.stop()
                raise ToolBusinessError("Execution service closed during startup.", code="execution_closed")
            return record
        except asyncio.CancelledError:
            cleanup = asyncio.create_task(record.stop())
            await settle(cleanup)
            raise
        except BaseException:
            with self._guard:
                self._records.pop(record.id, None)
            raise

    async def terminate_invocations(self, invocation_ids):
        self._check_loop()
        await asyncio.gather(*(record.stop() for record in self.records()
            if record.invocation_id in invocation_ids and self.running(record)))

    async def aclose(self):
        self._check_loop()
        self._closed = True
        cleanup = asyncio.create_task(self._close_all())
        await settle(cleanup)

    async def _close_all(self):
        records = self.records()
        results = await asyncio.gather(*(record.stop() for record in records), return_exceptions=True)
        with self._guard:
            for record, result in zip(records, results):
                if not isinstance(result, BaseException):
                    self._records.pop(record.id, None)
        for result in results:
            if isinstance(result, BaseException):
                raise result


__all__ = ["ProcessManager"]

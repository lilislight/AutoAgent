"""Task-owned graph barrier: concurrent transitions, exclusive lifecycle cuts."""
import asyncio


class GraphGate:
    """Writer-preferring barrier; reentrancy belongs to a Task, never its children.

    Ordinary commits share admission and serialize separately per Session.
    Exclusive owners can publish their own commits, but must release before
    joining drives, whose finalizers may need shared admission.
    """

    def __init__(self):
        self._readers = {}
        self._owner = None
        self._depth = 0
        self._writers = 0
        self._waiting_readers = 0
        self._writer_lock = asyncio.Lock()
        self._changed = None
        self._retire = None
        self._shared = _SharedGate(self)

    def _notify(self):
        if self._changed is not None:
            self._changed.set()
            self._changed = None
        if self._retire is not None and self.idle:
            retire, self._retire = self._retire, None
            retire()

    async def _wait_for_change(self):
        if self._changed is None:
            self._changed = asyncio.Event()
        await self._changed.wait()

    @property
    def idle(self):
        return not (self._readers or self._owner or self._writers or self._waiting_readers)

    def retire(self, callback):
        self._retire = callback
        self._notify()

    def shared(self):
        return self._shared

    async def __aenter__(self):
        task = asyncio.current_task()
        if self._owner is task:
            self._depth += 1
            return self
        if task in self._readers:
            raise RuntimeError('Cannot upgrade a graph transition to exclusive control.')
        self._writers += 1
        acquired = False
        try:
            await self._writer_lock.acquire()
            acquired = True
            while self._readers:
                await self._wait_for_change()
            self._owner = task
            self._depth = 1
        except BaseException:
            if acquired:
                self._writer_lock.release()
            raise
        finally:
            self._writers -= 1
            self._notify()
        return self

    async def __aexit__(self, *exc):
        self._depth -= 1
        if not self._depth:
            self._owner = None
            self._writer_lock.release()
            self._notify()


class _SharedGate:
    """Reusable context manager; per-Task depths live on the gate itself."""

    __slots__ = ("gate",)

    def __init__(self, gate):
        self.gate = gate

    async def __aenter__(self):
        gate = self.gate
        task = asyncio.current_task()
        if gate._owner is task:
            return
        if task in gate._readers or (gate._owner is None and not gate._writers):
            gate._readers[task] = gate._readers.get(task, 0) + 1
            return
        gate._waiting_readers += 1
        try:
            while gate._owner is not None or gate._writers:
                await gate._wait_for_change()
            gate._readers[task] = 1
        finally:
            gate._waiting_readers -= 1
            gate._notify()

    async def __aexit__(self, *exc):
        gate = self.gate
        task = asyncio.current_task()
        if gate._owner is task:
            return
        depth = gate._readers[task] - 1
        if depth:
            gate._readers[task] = depth
        else:
            del gate._readers[task]
            if not gate._readers:
                gate._readers.clear()
                gate._notify()

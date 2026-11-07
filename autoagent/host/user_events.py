"""Bounded live User Event delivery; history remains in the existing database."""

import asyncio
from collections import deque
from contextlib import asynccontextmanager
from threading import Lock

from .models import HostError


class UserEventStreamOverflow(HostError):
    """The live consumer fell behind and must reload history/current state."""


_CLOSED = object()


class UserEventSubscription:
    def __init__(self, invocation_id, capacity, session_id=None):
        self.invocation_id = invocation_id
        self.session_id = session_id
        self.capacity = capacity
        self.pending = deque()
        self.loop = asyncio.get_running_loop()
        self.ready = asyncio.Event()
        self.lock = Lock()
        self.notified = False
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            with self.lock:
                if self.pending:
                    item = self.pending.popleft()
                    break
                if self.closed:
                    raise StopAsyncIteration
                self.ready.clear()
            await self.ready.wait()
        if item is _CLOSED:
            raise StopAsyncIteration
        if isinstance(item, UserEventStreamOverflow):
            raise item
        return item

    def close(self, error=None):
        with self.lock:
            if self.closed:
                return
            self._close(error)

    def _close(self, error=None):
        self.closed = True
        self.pending.clear()
        self.pending.append(error if error is not None else _CLOSED)
        self._notify()

    def _notify(self):
        if not self.notified:
            self.notified = True
            try:
                self.loop.call_soon_threadsafe(self._wake)
            except RuntimeError:
                # An abandoned caller loop must not turn a successfully saved
                # User Event into a persistent sink failure.
                self.closed = True
                self.pending.clear()

    def _wake(self):
        with self.lock:
            self.notified = False
        self.ready.set()

    def publish(self, event):
        with self.lock:
            if self.closed:
                return False
            if len(self.pending) >= self.capacity:
                self._close(UserEventStreamOverflow("Live User Event stream overflowed; reload history and current state."))
                return False
            self.pending.append(event)
            self._notify()
            return not self.closed


class UserEventHub:
    def __init__(self):
        self.subscriptions = set()
        self.lock = Lock()

    def add(self, subscription):
        with self.lock:
            self.subscriptions.add(subscription)

    def remove(self, subscription):
        with self.lock:
            self.subscriptions.discard(subscription)

    def publish(self, event):
        with self.lock:
            for subscription in tuple(self.subscriptions):
                if (subscription.invocation_id in (None, event.invocation_id)
                        and subscription.session_id in (None, event.session_id) and not subscription.publish(event)):
                    self.subscriptions.discard(subscription)

    def close(self):
        with self.lock:
            for subscription in self.subscriptions:
                subscription.close()
            self.subscriptions.clear()


class UserEvents:
    @asynccontextmanager
    async def subscribe_user_events(self, invocation_id=None, *, session_id=None, capacity=256):
        """Subscribe before starting work; transient observations have no replay.

        The subscription belongs to this Host process and caller loop. Slow
        consumers are closed explicitly rather than blocking Workflow execution
        or silently losing an interior text delta. Persistent events can be
        recovered through get_user_events; transient events require fresh state.
        """
        self._check_loop()
        if self._state != "ready" or self._read_only:
            raise HostError("Live User Events require an open writable Host.")
        if invocation_id is not None and (not isinstance(invocation_id, str) or not invocation_id.strip()):
            raise ValueError("Invocation ID must be nonempty or None.")
        if session_id is not None and (not isinstance(session_id, str) or not session_id.strip()):
            raise ValueError("Session ID must be nonempty or None.")
        if type(capacity) is not int or not 1 <= capacity <= 10000:
            raise ValueError("capacity must be between 1 and 10000.")
        subscription = UserEventSubscription(invocation_id, capacity, session_id)
        self._user_events.add(subscription)
        try:
            yield subscription
        finally:
            self._user_events.remove(subscription)
            subscription.close()

    async def get_user_events(self, invocation_id, *, after=0, limit=100):
        """Read persistent User Events without activating an execution."""
        if not isinstance(invocation_id, str) or not invocation_id.strip():
            raise ValueError("Invocation ID must be nonempty.")
        if type(after) is not int or after < 0:
            raise ValueError("after must be a nonnegative integer.")
        if type(limit) is not int or not 1 <= limit <= 10000:
            raise ValueError("limit must be between 1 and 10000.")
        return await self._perform(lambda: self._persistence.user_events(invocation_id, after=after, limit=limit))

    def _notice_user_event(self, event):
        # Each subscriber bounds its mailbox before crossing event-loop threads.
        # Wakeups coalesce, so a blocked caller loop cannot accumulate one
        # call_soon_threadsafe callback per model token.
        self._user_events.publish(event)

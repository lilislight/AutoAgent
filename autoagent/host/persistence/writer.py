"""Bounded ordered acceptance, batch commits, backpressure and fault handling."""

from __future__ import annotations

import asyncio
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import AsyncExitStack
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.exc import DBAPIError, OperationalError

from .codec import canonical, retained_objects
from .models import (
    CapacityError,
    ClosedError,
    ConflictError,
    PersistenceError,
    PersistenceHealth,
)
from .schema import meta
from .storage import Storage, prepare


@dataclass(slots=True)
class Entry:
    kind: str
    identity: str
    value: object
    binding: str | None
    anchor: bool
    ticket: int
    objects: dict
    done: asyncio.Future
    accepted_at: float


class Writer:
    """All mutable state and SQLAlchemy objects belong to this dedicated loop."""

    def __init__(self, url, config):
        self.config = config
        self.storage = Storage(url)
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(
            target=self._thread_main, name="autoagent-persistence", daemon=True
        )
        self._closed = False
        self._lifecycle_lock = threading.Lock()
        self._close_future = None
        self.thread.start()
        self._startup = asyncio.run_coroutine_threadsafe(self._start(), self.loop)

    def _thread_main(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()
        self.loop.run_until_complete(self.loop.shutdown_asyncgens())
        self.loop.close()

    async def _start(self):
        self.executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="autoagent-persistence-codec"
        )
        self.condition, self.admission, self.mutation = (
            asyncio.Condition(),
            asyncio.Lock(),
            asyncio.Lock(),
        )
        self.pending, self.objects = deque(), {}
        self.pending_count, self.retained = 0, 0
        self.accepted_ticket = self.durable_ticket = self.store_order = 0
        self.flush_ticket = 0
        self.state, self.fault = "open", None
        self.operator_bindings = {}
        self.operations = set()
        self.task = None
        try:
            await self.storage.open()
            async with self.storage.engine.connect() as conn:
                self.store_order = await conn.scalar(
                    select(meta.tables["persistence_meta"].c.last_store_order)
                )
            self.task = asyncio.create_task(self._worker())
        except BaseException:
            await self.storage.close()
            self.executor.shutdown(wait=True)
            self.state = "closed"
            raise

    async def run(self, function, *args, **kwargs):
        if self._closed:
            raise ClosedError("Persistence is closed.")
        await self.ready()
        with self._lifecycle_lock:
            if self._closed or self._close_future is not None:
                raise ClosedError("Persistence is closing or closed.")

            async def execute():
                task = asyncio.current_task()
                self.operations.add(task)
                try:
                    return await function(*args, **kwargs)
                finally:
                    self.operations.discard(task)

            future = asyncio.run_coroutine_threadsafe(execute(), self.loop)
        return await asyncio.wrap_future(future)

    async def ready(self):
        try:
            await asyncio.shield(asyncio.wrap_future(self._startup))
        except Exception:
            with self._lifecycle_lock:
                if not self._closed:
                    self._closed = True
                    self.loop.call_soon_threadsafe(self.loop.stop)
            await asyncio.to_thread(self.thread.join)
            raise

    def check_open(self):
        if self.fault is not None:
            raise PersistenceError("Persistence writer faulted.") from self.fault
        if self.state != "open":
            raise ClosedError("Persistence is closing or closed.")

    async def cpu(self, function, *args):
        return await self.loop.run_in_executor(self.executor, function, *args)

    async def accept(
        self,
        kind,
        identity,
        value,
        *,
        binding=None,
        anchor=False,
        durable=False,
        strict_binding=False,
    ):
        self.check_open()
        # Serial admission preserves order even when measuring large shared values.
        async with self.admission:
            self.check_open()
            existing = next(
                (e for e in self.pending if e.kind == kind and e.identity == identity),
                None,
            )
            if existing:
                previous_value = existing.value
                if (
                    previous_value is not value
                    and not await self.cpu(
                        lambda: (
                            canonical(previous_value.to_record())
                            == canonical(value.to_record())
                        )
                    )
                    or strict_binding
                    and existing.binding != binding
                ):
                    raise ConflictError("Pending immutable identity conflicts.")
                completion = existing.done
            else:
                async with AsyncExitStack() as locks:
                    # WAL/MVCC reads continue during a commit. Only SQLite's
                    # single in-memory connection requires mutation serialization.
                    if self.storage.memory:
                        await locks.enter_async_context(self.mutation)
                    await locks.enter_async_context(self.condition)
                    async with self.storage.engine.connect() as conn:
                        stored = await self.storage.source(conn, kind, identity)
                        if stored is not None:
                            if not await self.cpu(
                                lambda: (
                                    canonical(stored) == canonical(value.to_record())
                                )
                            ):
                                raise ConflictError(
                                    "Stored immutable identity conflicts."
                                )
                            if kind == "runtime_event":
                                row = await self.storage.row(
                                    conn, "runtime_events", event_id=identity
                                )
                                if (
                                    strict_binding
                                    and row["operator_definition_id"] != binding
                                ):
                                    raise ConflictError(
                                        "Historical Operator binding conflicts."
                                    )
                            return
                        if kind in ("runtime_event", "user_event"):
                            field = (
                                "session_id"
                                if kind == "runtime_event"
                                else "invocation_id"
                            )
                            stream = getattr(value, field)
                            same = [
                                e
                                for e in self.pending
                                if e.kind == kind and getattr(e.value, field) == stream
                            ]
                            if same:
                                latest = same[-1].value.sequence
                            elif kind == "runtime_event":
                                sess = await self.storage.row(
                                    conn, "sessions", session_id=stream
                                )
                                latest = (sess or {}).get("durable_sequence", 0)
                            else:
                                table = meta.tables["user_events"]
                                latest = (
                                    await conn.scalar(
                                        select(table.c.sequence)
                                        .where(table.c.invocation_id == stream)
                                        .order_by(table.c.sequence.desc())
                                        .limit(1)
                                    )
                                    or 0
                                )
                            if value.sequence != latest + 1:
                                raise ConflictError(
                                    f"Expected {field} sequence {latest + 1}, got {value.sequence}."
                                )
                            revision = (
                                getattr(value.payload, "workflow_revision_id", None)
                                if kind == "runtime_event"
                                else None
                            )
                            if revision and not await self.storage.row(
                                conn, "workflow_revisions", revision_id=revision
                            ):
                                raise ConflictError(
                                    "Workflow revision must be durably registered first."
                                )
                            if binding:
                                definition = await self.storage.row(
                                    conn, "operator_definitions", definition_id=binding
                                )
                                if (
                                    not definition
                                    or definition["operator_id"]
                                    != value.payload.operator_id
                                ):
                                    raise ConflictError(
                                        "Operator binding must match a durably registered implementation."
                                    )
                        elif kind == "checkpoint":
                            await self._validate_checkpoint(conn, value, anchor)
                objects = await self.cpu(retained_objects, value)
                if sum(size for _, size in objects.values()) > self.config.queue_bytes:
                    raise CapacityError(
                        "One record exceeds queue_bytes; raise the explicit budget."
                    )
                async with self.condition:
                    while True:
                        self.check_open()
                        added = sum(
                            size
                            for key, (_, size) in objects.items()
                            if key not in self.objects
                        )
                        if (
                            self.pending_count < self.config.queue_count
                            and self.retained + added <= self.config.queue_bytes
                        ):
                            break
                        await self.condition.wait()
                    self.accepted_ticket += 1
                    completion = self.loop.create_future()
                    completion.add_done_callback(
                        lambda f: f.exception() if not f.cancelled() else None
                    )
                    entry = Entry(
                        kind,
                        identity,
                        value,
                        binding,
                        anchor,
                        self.accepted_ticket,
                        objects,
                        completion,
                        time.monotonic(),
                    )
                    self.pending.append(entry)
                    self.pending_count += 1
                    if durable:
                        self.flush_ticket = entry.ticket
                    for key, (item, size) in objects.items():
                        previous = self.objects.get(key)
                        if previous:
                            self.objects[key] = (
                                previous[0],
                                previous[1],
                                previous[2] + 1,
                            )
                        else:
                            self.objects[key] = (item, size, 1)
                            self.retained += size
                    self.condition.notify_all()
        if durable or self.config.mode == "durable":
            await asyncio.shield(completion)

    async def _validate_checkpoint(self, conn, checkpoint, anchor):
        revisions = set()
        for member in checkpoint.sessions:
            session = await self.storage.row(
                conn, "sessions", session_id=member.session_id
            )
            events = [
                e
                for e in self.pending
                if e.kind == "runtime_event" and e.value.session_id == member.session_id
            ]
            latest = (
                events[-1].value.sequence
                if events
                else (session or {}).get("durable_sequence", 0)
            )
            if anchor and latest:
                raise ConflictError(
                    "Checkpoint import requires fresh Session histories."
                )
            if not anchor and member.sequence > latest:
                raise ConflictError("Checkpoint frontier is ahead of accepted Events.")
            if anchor and any(
                e.kind == "checkpoint"
                and e.anchor
                and any(s.session_id == member.session_id for s in e.value.sessions)
                for e in self.pending
            ):
                raise ConflictError("Session already has a pending imported anchor.")
            invocation = member.state.invocation
            if invocation:
                revisions.add(invocation.workflow_revision_id)
                revisions.update(
                    p.workflow_revision_id for p in invocation.child_plans.values()
                )
        for revision in revisions:
            if not await self.storage.row(
                conn, "workflow_revisions", revision_id=revision
            ):
                raise ConflictError(
                    "Checkpoint Workflow closure must be durably registered first."
                )

    async def flush(self):
        if self.fault:
            raise PersistenceError("Persistence writer faulted.") from self.fault
        async with self.condition:
            ticket = self.accepted_ticket
            self.flush_ticket = max(self.flush_ticket, ticket)
            self.condition.notify_all()
            await self.condition.wait_for(
                lambda: self.durable_ticket >= ticket or self.fault is not None
            )
        if self.fault:
            raise PersistenceError(
                "Flush did not reach the captured frontier."
            ) from self.fault

    async def _commit(self, prepared):
        for attempt in range(self.config.retry_attempts + 1):
            try:
                return await self.storage.write(prepared)
            except DBAPIError as exc:
                code = getattr(exc.orig, "sqlstate", None)
                temporary = (
                    (
                        self.storage.sqlite
                        and isinstance(exc, OperationalError)
                        and any(
                            word in str(exc.orig).lower() for word in ("locked", "busy")
                        )
                    )
                    or code in {"40001", "40P01", "08000", "08003", "08006", "57P01"}
                    or exc.connection_invalidated
                )
                if not temporary or attempt == self.config.retry_attempts:
                    raise
                await asyncio.sleep(self.config.retry_delay_ms / 1000 * 2**attempt)

    async def _worker(self):
        while True:
            async with self.condition:
                await self.condition.wait_for(
                    lambda: self.pending or self.state != "open"
                )
                if not self.pending:
                    return
                delay = (
                    self.pending[0].accepted_at
                    + (self.config.durable_batch_delay_ms if self.config.mode == "durable" else self.config.batch_delay_ms) / 1000
                    - time.monotonic()
                )
                if (
                    delay > 0
                    and len(self.pending) < self.config.batch_count
                    and self.retained < self.config.batch_bytes
                    and self.retained < self.config.queue_bytes * self.config.batch_queue_ratio
                    and self.pending_count < self.config.queue_count * self.config.batch_queue_ratio
                    and self.state == "open"
                    and self.pending[0].ticket > self.flush_ticket
                ):
                    try:
                        await asyncio.wait_for(self.condition.wait(), timeout=delay)
                    except TimeoutError:
                        pass
                    continue
                entries = list(self.pending)[: self.config.batch_count]
            prepared = None
            try:
                prepared = await self.cpu(prepare, entries, self.config)
                entries = entries[: len(prepared)]
                async with self.mutation:
                    order = await self._commit(prepared)
                    async with self.condition:
                        self.store_order = order
                        for entry in entries:
                            assert self.pending[0] is entry
                            self.pending.popleft()
                            self.pending_count -= 1
                            self.durable_ticket = entry.ticket
                            for key in entry.objects:
                                obj, retained, refs = self.objects[key]
                                if refs == 1:
                                    del self.objects[key]
                                    self.retained -= retained
                                else:
                                    self.objects[key] = (obj, retained, refs - 1)
                            entry.objects.clear()
                            entry.value = None
                            entry.done.set_result(None)
                        # Release all batch data before waking blocked producers.
                        prepared.clear()
                        entries.clear()
                        entry = obj = None
                        self.condition.notify_all()
            except Exception as exc:  # noqa: BLE001 -- wake all producers on writer fault
                async with self.condition:
                    self.fault, self.state = exc, "faulted"
                    for entry in self.pending:
                        if not entry.done.done():
                            entry.done.set_exception(
                                PersistenceError(
                                    "Batch failed; its ordered tail remains uncommitted."
                                )
                            )
                    self.condition.notify_all()
                return
            finally:
                prepared = entries = None

    async def health(self):
        return PersistenceHealth(
            self.state,
            self.pending_count,
            self.retained,
            self.accepted_ticket,
            self.durable_ticket,
            str(self.fault) if self.fault else None,
        )

    async def _shutdown(self):
        async with self.condition:
            if self.state == "open":
                self.state = "closing"
            self.condition.notify_all()
        if self.task:
            await self.task
        if self.operations:
            await asyncio.gather(*tuple(self.operations), return_exceptions=True)
        failure = (
            f"{type(self.fault).__name__}: {str(self.fault)[:1000]}"
            if self.fault
            else None
        )
        # Exception tracebacks may retain encoded batches. Drop them on close.
        self.fault = None
        await self.storage.close()
        self.executor.shutdown(wait=True)
        self.pending.clear()
        self.objects.clear()
        self.pending_count = self.retained = 0
        self.state = "closed"
        if failure:
            raise PersistenceError(
                "Close failed to persist the buffered tail: " + failure
            ) from None

    async def close(self):
        if self._closed:
            await asyncio.to_thread(self.thread.join)
            return
        await self.ready()
        with self._lifecycle_lock:
            if self._closed:
                return
            if self._close_future is None:
                self._close_future = asyncio.run_coroutine_threadsafe(
                    self._shutdown(), self.loop
                )
            future = self._close_future
        future.add_done_callback(self._finish_close)
        try:
            await asyncio.shield(asyncio.wrap_future(future))
        finally:
            if future.done():
                await asyncio.to_thread(self.thread.join)

    def _finish_close(self, _future):
        # Close owns cleanup even if every caller cancels its waiting Future.
        with self._lifecycle_lock:
            if not self._closed:
                self._closed = True
                self.loop.call_soon_threadsafe(self.loop.stop)

"""Bounded ordered acceptance, batch commits, backpressure and fault handling."""

from __future__ import annotations

import asyncio
import threading
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import AsyncExitStack
from dataclasses import dataclass
from itertools import islice

from sqlalchemy import select
from sqlalchemy.exc import DBAPIError, OperationalError

from autoagent.core.runtime._memory import retained_entries

from .codec import canonical
from .admission import Admission
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
    done: Future
    accepted_at: float


OBSERVABLE_EVENTS = frozenset({
    "invocation.completed", "invocation.failed", "invocation.cancelled", "invocation.settling",
    "node_occurrence.waiting", "graph.paused", "graph.pause_requested",
    "invocation.cancellation_requested", "child_invocation.phase_changed",
})


class Writer:
    """SQL belongs to one loop; the bounded queue has short shared critical sections."""

    def __init__(self, url, config, *, read_only=False):
        self.config = config
        self.read_only = read_only
        self.storage = Storage(
            url, read_only=read_only,
            graph_cache_bytes=config.workflow_graph_cache_bytes,
            metadata_cache_bytes=config.writer_metadata_cache_bytes,
            config=config,
        )
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(
            target=self._thread_main, name="autoagent-persistence", daemon=True
        )
        self._closed = False
        self.queue_lock = threading.RLock()
        self._lifecycle_lock = self.queue_lock
        self._wake_scheduled = False
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
            Admission(self.queue_lock),
            asyncio.Lock(),
        )
        self.pending, self.objects = deque(), {}
        self.pending_by_identity, self.pending_tails = {}, {}
        self.pending_count, self.retained = 0, 0
        self.accepted_ticket = self.durable_ticket = self.store_order = 0
        self.commit_revision = 0
        self.execution_watches = {}
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
                self.storage._store_order = self.store_order
            if not self.read_only:
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
            if self._startup.done():
                self._startup.result()
                return
            await asyncio.shield(asyncio.wrap_future(self._startup))
        except Exception:
            with self._lifecycle_lock:
                if not self._closed:
                    self._closed = True
                    self.loop.call_soon_threadsafe(self.loop.stop)
            await asyncio.to_thread(self.thread.join)
            raise

    def check_open(self):
        if self.read_only:
            raise PersistenceError("Persistence is read-only.")
        if self.fault is not None:
            raise PersistenceError("Persistence writer faulted.") from self.fault
        if self.state != "open":
            raise ClosedError("Persistence is closing or closed.")

    async def cpu(self, function, *args):
        return await self.loop.run_in_executor(self.executor, function, *args)

    async def _account(self, value):
        objects = {}
        for key, item, size in retained_entries((value,)):
            objects[key] = (item, size)
            # Small Events finish without an executor roundtrip. Large object
            # graphs yield to commits, control reads and shutdown regularly.
            if len(objects) % 512 == 0:
                await asyncio.sleep(0)
        return objects

    def try_accept(self, kind, identity, value, *, binding=None, strict_binding=False):
        """Accept cache-proven Events locally; a miss uses serialized SQL admission."""
        if self.config.mode != "buffered" or not self._startup.done():
            return None
        if self._startup.cancelled() or self._startup.exception() is not None:
            # ready() owns startup failure cleanup, including stopping the loop.
            return None
        with self.queue_lock:
            if self._close_future is not None:
                raise ClosedError("Persistence is closing or closed.")
            self.check_open()
            if self.admission.waiters:
                return None
            if kind == 'runtime_event' and value.event_name in ('operator_call.started', 'capability.resolved') and binding is None:
                binding = self.operator_bindings.get(value.payload.operator_id)
            existing = self.pending_by_identity.get((kind, identity))
            if existing is not None:
                if existing.value is value and (not strict_binding or existing.binding == binding):
                    return sum(size for _, size in existing.objects.values())
                return None
            if self.storage.identities.may_contain(kind, identity):
                return None
            stream_key = self._stream_key(kind, value)
            tail = self.pending_tails.get(stream_key)
            if tail is not None:
                latest = tail.value.sequence
            elif kind == "runtime_event":
                row = self.storage.metadata_cache.get(('sessions', (value.session_id,)))
                if row is None:
                    return None
                latest = row['durable_sequence']
            else:
                latest = self.storage.metadata_cache.get(('user_event_tail', (value.invocation_id,)))
                if latest is None:
                    return None
            if value.sequence != latest + 1:
                raise ConflictError(f"Expected stream sequence {latest + 1}, got {value.sequence}.")
            if kind == 'runtime_event':
                revision = getattr(value.payload, 'workflow_revision_id', None)
                if revision and self.storage.metadata_cache.get(('definition_header', ('workflow_revision', revision))) is None:
                    return None
                if binding:
                    definition = self.storage.metadata_cache.get(('definition_header', ('operator_definition', binding)))
                    if definition is None:
                        return None
                    if definition['operator_id'] != value.payload.operator_id:
                        raise ConflictError("Operator binding must match a durably registered implementation.")
            objects = {}
            for key, item, size in retained_entries((value,)):
                objects[key] = (item, size)
                # Large graphs use cooperative accounting on the Writer loop.
                if len(objects) > 512:
                    return None
            record_size = sum(size for _, size in objects.values())
            if record_size > self.config.queue_bytes:
                raise CapacityError("One record exceeds queue_bytes; raise the explicit budget.")
            if not self._fits(objects):
                return None
            self._enqueue(kind, identity, value, binding, False, objects, False)
            if not self._wake_scheduled:
                self._wake_scheduled = True
                self.loop.call_soon_threadsafe(self._wake)
            return record_size

    def _wake(self):
        with self.queue_lock:
            self._wake_scheduled = False
        async def notify():
            async with self.condition:
                self.condition.notify_all()
        asyncio.create_task(notify())

    def _fits(self, objects):
        added = sum(size for key, (_, size) in objects.items() if key not in self.objects)
        return self.pending_count < self.config.queue_count and self.retained + added <= self.config.queue_bytes

    def _enqueue(self, kind, identity, value, binding, anchor, objects, durable):
        """Caller owns queue_lock; retain ownership until the transaction commits."""
        self.accepted_ticket += 1
        entry = Entry(kind, identity, value, binding, anchor, self.accepted_ticket,
                      objects, Future(), time.monotonic())
        self.pending.append(entry)
        self.pending_by_identity[(kind, identity)] = entry
        stream_key = self._stream_key(kind, value)
        if stream_key is not None:
            self.pending_tails[stream_key] = entry
        self.pending_count += 1
        if durable:
            self.flush_ticket = entry.ticket
        for key, (item, size) in objects.items():
            previous = self.objects.get(key)
            if previous:
                self.objects[key] = (previous[0], previous[1], previous[2] + 1)
            else:
                self.objects[key] = (item, size, 1)
                self.retained += size
        return entry.done

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
            objects = await self._account(value)
            record_size = sum(size for _, size in objects.values())
            if record_size > self.config.queue_bytes:
                raise CapacityError(
                    "One record exceeds queue_bytes; raise the explicit budget."
                )
            existing = self.pending_by_identity.get((kind, identity))
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
                self.check_open()
                completion = existing.done
            else:
                stream_key = self._stream_key(kind, value)
                tail = self.pending_tails.get(stream_key)
                # Admission excludes new producers, but commits may remove the
                # tail during SQL awaits. Keep its accepted sequence as a value.
                pending_sequence = tail.value.sequence if tail else None
                tail = None
                checkpoint_frontiers = (
                    self._checkpoint_frontiers(value, anchor)
                    if kind == "checkpoint" else None
                )
                async with AsyncExitStack() as locks:
                    # WAL/MVCC reads continue during a commit. Only SQLite's
                    # single in-memory connection requires mutation serialization.
                    if self.storage.memory:
                        await locks.enter_async_context(self.mutation)
                    async with self.storage.engine.connect() as conn:
                        header = await self.storage.source_header(conn, kind, identity)
                        if header is not None:
                            stored = await self.storage.source(conn, kind, identity)
                            if not await self.cpu(
                                lambda: (
                                    canonical(stored) == canonical(value.to_record())
                                )
                            ):
                                raise ConflictError(
                                    "Stored immutable identity conflicts."
                                )
                            if kind == "runtime_event":
                                if (
                                    strict_binding
                                    and header["operator_definition_id"] != binding
                                ):
                                    raise ConflictError(
                                        "Historical Operator binding conflicts."
                                    )
                            self.check_open()
                            return record_size
                        if kind in ("runtime_event", "user_event"):
                            field = (
                                "session_id"
                                if kind == "runtime_event"
                                else "invocation_id"
                            )
                            stream = getattr(value, field)
                            if pending_sequence is not None:
                                latest = pending_sequence
                            elif kind == "runtime_event":
                                sess = await self.storage.writer_row(
                                    conn, "sessions", columns=("durable_sequence",), session_id=stream
                                )
                                latest = (sess or {}).get("durable_sequence", 0)
                            else:
                                key = ('user_event_tail', (stream,))
                                latest = self.storage.metadata_cache.get(key)
                                if latest is None:
                                    table = meta.tables["user_events"]
                                    latest = (await conn.scalar(select(table.c.sequence)
                                        .where(table.c.invocation_id == stream)
                                        .order_by(table.c.sequence.desc()).limit(1))) or 0
                                    self.storage.metadata_cache.put(key, latest)
                            if value.sequence != latest + 1:
                                raise ConflictError(
                                    f"Expected {field} sequence {latest + 1}, got {value.sequence}."
                                )
                            revision = (
                                getattr(value.payload, "workflow_revision_id", None)
                                if kind == "runtime_event"
                                else None
                            )
                            if revision and not await self.storage.definition_header(
                                conn, "workflow_revision", revision
                            ):
                                raise ConflictError(
                                    "Workflow revision must be durably registered first."
                                )
                            if binding:
                                definition = await self.storage.definition_header(
                                    conn, "operator_definition", binding
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
                            await self._validate_checkpoint(
                                conn, value, anchor, *checkpoint_frontiers
                            )
                async with self.condition:
                    while True:
                        self.check_open()
                        with self.queue_lock:
                            if self._fits(objects):
                                completion = self._enqueue(kind, identity, value, binding, anchor, objects, durable)
                                break
                        await self.condition.wait()
                    self.condition.notify_all()
        if durable or self.config.mode == "durable":
            await asyncio.shield(asyncio.wrap_future(completion))
        return record_size

    @staticmethod
    def _stream_key(kind, value):
        if kind == "runtime_event":
            return kind, value.session_id
        if kind == "user_event":
            return kind, value.invocation_id
        return None

    def _checkpoint_frontiers(self, checkpoint, anchor):
        # Snapshot accepted frontiers before SQL yields to the commit worker.
        # Admission prevents new entries; commits can only make these durable.
        sequences = {
            member.session_id: entry.value.sequence
            for member in checkpoint.sessions
            if (entry := self.pending_tails.get(("runtime_event", member.session_id)))
            is not None
        }
        anchors = {
            member.session_id
            for entry in self.pending
            if entry.kind == "checkpoint" and entry.anchor
            for member in entry.value.sessions
        } if anchor else set()
        return sequences, anchors

    async def _validate_checkpoint(self, conn, checkpoint, anchor, sequences, anchors):
        revisions = set()
        for member in checkpoint.sessions:
            session = await self.storage.row(
                conn, "sessions", columns=("durable_sequence",), session_id=member.session_id
            )
            latest = sequences.get(
                member.session_id, (session or {}).get("durable_sequence", 0)
            )
            if anchor and latest:
                raise ConflictError(
                    "Checkpoint import requires fresh Session histories."
                )
            if not anchor and member.sequence > latest:
                raise ConflictError("Checkpoint frontier is ahead of accepted Events.")
            if anchor and member.session_id in anchors:
                raise ConflictError("Session already has a pending imported anchor.")
            invocation = member.state.invocation
            if invocation:
                revisions.add(invocation.workflow_revision_id)
                revisions.update(
                    p.workflow_revision_id for p in invocation.child_plans.values()
                )
        for revision in revisions:
            if not await self.storage.has_row(
                conn, "workflow_revisions", revision_id=revision
            ):
                raise ConflictError(
                    "Checkpoint Workflow closure must be durably registered first."
                )

    async def flush(self):
        if self.fault:
            raise PersistenceError("Persistence writer faulted.") from self.fault
        async with self.condition:
            with self.queue_lock:
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
                # Even a wrapper/driver may fail after COMMIT succeeded. Retry
                # against SQL position and sources rather than advancing twice.
                self.storage._store_order = None
                self.storage.metadata_cache.clear()
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

    async def wait_committed(self, after, timeout):
        """Wait without holding a DB connection or retaining committed records."""
        async with self.condition:
            try:
                await asyncio.wait_for(self.condition.wait_for(
                    lambda: self.commit_revision != after or self.state != "open"), timeout)
            except TimeoutError:
                pass
            return self.commit_revision

    async def subscribe_execution(self, invocation_id):
        token = object()
        self.execution_watches[token] = ({invocation_id}, 0)
        return token, 0

    async def unsubscribe_execution(self, token):
        self.execution_watches.pop(token, None)

    async def wait_execution_change(self, token, after, timeout, root_id):
        """Filter commit wakeups without retaining completed execution history."""
        identities, revision = self.execution_watches[token]
        if root_id not in identities:
            identities.add(root_id)
            self.execution_watches[token] = identities, revision + 1
        async with self.condition:
            try:
                await asyncio.wait_for(self.condition.wait_for(
                    lambda: self.execution_watches[token][1] != after or self.state != "open"), timeout)
            except TimeoutError:
                pass
            if self.fault:
                raise PersistenceError("Persistence writer faulted.") from self.fault
            if self.state != "open":
                raise ClosedError("Persistence is closing or closed.")
            return self.execution_watches[token][1]

    def notify_executions(self, identities):
        for token, (selected, revision) in self.execution_watches.items():
            if not selected.isdisjoint(identities):
                self.execution_watches[token] = selected, revision + 1

    async def _worker(self):
        while True:
            async with self.condition:
                await self.condition.wait_for(
                    lambda: self.pending or self.state != "open"
                )
                if not self.pending:
                    return
                with self.queue_lock:
                    delay = (self.pending[0].accepted_at + (
                        self.config.durable_batch_delay_ms if self.config.mode == "durable"
                        else self.config.batch_delay_ms) / 1000 - time.monotonic())
                    coalesce = (
                        delay > 0 and len(self.pending) < self.config.batch_count
                        and self.retained < self.config.batch_bytes
                        and self.retained < self.config.queue_bytes * self.config.batch_queue_ratio
                        and self.pending_count < self.config.queue_count * self.config.batch_queue_ratio
                        and self.state == "open" and self.pending[0].ticket > self.flush_ticket)
                if coalesce:
                    try:
                        await asyncio.wait_for(self.condition.wait(), timeout=delay)
                    except TimeoutError:
                        pass
                    continue
                with self.queue_lock:
                    entries = list(islice(self.pending, self.config.batch_count))
            prepared = None
            try:
                prepared = await self.cpu(prepare, entries, self.config)
                entries = entries[: len(prepared)]
                async with self.mutation:
                    order = await self._commit(prepared)
                    async with self.condition:
                        with self.queue_lock:
                            self.store_order = order
                            self.commit_revision += 1
                            if any(entry.kind == "runtime_event" and entry.value.event_name in OBSERVABLE_EVENTS for entry in entries):
                                self.notify_executions(self.storage.committed_invocations)
                            for entry in entries:
                                assert self.pending[0] is entry
                                self.pending.popleft()
                                del self.pending_by_identity[(entry.kind, entry.identity)]
                                stream_key = self._stream_key(entry.kind, entry.value)
                                if self.pending_tails.get(stream_key) is entry:
                                    del self.pending_tails[stream_key]
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
                    with self.queue_lock:
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
        with self.queue_lock:
            return PersistenceHealth(
                self.state, self.pending_count, self.retained,
                self.accepted_ticket, self.durable_ticket,
                str(self.fault) if self.fault else None,
            )

    async def _shutdown(self):
        async with self.condition:
            with self.queue_lock:
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
        with self.queue_lock:
            self.pending.clear()
            self.pending_by_identity.clear()
            self.pending_tails.clear()
            self.objects.clear()
            self.execution_watches.clear()
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

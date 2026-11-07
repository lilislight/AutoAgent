"""Idle-only database projection maintenance on an independently owned thread."""
from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
import threading
import time

from sqlalchemy import event, select, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.exc import DBAPIError

from .query_projections import prepare, publish
from .storage import Storage

log = logging.getLogger(__name__)


class ProjectionWorker:
    def __init__(self, writer):
        self.writer, self.config = writer, writer.config
        self.stop = threading.Event()
        self.thread = None
        self.loop = None
        self.task = None
        self.rounds = self.batches = self.events = self.failures = 0
        self.busy_rounds = 0
        self.error = None
        self.processed_order = None
        self._cpu_stamp = (time.monotonic(), time.process_time(), None)
        self._cpu_busy = False
        self.cores = len(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else (os.cpu_count() or 1)
        try:
            quota, period = Path('/sys/fs/cgroup/cpu.max').read_text().split()
            if quota != 'max':
                self.cores = min(self.cores, int(quota) / int(period))
        except (OSError, ValueError):
            pass  # CPU affinity remains usable outside cgroup v2.

    def start(self):
        self.thread = threading.Thread(target=self._main, name='autoagent-projections', daemon=True)
        self.thread.start()

    async def close(self):
        self.stop.set()
        loop, task = self.loop, self.task
        if loop and task:
            try:
                loop.call_soon_threadsafe(task.cancel)
            except RuntimeError:
                if not loop.is_closed():
                    raise
        if self.thread:
            await asyncio.to_thread(self.thread.join)

    def statistics(self):
        return dict(running=bool(self.thread and self.thread.is_alive()), rounds=self.rounds,
                    batches=self.batches, events=self.events, busy_rounds=self.busy_rounds,
                    failures=self.failures, error=self.error, processed_order=self.processed_order)

    def busy(self):
        if self.writer.pending_count or time.monotonic() - self.writer.storage.last_write < self.config.projection_quiet_seconds:
            return True
        now, cpu, own_cpu = time.monotonic(), time.process_time(), time.thread_time()
        previous, previous_cpu, previous_own_cpu = self._cpu_stamp
        if now - previous < .05:
            return self._cpu_busy
        self._cpu_stamp = now, cpu, own_cpu
        # Normalize to usable CPU affinity; a saturated application must not
        # acquire extra database work. This is a scheduling hint, not a quota.
        used = cpu - previous_cpu - (own_cpu - previous_own_cpu if previous_own_cpu is not None else 0)
        self._cpu_busy = used / (now - previous) / self.cores > self.config.projection_cpu_ratio
        return self._cpu_busy

    def _engine(self, storage, *, read_only):
        args = {'cached_statements': 0} if storage.sqlite else {'server_settings': {
            'lock_timeout': '50ms', 'statement_timeout': '2000ms',
            **({'default_transaction_read_only': 'on'} if read_only else {})}}
        engine = create_async_engine(storage.url, connect_args=args, poolclass=AsyncAdaptedQueuePool,
                                     pool_size=1, max_overflow=0)
        if storage.sqlite:
            @event.listens_for(engine.sync_engine, 'connect')
            def configure(connection, _):
                cursor = connection.cursor()
                cursor.execute('PRAGMA foreign_keys=ON')
                cursor.execute('PRAGMA busy_timeout=50')
                cursor.execute('PRAGMA synchronous=FULL')
                if read_only:
                    cursor.execute('PRAGMA query_only=ON')
                cursor.close()
                connection.isolation_level = None

            @event.listens_for(engine.sync_engine, 'begin')
            def begin(conn):
                conn.exec_driver_sql('BEGIN' if read_only else 'BEGIN IMMEDIATE')
        return engine

    def _main(self):
        try:
            asyncio.run(self._run())
        except Exception as exc:
            self.failures += 1
            self.error = str(exc)[:300]
            log.exception('Database ProjectionWorker stopped unexpectedly')

    async def _run(self):
        self.loop, self.task = asyncio.get_running_loop(), asyncio.current_task()
        storage = self._storage()
        try:
            await self._idle(storage)
        finally:
            await storage.close()
            self.task = self.loop = None

    def _storage(self):
        source = self.writer.storage
        # No Writer lease and no shared SQL pools/caches across event loops.
        storage = Storage(source.url, graph_cache_bytes=self.config.workflow_graph_cache_bytes,
                          metadata_cache_bytes=self.config.writer_metadata_cache_bytes)
        storage.url, storage.sqlite, storage.memory = source.url, source.sqlite, source.memory
        storage.engine = self._engine(storage, read_only=False)
        storage.query_engine = self._engine(storage, read_only=True)
        return storage

    async def build(self):
        """Explicit catch-up uses separate pools too; target is a fixed SQL cut."""
        storage = self._storage()
        count = retries = 0
        target = None
        try:
            while True:
                try:
                    async with storage.query_engine.connect() as conn:
                        if not storage.sqlite:
                            conn = await conn.execution_options(isolation_level='REPEATABLE READ')
                        async with conn.begin():
                            if target is None:
                                from .schema import meta
                                target = await conn.scalar(select(meta.tables['persistence_meta'].c.last_store_order))
                            prepared = await prepare(storage, conn, self.config)
                    if prepared.after is not None and prepared.after >= target:
                        return count
                    async with storage.engine.begin() as conn:
                        committed = await publish(storage, conn, prepared)
                    if committed:
                        count += prepared.count
                        if prepared.order >= target:
                            return count
                        retries = 0
                    await asyncio.sleep(0)
                except DBAPIError:
                    if retries >= self.config.retry_attempts:
                        raise
                    retries += 1
                    await asyncio.sleep(self.config.retry_delay_ms / 1000)
        finally:
            await storage.close()

    async def _idle(self, storage):
        try:
            delay = self.config.projection_idle_seconds
            while not self.stop.is_set():
                await asyncio.sleep(delay)
                if self.stop.is_set():
                    break
                if self.busy():
                    self.busy_rounds += 1
                    delay = self.config.projection_busy_seconds
                    continue
                self.rounds += 1
                delay = self.config.projection_idle_seconds
                deadline = time.monotonic() + self.config.projection_work_seconds
                try:
                    while not self.stop.is_set():
                        if self.busy():
                            self.busy_rounds += 1
                            delay = self.config.projection_busy_seconds
                            break
                        start = time.monotonic()
                        async with storage.query_engine.connect() as conn:
                            if not storage.sqlite:
                                conn = await conn.execution_options(isolation_level='REPEATABLE READ')
                            async with conn.begin():
                                await conn.execute(text('SELECT 1'))
                                if time.monotonic() - start > .1:
                                    self.busy_rounds += 1
                                    delay = self.config.projection_busy_seconds
                                    break
                                prepared = await prepare(storage, conn, self.config)
                        # Drop the read snapshot before trying the short write.
                        if prepared.after == prepared.order:
                            break
                        if self.busy():
                            self.busy_rounds += 1
                            delay = self.config.projection_busy_seconds
                            break
                        async with storage.engine.begin() as conn:
                            committed = await publish(storage, conn, prepared)
                        if not committed:
                            break
                        self.batches += 1
                        self.events += prepared.count
                        self.processed_order = prepared.order
                        self.error = None
                        if prepared.count < self.config.projection_batch_count or time.monotonic() >= deadline:
                            break
                except DBAPIError as exc:
                    self.busy_rounds += 1
                    delay = self.config.projection_busy_seconds
                    self.error = str(exc.orig)[:300]
                    # Lock/statement timeouts yield to the application. Other
                    # database failures remain observable and retry next round.
                    code = getattr(exc.orig, 'sqlstate', None)
                    if code not in ('55P03', '57014', '40001', '40P01') and not any(
                            word in str(exc.orig).lower() for word in ('locked', 'busy', 'timeout')):
                        self.failures += 1
                        log.exception('Database projection batch failed')
                except Exception as exc:
                    self.failures += 1
                    self.error = str(exc)[:300]
                    log.exception('Database projection batch failed')
                    delay = self.config.projection_busy_seconds
        except asyncio.CancelledError:
            pass

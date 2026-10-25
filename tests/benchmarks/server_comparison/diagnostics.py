"""Opt-in, bounded diagnostics for native Server benchmarks only."""
from contextvars import ContextVar
from functools import wraps
import inspect
import re
import threading
import time


class Diagnostics:
    """Keep aggregate counters only; never retain SQL parameters or Event values."""

    def __init__(self):
        self.phase = ContextVar('benchmark_phase', default='background')
        self.lock = threading.RLock()
        self.data = dict(statements=0, cursor_ms=0., commits=0, verbs={},
                         sql={}, phases={}, batches=dict(count=0, records=0, runtime_events=0),
                         queue=dict(peak_count=0, peak_retained_bytes=0, peak_pending_sessions=0))
        self.data['graph_cache'] = dict(available=False)
        self.data['writer_metadata_cache'] = dict(available=False)

    def snapshot(self):
        import copy
        with self.lock:
            return copy.deepcopy(self.data)

    def record(self, name, elapsed, failed=False):
        with self.lock:
            item = self.data['phases'].setdefault(name, dict(calls=0, elapsed_ms=0., errors=0))
            item['calls'] += 1
            item['elapsed_ms'] += elapsed * 1000
            item['errors'] += int(failed)

    def wrap(self, owner, name, phase):
        original = getattr(owner, name)
        if inspect.iscoroutinefunction(original):
            @wraps(original)
            async def measured(*args, **kwargs):
                token = self.phase.set('observation_sql' if self.phase.get() == 'observation_sql' else phase)
                started, failed = time.perf_counter(), True
                try:
                    result = await original(*args, **kwargs)
                    failed = False
                    return result
                finally:
                    self.record(phase, time.perf_counter() - started, failed)
                    self.phase.reset(token)
        else:
            @wraps(original)
            def measured(*args, **kwargs):
                token = self.phase.set('observation_sql' if self.phase.get() == 'observation_sql' else phase)
                started, failed = time.perf_counter(), True
                try:
                    result = original(*args, **kwargs)
                    failed = False
                    return result
                finally:
                    self.record(phase, time.perf_counter() - started, failed)
                    self.phase.reset(token)
        setattr(owner, name, measured)

    def install_sql(self):
        from sqlalchemy import event
        from sqlalchemy.engine import Engine

        @event.listens_for(Engine, 'before_cursor_execute')
        def before(conn, cursor, statement, parameters, context, executemany):
            context._comparison_started = time.perf_counter()
            context._comparison_phase = self.phase.get()

        @event.listens_for(Engine, 'after_cursor_execute')
        def after(conn, cursor, statement, parameters, context, executemany):
            elapsed = (time.perf_counter() - context._comparison_started) * 1000
            fingerprint = re.sub(r"'(?:''|[^'])*'", '?', statement)
            fingerprint = re.sub(r'\b\d+\b', '?', fingerprint)
            fingerprint = ' '.join(fingerprint.split())
            verb = fingerprint.split(' ', 1)[0].upper()
            phase = context._comparison_phase
            key = phase + ':' + fingerprint
            with self.lock:
                self.data['statements'] += 1
                self.data['cursor_ms'] += elapsed
                verbs = self.data['verbs']
                verbs[verb] = verbs.get(verb, 0) + 1
                groups = self.data['sql']
                if key not in groups and len(groups) >= 256:
                    key = 'overflow'
                item = groups.setdefault(key, dict(phase=phase, fingerprint=fingerprint if key != 'overflow' else 'overflow',
                    calls=0, parameter_sets=0, executemany_calls=0, cursor_ms=0.))
                item['calls'] += 1
                # Parameter sets are not affected-row counts; INSERT may conflict.
                item['parameter_sets'] += len(parameters) if executemany else 1
                item['executemany_calls'] += int(executemany)
                item['cursor_ms'] += elapsed

        @event.listens_for(Engine, 'commit')
        def commit(conn):
            with self.lock:
                self.data['commits'] += 1

    def install_v2(self):
        from autoagent.host.persistence import writer, storage
        from autoagent.host.persistence.projections import ProjectionBatch
        from autoagent.host.persistence.database import DatabaseSink
        from autoagent.host.observation import ExecutionObservation
        from autoagent.core.app import _residency

        self.wrap(writer.Writer, 'accept', 'admission_and_ack')
        self.wrap(writer, 'retained_objects', 'queue_residency')
        self.wrap(writer, 'prepare', 'encoding')
        if hasattr(storage, 'source_texts'):
            self.wrap(storage, 'source_texts', 'source_texts')
        self.wrap(_residency, 'retained_size', 'host_residency')
        self.wrap(ProjectionBatch, 'runtime', 'projection_compute')
        self.wrap(storage.Storage, 'projections', 'projection_write')
        self.wrap(DatabaseSink, 'execution_state', 'observation_sql')
        self.wrap(ExecutionObservation, 'wait_execution', 'observation_wait')
        self.wrap(storage.Storage, 'write_transaction', 'write_transaction')
        original_start = writer.Writer._start
        diagnostic = self

        class AdmissionProbe:
            def __init__(self, lock):
                self.lock = lock

            async def __aenter__(self):
                started = time.perf_counter()
                await self.lock.acquire()
                self.acquired = time.perf_counter()
                diagnostic.record('admission_lock_wait', self.acquired - started)
                return self

            async def __aexit__(self, *args):
                diagnostic.record('admission_lock_hold', time.perf_counter() - self.acquired)
                self.lock.release()

        async def started(instance):
            await original_start(instance)
            instance.admission = AdmissionProbe(instance.admission)
            original_notify = instance.condition.notify_all

            def notified():
                # Acceptance and commit update queue accounting before notifying;
                # this observes exact high watermarks without a sampling task.
                with diagnostic.lock:
                    queue = diagnostic.data['queue']
                    queue['peak_count'] = max(queue['peak_count'], instance.pending_count)
                    queue['peak_retained_bytes'] = max(queue['peak_retained_bytes'], instance.retained)
                    queue['peak_pending_sessions'] = max(queue['peak_pending_sessions'], len({
                        entry.value.session_id for entry in instance.pending
                        if entry.kind == 'runtime_event' and entry.value is not None}))
                return original_notify()

            instance.condition.notify_all = notified

        writer.Writer._start = started
        original_open = storage.Storage.open

        async def opened(instance):
            await original_open(instance)
            # Time the driver's real commit, including its await via greenlet.
            diagnostic.wrap(instance.engine.sync_engine.dialect, 'do_commit', 'driver_commit')
        storage.Storage.open = opened
        original_write = storage.Storage.write

        async def written(instance, entries):
            with diagnostic.lock:
                batch = diagnostic.data['batches']
                batch['count'] += 1
                batch['records'] += len(entries)
                batch['runtime_events'] += sum(e.kind == 'runtime_event' for e in entries)
                events = [e for e in entries if e.kind == 'runtime_event']
                sessions = len({e.light['session_id'] for e in events})
                for name, value in (('runtime_events_per_batch', len(events)), ('sessions_per_batch', sessions)):
                    histogram = batch.setdefault(name, {})
                    histogram[str(value)] = histogram.get(str(value), 0) + 1
                batch['multi_session_batches'] = batch.get('multi_session_batches', 0) + int(sessions > 1)
            try:
                return await original_write(instance, entries)
            finally:
                cache = getattr(instance, 'graph_cache', None)
                if cache is not None:
                    with diagnostic.lock:
                        diagnostic.data['graph_cache'] = dict(
                            available=True, hits=cache.hits, misses=cache.misses,
                            evictions=cache.evictions, entries=len(cache.entries),
                            bytes=cache.bytes, budget=cache.budget)
                cache = getattr(instance, 'metadata_cache', None)
                if cache is not None:
                    with diagnostic.lock:
                        previous = diagnostic.data['writer_metadata_cache']
                        diagnostic.data['writer_metadata_cache'] = dict(
                            available=True, hits=cache.hits, misses=cache.misses,
                            evictions=cache.evictions, entries=len(cache.entries),
                            bytes=cache.bytes, budget=cache.budget,
                            peak_committed_bytes=max(previous.get('peak_committed_bytes', 0), cache.bytes))
        storage.Storage.write = written

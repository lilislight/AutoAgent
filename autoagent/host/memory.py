"""Bounded Host telemetry. Measurements never capture or traverse business State."""

from __future__ import annotations

import asyncio
from collections import deque
from copy import deepcopy
from dataclasses import asdict
import heapq
import logging
import os
from pathlib import Path
import threading
import time

from .residency import automatic_budget

log = logging.getLogger(__name__)


class _ProcessSampler:
    """Share one sampling thread across all active Hosts in this process."""

    def __init__(self):
        self.lock = threading.Lock()
        self.wake = threading.Event()
        self.monitors = set()
        self.thread = None
        self.stopping = None

    def add(self, monitor):
        with self.lock:
            self.monitors.add(monitor)
            if self.thread is None:
                self.stopping = threading.Event()
                self.wake = threading.Event()
                self.thread = threading.Thread(target=self.run, args=(self.stopping, self.wake), name='autoagent-memory-monitor', daemon=True)
                try:
                    self.thread.start()
                except RuntimeError:
                    self.monitors.remove(monitor)
                    self.thread = None
                    raise
            thread = self.thread
            self.wake.set()
            return thread

    def remove(self, monitor):
        with self.lock:
            self.monitors.discard(monitor)
            last_thread = self.thread if not self.monitors else None
            if last_thread is not None:
                self.stopping.set()
                self.thread = None
            self.wake.set()
            return last_thread

    def run(self, stopping, wake):
        while not stopping.is_set():
            wake.clear()
            with self.lock:
                if not self.monitors:
                    return
                monitors = tuple(self.monitors)
            now = time.monotonic()
            due = [item for item in monitors if not item.stop_event.is_set() and item.next_sample <= now]
            if due:
                process = process_sample()
                for item in due:
                    try:
                        if not item.stop_event.is_set():
                            item._sample(process=process)
                    except Exception:
                        log.debug('Cannot sample process memory', exc_info=True)
                    item.next_sample = time.monotonic() + item.settings.memory_monitor_interval_seconds
            delay = max(.01, min(item.next_sample for item in monitors) - time.monotonic())
            if stopping.is_set():
                return
            wake.wait(min(delay, 60))


_sampler = _ProcessSampler()


def process_sample():
    """Linux current and peak RSS; unavailable measurements remain null."""
    result = dict(rss_bytes=None, peak_rss_bytes=None, cpu_seconds=time.process_time())
    try:
        fields = dict(line.split(':', 1) for line in Path('/proc/self/status').read_text().splitlines() if ':' in line)
        result.update(rss_bytes=int(fields['VmRSS'].split()[0]) * 1024,
                      peak_rss_bytes=int(fields['VmHWM'].split()[0]) * 1024)
    except (OSError, ValueError, KeyError):
        pass
    return result


def cgroup_directory():
    """Locate this process's memory controller, including nested cgroup v2."""
    try:
        groups = Path('/proc/self/cgroup').read_text().splitlines()
        mounts = Path('/proc/self/mountinfo').read_text().splitlines()
        for group in groups:
            _, controllers, relative = group.split(':', 2)
            for mount in mounts:
                before, after = mount.split(' - ', 1)
                fs, _, options = after.split(' ', 2)
                if not (fs == 'cgroup2' and not controllers or fs == 'cgroup' and 'memory' in controllers.split(',') and 'memory' in options.split(',')):
                    continue
                fields = before.split()
                root, point = fields[3:5]
                relative_path = Path(relative).relative_to(root)
                candidate = Path(point) / relative_path
                if candidate.is_dir():
                    return candidate, fs == 'cgroup2'
    except (OSError, ValueError, IndexError):
        pass
    return None


def cgroup_sample(location):
    """Controller usage includes sibling processes and file cache, unlike RSS."""
    if location is None:
        return None
    directory, unified = location
    result = dict(scope='shared_cgroup', current_bytes=None, limit_bytes=None, events={})
    for key, name in (('current_bytes', 'memory.current' if unified else 'memory.usage_in_bytes'),
                      ('limit_bytes', 'memory.max' if unified else 'memory.limit_in_bytes')):
        try:
            value = int((directory / name).read_text().strip())
            result[key] = value if value < 1 << 60 else None
        except (OSError, ValueError):
            pass
    if unified:
        try:
            result['events'] = {key: int(value) for key, value in (line.split() for line in (directory / 'memory.events').read_text().splitlines())}
        except (OSError, ValueError):
            pass
    return result


class MemoryMonitor:
    """A process sampler independent of the Runtime loop, with cached Host counters.

    Hosts share one process thread and each own one lightweight task. History and top Sessions are
    bounded. No payloads, Events, checkpoints, GC walks or heap profiles are kept.
    Component byte counts are cached estimates, never an RSS decomposition.
    """

    def __init__(self, host):
        self.host = host
        self.settings = host.settings
        self.enabled = self.settings.memory_monitor_enabled
        self.history = deque(maxlen=self.settings.memory_monitor_history_samples)
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = self.task = None
        self.location = cgroup_directory()
        self.latest = None
        self.counters = None
        self.pressure = 'unknown'
        self.next_sample = 0
        self.budget = self.settings.memory_budget_bytes or automatic_budget()

    async def start(self):
        if not self.enabled:
            return
        try:
            await self.refresh()
        except Exception:
            log.debug('Cannot initialize Host memory counters', exc_info=True)
        self._sample()
        self.next_sample = time.monotonic() + self.settings.memory_monitor_interval_seconds
        try:
            self.thread = _sampler.add(self)
        except RuntimeError:
            log.debug('Cannot start process memory sampler', exc_info=True)
        self.task = asyncio.create_task(self._refresh_loop())

    async def stop(self):
        self.stop_event.set()
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            self.task = None
        if self.thread is not None:
            last_thread = _sampler.remove(self)
            if last_thread is not None:
                await asyncio.to_thread(last_thread.join)
            self.thread = None

    async def ready(self):
        """Refresh once after startup without making telemetry a readiness dependency."""
        if not self.enabled:
            return
        try:
            await self.refresh()
        except Exception:
            log.debug('Cannot refresh ready Host memory counters', exc_info=True)
        self._sample()

    async def refresh(self):
        runtime = self.host._runtime
        residency = runtime.residency if runtime is not None else None
        entries = residency.entries.values() if residency is not None else ()
        known = estimated = tail_bytes = tail_events = 0
        # Residency already owns these estimates. Do not request fresh Core State.
        def summaries():
            nonlocal known, estimated, tail_bytes, tail_events
            for item in entries:
                tail_bytes += item.tail_bytes
                tail_events += item.tail_events
                if item.info is not None:
                    size = item.info.estimated_bytes
                    known += 1
                    estimated += size
                    yield dict(session_id=item.session_id, estimated_bytes=size,
                               can_unload=item.info.can_unload)
        top = heapq.nlargest(self.settings.memory_monitor_top_sessions, summaries(), key=lambda row: row['estimated_bytes'])
        health = asdict(await self.host._persistence.health()) if self.host._persistence is not None else dict(
            state='unopened', queued_count=0, retained_bytes=0, accepted_frontier=0, durable_frontier=0, fault=None)
        counters = dict(sampled_at_us=time.time_ns() // 1000,
                        resident_graphs=len(runtime.resident) if runtime is not None else 0,
                        loading_graphs=len(residency.loading) if residency is not None else 0,
                        checkpoint_jobs=len(residency.jobs) if residency is not None else 0,
                        requests=len(self.host._operations), request_bytes=self.host._request_bytes,
                        persistence=health,
                        residency=dict(estimated_bytes=estimated, estimated_graphs=known,
                                       tail_bytes=tail_bytes, tail_events=tail_events,
                                       estimates='cached; may be stale; not additive with process RSS', top_sessions=top))
        with self.lock:
            self.counters = counters

    async def _refresh_loop(self):
        while True:
            await asyncio.sleep(self.settings.memory_monitor_interval_seconds)
            try:
                await self.refresh()
            except Exception:
                # A telemetry failure must not fault Runtime or admission.
                log.debug('Cannot refresh Host memory counters', exc_info=True)

    def _sample(self, *, process=None):
        started = time.monotonic()
        sample = dict(timestamp_us=time.time_ns() // 1000, monotonic_seconds=started,
                      pid=os.getpid(), process=process if process is not None else process_sample(), cgroup=cgroup_sample(self.location))
        runtime = self.host._runtime
        residency = runtime.residency if runtime is not None else None
        budget = residency.budget if residency is not None else self.budget
        rss = sample['process']['rss_bytes']
        ratio = rss / budget if rss is not None and budget else None
        pressure = 'unknown' if ratio is None else (
            'critical' if ratio >= self.settings.memory_critical_ratio else
            'high' if ratio >= self.settings.memory_high_ratio else
            'normal' if ratio <= self.settings.memory_target_ratio else
            self.pressure if self.pressure in ('high', 'critical') else 'normal')
        if pressure != self.pressure:
            if pressure in ('high', 'critical'):
                log.warning('Host memory pressure=%s rss_bytes=%s budget_bytes=%s', pressure, rss, budget)
            elif self.pressure in ('high', 'critical'):
                log.info('Host memory pressure=%s rss_bytes=%s budget_bytes=%s', pressure, rss, budget)
            self.pressure = pressure
        sample.update(budget_bytes=budget or None, budget_ratio=ratio, pressure=pressure)
        with self.lock:
            sample['host'] = self.counters
            sample['sampling_ms'] = (time.monotonic() - started) * 1000
            self.latest = sample
            self.history.append(sample)

    def snapshot(self, *, history=0):
        if type(history) is not int or not 0 <= history <= self.settings.memory_monitor_history_samples:
            raise ValueError('history must be within the configured memory history capacity.')
        with self.lock:
            latest = deepcopy(self.latest)
            samples = deepcopy(list(self.history)[-history:]) if history else []
        now = time.monotonic()
        age = max(0, now - latest['monotonic_seconds']) if latest else None
        return dict(enabled=self.enabled, interval_seconds=self.settings.memory_monitor_interval_seconds,
                    history_capacity=self.history.maxlen, sample_age_seconds=age,
                    stale=age is None or age > self.settings.memory_monitor_interval_seconds * 3,
                    latest=latest, history=samples)


def prometheus(snapshot):
    """Small dependency-free exposition with fixed labels and no Session IDs."""
    sample = snapshot['latest']
    metrics = dict(enabled=int(snapshot['enabled']), stale=int(snapshot['stale']))
    if sample:
        metrics.update(rss_bytes=sample['process']['rss_bytes'], peak_rss_bytes=sample['process']['peak_rss_bytes'],
                       budget_bytes=sample['budget_bytes'], sample_age_seconds=snapshot['sample_age_seconds'])
        host = sample['host'] or {}
        metrics.update(resident_graphs=host.get('resident_graphs'), request_bytes=host.get('request_bytes'),
                       persistence_queued_count=host.get('persistence', {}).get('queued_count'),
                       persistence_retained_bytes=host.get('persistence', {}).get('retained_bytes'))
        cgroup = sample.get('cgroup') or {}
        metrics.update(cgroup_current_bytes=cgroup.get('current_bytes'), cgroup_limit_bytes=cgroup.get('limit_bytes'),
                       cgroup_oom_kills_total=cgroup.get('events', {}).get('oom_kill'))
    return ''.join(f'# TYPE autoagent_memory_{key} {"counter" if key.endswith("_total") else "gauge"}\nautoagent_memory_{key} {value}\n'
                   for key, value in metrics.items() if value is not None)

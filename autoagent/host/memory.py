"""Bounded Host telemetry. Measurements never capture or traverse business State."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import asdict
import heapq
import logging
import math
import os
from pathlib import Path
import threading
import time
from uuid import uuid4

from .memory_resources import MemoryResources
from .memory_history import (DEFAULT_HISTORY_POINTS, MAX_HISTORY_POINTS, MAX_HISTORY_SECONDS, WIDTH,
                             MemoryHistory, history_points)

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
    """Current RSS on every supported OS; unavailable peak measurements remain null."""
    result = dict(rss_bytes=None, peak_rss_bytes=None, cpu_seconds=time.process_time())
    try:
        fields = dict(line.split(':', 1) for line in Path('/proc/self/status').read_text().splitlines() if ':' in line)
        result.update(rss_bytes=int(fields['VmRSS'].split()[0]) * 1024,
                      peak_rss_bytes=int(fields['VmHWM'].split()[0]) * 1024)
    except (OSError, ValueError, KeyError):
        import psutil
        try:
            sample = psutil.Process().memory_info()
            result.update(rss_bytes=sample.rss, peak_rss_bytes=getattr(sample, 'peak_wset', None))
        except (OSError, psutil.Error):
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
        self.history = MemoryHistory(self.settings.memory_monitor_history_samples,
                                     self.settings.memory_monitor_history_seconds)
        self.monitor_id = uuid4().hex
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = self.task = None
        self.resources = MemoryResources(self.settings.memory_budget_bytes)
        self.latest = None
        self.counters = None
        self.pressure = 'unknown'
        self.next_sample = 0

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
        with self.lock:
            self.history.clear()
            self.latest = self.counters = None

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
                        projections=await self.host._persistence.projection_cache_statistics() if self.host._persistence is not None else {},
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
                      pid=os.getpid(), process=process if process is not None else process_sample())
        resources = self.resources.sample()
        sample['cgroup'] = resources.cgroup
        sample['resources'] = resources.metadata()
        runtime = self.host._runtime
        residency = runtime.residency if runtime is not None else None
        budget = residency.budget if residency is not None and self.settings.memory_budget_bytes else resources.budget_bytes
        rss = sample['process']['rss_bytes']
        ratio = rss / budget if rss is not None and budget else None
        effective_ratio = max(ratio or 0., resources.pressure_ratio()) if ratio is not None or resources.constraints else None
        pressure = 'unknown' if effective_ratio is None else (
            'critical' if effective_ratio >= self.settings.memory_critical_ratio else
            'high' if effective_ratio >= self.settings.memory_high_ratio else
            'normal' if effective_ratio <= self.settings.memory_target_ratio else
            self.pressure if self.pressure in ('high', 'critical') else 'normal')
        if pressure != self.pressure:
            if pressure in ('high', 'critical'):
                log.warning('Host memory pressure=%s rss_bytes=%s budget_bytes=%s shared_pressure=%.3f',
                            pressure, rss, budget, resources.pressure_ratio())
            elif self.pressure in ('high', 'critical'):
                log.info('Host memory pressure=%s rss_bytes=%s budget_bytes=%s', pressure, rss, budget)
            self.pressure = pressure
        sample.update(budget_bytes=budget or None, budget_ratio=ratio, pressure=pressure)
        with self.lock:
            if self.stop_event.is_set() or self.latest is not None and started <= self.latest['monotonic_seconds']:
                return
            sample['host'] = self.counters
            sample['sampling_ms'] = (time.monotonic() - started) * 1000
            self.latest = sample
            if self.enabled:
                self.history.append(sample)

    def snapshot(self, *, history=0, window_seconds=None, points=DEFAULT_HISTORY_POINTS):
        if type(history) is not int or not 0 <= history <= self.settings.memory_monitor_history_samples:
            raise ValueError('history must be within the configured memory history capacity.')
        if type(points) is not int or not 2 <= points <= MAX_HISTORY_POINTS:
            raise ValueError(f'points must be between 2 and {MAX_HISTORY_POINTS}.')
        if window_seconds is not None and (isinstance(window_seconds, bool) or not isinstance(window_seconds, (int, float))
                or not math.isfinite(window_seconds) or not 0 < window_seconds <= MAX_HISTORY_SECONDS):
            raise ValueError(f'window_seconds must be positive and at most {MAX_HISTORY_SECONDS}.')
        now = time.monotonic()
        with self.lock:
            latest = self.latest
            packed, retained = self.history.capture(history=history, window_seconds=window_seconds, now=now)
            storage_bytes = self.history.storage_bytes
        latest = deepcopy(latest)
        age = max(0, now - latest['monotonic_seconds']) if latest else None
        return dict(enabled=self.enabled, monitor_id=self.monitor_id,
                    interval_seconds=self.settings.memory_monitor_interval_seconds,
                    history_capacity=self.history.capacity, history_retention_seconds=self.history.retention_seconds,
                    history_retained_samples=retained, history_storage_bytes=storage_bytes,
                    history_count=len(packed) // WIDTH,
                    history_points_limit=points, sample_age_seconds=age,
                    stale=age is None or age > self.settings.memory_monitor_interval_seconds * 3,
                    latest=latest, history=history_points(packed, points))


def prometheus(snapshot):
    """Small dependency-free exposition with fixed labels and no Session IDs."""
    sample = snapshot['latest']
    metrics = dict(enabled=int(snapshot['enabled']), stale=int(snapshot['stale']),
                   history_storage_bytes=snapshot['history_storage_bytes'],
                   history_retained_samples=snapshot['history_retained_samples'])
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

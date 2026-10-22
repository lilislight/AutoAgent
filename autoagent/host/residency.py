"""Host-internal adaptive Root graph caching and durable wake scheduling."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path

from autoagent import RuntimeTransitionError

from .models import HostCapacityError

log = logging.getLogger(__name__)


def process_memory_bytes():
    """Current RSS, rather than the process lifetime's peak RSS."""
    try:
        return int(Path('/proc/self/statm').read_text().split()[1]) * os.sysconf('SC_PAGE_SIZE')
    except (OSError, ValueError, IndexError):
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if os.uname().sysname == 'Darwin' else 1024)


def automatic_budget():
    budget = 512 * 1024 * 1024
    for name in ('/sys/fs/cgroup/memory.max', '/sys/fs/cgroup/memory/memory.limit_in_bytes'):
        try:
            limit = int(Path(name).read_text().strip())
            if 0 < limit < (1 << 60):
                budget = min(budget, int(limit * 0.8))
        except (OSError, ValueError):
            pass
    return budget


@dataclass(slots=True)
class Entry:
    session_id: str
    last_access: float
    loaded_at: float
    accesses: int = 0
    interval: float = 0
    users: int = 0
    generation: int = 0
    info: object = None
    saved_version: tuple | None = None
    checkpoint_at: float = 0
    tail_events: int = 0
    tail_bytes: int = 0
    load_ms: float = 1
    save_ms: float = 1
    job: asyncio.Task | None = None
    retry_at: float = 0
    error: Exception | None = None


class Residency:
    """Policy metadata is bounded; business State stays in Core or Persistence."""

    def __init__(self, runtime, settings):
        self.runtime, self.settings = runtime, settings
        self.entries = OrderedDict()
        self.loading = set()
        self.jobs = set()
        self.recoveries = {}
        self.cursor = None
        self.fault = None
        self.stopping = False
        self.budget = settings.memory_budget_bytes or automatic_budget()
        self.memory_usage = process_memory_bytes
        self.baseline = self.memory_usage()
        self.last_rss = self.baseline
        self.reclaimed_credit = 0
        self.draining = False
        self.loop = asyncio.get_running_loop()
        self.wake = asyncio.Event()
        self.cycle_lock = asyncio.Lock()
        self.task = None

    def start(self):
        self.task = asyncio.create_task(self._run())

    def entry(self, sid):
        now = time.monotonic()
        item = self.entries.get(sid)
        if item is None:
            item = Entry(sid, now, now, checkpoint_at=now)
            self.entries[sid] = item
        self.entries.move_to_end(sid)
        return item

    def touch(self, sid):
        item, now = self.entry(sid), time.monotonic()
        if item.accesses:
            gap = max(0, now - item.last_access)
            item.interval = gap if item.accesses == 1 else 0.25 * gap + 0.75 * item.interval
        item.last_access = now
        item.accesses += 1
        item.generation += 1
        return item

    def note(self, sid, count, size):
        root = self.runtime.owners.get(sid, sid)
        if root in self.runtime.resident or root in self.loading:
            item = self.entry(root)
            item.tail_events += count
            item.tail_bytes += size
            # Do not wake on every Event: the periodic scan coalesces hot streams.

    def _usage(self):
        rss = self.memory_usage()
        # Python may retain freed arenas. Credit only known unloaded State;
        # new State growth and returned RSS consume this credit again.
        self.reclaimed_credit = max(0, self.reclaimed_credit - abs(rss - self.last_rss))
        self.last_rss = rss
        return max(self.baseline, rss - self.reclaimed_credit)

    async def reserve(self, sid):
        if self.fault is not None:
            raise HostCapacityError('Host residency persistence failed; admission is stopped.') from self.fault
        if sid in self.runtime.resident or sid in self.loading:
            return
        if len(self.runtime.resident) + len(self.loading) >= self.settings.max_resident_graphs or self._usage() >= self.budget * self.settings.memory_high_ratio:
            await self.cycle(activate=False, force_slot=True, exclude=sid)
            pending = tuple(self.jobs)
            if pending:
                await asyncio.shield(asyncio.gather(*pending, return_exceptions=True))
        if len(self.runtime.resident) + len(self.loading) >= self.settings.max_resident_graphs or self.memory_usage() >= self.budget * self.settings.memory_critical_ratio:
            raise HostCapacityError('Host graph or memory capacity is exhausted.')
        self.loading.add(sid)
        self.entry(sid).loaded_at = time.monotonic()

    async def admit_resident(self, sid):
        """A warm Session can reuse State but still needs process headroom."""
        if self._usage() >= self.budget * self.settings.memory_high_ratio:
            await self.cycle(activate=False, exclude=sid)
            if self.jobs:
                await asyncio.shield(asyncio.gather(*tuple(self.jobs), return_exceptions=True))
        if self.memory_usage() >= self.budget * self.settings.memory_critical_ratio:
            raise HostCapacityError('Host memory capacity is exhausted.')

    def installed(self, sid, *, version=None, load_ms=1):
        self.loading.discard(sid)
        item = self.entry(sid)
        item.loaded_at = time.monotonic()
        item.saved_version = version
        item.load_ms = load_ms
        self.wake.set()

    def trim(self):
        for sid in tuple(self.entries):
            if len(self.entries) <= self.settings.max_resident_graphs * 2:
                break
            item = self.entries[sid]
            if sid not in self.runtime.resident and sid not in self.loading and not item.users and item.job is None:
                del self.entries[sid]

    def _keep_value(self, item, info, now):
        age = now - item.last_access
        hot_window = max(self.settings.cold_after_seconds, item.interval * 3)
        hot = item.accesses > 1 and age < hot_window
        timer = info.control_state == 'active' and info.next_deadline_at_us is not None and (info.next_deadline_at_us / 1e6 - time.time()) <= self.settings.short_timer_seconds
        # Protection tier, then reuse/loading value per byte. Large cold State
        # is reclaimed sooner; expensive hot State is worth keeping.
        tier = 2 if timer else 1 if hot else 0
        value = (item.accesses / (1 + age)) * (item.load_ms + item.save_ms) / max(1, info.estimated_bytes)
        return tier, value, -age

    async def cycle(self, *, activate=True, force_slot=False, exclude=None):
        async with self.cycle_lock:
            now, candidates = time.monotonic(), []
            usage = self._usage()
            if usage >= self.budget * self.settings.memory_high_ratio:
                self.draining = True
            if usage <= self.budget * self.settings.memory_target_ratio:
                self.draining = False
            # Freed Python arenas may still be resident. They can reduce soft
            # reclamation pressure, but never bypass the actual RSS hard limit.
            critical = self.memory_usage() >= self.budget * self.settings.memory_critical_ratio
            for sid, ref in tuple(self.runtime.resident.items()):
                item = self.entry(sid)
                if item.users or item.job is not None or sid == exclude or now < item.retry_at:
                    continue
                try:
                    info = await self.runtime.app.aresidency_info(ref)
                except RuntimeTransitionError:
                    continue
                old_bytes = item.info.estimated_bytes if item.info else 0
                if info.estimated_bytes > old_bytes:
                    self.reclaimed_credit = max(0, self.reclaimed_credit - (info.estimated_bytes - old_bytes))
                item.info = info
                if not info.can_unload:
                    continue
                tier, value, neg_age = self._keep_value(item, info, now)
                if critical or force_slot or now - item.loaded_at >= self.settings.min_residency_seconds:
                    candidates.append(((tier, value, neg_age), item, info))
                tail = item.tail_events >= self.settings.checkpoint_tail_events or item.tail_bytes >= self.settings.checkpoint_tail_bytes
                aged = item.tail_events > 0 and now - item.checkpoint_at >= self.settings.checkpoint_interval_seconds
                if (tail or aged) and item.saved_version != info.version and not self.draining and not force_slot:
                    self._snapshot(item, info, evict=False)
            projected, evictions = usage, 0
            for rank, item, info in sorted(candidates, key=lambda x: x[0]):
                if len(self.jobs) >= self.settings.unload_concurrency:
                    break
                if item.job is not None:
                    continue
                age = now - item.last_access
                cold = age >= self.settings.cold_after_seconds
                expired = age >= self.settings.max_idle_seconds
                pressure = self.draining and projected > self.budget * self.settings.memory_target_ratio
                slot = force_slot and evictions == 0
                moderate = usage >= self.budget * self.settings.memory_target_ratio and cold and rank[0] == 0
                if rank[0] == 2 and not critical:
                    continue
                if not (pressure or slot or expired or moderate):
                    continue
                self._snapshot(item, info, evict=True)
                projected -= info.estimated_bytes
                evictions += 1
            if activate and not self.stopping and self.fault is None:
                await self._activate_due()
            self.trim()

    def _snapshot(self, item, info, *, evict):
        if len(self.jobs) >= self.settings.unload_concurrency:
            return
        task = asyncio.create_task(self._save(item, info, evict))
        item.job = task
        self.jobs.add(task)
        task.add_done_callback(self.jobs.discard)

    async def _save(self, item, info, evict):
        generation = item.generation
        try:
            ref = info.ref
            if item.saved_version != info.version:
                # Capture under the Session lock, but release it during slow I/O.
                async with self.runtime.coordinated(item.session_id):
                    if item.users or self.runtime.resident.get(item.session_id) != ref:
                        return
                    checkpoint = await self.runtime.app.acapture_checkpoint(ref)
                version = checkpoint.version
                events, size = item.tail_events, item.tail_bytes
                started = time.monotonic()
                await self.runtime.save_checkpoint(checkpoint, reason="unload" if evict else "automatic")
                item.save_ms = max(1, (time.monotonic() - started) * 1000)
                item.saved_version = version
                item.tail_events = max(0, item.tail_events - events)
                item.tail_bytes = max(0, item.tail_bytes - size)
                item.checkpoint_at = time.monotonic()
                checkpoint = None
            if evict:
                async with self.runtime.coordinated(item.session_id):
                    if item.users or item.generation != generation or self.runtime.resident.get(item.session_id) != ref:
                        return
                    await self.runtime.app.aunload_session(ref, expected_version=item.saved_version)
                    self.runtime.forget(item.session_id)
                    self.reclaimed_credit += info.estimated_bytes
                    item.info = None
        except RuntimeTransitionError as error:
            if error.code not in ('CHECKPOINT_GRAPH_CHANGED', 'RELATED_INVOCATION_NOT_UNLOADABLE', 'INVOCATION_RESULT_PENDING', 'INVOCATION_REF_STALE'):
                item.error = error
                item.retry_at = time.monotonic() + self.settings.background_retry_seconds
        except Exception as error:
            item.error = self.fault = error
            log.error('Host Checkpoint persistence failed; retaining resident State', exc_info=True)
        finally:
            item.job = None

    async def _activate_due(self):
        page = await self.runtime.persistence.activation_candidates(
            int(time.time() * 1e6), limit=self.settings.background_batch_size, cursor=self.cursor,
        )
        self.cursor = page.next_cursor
        for row in page.items:
            sid, iid = row['session_id'], row['invocation_id']
            if sid in self.runtime.resident or sid in self.loading or iid in self.recoveries:
                continue
            if len(self.recoveries) >= self.settings.background_batch_size:
                break
            item = self.entries.get(sid)
            if item is not None and time.monotonic() < item.retry_at:
                continue
            task = asyncio.create_task(self._recover(row))
            self.recoveries[iid] = task
            task.add_done_callback(lambda _, identity=iid: self.recoveries.pop(identity, None))

    async def _recover(self, row):
        sid = row['session_id']
        item = self.entry(sid)
        item.users += 1
        try:
            async with self.runtime.coordinated(sid):
                if self.stopping:
                    return
                ref = await self.runtime.reference(row['invocation_id'])
                await self.runtime.ensure_resident(ref)
            await self.runtime.activate(ref)
        except HostCapacityError:
            item.retry_at = time.monotonic() + self.settings.background_retry_seconds
        except Exception as error:
            item.error = error
            item.retry_at = time.monotonic() + self.settings.background_retry_seconds
            log.warning('Cannot activate Root Invocation %s: %s', row['invocation_id'], error)
        finally:
            item.users -= 1

    async def _run(self):
        while not self.stopping:
            self.wake.clear()
            try:
                await self.cycle()
            except Exception as error:
                self.fault = error
                log.error('Host residency scheduler failed', exc_info=True)
            try:
                await asyncio.wait_for(self.wake.wait(), self.settings.residency_interval_seconds)
            except TimeoutError:
                pass

    async def stop(self):
        self.stopping = True
        self.wake.set()
        if self.task is not None:
            await self.task
        if self.jobs:
            await asyncio.gather(*tuple(self.jobs), return_exceptions=True)

    async def finish(self):
        tasks = tuple(self.recoveries.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

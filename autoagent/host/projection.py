"""Optional local UI observation, owned by Host rather than the execution path."""

import asyncio
from collections import OrderedDict, defaultdict
import logging
import time

from sqlalchemy import select

from .persistence.read_models import ProjectionConnection
from .persistence.schema import meta

logger = logging.getLogger(__name__)


class LocalProjectionService:
    """Fold accepted Events in the background without another payload queue.

    Writer notifications coalesce work. A consistent SQL prefix and the bounded
    accepted tail supply every Event, including those committed between wakes.
    Cached generations ahead of a reader's SQL snapshot are never used by it.
    """

    def __init__(self, persistence, *, interval=0.02):
        self.writer = persistence._writer
        self.queries = persistence._queries
        self.cache = self.queries.projection_cache
        self.interval = interval
        self.task = None
        self.revision = None
        self.durable_order = 0
        self.accepted_order = self.observed_order = 0
        self.store_id = None
        self.failures = self.updates = 0
        self.last_error = None
        self.blocked = OrderedDict()

    def _version(self):
        return self.writer.accepted_ticket, self.writer.commit_revision

    async def start(self):
        await self.writer.run(self._start)

    async def _start(self):
        self.durable_order = self.writer.store_order
        self.revision = self._version()
        self.task = asyncio.create_task(self._run(), name='autoagent-local-projections')

    async def close(self):
        await self.writer.run(self._close)

    async def _close(self):
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            self.task = None
        self.blocked.clear()

    def key(self, root):
        return self.store_id, root, 'live'

    def statistics(self):
        return dict(enabled=True, running=self.task is not None and not self.task.done(),
                    accepted_order=self.accepted_order, observed_order=self.observed_order,
                    durable_order=self.durable_order, updates=self.updates,
                    failures=self.failures, last_error=self.last_error,
                    deferred_roots=len(self.blocked))

    async def _run(self):
        more = False
        while True:
            async with self.writer.condition:
                await self.writer.condition.wait_for(
                    lambda: more or self._version() != self.revision or self.writer.state != 'open')
                if self.writer.state != 'open':
                    return
            await asyncio.sleep(self.interval)
            revision = self._version()
            try:
                more = await self._update()
            except Exception as error:
                # An unavailable UI accelerator must never fault persistence.
                self.failures += 1
                self.last_error = type(error).__name__
                logger.warning('Local projection update failed: %s', type(error).__name__)
                more = False
            self.revision = revision

    async def _update(self):
        records = []
        async with self.queries.view('accepted', control=True, event_records=records) as (raw, overlay, _, frontier):
            metadata = meta.tables['persistence_meta']
            durable, store_id = (await raw.execute(select(metadata.c.last_store_order, metadata.c.store_id))).one()
            if self.store_id is not None and self.store_id != store_id:
                self.durable_order = 0
                self.blocked.clear()
            self.store_id = store_id
            self.accepted_order = frontier
            invs = meta.tables['invocations']
            heads = (await raw.execute(select(invs.c.root_invocation_id, invs.c.updated_store_order)
                .where(invs.c.updated_store_order > self.durable_order)
                .order_by(invs.c.updated_store_order).limit(256))).all()
            roots = {row.root_invocation_id for row in heads}
            pending_invocations = overlay.rows['invocations']
            root_by_id = {row['invocation_id']: row['root_invocation_id']
                          for row in pending_invocations.values() if row}
            sessions = defaultdict(set)
            tails = defaultdict(list)
            for entry, light in records:
                root = root_by_id.get(light['invocation_id'])
                if root is not None:
                    sessions[light['session_id']].add(root)
                    roots.add(root)
            for entry, light in records:
                targets = (sessions[light['session_id']] if light['invocation_id'] is None
                           else (root_by_id[light['invocation_id']],))
                for root in targets:
                    tails[root].append((light, entry.order, entry.binding))
            connection = ProjectionConnection(raw, self.cache, durable, store_id)
            now = time.monotonic()
            for root in roots:
                if self.blocked.get(root, 0) > now:
                    continue
                self.blocked.pop(root, None)
                try:
                    await self.cache.build(connection, root, frontier, store_id, tails=tails)
                    entry = self.cache.entries.get((store_id, root, 'live'))
                    if entry is not None and entry['order'] == frontier:
                        self.updates += 1
                        continue
                except Exception as error:
                    self.failures += 1
                    self.last_error = type(error).__name__
                    logger.warning('Local projection build failed: %s', type(error).__name__)
                # Oversized or broken roots are retried on later Events after a
                # cooldown, rather than rebuilding their full history per Event.
                self.blocked[root] = now + self.cache.config.projection_cache_ttl_seconds
                while len(self.blocked) > 256:
                    self.blocked.popitem(last=False)
            self.observed_order = frontier
            self.durable_order = heads[-1].updated_store_order if len(heads) == 256 else durable
            return len(heads) == 256

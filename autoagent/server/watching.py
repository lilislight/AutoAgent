"""Bounded, coalesced SSE heads. Slow clients retain one notification only."""
import asyncio
import time
from contextlib import suppress

from autoagent.host import HostCapacityError


class TraceWatchHub:
    def __init__(self, host, settings):
        self.host, self.settings = host, settings
        self.groups = {}
        self.count = 0
        self.closed = False

    def subscribe(self, iid, initial):
        if self.closed or self.count >= self.settings.trace_max_subscribers:
            raise HostCapacityError('Trace subscription capacity is exhausted.')
        group = self.groups.get(iid)
        if group is None:
            group = {'queues': set(), 'task': None}
            self.groups[iid] = group
        queue = asyncio.Queue(maxsize=1)
        group['queues'].add(queue)
        self.count += 1
        queue.put_nowait(('update', initial))
        if group['task'] is None or group['task'].done():
            group['task'] = asyncio.create_task(self._run(iid, group, initial))
        return queue

    async def unsubscribe(self, iid, queue):
        group = self.groups.get(iid)
        if group is None or queue not in group['queues']:
            return
        group['queues'].remove(queue)
        self.count -= 1
        if not group['queues']:
            self.groups.pop(iid, None)
            group['task'].cancel()
            with suppress(asyncio.CancelledError):
                await group['task']

    def _publish(self, group, kind, head):
        for queue in group['queues']:
            if queue.full():
                queue.get_nowait()
            queue.put_nowait((kind, head))

    async def _run(self, iid, group, head):
        published = time.monotonic()
        signature = lambda row: (row['store_id'], row['invocation_version'], row['family_version'], row['active'])
        try:
            # A terminal Invocation can be recovered in place. Keep its visible
            # observer attached so that a later reopen is delivered too.
            while not self.closed:
                remaining = self.settings.trace_heartbeat_seconds - (time.monotonic() - published)
                revision = await self.host.wait_trace_commit(head['revision'], timeout=max(.01, min(30, remaining)))
                if revision != head['revision'] and self.settings.trace_coalesce_ms:
                    await asyncio.sleep(self.settings.trace_coalesce_ms / 1000)
                latest = await asyncio.wait_for(self.host.get_trace_head(iid), self.settings.query_timeout_seconds)
                changed = signature(latest) != signature(head)
                if changed or time.monotonic() - published >= self.settings.trace_heartbeat_seconds:
                    self._publish(group, 'update' if changed else 'heartbeat', latest)
                    published = time.monotonic()
                head = latest
        except asyncio.CancelledError:
            raise
        except Exception:
            self._publish(group, 'unavailable', {'message': 'Live updates are unavailable. Reconnect to retry.'})

    async def close(self):
        self.closed = True
        tasks = [group['task'] for group in self.groups.values()]
        # Cancelling the producers alone leaves StreamingResponse waiting on
        # queue.get(). Wake every consumer before draining its producer.
        for group in self.groups.values():
            self._publish(group, 'closed', {})
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.groups.clear()
        self.count = 0

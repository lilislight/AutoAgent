"""Generic durable and live User Event delivery over Host APIs."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Callable
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask
from autoagent.host import UserEventStreamOverflow

class _UserEventHub:
    """Own this Router's subscriptions, without owning the borrowed Host."""

    def __init__(self):
        self.closed = False
        self.reservations = set()
        self.cleanups = {}

    async def close(self):
        self.closed = True
        await asyncio.gather(*(cleanup() for cleanup in tuple(self.cleanups.values())))

    @asynccontextmanager
    async def lifespan(self, app):
        hubs = getattr(app.state, '_autoagent_user_event_hubs', None)
        if hubs is None:
            hubs = app.state._autoagent_user_event_hubs = set()
        hubs.add(self)
        try:
            yield
        finally:
            await self.close()
            hubs.discard(self)


@dataclass
class UserEventRoutes:
    router: APIRouter
    history: Callable
    watch: Callable


def user_event_routes(host, settings, query):
    hub = _UserEventHub()
    router = APIRouter(lifespan=hub.lifespan)
    subscribers = hub.reservations

    @router.get('/user-events')
    async def history(invocation_id: str = Query(min_length=1, max_length=4096),
                      after: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=1000)):
        page = await query(host.get_user_events(invocation_id, after=after, limit=limit), settings)
        return dict(items=page.items, next_after=page.items[-1]['sequence'] if len(page.items) == limit else None)


    @router.get('/user-events/watch')
    async def watch(invocation_id: str | None = Query(None, min_length=1, max_length=4096),
                    session_id: str | None = Query(None, min_length=1, max_length=512)):
        if (invocation_id is None) == (session_id is None):
            raise HTTPException(400, detail="Choose exactly one Invocation or Session.")
        if hub.closed:
            raise HTTPException(503, detail="User Event delivery is stopping.")
        if len(subscribers) >= settings.user_event_max_subscribers:
            raise HTTPException(503, detail="User Event subscription capacity is exhausted.")
        scope = object()
        subscribers.add(scope)
        def release():
            subscribers.discard(scope)
            hub.cleanups.pop(scope, None)
        subscription = host.subscribe_user_events(invocation_id, session_id=session_id, capacity=settings.user_event_buffer)
        try:
            stream = await query(subscription.__aenter__(), settings)
        except BaseException:
            release()
            raise
        closed = False
        async def cleanup():
            nonlocal closed
            if closed:
                return
            closed = True
            try:
                await subscription.__aexit__(None, None, None)
            finally:
                release()
        hub.cleanups[scope] = cleanup
        if hub.closed:
            await cleanup()
            raise HTTPException(503, detail="User Event delivery is stopping.")
        async def events():
            pending = None
            try:
                yield 'retry: 1500\nevent: ready\ndata: {}\n\n'
                while True:
                    pending = pending or asyncio.create_task(anext(stream))
                    try:
                        event = await asyncio.wait_for(asyncio.shield(pending), settings.user_event_heartbeat_seconds)
                    except TimeoutError:
                        yield ': heartbeat\n\n'
                        continue
                    pending = None
                    yield 'event: user_event\ndata: ' + json.dumps(event.to_record(), separators=(',', ':')) + '\n\n'
            except UserEventStreamOverflow:
                yield 'event: reset\ndata: {"reason":"overflow"}\n\n'
            except StopAsyncIteration:
                return
            finally:
                if pending is not None:
                    pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
                await cleanup()
        return StreamingResponse(events(), media_type='text/event-stream', background=BackgroundTask(cleanup),
            headers={'Cache-Control': 'no-cache, no-transform', 'X-Accel-Buffering': 'no'})


    return UserEventRoutes(router, history, watch)

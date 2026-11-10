"""Agent App read models and generic User Event delivery over existing Host APIs."""

import asyncio
import json
import base64
from contextlib import asynccontextmanager

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field
from starlette.background import BackgroundTask

from autoagent.agent import AgentSessionSettings
from autoagent.host import HostNotFoundError, UserEventStreamOverflow
from autoagent.host.persistence import ConflictError


class ConfigureAgent(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    session_id: str = Field(min_length=1, max_length=512)
    expected_version: int = Field(ge=0)
    settings: AgentSessionSettings


class AgentMessage(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    session_id: str = Field(min_length=1, max_length=512)
    message: str = Field(min_length=1)
    workspace_id: str | None = Field(None, min_length=1, max_length=512)


class QueuedAgentMessage(AgentMessage):
    request_id: str = Field(min_length=1, max_length=64, pattern=r"^[a-zA-Z0-9_-]+$")
    send_to: str | None = Field(None, min_length=1, max_length=512)


class QueueAction(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    session_id: str = Field(min_length=1, max_length=512)
    item_id: str = Field(min_length=1, max_length=64)
    action: str = Field(pattern="^(cancel|send_now|continue)$")
    invocation_id: str | None = Field(None, min_length=1, max_length=512)


class CreateWorkspace(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    name: str = Field(min_length=1, max_length=128)
    path: str = Field(min_length=1, max_length=4096)
    create_directory: bool = False


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


def agent_router(host, settings, query):
    from .app import _browse_cursor, _browse_encode
    hub = _UserEventHub()
    router = APIRouter(lifespan=hub.lifespan)
    subscribers = hub.reservations

    async def agent_query(operation):
        try:
            return await query(operation, settings)
        except ConflictError as error:
            raise HTTPException(409, detail=str(error)) from error

    @router.get('/agent/catalog')
    async def catalog():
        return await query(host.agent_context.workspace_catalog(), settings)

    @router.post('/agent/messages', status_code=202)
    async def send_message(body: AgentMessage):
        ref = await agent_query(host.agent_context.start_conversation(body.session_id, body.message, body.workspace_id))
        return dict(accepted=True, invocation_id=ref.invocation_id, session_id=ref.session_id)

    @router.get('/agent/queue')
    async def message_queue(session_id: str = Query(min_length=1, max_length=512)):
        return await agent_query(host.agent_context.messages.view(session_id))

    @router.post('/agent/queue', status_code=202)
    async def queue_message(body: QueuedAgentMessage):
        return await agent_query(host.agent_context.messages.submit(body.session_id, body.message,
            body.request_id, body.workspace_id, send_to=body.send_to))

    @router.post('/agent/queue/actions')
    async def queue_action(body: QueueAction):
        return await agent_query(host.agent_context.messages.act(body.session_id, body.item_id,
            body.action, body.invocation_id))

    @router.get('/agent/sessions')
    async def sessions(cursor: str | None = Query(None, max_length=4096),
                       workspace_id: str | None = Query(None, min_length=1, max_length=512), unbound: bool = False,
                       limit: int = Query(20, ge=1, le=100)):
        from autoagent.agent import BUILTIN_AGENT_WORKFLOW_ID
        scope = ["agent.sessions", BUILTIN_AGENT_WORKFLOW_ID]
        if unbound and workspace_id is not None:
            raise HTTPException(400, detail="Choose a Workspace or unbound conversations.")
        if workspace_id is not None or unbound:
            scope.append(workspace_id)
        position = _browse_cursor(cursor, scope)
        page = await query(host.agent_context.list_conversation_sessions(workspace_id, limit=limit, cursor=position)
            if workspace_id is not None or unbound else host.list_execution_sessions(workflow_ids=(BUILTIN_AGENT_WORKFLOW_ID,), limit=limit, cursor=position), settings)
        async def describe(row):
            if row['workflow_count'] != 1:
                return None
            try:
                info = await host.agent_context.workspace_session(row['session_id'])
            except HostNotFoundError:
                return None
            if info['parent_invocation_id'] is not None:
                return None
            invocations = await host.list_session_invocations(row['session_id'], limit=1, workflow_ids=(BUILTIN_AGENT_WORKFLOW_ID,))
            if not invocations.items:
                return None
            current = invocations.items[0]
            events = await host.get_user_events(current['invocation_id'], limit=1)
            if not events.items or events.items[0]['kind'] != 'agent.messages':
                return None
            message = events.items[0]['payload']['messages'][0]
            title = next((part['text'] for part in message['content'] if part.get('type') == 'text'), 'Chat')
            return dict(session_id=row['session_id'], title=title[:100], invocation_id=current['invocation_id'],
                updated_at_us=row['updated_at_us'], invocation_count=row['invocation_count'])
        items = await query(asyncio.gather(*(describe(row) for row in page.items)), settings)
        next_cursor = _browse_encode(page.next_cursor, scope)
        return dict(items=[item for item in items if item is not None], next_cursor=next_cursor)

    @router.get('/agent/invocations')
    async def invocations(session_id: str = Query(min_length=1, max_length=512),
                          cursor: str | None = Query(None, max_length=4096)):
        from autoagent.agent import BUILTIN_AGENT_WORKFLOW_ID
        await query(host.agent_context.workspace_session(session_id), settings)
        scope = ["agent.invocations", session_id]
        position = _browse_cursor(cursor, scope)
        page = await query(host.list_session_invocations(session_id, workflow_ids=(BUILTIN_AGENT_WORKFLOW_ID,), limit=20, cursor=position), settings)
        next_cursor = _browse_encode(page.next_cursor, scope)
        return dict(items=page.items, next_cursor=next_cursor)

    @router.get('/agent/settings')
    async def get_settings(session_id: str = Query(min_length=1, max_length=512),
                           workspace_id: str | None = Query(None, min_length=1, max_length=512)):
        return await agent_query(host.agent_context.conversation_settings(session_id, workspace_id))

    @router.put('/agent/settings')
    async def configure(body: ConfigureAgent):
        await query(host.agent_context.workspace_session(body.session_id, allow_new=True), settings)
        try:
            return await query(host.agent_context.configure(body.session_id, body.settings,
                expected_version=body.expected_version), settings)
        except ConflictError as error:
            raise HTTPException(409, detail=str(error)) from error

    @router.get('/agent/resources')
    async def resources(revision_id: str = Query(min_length=1, max_length=4096),
                        session_id: str | None = Query(None, min_length=1, max_length=512),
                        workspace_id: str | None = Query(None, min_length=1, max_length=512)):
        return await agent_query(host.agent_context.workspace_resources(revision_id, builtin=True, session_id=session_id, workspace_id=workspace_id))

    @router.get('/agent/workspaces')
    async def workspaces():
        return dict(items=await query(host.agent_context.list_workspaces(), settings))

    @router.post('/agent/workspaces', status_code=201)
    async def create_workspace(body: CreateWorkspace):
        try:
            return await query(host.agent_context.create_workspace(**body.model_dump()), settings)
        except OSError as error:
            raise HTTPException(400, detail=f"Cannot prepare Workspace directory: {error.strerror or str(error)}") from error

    @router.get('/agent/execution')
    async def execution(invocation_id: str = Query(min_length=1, max_length=4096)):
        family, snapshot = await asyncio.gather(query(host.agent_context.workspace_invocation(invocation_id), settings),
            query(host.agent_context.get_snapshot(invocation_id), settings))
        root = family['root'] or family['current']
        controls, children = await asyncio.gather(query(host.get_execution_controls(root['invocation_id']), settings),
            query(host.list_child_executions(invocation_id, limit=100, order="asc"), settings))
        if controls['next_cursor']:
            controls['next_cursor'] = base64.urlsafe_b64encode(json.dumps([root['invocation_id'], controls['next_cursor']]).encode()).decode().rstrip('=')
        return dict(current=family['current'], root=root, ancestors=family['ancestors'],
            children=children.items, children_truncated=children.next_cursor is not None,
            controls=controls, snapshot=snapshot)

    @router.get('/agent/usage')
    async def usage(session_id: str = Query(min_length=1, max_length=512),
                    invocation_id: str | None = Query(None, min_length=1, max_length=4096)):
        await query(host.agent_context.workspace_session(session_id, allow_new=True), settings)
        if invocation_id:
            family = await query(host.agent_context.workspace_invocation(invocation_id), settings)
            if family['current']['session_id'] != session_id:
                raise HTTPException(400, detail="Execution belongs to another conversation.")
        session = await query(host.token_usage(session_id=session_id), settings)
        invocation = await query(host.token_usage(invocation_id=invocation_id, include_descendants=True), settings) if invocation_id else None
        return dict(session=session, invocation=invocation)

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

    @router.get('/agent/history')
    async def agent_history(invocation_id: str = Query(min_length=1, max_length=4096),
                            after: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=1000)):
        await query(host.agent_context.workspace_invocation(invocation_id), settings)
        return await history(invocation_id, after, limit)

    @router.get('/agent/events/watch')
    async def agent_watch(session_id: str = Query(min_length=1, max_length=512)):
        await query(host.agent_context.workspace_session(session_id, allow_new=True), settings)
        return await watch(invocation_id=None, session_id=session_id)

    return router

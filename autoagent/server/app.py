"""Independent FastAPI application and borrowed-Host APIRouter."""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import hashlib
from typing import Annotated
from contextlib import asynccontextmanager
from collections.abc import Awaitable

from fastapi import APIRouter, FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import Field

from autoagent import RuntimeTransitionError
from autoagent.host import AutoAgentHost, HostCapacityError, HostError, HostNotFoundError, HostRunConflict
from autoagent.host.persistence.models import CapacityError
from autoagent.host.persistence.trace_cursor import TraceResetRequired
from .watching import TraceWatchHub
from .operations import ExecutionOperations
from .models import ExecutionActionRequest
from .models import TracePage, WorkflowGraph, WorkflowCatalogEntry, WorkflowCatalogPage, WorkflowDetail, WorkflowPage, WorkflowSummary, RunWorkflowRequest
from .settings import ServerSettings
from .ui import mount_ui


def _cursor(value, workflow_id, history):
    if value is None:
        return None
    try:
        if len(value) > 2048:
            raise ValueError
        decoded = json.loads(base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True))
        if not isinstance(decoded, dict) or set(decoded) != {"v", "workflow_id", "history", "position"}:
            raise ValueError
        if type(decoded["v"]) is not int or decoded["v"] != 1 or decoded["workflow_id"] != workflow_id or decoded["history"] is not history:
            raise ValueError
        position = decoded["position"]
        if not isinstance(position, list) or len(position) != 2 or not isinstance(position[1], str) or not position[1]:
            raise ValueError
        if history:
            if type(position[0]) is not int or position[0] < 0:
                raise ValueError
        elif not isinstance(position[0], str) or not position[0]:
            raise ValueError
        return tuple(position)
    except (ValueError, TypeError, binascii.Error, UnicodeDecodeError):
        raise HTTPException(400, detail={"code": "invalid_cursor", "message": "Invalid cursor or cursor belongs to another filter."}) from None


def _encode_cursor(position, workflow_id, history):
    if position is None:
        return None
    body = json.dumps({"v": 1, "workflow_id": workflow_id, "history": history, "position": position}, separators=(",", ":"))
    return base64.urlsafe_b64encode(body.encode()).decode().rstrip("=")


async def _query(operation: Awaitable, settings: ServerSettings):
    try:
        return await asyncio.wait_for(operation, settings.query_timeout_seconds)
    except HostNotFoundError as exc:
        raise HTTPException(404, detail={"code": "not_found", "message": str(exc)}) from exc
    except (HostRunConflict, RuntimeTransitionError) as exc:
        raise HTTPException(409, detail={"code": exc.code, "message": str(exc)}) from exc
    except (HostCapacityError, CapacityError) as exc:
        raise HTTPException(503, detail={"code": "host_busy", "message": str(exc)}) from exc
    except HostError as exc:
        raise HTTPException(503, detail={"code": "host_unavailable", "message": str(exc)}) from exc
    except TimeoutError as exc:
        raise HTTPException(504, detail={"code": "query_timeout", "message": "Workflow query timed out."}) from exc
    except TraceResetRequired as exc:
        raise HTTPException(409, detail={"code": "trace_reset", "message": str(exc)}) from exc
    except ValueError as exc:
        raise HTTPException(400, detail={"code": "invalid_request", "message": str(exc)}) from exc


def _browse_cursor(value, scope):
    if value is None:
        return None
    try:
        decoded = json.loads(base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True))
        if not isinstance(decoded, dict) or set(decoded) != {"v", "scope", "position"} or type(decoded["v"]) is not int or decoded["v"] != 1 or decoded["scope"] != scope:
            raise ValueError
        position = decoded["position"]
        if scope[0] == "catalog":
            if not isinstance(position, list) or len(position) != 3 or type(position[0]) is not int or position[0] not in (0, 1) or type(position[1]) is not int or position[1] > 0 or not isinstance(position[2], str) or not position[2]:
                raise ValueError
        elif not isinstance(position, list) or len(position) != 2 or type(position[0]) is not int or position[0] < 0 or not isinstance(position[1], str) or not position[1]:
            raise ValueError
        return tuple(position)
    except (ValueError, TypeError, binascii.Error, UnicodeDecodeError):
        raise HTTPException(400, detail={"code": "invalid_cursor", "message": "Invalid cursor or cursor belongs to another search."}) from None


def _browse_encode(position, scope):
    if position is None:
        return None
    body = json.dumps({"v": 1, "scope": scope, "position": position}, separators=(",", ":"))
    return base64.urlsafe_b64encode(body.encode()).decode().rstrip("=")


def _filter_scope(workflows, revisions):
    # Keep cursors bounded even when many long IDs are selected.
    return hashlib.sha256(json.dumps([workflows, revisions], separators=(",", ":")).encode()).hexdigest()


def create_router(host: AutoAgentHost, *, settings: ServerSettings | None = None) -> APIRouter:
    """Use a Host already entered in the enclosing FastAPI application's lifespan.

    No Host is opened or closed by this router. Include it before mounting UI.
    """
    settings = settings or ServerSettings.from_environment(host.environment, root=host.root)
    hub = TraceWatchHub(host, settings)
    operations = ExecutionOperations(host, settings)
    @asynccontextmanager
    async def watch_lifespan(_app):
        try:
            yield
        finally:
            await operations.close()
            await hub.close()
    router = APIRouter(prefix=settings.api_prefix, tags=["AutoAgent Workflows"], lifespan=watch_lifespan)

    @router.get('/execution-controls')
    async def execution_controls(invocation_id: str=Query(min_length=1,max_length=4096),cursor: str|None=Query(None,max_length=16384)):
        position=None
        if cursor:
            try:
                scope,position=json.loads(base64.urlsafe_b64decode(cursor+'='*(-len(cursor)%4)))
                if scope!=invocation_id or not isinstance(position,list) or len(position)!=2 or not all(isinstance(s,str) for s in position):
                    raise ValueError
            except (ValueError,TypeError,binascii.Error):
                raise HTTPException(400,detail='Invalid response cursor.') from None
        result=await _query(host.get_execution_controls(invocation_id,cursor=position),settings)
        if result['next_cursor']:
            result['next_cursor']=base64.urlsafe_b64encode(json.dumps([invocation_id,result['next_cursor']]).encode()).decode().rstrip('=')
        return result

    @router.get('/execution-action-spec')
    async def execution_action_spec(invocation_id: str=Query(min_length=1,max_length=4096),action: str=Query(pattern='^(respond|signal)$'),
        target_invocation_id: str|None=Query(None,max_length=4096),wait_id: str|None=Query(None,max_length=4096),
        endpoint: str|None=Query(None,max_length=512),source_id: str|None=Query(None,max_length=128)):
        return await _query(host.get_execution_action_spec(invocation_id,action,target_invocation_id=target_invocation_id,
            wait_id=wait_id,endpoint=endpoint,source_id=source_id),settings)

    @router.post('/execution-actions',status_code=202)
    async def execution_action(body: ExecutionActionRequest):
        try:
            return await _query(operations.submit(body.model_dump(mode='json')),settings)
        except TypeError as exc:
            raise HTTPException(422,detail={'code':'invalid_input','message':str(exc)}) from exc

    @router.get('/execution-operations/{operation_id}')
    async def execution_operation(operation_id: str):
        try:
            return operations.get(operation_id)
        except HostNotFoundError as exc:
            raise HTTPException(404,detail={'code':'operation_unavailable','message':str(exc)}) from exc

    @router.get('/workflow-run-spec')
    async def workflow_run_spec(workflow_id: str = Query(min_length=1, max_length=512),
        revision_id: str = Query(min_length=1, max_length=4096), session_id: str | None = Query(None, min_length=1, max_length=512),
        entry_node_id: str | None = Query(None, min_length=1, max_length=4096)):
        return await _query(host.get_workflow_run_spec(workflow_id, revision_id=revision_id, session_id=session_id, entry_node_id=entry_node_id), settings)

    @router.post('/executions', status_code=202)
    async def run_workflow(body: RunWorkflowRequest):
        try:
            ref = await _query(host.execute_json(body.workflow_id, body.input,
                revision_id=body.revision_id, entry_node_id=body.entry_node_id,
                session_id=body.session_id, session_context=body.session_context,
                require_existing_session=body.session_mode == 'existing'), settings)
        except TypeError as exc:
            raise HTTPException(422, detail={"code": "invalid_input", "message": str(exc)}) from exc
        return dict(accepted=True, invocation_id=ref.invocation_id, session_id=ref.session_id,
            workflow_id=ref.workflow_id, revision_id=ref.workflow_revision_id)

    @router.get("/workflow-catalog", response_model=WorkflowCatalogPage)
    async def catalog(
        q: str = Query("", max_length=512),
        limit: int = Query(settings.page_size, ge=1, le=settings.max_page_size),
        cursor: str | None = Query(None, max_length=16384),
    ):
        search = q.strip().lower()
        scope = ["catalog", search]
        page = await _query(host.browse_workflows(search=search, limit=limit, cursor=_browse_cursor(cursor, scope)), settings)
        return WorkflowCatalogPage(items=[WorkflowCatalogEntry.model_validate(row) for row in page.items], next_cursor=_browse_encode(page.next_cursor, scope))

    @router.get("/workflow-revisions", response_model=WorkflowPage)
    async def revisions(
        workflow_id: str = Query(min_length=1, max_length=512),
        prefix: str | None = Query(None, max_length=512),
        limit: int = Query(settings.page_size, ge=1, le=settings.max_page_size),
        cursor: str | None = Query(None, max_length=16384),
    ):
        prefix = prefix.strip().lower() if prefix else None
        scope = ["revisions", workflow_id, prefix]
        page = await _query(host.list_workflow_revisions(workflow_id, prefix=prefix, limit=limit, cursor=_browse_cursor(cursor, scope)), settings)
        return WorkflowPage(items=[WorkflowSummary.model_validate(row) for row in page.items], next_cursor=_browse_encode(page.next_cursor, scope), frontier=page.frontier)

    @router.get("/workflows", response_model=WorkflowPage)
    async def workflows(
        workflow_id: str | None = Query(None, min_length=1, max_length=512),
        include_history: bool = False,
        limit: int = Query(settings.page_size, ge=1, le=settings.max_page_size),
        cursor: str | None = Query(None, max_length=2048),
    ):
        page = await _query(host.list_workflows(
            workflow_id=workflow_id, include_history=include_history, limit=limit,
            cursor=_cursor(cursor, workflow_id, include_history),
        ), settings)
        return WorkflowPage(
            items=[WorkflowSummary.model_validate(row) for row in page.items],
            next_cursor=_encode_cursor(page.next_cursor, workflow_id, include_history),
            frontier=page.frontier,
        )

    @router.get("/workflow-graph", response_model=WorkflowGraph)
    async def workflow_graph(revision_id: str = Query(min_length=1, max_length=4096)):
        return await _query(host.get_workflow_graph(revision_id), settings)

    @router.get("/workflow-object")
    async def workflow_object(revision_id: str = Query(min_length=1, max_length=4096), kind: str = Query(pattern="^(node|edge)$"), identity: str = Query(min_length=1, max_length=4096)):
        return await _query(host.get_workflow_object(revision_id, kind, identity), settings)

    def trace_page(page, scope):
        return TracePage(items=list(page.items), next_cursor=_browse_encode(page.next_cursor, scope), frontier=page.frontier, total_count=page.total_count)

    @router.get("/execution-sessions", response_model=TracePage)
    async def execution_sessions(
        q: str = Query("", max_length=512),
        workflow_id: list[Annotated[str, Field(min_length=1, max_length=512)]] | None = Query(None, max_length=64),
        revision_id: list[Annotated[str, Field(min_length=1, max_length=4096)]] | None = Query(None, max_length=64),
        session_id: str | None = Query(None, min_length=1, max_length=4096),
        started_after_us: int | None = Query(None, ge=0, le=9007199254740991),
        started_before_us: int | None = Query(None, ge=0, le=9007199254740991),
        limit: int = Query(settings.page_size, ge=1, le=settings.max_page_size),
        cursor: str | None = Query(None, max_length=16384),
    ):
        workflows, revisions = sorted(set(workflow_id or ())), sorted(set(revision_id or ()))
        scope = ["execution-sessions", q.strip().lower(), _filter_scope(workflows, revisions), session_id, started_after_us, started_before_us]
        page = await _query(host.list_execution_sessions(
            search=scope[1], workflow_ids=workflows, revision_ids=revisions,
            session_id=session_id, started_after_us=started_after_us, started_before_us=started_before_us, limit=limit, cursor=_browse_cursor(cursor, scope),
        ), settings)
        return trace_page(page, scope)

    @router.get("/session-invocations", response_model=TracePage)
    async def session_invocations(
        session_id: str = Query(min_length=1, max_length=4096),
        q: str = Query("", max_length=512),
        workflow_id: list[Annotated[str, Field(min_length=1, max_length=512)]] | None = Query(None, max_length=64),
        revision_id: list[Annotated[str, Field(min_length=1, max_length=4096)]] | None = Query(None, max_length=64),
        started_after_us: int | None = Query(None, ge=0, le=9007199254740991),
        started_before_us: int | None = Query(None, ge=0, le=9007199254740991),
        limit: int = Query(settings.page_size, ge=1, le=settings.max_page_size),
        cursor: str | None = Query(None, max_length=16384),
    ):
        workflows, revisions = sorted(set(workflow_id or ())), sorted(set(revision_id or ()))
        scope = ["session-invocations", session_id, q.strip().lower(), _filter_scope(workflows, revisions), started_after_us, started_before_us]
        page = await _query(host.list_session_invocations(
            session_id, search=scope[2], workflow_ids=workflows, revision_ids=revisions,
            started_after_us=started_after_us, started_before_us=started_before_us, limit=limit, cursor=_browse_cursor(cursor, scope),
        ), settings)
        return trace_page(page, scope)

    @router.get("/executions", response_model=TracePage)
    async def executions(q: str = Query("", max_length=512), status: str | None = None, limit: int = Query(settings.page_size, ge=1, le=settings.max_page_size), cursor: str | None = Query(None, max_length=16384)):
        scope = ["executions", q.strip().lower(), status]
        page = await _query(host.list_executions(search=scope[1], status=status, limit=limit, cursor=_browse_cursor(cursor, scope)), settings)
        return trace_page(page, scope)

    @router.get("/execution-trace")
    async def execution_trace(invocation_id: str = Query(min_length=1, max_length=4096)):
        return await _query(host.get_execution_trace(invocation_id), settings)

    @router.get('/execution-family')
    async def execution_family(invocation_id: str = Query(min_length=1, max_length=4096)):
        return await _query(host.get_execution_family(invocation_id), settings)

    @router.get('/trace-head')
    async def trace_head(invocation_id: str = Query(min_length=1, max_length=4096)):
        head = await _query(host.get_trace_head(invocation_id), settings)
        return {k:v for k,v in head.items() if k != 'revision'}

    @router.get('/trace-watch')
    async def trace_watch(invocation_id: str = Query(min_length=1, max_length=4096)):
        head = await _query(host.get_trace_head(invocation_id), settings)
        try:
            queue = hub.subscribe(invocation_id, head)
        except HostCapacityError as exc:
            raise HTTPException(503, detail=str(exc)) from exc
        async def events():
            try:
                yield 'retry: 1500\n\n'
                while True:
                    kind, value = await queue.get()
                    body = {k:v for k,v in value.items() if k != 'revision'}
                    identity = f"id: {value['cursor']}\n" if 'cursor' in value else ''
                    yield f'{identity}event: {kind}\ndata: {json.dumps(body, separators=(",", ":"))}\n\n'
                    if kind == 'unavailable' or value.get('active') is False:
                        break
            finally:
                await hub.unsubscribe(invocation_id, queue)
        return StreamingResponse(events(), media_type='text/event-stream', headers={'Cache-Control': 'no-cache, no-transform', 'X-Accel-Buffering': 'no'})

    @router.get('/trace-dataset')
    async def trace_dataset(invocation_id: str = Query(min_length=1, max_length=4096),
        replay: bool = False, snapshot_id: str | None = Query(None, pattern='^[a-f0-9]{32}$'),
        since: str | None = Query(None, pattern='^[a-f0-9]{32}$'),
        cursor: str | None = Query(None, max_length=16384),
        through_sequence: int | None = Query(None, ge=0, le=9007199254740991)):
        return await _query(host.get_trace_dataset(invocation_id, replay=replay, snapshot_id=snapshot_id, since=since, cursor=cursor, through_sequence=through_sequence), settings)

    @router.delete('/trace-dataset')
    async def release_dataset(snapshot_id: str = Query(pattern='^[a-f0-9]{32}$')):
        await _query(host.release_trace_dataset(snapshot_id), settings)
        return {'released': True}

    @router.get('/trace-dataset-page')
    async def trace_dataset_page(snapshot_id: str = Query(pattern='^[a-f0-9]{32}$'),
        section: str = Query(pattern='^(node|call|wait|event|edge)$'), page: int = Query(ge=0)):
        return await _query(host.get_trace_dataset_page(snapshot_id, section, page), settings)

    @router.get('/execution-timeline')
    async def execution_timeline(invocation_id: str = Query(min_length=1, max_length=4096)):
        return await _query(host.get_execution_timeline(invocation_id), settings)

    @router.get('/timeline-items', response_model=TracePage)
    async def timeline_items(invocation_id: str = Query(min_length=1, max_length=4096),
        kind: str = Query('node', pattern='^(node|call|wait)$'),
        start_us: int = Query(ge=0, le=9007199254740991), end_us: int = Query(ge=0, le=9007199254740991),
        occurrence_id: str | None = Query(None, min_length=1, max_length=4096),
        node_id: str | None = Query(None, min_length=1, max_length=4096),
        limit: int = Query(settings.page_size, ge=1, le=settings.max_page_size),
        cursor: str | None = Query(None, max_length=16384)):
        scope = ['timeline-items', invocation_id, kind, start_us, end_us, occurrence_id, node_id]
        page = await _query(host.list_timeline_items(invocation_id, kind=kind, start_us=start_us,
            end_us=end_us, occurrence_id=occurrence_id, node_id=node_id, limit=limit, cursor=_browse_cursor(cursor, scope)), settings)
        return trace_page(page, scope)

    @router.get('/timeline-position')
    async def timeline_position(invocation_id: str = Query(min_length=1, max_length=4096),
        kind: str = Query(pattern='^(node|call)$'), identity: str = Query(min_length=1, max_length=4096),
        start_us: int = Query(ge=0, le=9007199254740991), end_us: int = Query(ge=0, le=9007199254740991),
        occurrence_id: str | None = Query(None, min_length=1, max_length=4096),
        node_id: str | None = Query(None, min_length=1, max_length=4096),
        limit: int = Query(settings.page_size, ge=1, le=settings.max_page_size),
        page_index: int | None = Query(None, ge=0, le=9007199254740991)):
        result = await _query(host.get_timeline_position(invocation_id, kind, identity,
            start_us=start_us, end_us=end_us, occurrence_id=occurrence_id, node_id=node_id,
            limit=limit, page_index=page_index), settings)
        scope = ['timeline-items', invocation_id, kind, start_us, end_us, occurrence_id, node_id]
        return dict(result, cursor=_browse_encode(result['cursor'], scope))

    @router.get('/timeline-event-groups')
    async def timeline_event_groups(invocation_id: str = Query(min_length=1, max_length=4096),
        start_us: int = Query(ge=0, le=9007199254740991), end_us: int = Query(ge=0, le=9007199254740991),
        buckets: int = Query(100, ge=1, le=512), through_sequence: int | None = Query(None, ge=0, le=9007199254740991)):
        return await _query(host.get_timeline_event_groups(invocation_id, start_us=start_us, end_us=end_us,
            buckets=buckets, through_sequence=through_sequence), settings)

    @router.get('/timeline-markers', response_model=TracePage)
    async def timeline_markers(invocation_id: str = Query(min_length=1, max_length=4096),
        start_us: int = Query(ge=0, le=9007199254740991), end_us: int = Query(ge=0, le=9007199254740991),
        through_sequence: int | None = Query(None, ge=0, le=9007199254740991),
        limit: int = Query(100, ge=1, le=settings.max_page_size), cursor: str | None = Query(None, max_length=16384)):
        scope = ['timeline-markers', invocation_id, start_us, end_us, through_sequence]
        page = await _query(host.list_timeline_markers(invocation_id, start_us=start_us, end_us=end_us, through_sequence=through_sequence,
            limit=limit, cursor=_browse_cursor(cursor, scope)), settings)
        return trace_page(page, scope)

    @router.get('/timeline-frame')
    async def timeline_frame(invocation_id: str = Query(min_length=1, max_length=4096),
        sequence: int | None = Query(None, ge=0, le=9007199254740991),
        at_us: int | None = Query(None, ge=0, le=9007199254740991), direction: int = Query(0, ge=-1, le=1)):
        return await _query(host.get_timeline_frame(invocation_id, sequence=sequence, at_us=at_us, direction=direction), settings)

    @router.get('/timeline-replay')
    async def timeline_replay(invocation_id: str = Query(min_length=1, max_length=4096),
        sequence: int | None = Query(None, ge=0, le=9007199254740991),
        at_us: int | None = Query(None, ge=0, le=9007199254740991),
        through_sequence: int | None = Query(None, ge=0, le=9007199254740991),
        limit: int = Query(min(200, settings.max_page_size), ge=1, le=settings.max_page_size)):
        return await _query(host.get_timeline_replay(invocation_id, sequence=sequence, at_us=at_us,
            through_sequence=through_sequence, limit=limit), settings)

    @router.get('/execution-record')
    async def execution_record(invocation_id: str = Query(min_length=1, max_length=4096),
        kind: str = Query(pattern='^(node|call|wait|edge)$'), identity: str = Query(min_length=1, max_length=4096)):
        return await _query(host.get_execution_record(invocation_id, kind, identity), settings)

    @router.get("/execution-items", response_model=TracePage)
    async def execution_items(invocation_id: str = Query(min_length=1, max_length=4096), kind: str = Query(pattern="^(node|call|edge|child|wait)$"), node_id: str | None = Query(None, max_length=4096), occurrence_id: str | None = Query(None, max_length=4096), call_id: str | None = Query(None, max_length=4096), edge_id: str | None = Query(None, max_length=4096), order: str = Query("desc", pattern="^(asc|desc)$"), limit: int = Query(settings.page_size, ge=1, le=settings.max_page_size), cursor: str | None = Query(None, max_length=16384)):
        if (kind == "node" and not node_id) or (kind == "edge" and not edge_id):
            raise HTTPException(400, detail="A Node or Edge ID is required.")
        if call_id is not None and kind != 'child':
            raise HTTPException(400, detail='A Call filter is only valid for child executions.')
        if order != 'desc' and kind != 'child':
            raise HTTPException(400, detail='Collection ordering is only supported for child executions.')
        scope = ["execution-items", invocation_id, kind, node_id, occurrence_id, call_id, edge_id, order]
        position = _browse_cursor(cursor, scope)
        if kind == "node":
            operation = host.list_node_executions(invocation_id, node_id, limit=limit, cursor=position)
        elif kind == "call":
            operation = host.list_operator_calls(invocation_id, occurrence_id=occurrence_id, limit=limit, cursor=position)
        elif kind == "wait":
            operation = host.list_execution_waits(invocation_id, occurrence_id=occurrence_id, limit=limit, cursor=position)
        elif kind == "edge":
            operation = host.list_edge_evaluations(invocation_id, edge_id, occurrence_id=occurrence_id, limit=limit, cursor=position)
        else:
            operation = host.list_child_executions(invocation_id, occurrence_id=occurrence_id, call_id=call_id, order=order, limit=limit, cursor=position)
        return trace_page(await _query(operation, settings), scope)

    @router.get('/execution-stages', response_model=TracePage)
    async def execution_stages(invocation_id: str = Query(min_length=1, max_length=4096), occurrence_id: str = Query(min_length=1, max_length=4096), limit: int = Query(20, ge=1, le=settings.max_page_size), cursor: str | None = Query(None, max_length=16384)):
        scope = ['execution-stages', invocation_id, occurrence_id]
        page = await _query(host.list_execution_stages(invocation_id, occurrence_id, limit=limit, cursor=_browse_cursor(cursor, scope)), settings)
        return trace_page(page, scope)

    @router.get('/execution-stage-value')
    async def execution_stage_value(invocation_id: str = Query(min_length=1, max_length=4096), occurrence_id: str = Query(min_length=1, max_length=4096), event_id: str = Query(min_length=1, max_length=4096), field: str = Query(pattern='^(mapped_input|output|patch|conditions|error)$')):
        return await _query(host.get_execution_stage_value(invocation_id, occurrence_id, event_id, field), settings)

    @router.get("/execution-value")
    async def execution_value(invocation_id: str = Query(min_length=1, max_length=4096), kind: str = Query(pattern="^(invocation|node|call|wait)$"), identity: str = Query(min_length=1, max_length=4096), field: str = Query(pattern="^(input|output|error|metrics|definition|request|response)$")):
        return await _query(host.get_execution_value(invocation_id, kind, identity, field), settings)

    @router.get("/workflow-revisions/{revision_id:path}", response_model=WorkflowDetail)
    async def revision(revision_id: str):
        workflow_id, separator, digest = revision_id.rpartition(":")
        if not workflow_id or not separator or not digest:
            raise HTTPException(400, detail={"code": "invalid_revision", "message": "Expected a Workflow Revision ID."})
        return await _query(host.get_workflow(workflow_id, revision_id=revision_id), settings)

    @router.api_route("/{unknown:path}", methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"], include_in_schema=False)
    async def unknown_api(unknown: str):
        raise HTTPException(404, detail={"code": "not_found", "message": "Unknown AutoAgent API route."})

    return router


def create_app(
    host: AutoAgentHost | None = None,
    *,
    project=".",
    settings: ServerSettings | None = None,
    ui: bool = True,
) -> FastAPI:
    """Create standalone API + UI, or borrow an already-open Host.

    Without `host`, Server owns a project Host and opens/closes it in lifespan.
    With `host`, the caller owns its lifecycle, just as for create_router.
    """
    owns_host = host is None
    host = host if host is not None else AutoAgentHost.from_project(project)
    settings = settings or ServerSettings.from_environment(host.environment, root=host.root)

    @asynccontextmanager
    async def lifespan(app):
        if owns_host:
            async with host:
                yield
        else:
            yield

    app = FastAPI(title="AutoAgent Local Server", version="2", lifespan=lifespan)
    app.state.autoagent_host = host
    app.state.autoagent_settings = settings
    app.include_router(create_router(host, settings=settings))
    if ui:
        mount_ui(app, settings=settings)
    return app

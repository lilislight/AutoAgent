from __future__ import annotations

import secrets
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

from fastapi import Cookie, Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.sse import EventSourceResponse, ServerSentEvent
from pydantic import BaseModel, ConfigDict, Field

from autoagent.core.app import AutoAgentApp
from autoagent.core.trace.models import (
    InvocationDetail,
    InvocationSummary,
    RuntimeEventPage,
    SessionSummary,
    TimelineView,
    TraceBootstrap,
    WorkflowGraphView,
    WorkflowSummary,
)
from autoagent.core.trace.service import TraceQueryService
from autoagent.core.runtime import RuntimeEvent


_AUTH_COOKIE = "autoagent_server_session"


class _AuthenticationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    token: str = Field(min_length=1)


class InvocationSubmitRequest(BaseModel):
    """Body for UI/API-triggered workflow execution."""

    model_config = ConfigDict(extra="forbid")

    input: dict[str, Any] | None = None
    session_id: str | None = None
    entry_node_id: str | None = None


class InvocationSubmitResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    workflow_id: str
    session_id: UUID
    invocation_id: UUID
    state: str


class InvocationResumeRequest(BaseModel):
    """Body for externally resuming a waiting workflow invocation."""

    model_config = ConfigDict(extra="forbid")

    session_id: str = Field(min_length=1)
    wait_key: str = Field(min_length=1)
    output: Any | None = None


class InvocationResumeResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    workflow_id: str
    session_id: UUID
    invocation_id: UUID
    state: str


class AutoAgentServer:
    """FastAPI adapter for AutoAgentApp execution and trace APIs.

    AutoAgentApp remains the framework center: it owns workflows, registries,
    executors, and the RuntimeStore. This server only exposes those capabilities
    through HTTP/SSE so the tracing UI can be deployed as a separate frontend.
    """

    def __init__(
        self,
        app: AutoAgentApp,
        *,
        execution_enabled: bool = True,
        poll_interval_ms: int = 200,
        manage_store_lifecycle: bool = False,
        access_token: str | None = None,
        secure_cookies: bool = False,
        redactor: Callable[[Any], Any] | None = None,
        bootstrap_event_limit: int = 1000,
        event_page_size: int = 1000,
    ) -> None:
        if poll_interval_ms <= 0:
            raise ValueError("poll_interval_ms must be positive.")
        if access_token is not None and not access_token:
            raise ValueError("access_token cannot be empty.")
        self.agent = app
        self.runtime_store = app.runtime_store
        self.execution_enabled = execution_enabled
        self.poll_interval_ms = poll_interval_ms
        self.manage_store_lifecycle = manage_store_lifecycle
        self.access_token = access_token
        self.secure_cookies = secure_cookies
        self.service = TraceQueryService(
            self.runtime_store,
            redactor=redactor,
            bootstrap_event_limit=bootstrap_event_limit,
            event_page_size=event_page_size,
        )
        self.api = self._build_api()

    def run(
        self,
        *,
        host: str = "0.0.0.0",
        port: int = 8765,
        reload: bool = False,
    ) -> None:
        """Start the API server. The tracing UI should run as a separate app."""

        import uvicorn

        uvicorn.run(self.api, host=host, port=port, reload=reload)

    def _build_api(self) -> FastAPI:
        @asynccontextmanager
        async def lifespan(_: FastAPI) -> AsyncIterator[None]:
            if self.manage_store_lifecycle:
                await self.runtime_store.ainitialize()
            try:
                yield
            finally:
                await self.agent.aclose()

        api = FastAPI(
            title="AutoAgent Server API",
            version="1",
            lifespan=lifespan,
        )

        def is_authenticated(
            authorization: str | None,
            session_cookie: str | None,
        ) -> bool:
            if self.access_token is None:
                return True
            candidate = session_cookie
            if authorization is not None and authorization.startswith("Bearer "):
                candidate = authorization[7:]
            return candidate is not None and secrets.compare_digest(
                candidate,
                self.access_token,
            )

        async def require_authentication(
            authorization: str | None = Header(default=None),
            session_cookie: str | None = Cookie(default=None, alias=_AUTH_COOKIE),
        ) -> None:
            if not is_authenticated(authorization, session_cookie):
                raise HTTPException(status_code=401, detail="Authentication required.")

        auth_dependencies = [Depends(require_authentication)]

        @api.get("/api/health")
        async def health(
            authorization: str | None = Header(default=None),
            session_cookie: str | None = Cookie(default=None, alias=_AUTH_COOKIE),
        ) -> dict[str, str | bool]:
            return {
                "status": "ok",
                "execution_enabled": self.execution_enabled,
                "authentication_required": self.access_token is not None,
                "authenticated": is_authenticated(authorization, session_cookie),
            }

        @api.post("/api/auth/session")
        async def create_authentication_session(
            body: _AuthenticationRequest,
            response: Response,
        ) -> dict[str, bool]:
            if self.access_token is not None and not secrets.compare_digest(
                body.token,
                self.access_token,
            ):
                raise HTTPException(status_code=401, detail="Invalid access token.")
            response.set_cookie(
                _AUTH_COOKIE,
                body.token,
                httponly=True,
                secure=self.secure_cookies,
                samesite="strict",
                max_age=28_800,
            )
            return {"authenticated": True}

        @api.delete("/api/auth/session")
        async def delete_authentication_session(response: Response) -> None:
            response.delete_cookie(_AUTH_COOKIE)

        @api.post(
            "/api/workflows/{workflow_id}/invocations",
            response_model=InvocationSubmitResponse,
            dependencies=auth_dependencies,
        )
        async def submit_invocation(
            workflow_id: str,
            body: InvocationSubmitRequest,
        ) -> InvocationSubmitResponse:
            if not self.execution_enabled:
                raise HTTPException(status_code=403, detail="Execution API is disabled.")
            entry = self.agent.workflow_registry.get(workflow_id)
            if entry is None:
                raise HTTPException(status_code=404, detail=f"Unknown Workflow: {workflow_id}")
            try:
                submitted = await self.agent.asubmit(
                    entry.workflow,
                    input=body.input,
                    session_id=body.session_id,
                    entry_node_id=body.entry_node_id,
                )
            except Exception as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            return InvocationSubmitResponse(
                workflow_id=workflow_id,
                session_id=submitted.session_id,
                invocation_id=submitted.invocation.id,
                state=submitted.invocation.state,
            )

        @api.post(
            "/api/workflows/{workflow_id}/resume",
            response_model=InvocationResumeResponse,
            dependencies=auth_dependencies,
        )
        async def resume_invocation(
            workflow_id: str,
            body: InvocationResumeRequest,
        ) -> InvocationResumeResponse:
            if not self.execution_enabled:
                raise HTTPException(status_code=403, detail="Execution API is disabled.")
            entry = self.agent.workflow_registry.get(workflow_id)
            if entry is None:
                raise HTTPException(status_code=404, detail=f"Unknown Workflow: {workflow_id}")
            kwargs: dict[str, Any] = {
                "session_id": body.session_id,
                "wait_key": body.wait_key,
            }
            if "output" in body.model_fields_set:
                kwargs["output"] = body.output
            try:
                resumed = await self.agent.aresume(entry.workflow, **kwargs)
                session = await self.runtime_store.afind_session(
                    namespace=self.agent.namespace,
                    workflow_id=workflow_id,
                    session_key=body.session_id,
                )
            except KeyError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except Exception as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            if session is None:
                raise HTTPException(status_code=404, detail="Session disappeared after resume.")
            return InvocationResumeResponse(
                workflow_id=workflow_id,
                session_id=session.id,
                invocation_id=resumed.id,
                state=resumed.state,
            )

        @api.get(
            "/api/workflows",
            response_model=tuple[WorkflowSummary, ...],
            dependencies=auth_dependencies,
        )
        async def workflows(namespace: str | None = None) -> tuple[WorkflowSummary, ...]:
            await self._save_registered_workflow_snapshots()
            return await self.service.list_workflows(namespace=namespace)

        @api.get(
            "/api/registered-workflows",
            response_model=tuple[WorkflowSummary, ...],
            dependencies=auth_dependencies,
        )
        async def registered_workflows(
            namespace: str | None = None,
        ) -> tuple[WorkflowSummary, ...]:
            """List only the compiled Workflow revisions executable by this App.

            ``/api/workflows`` is intentionally a durable-history endpoint.  It
            may contain revisions written by a previous process or an older
            application definition, which remain inspectable but cannot be
            invoked through this server.  The UI uses this endpoint for its
            Invoke dialog so a historical snapshot is never offered as an
            executable Workflow.
            """

            if namespace is not None and namespace != self.agent.namespace:
                return ()
            await self._save_registered_workflow_snapshots()
            registered_revisions = {
                (
                    entry.workflow_snapshot.workflow_id,
                    entry.workflow_snapshot.definition_hash,
                    entry.workflow_snapshot.operator_manifest_hash,
                )
                for entry in self.agent.workflow_registry.values()
            }
            snapshots = await self.service.list_workflows(
                namespace=self.agent.namespace,
            )
            return tuple(
                workflow
                for workflow in snapshots
                if (
                    workflow.workflow_id,
                    workflow.definition_hash,
                    workflow.operator_manifest_hash,
                )
                in registered_revisions
            )

        @api.get(
            "/api/workflows/{workflow_id}/versions/{definition_hash}",
            response_model=WorkflowGraphView,
            dependencies=auth_dependencies,
        )
        async def workflow_graph(
            workflow_id: str,
            definition_hash: str,
            namespace: str | None = None,
            operator_manifest_hash: str | None = None,
        ) -> WorkflowGraphView:
            await self._save_registered_workflow_snapshots()
            try:
                return await self.service.get_graph(
                    namespace=namespace,
                    workflow_id=workflow_id,
                    definition_hash=definition_hash,
                    operator_manifest_hash=operator_manifest_hash,
                )
            except KeyError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc

        @api.get(
            "/api/sessions",
            response_model=tuple[SessionSummary, ...],
            dependencies=auth_dependencies,
        )
        async def sessions(
            namespace: str | None = None,
            workflow_id: str | None = None,
        ) -> tuple[SessionSummary, ...]:
            return await self.service.list_sessions(
                namespace=namespace,
                workflow_id=workflow_id,
            )

        @api.get(
            "/api/sessions/{session_id}/invocations",
            response_model=tuple[InvocationSummary, ...],
            dependencies=auth_dependencies,
        )
        async def invocations(session_id: UUID) -> tuple[InvocationSummary, ...]:
            return await self.service.list_invocations(session_id)

        @api.get(
            "/api/sessions/{session_id}/invocations/{invocation_id}",
            response_model=InvocationDetail,
            dependencies=auth_dependencies,
        )
        async def invocation_detail(
            session_id: UUID,
            invocation_id: UUID,
        ) -> InvocationDetail:
            try:
                return await self.service.get_invocation(
                    session_id=session_id,
                    invocation_id=invocation_id,
                )
            except KeyError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc

        @api.get(
            "/api/sessions/{session_id}/invocations/{invocation_id}/timeline",
            response_model=TimelineView,
            dependencies=auth_dependencies,
        )
        async def invocation_timeline(
            session_id: UUID,
            invocation_id: UUID,
        ) -> TimelineView:
            try:
                return await self.service.get_timeline(
                    session_id=session_id,
                    invocation_id=invocation_id,
                )
            except KeyError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc

        @api.get(
            "/api/sessions/{session_id}/invocations/{invocation_id}/view",
            response_model=TraceBootstrap,
            dependencies=auth_dependencies,
        )
        async def invocation_view(
            session_id: UUID,
            invocation_id: UUID,
        ) -> TraceBootstrap:
            try:
                return await self.service.bootstrap(
                    session_id=session_id,
                    invocation_id=invocation_id,
                )
            except KeyError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc

        @api.get(
            "/api/sessions/{session_id}/invocations/{invocation_id}/events",
            response_model=RuntimeEventPage,
            dependencies=auth_dependencies,
        )
        async def invocation_events(
            session_id: UUID,
            invocation_id: UUID,
            after_sequence: int = Query(default=0, ge=0),
            before_sequence: int | None = Query(default=None, ge=1),
            limit: int = Query(default=1000, ge=1, le=9999),
            visibility: str | None = None,
        ) -> RuntimeEventPage:
            try:
                return await self.service.list_event_page(
                    session_id=session_id,
                    invocation_id=invocation_id,
                    after_sequence=after_sequence,
                    before_sequence=before_sequence,
                    limit=limit,
                    visibility=visibility,
                )
            except KeyError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc

        async def validate_invocation_scope(
            session_id: UUID,
            invocation_id: UUID,
        ) -> None:
            try:
                await self.service.get_invocation(
                    session_id=session_id,
                    invocation_id=invocation_id,
                )
            except KeyError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc

        @api.get(
            "/api/sessions/{session_id}/invocations/{invocation_id}/stream",
            response_class=EventSourceResponse,
            dependencies=auth_dependencies,
        )
        async def invocation_stream(
            request: Request,
            session_id: UUID,
            invocation_id: UUID,
            after_sequence: int = Query(default=0, ge=0),
            last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
            _scope: None = Depends(validate_invocation_scope),
        ) -> AsyncIterator[ServerSentEvent]:
            del _scope
            cursor = _resume_cursor(after_sequence, last_event_id)
            async for runtime_event in self._event_stream(
                request=request,
                session_id=session_id,
                invocation_id=invocation_id,
                after_sequence=cursor,
            ):
                yield runtime_event

        return api

    async def _event_stream(
        self,
        *,
        request: Request,
        session_id: UUID,
        invocation_id: UUID,
        after_sequence: int,
    ) -> AsyncIterator[ServerSentEvent]:
        cursor = after_sequence
        idle_polls = 0
        while not await request.is_disconnected():
            events = await self.service.list_events(
                session_id=session_id,
                invocation_id=invocation_id,
                after_sequence=cursor,
                limit=100,
            )
            if events:
                for event in events:
                    cursor = max(cursor, event.sequence)
                    idle_polls = 0
                    yield ServerSentEvent(
                        data=event.model_dump_json(),
                        event=event.channel,
                        id=str(event.sequence),
                    )
                continue
            idle_polls += 1
            if idle_polls % 100 == 0:
                yield ServerSentEvent(comment="keepalive")
            import asyncio

            await asyncio.sleep(self.poll_interval_ms / 1000)

    async def _save_registered_workflow_snapshots(self) -> None:
        """Expose registered-but-not-yet-invoked Workflows to trace clients."""

        for entry in tuple(self.agent.workflow_registry.values()):
            await self.runtime_store.asave_workflow_snapshot(
                self.agent.namespace,
                entry.workflow_snapshot,
            )


def _resume_cursor(after_sequence: int, last_event_id: str | None) -> int:
    if last_event_id is None:
        return after_sequence
    try:
        return max(after_sequence, int(last_event_id))
    except ValueError:
        return after_sequence
    TraceBootstrap,

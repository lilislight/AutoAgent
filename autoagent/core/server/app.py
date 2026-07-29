from __future__ import annotations

import asyncio
import json
import logging
import secrets
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator
from uuid import UUID

from fastapi import (
    APIRouter,
    Cookie,
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Query,
    Request,
    Response,
)
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict

from autoagent.core.app import AutoAgentApp
from autoagent.core.runtime import (
    RuntimeEventMode,
    RuntimeSerializationError,
    SessionBusyError,
)
from autoagent.core.server.trace import TraceService


_AUTH_COOKIE = "autoagent_session"
logger = logging.getLogger(__name__)


class _ApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _AuthenticationRequest(_ApiModel):
    token: str


class InvocationSubmitRequest(_ApiModel):
    input: dict[str, Any] | None = None
    session_key: str | None = None
    entry_node_id: str | None = None
    event_mode: RuntimeEventMode = "standard"


class InvocationSubmitResponse(_ApiModel):
    workflow_id: str
    session_id: UUID
    session_key: str
    invocation_id: UUID
    state: str


class InvocationResumeRequest(_ApiModel):
    session_key: str
    wait_key: str
    output: Any | None = None


class InvocationResumeResponse(_ApiModel):
    workflow_id: str
    session_id: UUID
    session_key: str
    invocation_id: UUID
    state: str


class InvocationCancelResponse(_ApiModel):
    invocation_id: UUID
    state: str


class AutoAgentServer:
    """Standalone FastAPI application and embeddable tracing/execution router."""

    def __init__(
        self,
        app: AutoAgentApp,
        *,
        execution_enabled: bool = True,
        access_token: str | None = None,
        secure_cookies: bool = False,
        ui_directory: str | Path | None = None,
        trace_cache_size: int = 128,
    ) -> None:
        if access_token is not None and not access_token:
            raise ValueError("access_token cannot be empty.")
        self.agent = app
        self.execution_enabled = execution_enabled
        self.access_token = access_token
        self.secure_cookies = secure_cookies
        packaged_ui = Path(__file__).resolve().parent / "ui"
        repository_ui = Path(__file__).resolve().parents[3] / "ui" / "dist"
        default_ui = (
            packaged_ui if packaged_ui.is_dir() else repository_ui
        )
        self.ui_directory = (
            Path(ui_directory)
            if ui_directory is not None
            else default_ui
        )
        self._invocation_tasks: dict[UUID, asyncio.Task[Any]] = {}
        self._invocation_failures: dict[UUID, BaseException] = {}
        self._started_at_ms = time.time_ns() // 1_000_000
        self.trace = TraceService(app, cache_size=trace_cache_size)
        self.router = self._build_router()
        self.api = self.create_app()

    def run(
        self,
        *,
        host: str = "0.0.0.0",
        port: int = 8765,
        reload: bool = False,
    ) -> None:
        import uvicorn

        uvicorn.run(
            self.api,
            host=host,
            port=port,
            reload=reload,
            timeout_graceful_shutdown=max(
                0.1,
                self.agent.settings.shutdown_grace_timeout_ms / 1_000,
            ),
        )

    def create_app(self) -> FastAPI:
        api = FastAPI(title="AutoAgent Server API", version="1")
        api.include_router(self.router)
        if self.ui_directory.is_dir():
            api.mount(
                "/",
                StaticFiles(directory=self.ui_directory, html=True),
                name="tracing_ui",
            )
        return api

    async def ashutdown(self) -> None:
        """Bound graceful execution shutdown before closing App resources."""

        tasks = {
            task for task in self._invocation_tasks.values() if not task.done()
        }
        timeout_s = self.agent.settings.shutdown_grace_timeout_ms / 1000
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=timeout_s)
            if pending:
                logger.warning(
                    "Server shutdown grace period expired; cancelling %d "
                    "running Invocation task(s).",
                    len(pending),
                )
                for task in pending:
                    task.cancel()
                _, still_pending = await asyncio.wait(
                    pending,
                    timeout=timeout_s,
                )
                if still_pending:
                    logger.error(
                        "%d Invocation task(s) did not acknowledge cancellation "
                        "before App shutdown.",
                        len(still_pending),
                    )
        await self.agent.aclose()

    def _build_router(self) -> APIRouter:
        @asynccontextmanager
        async def lifespan(_: FastAPI) -> AsyncIterator[None]:
            await self.agent.astart()
            try:
                yield
            finally:
                await self.ashutdown()

        router = APIRouter(prefix="/api/v1", lifespan=lifespan)

        def authenticated(
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
            if not authenticated(authorization, session_cookie):
                raise HTTPException(status_code=401, detail="Authentication required.")

        auth = [Depends(require_authentication)]

        @router.get("/health")
        async def health(
            authorization: str | None = Header(default=None),
            session_cookie: str | None = Cookie(default=None, alias=_AUTH_COOKIE),
        ) -> dict[str, str | bool]:
            return {
                "status": "ok",
                "execution_enabled": self.execution_enabled,
                "authentication_required": self.access_token is not None,
                "authenticated": authenticated(authorization, session_cookie),
            }

        @router.get("/health/live")
        async def liveness() -> dict[str, str]:
            """Cheap process liveness probe; it deliberately does not touch storage."""

            return {"status": "alive"}

        @router.get("/health/ready")
        async def readiness() -> dict[str, str | bool]:
            """Report whether this process may accept execution requests."""

            return {
                "status": "ready",
                "execution_enabled": self.execution_enabled,
                "accepting_invocations": (
                    self.execution_enabled
                    and not self.agent.runtime_store.admission_paused
                ),
            }

        @router.get("/runtime/status", dependencies=auth)
        async def runtime_status() -> dict[str, Any]:
            return self._runtime_status()

        @router.get("/runtime/stream", dependencies=auth)
        async def stream_runtime_status(request: Request) -> StreamingResponse:
            async def generate() -> AsyncIterator[str]:
                previous: str | None = None
                heartbeat_at = asyncio.get_running_loop().time()
                while not await request.is_disconnected():
                    payload = json.dumps(
                        self._runtime_status(),
                        separators=(",", ":"),
                    )
                    if payload != previous:
                        previous = payload
                        yield f"event: runtime_status\ndata: {payload}\n\n"
                    now = asyncio.get_running_loop().time()
                    if now - heartbeat_at >= 15:
                        yield ": heartbeat\n\n"
                        heartbeat_at = now
                    await asyncio.sleep(0.5)

            return StreamingResponse(
                generate(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "X-Accel-Buffering": "no",
                },
            )

        @router.post("/auth/session")
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

        @router.delete("/auth/session")
        async def delete_authentication_session(response: Response) -> None:
            response.delete_cookie(_AUTH_COOKIE)

        @router.get("/workflows", dependencies=auth)
        async def list_workflows(
            cursor: str | None = None,
            limit: int = Query(default=50, ge=1, le=200),
        ) -> dict[str, Any]:
            return await self._trace_call(
                self.trace.list_workflows(
                    cursor=cursor,
                    limit=limit,
                )
            )

        @router.get("/registered-workflows", dependencies=auth)
        async def list_registered_workflows(
            cursor: str | None = None,
            limit: int = Query(default=50, ge=1, le=200),
        ) -> dict[str, Any]:
            return await self._trace_call(
                self.trace.list_workflows(
                    cursor=cursor,
                    limit=limit,
                    registered_only=True,
                )
            )

        @router.get("/workflows/{workflow_id}/revisions", dependencies=auth)
        async def list_workflow_revisions(
            workflow_id: str,
            cursor: str | None = None,
            limit: int = Query(default=50, ge=1, le=200),
        ) -> dict[str, Any]:
            return await self._trace_call(
                self.trace.list_workflow_versions(
                    workflow_id,
                    cursor=cursor,
                    limit=limit,
                )
            )

        @router.get("/workflow-revisions/{revision_id}", dependencies=auth)
        async def get_workflow_revision(revision_id: str) -> dict[str, Any]:
            return await self._trace_call(
                self.trace.workflow_graph(revision_id)
            )

        @router.get("/workflows/{workflow_id}/sessions", dependencies=auth)
        async def list_sessions(
            workflow_id: str,
            cursor: str | None = None,
            limit: int = Query(default=50, ge=1, le=200),
        ) -> dict[str, Any]:
            return await self._trace_call(
                self.trace.list_sessions(
                    workflow_id,
                    cursor=cursor,
                    limit=limit,
                )
            )

        @router.get("/sessions/{session_id}/invocations", dependencies=auth)
        async def list_invocations(
            session_id: UUID,
            cursor: str | None = None,
            limit: int = Query(default=50, ge=1, le=200),
        ) -> dict[str, Any]:
            return await self._trace_call(
                self.trace.list_invocations(
                    session_id,
                    cursor=cursor,
                    limit=limit,
                )
            )

        @router.get(
            "/sessions/{session_id}/agent-invocations",
            dependencies=auth,
        )
        async def list_agent_invocations(
            session_id: UUID,
            anchor_invocation_id: UUID,
            direction: str = Query(pattern="^(older|newer)$"),
            limit: int = Query(default=20, ge=1, le=100),
        ) -> dict[str, Any]:
            return await self._trace_call(
                self.trace.list_invocation_neighbors(
                    session_id,
                    anchor_invocation_id=anchor_invocation_id,
                    direction=direction,
                    limit=limit,
                )
            )

        @router.get("/invocations/{invocation_id}/trace", dependencies=auth)
        async def get_invocation_trace(
            invocation_id: UUID,
            tail_limit: int = Query(default=200, ge=1, le=1_000),
        ) -> dict[str, Any]:
            return await self._trace_call(
                self.trace.trace_bootstrap(
                    invocation_id,
                    tail_limit=tail_limit,
                )
            )

        @router.get("/invocations/{invocation_id}", dependencies=auth)
        async def get_invocation(invocation_id: UUID) -> dict[str, Any]:
            return await self._trace_call(
                self.trace.invocation_detail(invocation_id)
            )

        @router.get("/invocations/{invocation_id}/events", dependencies=auth)
        async def list_invocation_events(
            invocation_id: UUID,
            after_sequence: int = Query(default=0, ge=0),
            before_sequence: int | None = Query(default=None, ge=1),
            limit: int = Query(default=200, ge=1, le=1_000),
        ) -> dict[str, Any]:
            return await self._trace_call(
                self.trace.event_page(
                    invocation_id,
                    after_sequence=after_sequence,
                    before_sequence=before_sequence,
                    limit=limit,
                )
            )

        @router.get(
            "/invocations/{invocation_id}/user-events",
            dependencies=auth,
        )
        async def list_invocation_user_events(
            invocation_id: UUID,
            after_sequence: int = Query(default=0, ge=0),
            limit: int = Query(default=200, ge=1, le=1_000),
        ) -> dict[str, Any]:
            return await self._trace_call(
                self.trace.user_event_page(
                    invocation_id,
                    after_sequence=after_sequence,
                    limit=limit,
                )
            )

        @router.get(
            "/invocations/{invocation_id}/events/{sequence}",
            dependencies=auth,
        )
        async def get_invocation_event(
            invocation_id: UUID,
            sequence: int,
        ) -> dict[str, Any]:
            return await self._trace_call(
                self.trace.event_detail(invocation_id, sequence)
            )

        @router.get(
            "/invocations/{invocation_id}/projection",
            dependencies=auth,
        )
        async def get_invocation_projection(
            invocation_id: UUID,
            through_sequence: int = Query(ge=0),
        ) -> dict[str, Any]:
            return await self._trace_call(
                self.trace.projection(
                    invocation_id,
                    through_sequence=through_sequence,
                )
            )

        @router.get("/invocations/{invocation_id}/state", dependencies=auth)
        async def get_invocation_state(
            invocation_id: UUID,
            through_sequence: int | None = Query(default=None, ge=0),
        ) -> dict[str, Any]:
            return await self._trace_call(
                self.trace.runtime_state(
                    invocation_id,
                    through_sequence=through_sequence,
                )
            )

        @router.get("/invocations/{invocation_id}/stream", dependencies=auth)
        async def stream_invocation(
            request: Request,
            invocation_id: UUID,
            after_sequence: int = Query(default=0, ge=0),
            last_event_id: str | None = Header(
                default=None,
                alias="Last-Event-ID",
            ),
        ) -> StreamingResponse:
            if last_event_id is not None:
                try:
                    after_sequence = max(after_sequence, int(last_event_id))
                except ValueError as exc:
                    raise HTTPException(
                        status_code=400,
                        detail="Invalid Last-Event-ID.",
                    ) from exc

            async def generate() -> AsyncIterator[str]:
                cursor = after_sequence
                heartbeat_at = asyncio.get_running_loop().time()
                previous_detail: str | None = None
                while not await request.is_disconnected():
                    page = await self.trace.event_page(
                        invocation_id,
                        after_sequence=cursor,
                        before_sequence=None,
                        limit=200,
                    )
                    for event in page["items"]:
                        cursor = int(event["sequence"])
                        yield (
                            f"id: {cursor}\n"
                            "event: runtime_event\n"
                            f"data: {json.dumps(event, separators=(',', ':'))}\n\n"
                        )
                    detail = await self.trace.invocation_detail(invocation_id)
                    encoded_detail = json.dumps(detail, separators=(",", ":"))
                    if encoded_detail != previous_detail:
                        previous_detail = encoded_detail
                        yield (
                            "event: invocation_status\n"
                            f"data: {encoded_detail}\n\n"
                        )
                    terminal = detail["state"] in {
                        "completed",
                        "failed",
                        "cancelled",
                        "interrupted",
                    }
                    persistence_settled = detail["persistence_status"] in {
                        "memory_only",
                        "durable",
                        "degraded",
                        "unserializable",
                    }
                    if (
                        terminal
                        and cursor >= int(detail["live_sequence"])
                        and persistence_settled
                    ):
                        yield "event: stream_end\ndata: {}\n\n"
                        break
                    now = asyncio.get_running_loop().time()
                    if now - heartbeat_at >= 15:
                        yield ": heartbeat\n\n"
                        heartbeat_at = now
                    await asyncio.sleep(0.25)

            await self._trace_call(self.trace.invocation_detail(invocation_id))
            return StreamingResponse(
                generate(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "X-Accel-Buffering": "no",
                },
            )

        @router.get(
            "/invocations/{invocation_id}/user-events/stream",
            dependencies=auth,
        )
        async def stream_invocation_user_events(
            request: Request,
            invocation_id: UUID,
            after_sequence: int = Query(default=0, ge=0),
            last_event_id: str | None = Header(
                default=None,
                alias="Last-Event-ID",
            ),
        ) -> StreamingResponse:
            if last_event_id is not None:
                try:
                    after_sequence = max(after_sequence, int(last_event_id))
                except ValueError as exc:
                    raise HTTPException(
                        status_code=400,
                        detail="Invalid Last-Event-ID.",
                    ) from exc

            async def generate() -> AsyncIterator[str]:
                cursor = after_sequence
                loop = asyncio.get_running_loop()
                changed = asyncio.Event()

                def notify() -> None:
                    loop.call_soon_threadsafe(changed.set)

                unsubscribe = (
                    self.agent.runtime_store.subscribe_user_event_changes(
                        invocation_id,
                        notify,
                    )
                )
                try:
                    while not await request.is_disconnected():
                        # Clear before reading. A concurrent notification is
                        # either included in this page or leaves the Event set
                        # for the next iteration, so no wakeup can be lost.
                        changed.clear()
                        page = await self.trace.user_event_page(
                            invocation_id,
                            after_sequence=cursor,
                            limit=200,
                        )
                        for event in page["items"]:
                            cursor = int(event["sequence"])
                            yield (
                                f"id: {cursor}\n"
                                "event: user_event\n"
                                f"data: {json.dumps(event, separators=(',', ':'))}\n\n"
                            )
                        detail = await self.trace.invocation_detail(
                            invocation_id
                        )
                        terminal = detail["state"] in {
                            "completed",
                            "failed",
                            "cancelled",
                            "interrupted",
                        }
                        if (
                            terminal
                            and cursor >= int(page["live_sequence"])
                        ):
                            yield "event: stream_end\ndata: {}\n\n"
                            break
                        try:
                            await asyncio.wait_for(
                                changed.wait(),
                                timeout=15,
                            )
                        except TimeoutError:
                            yield ": heartbeat\n\n"
                finally:
                    unsubscribe()

            await self._trace_call(self.trace.invocation_detail(invocation_id))
            return StreamingResponse(
                generate(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "X-Accel-Buffering": "no",
                },
            )

        @router.post(
            "/workflows/{workflow_id}/invocations",
            response_model=InvocationSubmitResponse,
            dependencies=auth,
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
                admitted = await self.agent._aadmit_invocation(
                    entry.workflow,
                    input=body.input,
                    session_id=body.session_key,
                    entry_node_id=body.entry_node_id,
                    event_mode=body.event_mode,
                )
            except SessionBusyError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            except RuntimeSerializationError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            except (KeyError, ValueError) as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except TimeoutError as exc:
                raise HTTPException(status_code=503, detail=str(exc)) from exc
            except RuntimeError as exc:
                raise HTTPException(status_code=503, detail=str(exc)) from exc
            task = asyncio.create_task(
                self.agent._aexecute_admitted(admitted, input=body.input)
            )
            invocation_id = admitted.invocation.id
            self._invocation_tasks[invocation_id] = task
            task.add_done_callback(
                lambda completed: self._finish_invocation_task(
                    invocation_id,
                    completed,
                )
            )
            session_key = admitted.session.session_key
            if session_key is None:
                raise RuntimeError("Admitted Server Session has no external key.")
            return InvocationSubmitResponse(
                workflow_id=workflow_id,
                session_id=admitted.session.id,
                session_key=session_key,
                invocation_id=invocation_id,
                state=admitted.invocation.state,
            )

        @router.post(
            "/workflows/{workflow_id}/resume",
            response_model=InvocationResumeResponse,
            dependencies=auth,
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
                "session_id": body.session_key,
                "wait_key": body.wait_key,
            }
            if "output" in body.model_fields_set:
                kwargs["output"] = body.output
            try:
                invocation = await self.agent.aresume(entry.workflow, **kwargs)
            except SessionBusyError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            except RuntimeSerializationError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            except (KeyError, ValueError) as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except RuntimeError as exc:
                raise HTTPException(status_code=503, detail=str(exc)) from exc
            session = self.agent.runtime_store.find_session(
                namespace=self.agent.namespace,
                workflow_id=workflow_id,
                session_key=body.session_key,
            )
            if session is None:
                raise HTTPException(status_code=404, detail="Session disappeared.")
            return InvocationResumeResponse(
                workflow_id=workflow_id,
                session_id=session.id,
                session_key=body.session_key,
                invocation_id=invocation.id,
                state=invocation.state,
            )

        @router.post(
            "/invocations/{invocation_id}/cancel",
            response_model=InvocationCancelResponse,
            dependencies=auth,
        )
        async def cancel_invocation(
            invocation_id: UUID,
        ) -> InvocationCancelResponse:
            if not self.execution_enabled:
                raise HTTPException(status_code=403, detail="Execution API is disabled.")
            invocation = self.agent.runtime_store.invocations.get(invocation_id)
            if invocation is None:
                raise HTTPException(status_code=404, detail="Unknown Invocation.")
            if invocation.state not in {"created", "running", "waiting"}:
                raise HTTPException(
                    status_code=409,
                    detail=f"Invocation cannot be cancelled from state {invocation.state}.",
                )
            task = self._invocation_tasks.get(invocation_id)
            if task is None or task.done():
                if invocation.state != "waiting":
                    raise HTTPException(
                        status_code=409,
                        detail="Invocation is no longer executing in this Server process.",
                    )
                session_id = self.agent.runtime_store.invocation_sessions[
                    invocation_id
                ]
                session = self.agent.runtime_store.sessions[session_id]
                await self.agent._runtime_loop.arun(
                    self.agent.workflow_executor.acancel(
                        session=session,
                        invocation=invocation,
                    )
                )
                self.agent.runtime_store.notify_user_event_execution_settled(
                    invocation_id
                )
            else:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            return InvocationCancelResponse(
                invocation_id=invocation_id,
                state=invocation.state,
            )

        return router

    def _runtime_status(self) -> dict[str, Any]:
        store = self.agent.runtime_store
        persistence = store.persistence
        if persistence is None:
            persistence_status: dict[str, Any] = {
                "enabled": False,
                "backend_kind": None,
                "worker_state": "not_configured",
                "health": "memory_only",
                "pending_count": 0,
                "pending_bytes": 0,
                "low_watermark_bytes": 0,
                "high_watermark_bytes": 0,
                "hard_watermark_bytes": 0,
                "pressure": "normal",
                "last_error": None,
                "changed_at_ms": None,
                "last_success_at_ms": None,
            }
        else:
            policy = persistence.policy
            health = persistence.health
            pending = persistence.pending_bytes
            high = policy.queue_high_watermark_bytes
            hard = policy.queue_hard_watermark_bytes
            assert policy.queue_low_watermark_bytes is not None
            assert hard is not None
            pressure = (
                "hard"
                if pending >= hard
                else "high"
                if pending >= high
                else "normal"
            )
            persistence_status = {
                "enabled": True,
                "backend_kind": type(store.backend).__name__,
                "worker_state": getattr(
                    store.backend,
                    "persistence_worker_state",
                    "unknown",
                ),
                "health": health.state,
                "pending_count": persistence.pending_count,
                "pending_bytes": pending,
                "low_watermark_bytes": policy.queue_low_watermark_bytes,
                "high_watermark_bytes": high,
                "hard_watermark_bytes": hard,
                "pressure": pressure,
                "last_error": health.last_error,
                "changed_at_ms": health.changed_at_ms,
                "last_success_at_ms": health.last_success_at_ms,
            }
        return {
            "service": {
                "status": "ok",
                "started_at_ms": self._started_at_ms,
            },
            "execution": {
                "enabled": self.execution_enabled,
                "accepting_invocations": (
                    self.execution_enabled and not store.admission_paused
                ),
                "refusal_reason": (
                    "Persistence queue pressure is above the admission watermark."
                    if store.admission_paused
                    else None
                ),
            },
            "store": {
                "kind": "durable" if store.backend is not None else "memory",
            },
            "persistence": persistence_status,
        }

    async def _trace_call(self, awaitable):
        try:
            return await awaitable
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    def _finish_invocation_task(
        self,
        invocation_id: UUID,
        task: asyncio.Task[Any],
    ) -> None:
        self._invocation_tasks.pop(invocation_id, None)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            if len(self._invocation_failures) >= 1_024:
                oldest = next(iter(self._invocation_failures))
                del self._invocation_failures[oldest]
            self._invocation_failures[invocation_id] = error

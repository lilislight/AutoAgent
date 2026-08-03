from __future__ import annotations

import asyncio
import json
import logging
import secrets
import time
from contextlib import asynccontextmanager
from pathlib import Path
from threading import RLock
from typing import Any, AsyncIterator, Awaitable, Callable
from uuid import UUID

import uvicorn
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


class _ShutdownAwareUvicornServer(uvicorn.Server):
    """Wake application streams as soon as Uvicorn receives a stop signal."""

    def __init__(
        self,
        config: uvicorn.Config,
        notify_shutdown: Callable[[], None],
    ) -> None:
        super().__init__(config)
        self._notify_shutdown = notify_shutdown

    def handle_exit(self, sig: int, frame: Any) -> None:
        self._notify_shutdown()
        super().handle_exit(sig, frame)


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
    workflow_revision_id: str
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
    workflow_revision_id: str
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
        shutdown_callback: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        if access_token is not None and not access_token:
            raise ValueError("access_token cannot be empty.")
        self.agent = app
        self.execution_enabled = execution_enabled
        self.access_token = access_token
        self.secure_cookies = secure_cookies
        self._shutdown_callback = shutdown_callback or app.aclose
        self._shutdown_requested = False
        self._shutdown_subscribers: set[Callable[[], None]] = set()
        self._trace_directory_subscribers: set[Callable[[], None]] = set()
        self._shutdown_lock = RLock()
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
        timeout_graceful_shutdown = max(
            0.1,
            self.agent.settings.shutdown_grace_timeout_ms / 1_000,
        )
        if reload:
            uvicorn.run(
                self.api,
                host=host,
                port=port,
                reload=True,
                timeout_graceful_shutdown=timeout_graceful_shutdown,
            )
            return
        config = uvicorn.Config(
            self.api,
            host=host,
            port=port,
            reload=False,
            timeout_graceful_shutdown=timeout_graceful_shutdown,
        )
        try:
            _ShutdownAwareUvicornServer(config, self.request_shutdown).run()
        except KeyboardInterrupt:
            # Uvicorn restores and re-raises captured signals after completing
            # graceful shutdown. Its public ``uvicorn.run`` helper suppresses
            # this final KeyboardInterrupt; keep the same clean CLI behavior
            # when using our shutdown-aware Server subclass.
            pass

    def request_shutdown(self) -> None:
        """Stop long-lived streams before Uvicorn waits for connections."""

        with self._shutdown_lock:
            if self._shutdown_requested:
                return
            self._shutdown_requested = True
            subscribers = tuple(self._shutdown_subscribers)
        for notify in subscribers:
            notify()

    def _subscribe_shutdown(
        self,
        notify: Callable[[], None],
    ) -> Callable[[], None]:
        with self._shutdown_lock:
            if self._shutdown_requested:
                notify_immediately = True
            else:
                self._shutdown_subscribers.add(notify)
                notify_immediately = False
        if notify_immediately:
            notify()

        def unsubscribe() -> None:
            with self._shutdown_lock:
                self._shutdown_subscribers.discard(notify)

        return unsubscribe

    def _subscribe_trace_directory_changes(
        self,
        notify: Callable[[], None],
    ) -> Callable[[], None]:
        with self._shutdown_lock:
            self._trace_directory_subscribers.add(notify)

        def unsubscribe() -> None:
            with self._shutdown_lock:
                self._trace_directory_subscribers.discard(notify)

        return unsubscribe

    def _notify_trace_directory_changed(self) -> None:
        with self._shutdown_lock:
            subscribers = tuple(self._trace_directory_subscribers)
        for notify in subscribers:
            notify()

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

        self.request_shutdown()
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
        await self._shutdown_callback()

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

        @router.get("/system/stream", dependencies=auth)
        async def stream_system_updates(request: Request) -> StreamingResponse:
            """Multiplex low-volume App status and Workflow directory changes."""

            async def generate() -> AsyncIterator[str]:
                loop = asyncio.get_running_loop()
                changed = asyncio.Event()
                workflow_changed = True
                trace_directory_changed = True
                previous_status: str | None = None

                def notify_workflow_changed() -> None:
                    nonlocal workflow_changed
                    workflow_changed = True
                    loop.call_soon_threadsafe(changed.set)

                def notify_runtime_status_changed() -> None:
                    loop.call_soon_threadsafe(changed.set)

                def notify_trace_directory_changed() -> None:
                    nonlocal trace_directory_changed
                    trace_directory_changed = True
                    loop.call_soon_threadsafe(changed.set)

                unsubscribe_workflows = (
                    self.agent.runtime_store.subscribe_workflow_changes(
                        notify_workflow_changed
                    )
                )
                persistence = self.agent.runtime_store.persistence
                unsubscribe_status = (
                    persistence.subscribe_status_changes(
                        notify_runtime_status_changed
                    )
                    if persistence is not None
                    else lambda: None
                )
                unsubscribe_shutdown = self._subscribe_shutdown(
                    notify_runtime_status_changed
                )
                unsubscribe_trace_directory = (
                    self._subscribe_trace_directory_changes(
                        notify_trace_directory_changed
                    )
                )
                try:
                    # Reconnecting is a synchronization boundary for both
                    # channels. The client refreshes the durable Workflow
                    # directory and receives the current Runtime status.
                    changed.set()
                    while (
                        not self._shutdown_requested
                        and not await request.is_disconnected()
                    ):
                        changed.clear()
                        chunks: list[str] = []
                        payload = json.dumps(
                            self._runtime_status(),
                            separators=(",", ":"),
                        )
                        if payload != previous_status:
                            previous_status = payload
                            chunks.append(
                                f"event: runtime_status\ndata: {payload}\n\n"
                            )
                        if workflow_changed:
                            workflow_changed = False
                            chunks.append(
                                "event: workflow_catalog_changed\n"
                                "data: {}\n\n"
                            )
                        if trace_directory_changed:
                            trace_directory_changed = False
                            chunks.append(
                                "event: trace_directory_changed\n"
                                "data: {}\n\n"
                            )
                        if chunks:
                            yield "".join(chunks)
                        try:
                            await asyncio.wait_for(
                                changed.wait(),
                                timeout=15,
                            )
                        except TimeoutError:
                            yield ": heartbeat\n\n"
                finally:
                    unsubscribe_trace_directory()
                    unsubscribe_shutdown()
                    unsubscribe_status()
                    unsubscribe_workflows()

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
            limit: int = Query(default=20, ge=1, le=200),
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
            limit: int = Query(default=20, ge=1, le=200),
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
            limit: int = Query(default=20, ge=1, le=200),
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

        @router.get(
            "/workflow-revisions/{workflow_revision_id}/sessions",
            dependencies=auth,
        )
        async def list_sessions(
            workflow_revision_id: str,
            cursor: str | None = None,
            limit: int = Query(default=20, ge=1, le=200),
        ) -> dict[str, Any]:
            return await self._trace_call(
                self.trace.list_sessions(
                    workflow_revision_id,
                    cursor=cursor,
                    limit=limit,
                )
            )

        @router.get("/sessions/{session_id}/invocations", dependencies=auth)
        async def list_invocations(
            session_id: UUID,
            cursor: str | None = None,
            limit: int = Query(default=20, ge=1, le=200),
        ) -> dict[str, Any]:
            return await self._trace_call(
                self.trace.list_invocations(
                    session_id,
                    cursor=cursor,
                    limit=limit,
                )
            )

        @router.get(
            "/sessions/{session_id}/user-events/stream",
            dependencies=auth,
        )
        async def stream_session_user_event_changes(
            request: Request,
            session_id: UUID,
        ) -> StreamingResponse:
            async def generate() -> AsyncIterator[str]:
                loop = asyncio.get_running_loop()
                changed = asyncio.Event()

                def notify() -> None:
                    loop.call_soon_threadsafe(changed.set)

                unsubscribe = (
                    self.agent.runtime_store
                    .subscribe_session_user_event_changes(
                        session_id,
                        notify,
                    )
                )
                unsubscribe_shutdown = self._subscribe_shutdown(notify)
                try:
                    yield (
                        "event: session_user_events_changed\n"
                        "data: {}\n\n"
                    )
                    while (
                        not self._shutdown_requested
                        and not await request.is_disconnected()
                    ):
                        try:
                            await asyncio.wait_for(
                                changed.wait(),
                                timeout=15,
                            )
                        except TimeoutError:
                            yield ": heartbeat\n\n"
                            continue
                        changed.clear()
                        yield (
                            "event: session_user_events_changed\n"
                            "data: {}\n\n"
                        )
                finally:
                    unsubscribe_shutdown()
                    unsubscribe()

            await self._trace_call(
                self.trace.list_invocations(
                    session_id,
                    cursor=None,
                    limit=1,
                )
            )
            return StreamingResponse(
                generate(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "X-Accel-Buffering": "no",
                },
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
                loop = asyncio.get_running_loop()
                changed = asyncio.Event()
                previous_detail: str | None = None

                def notify() -> None:
                    loop.call_soon_threadsafe(changed.set)

                unsubscribe = (
                    self.agent.runtime_store.subscribe_runtime_changes(
                        invocation_id,
                        notify,
                    )
                )
                unsubscribe_shutdown = self._subscribe_shutdown(notify)
                try:
                    changed.set()
                    while (
                        not self._shutdown_requested
                        and not await request.is_disconnected()
                    ):
                        changed.clear()
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
                        encoded_detail = json.dumps(
                            detail,
                            separators=(",", ":"),
                        )
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
                        try:
                            await asyncio.wait_for(
                                changed.wait(),
                                timeout=15,
                            )
                        except TimeoutError:
                            yield ": heartbeat\n\n"
                finally:
                    unsubscribe_shutdown()
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
                unsubscribe_shutdown = self._subscribe_shutdown(notify)
                try:
                    while (
                        not self._shutdown_requested
                        and not await request.is_disconnected()
                    ):
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
                    unsubscribe_shutdown()
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
            "/workflow-revisions/{workflow_revision_id}/invocations",
            response_model=InvocationSubmitResponse,
            dependencies=auth,
        )
        async def submit_invocation(
            workflow_revision_id: str,
            body: InvocationSubmitRequest,
        ) -> InvocationSubmitResponse:
            if not self.execution_enabled:
                raise HTTPException(status_code=403, detail="Execution API is disabled.")
            entry = self._registered_entry_for_revision(workflow_revision_id)
            if entry is None:
                raise HTTPException(
                    status_code=404,
                    detail=(
                        "Workflow revision is not registered for execution: "
                        f"{workflow_revision_id}"
                    ),
                )
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
            self._notify_trace_directory_changed()
            session_key = admitted.session.session_key
            if session_key is None:
                raise RuntimeError("Admitted Server Session has no external key.")
            return InvocationSubmitResponse(
                workflow_id=entry.workflow_ir.workflow_id,
                workflow_revision_id=workflow_revision_id,
                session_id=admitted.session.id,
                session_key=session_key,
                invocation_id=invocation_id,
                state=admitted.invocation.state,
            )

        @router.post(
            "/workflow-revisions/{workflow_revision_id}/resume",
            response_model=InvocationResumeResponse,
            dependencies=auth,
        )
        async def resume_invocation(
            workflow_revision_id: str,
            body: InvocationResumeRequest,
        ) -> InvocationResumeResponse:
            if not self.execution_enabled:
                raise HTTPException(status_code=403, detail="Execution API is disabled.")
            entry = self._registered_entry_for_revision(workflow_revision_id)
            if entry is None:
                raise HTTPException(
                    status_code=404,
                    detail=(
                        "Workflow revision is not registered for execution: "
                        f"{workflow_revision_id}"
                    ),
                )
            kwargs: dict[str, Any] = {
                "session_id": body.session_key,
                "wait_key": body.wait_key,
            }
            if "output" in body.model_fields_set:
                kwargs["output"] = body.output
            try:
                invocation = await self.agent.aresume(entry.workflow, **kwargs)
                self._notify_trace_directory_changed()
            except SessionBusyError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            except RuntimeSerializationError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            except (KeyError, ValueError) as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except RuntimeError as exc:
                raise HTTPException(status_code=503, detail=str(exc)) from exc
            session = self.agent.runtime_store.find_session(
                workflow_revision_id=workflow_revision_id,
                session_key=body.session_key,
            )
            if session is None:
                raise HTTPException(status_code=404, detail="Session disappeared.")
            return InvocationResumeResponse(
                workflow_id=entry.workflow_ir.workflow_id,
                workflow_revision_id=workflow_revision_id,
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
            self._notify_trace_directory_changed()
            return InvocationCancelResponse(
                invocation_id=invocation_id,
                state=invocation.state,
            )

        return router

    def _registered_entry_for_revision(self, revision_id: str):
        return self.agent.workflow_registry.get(revision_id)

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
        self._notify_trace_directory_changed()
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.error(
                "Background Invocation execution failed: invocation_id=%s",
                invocation_id,
                exc_info=(type(error), error, error.__traceback__),
            )

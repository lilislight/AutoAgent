from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.sse import EventSourceResponse, ServerSentEvent
from fastapi.staticfiles import StaticFiles

from autoagent.observer.models import (
    InvocationDetail,
    InvocationSummary,
    ObservationBootstrap,
    SessionSummary,
    TimelineView,
    WorkflowGraphView,
    WorkflowSummary,
)
from autoagent.observer.service import ObservationService
from autoagent.runtime import RuntimeEvent, RuntimeStore


class ObservationApp:
    """Read-only tracing HTTP service backed by the same RuntimeStore as execution.

    It may run beside AutoAgentApp in one process or as a separate process that
    opens the same durable database. The browser never receives Runtime business
    objects and never connects directly to the database.
    """

    def __init__(
        self,
        runtime_store: RuntimeStore,
        *,
        poll_interval_ms: int = 200,
        manage_store_lifecycle: bool = False,
        ui_directory: str | Path | None = None,
    ) -> None:
        if poll_interval_ms <= 0:
            raise ValueError("poll_interval_ms must be positive.")
        self.runtime_store = runtime_store
        self.service = ObservationService(runtime_store)
        self.poll_interval_ms = poll_interval_ms
        self.manage_store_lifecycle = manage_store_lifecycle
        self.ui_directory = Path(ui_directory) if ui_directory else _default_ui_dist()
        self.api = self._build_api()

    def run(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 8765,
        reload: bool = False,
    ) -> None:
        """Start the tracing server; production callers may use `api` directly."""

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
                if self.manage_store_lifecycle:
                    await self.runtime_store.aclose()

        api = FastAPI(
            title="AutoAgent Observation API",
            version="1",
            lifespan=lifespan,
        )

        @api.get("/api/health")
        async def health() -> dict[str, str]:
            return {"status": "ok"}

        @api.get("/api/workflows", response_model=tuple[WorkflowSummary, ...])
        async def workflows(namespace: str | None = None) -> tuple[WorkflowSummary, ...]:
            return await self.service.list_workflows(namespace=namespace)

        @api.get(
            "/api/workflows/{workflow_id}/versions/{definition_hash}",
            response_model=WorkflowGraphView,
        )
        async def workflow_graph(
            workflow_id: str,
            definition_hash: str,
            namespace: str | None = None,
            operator_manifest_hash: str | None = None,
        ) -> WorkflowGraphView:
            try:
                return await self.service.get_graph(
                    namespace=namespace,
                    workflow_id=workflow_id,
                    definition_hash=definition_hash,
                    operator_manifest_hash=operator_manifest_hash,
                )
            except KeyError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc

        @api.get("/api/sessions", response_model=tuple[SessionSummary, ...])
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
        )
        async def invocations(session_id: UUID) -> tuple[InvocationSummary, ...]:
            return await self.service.list_invocations(session_id)

        @api.get(
            "/api/sessions/{session_id}/invocations/{invocation_id}",
            response_model=InvocationDetail,
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
            response_model=ObservationBootstrap,
        )
        async def invocation_view(
            session_id: UUID,
            invocation_id: UUID,
        ) -> ObservationBootstrap:
            try:
                return await self.service.bootstrap(
                    session_id=session_id,
                    invocation_id=invocation_id,
                )
            except KeyError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc

        @api.get(
            "/api/sessions/{session_id}/invocations/{invocation_id}/events",
            response_model=tuple[RuntimeEvent, ...],
        )
        async def invocation_events(
            session_id: UUID,
            invocation_id: UUID,
            after_sequence: int = Query(default=0, ge=0),
            limit: int = Query(default=1000, ge=1, le=10_000),
            visibility: str | None = None,
        ) -> tuple[RuntimeEvent, ...]:
            try:
                return await self.service.list_events(
                    session_id=session_id,
                    invocation_id=invocation_id,
                    after_sequence=after_sequence,
                    limit=limit,
                    visibility=visibility,
                )
            except KeyError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc

        async def validate_invocation_scope(
            session_id: UUID,
            invocation_id: UUID,
        ) -> None:
            """Resolve the stream scope before SSE response headers are sent."""

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

        if self.ui_directory.is_dir():
            api.mount(
                "/",
                StaticFiles(directory=self.ui_directory, html=True),
                name="tracing-ui",
            )
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
                limit=1000,
            )
            if events:
                idle_polls = 0
                for runtime_event in events:
                    cursor = runtime_event.sequence
                    yield ServerSentEvent(
                        data=runtime_event.model_dump(mode="json"),
                        event=runtime_event.channel,
                        id=str(runtime_event.sequence),
                        retry=1000,
                    )
                continue
            idle_polls += 1
            if idle_polls * self.poll_interval_ms >= 15_000:
                idle_polls = 0
                yield ServerSentEvent(comment="keepalive")
            await asyncio.sleep(self.poll_interval_ms / 1000)


def _resume_cursor(after_sequence: int, last_event_id: str | None) -> int:
    if last_event_id is None:
        return after_sequence
    try:
        return max(after_sequence, int(last_event_id))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid Last-Event-ID.") from exc


def _default_ui_dist() -> Path:
    return Path(__file__).resolve().parents[2] / "ui" / "dist"

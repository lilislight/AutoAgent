from __future__ import annotations

import asyncio
import secrets
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator
from uuid import UUID

from fastapi import Cookie, Depends, FastAPI, Header, HTTPException, Response
from pydantic import BaseModel, ConfigDict

from autoagent.core.app import AutoAgentApp
from autoagent.core.runtime import (
    RuntimeSerializationError,
    SessionBusyError,
)


_AUTH_COOKIE = "autoagent_session"


class _ApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _AuthenticationRequest(_ApiModel):
    token: str


class InvocationSubmitRequest(_ApiModel):
    input: dict[str, Any] | None = None
    session_key: str | None = None
    entry_node_id: str | None = None


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


class AutoAgentServer:
    """Thin execution adapter.

    Runtime inspection, graph preview, Trace projections, event paging, and SSE
    are intentionally absent. The next Server/UI design can consume the new
    RuntimeStore contract without keeping the deleted projection APIs alive.
    """

    def __init__(
        self,
        app: AutoAgentApp,
        *,
        execution_enabled: bool = True,
        access_token: str | None = None,
        secure_cookies: bool = False,
    ) -> None:
        if access_token is not None and not access_token:
            raise ValueError("access_token cannot be empty.")
        self.agent = app
        self.execution_enabled = execution_enabled
        self.access_token = access_token
        self.secure_cookies = secure_cookies
        self._invocation_tasks: dict[UUID, asyncio.Task[Any]] = {}
        self._invocation_failures: dict[UUID, BaseException] = {}
        self.api = self._build_api()

    def run(
        self,
        *,
        host: str = "0.0.0.0",
        port: int = 8765,
        reload: bool = False,
    ) -> None:
        import uvicorn

        uvicorn.run(self.api, host=host, port=port, reload=reload)

    def _build_api(self) -> FastAPI:
        @asynccontextmanager
        async def lifespan(_: FastAPI) -> AsyncIterator[None]:
            await self.agent.astart()
            try:
                yield
            finally:
                if self._invocation_tasks:
                    await asyncio.gather(
                        *tuple(self._invocation_tasks.values()),
                        return_exceptions=True,
                    )
                await self.agent.aclose()

        api = FastAPI(title="AutoAgent Server API", version="1", lifespan=lifespan)

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

        @api.get("/api/health")
        async def health() -> dict[str, str | bool]:
            return {
                "status": "ok",
                "execution_enabled": self.execution_enabled,
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
                )
            except SessionBusyError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            except RuntimeSerializationError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            except (KeyError, ValueError) as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
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

        @api.post(
            "/api/workflows/{workflow_id}/resume",
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

        return api

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

from __future__ import annotations

import asyncio
from time import monotonic
from typing import Any, Mapping
from urllib.parse import quote, urlsplit

import httpx


class ServerClientError(RuntimeError):
    """The CLI could not reach or use an AutoAgent Server."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


def resolve_server_url(
    environment: Mapping[str, str],
    *,
    explicit_url: str | None = None,
) -> str:
    """Resolve the CLI endpoint without requiring host/port arguments."""

    configured = (
        explicit_url
        or environment.get("AUTOAGENT_SERVER_URL")
        or ""
    ).strip()
    if configured:
        value = configured.rstrip("/")
    else:
        host = environment.get("AUTOAGENT_SERVER_HOST", "127.0.0.1").strip()
        if host in {"", "0.0.0.0", "::", "[::]"}:
            host = "127.0.0.1"
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        port_text = environment.get("AUTOAGENT_SERVER_PORT", "8765").strip()
        try:
            port = int(port_text)
        except ValueError as exc:
            raise ValueError("AUTOAGENT_SERVER_PORT must be an integer.") from exc
        if not 1 <= port <= 65_535:
            raise ValueError(
                "AUTOAGENT_SERVER_PORT must be between 1 and 65535."
            )
        value = f"http://{host}:{port}"

    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(
            "Server URL must be an absolute http:// or https:// URL."
        )
    try:
        parsed.port
    except ValueError as exc:
        raise ValueError("Server URL contains an invalid port.") from exc
    if (
        parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError(
            "Server URL must contain only scheme, host, and optional port."
        )
    return value


class AutoAgentServerClient:
    """Small async client for CLI execution against AutoAgentServer."""

    def __init__(
        self,
        base_url: str,
        *,
        access_token: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout: float = 30.0,
    ) -> None:
        headers = (
            {"Authorization": f"Bearer {access_token}"}
            if access_token
            else None
        )
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=headers,
            timeout=timeout,
            transport=transport,
        )

    async def __aenter__(self) -> AutoAgentServerClient:
        try:
            health = await self.health()
            if health.get("authentication_required") and not health.get(
                "authenticated"
            ):
                raise ServerClientError(
                    "Server requires AUTOAGENT_SERVER_ACCESS_TOKEN."
                )
        except BaseException:
            await self._client.aclose()
            raise
        return self

    async def __aexit__(self, *_: object) -> None:
        await self._client.aclose()

    async def health(self) -> dict[str, Any]:
        return await self._request("GET", "/api/v1/health")

    async def workflow_revision_id(self, workflow_id: str) -> str:
        page = await self._request(
            "GET",
            f"/api/v1/workflows/{quote(workflow_id, safe='')}/revisions",
            params={"limit": 200},
        )
        matches = [
            item
            for item in page.get("items", ())
            if item.get("workflow_id") == workflow_id and item.get("registered")
        ]
        if not matches:
            raise ServerClientError(
                f"Server has no registered Workflow with id '{workflow_id}'."
            )
        if len(matches) > 1:
            raise ServerClientError(
                f"Server has multiple registered revisions for Workflow '{workflow_id}'."
            )
        return str(matches[0]["revision_id"])

    async def submit(
        self,
        workflow_id: str,
        *,
        input: dict[str, Any] | None,
        session_key: str | None,
        entry_node_id: str | None,
        event_mode: str,
    ) -> dict[str, Any]:
        revision_id = await self.workflow_revision_id(workflow_id)
        return await self._request(
            "POST",
            f"/api/v1/workflow-revisions/{revision_id}/invocations",
            json={
                "input": input,
                "session_key": session_key,
                "entry_node_id": entry_node_id,
                "event_mode": event_mode,
            },
        )

    async def resume(
        self,
        workflow_id: str,
        *,
        session_key: str,
        wait_key: str,
        output_supplied: bool,
        output: Any,
    ) -> dict[str, Any]:
        revision_id = await self.workflow_revision_id(workflow_id)
        body: dict[str, Any] = {
            "session_key": session_key,
            "wait_key": wait_key,
        }
        if output_supplied:
            body["output"] = output
        return await self._request(
            "POST",
            f"/api/v1/workflow-revisions/{revision_id}/resume",
            json=body,
        )

    async def invocation(self, invocation_id: str) -> dict[str, Any]:
        return await self._request(
            "GET",
            f"/api/v1/invocations/{invocation_id}",
        )

    async def invocation_report(self, invocation_id: str) -> dict[str, Any]:
        return await self._request(
            "GET",
            f"/api/v1/invocations/{invocation_id}/report",
        )

    async def debug_runtime_events(
        self,
        invocation_id: str,
        *,
        cursor: str | None = None,
        through_sequence: int | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        if through_sequence is not None:
            params["through_sequence"] = through_sequence
        return await self._request(
            "GET",
            f"/api/v1/invocations/{invocation_id}/debug/runtime-events",
            params=params,
        )

    async def debug_runtime_event(
        self,
        invocation_id: str,
        sequence: int,
        *,
        through_sequence: int | None = None,
    ) -> dict[str, Any]:
        params = (
            {}
            if through_sequence is None
            else {"through_sequence": through_sequence}
        )
        return await self._request(
            "GET",
            f"/api/v1/invocations/{invocation_id}/debug/runtime-events/{sequence}",
            params=params,
        )

    async def debug_user_events(
        self,
        invocation_id: str,
        *,
        cursor: str | None = None,
        through_sequence: int | None = None,
        limit: int = 20,
        include_stream_deltas: bool = False,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "limit": limit,
            "include_stream_deltas": include_stream_deltas,
        }
        if cursor is not None:
            params["cursor"] = cursor
        if through_sequence is not None:
            params["through_sequence"] = through_sequence
        return await self._request(
            "GET",
            f"/api/v1/invocations/{invocation_id}/debug/user-events",
            params=params,
        )

    async def debug_user_event(
        self,
        invocation_id: str,
        sequence: int,
    ) -> dict[str, Any]:
        return await self._request(
            "GET",
            f"/api/v1/invocations/{invocation_id}/debug/user-events/{sequence}",
        )

    async def wait_for_invocation(
        self,
        invocation_id: str,
        *,
        timeout: float | None,
    ) -> dict[str, Any]:
        deadline = None if timeout is None else monotonic() + timeout
        while True:
            detail = await self.invocation(invocation_id)
            if detail.get("state") in {
                "waiting",
                "completed",
                "failed",
                "interrupted",
                "cancelled",
            }:
                return detail
            if deadline is not None and monotonic() >= deadline:
                raise TimeoutError("Invocation timed out.")
            await asyncio.sleep(0.1)

    async def events(self, invocation_id: str) -> list[dict[str, Any]]:
        values: list[dict[str, Any]] = []
        cursor = 0
        while True:
            page = await self._request(
                "GET",
                f"/api/v1/invocations/{invocation_id}/events",
                params={"after_sequence": cursor, "limit": 1_000},
            )
            items = list(page.get("items", ()))
            values.extend(items)
            if not page.get("has_later") or not items:
                return values
            cursor = int(items[-1]["sequence"])

    async def _request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        try:
            response = await self._client.request(method, path, **kwargs)
        except httpx.RequestError as exc:
            raise ServerClientError(
                f"Cannot connect to AutoAgent Server at {self.base_url}."
            ) from exc
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            try:
                body = response.json()
                detail = body.get("detail", body)
            except ValueError:
                detail = response.text or response.reason_phrase
            raise ServerClientError(
                f"Server returned HTTP {response.status_code}: {detail}",
                status_code=response.status_code,
            ) from exc
        value = response.json()
        if not isinstance(value, dict):
            raise ServerClientError("Server returned an invalid JSON response.")
        return value

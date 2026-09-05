"""Remote Runtime Event sink with a dedicated blocking connection pool."""

from __future__ import annotations

import math
import threading
from concurrent.futures import Future as ThreadFuture
from typing import Protocol
from urllib.parse import urlsplit, urlunsplit

from autoagent.core.compiler import WorkflowDefinitionSnapshot
from autoagent.core.runtime import RuntimeEvent, UserEvent

from .errors import RuntimeEventStoreClosedError, RuntimeEventStoreError
from ._worker import ConcurrentWorker, run_in_daemon


class _Response(Protocol):
    status_code: int
    text: str


class _HttpClient(Protocol):
    def post(
        self,
        url: str,
        *,
        json: object,
        headers: dict[str, str],
    ) -> _Response: ...

    def close(self) -> None: ...


class HttpRuntimeEventSink:
    """POST canonical records and await remote durable acceptance.

    A bounded worker pool owns synchronous requests without binding the adapter
    to Core's private event loop. Injected clients must support concurrent
    ``post`` calls when ``max_concurrency`` is greater than one.
    """

    def __init__(
        self,
        url: str,
        *,
        user_event_url: str | None = None,
        token: str | None = None,
        timeout_seconds: float = 10.0,
        client: _HttpClient | None = None,
        max_concurrency: int = 8,
    ) -> None:
        self.url = _non_empty(url, "url")
        self.user_event_url = (
            _user_event_url(self.url)
            if user_event_url is None
            else _non_empty(user_event_url, "user_event_url")
        )
        if self.user_event_url == self.url:
            raise ValueError("user_event_url must differ from url.")
        if token is not None:
            token = _non_empty(token, "token")
        try:
            finite_timeout = math.isfinite(timeout_seconds)
        except (TypeError, OverflowError):
            finite_timeout = False
        if (
            not isinstance(timeout_seconds, (int, float))
            or isinstance(timeout_seconds, bool)
            or not finite_timeout
            or timeout_seconds <= 0
        ):
            raise ValueError("timeout_seconds must be positive and finite.")
        if (
            not isinstance(max_concurrency, int)
            or isinstance(max_concurrency, bool)
            or max_concurrency < 1
        ):
            raise ValueError("max_concurrency must be a positive integer.")
        self.token = token
        self.timeout_seconds = float(timeout_seconds)
        self._worker = ConcurrentWorker(
            "autoagent-http-sink",
            max_workers=max_concurrency,
        )
        self._client = client
        self._owns_client = client is None
        self._lock = threading.Lock()
        self._closed = False
        self._close_future: ThreadFuture[None] | None = None

    async def append(self, event: RuntimeEvent) -> None:
        """Send one Event using its stable idempotency identity."""

        if not isinstance(event, RuntimeEvent):
            raise TypeError("event must be a RuntimeEvent.")
        with self._lock:
            self._ensure_open_locked()
            completion = self._worker.call_async(self._append, event)
        await completion

    async def append_user_event(self, event: UserEvent) -> None:
        """Send one independently ordered User Event to its own endpoint."""

        if not isinstance(event, UserEvent):
            raise TypeError("event must be a UserEvent.")
        with self._lock:
            self._ensure_open_locked()
            completion = self._worker.call_async(self._append_user_event, event)
        await completion

    def save_workflow(self, snapshot: WorkflowDefinitionSnapshot) -> None:
        """Send one portable Workflow definition before its Runtime Events."""

        if not isinstance(snapshot, WorkflowDefinitionSnapshot):
            raise TypeError("snapshot must be a WorkflowDefinitionSnapshot.")
        with self._lock:
            self._ensure_open_locked()
            completion = self._worker.submit(self._save_workflow, snapshot)
        completion.result()

    async def asave_workflow(self, snapshot: WorkflowDefinitionSnapshot) -> None:
        """Asynchronously send one portable Workflow definition."""

        if not isinstance(snapshot, WorkflowDefinitionSnapshot):
            raise TypeError("snapshot must be a WorkflowDefinitionSnapshot.")
        with self._lock:
            self._ensure_open_locked()
            completion = self._worker.call_async(self._save_workflow, snapshot)
        await completion

    def close(self) -> None:
        """Drain submitted requests and close the connection pool."""

        with self._lock:
            future = self._close_future
            owner = future is None
            if owner:
                future = ThreadFuture()
                self._close_future = future
                self._closed = True
        assert future is not None
        if not owner:
            future.result()
            return
        try:
            self._worker.close()
            self._close_client()
        except BaseException as error:
            future.set_exception(error)
            raise
        else:
            future.set_result(None)

    async def aclose(self) -> None:
        """Close without blocking the caller's event loop."""

        await run_in_daemon(self.close, name="autoagent-http-sink-close")

    def _append(self, event: RuntimeEvent) -> None:
        headers = {
            "Idempotency-Key": event.id,
            "X-AutoAgent-Event-Sequence": str(event.sequence),
            "X-AutoAgent-Record-Type": "runtime_event",
        }
        self._post(self.url, event.to_record(), headers)

    def _append_user_event(self, event: UserEvent) -> None:
        headers = {
            "Idempotency-Key": event.id,
            "X-AutoAgent-User-Event-Sequence": str(event.sequence),
            "X-AutoAgent-Record-Type": "user_event",
        }
        self._post(self.user_event_url, event.to_record(), headers)

    def _save_workflow(self, snapshot: WorkflowDefinitionSnapshot) -> None:
        self._post(
            self.url,
            snapshot.to_record(),
            {
                "Idempotency-Key": snapshot.workflow_revision_id,
                "X-AutoAgent-Record-Type": "workflow_definition",
            },
        )

    def _post(
        self,
        url: str,
        record: object,
        headers: dict[str, str],
    ) -> None:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **headers,
        }
        if self.token is not None:
            headers["Authorization"] = f"Bearer {self.token}"
        try:
            client = self._http_client()
            response = client.post(
                url,
                json=record,
                headers=headers,
            )
            status_code = response.status_code
            if (
                not isinstance(status_code, int)
                or isinstance(status_code, bool)
                or not 100 <= status_code <= 599
            ):
                raise RuntimeEventStoreError(
                    "Remote Runtime Event sink returned an invalid HTTP status."
                )
            if not 200 <= status_code < 300:
                raise RuntimeEventStoreError(
                    f"Remote Runtime Event sink returned {status_code}."
                )
            if not isinstance(response.text, str):
                raise RuntimeEventStoreError(
                    "Remote Runtime Event sink returned an invalid HTTP response body."
                )
        except RuntimeEventStoreError:
            raise
        except Exception as error:
            raise RuntimeEventStoreError(
                "Remote Runtime Event sink request failed."
            ) from error

    def _http_client(self) -> _HttpClient:
        with self._lock:
            if self._client is None:
                try:
                    import httpx
                except ImportError as error:  # pragma: no cover - packaging guard
                    raise RuntimeEventStoreError(
                        "HttpRuntimeEventSink requires the 'http' optional dependency."
                    ) from error
                self._client = httpx.Client(
                    timeout=self.timeout_seconds,
                    follow_redirects=False,
                )
            return self._client

    def _close_client(self) -> None:
        client = self._client
        self._client = None
        if client is not None and self._owns_client:
            client.close()

    def _ensure_open_locked(self) -> None:
        if self._closed:
            raise RuntimeEventStoreClosedError("HTTP Runtime Event sink is closed.")


def _non_empty(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} cannot be empty.")
    return value.strip()


def _user_event_url(runtime_event_url: str) -> str:
    """Derive the conventional sibling endpoint for direct SDK construction."""

    parsed = urlsplit(runtime_event_url)
    segments = parsed.path.rstrip("/").split("/")
    if not segments or segments[-1] != "runtime-events":
        raise ValueError(
            "user_event_url is required unless url ends with '/runtime-events'."
        )
    segments[-1] = "user-events"
    return urlunsplit(parsed._replace(path="/".join(segments)))


__all__ = ["HttpRuntimeEventSink"]

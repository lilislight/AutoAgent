"""Read-only FastAPI and SSE surface over a Host query Store."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
import base64
import binascii
import hashlib
import json
import math
import mimetypes
from pathlib import Path
from typing import Protocol, TypeVar

from autoagent.core.runtime import RuntimeState, TraceEvent
from autoagent.hosting import Page, RuntimeEventStoreError

from .dto import (
    ChildSessionPageResponse,
    HealthResponse,
    InvocationPageResponse,
    InvocationStateResponse,
    InvocationSummaryResponse,
    SessionPageResponse,
    StreamEndResponse,
    TRACING_API_VERSION,
    TraceEventResponse,
    TracePageResponse,
    WorkflowDefinitionResponse,
    WorkflowPageResponse,
    tracing_record,
    tracing_state_record,
)
from .errors import TracingDependencyError


try:
    from fastapi import (
        FastAPI as _FastAPI,
        Header as _Header,
        HTTPException as _HTTPException,
        Query as _Query,
    )
    from starlette.requests import Request as _Request
    from starlette.responses import JSONResponse as _JSONResponse
    from starlette.responses import Response as _Response
    from starlette.responses import StreamingResponse as _StreamingResponse
except ImportError as _fastapi_error:  # pragma: no cover - optional dependency
    _FASTAPI_IMPORT_ERROR: ImportError | None = _fastapi_error
    _FastAPI = None  # type: ignore[assignment]
    _Header = None  # type: ignore[assignment]
    _HTTPException = None  # type: ignore[assignment]
    _Query = None  # type: ignore[assignment]
    _Request = object  # type: ignore[assignment,misc]
    _JSONResponse = None  # type: ignore[assignment]
    _Response = None  # type: ignore[assignment]
    _StreamingResponse = None  # type: ignore[assignment]
else:
    _FASTAPI_IMPORT_ERROR = None


_MAX_LIST_LIMIT = 200
_MAX_TRACE_LIMIT = 500
_STREAM_BATCH_SIZE = 200
_TRACE_CURSOR_VERSION = 2
_T = TypeVar("_T")
_DisconnectProbe = Callable[[], Awaitable[bool]]


class TracingStore(Protocol):
    """Read-only Store methods consumed by the Tracing Server."""

    async def list_workflows(
        self, *, limit: int = 50, cursor: str | None = None
    ) -> Page[dict[str, object]]: ...

    async def get_workflow(self, revision_id: str) -> dict[str, object]: ...

    async def list_sessions(
        self,
        *,
        workflow_revision_id: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> Page[dict[str, object]]: ...

    async def get_session(self, session_id: str) -> dict[str, object]: ...

    async def list_invocations(
        self,
        session_id: str,
        *,
        workflow_revision_id: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> Page[dict[str, object]]: ...

    async def list_child_sessions(
        self,
        parent_invocation_id: str,
        *,
        limit: int = 50,
        cursor: str | None = None,
    ) -> Page[dict[str, object]]: ...

    async def get_invocation(self, invocation_id: str) -> dict[str, object]: ...

    async def list_trace_events(
        self,
        invocation_id: str,
        *,
        after_sequence: int = 0,
        limit: int = 200,
    ) -> tuple[TraceEvent, ...]: ...

    async def tail_trace_events(
        self,
        invocation_id: str,
        *,
        limit: int = 200,
        before_sequence: int | None = None,
    ) -> tuple[TraceEvent, ...]: ...

    async def rebuild_state(
        self, session_id: str, *, through_sequence: int | None = None
    ) -> RuntimeState: ...

    async def rebuild_invocation_state(
        self,
        invocation_id: str,
        *,
        through_sequence: int | None = None,
    ) -> RuntimeState: ...

    async def latest_trace_sequence(self, invocation_id: str) -> int: ...

    async def terminal_trace_status(
        self,
        invocation_id: str,
        *,
        through_sequence: int,
    ) -> tuple[str, int] | None: ...

    async def wait_for_trace(
        self,
        invocation_id: str,
        *,
        after_sequence: int,
        timeout: float = 15.0,
    ) -> bool: ...


def create_tracing_app(
    store: TracingStore,
    *,
    ui_directory: str | Path | None = None,
    heartbeat_seconds: float = 15.0,
) -> object:
    """Create a read-only Tracing ASGI app over an existing Store."""

    _require_server_dependencies()
    heartbeat_seconds = _positive_seconds(
        heartbeat_seconds, "heartbeat_seconds"
    )
    static_directory = _resolve_ui_directory(ui_directory)
    static_assets = _load_ui_assets(static_directory)
    assert _FastAPI is not None
    assert _Header is not None
    assert _Query is not None
    assert _JSONResponse is not None
    assert _Response is not None
    assert _StreamingResponse is not None
    app = _FastAPI(
        title="AutoAgent Tracing API",
        version=str(TRACING_API_VERSION),
    )
    app.state.tracing_store = store

    @app.get("/api/v1/health", response_model=HealthResponse)
    async def health() -> object:
        try:
            await store.list_workflows(limit=1)
        except Exception:
            return _JSONResponse(
                HealthResponse(status="unavailable").to_record(),
                status_code=503,
            )
        return HealthResponse(status="ok").to_record()

    @app.get("/api/v1/workflows", response_model=WorkflowPageResponse)
    async def list_workflows(
        limit: int = _Query(default=50, ge=1, le=_MAX_LIST_LIMIT),
        cursor: str | None = _Query(default=None, min_length=1, max_length=512),
    ) -> object:
        page = await _query_store(
            store.list_workflows(limit=limit, cursor=cursor)
        )
        return WorkflowPageResponse.from_page(page).to_record()

    @app.get(
        "/api/v1/workflows/detail",
        response_model=WorkflowDefinitionResponse,
    )
    async def get_workflow(
        revision_id: str = _Query(min_length=1),
    ) -> object:
        return tracing_record(
            await _query_store(
                store.get_workflow(revision_id),
                resource="Workflow revision",
                identity=revision_id,
            )
        )

    @app.get(
        "/api/v1/workflows/sessions",
        response_model=SessionPageResponse,
    )
    async def list_workflow_sessions(
        revision_id: str = _Query(min_length=1),
        limit: int = _Query(default=50, ge=1, le=_MAX_LIST_LIMIT),
        cursor: str | None = _Query(default=None, min_length=1, max_length=512),
    ) -> object:
        await _query_store(
            store.get_workflow(revision_id),
            resource="Workflow revision",
            identity=revision_id,
        )
        page = await _query_store(
            store.list_sessions(
                workflow_revision_id=revision_id,
                limit=limit,
                cursor=cursor,
            )
        )
        return SessionPageResponse.from_page(page).to_record()

    @app.get(
        "/api/v1/sessions/invocations",
        response_model=InvocationPageResponse,
    )
    async def list_session_invocations(
        session_id: str = _Query(min_length=1),
        workflow_revision_id: str | None = _Query(
            default=None, min_length=1
        ),
        limit: int = _Query(default=50, ge=1, le=_MAX_LIST_LIMIT),
        cursor: str | None = _Query(default=None, min_length=1, max_length=512),
    ) -> object:
        await _query_store(
            store.get_session(session_id),
            resource="Session",
            identity=session_id,
        )
        page = await _query_store(
            store.list_invocations(
                session_id,
                workflow_revision_id=workflow_revision_id,
                limit=limit,
                cursor=cursor,
            )
        )
        return InvocationPageResponse.from_page(page).to_record()

    @app.get(
        "/api/v1/invocations/children",
        response_model=ChildSessionPageResponse,
    )
    async def list_invocation_children(
        invocation_id: str = _Query(min_length=1),
        limit: int = _Query(default=50, ge=1, le=_MAX_LIST_LIMIT),
        cursor: str | None = _Query(default=None, min_length=1, max_length=512),
    ) -> object:
        await _get_invocation(store, invocation_id)
        page = await _query_store(
            store.list_child_sessions(
                invocation_id,
                limit=limit,
                cursor=cursor,
            )
        )
        return ChildSessionPageResponse.from_page(page).to_record()

    @app.get(
        "/api/v1/invocations/detail",
        response_model=InvocationSummaryResponse,
    )
    async def get_invocation(
        invocation_id: str = _Query(min_length=1),
    ) -> object:
        return tracing_record(await _get_invocation(store, invocation_id))

    @app.get(
        "/api/v1/invocations/trace",
        response_model=TracePageResponse,
    )
    async def list_invocation_trace(
        invocation_id: str = _Query(min_length=1),
        limit: int = _Query(default=200, ge=1, le=_MAX_TRACE_LIMIT),
        cursor: str | None = _Query(default=None, min_length=1, max_length=512),
        after_sequence: int | None = _Query(default=None, ge=0),
        tail_limit: int | None = _Query(
            default=None, ge=1, le=_MAX_TRACE_LIMIT
        ),
        before_sequence: int | None = _Query(default=None, ge=1),
    ) -> object:
        await _get_invocation(store, invocation_id)
        if before_sequence is not None:
            if cursor is not None or after_sequence is not None or tail_limit is not None:
                raise _bad_request(
                    "before_sequence cannot be combined with cursor, "
                    "after_sequence, or tail_limit."
                )
            traces = await _query_store(
                store.tail_trace_events(
                    invocation_id,
                    limit=limit + 1,
                    before_sequence=before_sequence,
                )
            )
            has_earlier = len(traces) > limit
            visible = traces[-limit:]
            resume_sequence = visible[-1].trace_sequence if visible else 0
            resume_cursor = (
                _encode_trace_cursor(invocation_id, resume_sequence)
                if resume_sequence > 0
                else None
            )
            return TracePageResponse(
                items=[tracing_record(trace.to_record()) for trace in visible],
                next_cursor=None,
                resume_cursor=resume_cursor,
                resume_sequence=resume_sequence,
                has_more=False,
                has_earlier=has_earlier,
            ).to_record()
        if tail_limit is not None:
            if cursor is not None or after_sequence is not None:
                raise _bad_request(
                    "tail_limit cannot be combined with cursor or after_sequence."
                )
            traces = await _query_store(
                store.tail_trace_events(
                    invocation_id,
                    limit=tail_limit + 1,
                )
            )
            has_earlier = len(traces) > tail_limit
            visible = traces[-tail_limit:]
            resume_sequence = visible[-1].trace_sequence if visible else 0
            resume_cursor = (
                _encode_trace_cursor(invocation_id, resume_sequence)
                if resume_sequence > 0
                else None
            )
            return TracePageResponse(
                items=[tracing_record(trace.to_record()) for trace in visible],
                next_cursor=None,
                resume_cursor=resume_cursor,
                resume_sequence=resume_sequence,
                has_more=False,
                has_earlier=has_earlier,
            ).to_record()

        position = _trace_position(cursor, after_sequence, invocation_id)
        await _ensure_trace_position(store, invocation_id, position)
        traces = await _query_store(
            store.list_trace_events(
                invocation_id,
                after_sequence=position,
                limit=limit + 1,
            )
        )
        visible = traces[:limit]
        has_more = len(traces) > limit
        resume_sequence = visible[-1].trace_sequence if visible else position
        resume_cursor = (
            _encode_trace_cursor(invocation_id, resume_sequence)
            if resume_sequence > 0
            else None
        )
        return TracePageResponse(
            items=[tracing_record(trace.to_record()) for trace in visible],
            next_cursor=resume_cursor if has_more else None,
            resume_cursor=resume_cursor,
            resume_sequence=resume_sequence,
            has_more=has_more,
            has_earlier=False,
        ).to_record()

    @app.get(
        "/api/v1/invocations/state",
        response_model=InvocationStateResponse,
    )
    async def get_invocation_state(
        invocation_id: str = _Query(min_length=1),
        through_sequence: int | None = _Query(default=None, ge=1),
    ) -> object:
        invocation = await _get_invocation(store, invocation_id)
        state = await _query_store(
            store.rebuild_invocation_state(
                invocation_id,
                through_sequence=through_sequence,
            ),
            resource="Invocation",
            identity=invocation_id,
        )
        if state.session is None:
            raise _store_unavailable()
        session_id = state.session.id
        return InvocationStateResponse(
            invocation_id=invocation_id,
            session_id=session_id,
            through_sequence=state.sequence,
            state=tracing_state_record(state.to_record()),
        ).to_record()

    @app.get(
        "/api/v1/invocations/stream",
        response_class=_StreamingResponse,
        response_model=None,
        responses={
            200: {
                "description": "TraceEvent frames followed by stream_end at terminal.",
                "content": {
                    "text/event-stream": {"schema": {"type": "string"}}
                },
            }
        },
    )
    async def stream_invocation_trace(
        request: _Request,
        invocation_id: str = _Query(min_length=1),
        cursor: str | None = _Query(default=None, min_length=1, max_length=512),
        after_sequence: int | None = _Query(default=None, ge=0),
        last_event_id: str | None = _Header(
            default=None,
            alias="Last-Event-ID",
            min_length=1,
            max_length=512,
        ),
    ) -> object:
        await _get_invocation(store, invocation_id)
        if last_event_id is not None:
            position = _decode_trace_cursor(last_event_id, invocation_id)
        else:
            position = _trace_position(cursor, after_sequence, invocation_id)
        await _ensure_trace_position(store, invocation_id, position)
        stream = _iter_trace_stream(
            store,
            invocation_id,
            after_sequence=position,
            heartbeat_seconds=heartbeat_seconds,
            disconnected=request.is_disconnected,
        )
        return _StreamingResponse(
            stream,
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    if static_assets:

        @app.get("/{asset_path:path}", include_in_schema=False, name="tracing-ui")
        async def serve_tracing_ui(asset_path: str) -> object:
            requested = asset_path or "index.html"
            if requested.startswith("api/"):
                raise _not_found("Tracing API route was not found.")
            asset = static_assets.get(requested)
            if asset is None:
                if _private_asset_path(requested) or requested.startswith(
                    "assets/"
                ) or Path(requested).suffix:
                    raise _not_found("Tracing UI asset was not found.")
                asset = static_assets.get("index.html")
            if asset is None:
                raise _not_found("Tracing UI asset was not found.")
            content, media_type = asset
            return _Response(content=content, media_type=media_type)
    return app


async def _get_invocation(
    store: TracingStore, invocation_id: str
) -> dict[str, object]:
    return await _query_store(
        store.get_invocation(invocation_id),
        resource="Invocation",
        identity=invocation_id,
    )


async def _ensure_trace_position(
    store: TracingStore,
    invocation_id: str,
    position: int,
) -> None:
    latest = await _query_store(store.latest_trace_sequence(invocation_id))
    if position > latest:
        raise _bad_request("cursor points beyond the latest Trace Event.")


async def _query_store(
    operation: Awaitable[_T],
    *,
    resource: str = "Resource",
    identity: str | None = None,
) -> _T:
    try:
        return await operation
    except KeyError as error:
        label = f" {identity!r}" if identity is not None else ""
        raise _not_found(f"{resource}{label} was not found.") from error
    except ValueError as error:
        raise _bad_request(str(error)) from error
    except RuntimeEventStoreError as error:
        raise _store_unavailable() from error


async def _iter_trace_stream(
    store: TracingStore,
    invocation_id: str,
    *,
    after_sequence: int,
    heartbeat_seconds: float,
    disconnected: _DisconnectProbe | None = None,
) -> AsyncIterator[bytes]:
    """Yield incremental Trace Events and heartbeat comments."""

    while True:
        if disconnected is not None and await disconnected():
            return
        traces = await store.list_trace_events(
            invocation_id,
            after_sequence=after_sequence,
            limit=_STREAM_BATCH_SIZE,
        )
        if traces:
            for trace in traces:
                after_sequence = trace.trace_sequence
                yield _trace_sse_frame(trace)
            if len(traces) == _STREAM_BATCH_SIZE:
                continue

        terminal = await store.terminal_trace_status(
            invocation_id,
            through_sequence=after_sequence,
        )
        if terminal is not None:
            status, latest_sequence = terminal
            yield _stream_end_sse_frame(
                invocation_id,
                status,
                latest_sequence,
            )
            return
        changed = await store.wait_for_trace(
            invocation_id,
            after_sequence=after_sequence,
            timeout=heartbeat_seconds,
        )
        if not changed:
            if disconnected is not None and await disconnected():
                return
            yield b": heartbeat\n\n"


def _trace_sse_frame(trace: TraceEvent) -> bytes:
    cursor = _encode_trace_cursor(trace.invocation_id or "", trace.trace_sequence)
    data = json.dumps(
        TraceEventResponse.model_validate(
            tracing_record(trace.to_record())
        ).to_record(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return f"id: {cursor}\nevent: trace\ndata: {data}\n\n".encode("utf-8")


def _stream_end_sse_frame(
    invocation_id: str,
    status: str,
    resume_sequence: int,
) -> bytes:
    resume_cursor = (
        _encode_trace_cursor(invocation_id, resume_sequence)
        if resume_sequence > 0
        else None
    )
    data = json.dumps(
        StreamEndResponse(
            invocation_id=invocation_id,
            status=status,
            resume_cursor=resume_cursor,
            resume_sequence=resume_sequence,
        ).to_record(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    identifier = f"id: {resume_cursor}\n" if resume_cursor is not None else ""
    return f"{identifier}event: stream_end\ndata: {data}\n\n".encode("utf-8")


def _encode_trace_cursor(invocation_id: str, sequence: int) -> str:
    record = {
        "invocation_digest": _invocation_digest(invocation_id),
        "sequence": sequence,
        "version": _TRACE_CURSOR_VERSION,
    }
    encoded = json.dumps(
        record,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(encoded).decode("ascii").rstrip("=")


def _decode_trace_cursor(value: str | None, invocation_id: str) -> int:
    if value is None:
        return 0
    try:
        padded = value + "=" * (-len(value) % 4)
        decoded = base64.b64decode(
            padded.encode("ascii"),
            altchars=b"-_",
            validate=True,
        )
        record = json.loads(decoded.decode("utf-8"))
    except (binascii.Error, UnicodeError, ValueError) as error:
        raise _bad_request("cursor is invalid.") from error
    if (
        not isinstance(record, dict)
        or set(record) != {"invocation_digest", "sequence", "version"}
        or record.get("version") != _TRACE_CURSOR_VERSION
        or record.get("invocation_digest") != _invocation_digest(invocation_id)
        or not isinstance(record.get("sequence"), int)
        or isinstance(record.get("sequence"), bool)
        or record["sequence"] < 1
    ):
        raise _bad_request("cursor is invalid for this Invocation.")
    return int(record["sequence"])


def _invocation_digest(invocation_id: str) -> str:
    return hashlib.sha256(invocation_id.encode("utf-8")).hexdigest()


def _trace_position(
    cursor: str | None,
    after_sequence: int | None,
    invocation_id: str,
) -> int:
    if cursor is not None and after_sequence is not None:
        raise _bad_request("cursor and after_sequence cannot be combined.")
    if cursor is not None:
        return _decode_trace_cursor(cursor, invocation_id)
    return after_sequence or 0


def _not_found(message: str) -> Exception:
    assert _HTTPException is not None
    return _HTTPException(
        status_code=404,
        detail={"code": "not_found", "message": message},
    )


def _bad_request(message: str) -> Exception:
    assert _HTTPException is not None
    return _HTTPException(
        status_code=400,
        detail={"code": "invalid_request", "message": message},
    )


def _store_unavailable() -> Exception:
    assert _HTTPException is not None
    return _HTTPException(
        status_code=503,
        detail={
            "code": "store_unavailable",
            "message": "Tracing data is unavailable or corrupt.",
        },
    )


def _positive_seconds(value: float, name: str) -> float:
    try:
        finite = math.isfinite(value)
    except (TypeError, OverflowError):
        finite = False
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not finite
        or value <= 0
    ):
        raise ValueError(f"{name} must be a positive finite number.")
    return float(value)


def _resolve_ui_directory(value: str | Path | None) -> Path | None:
    if value is None:
        bundled = Path(__file__).with_name("ui")
        return bundled if bundled.is_dir() else None
    if isinstance(value, str) and not value.strip():
        raise ValueError("ui_directory cannot be empty.")
    try:
        directory = Path(value).expanduser().resolve()
    except TypeError as error:
        raise TypeError("ui_directory must be a path or None.") from error
    if not directory.is_dir():
        raise ValueError(f"Tracing UI directory does not exist: {directory}")
    return directory


def _load_ui_assets(
    directory: Path | None,
) -> dict[str, tuple[bytes, str]]:
    if directory is None:
        return {}
    assets: dict[str, tuple[bytes, str]] = {}
    for candidate in sorted(directory.rglob("*")):
        if not candidate.is_file():
            continue
        resolved = candidate.resolve()
        if not resolved.is_relative_to(directory):
            raise ValueError("Tracing UI cannot contain escaping symbolic links.")
        relative = candidate.relative_to(directory).as_posix()
        if relative != "index.html" and not relative.startswith("assets/"):
            continue
        if _private_asset_path(relative):
            continue
        media_type = mimetypes.guess_type(candidate.name)[0]
        assets[relative] = (
            candidate.read_bytes(),
            media_type or "application/octet-stream",
        )
    return assets


def _private_asset_path(value: str) -> bool:
    return any(part.startswith(".") for part in Path(value).parts)


def _require_server_dependencies() -> None:
    if _FASTAPI_IMPORT_ERROR is not None:
        raise TracingDependencyError(
            "FastAPI server dependencies are optional; install "
            "autoagent[server]."
        ) from _FASTAPI_IMPORT_ERROR


__all__ = ["TracingStore", "create_tracing_app"]

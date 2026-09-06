"""SQLite Runtime Event sink, query store, and reducer-backed recovery source."""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import math
import os
import sqlite3
import threading
import time
from collections import OrderedDict
from collections.abc import Mapping
from concurrent.futures import Future as ThreadFuture
from contextlib import closing
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Callable, TypeVar

from autoagent.core.compiler import WorkflowDefinitionSnapshot
from autoagent.core.runtime import (
    ChildAwaitReady,
    ChildAwaitSuspended,
    ChildInvocationPhaseChanged,
    ChildInvocationPlanned,
    InvocationCancelled,
    InvocationCompleted,
    InvocationFailed,
    InvocationOpened,
    InvocationRecoveryRequested,
    InvocationStarted,
    InvocationWaiting,
    SessionCheckpoint,
    RuntimeEvent,
    RuntimeState,
    SessionOpened,
    StateReducer,
    UserEvent,
    WaitResumed,
)
from .trace import TraceEvent, project_trace_events

from .errors import (
    RuntimeEventConflictError,
    RuntimeEventQueryError,
    RuntimeEventSequenceError,
    RuntimeEventStoreClosedError,
    RuntimeEventStoreError,
)
from .models import Page, ResumablePage
from ._worker import ConcurrentWorker, SerialWorker, run_in_daemon


SQLITE_STORE_SCHEMA_VERSION = 5
_PAGE_CURSOR_VERSION = 1
_SQLITE_MAX_INTEGER = (1 << 63) - 1
_VALIDATED_STATE_CACHE_SIZE = 256
_VALIDATED_OWNERSHIP_CACHE_SIZE = 256
_INVOCATION_STATUSES = frozenset(
    {"created", "running", "waiting", "completed", "failed", "cancelled"}
)
_CHILD_PHASES = frozenset({"planned", "opened", "accepted", "terminal"})
_RUNTIME_EVENT_SELECT = """
SELECT id, session_id, invocation_id, sequence, event_name, occurred_at_ns,
       previous_event_id, previous_event_digest, from_state_version,
       to_state_version, event_digest, record_json
FROM runtime_events
"""
_TRACE_EVENT_SELECT = """
SELECT id, runtime_event_id, session_id, invocation_id, trace_sequence,
       kind, status, occurred_at_ns, record_json
FROM trace_events
"""
_USER_EVENT_SELECT = """
SELECT id, session_id, invocation_id, sequence, kind, occurred_at_ns,
       event_digest, record_json
FROM user_events
"""
_WORKFLOW_DEFINITION_SELECT = """
SELECT row_id, revision_id, workflow_id, workflow_version, definition_hash,
       created_at_ns, record_json
FROM workflow_definitions
"""
_T = TypeVar("_T")
_OwnershipKey = tuple[str, str, int]
_ReadOwnershipCache = dict[
    str,
    dict[_OwnershipKey, "_CanonicalChildOwnership"],
]
_SCHEMA_INITIALIZATION_LOCK = threading.Lock()
_REQUIRED_TABLES = frozenset(
    {
        "invocations",
        "runtime_events",
        "schema_metadata",
        "session_ownership",
        "sessions",
        "trace_events",
        "user_events",
        "user_event_streams",
        "workflow_definitions",
    }
)
_REQUIRED_COLUMNS = {
    "schema_metadata": frozenset({"key", "value"}),
    "workflow_definitions": frozenset(
        {
            "row_id",
            "revision_id",
            "workflow_id",
            "workflow_version",
            "definition_hash",
            "created_at_ns",
            "record_json",
        }
    ),
    "runtime_events": frozenset(
        {
            "row_id",
            "id",
            "session_id",
            "invocation_id",
            "sequence",
            "event_name",
            "occurred_at_ns",
            "from_state_version",
            "to_state_version",
            "previous_event_id",
            "previous_event_digest",
            "event_digest",
            "record_json",
        }
    ),
    "trace_events": frozenset(
        {
            "row_id",
            "id",
            "runtime_event_id",
            "session_id",
            "invocation_id",
            "trace_sequence",
            "kind",
            "status",
            "occurred_at_ns",
            "record_json",
        }
    ),
    "user_events": frozenset(
        {
            "row_id",
            "id",
            "session_id",
            "invocation_id",
            "sequence",
            "kind",
            "occurred_at_ns",
            "event_digest",
            "record_json",
        }
    ),
    "user_event_streams": frozenset(
        {
            "invocation_id",
            "session_id",
            "event_count",
            "last_sequence",
            "last_event_id",
            "last_event_digest",
            "updated_at_ns",
        }
    ),
    "session_ownership": frozenset(
        {
            "session_id",
            "root_session_id",
            "parent_session_id",
            "parent_invocation_id",
            "creation_id",
            "unit_index",
            "parent_occurrence_id",
            "mode",
            "workflow_id",
            "workflow_revision_id",
            "planned_invocation_id",
            "planned_event_sequence",
            "planned_log_id",
            "change_event_sequence",
            "phase",
            "updated_at_ns",
        }
    ),
    "sessions": frozenset(
        {
            "row_id",
            "session_id",
            "root_session_id",
            "current_invocation_id",
            "invocation_count",
            "last_event_sequence",
            "last_event_id",
            "last_event_digest",
            "trace_count",
            "last_trace_sequence",
            "last_trace_id",
            "last_trace_digest",
            "created_at_ns",
            "updated_at_ns",
        }
    ),
    "invocations": frozenset(
        {
            "row_id",
            "invocation_id",
            "session_id",
            "root_session_id",
            "workflow_id",
            "workflow_revision_id",
            "entry_node_id",
            "status",
            "first_event_sequence",
            "last_event_sequence",
            "trace_count",
            "last_trace_sequence",
            "created_at_ns",
            "updated_at_ns",
            "ended_at_ns",
        }
    ),
}
_REQUIRED_INDEXES = frozenset(
    {
        "invocations_session",
        "invocations_workflow",
        "runtime_events_invocation",
        "runtime_events_session_time",
        "session_ownership_parent_unit",
        "session_ownership_parent_change",
        "session_ownership_root",
        "sessions_root",
        "trace_events_invocation",
        "trace_events_invocation_kind",
        "user_events_invocation",
        "user_events_session_time",
        "workflow_definitions_workflow",
    }
)


@dataclass(frozen=True, slots=True)
class _CanonicalChildOwnership:
    """One Child descriptor derived only from canonical parent Events."""

    creation_id: str
    unit_index: int
    parent_occurrence_id: str
    mode: str
    workflow_id: str
    workflow_revision_id: str
    planned_invocation_id: str
    child_session_id: str
    planned_event_sequence: int
    planned_log_id: str
    change_event_sequence: int
    phase: str


@dataclass(frozen=True, slots=True)
class _CanonicalOwnershipCache:
    """Incrementally validated canonical Child facts for one parent Session."""

    generation: int
    last_sequence: int
    last_event_id: str
    last_event_digest: str
    children: dict[tuple[str, str, int], _CanonicalChildOwnership]


@dataclass(frozen=True, slots=True)
class _CanonicalInvocationProjection:
    """Query fields derived in one pass over a canonical Invocation prefix."""

    invocation_id: str
    session_id: str
    workflow_id: str
    workflow_revision_id: str
    entry_node_id: str
    status: str
    first_event_sequence: int
    last_event_sequence: int
    trace_count: int
    last_trace_sequence: int
    created_at_ns: int
    updated_at_ns: int
    ended_at_ns: int | None
    children: tuple[_CanonicalChildOwnership, ...]


@dataclass(slots=True)
class _CanonicalInvocationBuilder:
    """Mutable accumulator used only while one Session prefix is validated."""

    invocation_id: str
    workflow_id: str | None = None
    workflow_revision_id: str | None = None
    entry_node_id: str | None = None
    status: str | None = None
    first_event_sequence: int | None = None
    last_event_sequence: int | None = None
    created_at_ns: int | None = None
    updated_at_ns: int | None = None
    ended_at_ns: int | None = None
    children: dict[tuple[str, int], _CanonicalChildOwnership] = field(
        default_factory=dict
    )


@dataclass(frozen=True, slots=True)
class _CanonicalSessionProjection:
    """Session and Invocation query fields derived in one canonical pass."""

    session_id: str
    current_invocation_id: str | None
    invocation_count: int
    created_at_ns: int
    updated_at_ns: int
    trace_count: int
    last_trace_sequence: int
    invocations: dict[str, _CanonicalInvocationProjection]


def _operation_invocation_status(
    event: RuntimeEvent,
    log_id: str,
) -> str | None:
    """Read the exact Invocation status written by one semantic Runtime Log."""

    for batch in event.operation_batches:
        if batch.id != f"{log_id}:state:{batch.to_state_version}":
            continue
        status: object | None = None
        for operation in batch.operations:
            if operation.path == ("invocation",):
                if operation.op == "remove" or not isinstance(
                    operation.value,
                    Mapping,
                ):
                    raise RuntimeEventStoreError(
                        "Canonical Invocation operation is malformed."
                    )
                status = operation.value.get("status")
            elif operation.path == ("invocation", "status"):
                if operation.op == "remove":
                    raise RuntimeEventStoreError(
                        "Canonical Invocation status operation is malformed."
                    )
                status = operation.value
        if status is None:
            return None
        if not isinstance(status, str) or status not in _INVOCATION_STATUSES:
            raise RuntimeEventStoreError(
                "Canonical Invocation status operation is invalid."
            )
        return status
    return None


class SQLiteRuntimeStore:
    """Durably accept Runtime Events without blocking the Core RuntimeLoop.

    All writes use one dedicated thread and connection. Read queries use short-lived
    independent connections so a slow Tracing request cannot queue ahead of Runtime
    durability. WAL keeps both paths concurrent.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        refresh_seconds: float = 0.5,
        read_only: bool = False,
    ) -> None:
        self.path = Path(path).expanduser().resolve()
        try:
            finite_refresh = math.isfinite(refresh_seconds)
        except (TypeError, OverflowError):
            finite_refresh = False
        if (
            not isinstance(refresh_seconds, (int, float))
            or isinstance(refresh_seconds, bool)
            or not finite_refresh
            or refresh_seconds <= 0
        ):
            raise ValueError("refresh_seconds must be positive and finite.")
        if not isinstance(read_only, bool):
            raise TypeError("read_only must be a bool.")
        self.refresh_seconds = float(refresh_seconds)
        self.read_only = read_only
        self._writer = (
            None if read_only else SerialWorker("autoagent-sqlite-writer")
        )
        self._reader_worker = ConcurrentWorker(
            "autoagent-sqlite-reader",
            max_workers=4,
        )
        self._write_connection: sqlite3.Connection | None = None
        self._lifecycle_lock = threading.Lock()
        self._listener_lock = threading.Lock()
        self._listener_writers: set[int] = set()
        self._listeners: set[Callable[[], None]] = set()
        self._validated_states: OrderedDict[str, RuntimeState] = OrderedDict()
        self._validated_generations: OrderedDict[str, int] = OrderedDict()
        self._canonical_ownership: OrderedDict[
            str,
            _CanonicalOwnershipCache,
        ] = OrderedDict()
        self._validation_generation = 0
        self._write_data_version: int | None = None
        self._started = False
        self._closed = False
        self._close_future: ThreadFuture[None] | None = None

    @classmethod
    def open_read_only(
        cls,
        path: str | Path,
        *,
        refresh_seconds: float = 0.5,
    ) -> "SQLiteRuntimeStore":
        """Open an existing Store without schema or application-data writes.

        Live WAL readers may still let SQLite create ``-wal``/``-shm`` sidecars;
        this mode intentionally remains able to observe a running Host.
        """

        return cls(path, refresh_seconds=refresh_seconds, read_only=True)

    # ------------------------------------------------------------------
    # Lifecycle and sink boundary

    def start(self) -> None:
        """Initialize the exact schema before the Store accepts Events."""

        with self._lifecycle_lock:
            if self._closed:
                raise RuntimeEventStoreClosedError("SQLite Runtime Store is closed.")
            if self._started:
                return
            try:
                if self.read_only:
                    self._reader_worker.call(self._validate_read_only)
                else:
                    assert self._writer is not None
                    self._writer.call(self._initialize)
            except RuntimeEventStoreError:
                raise
            except (OSError, sqlite3.Error) as error:
                raise RuntimeEventStoreError(
                    f"Cannot initialize SQLite Runtime Store {self.path}."
                ) from error
            self._started = True

    async def append(self, event: RuntimeEvent) -> None:
        """Durably and idempotently append one canonical Runtime Event."""

        if not isinstance(event, RuntimeEvent):
            raise TypeError("event must be a RuntimeEvent.")
        self._ensure_writable()
        self.start()
        inserted = await self._write_async(self._append, event)
        if inserted:
            self._announce_change()

    async def append_user_event(self, event: UserEvent) -> None:
        """Durably append one independent Invocation-ordered User Event."""

        if not isinstance(event, UserEvent):
            raise TypeError("event must be a UserEvent.")
        self._ensure_writable()
        self.start()
        inserted = await self._write_async(self._append_user_event, event)
        if inserted:
            self._announce_change()

    def save_workflow(self, snapshot: WorkflowDefinitionSnapshot) -> None:
        """Persist one portable Workflow definition idempotently."""

        if not isinstance(snapshot, WorkflowDefinitionSnapshot):
            raise TypeError("snapshot must be a WorkflowDefinitionSnapshot.")
        self._ensure_writable()
        self.start()
        assert self._writer is not None
        with self._lifecycle_lock:
            self._ensure_available()
            completion = self._writer.submit(self._save_workflow, snapshot)
        try:
            inserted = completion.result()
        except (KeyError, RuntimeEventStoreError, RuntimeEventQueryError):
            raise
        except (sqlite3.Error, TypeError, ValueError, OverflowError) as error:
            raise RuntimeEventStoreError(
                "SQLite Runtime Store could not save the Workflow definition."
            ) from error
        if inserted:
            self._announce_change()

    async def asave_workflow(self, snapshot: WorkflowDefinitionSnapshot) -> None:
        """Asynchronously persist one portable Workflow definition."""

        if not isinstance(snapshot, WorkflowDefinitionSnapshot):
            raise TypeError("snapshot must be a WorkflowDefinitionSnapshot.")
        self._ensure_writable()
        self.start()
        inserted = await self._write_async(self._save_workflow, snapshot)
        if inserted:
            self._announce_change()

    def close(self) -> None:
        """Close the writer after all already-submitted operations finish."""

        started = False
        with self._lifecycle_lock:
            future = self._close_future
            owner = future is None
            if owner:
                future = ThreadFuture()
                self._close_future = future
                self._closed = True
                started = self._started
        assert future is not None
        if not owner:
            future.result()
            return

        error: BaseException | None = None
        try:
            if started and self._writer is not None:
                self._writer.call(self._close_connection)
        except BaseException as caught:
            error = caught
        close_resources = [self._reader_worker.close]
        if self._writer is not None:
            close_resources.insert(0, self._writer.close)
        for close_resource in close_resources:
            try:
                close_resource()
            except BaseException as caught:
                if error is None:
                    error = caught
                else:
                    error.add_note(f"Additional close failure: {caught}")
        try:
            self._announce_change()
        except BaseException as caught:
            if error is None:
                error = caught
            else:
                error.add_note(f"Additional close failure: {caught}")
        if error is not None:
            future.set_exception(error)
            raise error
        future.set_result(None)

    async def aclose(self) -> None:
        """Close without blocking the caller's event loop."""

        await run_in_daemon(self.close, name="autoagent-sqlite-store-close")

    def __enter__(self) -> "SQLiteRuntimeStore":
        self.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Public read model

    async def list_workflows(
        self, *, limit: int = 50, cursor: str | None = None
    ) -> Page[dict[str, object]]:
        """List stored Workflow revisions newest first."""

        return await self._read_async(
            self._list_workflows,
            _limit(limit),
            _decode_cursor(
                cursor,
                collection="workflows",
                scope=_cursor_scope(),
            ),
        )

    async def get_workflow(self, revision_id: str) -> dict[str, object]:
        """Return one portable Workflow definition record."""

        return await self._read_async(self._get_workflow, _identity(revision_id))

    async def list_sessions(
        self,
        *,
        workflow_revision_id: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> Page[dict[str, object]]:
        """List Sessions, optionally restricted to one Workflow revision."""

        if workflow_revision_id is not None:
            _identity(workflow_revision_id)
        return await self._read_async(
            self._list_sessions,
            workflow_revision_id,
            _limit(limit),
            _decode_cursor(
                cursor,
                collection="sessions",
                scope=_cursor_scope(workflow_revision_id),
            ),
        )

    async def get_session(self, session_id: str) -> dict[str, object]:
        """Return one Session summary."""

        return await self._read_async(
            self._get_session,
            _identity(session_id),
        )

    async def list_invocations(
        self,
        session_id: str,
        *,
        workflow_revision_id: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> Page[dict[str, object]]:
        """List one Session's Invocation history newest first."""

        if workflow_revision_id is not None:
            _identity(workflow_revision_id)
        return await self._read_async(
            self._list_invocations,
            _identity(session_id),
            workflow_revision_id,
            _limit(limit),
            _decode_cursor(
                cursor,
                collection="invocations",
                scope=_cursor_scope(session_id, workflow_revision_id),
            ),
        )

    async def list_child_sessions(
        self,
        parent_invocation_id: str,
        *,
        limit: int = 50,
        cursor: str | None = None,
    ) -> Page[dict[str, object]]:
        """List Child Sessions planned by one parent Invocation."""

        return await self._read_async(
            self._list_child_sessions,
            _identity(parent_invocation_id),
            _limit(limit),
            _decode_child_cursor(cursor, parent_invocation_id),
        )

    async def get_invocation(self, invocation_id: str) -> dict[str, object]:
        """Return one Invocation summary."""

        return await self._read_async(
            self._get_invocation,
            _identity(invocation_id),
        )

    async def list_trace_events(
        self,
        invocation_id: str,
        *,
        after_sequence: int = 0,
        limit: int = 200,
    ) -> tuple[TraceEvent, ...]:
        """Read safe Trace projections after one Session trace sequence."""

        _sqlite_integer(
            after_sequence,
            "after_sequence",
            minimum=0,
        )
        return await self._read_async(
            self._list_trace_events,
            _identity(invocation_id),
            after_sequence,
            _limit(limit, maximum=1_000),
        )

    async def tail_trace_events(
        self,
        invocation_id: str,
        *,
        limit: int = 200,
        before_sequence: int | None = None,
    ) -> tuple[TraceEvent, ...]:
        """Read the latest Trace projections before an optional position."""

        return await self._read_async(
            self._tail_trace_events,
            _identity(invocation_id),
            _limit(limit, maximum=1_000),
            (
                None
                if before_sequence is None
                else _sqlite_integer(
                    before_sequence,
                    "before_sequence",
                    minimum=1,
                )
            ),
        )

    async def list_user_events(
        self,
        invocation_id: str,
        *,
        after_sequence: int = 0,
        limit: int = 200,
    ) -> tuple[UserEvent, ...]:
        """Read independent User Events after an Invocation-local sequence."""

        _sqlite_integer(after_sequence, "after_sequence", minimum=0)
        return await self._read_async(
            self._list_user_events,
            _identity(invocation_id),
            after_sequence,
            _limit(limit, maximum=1_000),
        )

    async def tail_user_events(
        self,
        invocation_id: str,
        *,
        limit: int = 200,
        before_sequence: int | None = None,
    ) -> tuple[UserEvent, ...]:
        """Read the latest User Events before an optional sequence."""

        return await self._read_async(
            self._tail_user_events,
            _identity(invocation_id),
            _limit(limit, maximum=1_000),
            (
                None
                if before_sequence is None
                else _sqlite_integer(
                    before_sequence,
                    "before_sequence",
                    minimum=1,
                )
            ),
        )

    async def rebuild_state(
        self, session_id: str, *, through_sequence: int | None = None
    ) -> RuntimeState:
        """Replay one Session's canonical Event prefix with Core StateReducer."""

        if through_sequence is not None:
            _sqlite_integer(
                through_sequence,
                "through_sequence",
                minimum=1,
            )
        return await self._read_async(
            self._rebuild_state,
            _identity(session_id),
            through_sequence,
        )

    async def rebuild_invocation_state(
        self,
        invocation_id: str,
        *,
        through_sequence: int | None = None,
    ) -> RuntimeState:
        """Replay one canonical Invocation Event prefix."""

        if through_sequence is not None:
            _sqlite_integer(
                through_sequence,
                "through_sequence",
                minimum=1,
            )

        return await self._read_async(
            self._rebuild_invocation_state,
            _identity(invocation_id),
            through_sequence,
        )

    async def runtime_event_sequence_for_trace(
        self,
        invocation_id: str,
        trace_sequence: int,
    ) -> int:
        """Resolve one Trace position to its canonical Runtime Event boundary."""

        return await self._read_async(
            self._runtime_event_sequence_for_trace,
            _identity(invocation_id),
            _sqlite_integer(trace_sequence, "trace_sequence", minimum=1),
        )

    async def rebuild_checkpoint(
        self, session_id: str
    ) -> SessionCheckpoint:
        """Rebuild the latest recoverable State for one Runtime Session."""

        return await self._read_async(
            self._rebuild_checkpoint,
            _identity(session_id),
        )

    async def latest_trace_sequence(self, invocation_id: str) -> int:
        """Return the latest safe Trace sequence for one Invocation."""

        return await self._read_async(
            self._latest_trace_sequence,
            _identity(invocation_id),
        )

    async def latest_user_event_sequence(self, invocation_id: str) -> int:
        """Return the latest independent User Event sequence."""

        return await self._read_async(
            self._latest_user_event_sequence,
            _identity(invocation_id),
        )

    async def terminal_trace_status(
        self,
        invocation_id: str,
        *,
        through_sequence: int,
    ) -> tuple[str, int] | None:
        """Return terminal status only after all planned Children settle."""

        _sqlite_integer(
            through_sequence,
            "through_sequence",
            minimum=0,
        )
        return await self._read_async(
            self._terminal_trace_status,
            _identity(invocation_id),
            through_sequence,
        )

    async def wait_for_trace(
        self,
        invocation_id: str,
        *,
        after_sequence: int,
        timeout: float = 15.0,
    ) -> bool:
        """Wait efficiently for an in-process write, with bounded DB refresh.

        The timed refresh also observes another process writing the same WAL file.
        No background polling task exists when there are no callers.
        """

        return await self._wait_for_projected_sequence(
            invocation_id,
            after_sequence=after_sequence,
            timeout=timeout,
            latest_reader=self._latest_trace_sequence_hint,
        )

    async def _wait_for_projected_sequence(
        self,
        invocation_id: str,
        *,
        after_sequence: int,
        timeout: float,
        latest_reader: Callable[[str], int],
    ) -> bool:
        """Share event-driven waits across independent projection streams."""

        invocation_id = _identity(invocation_id)
        _sqlite_integer(after_sequence, "after_sequence", minimum=0)
        try:
            finite_timeout = math.isfinite(timeout)
        except (TypeError, OverflowError):
            finite_timeout = False
        if (
            not isinstance(timeout, (int, float))
            or isinstance(timeout, bool)
            or not finite_timeout
            or timeout < 0
        ):
            raise ValueError("timeout must be non-negative and finite.")
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return (
                    await self._read_async(
                        latest_reader,
                        invocation_id,
                    )
                    > after_sequence
                )
            wait_for = min(remaining, self.refresh_seconds)
            waiter = asyncio.create_task(self._wait_for_change(wait_for))
            await asyncio.sleep(0)
            try:
                latest_sequence = await self._read_async(
                    latest_reader,
                    invocation_id,
                )
                if latest_sequence > after_sequence:
                    return True
                await waiter
            finally:
                if not waiter.done():
                    waiter.cancel()
                    try:
                        await waiter
                    except asyncio.CancelledError:
                        pass

    async def wait_for_user_event(
        self,
        invocation_id: str,
        *,
        after_sequence: int,
        timeout: float = 15.0,
    ) -> bool:
        """Wait efficiently until a newer User Event becomes queryable."""

        return await self._wait_for_projected_sequence(
            invocation_id,
            after_sequence=after_sequence,
            timeout=timeout,
            latest_reader=self._latest_user_event_sequence_hint,
        )

    # ------------------------------------------------------------------
    # Writer implementation

    def _initialize(self) -> None:
        _prepare_private_database_path(self.path)
        connection = sqlite3.connect(
            self.path,
            timeout=30,
            isolation_level=None,
            check_same_thread=True,
        )
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA busy_timeout = 30000")
            connection.execute("PRAGMA foreign_keys = ON")
            with _SCHEMA_INITIALIZATION_LOCK:
                _with_busy_retry(
                    lambda: connection.execute("BEGIN IMMEDIATE")
                )
                try:
                    # Re-read after acquiring SQLite's cross-process write
                    # lock. Another initializer may have populated the file
                    # after this process atomically pre-created it.
                    tables = _sqlite_tables(connection)
                    if tables:
                        _validate_existing_schema(connection, tables)
                    else:
                        for statement in _schema_statements():
                            connection.execute(statement)
                        connection.execute(
                            "INSERT INTO schema_metadata(key, value) VALUES (?, ?)",
                            ("schema_version", str(SQLITE_STORE_SCHEMA_VERSION)),
                        )
                        _validate_existing_schema(
                            connection,
                            _sqlite_tables(connection),
                        )
                    connection.commit()
                except BaseException:
                    if connection.in_transaction:
                        connection.rollback()
                    raise
                _with_busy_retry(
                    lambda: connection.execute("PRAGMA journal_mode = WAL").fetchone()
                )
                connection.execute("PRAGMA synchronous = FULL")
            self._write_data_version = _sqlite_data_version(connection)
        except BaseException:
            connection.close()
            raise
        self._write_connection = connection

    def _validate_read_only(self) -> None:
        try:
            with closing(self._reader()) as connection:
                _validate_existing_schema(
                    connection,
                    _sqlite_tables(connection),
                )
        except sqlite3.Error as error:
            raise RuntimeEventStoreError(
                f"Cannot open existing SQLite Runtime Store {self.path}."
            ) from error

    def _append(self, event: RuntimeEvent) -> bool:
        connection = self._connection()
        record = event.to_record()
        encoded = _canonical_json(record)
        event_digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        connection.execute("BEGIN IMMEDIATE")
        try:
            external_change = self._observe_external_database_change(connection)
            previous = self._validated_session_tail(connection, event.session_id)
            if previous is not None:
                self._validated_session_trace_head(
                    connection,
                    event.session_id,
                )
                if (
                    external_change
                    or self._validated_generations.get(event.session_id)
                    != self._validation_generation
                ):
                    previous_state = self._rebuild_state_with_connection(
                        connection,
                        event.session_id,
                        None,
                    )
                    self._validate_state_projection(connection, previous_state)
                    self._validate_session_invocation_projections(
                        connection,
                        event.session_id,
                    )
            else:
                ownership = connection.execute(
                    "SELECT 1 FROM session_ownership WHERE session_id = ?",
                    (event.session_id,),
                ).fetchone()
                if ownership is not None:
                    self._validated_ownership_root(
                        connection,
                        event.session_id,
                    )
            existing = connection.execute(
                _RUNTIME_EVENT_SELECT + " WHERE id = ?",
                (event.id,),
            ).fetchone()
            if existing is not None:
                stored_event = _decode_verified_runtime_event(existing)
                if existing["record_json"] != encoded:
                    raise RuntimeEventConflictError(
                        f"Runtime Event id {event.id!r} has conflicting content."
                    )
                if stored_event != event:
                    raise RuntimeEventConflictError(
                        f"Runtime Event id {event.id!r} has conflicting content."
                    )
                head = _session_head(connection, event.session_id)
                if head is None or _stored_integer(
                    head,
                    "last_event_sequence",
                    minimum=1,
                ) < event.sequence:
                    raise RuntimeEventStoreError(
                        "Stored Session head does not cover the retried Runtime Event."
                    )
                if (
                    self._validated_generations.get(event.session_id)
                    != self._validation_generation
                ):
                    state = self._rebuild_state_with_connection(
                        connection,
                        event.session_id,
                        None,
                    )
                else:
                    state = self._validated_states.get(event.session_id)
                    if state is None:
                        state = self._rebuild_state_with_connection(
                            connection,
                            event.session_id,
                            None,
                        )
                self._validate_state_projection(connection, state)
                if stored_event.invocation_id is not None:
                    self._validate_invocation_projection(
                        connection,
                        stored_event.invocation_id,
                    )
                connection.commit()
                self._mark_session_validated(event.session_id)
                return False

            at_sequence = connection.execute(
                """
                SELECT id, record_json FROM runtime_events
                WHERE session_id = ? AND sequence = ?
                """,
                (event.session_id, event.sequence),
            ).fetchone()
            if at_sequence is not None:
                raise RuntimeEventConflictError(
                    "A Session sequence already contains another Runtime Event."
                )

            self._validate_chain(event, previous)
            next_state = self._validate_replayable(connection, event, previous)
            connection.execute(
                """
                INSERT INTO runtime_events(
                    id, session_id, invocation_id, sequence, event_name,
                    occurred_at_ns, previous_event_id, previous_event_digest,
                    from_state_version, to_state_version, event_digest, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.id,
                    event.session_id,
                    event.invocation_id,
                    event.sequence,
                    event.event_name,
                    event.occurred_at_ns,
                    event.previous_event_id,
                    event.previous_event_digest,
                    event.from_state_version,
                    event.to_state_version,
                    event_digest,
                    encoded,
                ),
            )
            self._index_event(connection, event, next_state, event_digest)
            updated = connection.execute(
                """
                UPDATE sessions
                SET last_event_sequence = ?, last_event_id = ?,
                    last_event_digest = ?
                WHERE session_id = ?
                """,
                (event.sequence, event.id, event_digest, event.session_id),
            )
            if updated.rowcount != 1:
                raise RuntimeEventStoreError(
                    "Runtime Event did not advance exactly one Session head."
                )
            connection.commit()
            self._cache_validated_state(event.session_id, next_state)
            return True
        except BaseException:
            connection.rollback()
            raise

    def _append_user_event(self, event: UserEvent) -> bool:
        """Commit one observation without changing canonical Runtime State."""

        connection = self._connection()
        encoded = _canonical_json(event.to_record())
        event_digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        connection.execute("BEGIN IMMEDIATE")
        try:
            invocation = connection.execute(
                """
                SELECT invocation_id, session_id
                FROM invocations WHERE invocation_id = ?
                """,
                (event.invocation_id,),
            ).fetchone()
            if invocation is None:
                raise RuntimeEventSequenceError(
                    "User Event requires an already persisted Invocation."
                )
            if _stored_string(invocation, "session_id") != event.session_id:
                raise RuntimeEventConflictError(
                    "User Event Session does not own its Invocation."
                )

            existing = connection.execute(
                _USER_EVENT_SELECT + " WHERE id = ?",
                (event.id,),
            ).fetchone()
            if existing is not None:
                stored = _decode_verified_user_event(existing)
                if existing["record_json"] != encoded or stored != event:
                    raise RuntimeEventConflictError(
                        f"User Event id {event.id!r} has conflicting content."
                    )
                latest = self._validated_user_event_stream(
                    connection,
                    event.invocation_id,
                )
                if latest < event.sequence:
                    raise RuntimeEventStoreError(
                        "Stored User Event stream head does not cover the retry."
                    )
                connection.commit()
                return False

            at_sequence = connection.execute(
                """
                SELECT id FROM user_events
                WHERE invocation_id = ? AND sequence = ?
                """,
                (event.invocation_id, event.sequence),
            ).fetchone()
            if at_sequence is not None:
                raise RuntimeEventConflictError(
                    "An Invocation User Event sequence already contains another Event."
                )
            tail = connection.execute(
                """
                SELECT sequence FROM user_events
                WHERE invocation_id = ? ORDER BY sequence DESC LIMIT 1
                """,
                (event.invocation_id,),
            ).fetchone()
            expected = (
                1
                if tail is None
                else _stored_integer(tail, "sequence", minimum=1) + 1
            )
            if event.sequence != expected:
                raise RuntimeEventSequenceError(
                    "User Event sequence is not continuous for its Invocation."
                )
            connection.execute(
                """
                INSERT INTO user_events(
                    id, session_id, invocation_id, sequence, kind,
                    occurred_at_ns, event_digest, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.id,
                    event.session_id,
                    event.invocation_id,
                    event.sequence,
                    event.kind,
                    event.occurred_at_ns,
                    event_digest,
                    encoded,
                ),
            )
            if tail is None:
                connection.execute(
                    """
                    INSERT INTO user_event_streams(
                        invocation_id, session_id, event_count, last_sequence,
                        last_event_id, last_event_digest, updated_at_ns
                    ) VALUES (?, ?, 1, 1, ?, ?, ?)
                    """,
                    (
                        event.invocation_id,
                        event.session_id,
                        event.id,
                        event_digest,
                        event.occurred_at_ns,
                    ),
                )
            else:
                updated = connection.execute(
                    """
                    UPDATE user_event_streams
                    SET event_count = event_count + 1,
                        last_sequence = ?, last_event_id = ?,
                        last_event_digest = ?, updated_at_ns = ?
                    WHERE invocation_id = ? AND session_id = ?
                      AND event_count = ? AND last_sequence = ?
                    """,
                    (
                        event.sequence,
                        event.id,
                        event_digest,
                        event.occurred_at_ns,
                        event.invocation_id,
                        event.session_id,
                        event.sequence - 1,
                        event.sequence - 1,
                    ),
                )
                if updated.rowcount != 1:
                    raise RuntimeEventStoreError(
                        "User Event did not advance exactly one stream head."
                    )
            connection.commit()
            return True
        except BaseException:
            connection.rollback()
            raise

    @staticmethod
    def _validated_session_tail(
        connection: sqlite3.Connection,
        session_id: str,
    ) -> sqlite3.Row | None:
        head = _session_head(connection, session_id)
        tail = connection.execute(
            _RUNTIME_EVENT_SELECT
            + " WHERE session_id = ? ORDER BY sequence DESC LIMIT 1",
            (session_id,),
        ).fetchone()
        if head is None and tail is None:
            return None
        if head is None or tail is None:
            raise RuntimeEventStoreError(
                "Stored Session head and Runtime Event tail are inconsistent."
            )
        stored_event = _decode_verified_runtime_event(tail)
        if (
            _stored_integer(head, "last_event_sequence", minimum=1)
            != stored_event.sequence
            or _stored_string(head, "last_event_id") != stored_event.id
            or _stored_string(head, "last_event_digest")
            != tail["event_digest"]
        ):
            raise RuntimeEventStoreError(
                "Stored Session head does not match its Runtime Event tail."
            )
        return tail

    @staticmethod
    def _validated_session_trace_projection(
        connection: sqlite3.Connection,
        session_id: str,
    ) -> tuple[int, int]:
        """Bind one Session's materialized Trace head to its stored rows."""

        head = connection.execute(
            """
            SELECT trace_count, last_trace_sequence, last_trace_id,
                   last_trace_digest
            FROM sessions WHERE session_id = ?
            """,
            (session_id,),
        ).fetchone()
        aggregate = connection.execute(
            """
            SELECT COUNT(*) AS trace_count,
                   MIN(trace_sequence) AS first_trace_sequence,
                   MAX(trace_sequence) AS last_trace_sequence
            FROM trace_events WHERE session_id = ?
            """,
            (session_id,),
        ).fetchone()
        assert aggregate is not None
        actual_count = _stored_integer(aggregate, "trace_count", minimum=0)
        if head is None:
            if actual_count:
                raise RuntimeEventStoreError(
                    "Stored Trace rows have no owning Session projection."
                )
            raise KeyError(session_id)
        expected_count = _stored_integer(head, "trace_count", minimum=0)
        expected_last = _stored_integer(
            head,
            "last_trace_sequence",
            minimum=0,
        )
        if actual_count != expected_count:
            raise RuntimeEventStoreError(
                "Stored Session Trace count does not match its projection head."
            )
        if actual_count == 0:
            if (
                expected_last != 0
                or _stored_string(head, "last_trace_id", optional=True) is not None
                or _stored_string(head, "last_trace_digest", optional=True) is not None
            ):
                raise RuntimeEventStoreError(
                    "Empty Session Trace projection has a non-empty head."
                )
            return 0, 0
        first = _stored_integer(
            aggregate,
            "first_trace_sequence",
            minimum=1,
        )
        actual_last = _stored_integer(
            aggregate,
            "last_trace_sequence",
            minimum=1,
        )
        if first != 1 or actual_last != actual_count or expected_last != actual_last:
            raise RuntimeEventStoreError(
                "Stored Session Trace sequence is incomplete or non-contiguous."
            )
        tail = connection.execute(
            _TRACE_EVENT_SELECT
            + " WHERE session_id = ? ORDER BY trace_sequence DESC LIMIT 1",
            (session_id,),
        ).fetchone()
        if tail is None:
            raise RuntimeEventStoreError(
                "Stored Session Trace head has no tail row."
            )
        trace = _decode_anchored_trace_event(connection, tail)
        record_json = tail["record_json"]
        if not isinstance(record_json, str):
            raise RuntimeEventStoreError("Stored Trace Event record must be text.")
        digest = hashlib.sha256(record_json.encode("utf-8")).hexdigest()
        if (
            trace.trace_sequence != expected_last
            or trace.id != _stored_string(head, "last_trace_id")
            or digest != _stored_string(head, "last_trace_digest")
        ):
            raise RuntimeEventStoreError(
                "Stored Session Trace head does not match its tail row."
            )
        return actual_count, actual_last

    @staticmethod
    def _validated_session_trace_head(
        connection: sqlite3.Connection,
        session_id: str,
    ) -> tuple[int, int]:
        """Validate only the durable Session Trace tail in logarithmic time."""

        head = connection.execute(
            """
            SELECT trace_count, last_trace_sequence, last_trace_id,
                   last_trace_digest
            FROM sessions WHERE session_id = ?
            """,
            (session_id,),
        ).fetchone()
        if head is None:
            raise KeyError(session_id)
        count = _stored_integer(head, "trace_count", minimum=0)
        latest = _stored_integer(head, "last_trace_sequence", minimum=0)
        if count == 0:
            if (
                latest != 0
                or _stored_string(head, "last_trace_id", optional=True) is not None
                or _stored_string(head, "last_trace_digest", optional=True) is not None
            ):
                raise RuntimeEventStoreError(
                    "Empty Session Trace projection has a non-empty head."
                )
            return 0, 0
        tail = connection.execute(
            _TRACE_EVENT_SELECT
            + " WHERE session_id = ? ORDER BY trace_sequence DESC LIMIT 1",
            (session_id,),
        ).fetchone()
        if tail is None:
            raise RuntimeEventStoreError(
                "Stored Session Trace head has no tail row."
            )
        trace = _decode_anchored_trace_event(connection, tail)
        record_json = tail["record_json"]
        if not isinstance(record_json, str):
            raise RuntimeEventStoreError("Stored Trace Event record must be text.")
        digest = hashlib.sha256(record_json.encode("utf-8")).hexdigest()
        if (
            trace.trace_sequence != latest
            or trace.id != _stored_string(head, "last_trace_id")
            or digest != _stored_string(head, "last_trace_digest")
        ):
            raise RuntimeEventStoreError(
                "Stored Session Trace head does not match its tail row."
            )
        return count, latest

    @staticmethod
    def _invocation_trace_head(
        connection: sqlite3.Connection,
        invocation_id: str,
        *,
        allow_absent: bool,
    ) -> tuple[sqlite3.Row | None, int]:
        """Read and anchor an Invocation Trace tail without scanning history."""

        invocation = connection.execute(
            """
            SELECT invocation_id, session_id, status, trace_count,
                   last_trace_sequence
            FROM invocations WHERE invocation_id = ?
            """,
            (invocation_id,),
        ).fetchone()
        if invocation is None:
            evidence = connection.execute(
                """
                SELECT 1 FROM runtime_events WHERE invocation_id = ?
                UNION ALL
                SELECT 1 FROM trace_events WHERE invocation_id = ?
                LIMIT 1
                """,
                (invocation_id, invocation_id),
            ).fetchone()
            if evidence is not None:
                raise RuntimeEventStoreError(
                    "Canonical Invocation data has no Invocation projection."
                )
            if allow_absent:
                return None, 0
            raise KeyError(invocation_id)
        if _stored_string(invocation, "invocation_id") != invocation_id:
            raise RuntimeEventStoreError(
                "Stored Invocation Trace projection has inconsistent identity."
            )
        _stored_string(invocation, "session_id")
        count = _stored_integer(invocation, "trace_count", minimum=0)
        latest = _stored_integer(
            invocation,
            "last_trace_sequence",
            minimum=0,
        )
        if count == 0:
            if latest != 0:
                raise RuntimeEventStoreError(
                    "Empty Invocation Trace projection has a non-empty head."
                )
            return invocation, 0
        tail = connection.execute(
            _TRACE_EVENT_SELECT
            + " WHERE invocation_id = ? ORDER BY trace_sequence DESC LIMIT 1",
            (invocation_id,),
        ).fetchone()
        if tail is None:
            raise RuntimeEventStoreError(
                "Stored Invocation Trace head has no tail row."
            )
        trace = _decode_anchored_trace_event(connection, tail)
        if trace.trace_sequence != latest:
            raise RuntimeEventStoreError(
                "Stored Invocation Trace head does not match its tail row."
            )
        return invocation, latest

    @staticmethod
    def _validated_invocation_trace_projection(
        connection: sqlite3.Connection,
        invocation_id: str,
    ) -> tuple[
        sqlite3.Row,
        int,
        _CanonicalInvocationProjection,
    ]:
        """Validate one Invocation's Trace subset against its canonical prefix."""

        invocation, _ = SQLiteRuntimeStore._invocation_trace_head(
            connection,
            invocation_id,
            allow_absent=False,
        )
        assert invocation is not None
        session_id = _stored_string(invocation, "session_id")
        canonical = SQLiteRuntimeStore._canonical_invocation_projection(
            connection,
            invocation_id,
        )
        if canonical.session_id != session_id:
            raise RuntimeEventStoreError(
                "Stored Invocation Trace belongs to another canonical Session."
            )
        expected_count = _stored_integer(invocation, "trace_count", minimum=0)
        expected_last = _stored_integer(
            invocation,
            "last_trace_sequence",
            minimum=0,
        )
        if (
            expected_count != canonical.trace_count
            or expected_last != canonical.last_trace_sequence
        ):
            raise RuntimeEventStoreError(
                "Stored Invocation Trace projection is incomplete."
            )
        if canonical.trace_count == 0:
            if expected_last != 0:
                raise RuntimeEventStoreError(
                    "Empty Invocation Trace projection has a non-empty head."
                )
            return invocation, 0, canonical
        return invocation, canonical.last_trace_sequence, canonical

    @staticmethod
    def _canonical_invocation_projection(
        connection: sqlite3.Connection,
        invocation_id: str,
    ) -> _CanonicalInvocationProjection:
        """Derive one Invocation query projection without rebuilding Scheduler State."""

        ranges = connection.execute(
            """
            SELECT session_id, MIN(sequence) AS first_event_sequence,
                   MAX(sequence) AS last_event_sequence
            FROM runtime_events WHERE invocation_id = ?
            GROUP BY session_id
            """,
            (invocation_id,),
        ).fetchall()
        if len(ranges) != 1:
            raise RuntimeEventStoreError(
                "Canonical Invocation Event range is missing or ambiguous."
            )
        row = ranges[0]
        session_id = _stored_string(row, "session_id")
        last_sequence = _stored_integer(
            row,
            "last_event_sequence",
            minimum=1,
        )
        session = SQLiteRuntimeStore._canonical_session_projection(
            connection,
            session_id,
            through_sequence=last_sequence,
        )
        projection = session.invocations.get(invocation_id)
        if projection is None:
            raise RuntimeEventStoreError(
                "Canonical Invocation range has no matching Runtime logs."
            )
        if (
            projection.first_event_sequence
            != _stored_integer(row, "first_event_sequence", minimum=1)
            or projection.last_event_sequence != last_sequence
        ):
            raise RuntimeEventStoreError(
                "Canonical Invocation Event envelope disagrees with its Runtime logs."
            )
        return projection

    @staticmethod
    def _canonical_session_projection(
        connection: sqlite3.Connection,
        session_id: str,
        *,
        through_sequence: int | None = None,
    ) -> _CanonicalSessionProjection:
        """Validate one Session prefix and derive every Invocation query field once."""

        query = _RUNTIME_EVENT_SELECT + " WHERE session_id = ?"
        parameters: list[object] = [session_id]
        if through_sequence is not None:
            query += " AND sequence <= ?"
            parameters.append(through_sequence)
        query += " ORDER BY sequence"
        event_rows = connection.execute(query, parameters).fetchall()
        if not event_rows:
            raise RuntimeEventStoreError(
                "Canonical Session has no Runtime Event prefix."
            )
        builders: dict[str, _CanonicalInvocationBuilder] = {}
        expected_traces: list[tuple[str, TraceEvent]] = []
        previous: sqlite3.Row | None = None
        next_trace_sequence = 1
        created_at_ns: int | None = None
        updated_at_ns: int | None = None
        current_invocation_id: str | None = None
        for event_row in event_rows:
            event = _decode_verified_runtime_event(event_row)
            SQLiteRuntimeStore._validate_chain(event, previous)
            previous = event_row
            updated_at_ns = event.occurred_at_ns
            if event.invocation_id is not None:
                builder = builders.setdefault(
                    event.invocation_id,
                    _CanonicalInvocationBuilder(event.invocation_id),
                )
                if builder.first_event_sequence is None:
                    builder.first_event_sequence = event.sequence
                builder.last_event_sequence = event.sequence
            projected = project_trace_events(
                event,
                start_sequence=next_trace_sequence,
            )
            expected_traces.extend((event.id, trace) for trace in projected)
            next_trace_sequence += len(projected)
            for log in event.logs:
                payload = log.payload
                if log.invocation_id is None:
                    if not isinstance(payload, SessionOpened):
                        raise RuntimeEventStoreError(
                            "Canonical Session Log has no Invocation identity."
                        )
                    if created_at_ns is not None:
                        raise RuntimeEventStoreError(
                            "Canonical Session contains multiple open transitions."
                        )
                    created_at_ns = log.occurred_at_ns
                    continue
                builder = builders.setdefault(
                    log.invocation_id,
                    _CanonicalInvocationBuilder(log.invocation_id),
                )
                if builder.first_event_sequence is None:
                    builder.first_event_sequence = event.sequence
                builder.last_event_sequence = event.sequence
                if isinstance(payload, InvocationOpened):
                    if builder.workflow_id is not None or builder.status is not None:
                        raise RuntimeEventStoreError(
                            "Canonical Invocation contains multiple open transitions."
                        )
                    builder.workflow_id = payload.workflow_id
                    builder.workflow_revision_id = payload.workflow_revision_id
                    builder.entry_node_id = payload.entry_node_id
                    builder.status = "created"
                    builder.created_at_ns = log.occurred_at_ns
                    current_invocation_id = log.invocation_id
                elif isinstance(payload, InvocationStarted):
                    if builder.status != "created":
                        raise RuntimeEventStoreError(
                            "Canonical Invocation start transition is invalid."
                        )
                    builder.status = "running"
                elif isinstance(payload, InvocationWaiting):
                    if builder.status != "running":
                        raise RuntimeEventStoreError(
                            "Canonical Invocation wait transition is invalid."
                        )
                    builder.status = "waiting"
                elif isinstance(payload, (WaitResumed, ChildAwaitReady)):
                    if builder.status not in {"running", "waiting"}:
                        raise RuntimeEventStoreError(
                            "Canonical Invocation resume transition is invalid."
                        )
                    builder.status = "running"
                elif isinstance(payload, InvocationRecoveryRequested):
                    if builder.status not in {"running", "waiting"}:
                        raise RuntimeEventStoreError(
                            "Canonical Invocation recovery transition is invalid."
                        )
                elif isinstance(payload, ChildAwaitSuspended):
                    if builder.status != "running":
                        raise RuntimeEventStoreError(
                            "Canonical Child await transition is invalid."
                        )
                elif isinstance(payload, ChildInvocationPlanned):
                    if builder.status != "running" or any(
                        key[0] == payload.creation_id for key in builder.children
                    ):
                        raise RuntimeEventStoreError(
                            "Canonical Child plan transition is invalid."
                        )
                    existing_sessions = {
                        child.child_session_id
                        for child in builder.children.values()
                    }
                    existing_invocations = {
                        child.planned_invocation_id
                        for child in builder.children.values()
                    }
                    for index, unit in enumerate(payload.units):
                        if (
                            unit.unit_index != index
                            or unit.child_session_id in existing_sessions
                            or unit.child_invocation_id in existing_invocations
                        ):
                            raise RuntimeEventStoreError(
                                "Canonical Child plan contains duplicate unit identity."
                            )
                        child = _CanonicalChildOwnership(
                            creation_id=payload.creation_id,
                            unit_index=unit.unit_index,
                            parent_occurrence_id=payload.parent_occurrence_id,
                            mode=payload.mode,
                            workflow_id=payload.workflow_id,
                            workflow_revision_id=payload.workflow_revision_id,
                            planned_invocation_id=unit.child_invocation_id,
                            child_session_id=unit.child_session_id,
                            planned_event_sequence=event.sequence,
                            planned_log_id=log.id,
                            change_event_sequence=event.sequence,
                            phase="planned",
                        )
                        builder.children[(payload.creation_id, unit.unit_index)] = child
                        existing_sessions.add(unit.child_session_id)
                        existing_invocations.add(unit.child_invocation_id)
                elif isinstance(payload, ChildInvocationPhaseChanged):
                    key = (payload.creation_id, payload.unit_index)
                    child = builder.children.get(key)
                    expected_phase = (
                        None
                        if child is None
                        else {
                            "planned": "opened",
                            "opened": "accepted",
                            "accepted": "terminal",
                        }.get(child.phase)
                    )
                    if child is None or payload.phase != expected_phase:
                        raise RuntimeEventStoreError(
                            "Canonical Child phase transition is invalid."
                        )
                    builder.children[key] = _CanonicalChildOwnership(
                        creation_id=child.creation_id,
                        unit_index=child.unit_index,
                        parent_occurrence_id=child.parent_occurrence_id,
                        mode=child.mode,
                        workflow_id=child.workflow_id,
                        workflow_revision_id=child.workflow_revision_id,
                        planned_invocation_id=child.planned_invocation_id,
                        child_session_id=child.child_session_id,
                        planned_event_sequence=child.planned_event_sequence,
                        planned_log_id=child.planned_log_id,
                        change_event_sequence=event.sequence,
                        phase=payload.phase,
                    )
                elif isinstance(payload, InvocationCompleted):
                    if builder.status != "running":
                        raise RuntimeEventStoreError(
                            "Canonical Invocation completion transition is invalid."
                        )
                    builder.status = "completed"
                    builder.ended_at_ns = log.occurred_at_ns
                elif isinstance(payload, (InvocationFailed, InvocationCancelled)):
                    if builder.status not in {"created", "running", "waiting"}:
                        raise RuntimeEventStoreError(
                            "Canonical Invocation terminal transition is invalid."
                        )
                    builder.status = (
                        "failed" if isinstance(payload, InvocationFailed) else "cancelled"
                    )
                    builder.ended_at_ns = log.occurred_at_ns
                operation_status = _operation_invocation_status(event, log.id)
                if operation_status is not None:
                    builder.status = operation_status
                builder.updated_at_ns = max(
                    builder.updated_at_ns or 0,
                    log.occurred_at_ns,
                )

        last_event = _decode_verified_runtime_event(event_rows[-1])
        if through_sequence is not None and last_event.sequence != through_sequence:
            raise RuntimeEventStoreError(
                "Canonical Session Event prefix is incomplete."
            )
        if through_sequence is None:
            tail = SQLiteRuntimeStore._validated_session_tail(connection, session_id)
            if tail is None or (
                _stored_integer(tail, "sequence", minimum=1) != last_event.sequence
                or _stored_string(tail, "id") != last_event.id
            ):
                raise RuntimeEventStoreError(
                    "Canonical Session Event prefix does not reach its durable head."
                )

        trace_query = _TRACE_EVENT_SELECT + " WHERE session_id = ?"
        trace_parameters: list[object] = [session_id]
        if through_sequence is not None:
            trace_query += " AND trace_sequence <= ?"
            trace_parameters.append(len(expected_traces))
        trace_query += " ORDER BY trace_sequence"
        actual_rows = connection.execute(
            trace_query,
            trace_parameters,
        ).fetchall()
        if len(actual_rows) != len(expected_traces):
            raise RuntimeEventStoreError(
                "Stored Session Trace count does not cover canonical Runtime logs."
            )
        trace_counts: dict[str, int] = {}
        trace_tails: dict[str, int] = {}
        for row, (runtime_event_id, trace) in zip(
            actual_rows,
            expected_traces,
            strict=True,
        ):
            if (
                _stored_string(row, "runtime_event_id") != runtime_event_id
                or _decode_verified_trace_event(row) != trace
            ):
                raise RuntimeEventStoreError(
                    "Stored Trace row does not match its canonical Runtime log."
                )
            if trace.invocation_id is not None:
                trace_counts[trace.invocation_id] = (
                    trace_counts.get(trace.invocation_id, 0) + 1
                )
                trace_tails[trace.invocation_id] = trace.trace_sequence

        projections: dict[str, _CanonicalInvocationProjection] = {}
        for invocation_id, builder in builders.items():
            if (
                builder.workflow_id is None
                or builder.workflow_revision_id is None
                or builder.entry_node_id is None
                or builder.status is None
                or builder.first_event_sequence is None
                or builder.last_event_sequence is None
                or builder.created_at_ns is None
                or builder.updated_at_ns is None
            ):
                raise RuntimeEventStoreError(
                    "Canonical Invocation Runtime logs are incomplete."
                )
            projections[invocation_id] = _CanonicalInvocationProjection(
                invocation_id=invocation_id,
                session_id=session_id,
                workflow_id=builder.workflow_id,
                workflow_revision_id=builder.workflow_revision_id,
                entry_node_id=builder.entry_node_id,
                status=builder.status,
                first_event_sequence=builder.first_event_sequence,
                last_event_sequence=builder.last_event_sequence,
                trace_count=trace_counts.get(invocation_id, 0),
                last_trace_sequence=trace_tails.get(invocation_id, 0),
                created_at_ns=builder.created_at_ns,
                updated_at_ns=builder.updated_at_ns,
                ended_at_ns=builder.ended_at_ns,
                children=tuple(builder.children.values()),
            )
        if created_at_ns is None or updated_at_ns is None:
            raise RuntimeEventStoreError(
                "Canonical Session Runtime logs are incomplete."
            )
        trace_last = expected_traces[-1][1].trace_sequence if expected_traces else 0
        if through_sequence is None:
            session_trace_count, session_trace_last = (
                SQLiteRuntimeStore._validated_session_trace_projection(
                    connection,
                    session_id,
                )
            )
            if (
                session_trace_count != len(expected_traces)
                or session_trace_last != trace_last
            ):
                raise RuntimeEventStoreError(
                    "Stored Session Trace head does not cover canonical Runtime logs."
                )
        return _CanonicalSessionProjection(
            session_id=session_id,
            current_invocation_id=current_invocation_id,
            invocation_count=len(projections),
            created_at_ns=created_at_ns,
            updated_at_ns=updated_at_ns,
            trace_count=len(expected_traces),
            last_trace_sequence=trace_last,
            invocations=projections,
        )

    def _validate_state_projection(
        self,
        connection: sqlite3.Connection,
        state: RuntimeState,
        *,
        ownership_cache: _ReadOwnershipCache | None = None,
    ) -> None:
        """Validate canonical current State against required query projections."""

        session = state.session
        invocation = state.invocation
        if session is None:
            raise RuntimeEventStoreError(
                "Canonical Runtime State has no Session projection identity."
            )
        session_row = connection.execute(
            """
            SELECT session_id, root_session_id, current_invocation_id
            FROM sessions WHERE session_id = ?
            """,
            (session.id,),
        ).fetchone()
        if session_row is None:
            raise RuntimeEventStoreError(
                "Canonical Runtime State has no Session projection."
            )
        if (
            _stored_string(session_row, "session_id") != session.id
            or _stored_string(
                session_row,
                "current_invocation_id",
                optional=True,
            )
            != session.latest_invocation_id
        ):
            raise RuntimeEventStoreError(
                "Stored Session projection does not match canonical Runtime State."
            )
        expected_root = self._validated_ownership_root(
            connection,
            session.id,
            ownership_cache=ownership_cache,
        )
        if _stored_string(session_row, "root_session_id") != expected_root:
            raise RuntimeEventStoreError(
                "Stored Session Root projection is inconsistent."
            )
        SQLiteRuntimeStore._validated_session_trace_projection(
            connection,
            session.id,
        )
        if invocation is None:
            return

        row = connection.execute(
            """
            SELECT invocation_id, session_id, root_session_id, workflow_id,
                   workflow_revision_id, entry_node_id, status,
                   first_event_sequence, last_event_sequence
            FROM invocations WHERE invocation_id = ?
            """,
            (invocation.id,),
        ).fetchone()
        if row is None:
            raise RuntimeEventStoreError(
                "Canonical Runtime State has no Invocation projection."
            )
        canonical = connection.execute(
            """
            SELECT session_id, MIN(sequence) AS first_event_sequence,
                   MAX(sequence) AS last_event_sequence
            FROM runtime_events WHERE invocation_id = ?
            GROUP BY session_id
            """,
            (invocation.id,),
        ).fetchall()
        if len(canonical) != 1:
            raise RuntimeEventStoreError(
                "Canonical Invocation Event range is missing or ambiguous."
            )
        canonical_row = canonical[0]
        expected = {
            "invocation_id": invocation.id,
            "session_id": session.id,
            "root_session_id": expected_root,
            "workflow_id": invocation.workflow_id,
            "workflow_revision_id": invocation.workflow_revision_id,
            "entry_node_id": invocation.entry_node_id,
            "status": invocation.status,
            "first_event_sequence": _stored_integer(
                canonical_row,
                "first_event_sequence",
                minimum=1,
            ),
            "last_event_sequence": _stored_integer(
                canonical_row,
                "last_event_sequence",
                minimum=1,
            ),
        }
        actual = {
            "invocation_id": _stored_string(row, "invocation_id"),
            "session_id": _stored_string(row, "session_id"),
            "root_session_id": _stored_string(row, "root_session_id"),
            "workflow_id": _stored_string(row, "workflow_id"),
            "workflow_revision_id": _stored_string(
                row,
                "workflow_revision_id",
            ),
            "entry_node_id": _stored_string(row, "entry_node_id"),
            "status": _stored_enum(row, "status", _INVOCATION_STATUSES),
            "first_event_sequence": _stored_integer(
                row,
                "first_event_sequence",
                minimum=1,
            ),
            "last_event_sequence": _stored_integer(
                row,
                "last_event_sequence",
                minimum=1,
            ),
        }
        if actual != expected or _stored_string(
            canonical_row,
            "session_id",
        ) != session.id:
            raise RuntimeEventStoreError(
                "Stored Invocation projection does not match canonical Runtime State."
            )
        SQLiteRuntimeStore._validated_invocation_trace_projection(
            connection,
            invocation.id,
        )

        SQLiteRuntimeStore._validate_child_units_projection(
            connection,
            invocation,
            parent_session_id=session.id,
            expected_root=expected_root,
        )

    def _validated_ownership_root(
        self,
        connection: sqlite3.Connection,
        session_id: str,
        *,
        ownership_cache: _ReadOwnershipCache | None = None,
    ) -> str:
        """Bind one ownership chain to canonical parent plans and return its Root."""

        if ownership_cache is None:
            ownership_cache = {}
        current = session_id
        visited: set[str] = set()
        chain: list[sqlite3.Row] = []
        while True:
            if current in visited:
                raise RuntimeEventStoreError(
                    "Stored Session ownership projection contains a cycle."
                )
            visited.add(current)
            row = connection.execute(
                """
                SELECT session_id, root_session_id, parent_session_id,
                       parent_invocation_id, creation_id, unit_index,
                       parent_occurrence_id, mode, workflow_id,
                       workflow_revision_id, planned_invocation_id,
                       planned_event_sequence, planned_log_id,
                       change_event_sequence, phase,
                       updated_at_ns
                FROM session_ownership WHERE session_id = ?
                """,
                (current,),
            ).fetchone()
            if row is None or _stored_string(row, "session_id") != current:
                raise RuntimeEventStoreError(
                    "Stored Session ownership projection is missing or invalid."
                )
            chain.append(row)
            parent_session_id = _stored_string(
                row,
                "parent_session_id",
                optional=True,
            )
            parent_invocation_id = _stored_string(
                row,
                "parent_invocation_id",
                optional=True,
            )
            if parent_session_id is None and parent_invocation_id is None:
                optional_fields = (
                    _stored_string(row, "creation_id", optional=True),
                    _stored_integer(row, "unit_index", optional=True),
                    _stored_string(row, "parent_occurrence_id", optional=True),
                    _stored_string(row, "mode", optional=True),
                    _stored_string(row, "workflow_id", optional=True),
                    _stored_string(
                        row,
                        "workflow_revision_id",
                        optional=True,
                    ),
                    _stored_string(
                        row,
                        "planned_invocation_id",
                        optional=True,
                    ),
                    _stored_integer(
                        row,
                        "planned_event_sequence",
                        optional=True,
                        minimum=1,
                    ),
                    _stored_string(row, "planned_log_id", optional=True),
                    _stored_integer(
                        row,
                        "change_event_sequence",
                        optional=True,
                        minimum=1,
                    ),
                    _stored_string(row, "phase", optional=True),
                )
                if any(value is not None for value in optional_fields):
                    raise RuntimeEventStoreError(
                        "Stored Root Session ownership contains Child fields."
                    )
                root_session_id = current
                break
            if parent_session_id is None or parent_invocation_id is None:
                raise RuntimeEventStoreError(
                    "Stored Child Session ownership is incomplete."
                )
            creation_id = _stored_string(row, "creation_id")
            unit_index = _stored_integer(row, "unit_index", minimum=0)
            parent_occurrence_id = _stored_string(row, "parent_occurrence_id")
            mode = _stored_enum(row, "mode", frozenset({"await", "spawn"}))
            workflow_id = _stored_string(row, "workflow_id")
            workflow_revision_id = _stored_string(
                row,
                "workflow_revision_id",
            )
            planned_invocation_id = _stored_string(
                row,
                "planned_invocation_id",
            )
            planned_event_sequence = _stored_integer(
                row,
                "planned_event_sequence",
                minimum=1,
            )
            planned_log_id = _stored_string(row, "planned_log_id")
            change_event_sequence = _stored_integer(
                row,
                "change_event_sequence",
                minimum=planned_event_sequence,
            )
            projected_phase = _stored_enum(row, "phase", _CHILD_PHASES)
            _stored_integer(row, "updated_at_ns", minimum=0)
            canonical = self._canonical_child_ownership(
                connection,
                parent_session_id=parent_session_id,
                parent_invocation_id=parent_invocation_id,
                creation_id=creation_id,
                unit_index=unit_index,
                planned_event_sequence=planned_event_sequence,
                planned_log_id=planned_log_id,
                ownership_cache=ownership_cache,
            )
            actual_descriptor = (
                creation_id,
                parent_occurrence_id,
                mode,
                workflow_id,
                workflow_revision_id,
                planned_invocation_id,
                current,
                unit_index,
                planned_event_sequence,
                planned_log_id,
                change_event_sequence,
            )
            canonical_descriptor = (
                canonical.creation_id,
                canonical.parent_occurrence_id,
                canonical.mode,
                canonical.workflow_id,
                canonical.workflow_revision_id,
                canonical.planned_invocation_id,
                canonical.child_session_id,
                canonical.unit_index,
                canonical.planned_event_sequence,
                canonical.planned_log_id,
                canonical.change_event_sequence,
            )
            if actual_descriptor != canonical_descriptor:
                raise RuntimeEventStoreError(
                    "Stored Child ownership does not match its canonical parent plan."
                )
            if projected_phase != canonical.phase:
                raise RuntimeEventStoreError(
                    "Stored Child ownership phase does not match canonical Events."
                )
            current = parent_session_id
        for row in chain:
            if _stored_string(row, "root_session_id") != root_session_id:
                raise RuntimeEventStoreError(
                    "Stored Session ownership Root does not match its parent chain."
                )
        return root_session_id

    def _canonical_child_ownership(
        self,
        connection: sqlite3.Connection,
        *,
        parent_session_id: str,
        parent_invocation_id: str,
        creation_id: str,
        unit_index: int,
        planned_event_sequence: int,
        planned_log_id: str,
        ownership_cache: _ReadOwnershipCache,
    ) -> _CanonicalChildOwnership:
        """Resolve one projected Child against canonical parent Events."""

        if connection is self._write_connection:
            children = self._cached_parent_child_ownership(
                connection,
                parent_session_id,
            )
            canonical = children.get(
                (parent_invocation_id, creation_id, unit_index)
            )
            if canonical is None:
                raise RuntimeEventStoreError(
                    "Stored Child owner has no canonical parent plan."
                )
            return canonical

        children = ownership_cache.get(parent_session_id)
        if children is None:
            children = self._read_parent_child_ownership(
                connection,
                parent_session_id,
            )
            ownership_cache[parent_session_id] = children
        canonical = children.get(
            (parent_invocation_id, creation_id, unit_index)
        )
        if canonical is None:
            raise RuntimeEventStoreError(
                "Stored Child owner has no canonical parent plan."
            )
        if (
            canonical.planned_event_sequence != planned_event_sequence
            or canonical.planned_log_id != planned_log_id
        ):
            raise RuntimeEventStoreError(
                "Stored Child owner refers to another canonical plan source."
            )
        return canonical

    def _read_parent_child_ownership(
        self,
        connection: sqlite3.Connection,
        parent_session_id: str,
    ) -> dict[_OwnershipKey, _CanonicalChildOwnership]:
        """Derive all Child facts once for one read-transaction snapshot."""

        tail = self._validated_session_tail(connection, parent_session_id)
        if tail is None:
            raise RuntimeEventStoreError(
                "Stored Child owner has no canonical parent Session."
            )
        tail_event = _decode_verified_runtime_event(tail)
        tail_digest = _verify_runtime_event_digest(tail)
        children, last_row = self._scan_parent_child_ownership(
            connection,
            parent_session_id,
            start_sequence=1,
            previous=None,
            children={},
        )
        if last_row is None:
            raise RuntimeEventStoreError(
                "Stored Child owner has no canonical parent Event chain."
            )
        last_event = _decode_verified_runtime_event(last_row)
        if (
            last_event.sequence != tail_event.sequence
            or last_event.id != tail_event.id
            or _verify_runtime_event_digest(last_row) != tail_digest
        ):
            raise RuntimeEventStoreError(
                "Canonical parent Event chain does not reach its durable head."
            )
        return children

    @staticmethod
    def _scan_parent_child_ownership(
        connection: sqlite3.Connection,
        parent_session_id: str,
        *,
        start_sequence: int,
        previous: sqlite3.Row | None,
        children: dict[_OwnershipKey, _CanonicalChildOwnership],
    ) -> tuple[
        dict[_OwnershipKey, _CanonicalChildOwnership],
        sqlite3.Row | None,
    ]:
        """Validate one parent Event suffix and update its Child facts."""

        rows = connection.execute(
            _RUNTIME_EVENT_SELECT
            + " WHERE session_id = ? AND sequence >= ? ORDER BY sequence",
            (parent_session_id, start_sequence),
        ).fetchall()
        last_row = previous
        for event_row in rows:
            event = _decode_verified_runtime_event(event_row)
            SQLiteRuntimeStore._validate_chain(event, last_row)
            last_row = event_row
            for log in event.logs:
                payload = log.payload
                if isinstance(payload, ChildInvocationPlanned):
                    if log.invocation_id is None:
                        raise RuntimeEventStoreError(
                            "Canonical Child plan has no parent Invocation."
                        )
                    for unit in payload.units:
                        key = (
                            log.invocation_id,
                            payload.creation_id,
                            unit.unit_index,
                        )
                        canonical = _CanonicalChildOwnership(
                            creation_id=payload.creation_id,
                            unit_index=unit.unit_index,
                            parent_occurrence_id=payload.parent_occurrence_id,
                            mode=payload.mode,
                            workflow_id=payload.workflow_id,
                            workflow_revision_id=payload.workflow_revision_id,
                            planned_invocation_id=unit.child_invocation_id,
                            child_session_id=unit.child_session_id,
                            planned_event_sequence=event.sequence,
                            planned_log_id=log.id,
                            change_event_sequence=event.sequence,
                            phase="planned",
                        )
                        existing = children.get(key)
                        if existing is not None:
                            raise RuntimeEventStoreError(
                                "Canonical parent Events contain conflicting Child plans."
                            )
                        children[key] = canonical
                elif isinstance(payload, ChildInvocationPhaseChanged):
                    if log.invocation_id is None:
                        raise RuntimeEventStoreError(
                            "Canonical Child phase has no parent Invocation."
                        )
                    key = (
                        log.invocation_id,
                        payload.creation_id,
                        payload.unit_index,
                    )
                    canonical = children.get(key)
                    if canonical is None:
                        raise RuntimeEventStoreError(
                            "Canonical Child phase has no preceding plan."
                        )
                    expected_phase = {
                        "planned": "opened",
                        "opened": "accepted",
                        "accepted": "terminal",
                    }.get(canonical.phase)
                    if payload.phase != expected_phase:
                        raise RuntimeEventStoreError(
                            "Canonical Child phase transition is invalid."
                        )
                    children[key] = _CanonicalChildOwnership(
                        creation_id=canonical.creation_id,
                        unit_index=canonical.unit_index,
                        parent_occurrence_id=canonical.parent_occurrence_id,
                        mode=canonical.mode,
                        workflow_id=canonical.workflow_id,
                        workflow_revision_id=canonical.workflow_revision_id,
                        planned_invocation_id=canonical.planned_invocation_id,
                        child_session_id=canonical.child_session_id,
                        planned_event_sequence=canonical.planned_event_sequence,
                        planned_log_id=canonical.planned_log_id,
                        change_event_sequence=event.sequence,
                        phase=payload.phase,
                    )
        return children, last_row

    def _cached_parent_child_ownership(
        self,
        connection: sqlite3.Connection,
        parent_session_id: str,
    ) -> dict[_OwnershipKey, _CanonicalChildOwnership]:
        """Incrementally validate and cache one writer-owned parent Event chain."""

        tail = self._validated_session_tail(connection, parent_session_id)
        if tail is None:
            raise RuntimeEventStoreError(
                "Stored Child owner has no canonical parent Session."
            )
        tail_event = _decode_verified_runtime_event(tail)
        tail_digest = _verify_runtime_event_digest(tail)
        cached = self._canonical_ownership.get(parent_session_id)
        can_extend = bool(
            cached is not None
            and cached.generation == self._validation_generation
            and cached.last_sequence <= tail_event.sequence
        )
        if can_extend and cached is not None:
            previous = connection.execute(
                _RUNTIME_EVENT_SELECT
                + " WHERE session_id = ? AND sequence = ?",
                (parent_session_id, cached.last_sequence),
            ).fetchone()
            if (
                previous is None
                or _stored_string(previous, "id") != cached.last_event_id
                or _verify_runtime_event_digest(previous) != cached.last_event_digest
            ):
                can_extend = False
        else:
            previous = None

        if can_extend and cached is not None:
            if (
                cached.last_sequence == tail_event.sequence
                and cached.last_event_id == tail_event.id
                and cached.last_event_digest == tail_digest
            ):
                self._canonical_ownership.move_to_end(parent_session_id)
                return cached.children
            start_sequence = cached.last_sequence + 1
            children = dict(cached.children)
        else:
            start_sequence = 1
            previous = None
            children = {}

        children, last_row = self._scan_parent_child_ownership(
            connection,
            parent_session_id,
            start_sequence=start_sequence,
            previous=previous,
            children=children,
        )
        if last_row is None:
            raise RuntimeEventStoreError(
                "Stored Child owner has no canonical parent Event chain."
            )
        last_event = _decode_verified_runtime_event(last_row)
        if (
            last_event.sequence != tail_event.sequence
            or last_event.id != tail_event.id
            or _verify_runtime_event_digest(last_row) != tail_digest
        ):
            raise RuntimeEventStoreError(
                "Canonical parent Event chain does not reach its durable head."
            )
        entry = _CanonicalOwnershipCache(
            generation=self._validation_generation,
            last_sequence=tail_event.sequence,
            last_event_id=tail_event.id,
            last_event_digest=tail_digest,
            children=children,
        )
        self._canonical_ownership[parent_session_id] = entry
        self._canonical_ownership.move_to_end(parent_session_id)
        while len(self._canonical_ownership) > _VALIDATED_OWNERSHIP_CACHE_SIZE:
            self._canonical_ownership.popitem(last=False)
        return entry.children

    @staticmethod
    def _validate_child_units_projection(
        connection: sqlite3.Connection,
        invocation,
        *,
        parent_session_id: str,
        expected_root: str,
    ) -> None:
        expected_units = tuple(
            _CanonicalChildOwnership(
                creation_id=plan.creation_id,
                unit_index=unit.unit_index,
                parent_occurrence_id=plan.parent_occurrence_id,
                mode=plan.mode,
                workflow_id=plan.workflow_id,
                workflow_revision_id=plan.workflow_revision_id,
                planned_invocation_id=unit.invocation_id,
                child_session_id=unit.session_id,
                planned_event_sequence=0,
                planned_log_id="",
                change_event_sequence=0,
                phase=unit.phase,
            )
            for plan in invocation.child_plans.values()
            for unit in plan.units
        )
        SQLiteRuntimeStore._validate_child_unit_facts(
            connection,
            invocation.id,
            parent_session_id=parent_session_id,
            expected_root=expected_root,
            children=expected_units,
            include_plan_source=False,
        )

    @staticmethod
    def _validate_child_unit_facts(
        connection: sqlite3.Connection,
        invocation_id: str,
        *,
        parent_session_id: str,
        expected_root: str,
        children: tuple[_CanonicalChildOwnership, ...],
        include_plan_source: bool = True,
    ) -> None:
        """Compare materialized Child ownership with canonical parent facts."""

        expected_units = {
            child.child_session_id: (
                expected_root,
                parent_session_id,
                invocation_id,
                child.creation_id,
                child.unit_index,
                child.parent_occurrence_id,
                child.mode,
                child.workflow_id,
                child.workflow_revision_id,
                child.planned_invocation_id,
                child.phase,
                *(
                    (
                        child.planned_event_sequence,
                        child.planned_log_id,
                        child.change_event_sequence,
                    )
                    if include_plan_source
                    else ()
                ),
            )
            for child in children
        }
        selected_source = (
            ", planned_event_sequence, planned_log_id, change_event_sequence"
            if include_plan_source
            else ""
        )
        child_rows = connection.execute(
            f"""
            SELECT session_id, root_session_id, parent_session_id,
                   parent_invocation_id, creation_id, unit_index,
                   parent_occurrence_id, mode, workflow_id, workflow_revision_id,
                   planned_invocation_id, phase{selected_source}
            FROM session_ownership WHERE parent_invocation_id = ?
            """,
            (invocation_id,),
        ).fetchall()
        actual_units = {
            _stored_string(child, "session_id"): (
                _stored_string(child, "root_session_id"),
                _stored_string(child, "parent_session_id"),
                _stored_string(child, "parent_invocation_id"),
                _stored_string(child, "creation_id"),
                _stored_integer(child, "unit_index", minimum=0),
                _stored_string(child, "parent_occurrence_id"),
                _stored_string(child, "mode"),
                _stored_string(child, "workflow_id"),
                _stored_string(child, "workflow_revision_id"),
                _stored_string(child, "planned_invocation_id"),
                _stored_enum(child, "phase", _CHILD_PHASES),
                *(
                    (
                        _stored_integer(
                            child,
                            "planned_event_sequence",
                            minimum=1,
                        ),
                        _stored_string(child, "planned_log_id"),
                        _stored_integer(
                            child,
                            "change_event_sequence",
                            minimum=1,
                        ),
                    )
                    if include_plan_source
                    else ()
                ),
            )
            for child in child_rows
        }
        if actual_units != expected_units:
            raise RuntimeEventStoreError(
                "Stored Child ownership projection does not match canonical Runtime State."
            )

    def _validate_invocation_projection(
        self,
        connection: sqlite3.Connection,
        invocation_id: str,
    ) -> _CanonicalInvocationProjection:
        """Validate one current or historical Invocation materialization."""

        row = connection.execute(
            "SELECT * FROM invocations WHERE invocation_id = ?",
            (invocation_id,),
        ).fetchone()
        if row is None:
            raise RuntimeEventStoreError(
                "Canonical Invocation has no materialized projection."
            )
        canonical = SQLiteRuntimeStore._canonical_invocation_projection(
            connection,
            invocation_id,
        )
        expected_root = self._validated_ownership_root(
            connection,
            canonical.session_id,
        )
        self._validate_invocation_projection_row(
            connection,
            row,
            canonical,
            expected_root=expected_root,
        )
        return canonical

    @staticmethod
    def _validate_invocation_projection_row(
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        canonical: _CanonicalInvocationProjection,
        *,
        expected_root: str,
    ) -> None:
        """Compare one SQL Invocation row with single-pass canonical facts."""

        actual = {
            "invocation_id": _stored_string(row, "invocation_id"),
            "session_id": _stored_string(row, "session_id"),
            "root_session_id": _stored_string(row, "root_session_id"),
            "workflow_id": _stored_string(row, "workflow_id"),
            "workflow_revision_id": _stored_string(
                row,
                "workflow_revision_id",
            ),
            "entry_node_id": _stored_string(row, "entry_node_id"),
            "status": _stored_enum(row, "status", _INVOCATION_STATUSES),
            "first_event_sequence": _stored_integer(
                row,
                "first_event_sequence",
                minimum=1,
            ),
            "last_event_sequence": _stored_integer(
                row,
                "last_event_sequence",
                minimum=1,
            ),
            "trace_count": _stored_integer(row, "trace_count", minimum=0),
            "last_trace_sequence": _stored_integer(
                row,
                "last_trace_sequence",
                minimum=0,
            ),
            "created_at_ns": _stored_integer(row, "created_at_ns", minimum=0),
            "updated_at_ns": _stored_integer(row, "updated_at_ns", minimum=0),
            "ended_at_ns": _stored_integer(
                row,
                "ended_at_ns",
                minimum=0,
                optional=True,
            ),
        }
        expected = {
            "invocation_id": canonical.invocation_id,
            "session_id": canonical.session_id,
            "root_session_id": expected_root,
            "workflow_id": canonical.workflow_id,
            "workflow_revision_id": canonical.workflow_revision_id,
            "entry_node_id": canonical.entry_node_id,
            "status": canonical.status,
            "first_event_sequence": canonical.first_event_sequence,
            "last_event_sequence": canonical.last_event_sequence,
            "trace_count": canonical.trace_count,
            "last_trace_sequence": canonical.last_trace_sequence,
            "created_at_ns": canonical.created_at_ns,
            "updated_at_ns": canonical.updated_at_ns,
            "ended_at_ns": canonical.ended_at_ns,
        }
        if actual != expected:
            raise RuntimeEventStoreError(
                "Stored Invocation projection does not match its canonical range."
            )
        SQLiteRuntimeStore._validate_child_unit_facts(
            connection,
            _stored_string(row, "invocation_id"),
            parent_session_id=canonical.session_id,
            expected_root=expected_root,
            children=canonical.children,
        )

    def _validate_session_invocation_projections(
        self,
        connection: sqlite3.Connection,
        session_id: str,
    ) -> None:
        """Validate all historical Invocation projections after external writes."""

        canonical = SQLiteRuntimeStore._canonical_session_projection(
            connection,
            session_id,
        )
        rows = connection.execute(
            "SELECT * FROM invocations WHERE session_id = ?",
            (session_id,),
        ).fetchall()
        actual_ids = {_stored_string(row, "invocation_id") for row in rows}
        if actual_ids != set(canonical.invocations):
            raise RuntimeEventStoreError(
                "Canonical Invocation has no materialized projection or an "
                "unexpected projection lacks canonical Runtime logs."
            )
        expected_root = self._validated_ownership_root(connection, session_id)
        for row in rows:
            invocation_id = _stored_string(row, "invocation_id")
            self._validate_invocation_projection_row(
                connection,
                row,
                canonical.invocations[invocation_id],
                expected_root=expected_root,
            )

    def _validate_session_projection(
        self,
        connection: sqlite3.Connection,
        session_id: str,
    ) -> tuple[_CanonicalSessionProjection, str]:
        """Validate one Session summary without rebuilding Scheduler State."""

        canonical = SQLiteRuntimeStore._canonical_session_projection(
            connection,
            session_id,
        )
        row = connection.execute(
            "SELECT * FROM sessions WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        if row is None:
            raise RuntimeEventStoreError(
                "Canonical Session data has no Session projection."
            )
        expected_root = self._validated_ownership_root(connection, session_id)
        actual = {
            "session_id": _stored_string(row, "session_id"),
            "root_session_id": _stored_string(row, "root_session_id"),
            "current_invocation_id": _stored_string(
                row,
                "current_invocation_id",
                optional=True,
            ),
            "invocation_count": _stored_integer(
                row,
                "invocation_count",
                minimum=0,
            ),
            "trace_count": _stored_integer(row, "trace_count", minimum=0),
            "last_trace_sequence": _stored_integer(
                row,
                "last_trace_sequence",
                minimum=0,
            ),
            "created_at_ns": _stored_integer(row, "created_at_ns", minimum=0),
            "updated_at_ns": _stored_integer(row, "updated_at_ns", minimum=0),
        }
        expected = {
            "session_id": session_id,
            "root_session_id": expected_root,
            "current_invocation_id": canonical.current_invocation_id,
            "invocation_count": canonical.invocation_count,
            "trace_count": canonical.trace_count,
            "last_trace_sequence": canonical.last_trace_sequence,
            "created_at_ns": canonical.created_at_ns,
            "updated_at_ns": canonical.updated_at_ns,
        }
        if actual != expected:
            raise RuntimeEventStoreError(
                "Stored Session projection does not match canonical Runtime logs."
            )
        materialized_ids = {
            _stored_string(item, "invocation_id")
            for item in connection.execute(
                "SELECT invocation_id FROM invocations WHERE session_id = ?",
                (session_id,),
            ).fetchall()
        }
        if materialized_ids != set(canonical.invocations):
            raise RuntimeEventStoreError(
                "Canonical Invocation has no materialized projection or an "
                "unexpected projection lacks canonical Runtime logs."
            )
        return canonical, expected_root

    @staticmethod
    def _validate_session_summary_row(
        row: sqlite3.Row,
        canonical: _CanonicalSessionProjection,
        expected_root: str,
        *,
        workflow_revision_id: str | None,
    ) -> _CanonicalInvocationProjection | None:
        """Validate fields exposed by one filtered or unfiltered Session row."""

        selected_id = _stored_string(
            row,
            "current_invocation_id",
            optional=True,
        )
        selected = (
            canonical.invocations.get(selected_id)
            if selected_id is not None
            else None
        )
        if selected_id is not None and selected is None:
            raise RuntimeEventStoreError(
                "Stored Session current Invocation has no canonical Runtime logs."
            )
        if workflow_revision_id is None:
            expected_count = canonical.invocation_count
            expected_updated_at = canonical.updated_at_ns
            expected_selected_id = canonical.current_invocation_id
        else:
            matching = tuple(
                invocation
                for invocation in canonical.invocations.values()
                if invocation.workflow_revision_id == workflow_revision_id
            )
            expected_count = len(matching)
            expected_updated_at = selected.updated_at_ns if selected is not None else None
            expected_selected_id = (
                max(matching, key=lambda item: item.first_event_sequence).invocation_id
                if matching
                else None
            )
        actual = {
            "session_id": _stored_string(row, "session_id"),
            "root_session_id": _stored_string(row, "root_session_id"),
            "current_invocation_id": selected_id,
            "invocation_count": _stored_integer(
                row,
                "invocation_count",
                minimum=0,
            ),
            "workflow_id": _stored_string(row, "workflow_id", optional=True),
            "workflow_revision_id": _stored_string(
                row,
                "workflow_revision_id",
                optional=True,
            ),
            "status": _stored_enum(
                row,
                "status",
                _INVOCATION_STATUSES,
                optional=True,
            ),
            "created_at_ns": _stored_integer(row, "created_at_ns", minimum=0),
            "updated_at_ns": _stored_integer(row, "updated_at_ns", minimum=0),
        }
        expected = {
            "session_id": canonical.session_id,
            "root_session_id": expected_root,
            "current_invocation_id": expected_selected_id,
            "invocation_count": expected_count,
            "workflow_id": selected.workflow_id if selected is not None else None,
            "workflow_revision_id": (
                selected.workflow_revision_id if selected is not None else None
            ),
            "status": selected.status if selected is not None else None,
            "created_at_ns": canonical.created_at_ns,
            "updated_at_ns": expected_updated_at,
        }
        if actual != expected:
            raise RuntimeEventStoreError(
                "Stored Session summary does not match canonical Runtime logs."
            )
        return selected

    @staticmethod
    def _validate_chain(event: RuntimeEvent, previous: sqlite3.Row | None) -> None:
        if event.from_state_version is None or event.to_state_version is None:
            raise RuntimeEventSequenceError(
                "SQLite Runtime Store only accepts sealed Runtime Events."
            )
        if previous is None:
            if event.sequence != 1:
                raise RuntimeEventSequenceError(
                    f"First Session Event must use sequence 1, got {event.sequence}."
                )
            if event.previous_event_id is not None or event.previous_event_digest is not None:
                raise RuntimeEventSequenceError(
                    "First Session Event cannot reference a previous Event."
                )
            if event.from_state_version != 0:
                raise RuntimeEventSequenceError(
                    "First Session Event must start at state version 0."
                )
            return
        expected_sequence = int(previous["sequence"]) + 1
        previous_digest = _verify_runtime_event_digest(previous)
        if event.sequence != expected_sequence:
            raise RuntimeEventSequenceError(
                f"Expected Session Event sequence {expected_sequence}, got {event.sequence}."
            )
        if (
            event.previous_event_id != previous["id"]
            or event.previous_event_digest != previous_digest
        ):
            raise RuntimeEventSequenceError(
                "Runtime Event does not extend the stored Session hash chain."
            )
        previous_event = _decode_verified_runtime_event(previous)
        if previous_event.to_state_version != event.from_state_version:
            raise RuntimeEventSequenceError(
                "Runtime Event does not extend the stored state-version chain."
            )

    def _validate_replayable(
        self,
        connection: sqlite3.Connection,
        event: RuntimeEvent,
        previous: sqlite3.Row | None,
    ) -> RuntimeState:
        if previous is None:
            state = RuntimeState()
        else:
            previous_digest = _verify_runtime_event_digest(previous)
            cached = self._validated_states.get(event.session_id)
            if (
                cached is not None
                and self._validated_generations.get(event.session_id)
                == self._validation_generation
                and cached.sequence == int(previous["sequence"])
                and cached.last_event_id == previous["id"]
                and cached.last_event_digest == previous_digest
            ):
                state = cached
                self._validated_states.move_to_end(event.session_id)
            else:
                state = self._rebuild_state_with_connection(
                    connection,
                    event.session_id,
                    int(previous["sequence"]),
                )
        try:
            return StateReducer().validate_sealed(state, event)
        except Exception as error:
            raise RuntimeEventStoreError(
                "Runtime Event cannot be replayed into a valid Runtime State."
            ) from error

    def _cache_validated_state(
        self,
        session_id: str,
        state: RuntimeState,
    ) -> None:
        self._mark_session_validated(session_id)
        invocation = state.invocation
        has_unsettled_children = bool(
            invocation is not None
            and any(
                unit.phase != "terminal"
                for plan in invocation.child_plans.values()
                for unit in plan.units
            )
        )
        if (
            invocation is not None
            and invocation.terminal
            and not has_unsettled_children
        ):
            self._validated_states.pop(session_id, None)
            self._canonical_ownership.pop(session_id, None)
            return
        self._validated_states[session_id] = state
        self._validated_states.move_to_end(session_id)
        while len(self._validated_states) > _VALIDATED_STATE_CACHE_SIZE:
            self._validated_states.popitem(last=False)

    def _mark_session_validated(self, session_id: str) -> None:
        self._validated_generations[session_id] = self._validation_generation
        self._validated_generations.move_to_end(session_id)
        while len(self._validated_generations) > _VALIDATED_STATE_CACHE_SIZE:
            self._validated_generations.popitem(last=False)

    def _observe_external_database_change(
        self,
        connection: sqlite3.Connection,
    ) -> bool:
        current = _sqlite_data_version(connection)
        if self._write_data_version is None:
            self._write_data_version = current
            return False
        if current == self._write_data_version:
            return False
        _validate_existing_schema(connection, _sqlite_tables(connection))
        self._write_data_version = current
        self._validation_generation += 1
        self._validated_states.clear()
        self._canonical_ownership.clear()
        return True

    def _index_event(
        self,
        connection: sqlite3.Connection,
        event: RuntimeEvent,
        state: RuntimeState,
        event_digest: str,
    ) -> None:
        previous_trace = connection.execute(
            _TRACE_EVENT_SELECT
            + " WHERE session_id = ? ORDER BY trace_sequence DESC LIMIT 1",
            (event.session_id,),
        ).fetchone()
        previous_trace_sequence = (
            0
            if previous_trace is None
            else _decode_anchored_trace_event(
                connection,
                previous_trace,
            ).trace_sequence
        )
        start_sequence = previous_trace_sequence + 1
        traces = project_trace_events(event, start_sequence=start_sequence)
        for trace in traces:
            connection.execute(
                """
                INSERT INTO trace_events(
                    id, runtime_event_id, session_id, invocation_id,
                    trace_sequence, kind, status, occurred_at_ns, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trace.id,
                    event.id,
                    trace.session_id,
                    trace.invocation_id,
                    trace.trace_sequence,
                    trace.kind,
                    trace.status,
                    trace.occurred_at_ns,
                    _canonical_json(trace.to_record()),
                ),
            )
        for log in event.logs:
            payload = log.payload
            if isinstance(payload, SessionOpened):
                self._index_session_opened(
                    connection,
                    event,
                    log.occurred_at_ns,
                    event_digest,
                )
            if isinstance(payload, InvocationOpened):
                assert log.invocation_id is not None
                self._index_invocation_opened(
                    connection,
                    event,
                    log.invocation_id,
                    payload,
                    log.occurred_at_ns,
                )
            if isinstance(payload, ChildInvocationPlanned):
                self._index_child_ownership(
                    connection,
                    event,
                    log.invocation_id,
                    log.id,
                    payload,
                )
            elif isinstance(payload, ChildInvocationPhaseChanged):
                self._index_child_phase(
                    connection,
                    event,
                    log.invocation_id,
                    payload,
                )
            if log.invocation_id is not None:
                updated_invocation = connection.execute(
                    """
                    UPDATE invocations
                    SET last_event_sequence = MAX(last_event_sequence, ?),
                        updated_at_ns = MAX(updated_at_ns, ?)
                    WHERE invocation_id = ?
                    """,
                    (event.sequence, log.occurred_at_ns, log.invocation_id),
                )
                if updated_invocation.rowcount != 1:
                    raise RuntimeEventStoreError(
                        "Canonical Runtime log refers to a missing Invocation projection."
                    )
        updated_session = connection.execute(
            "UPDATE sessions SET updated_at_ns = MAX(updated_at_ns, ?) "
            "WHERE session_id = ?",
            (event.occurred_at_ns, event.session_id),
        )
        if updated_session.rowcount != 1:
            raise RuntimeEventStoreError(
                "Canonical Runtime Event refers to a missing Session projection."
            )
        self._advance_trace_projection(
            connection,
            event.session_id,
            start_sequence,
            traces,
        )
        self._sync_runtime_projection(connection, event, state)

    @staticmethod
    def _advance_trace_projection(
        connection: sqlite3.Connection,
        session_id: str,
        start_sequence: int,
        traces: tuple[TraceEvent, ...],
    ) -> None:
        if not traces:
            return
        last = traces[-1]
        last_record = _canonical_json(last.to_record())
        last_digest = hashlib.sha256(last_record.encode("utf-8")).hexdigest()
        updated_session = connection.execute(
            """
            UPDATE sessions
            SET trace_count = trace_count + ?, last_trace_sequence = ?,
                last_trace_id = ?, last_trace_digest = ?
            WHERE session_id = ? AND last_trace_sequence = ?
            """,
            (
                len(traces),
                last.trace_sequence,
                last.id,
                last_digest,
                session_id,
                start_sequence - 1,
            ),
        )
        if updated_session.rowcount != 1:
            raise RuntimeEventStoreError(
                "Trace projection does not extend exactly one Session head."
            )

        by_invocation: dict[str, list[TraceEvent]] = {}
        for trace in traces:
            if trace.invocation_id is not None:
                by_invocation.setdefault(trace.invocation_id, []).append(trace)
        for invocation_id, invocation_traces in by_invocation.items():
            updated_invocation = connection.execute(
                """
                UPDATE invocations
                SET trace_count = trace_count + ?, last_trace_sequence = ?
                WHERE invocation_id = ? AND last_trace_sequence < ?
                """,
                (
                    len(invocation_traces),
                    invocation_traces[-1].trace_sequence,
                    invocation_id,
                    invocation_traces[0].trace_sequence,
                ),
            )
            if updated_invocation.rowcount != 1:
                raise RuntimeEventStoreError(
                    "Trace projection does not extend exactly one Invocation head."
                )

    @staticmethod
    def _index_session_opened(
        connection: sqlite3.Connection,
        event: RuntimeEvent,
        occurred_at_ns: int,
        event_digest: str,
    ) -> None:
        ownership = connection.execute(
            "SELECT root_session_id FROM session_ownership WHERE session_id = ?",
            (event.session_id,),
        ).fetchone()
        root_session_id = (
            ownership["root_session_id"] if ownership is not None else event.session_id
        )
        connection.execute(
            """
            INSERT INTO session_ownership(session_id, root_session_id, updated_at_ns)
            VALUES (?, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                updated_at_ns = MAX(updated_at_ns, excluded.updated_at_ns)
            """,
            (event.session_id, root_session_id, occurred_at_ns),
        )
        connection.execute(
            """
            INSERT INTO sessions(
                session_id, root_session_id, current_invocation_id,
                invocation_count, last_event_sequence, last_event_id,
                last_event_digest, trace_count, last_trace_sequence,
                last_trace_id, last_trace_digest, created_at_ns, updated_at_ns
            ) VALUES (?, ?, NULL, 0, ?, ?, ?, 0, 0, NULL, NULL, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                root_session_id = excluded.root_session_id,
                updated_at_ns = MAX(updated_at_ns, excluded.updated_at_ns)
            """,
            (
                event.session_id,
                root_session_id,
                event.sequence,
                event.id,
                event_digest,
                occurred_at_ns,
                occurred_at_ns,
            ),
        )

    @staticmethod
    def _index_invocation_opened(
        connection: sqlite3.Connection,
        event: RuntimeEvent,
        invocation_id: str,
        payload: InvocationOpened,
        occurred_at_ns: int,
    ) -> None:
        ownership = connection.execute(
            """
            SELECT root_session_id, planned_invocation_id,
                   workflow_id, workflow_revision_id
            FROM session_ownership WHERE session_id = ?
            """,
            (event.session_id,),
        ).fetchone()
        if (
            ownership is not None
            and ownership["planned_invocation_id"] is not None
            and (
                ownership["planned_invocation_id"] != invocation_id
                or ownership["workflow_id"] != payload.workflow_id
                or ownership["workflow_revision_id"]
                != payload.workflow_revision_id
            )
        ):
            raise RuntimeEventConflictError(
                "Opened Child Invocation does not match its planned identity "
                "and Workflow revision."
            )
        root_session_id = (
            ownership["root_session_id"] if ownership is not None else event.session_id
        )
        existed = connection.execute(
            "SELECT 1 FROM invocations WHERE invocation_id = ?",
            (invocation_id,),
        ).fetchone()
        connection.execute(
            """
            INSERT INTO invocations(
                invocation_id, session_id, root_session_id, workflow_id,
                workflow_revision_id, entry_node_id, status,
                first_event_sequence, last_event_sequence,
                trace_count, last_trace_sequence, created_at_ns,
                updated_at_ns, ended_at_ns
            ) VALUES (?, ?, ?, ?, ?, ?, 'created', ?, ?, 0, 0, ?, ?, NULL)
            """,
            (
                invocation_id,
                event.session_id,
                root_session_id,
                payload.workflow_id,
                payload.workflow_revision_id,
                payload.entry_node_id,
                event.sequence,
                event.sequence,
                occurred_at_ns,
                occurred_at_ns,
            ),
        )
        connection.execute(
            """
            UPDATE sessions
            SET current_invocation_id = ?,
                invocation_count = invocation_count + ?,
                updated_at_ns = MAX(updated_at_ns, ?)
            WHERE session_id = ?
            """,
            (
                invocation_id,
                0 if existed is not None else 1,
                occurred_at_ns,
                event.session_id,
            ),
        )

    @staticmethod
    def _sync_runtime_projection(
        connection: sqlite3.Connection,
        event: RuntimeEvent,
        state: RuntimeState,
    ) -> None:
        if state.session is not None:
            updated_session = connection.execute(
                """
                UPDATE sessions
                SET current_invocation_id = ?, updated_at_ns = ?
                WHERE session_id = ?
                """,
                (
                    state.session.latest_invocation_id,
                    state.session.updated_at_ns,
                    state.session.id,
                ),
            )
            if updated_session.rowcount != 1:
                raise RuntimeEventStoreError(
                    "Canonical Runtime State has no matching Session projection."
                )
        invocation = state.invocation
        if invocation is not None:
            updated_invocation = connection.execute(
                """
                UPDATE invocations
                SET status = ?, last_event_sequence = ?, updated_at_ns = ?,
                    ended_at_ns = ?
                WHERE invocation_id = ? AND session_id = ?
                  AND workflow_id = ? AND workflow_revision_id = ?
                  AND entry_node_id = ?
                """,
                (
                    invocation.status,
                    event.sequence,
                    event.occurred_at_ns,
                    invocation.completed_at_ns if invocation.terminal else None,
                    invocation.id,
                    event.session_id,
                    invocation.workflow_id,
                    invocation.workflow_revision_id,
                    invocation.entry_node_id,
                ),
            )
            if updated_invocation.rowcount != 1:
                raise RuntimeEventStoreError(
                    "Canonical Runtime State has no matching Invocation projection."
                )

    @staticmethod
    def _index_child_ownership(
        connection: sqlite3.Connection,
        event: RuntimeEvent,
        parent_invocation_id: str | None,
        planned_log_id: str,
        payload: ChildInvocationPlanned,
    ) -> None:
        if parent_invocation_id is None:
            return
        ownership = connection.execute(
            "SELECT root_session_id FROM session_ownership WHERE session_id = ?",
            (event.session_id,),
        ).fetchone()
        root_session_id = (
            ownership["root_session_id"] if ownership is not None else event.session_id
        )
        for unit in payload.units:
            existing = connection.execute(
                """
                SELECT root_session_id, parent_session_id, parent_invocation_id,
                       creation_id, unit_index, parent_occurrence_id, mode,
                       workflow_id, workflow_revision_id,
                       planned_invocation_id, planned_event_sequence,
                       planned_log_id, change_event_sequence
                FROM session_ownership WHERE session_id = ?
                """,
                (unit.child_session_id,),
            ).fetchone()
            descriptor = (
                root_session_id,
                event.session_id,
                parent_invocation_id,
                payload.creation_id,
                unit.unit_index,
                payload.parent_occurrence_id,
                payload.mode,
                payload.workflow_id,
                payload.workflow_revision_id,
                unit.child_invocation_id,
                event.sequence,
                planned_log_id,
                event.sequence,
            )
            if existing is not None and tuple(existing) != descriptor:
                raise RuntimeEventConflictError(
                    f"Child Session {unit.child_session_id!r} has another owner."
                )
            connection.execute(
                """
                INSERT INTO session_ownership(
                    session_id, root_session_id, parent_session_id,
                    parent_invocation_id, creation_id, unit_index,
                    parent_occurrence_id, mode, workflow_id,
                    workflow_revision_id, planned_invocation_id,
                    planned_event_sequence, planned_log_id,
                    change_event_sequence, phase, updated_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'planned', ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    updated_at_ns = MAX(updated_at_ns, excluded.updated_at_ns)
                """,
                (
                    unit.child_session_id,
                    *descriptor,
                    event.occurred_at_ns,
                ),
            )

    @staticmethod
    def _index_child_phase(
        connection: sqlite3.Connection,
        event: RuntimeEvent,
        parent_invocation_id: str | None,
        payload: ChildInvocationPhaseChanged,
    ) -> None:
        if parent_invocation_id is None:
            return
        updated = connection.execute(
            """
            UPDATE session_ownership
            SET phase = ?, change_event_sequence = ?,
                updated_at_ns = MAX(updated_at_ns, ?)
            WHERE parent_invocation_id = ?
              AND creation_id = ? AND unit_index = ?
            """,
            (
                payload.phase,
                event.sequence,
                event.occurred_at_ns,
                parent_invocation_id,
                payload.creation_id,
                payload.unit_index,
            ),
        )
        if updated.rowcount != 1:
            raise RuntimeEventConflictError(
                "Child Invocation phase does not identify one planned unit."
            )

    def _save_workflow(self, snapshot: WorkflowDefinitionSnapshot) -> bool:
        connection = self._connection()
        encoded = _canonical_json(snapshot.to_record())
        connection.execute("BEGIN IMMEDIATE")
        try:
            self._observe_external_database_change(connection)
            existing = connection.execute(
                _WORKFLOW_DEFINITION_SELECT + " WHERE revision_id = ?",
                (snapshot.workflow_revision_id,),
            ).fetchone()
            if existing is not None:
                _decode_verified_workflow_definition(existing)
                if existing["record_json"] != encoded:
                    raise RuntimeEventConflictError(
                        "Workflow revision has conflicting portable content."
                    )
                connection.commit()
                return False
            connection.execute(
                """
                INSERT INTO workflow_definitions(
                    revision_id, workflow_id, workflow_version, definition_hash,
                    created_at_ns, record_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.workflow_revision_id,
                    snapshot.workflow_id,
                    snapshot.workflow_version,
                    snapshot.definition_hash,
                    time.time_ns(),
                    encoded,
                ),
            )
            connection.commit()
            return True
        except BaseException:
            connection.rollback()
            raise

    # ------------------------------------------------------------------
    # Reader implementation

    def _list_workflows(
        self, limit: int, before_row_id: int | None
    ) -> Page[dict[str, object]]:
        query = _WORKFLOW_DEFINITION_SELECT
        parameters: list[object] = []
        if before_row_id is not None:
            query += " WHERE row_id < ?"
            parameters.append(before_row_id)
        query += " ORDER BY row_id DESC LIMIT ?"
        parameters.append(limit + 1)
        with closing(self._reader()) as connection:
            rows = connection.execute(query, parameters).fetchall()
        return _page(
            rows,
            limit,
            _workflow_summary,
            collection="workflows",
            scope=_cursor_scope(),
        )

    def _get_workflow(self, revision_id: str) -> dict[str, object]:
        with closing(self._reader()) as connection:
            row = connection.execute(
                _WORKFLOW_DEFINITION_SELECT + " WHERE revision_id = ?",
                (revision_id,),
            ).fetchone()
        if row is None:
            raise KeyError(revision_id)
        snapshot = _decode_verified_workflow_definition(row)
        return snapshot.to_record()

    def _list_sessions(
        self,
        workflow_revision_id: str | None,
        limit: int,
        before_row_id: int | None,
    ) -> Page[dict[str, object]]:
        where: list[str] = []
        parameters: list[object]
        if workflow_revision_id is None:
            query = """
                SELECT s.row_id, s.session_id, s.root_session_id,
                       s.current_invocation_id, s.invocation_count,
                       s.created_at_ns, s.updated_at_ns,
                       i.workflow_id, i.workflow_revision_id, i.status,
                       o.parent_session_id, o.parent_invocation_id,
                       o.creation_id, o.unit_index
                FROM sessions s
                LEFT JOIN invocations i
                    ON i.invocation_id = s.current_invocation_id
                LEFT JOIN session_ownership o ON o.session_id = s.session_id
            """
            parameters = []
        else:
            query = """
                SELECT s.row_id, s.session_id, s.root_session_id,
                       i.invocation_id AS current_invocation_id,
                       (
                           SELECT COUNT(*)
                           FROM invocations counted
                           WHERE counted.session_id = s.session_id
                             AND counted.workflow_revision_id = ?
                       ) AS invocation_count,
                       s.created_at_ns, i.updated_at_ns,
                       i.workflow_id, i.workflow_revision_id, i.status,
                       o.parent_session_id, o.parent_invocation_id,
                       o.creation_id, o.unit_index
                FROM sessions s
                JOIN invocations i ON i.row_id = (
                    SELECT MAX(latest.row_id)
                    FROM invocations latest
                    WHERE latest.session_id = s.session_id
                      AND latest.workflow_revision_id = ?
                )
                LEFT JOIN session_ownership o ON o.session_id = s.session_id
            """
            parameters = [workflow_revision_id, workflow_revision_id]
        if before_row_id is not None:
            where.append("s.row_id < ?")
            parameters.append(before_row_id)
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY s.row_id DESC LIMIT ?"
        parameters.append(limit + 1)
        with closing(self._reader()) as connection:
            connection.execute("BEGIN")
            try:
                rows = connection.execute(query, parameters).fetchall()
                for row in rows[:limit]:
                    session_id = _stored_string(row, "session_id")
                    canonical, expected_root = self._validate_session_projection(
                        connection,
                        session_id,
                    )
                    selected = self._validate_session_summary_row(
                        row,
                        canonical,
                        expected_root,
                        workflow_revision_id=workflow_revision_id,
                    )
                    if selected is not None:
                        invocation_row = connection.execute(
                            "SELECT * FROM invocations WHERE invocation_id = ?",
                            (selected.invocation_id,),
                        ).fetchone()
                        if invocation_row is None:
                            raise RuntimeEventStoreError(
                                "Canonical Invocation has no materialized projection."
                            )
                        self._validate_invocation_projection_row(
                            connection,
                            invocation_row,
                            selected,
                            expected_root=expected_root,
                        )
            finally:
                connection.rollback()
        return _page(
            rows,
            limit,
            _session_summary,
            collection="sessions",
            scope=_cursor_scope(workflow_revision_id),
        )

    def _list_invocations(
        self,
        session_id: str,
        workflow_revision_id: str | None,
        limit: int,
        before_row_id: int | None,
    ) -> Page[dict[str, object]]:
        parameters: list[object] = [session_id]
        query = """
            SELECT i.*, o.parent_session_id, o.parent_invocation_id,
                   o.creation_id, o.unit_index
            FROM invocations i
            LEFT JOIN session_ownership o ON o.session_id = i.session_id
            WHERE i.session_id = ?
        """
        if workflow_revision_id is not None:
            query += " AND i.workflow_revision_id = ?"
            parameters.append(workflow_revision_id)
        if before_row_id is not None:
            query += " AND i.row_id < ?"
            parameters.append(before_row_id)
        query += " ORDER BY i.row_id DESC LIMIT ?"
        parameters.append(limit + 1)
        with closing(self._reader()) as connection:
            connection.execute("BEGIN")
            try:
                rows = connection.execute(query, parameters).fetchall()
                if rows:
                    canonical = SQLiteRuntimeStore._canonical_session_projection(
                        connection,
                        session_id,
                    )
                    expected_root = self._validated_ownership_root(
                        connection,
                        session_id,
                    )
                    for row in rows[:limit]:
                        invocation_id = _stored_string(row, "invocation_id")
                        invocation = canonical.invocations.get(invocation_id)
                        if invocation is None:
                            raise RuntimeEventStoreError(
                                "Stored Invocation has no canonical Runtime logs."
                            )
                        self._validate_invocation_projection_row(
                            connection,
                            row,
                            invocation,
                            expected_root=expected_root,
                        )
            finally:
                connection.rollback()
        return _page(
            rows,
            limit,
            _invocation_summary,
            collection="invocations",
            scope=_cursor_scope(session_id, workflow_revision_id),
        )

    def _list_child_sessions(
        self,
        parent_invocation_id: str,
        limit: int,
        after: tuple[int, str, int, str] | None,
    ) -> Page[dict[str, object]]:
        parameters: list[object] = [parent_invocation_id]
        query = """
            SELECT o.session_id, o.root_session_id, o.parent_session_id,
                   o.parent_invocation_id, o.creation_id, o.unit_index,
                   o.parent_occurrence_id, o.mode,
                   o.workflow_id AS planned_workflow_id,
                   o.workflow_revision_id AS planned_workflow_revision_id,
                   o.planned_invocation_id, o.planned_event_sequence,
                   o.planned_log_id, o.change_event_sequence, o.phase,
                   s.root_session_id AS session_root_session_id,
                   s.current_invocation_id, s.invocation_count,
                   s.created_at_ns, s.updated_at_ns,
                   i.invocation_id AS indexed_invocation_id,
                   i.session_id AS invocation_session_id,
                   i.root_session_id AS invocation_root_session_id,
                   i.workflow_id, i.workflow_revision_id, i.status
            FROM session_ownership o
            LEFT JOIN sessions s ON s.session_id = o.session_id
            LEFT JOIN invocations i ON i.invocation_id = s.current_invocation_id
            WHERE o.parent_invocation_id = ?
        """
        if after is not None:
            event_sequence, log_id, unit_index, session_id = after
            query += """
                AND (
                    o.planned_event_sequence > ?
                    OR (
                        o.planned_event_sequence = ?
                        AND o.planned_log_id > ?
                    )
                    OR (
                        o.planned_event_sequence = ?
                        AND o.planned_log_id = ?
                        AND o.unit_index > ?
                    )
                    OR (
                        o.planned_event_sequence = ?
                        AND o.planned_log_id = ?
                        AND o.unit_index = ? AND o.session_id > ?
                    )
                )
            """
            parameters.extend(
                (
                    event_sequence,
                    event_sequence,
                    log_id,
                    event_sequence,
                    log_id,
                    unit_index,
                    event_sequence,
                    log_id,
                    unit_index,
                    session_id,
                )
            )
        query += """
            ORDER BY o.planned_event_sequence, o.planned_log_id,
                     o.unit_index, o.session_id
            LIMIT ?
        """
        parameters.append(limit + 1)
        with closing(self._reader()) as connection:
            connection.execute("BEGIN")
            try:
                rows = connection.execute(query, parameters).fetchall()
                ownership_cache: _ReadOwnershipCache = {}
                for row in rows[:limit]:
                    child_session_id = _stored_string(row, "session_id")
                    self._validated_ownership_root(
                        connection,
                        child_session_id,
                        ownership_cache=ownership_cache,
                    )
                    if (
                        _stored_string(row, "parent_invocation_id")
                        != parent_invocation_id
                    ):
                        raise RuntimeEventStoreError(
                            "Stored Child ownership has another canonical parent."
                        )
            finally:
                connection.rollback()
        visible = rows[:limit]
        next_cursor = None
        if len(rows) > limit and visible:
            last = visible[-1]
            next_cursor = _encode_child_cursor(
                parent_invocation_id,
                int(last["planned_event_sequence"]),
                str(last["planned_log_id"]),
                int(last["unit_index"]),
                str(last["session_id"]),
            )
        return Page(
            tuple(_validated_child_session_summary(row) for row in visible),
            next_cursor,
        )

    def _get_session(self, session_id: str) -> dict[str, object]:
        with closing(self._reader()) as connection:
            connection.execute("BEGIN")
            try:
                row = connection.execute(
                    """
                    SELECT s.row_id, s.session_id, s.root_session_id,
                           s.current_invocation_id, s.invocation_count,
                           s.created_at_ns, s.updated_at_ns,
                           i.workflow_id, i.workflow_revision_id, i.status,
                           o.parent_session_id, o.parent_invocation_id,
                           o.creation_id, o.unit_index
                    FROM sessions s
                    LEFT JOIN invocations i
                        ON i.invocation_id = s.current_invocation_id
                    LEFT JOIN session_ownership o ON o.session_id = s.session_id
                    WHERE s.session_id = ?
                    """,
                    (session_id,),
                ).fetchone()
                if row is None:
                    evidence = connection.execute(
                        "SELECT 1 FROM runtime_events WHERE session_id = ? LIMIT 1",
                        (session_id,),
                    ).fetchone()
                    if evidence is not None:
                        raise RuntimeEventStoreError(
                            "Canonical Session data has no Session projection."
                        )
                    raise KeyError(session_id)
                canonical, expected_root = self._validate_session_projection(
                    connection,
                    session_id,
                )
                selected = self._validate_session_summary_row(
                    row,
                    canonical,
                    expected_root,
                    workflow_revision_id=None,
                )
                if selected is not None:
                    invocation_row = connection.execute(
                        "SELECT * FROM invocations WHERE invocation_id = ?",
                        (selected.invocation_id,),
                    ).fetchone()
                    if invocation_row is None:
                        raise RuntimeEventStoreError(
                            "Canonical Invocation has no materialized projection."
                        )
                    self._validate_invocation_projection_row(
                        connection,
                        invocation_row,
                        selected,
                        expected_root=expected_root,
                    )
            finally:
                connection.rollback()
        return _session_summary(row)

    def _get_invocation(self, invocation_id: str) -> dict[str, object]:
        with closing(self._reader()) as connection:
            connection.execute("BEGIN")
            try:
                row = connection.execute(
                    """
                    SELECT i.*, o.parent_session_id, o.parent_invocation_id,
                           o.creation_id, o.unit_index
                    FROM invocations i
                    LEFT JOIN session_ownership o ON o.session_id = i.session_id
                    WHERE i.invocation_id = ?
                    """,
                    (invocation_id,),
                ).fetchone()
                if row is None:
                    evidence = connection.execute(
                        """
                        SELECT 1 FROM runtime_events WHERE invocation_id = ?
                        UNION ALL
                        SELECT 1 FROM trace_events WHERE invocation_id = ?
                        UNION ALL
                        SELECT 1 FROM user_events WHERE invocation_id = ?
                        LIMIT 1
                        """,
                        (invocation_id, invocation_id, invocation_id),
                    ).fetchone()
                    if evidence is not None:
                        raise RuntimeEventStoreError(
                            "Canonical Invocation data has no Invocation projection."
                        )
                    raise KeyError(invocation_id)
                self._validate_invocation_projection(
                    connection,
                    invocation_id,
                )
            finally:
                connection.rollback()
        return _invocation_summary(row)

    def _list_trace_events(
        self,
        invocation_id: str,
        after_sequence: int,
        limit: int,
    ) -> tuple[TraceEvent, ...]:
        with closing(self._reader()) as connection:
            connection.execute("BEGIN")
            try:
                invocation, latest = self._invocation_trace_head(
                    connection,
                    invocation_id,
                    allow_absent=True,
                )
                rows = connection.execute(
                    _TRACE_EVENT_SELECT
                    + """
                    WHERE invocation_id = ? AND trace_sequence > ?
                    ORDER BY trace_sequence ASC LIMIT ?
                    """,
                    (invocation_id, after_sequence, limit),
                ).fetchall()
                traces = tuple(
                    trace
                    for trace, _payload in _decode_anchored_traces(
                        connection,
                        rows,
                    )
                )
                if invocation is not None and latest > 0:
                    trace_count = _stored_integer(
                        invocation,
                        "trace_count",
                        minimum=1,
                    )
                    first = latest - trace_count + 1
                    expected = max(after_sequence + 1, first)
                    for trace in traces:
                        if trace.trace_sequence != expected:
                            raise RuntimeEventStoreError(
                                "Stored Invocation Trace sequence is incomplete."
                            )
                        expected += 1
                    if not traces and after_sequence < latest:
                        raise RuntimeEventStoreError(
                            "Stored Invocation Trace sequence is incomplete."
                        )
                reached_tail = (
                    bool(traces) and traces[-1].trace_sequence == latest
                ) or (not traces and after_sequence >= latest)
                if reached_tail and latest > 0:
                    self._validated_invocation_trace_projection(
                        connection,
                        invocation_id,
                    )
                return traces
            finally:
                connection.rollback()

    def _runtime_event_sequence_for_trace(
        self,
        invocation_id: str,
        trace_sequence: int,
    ) -> int:
        with closing(self._reader()) as connection:
            connection.execute("BEGIN")
            try:
                row = connection.execute(
                    _TRACE_EVENT_SELECT
                    + """
                    WHERE invocation_id = ? AND trace_sequence = ?
                    """,
                    (invocation_id, trace_sequence),
                ).fetchone()
                if row is None:
                    # Distinguish a missing Invocation from an invalid position.
                    invocation = connection.execute(
                        "SELECT 1 FROM invocations WHERE invocation_id = ?",
                        (invocation_id,),
                    ).fetchone()
                    if invocation is None:
                        raise KeyError(invocation_id)
                    raise ValueError(
                        "trace_sequence does not exist for this Invocation."
                    )
                _decode_anchored_trace_event(connection, row)
                source = connection.execute(
                    """
                    SELECT sequence, invocation_id FROM runtime_events
                    WHERE id = ?
                    """,
                    (_stored_string(row, "runtime_event_id"),),
                ).fetchone()
                if source is None:
                    raise RuntimeEventStoreError(
                        "Stored Trace Event has no canonical Runtime Event."
                    )
                source_invocation = _stored_string(
                    source,
                    "invocation_id",
                    optional=True,
                )
                if source_invocation != invocation_id:
                    raise RuntimeEventStoreError(
                        "Stored Trace source belongs to another Invocation."
                    )
                return _stored_integer(source, "sequence", minimum=1)
            finally:
                connection.rollback()

    def _tail_trace_events(
        self,
        invocation_id: str,
        limit: int,
        before_sequence: int | None,
    ) -> tuple[TraceEvent, ...]:
        query = _TRACE_EVENT_SELECT + " WHERE invocation_id = ?"
        parameters: list[object] = [invocation_id]
        if before_sequence is not None:
            query += " AND trace_sequence < ?"
            parameters.append(before_sequence)
        query += " ORDER BY trace_sequence DESC LIMIT ?"
        parameters.append(limit)
        with closing(self._reader()) as connection:
            connection.execute("BEGIN")
            try:
                self._validated_invocation_trace_projection(
                    connection,
                    invocation_id,
                )
                rows = connection.execute(query, parameters).fetchall()
                return tuple(
                    trace
                    for trace, _payload in _decode_anchored_traces(
                        connection,
                        tuple(reversed(rows)),
                    )
                )
            finally:
                connection.rollback()

    def _list_user_events(
        self,
        invocation_id: str,
        after_sequence: int,
        limit: int,
    ) -> tuple[UserEvent, ...]:
        with closing(self._reader()) as connection:
            connection.execute("BEGIN")
            try:
                latest = self._validated_user_event_stream(
                    connection,
                    invocation_id,
                    allow_absent=True,
                )
                rows = connection.execute(
                    _USER_EVENT_SELECT
                    + """
                    WHERE invocation_id = ? AND sequence > ?
                    ORDER BY sequence ASC LIMIT ?
                    """,
                    (invocation_id, after_sequence, limit),
                ).fetchall()
                events = tuple(_decode_verified_user_event(row) for row in rows)
                expected = after_sequence + 1
                for event in events:
                    if event.sequence != expected:
                        raise RuntimeEventStoreError(
                            "Stored Invocation User Event sequence is incomplete."
                        )
                    expected += 1
                if not events and after_sequence < latest:
                    raise RuntimeEventStoreError(
                        "Stored Invocation User Event sequence is incomplete."
                    )
                return events
            finally:
                connection.rollback()

    def _tail_user_events(
        self,
        invocation_id: str,
        limit: int,
        before_sequence: int | None,
    ) -> tuple[UserEvent, ...]:
        query = _USER_EVENT_SELECT + " WHERE invocation_id = ?"
        parameters: list[object] = [invocation_id]
        if before_sequence is not None:
            query += " AND sequence < ?"
            parameters.append(before_sequence)
        query += " ORDER BY sequence DESC LIMIT ?"
        parameters.append(limit)
        with closing(self._reader()) as connection:
            connection.execute("BEGIN")
            try:
                self._validated_user_event_stream(connection, invocation_id)
                rows = connection.execute(query, parameters).fetchall()
                return tuple(
                    _decode_verified_user_event(row)
                    for row in reversed(rows)
                )
            finally:
                connection.rollback()

    @staticmethod
    def _validated_user_event_stream(
        connection: sqlite3.Connection,
        invocation_id: str,
        *,
        allow_absent: bool = False,
    ) -> int:
        invocation = connection.execute(
            "SELECT session_id FROM invocations WHERE invocation_id = ?",
            (invocation_id,),
        ).fetchone()
        if invocation is None:
            raise KeyError(invocation_id)
        head = connection.execute(
            "SELECT * FROM user_event_streams WHERE invocation_id = ?",
            (invocation_id,),
        ).fetchone()
        aggregate = connection.execute(
            """
            SELECT COUNT(*) AS event_count, MIN(sequence) AS first_sequence,
                   MAX(sequence) AS last_sequence
            FROM user_events WHERE invocation_id = ?
            """,
            (invocation_id,),
        ).fetchone()
        assert aggregate is not None
        count = _stored_integer(aggregate, "event_count", minimum=0)
        if head is None:
            if count:
                raise RuntimeEventStoreError(
                    "Stored User Events have no stream projection."
                )
            if allow_absent:
                return 0
            return 0
        expected_count = _stored_integer(head, "event_count", minimum=1)
        latest = _stored_integer(head, "last_sequence", minimum=1)
        if (
            expected_count != count
            or count != latest
            or _stored_integer(aggregate, "first_sequence", minimum=1) != 1
            or _stored_integer(aggregate, "last_sequence", minimum=1) != latest
            or _stored_string(head, "session_id")
            != _stored_string(invocation, "session_id")
        ):
            raise RuntimeEventStoreError(
                "Stored User Event stream projection is inconsistent."
            )
        tail = connection.execute(
            _USER_EVENT_SELECT
            + " WHERE invocation_id = ? ORDER BY sequence DESC LIMIT 1",
            (invocation_id,),
        ).fetchone()
        if tail is None:
            raise RuntimeEventStoreError(
                "Stored User Event stream has no tail Event."
            )
        tail_event = _decode_verified_user_event(tail)
        if (
            tail_event.sequence != latest
            or tail_event.id != _stored_string(head, "last_event_id")
            or _stored_string(tail, "event_digest")
            != _stored_string(head, "last_event_digest")
        ):
            raise RuntimeEventStoreError(
                "Stored User Event stream head does not match its tail."
            )
        return latest

    def _latest_user_event_sequence(self, invocation_id: str) -> int:
        with closing(self._reader()) as connection:
            connection.execute("BEGIN")
            try:
                return self._validated_user_event_stream(
                    connection,
                    invocation_id,
                    allow_absent=True,
                )
            finally:
                connection.rollback()

    def _latest_user_event_sequence_hint(self, invocation_id: str) -> int:
        with closing(self._reader()) as connection:
            invocation = connection.execute(
                "SELECT 1 FROM invocations WHERE invocation_id = ?",
                (invocation_id,),
            ).fetchone()
            if invocation is None:
                raise KeyError(invocation_id)
            row = connection.execute(
                """
                SELECT last_sequence FROM user_event_streams
                WHERE invocation_id = ?
                """,
                (invocation_id,),
            ).fetchone()
            return (
                0
                if row is None
                else _stored_integer(row, "last_sequence", minimum=1)
            )

    def _latest_trace_sequence(self, invocation_id: str) -> int:
        with closing(self._reader()) as connection:
            connection.execute("BEGIN")
            try:
                _, latest, _ = self._validated_invocation_trace_projection(
                    connection,
                    invocation_id,
                )
                return latest
            finally:
                connection.rollback()

    def _latest_trace_sequence_hint(self, invocation_id: str) -> int:
        """Read an anchored head used only to wake a later validated query."""

        with closing(self._reader()) as connection:
            connection.execute("BEGIN")
            try:
                _, latest = self._invocation_trace_head(
                    connection,
                    invocation_id,
                    allow_absent=True,
                )
                return latest
            finally:
                connection.rollback()

    def _terminal_trace_status(
        self,
        invocation_id: str,
        through_sequence: int,
    ) -> tuple[str, int] | None:
        with closing(self._reader()) as connection:
            connection.execute("BEGIN")
            try:
                invocation, latest, canonical = (
                    self._validated_invocation_trace_projection(
                        connection,
                        invocation_id,
                    )
                )
                projected_status = _stored_enum(
                    invocation,
                    "status",
                    _INVOCATION_STATUSES,
                )
                if projected_status != canonical.status:
                    raise RuntimeEventStoreError(
                        "Stored Invocation status does not match canonical Runtime logs."
                    )
                terminal_status = (
                    projected_status
                    if projected_status in {"completed", "failed", "cancelled"}
                    else None
                )
                terminal_rows = connection.execute(
                    _TRACE_EVENT_SELECT
                    + """
                    WHERE invocation_id = ? AND kind IN (
                        'invocation.completed',
                        'invocation.failed',
                        'invocation.cancelled'
                    )
                    ORDER BY trace_sequence DESC LIMIT 2
                    """,
                    (invocation_id,),
                ).fetchall()
                if len(terminal_rows) > 1:
                    raise RuntimeEventStoreError(
                        "Canonical Invocation contains multiple terminal Trace Events."
                    )
                if terminal_status is None:
                    if terminal_rows:
                        raise RuntimeEventStoreError(
                            "Running Invocation has a terminal Trace Event."
                        )
                    return None
                if not terminal_rows:
                    raise RuntimeEventStoreError(
                        "Stored Invocation terminal status has no canonical Trace Event."
                    )
                terminal_trace, terminal_payload = _decode_anchored_trace(
                    connection,
                    terminal_rows[0],
                )
                expected_terminal = (
                    "completed"
                    if isinstance(terminal_payload, InvocationCompleted)
                    else "failed"
                    if isinstance(terminal_payload, InvocationFailed)
                    else "cancelled"
                    if isinstance(terminal_payload, InvocationCancelled)
                    else None
                )
                if (
                    expected_terminal is None
                    or terminal_trace.status != expected_terminal
                    or terminal_status != expected_terminal
                ):
                    raise RuntimeEventStoreError(
                        "Stored Invocation status does not match its terminal Trace Event."
                    )
                expected_root = self._validated_ownership_root(
                    connection,
                    canonical.session_id,
                )
                self._validate_child_unit_facts(
                    connection,
                    invocation_id,
                    parent_session_id=canonical.session_id,
                    expected_root=expected_root,
                    children=canonical.children,
                )
                if any(
                    child.phase != "terminal"
                    for child in canonical.children
                ):
                    return None
                if latest <= through_sequence:
                    return terminal_status, latest
                return None
            finally:
                connection.rollback()

    def _rebuild_state(
        self, session_id: str, through_sequence: int | None
    ) -> RuntimeState:
        with closing(self._reader()) as connection:
            connection.execute("BEGIN")
            try:
                return self._rebuild_state_with_connection(
                    connection,
                    session_id,
                    through_sequence,
                )
            finally:
                connection.rollback()

    def _rebuild_invocation_state(
        self,
        invocation_id: str,
        through_sequence: int | None,
    ) -> RuntimeState:
        with closing(self._reader()) as connection:
            connection.execute("BEGIN")
            try:
                canonical_rows = connection.execute(
                    """
                    SELECT session_id,
                           MIN(sequence) AS first_event_sequence,
                           MAX(sequence) AS last_event_sequence
                    FROM runtime_events
                    WHERE invocation_id = ?
                    GROUP BY session_id
                    """,
                    (invocation_id,),
                ).fetchall()
                row = connection.execute(
                    """
                    SELECT invocation_id, session_id, first_event_sequence,
                           last_event_sequence
                    FROM invocations WHERE invocation_id = ?
                    """,
                    (invocation_id,),
                ).fetchone()
                if not canonical_rows:
                    if row is None:
                        raise KeyError(invocation_id)
                    raise RuntimeEventStoreError(
                        "Stored Invocation has no canonical Runtime Events."
                    )
                if len(canonical_rows) != 1:
                    raise RuntimeEventStoreError(
                        "Canonical Invocation spans multiple Runtime Sessions."
                    )
                if row is None:
                    raise RuntimeEventStoreError(
                        "Canonical Invocation has no materialized projection."
                    )
                stored_invocation_id = _stored_string(row, "invocation_id")
                canonical = canonical_rows[0]
                session_id = _stored_string(canonical, "session_id")
                projected_session_id = _stored_string(row, "session_id")
                canonical_first = _stored_integer(
                    canonical,
                    "first_event_sequence",
                    minimum=1,
                )
                canonical_last = _stored_integer(
                    canonical,
                    "last_event_sequence",
                    minimum=1,
                )
                projected_first = _stored_integer(
                    row,
                    "first_event_sequence",
                    minimum=1,
                )
                projected_last = _stored_integer(
                    row,
                    "last_event_sequence",
                    minimum=1,
                )
                if stored_invocation_id != invocation_id:
                    raise RuntimeEventStoreError(
                        "Stored Invocation projection has inconsistent identity."
                    )
                if (
                    projected_session_id != session_id
                    or projected_first != canonical_first
                    or projected_last != canonical_last
                ):
                    raise RuntimeEventStoreError(
                        "Stored Invocation range does not match canonical Runtime Events."
                    )
                target = canonical_last if through_sequence is None else through_sequence
                if not canonical_first <= target <= canonical_last:
                    raise RuntimeEventQueryError(
                        "through_sequence must be within the canonical Invocation Event range."
                    )
                future_event = connection.execute(
                    """
                    SELECT 1 FROM runtime_events
                    WHERE session_id = ? AND sequence > ? LIMIT 1
                    """,
                    (session_id, target),
                ).fetchone()
                if future_event is None:
                    tail = self._validated_session_tail(connection, session_id)
                    if tail is None or _stored_integer(
                        tail,
                        "sequence",
                        minimum=1,
                    ) != target:
                        raise RuntimeEventStoreError(
                            "Stored Invocation does not reach the Session head."
                        )
                try:
                    state = self._rebuild_state_with_connection(
                        connection,
                        session_id,
                        target,
                    )
                except KeyError as error:
                    raise RuntimeEventStoreError(
                        "Stored Invocation refers to a missing Session Event chain."
                    ) from error
                if state.invocation is None or state.invocation.id != invocation_id:
                    raise RuntimeEventStoreError(
                        "Stored Invocation head resolves to another Invocation."
                    )
                return state
            finally:
                connection.rollback()

    def _rebuild_checkpoint(self, session_id: str) -> SessionCheckpoint:
        with closing(self._reader()) as connection:
            connection.execute("BEGIN")
            ownership_cache: _ReadOwnershipCache = {}
            try:
                state = self._rebuild_state_with_connection(
                    connection, session_id, None
                )
                if state.session is None or state.invocation is None:
                    raise RuntimeEventStoreError(
                        f"Session {session_id!r} has no recoverable Invocation."
                    )
                self._validate_state_projection(
                    connection,
                    state,
                    ownership_cache=ownership_cache,
                )
                return SessionCheckpoint.from_state(state)
            finally:
                connection.rollback()

    @staticmethod
    def _rebuild_state_with_connection(
        connection: sqlite3.Connection,
        session_id: str,
        through_sequence: int | None,
    ) -> RuntimeState:
        head: sqlite3.Row | None = None
        if through_sequence is None:
            tail = SQLiteRuntimeStore._validated_session_tail(
                connection,
                session_id,
            )
            if tail is None:
                raise KeyError(session_id)
            head = _session_head(connection, session_id)
            assert head is not None
            target_sequence = _stored_integer(
                head,
                "last_event_sequence",
                minimum=1,
            )
            assert target_sequence is not None
        else:
            target_sequence = through_sequence
        query = _RUNTIME_EVENT_SELECT + " WHERE session_id = ?"
        parameters: list[object] = [session_id]
        if target_sequence is not None:
            query += " AND sequence <= ?"
            parameters.append(target_sequence)
        query += " ORDER BY sequence ASC"
        rows = connection.execute(query, parameters).fetchall()
        if not rows:
            raise KeyError(session_id)
        events = tuple(_decode_verified_runtime_event(row) for row in rows)
        try:
            # RuntimeEvent ingestion already validates every sealed Event against
            # its semantic logs.  Recovery still verifies each stored envelope,
            # digest/header chain and operation batch, but keeps one canonical
            # record and decodes the typed RuntimeState only at this boundary.
            state = StateReducer().reduce(events)
        except Exception as error:
            raise RuntimeEventStoreError(
                "Stored Runtime Event chain cannot rebuild Runtime State."
            ) from error
        if state.sequence != target_sequence:
            raise RuntimeEventStoreError(
                "Stored Runtime Event chain does not reach the requested prefix."
            )
        if head is not None and (
            state.last_event_id != _stored_string(head, "last_event_id")
            or state.last_event_digest
            != _stored_string(head, "last_event_digest")
        ):
            raise RuntimeEventStoreError(
                "Rebuilt Runtime State does not match the stored Session head."
            )
        return state

    # ------------------------------------------------------------------
    # Thread and connection helpers

    async def _write_async(self, fn: Callable[..., _T], *args: object) -> _T:
        with self._lifecycle_lock:
            self._ensure_available()
            self._ensure_writable()
            assert self._writer is not None
            completion = self._writer.call_async(fn, *args)
        try:
            return await completion
        except (KeyError, RuntimeEventStoreError, RuntimeEventQueryError):
            raise
        except (sqlite3.Error, TypeError, ValueError, OverflowError) as error:
            raise RuntimeEventStoreError(
                "SQLite Runtime Store write failed."
            ) from error

    async def _read_async(self, fn: Callable[..., _T], *args: object) -> _T:
        self.start()
        with self._lifecycle_lock:
            self._ensure_available()
            completion = self._reader_worker.call_async(fn, *args)
        try:
            return await completion
        except (KeyError, RuntimeEventStoreError, RuntimeEventQueryError):
            raise
        except (sqlite3.Error, TypeError, ValueError, OverflowError) as error:
            raise RuntimeEventStoreError(
                "SQLite Runtime Store contains invalid persisted data."
            ) from error

    def _connection(self) -> sqlite3.Connection:
        self._ensure_writable()
        connection = self._write_connection
        if connection is None:
            raise RuntimeEventStoreError("SQLite Runtime Store is not initialized.")
        return connection

    def _reader(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            f"{self.path.as_uri()}?mode=ro",
            uri=True,
            timeout=30,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA query_only = ON")
        return connection

    def _close_connection(self) -> None:
        if self._write_connection is not None:
            self._write_connection.close()
            self._write_connection = None

    def _ensure_available(self) -> None:
        if self._closed:
            raise RuntimeEventStoreClosedError("SQLite Runtime Store is closed.")

    def _ensure_writable(self) -> None:
        if self.read_only:
            raise RuntimeEventStoreError("SQLite Runtime Store is read-only.")

    def _announce_change(self) -> None:
        with self._listener_lock:
            for writer in self._listener_writers:
                try:
                    os.write(writer, b"\0")
                except (BlockingIOError, OSError):
                    pass
            listeners = tuple(self._listeners)
        for notify in listeners:
            notify()

    async def _wait_for_change(self, timeout: float) -> None:
        if os.name != "nt":
            await self._wait_for_pipe_change(timeout)
            return
        await self._wait_for_callback_change(timeout)

    async def _wait_for_pipe_change(self, timeout: float) -> None:
        loop = asyncio.get_running_loop()
        reader, writer = os.pipe()
        os.set_blocking(reader, False)
        os.set_blocking(writer, False)
        ready = loop.create_future()

        def drain() -> None:
            try:
                while os.read(reader, 4096):
                    pass
            except (BlockingIOError, OSError):
                pass
            if not ready.done():
                ready.set_result(None)

        registered = False
        try:
            loop.add_reader(reader, drain)
            registered = True
            with self._listener_lock:
                self._listener_writers.add(writer)
            try:
                await asyncio.wait_for(ready, timeout)
            except TimeoutError:
                pass
            finally:
                with self._listener_lock:
                    self._listener_writers.discard(writer)
        finally:
            if registered:
                loop.remove_reader(reader)
            os.close(reader)
            os.close(writer)

    async def _wait_for_callback_change(self, timeout: float) -> None:
        loop = asyncio.get_running_loop()
        ready = loop.create_future()

        def resolve() -> None:
            if not ready.done():
                ready.set_result(None)

        def notify() -> None:
            try:
                loop.call_soon_threadsafe(resolve)
            except RuntimeError:
                # A cancelled subscriber may close its event loop immediately.
                pass

        with self._listener_lock:
            self._listeners.add(notify)
        try:
            try:
                await asyncio.wait_for(ready, timeout)
            except TimeoutError:
                pass
        finally:
            with self._listener_lock:
                self._listeners.discard(notify)

    def _active_listener_count(self) -> int:
        with self._listener_lock:
            return len(self._listener_writers) + len(self._listeners)


def _prepare_private_database_path(path: Path) -> None:
    """Create only missing Store resources with private POSIX defaults."""

    missing_parents: list[Path] = []
    current = path.parent
    while True:
        try:
            current.stat()
            break
        except FileNotFoundError:
            missing_parents.append(current)
        parent = current.parent
        if parent == current:
            break
        current = parent

    for directory in reversed(missing_parents):
        try:
            os.mkdir(directory, 0o700)
        except FileExistsError:
            # Another Store initializer won the race. Existing resources are
            # user-owned and must never be chmod'ed as a side effect of open.
            pass

    flags = os.O_CREAT | os.O_EXCL | os.O_RDWR
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError:
        return
    # Keep a private empty file when later schema initialization fails. It is
    # safe to retry and must not be unlinked because another process may have
    # observed it and started initialization already.
    os.close(descriptor)


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def _sqlite_tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(item["name"])
        for item in connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
            """
        ).fetchall()
    }


def _sqlite_data_version(connection: sqlite3.Connection) -> int:
    row = connection.execute("PRAGMA data_version").fetchone()
    if row is None:
        raise RuntimeEventStoreError("SQLite did not return a data version.")
    value = row[0]
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 1
        or value > _SQLITE_MAX_INTEGER
    ):
        raise RuntimeEventStoreError("SQLite returned an invalid data version.")
    return value


def _validate_existing_schema(
    connection: sqlite3.Connection,
    tables: set[str],
) -> None:
    if "schema_metadata" not in tables:
        raise RuntimeEventStoreError(
            "Existing SQLite database is not an AutoAgent Runtime Store."
        )
    try:
        row = connection.execute(
            "SELECT value FROM schema_metadata WHERE key = 'schema_version'"
        ).fetchone()
    except sqlite3.Error as error:
        raise RuntimeEventStoreError(
            "Cannot read SQLite Runtime Store schema metadata."
        ) from error
    if row is None or row["value"] != str(SQLITE_STORE_SCHEMA_VERSION):
        actual = None if row is None else row["value"]
        raise RuntimeEventStoreError(
            f"Unsupported SQLite Store schema {actual!r}."
        )
    missing = sorted(_REQUIRED_TABLES - tables)
    if missing:
        raise RuntimeEventStoreError(
            "SQLite Runtime Store is missing required tables: "
            + ", ".join(missing)
            + "."
        )
    for table, required in _REQUIRED_COLUMNS.items():
        actual = {
            str(item["name"])
            for item in connection.execute(
                f'PRAGMA table_info("{table}")'
            ).fetchall()
        }
        missing_columns = sorted(required - actual)
        if missing_columns:
            raise RuntimeEventStoreError(
                f"SQLite Runtime Store table {table!r} is missing columns: "
                + ", ".join(missing_columns)
                + "."
            )
    indexes = {
        str(item["name"])
        for item in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index'"
        ).fetchall()
    }
    missing_indexes = sorted(_REQUIRED_INDEXES - indexes)
    if missing_indexes:
        raise RuntimeEventStoreError(
            "SQLite Runtime Store is missing required indexes: "
            + ", ".join(missing_indexes)
            + "."
        )
    expected_objects = _expected_schema_objects()
    actual_objects = _schema_objects(connection)
    incompatible = sorted(
        name
        for name, signature in expected_objects.items()
        if actual_objects.get(name) != signature
    )
    unsupported_triggers = sorted(
        str(row["name"])
        for row in connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'trigger' AND tbl_name IN (
                'invocations', 'runtime_events', 'schema_metadata',
                'session_ownership', 'sessions', 'trace_events',
                'user_events', 'user_event_streams', 'workflow_definitions'
            )
            """
        ).fetchall()
    )
    incompatible.extend(unsupported_triggers)
    if incompatible:
        raise RuntimeEventStoreError(
            "SQLite Runtime Store has incompatible schema objects: "
            + ", ".join(incompatible)
            + "."
        )


@lru_cache(maxsize=1)
def _expected_schema_objects() -> dict[str, tuple[str, str, str]]:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    try:
        connection.executescript(_SCHEMA)
        return _schema_objects(connection)
    finally:
        connection.close()


@lru_cache(maxsize=1)
def _schema_statements() -> tuple[str, ...]:
    """Split the static schema so it can run inside an existing transaction."""

    statements: list[str] = []
    pending = ""
    for line in _SCHEMA.splitlines(keepends=True):
        pending += line
        if sqlite3.complete_statement(pending):
            statement = pending.strip()
            if statement:
                statements.append(statement)
            pending = ""
    if pending.strip():
        raise RuntimeError("SQLite Runtime Store schema has incomplete SQL.")
    return tuple(statements)


def _schema_objects(
    connection: sqlite3.Connection,
) -> dict[str, tuple[str, str, str]]:
    names = _REQUIRED_TABLES | _REQUIRED_INDEXES
    rows = connection.execute(
        """
        SELECT type, name, tbl_name, sql FROM sqlite_master
        WHERE type IN ('table', 'index') AND sql IS NOT NULL
        """
    ).fetchall()
    return {
        str(row["name"]): (
            str(row["type"]),
            str(row["tbl_name"]),
            " ".join(str(row["sql"]).split()).casefold(),
        )
        for row in rows
        if str(row["name"]) in names
    }


def _with_busy_retry(
    operation: Callable[[], _T],
    *,
    timeout: float = 30.0,
) -> _T:
    """Retry only SQLite lock acquisition during idempotent schema setup."""

    deadline = time.monotonic() + timeout
    delay = 0.01
    while True:
        try:
            return operation()
        except sqlite3.OperationalError as error:
            message = str(error).lower()
            if (
                ("locked" not in message and "busy" not in message)
                or time.monotonic() >= deadline
            ):
                raise
            time.sleep(delay)
            delay = min(delay * 2, 0.25)


def _json_object(value: str) -> dict[str, object]:
    if not isinstance(value, str):
        raise RuntimeEventStoreError("Stored canonical JSON must be text.")
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError) as error:
        raise RuntimeEventStoreError(
            "Stored canonical JSON is invalid."
        ) from error
    if not isinstance(decoded, dict):
        raise RuntimeEventStoreError("Stored canonical JSON is not an object.")
    return decoded


def _decode_stored_record(
    value: str,
    label: str,
    decoder: Callable[[dict[str, object]], _T],
) -> _T:
    try:
        return decoder(_json_object(value))
    except RuntimeEventStoreError:
        raise
    except Exception as error:
        raise RuntimeEventStoreError(
            f"Stored {label} record is invalid."
        ) from error


def _verify_runtime_event_digest(row: sqlite3.Row) -> str:
    record_json = row["record_json"]
    event_digest = row["event_digest"]
    if not isinstance(record_json, str) or not isinstance(event_digest, str):
        raise RuntimeEventStoreError(
            "Stored Runtime Event record and digest must be text."
        )
    actual = hashlib.sha256(record_json.encode("utf-8")).hexdigest()
    if event_digest != actual:
        raise RuntimeEventStoreError(
            "Stored Runtime Event digest does not match its canonical record."
        )
    return actual


def _decode_verified_runtime_event(row: sqlite3.Row) -> RuntimeEvent:
    """Decode one canonical Event and bind every indexed SQL field to it."""

    digest = _verify_runtime_event_digest(row)
    event = _decode_stored_record(
        row["record_json"],
        "Runtime Event",
        RuntimeEvent.from_record,
    )
    envelope = {
        "id": _stored_string(row, "id"),
        "session_id": _stored_string(row, "session_id"),
        "invocation_id": _stored_string(row, "invocation_id", optional=True),
        "sequence": _stored_integer(row, "sequence", minimum=1),
        "event_name": _stored_string(row, "event_name"),
        "occurred_at_ns": _stored_integer(row, "occurred_at_ns", minimum=0),
        "previous_event_id": _stored_string(
            row,
            "previous_event_id",
            optional=True,
        ),
        "previous_event_digest": _stored_string(
            row,
            "previous_event_digest",
            optional=True,
        ),
        "from_state_version": _stored_integer(
            row,
            "from_state_version",
            minimum=0,
            optional=True,
        ),
        "to_state_version": _stored_integer(
            row,
            "to_state_version",
            minimum=0,
            optional=True,
        ),
    }
    expected = {
        "id": event.id,
        "session_id": event.session_id,
        "invocation_id": event.invocation_id,
        "sequence": event.sequence,
        "event_name": event.event_name,
        "occurred_at_ns": event.occurred_at_ns,
        "previous_event_id": event.previous_event_id,
        "previous_event_digest": event.previous_event_digest,
        "from_state_version": event.from_state_version,
        "to_state_version": event.to_state_version,
    }
    if envelope != expected or row["event_digest"] != digest:
        raise RuntimeEventStoreError(
            "Stored Runtime Event SQL envelope does not match its canonical record."
        )
    return event


def _decode_verified_trace_event(row: sqlite3.Row) -> TraceEvent:
    """Decode one Trace projection and bind its query columns to the record."""

    event = _decode_stored_record(
        row["record_json"],
        "Trace Event",
        TraceEvent.from_record,
    )
    envelope = {
        "id": _stored_string(row, "id"),
        "session_id": _stored_string(row, "session_id"),
        "invocation_id": _stored_string(row, "invocation_id", optional=True),
        "trace_sequence": _stored_integer(row, "trace_sequence", minimum=1),
        "kind": _stored_string(row, "kind"),
        "status": _stored_string(row, "status", optional=True),
        "occurred_at_ns": _stored_integer(row, "occurred_at_ns", minimum=0),
    }
    expected = {
        "id": event.id,
        "session_id": event.session_id,
        "invocation_id": event.invocation_id,
        "trace_sequence": event.trace_sequence,
        "kind": event.kind,
        "status": event.status,
        "occurred_at_ns": event.occurred_at_ns,
    }
    if envelope != expected:
        raise RuntimeEventStoreError(
            "Stored Trace Event SQL envelope does not match its record."
        )
    return event


def _decode_verified_user_event(row: sqlite3.Row) -> UserEvent:
    """Decode one independent observation and verify its SQL envelope."""

    record_json = row["record_json"]
    event_digest = row["event_digest"]
    if not isinstance(record_json, str) or not isinstance(event_digest, str):
        raise RuntimeEventStoreError(
            "Stored User Event record and digest must be text."
        )
    actual_digest = hashlib.sha256(record_json.encode("utf-8")).hexdigest()
    if event_digest != actual_digest:
        raise RuntimeEventStoreError(
            "Stored User Event digest does not match its canonical record."
        )
    event = _decode_stored_record(
        record_json,
        "User Event",
        UserEvent.from_record,
    )
    envelope = {
        "id": _stored_string(row, "id"),
        "session_id": _stored_string(row, "session_id"),
        "invocation_id": _stored_string(row, "invocation_id"),
        "sequence": _stored_integer(row, "sequence", minimum=1),
        "kind": _stored_string(row, "kind"),
        "occurred_at_ns": _stored_integer(row, "occurred_at_ns", minimum=0),
    }
    expected = {
        "id": event.id,
        "session_id": event.session_id,
        "invocation_id": event.invocation_id,
        "sequence": event.sequence,
        "kind": event.kind,
        "occurred_at_ns": event.occurred_at_ns,
    }
    if envelope != expected:
        raise RuntimeEventStoreError(
            "Stored User Event SQL envelope does not match its record."
        )
    return event


def _decode_anchored_trace_event(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
) -> TraceEvent:
    trace, _ = _decode_anchored_traces(connection, (row,))[0]
    return trace


def _decode_anchored_trace(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
) -> tuple[TraceEvent, object]:
    """Bind a materialized Trace row to its canonical Runtime log."""

    return _decode_anchored_traces(connection, (row,))[0]


def _decode_anchored_traces(
    connection: sqlite3.Connection,
    rows: tuple[sqlite3.Row, ...] | list[sqlite3.Row],
) -> tuple[tuple[TraceEvent, object], ...]:
    """Bind a Trace page while decoding each canonical source Event once."""

    decoded = tuple(
        (
            _decode_verified_trace_event(row),
            _stored_string(row, "runtime_event_id"),
        )
        for row in rows
    )
    by_source: dict[str, list[tuple[int, TraceEvent]]] = {}
    for position, (trace, runtime_event_id) in enumerate(decoded):
        by_source.setdefault(runtime_event_id, []).append((position, trace))

    results: list[tuple[TraceEvent, object] | None] = [None] * len(decoded)
    for runtime_event_id, items in by_source.items():
        source_row = connection.execute(
            _RUNTIME_EVENT_SELECT + " WHERE id = ?",
            (runtime_event_id,),
        ).fetchone()
        if source_row is None:
            raise RuntimeEventStoreError(
                "Stored Trace Event refers to a missing Runtime Event."
            )
        source = _decode_verified_runtime_event(source_row)
        log_indexes = {log.id: index for index, log in enumerate(source.logs)}
        if len(log_indexes) != len(source.logs):
            raise RuntimeEventStoreError(
                "Canonical Runtime Event contains duplicate Log identity."
            )
        start_sequence: int | None = None
        for _position, trace in items:
            if source.session_id != trace.session_id:
                raise RuntimeEventStoreError(
                    "Stored Trace Event and Runtime Event belong to different Sessions."
                )
            index = log_indexes.get(trace.id)
            if index is None:
                raise RuntimeEventStoreError(
                    "Stored Trace Event id is absent from its canonical Runtime Event."
                )
            candidate_start = trace.trace_sequence - index
            if candidate_start < 1 or (
                start_sequence is not None
                and candidate_start != start_sequence
            ):
                raise RuntimeEventStoreError(
                    "Stored Trace Event sequence cannot identify its Runtime log."
                )
            start_sequence = candidate_start
        assert start_sequence is not None
        expected = project_trace_events(source, start_sequence=start_sequence)
        for position, trace in items:
            index = log_indexes[trace.id]
            if expected[index] != trace:
                raise RuntimeEventStoreError(
                    "Stored Trace Event does not match its canonical Runtime log."
                )
            results[position] = (trace, source.logs[index].payload)
    if any(result is None for result in results):
        raise RuntimeEventStoreError(
            "Stored Trace page could not be bound to canonical Runtime Events."
        )
    return tuple(result for result in results if result is not None)


def _decode_verified_workflow_definition(
    row: sqlite3.Row,
) -> WorkflowDefinitionSnapshot:
    """Decode a portable Workflow and bind every indexed SQL field to it."""

    snapshot = _decode_stored_record(
        row["record_json"],
        "Workflow definition",
        WorkflowDefinitionSnapshot.from_record,
    )
    envelope = {
        "workflow_revision_id": _stored_string(row, "revision_id"),
        "workflow_id": _stored_string(row, "workflow_id"),
        "workflow_version": _stored_string(row, "workflow_version"),
        "definition_hash": _stored_string(row, "definition_hash"),
    }
    expected = {
        "workflow_revision_id": snapshot.workflow_revision_id,
        "workflow_id": snapshot.workflow_id,
        "workflow_version": snapshot.workflow_version,
        "definition_hash": snapshot.definition_hash,
    }
    _stored_integer(row, "created_at_ns", minimum=0)
    if envelope != expected:
        raise RuntimeEventStoreError(
            "Stored Workflow Definition SQL envelope does not match its record."
        )
    return snapshot


def _session_head(
    connection: sqlite3.Connection,
    session_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT session_id, last_event_sequence, last_event_id, last_event_digest
        FROM sessions WHERE session_id = ?
        """,
        (session_id,),
    ).fetchone()


def _identity(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Identity cannot be empty.")
    return value


def _limit(value: int, *, maximum: int = 200) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= maximum:
        raise ValueError(f"limit must be between 1 and {maximum}.")
    return value


def _sqlite_integer(value: int, name: str, *, minimum: int) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not minimum <= value <= _SQLITE_MAX_INTEGER
    ):
        raise ValueError(
            f"{name} must be an integer between {minimum} and "
            f"{_SQLITE_MAX_INTEGER}."
        )
    return value


def _cursor_scope(*identities: str | None) -> str:
    encoded = _canonical_json({"identities": identities}).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _encode_cursor(row_id: int, *, collection: str, scope: str) -> str:
    record = _canonical_json(
        {
            "collection": collection,
            "row_id": row_id,
            "scope": scope,
            "version": _PAGE_CURSOR_VERSION,
        }
    )
    return base64.urlsafe_b64encode(record.encode()).decode().rstrip("=")


def _decode_cursor(
    value: str | None,
    *,
    collection: str,
    scope: str,
) -> int | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError("cursor must be a non-empty string or None.")
    try:
        padded = value + "=" * (-len(value) % 4)
        decoded = base64.b64decode(
            padded.encode("ascii"),
            altchars=b"-_",
            validate=True,
        )
        record = _cursor_object(decoded.decode("utf-8"))
    except (binascii.Error, TypeError, ValueError, UnicodeError) as error:
        raise ValueError("cursor is invalid.") from error
    if (
        set(record) != {"collection", "row_id", "scope", "version"}
        or record["version"] != _PAGE_CURSOR_VERSION
        or record["collection"] != collection
        or record["scope"] != scope
        or not isinstance(record["row_id"], int)
        or isinstance(record["row_id"], bool)
        or record["row_id"] < 1
        or record["row_id"] > _SQLITE_MAX_INTEGER
    ):
        raise ValueError("cursor is invalid.")
    return int(record["row_id"])


def _encode_child_cursor(
    parent_invocation_id: str,
    event_sequence: int,
    log_id: str,
    unit_index: int,
    session_id: str,
) -> str:
    record = _canonical_json(
        {
            "collection": "child_sessions",
            "event_sequence": event_sequence,
            "log_id": log_id,
            "parent_scope": _cursor_scope(parent_invocation_id),
            "session_id": session_id,
            "unit_index": unit_index,
            "version": _PAGE_CURSOR_VERSION,
        }
    )
    return base64.urlsafe_b64encode(record.encode()).decode().rstrip("=")


def _decode_child_cursor(
    value: str | None,
    parent_invocation_id: str,
) -> tuple[int, str, int, str] | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError("cursor must be a non-empty string or None.")
    try:
        padded = value + "=" * (-len(value) % 4)
        decoded = base64.b64decode(
            padded.encode("ascii"),
            altchars=b"-_",
            validate=True,
        )
        record = _cursor_object(decoded.decode("utf-8"))
    except (binascii.Error, UnicodeError, ValueError, TypeError) as error:
        raise ValueError("cursor is invalid.") from error
    if set(record) != {
        "collection",
        "event_sequence",
        "log_id",
        "parent_scope",
        "session_id",
        "unit_index",
        "version",
    }:
        raise ValueError("cursor is invalid.")
    event_sequence = record["event_sequence"]
    log_id = record["log_id"]
    session_id = record["session_id"]
    unit_index = record["unit_index"]
    if (
        record["collection"] != "child_sessions"
        or record["parent_scope"] != _cursor_scope(parent_invocation_id)
        or record["version"] != _PAGE_CURSOR_VERSION
        or not isinstance(event_sequence, int)
        or isinstance(event_sequence, bool)
        or event_sequence < 1
        or event_sequence > _SQLITE_MAX_INTEGER
        or not isinstance(log_id, str)
        or not log_id
        or not isinstance(session_id, str)
        or not session_id
        or not isinstance(unit_index, int)
        or isinstance(unit_index, bool)
        or unit_index < 0
        or unit_index > _SQLITE_MAX_INTEGER
    ):
        raise ValueError("cursor is invalid.")
    return event_sequence, log_id, unit_index, session_id


def _cursor_object(value: str) -> dict[str, object]:
    decoded = json.loads(value)
    if not isinstance(decoded, dict):
        raise ValueError("cursor is invalid.")
    return decoded


def _page(
    rows: list[sqlite3.Row],
    limit: int,
    projector: Callable[[sqlite3.Row], dict[str, object]],
    *,
    collection: str,
    scope: str,
) -> Page[dict[str, object]]:
    visible = rows[:limit]
    next_cursor = (
        _encode_cursor(
            int(visible[-1]["row_id"]),
            collection=collection,
            scope=scope,
        )
        if len(rows) > limit and visible
        else None
    )
    return Page(tuple(projector(row) for row in visible), next_cursor)


def _workflow_summary(row: sqlite3.Row) -> dict[str, object]:
    snapshot = _decode_verified_workflow_definition(row)
    return {
        "workflow_id": snapshot.workflow_id,
        "workflow_version": snapshot.workflow_version,
        "workflow_revision_id": snapshot.workflow_revision_id,
        "definition_hash": snapshot.definition_hash,
        "created_at_ns": _stored_integer(row, "created_at_ns"),
    }


def _session_summary(row: sqlite3.Row) -> dict[str, object]:
    return {
        "session_id": _stored_string(row, "session_id"),
        "root_session_id": _stored_string(row, "root_session_id"),
        "current_invocation_id": _stored_string(
            row, "current_invocation_id", optional=True
        ),
        "invocation_count": _stored_integer(row, "invocation_count"),
        "workflow_id": _stored_string(row, "workflow_id", optional=True),
        "workflow_revision_id": _stored_string(
            row, "workflow_revision_id", optional=True
        ),
        "status": _stored_enum(
            row, "status", _INVOCATION_STATUSES, optional=True
        ),
        "parent_session_id": _stored_string(
            row, "parent_session_id", optional=True
        ),
        "parent_invocation_id": _stored_string(
            row, "parent_invocation_id", optional=True
        ),
        "creation_id": _stored_string(row, "creation_id", optional=True),
        "unit_index": _stored_integer(row, "unit_index", optional=True),
        "created_at_ns": _stored_integer(row, "created_at_ns"),
        "updated_at_ns": _stored_integer(row, "updated_at_ns"),
    }


def _invocation_summary(row: sqlite3.Row) -> dict[str, object]:
    return {
        "invocation_id": _stored_string(row, "invocation_id"),
        "session_id": _stored_string(row, "session_id"),
        "root_session_id": _stored_string(row, "root_session_id"),
        "workflow_id": _stored_string(row, "workflow_id"),
        "workflow_revision_id": _stored_string(row, "workflow_revision_id"),
        "entry_node_id": _stored_string(row, "entry_node_id"),
        "status": _stored_enum(row, "status", _INVOCATION_STATUSES),
        "parent_session_id": _stored_string(
            row, "parent_session_id", optional=True
        ),
        "parent_invocation_id": _stored_string(
            row, "parent_invocation_id", optional=True
        ),
        "creation_id": _stored_string(row, "creation_id", optional=True),
        "unit_index": _stored_integer(row, "unit_index", optional=True),
        "first_event_sequence": _stored_integer(
            row, "first_event_sequence", minimum=1
        ),
        "last_event_sequence": _stored_integer(
            row, "last_event_sequence", minimum=1
        ),
        "created_at_ns": _stored_integer(row, "created_at_ns"),
        "updated_at_ns": _stored_integer(row, "updated_at_ns"),
        "ended_at_ns": _stored_integer(row, "ended_at_ns", optional=True),
    }


def _child_session_summary(row: sqlite3.Row) -> dict[str, object]:
    status = _stored_enum(
        row, "status", _INVOCATION_STATUSES, optional=True
    )
    return {
        "session_id": _stored_string(row, "session_id"),
        "root_session_id": _stored_string(row, "root_session_id"),
        "parent_session_id": _stored_string(row, "parent_session_id"),
        "parent_invocation_id": _stored_string(row, "parent_invocation_id"),
        "creation_id": _stored_string(row, "creation_id"),
        "unit_index": _stored_integer(row, "unit_index"),
        "parent_occurrence_id": _stored_string(row, "parent_occurrence_id"),
        "mode": _stored_enum(row, "mode", frozenset({"await", "spawn"})),
        "planned_workflow_id": _stored_string(row, "planned_workflow_id"),
        "planned_workflow_revision_id": _stored_string(
            row, "planned_workflow_revision_id"
        ),
        "planned_invocation_id": _stored_string(row, "planned_invocation_id"),
        "planned_event_sequence": _stored_integer(
            row, "planned_event_sequence", minimum=1
        ),
        "change_event_sequence": _stored_integer(
            row, "change_event_sequence", minimum=1
        ),
        "phase": _stored_enum(row, "phase", _CHILD_PHASES),
        "current_invocation_id": _stored_string(
            row, "current_invocation_id", optional=True
        ),
        "invocation_count": _stored_integer(
            row, "invocation_count", optional=True
        ) or 0,
        "workflow_id": _stored_string(row, "workflow_id", optional=True),
        "workflow_revision_id": _stored_string(
            row, "workflow_revision_id", optional=True
        ),
        "status": status or "planned",
        "created_at_ns": _stored_integer(row, "created_at_ns", optional=True),
        "updated_at_ns": _stored_integer(row, "updated_at_ns", optional=True),
    }


def _validated_child_session_summary(
    row: sqlite3.Row,
) -> dict[str, object]:
    """Reject a Child lifecycle projection that lost its opened Runtime rows."""

    phase = _stored_enum(row, "phase", _CHILD_PHASES)
    session_id = _stored_string(row, "session_id")
    root_session_id = _stored_string(row, "root_session_id")
    planned_invocation_id = _stored_string(row, "planned_invocation_id")
    planned_workflow_id = _stored_string(row, "planned_workflow_id")
    planned_revision_id = _stored_string(
        row,
        "planned_workflow_revision_id",
    )
    session_root = _stored_string(
        row,
        "session_root_session_id",
        optional=True,
    )
    current_invocation_id = _stored_string(
        row,
        "current_invocation_id",
        optional=True,
    )
    indexed_invocation_id = _stored_string(
        row,
        "indexed_invocation_id",
        optional=True,
    )
    if session_root is None:
        if current_invocation_id is not None or phase != "planned":
            raise RuntimeEventStoreError(
                "Opened Child ownership has no Session projection."
            )
        return _child_session_summary(row)
    invocation_count = _stored_integer(
        row,
        "invocation_count",
        optional=True,
    )
    status = _stored_enum(
        row,
        "status",
        _INVOCATION_STATUSES,
        optional=True,
    )
    if (
        invocation_count is None
        or invocation_count < 1
        or current_invocation_id != planned_invocation_id
        or indexed_invocation_id != current_invocation_id
        or _stored_string(row, "invocation_session_id", optional=True)
        != session_id
        or session_root != root_session_id
        or _stored_string(row, "invocation_root_session_id", optional=True)
        != root_session_id
        or _stored_string(row, "workflow_id", optional=True)
        != planned_workflow_id
        or _stored_string(row, "workflow_revision_id", optional=True)
        != planned_revision_id
        or status is None
        or (phase == "terminal" and status not in {"completed", "failed", "cancelled"})
    ):
        raise RuntimeEventStoreError(
            "Stored Child Session lifecycle does not match its canonical plan."
        )
    return _child_session_summary(row)


def _stored_string(
    row: sqlite3.Row,
    key: str,
    *,
    optional: bool = False,
) -> str | None:
    value = row[key]
    if value is None and optional:
        return None
    if not isinstance(value, str) or not value:
        raise RuntimeEventStoreError(
            f"Stored projection field {key!r} must be non-empty text."
        )
    return value


def _stored_integer(
    row: sqlite3.Row,
    key: str,
    *,
    optional: bool = False,
    minimum: int = 0,
) -> int | None:
    value = row[key]
    if value is None and optional:
        return None
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < minimum
        or value > _SQLITE_MAX_INTEGER
    ):
        raise RuntimeEventStoreError(
            f"Stored projection field {key!r} must be a valid integer."
        )
    return value


def _stored_enum(
    row: sqlite3.Row,
    key: str,
    allowed: frozenset[str],
    *,
    optional: bool = False,
) -> str | None:
    value = _stored_string(row, key, optional=optional)
    if value is not None and value not in allowed:
        raise RuntimeEventStoreError(
            f"Stored projection field {key!r} has an unsupported value."
        )
    return value


_SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS workflow_definitions (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    revision_id TEXT NOT NULL UNIQUE,
    workflow_id TEXT NOT NULL,
    workflow_version TEXT NOT NULL,
    definition_hash TEXT NOT NULL,
    created_at_ns INTEGER NOT NULL,
    record_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS workflow_definitions_workflow
    ON workflow_definitions(workflow_id, row_id DESC);

CREATE TABLE IF NOT EXISTS runtime_events (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    session_id TEXT NOT NULL,
    invocation_id TEXT,
    sequence INTEGER NOT NULL,
    event_name TEXT NOT NULL,
    occurred_at_ns INTEGER NOT NULL,
    previous_event_id TEXT,
    previous_event_digest TEXT,
    from_state_version INTEGER,
    to_state_version INTEGER,
    event_digest TEXT NOT NULL,
    record_json TEXT NOT NULL,
    UNIQUE(session_id, sequence)
);
CREATE INDEX IF NOT EXISTS runtime_events_invocation
    ON runtime_events(invocation_id, sequence);
CREATE INDEX IF NOT EXISTS runtime_events_session_time
    ON runtime_events(session_id, occurred_at_ns);

CREATE TABLE IF NOT EXISTS trace_events (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    runtime_event_id TEXT NOT NULL REFERENCES runtime_events(id) ON DELETE CASCADE,
    session_id TEXT NOT NULL,
    invocation_id TEXT,
    trace_sequence INTEGER NOT NULL,
    kind TEXT NOT NULL,
    status TEXT,
    occurred_at_ns INTEGER NOT NULL,
    record_json TEXT NOT NULL,
    UNIQUE(session_id, trace_sequence)
);
CREATE INDEX IF NOT EXISTS trace_events_invocation
    ON trace_events(invocation_id, trace_sequence);
CREATE INDEX IF NOT EXISTS trace_events_invocation_kind
    ON trace_events(invocation_id, kind, trace_sequence DESC);

CREATE TABLE IF NOT EXISTS user_event_streams (
    invocation_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    event_count INTEGER NOT NULL,
    last_sequence INTEGER NOT NULL,
    last_event_id TEXT NOT NULL,
    last_event_digest TEXT NOT NULL,
    updated_at_ns INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS user_events (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    session_id TEXT NOT NULL,
    invocation_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    kind TEXT NOT NULL,
    occurred_at_ns INTEGER NOT NULL,
    event_digest TEXT NOT NULL,
    record_json TEXT NOT NULL,
    UNIQUE(invocation_id, sequence)
);
CREATE INDEX IF NOT EXISTS user_events_invocation
    ON user_events(invocation_id, sequence);
CREATE INDEX IF NOT EXISTS user_events_session_time
    ON user_events(session_id, occurred_at_ns);

CREATE TABLE IF NOT EXISTS session_ownership (
    session_id TEXT PRIMARY KEY,
    root_session_id TEXT NOT NULL,
    parent_session_id TEXT,
    parent_invocation_id TEXT,
    creation_id TEXT,
    unit_index INTEGER,
    parent_occurrence_id TEXT,
    mode TEXT,
    workflow_id TEXT,
    workflow_revision_id TEXT,
    planned_invocation_id TEXT,
    planned_event_sequence INTEGER,
    planned_log_id TEXT,
    change_event_sequence INTEGER,
    phase TEXT,
    updated_at_ns INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS session_ownership_root
    ON session_ownership(root_session_id);
CREATE UNIQUE INDEX IF NOT EXISTS session_ownership_parent_unit
    ON session_ownership(
        parent_invocation_id, planned_event_sequence, planned_log_id, unit_index
    )
    WHERE parent_invocation_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS session_ownership_parent_change
    ON session_ownership(
        parent_invocation_id, change_event_sequence, session_id
    )
    WHERE parent_invocation_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS sessions (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL UNIQUE,
    root_session_id TEXT NOT NULL,
    current_invocation_id TEXT,
    invocation_count INTEGER NOT NULL,
    last_event_sequence INTEGER NOT NULL,
    last_event_id TEXT NOT NULL,
    last_event_digest TEXT NOT NULL,
    trace_count INTEGER NOT NULL,
    last_trace_sequence INTEGER NOT NULL,
    last_trace_id TEXT,
    last_trace_digest TEXT,
    created_at_ns INTEGER NOT NULL,
    updated_at_ns INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS sessions_root ON sessions(root_session_id);

CREATE TABLE IF NOT EXISTS invocations (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    invocation_id TEXT NOT NULL UNIQUE,
    session_id TEXT NOT NULL,
    root_session_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    workflow_revision_id TEXT NOT NULL,
    entry_node_id TEXT NOT NULL,
    status TEXT NOT NULL,
    first_event_sequence INTEGER NOT NULL,
    last_event_sequence INTEGER NOT NULL,
    trace_count INTEGER NOT NULL,
    last_trace_sequence INTEGER NOT NULL,
    created_at_ns INTEGER NOT NULL,
    updated_at_ns INTEGER NOT NULL,
    ended_at_ns INTEGER
);
CREATE INDEX IF NOT EXISTS invocations_session
    ON invocations(session_id, row_id DESC);
CREATE INDEX IF NOT EXISTS invocations_workflow
    ON invocations(workflow_revision_id, row_id DESC);
"""


__all__ = ["SQLITE_STORE_SCHEMA_VERSION", "SQLiteRuntimeStore"]

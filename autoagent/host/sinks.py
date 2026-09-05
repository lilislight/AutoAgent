"""Create the Runtime Event sink selected by immutable Host settings."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from autoagent.core.compiler import WorkflowDefinitionSnapshot
from autoagent.core.runtime import RuntimeCheckpointBundle, RuntimeEvent, UserEvent
from autoagent.hosting import HttpRuntimeEventSink, SQLiteRuntimeStore

from .settings import HostSettings


@runtime_checkable
class HostRuntimeEventSink(Protocol):
    """Durable Runtime and User Event sink owned and closed by one Host."""

    async def append(self, event: RuntimeEvent) -> None: ...

    async def append_user_event(self, event: UserEvent) -> None: ...

    def close(self) -> None: ...


@runtime_checkable
class WorkflowDefinitionSink(Protocol):
    """Optional sink capability for portable Workflow definitions."""

    def save_workflow(self, snapshot: WorkflowDefinitionSnapshot) -> None: ...


@runtime_checkable
class RecoverySource(Protocol):
    """Optional sink capability used to reconstruct one Root Runtime graph."""

    async def rebuild_checkpoint(
        self, root_session_id: str
    ) -> RuntimeCheckpointBundle: ...


def create_runtime_event_sink(
    settings: HostSettings,
) -> HostRuntimeEventSink | None:
    """Construct and, where applicable, start the configured sink."""

    if not isinstance(settings, HostSettings):
        raise TypeError("settings must be HostSettings.")
    if settings.runtime_event_sink == "none":
        return None
    if settings.runtime_event_sink == "http":
        assert settings.http_sink_url is not None
        assert settings.http_user_event_sink_url is not None
        return HttpRuntimeEventSink(
            settings.http_sink_url,
            user_event_url=settings.http_user_event_sink_url,
            token=settings.http_sink_token,
            timeout_seconds=settings.http_sink_timeout_seconds,
        )
    store = SQLiteRuntimeStore(
        settings.sqlite_path,
        refresh_seconds=settings.trace_refresh_seconds,
    )
    try:
        store.start()
    except BaseException as error:
        try:
            store.close()
        except BaseException as cleanup_error:
            error.add_note(f"SQLite Runtime Store cleanup failed: {cleanup_error}")
        raise
    return store


__all__ = [
    "HostRuntimeEventSink",
    "RecoverySource",
    "WorkflowDefinitionSink",
    "create_runtime_event_sink",
]

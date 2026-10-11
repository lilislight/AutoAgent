"""Storage-independent interface consumed by Host."""

from __future__ import annotations

from typing import Protocol

from autoagent.core.compiler.snapshot import WorkflowDefinitionSnapshot
from autoagent.core.runtime.events import RuntimeEvent
from autoagent.core.runtime.graph_checkpoint import RuntimeGraphCheckpoint
from autoagent.core.runtime.user_events import UserEvent

from .models import OperatorDefinition, Page, PersistenceHealth, ReadView


class Persistence(Protocol):
    async def execution_snapshot(self, invocation_id: str, *, view: ReadView = "accepted") -> dict | None: ...
    async def activation_candidates(self, now_us: int, *, limit: int = 32, cursor: str | None = None) -> Page: ...
    async def register_workflow(self, snapshot: WorkflowDefinitionSnapshot) -> str: ...
    async def register_operator(self, definition: OperatorDefinition) -> str: ...
    async def get_workflow(self, revision_id: str) -> dict | None: ...
    async def get_operator(self, definition_id: str) -> dict | None: ...
    async def list_workflows(
        self,
        *,
        workflow_id: str | None = None,
        limit: int = 100,
        cursor: tuple | None = None,
    ) -> Page: ...
    async def list_operators(
        self,
        *,
        operator_id: str | None = None,
        limit: int = 100,
        cursor: tuple | None = None,
    ) -> Page: ...
    async def append(self, event: RuntimeEvent) -> None: ...
    async def append_runtime_event(
        self, event: RuntimeEvent, *, operator_definition_id: str | None = None
    ) -> None: ...
    async def append_user_event(self, event: UserEvent) -> None: ...
    async def get_runtime_event(self, event_id: str) -> RuntimeEvent | None: ...
    async def get_user_event(self, event_id: str) -> UserEvent | None: ...
    async def runtime_events(
        self,
        session_id: str,
        *,
        after: int = 0,
        limit: int = 100,
        view: ReadView = "durable",
        invocation_id: str | None = None,
    ) -> Page: ...
    async def user_events(
        self,
        invocation_id: str,
        *,
        after: int = 0,
        occurrence_id: str | None = None,
        limit: int = 100,
        view: ReadView = "durable",
    ) -> Page: ...
    async def save_checkpoint(self, checkpoint: RuntimeGraphCheckpoint) -> str: ...
    async def import_checkpoint(self, checkpoint: RuntimeGraphCheckpoint) -> str: ...
    async def get_checkpoint(
        self, checkpoint_id: str
    ) -> RuntimeGraphCheckpoint | None: ...
    async def list_checkpoints(
        self, root_session_id: str, *, limit: int = 100, cursor: tuple | None = None
    ) -> Page: ...
    async def list_sessions(
        self,
        *,
        limit: int = 100,
        cursor: tuple | None = None,
        view: ReadView = "durable",
    ) -> Page: ...
    async def get_session(
        self, session_id: str, *, view: ReadView = "durable"
    ) -> dict | None: ...
    async def list_invocations(
        self,
        *,
        workflow_revision_id: str | None = None,
        session_id: str | None = None,
        root_invocation_id: str | None = None,
        status: str | None = None,
        include_planned: bool = False,
        limit: int = 100,
        cursor: tuple | None = None,
        view: ReadView = "durable",
    ) -> Page: ...
    async def get_invocation(
        self, invocation_id: str, *, view: ReadView = "durable"
    ) -> dict | None: ...
    async def graph_changes(
        self,
        invocation_id: str,
        *,
        after_sequence: int = 0,
        limit: int = 100,
        root_graph: bool = False,
        view: ReadView = "durable",
    ) -> Page: ...
    async def list_nodes(
        self,
        invocation_id: str,
        *,
        limit: int = 100,
        cursor: tuple | None = None,
        view: ReadView = "durable",
    ) -> Page: ...
    async def list_calls(
        self,
        invocation_id: str,
        *,
        occurrence_id: str | None = None,
        limit: int = 100,
        cursor: tuple | None = None,
        view: ReadView = "durable",
    ) -> Page: ...
    async def list_waits(
        self,
        invocation_id: str,
        *,
        limit: int = 100,
        cursor: tuple | None = None,
        view: ReadView = "durable",
    ) -> Page: ...
    async def node_detail(
        self, invocation_id: str, occurrence_id: str, *, view: ReadView = "durable"
    ) -> dict | None: ...
    async def call_detail(
        self, invocation_id: str, call_id: str, *, view: ReadView = "durable"
    ) -> dict | None: ...
    async def wait_detail(
        self, invocation_id: str, wait_id: str, *, view: ReadView = "durable"
    ) -> dict | None: ...
    async def edge_detail(
        self,
        invocation_id: str,
        edge_id: str,
        *,
        limit: int = 100,
        evaluations_cursor: tuple | None = None,
        changes_cursor: tuple | None = None,
        view: ReadView = "durable",
    ) -> dict: ...
    async def statistics(
        self, entity: str, *, workflow_revision_id: str | None = None
    ) -> tuple[dict, ...]: ...
    async def resolve(self, reference: dict | None) -> dict: ...
    async def flush(self) -> None: ...
    async def read_recovery_bundle(self, root_invocation_id: str) -> dict: ...
    async def health(self) -> PersistenceHealth: ...
    async def close(self) -> None: ...
    async def delete_checkpoint(self, checkpoint_id: str) -> None: ...
    async def delete_workflow(self, revision_id: str) -> None: ...
    async def delete_operator(self, definition_id: str) -> None: ...
    async def purge_root_graph(self, root_invocation_id: str) -> None: ...
    async def rebuild_projections(self) -> None: ...
    async def gc_artifacts(self) -> int: ...

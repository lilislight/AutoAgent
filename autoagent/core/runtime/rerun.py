from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from autoagent.core.runtime.context import SessionContext
from autoagent.core.runtime.event import RuntimeEventMode


@dataclass(frozen=True, slots=True)
class InvocationRerunSeed:
    """Private executable values captured at an Invocation's start boundary."""

    invocation_id: UUID
    workflow_id: str
    workflow_revision_id: str
    entry_node_id: str
    state: str
    event_mode: RuntimeEventMode
    input: dict[str, Any]
    session_context: SessionContext | None


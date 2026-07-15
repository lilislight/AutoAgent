from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from autoagent.runtime.context import SessionContext
from autoagent.runtime.execution import _parse_datetime, utc_now
from autoagent.runtime.invocation import Invocation


class Session:
    """Long-lived workflow session with shared context and invocation history.

    Session is owned by RuntimeStore/AutoAgentApp, not by end users directly.
    Identity is `(namespace, workflow_id, session_key)` at store level; `id` is
    the internal UUID persisted by the store. A session keeps its Invocation
    objects as a list so an in-memory runtime has the same tree shape that a UI
    or database snapshot needs to reconstruct later.

    Mutable execution progress belongs to Invocation/NodeExecution, not here.
    The only execution pointer stored on Session is current_invocation_id, which
    helps APIs find the latest invocation without scanning UI-facing history.
    """

    def __init__(
        self,
        workflow_id: str,
        session_key: str | None = None,
        namespace: str = "default",
        *,
        id: UUID | None = None,
        context: SessionContext | None = None,
        invocations: list[Invocation] | None = None,
        current_invocation_id: UUID | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
    ) -> None:
        self.id = id or uuid4()
        self.namespace = namespace
        self.workflow_id = workflow_id
        self.session_key = session_key
        self.context = context or SessionContext()
        self.invocations: list[Invocation] = list(invocations or [])
        self.current_invocation_id = current_invocation_id
        self.created_at = created_at or utc_now()
        self.updated_at = updated_at or self.created_at

    def add_invocation(self, invocation: Invocation) -> None:
        """Attach an invocation to this session and mark it current.

        AutoAgentApp or RuntimeStore calls this after creating a new invocation
        or restoring one from persistence. It rejects workflow mismatches because
        a session is scoped to exactly one workflow id.
        """

        if invocation.workflow_id != self.workflow_id:
            raise ValueError("Invocation workflow_id does not match this session.")
        if not any(existing.id == invocation.id for existing in self.invocations):
            self.invocations.append(invocation)
        self.current_invocation_id = invocation.id
        self.updated_at = utc_now()

    def list_invocations(self) -> tuple[Invocation, ...]:
        return tuple(self.invocations)

    def get_invocation(self, invocation_id: UUID) -> Invocation | None:
        for invocation in self.invocations:
            if invocation.id == invocation_id:
                return invocation
        return None

    def get_current_invocation(self) -> Invocation | None:
        if self.current_invocation_id is None:
            return None
        return self.get_invocation(self.current_invocation_id)

    def to_record(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "namespace": self.namespace,
            "workflow_id": self.workflow_id,
            "session_key": self.session_key,
            "context": self.context.to_record(),
            "current_invocation_id": (
                str(self.current_invocation_id)
                if self.current_invocation_id is not None
                else None
            ),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_record(
        cls,
        record: Mapping[str, Any],
        *,
        invocations: list[Invocation] | None = None,
    ) -> Session:
        current_invocation_id = record.get("current_invocation_id")
        return cls(
            id=UUID(str(record["id"])),
            namespace=str(record["namespace"]),
            workflow_id=str(record["workflow_id"]),
            session_key=record.get("session_key"),
            context=SessionContext.from_record(record.get("context", {})),
            invocations=list(invocations or []),
            current_invocation_id=(
                UUID(str(current_invocation_id))
                if current_invocation_id is not None
                else None
            ),
            created_at=_parse_datetime(record.get("created_at")),
            updated_at=_parse_datetime(record.get("updated_at")),
        )

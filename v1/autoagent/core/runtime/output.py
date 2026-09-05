from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from autoagent.core.runtime.execution import NodeExecution
from autoagent.core.runtime.scheduler import ExecutionScope

_MISSING = object()


@dataclass(frozen=True)
class NodeOutput:
    """One copied output returned by ``OutputView.timeline``."""

    node_execution_id: UUID
    node_id: str
    sequence: int
    execution_scope: ExecutionScope
    value: Any


@dataclass(frozen=True)
class _OutputEntry:
    node_execution_id: UUID
    node_id: str
    sequence: int
    execution_scope: ExecutionScope
    value: Any


class OutputIndex:
    """Invocation-owned index of completed logical node outputs.

    Entries reference the authoritative ``NodeExecution.output`` value. User
    hooks never receive these references directly: ``OutputView`` deep-copies
    only values that a hook actually reads.
    """

    def __init__(self, executions: list[NodeExecution] | None = None) -> None:
        self._timeline: list[_OutputEntry] = []
        self._by_node_id: dict[str, list[_OutputEntry]] = {}
        for execution in sorted(executions or (), key=lambda item: item.sequence):
            if execution.state == "completed":
                self.add(execution)

    def add(self, execution: NodeExecution) -> None:
        entry = _OutputEntry(
            node_execution_id=execution.id,
            node_id=execution.node_id,
            sequence=execution.sequence,
            execution_scope=execution.execution_scope,
            value=execution.output,
        )
        self._timeline.append(entry)
        self._by_node_id.setdefault(entry.node_id, []).append(entry)

    def view(
        self,
        *,
        node_id_aliases: dict[str, str] | None = None,
    ) -> OutputView:
        through_sequence = self._timeline[-1].sequence if self._timeline else 0
        return OutputView(
            self,
            through_sequence=through_sequence,
            node_id_aliases=node_id_aliases,
        )


class OutputView:
    """Lazy, isolated view of completed logical node outputs.

    Creating a view is constant-size and does not copy historical output
    values. ``latest``, ``all`` and ``timeline`` copy only values returned to
    the user hook, so hook mutation cannot alter runtime-owned state.
    """

    def __init__(
        self,
        index: OutputIndex,
        *,
        through_sequence: int,
        node_id_aliases: dict[str, str] | None = None,
    ) -> None:
        self._index = index
        self._through_sequence = through_sequence
        self._node_id_aliases = dict(node_id_aliases or {})

    def has(self, node_id: str) -> bool:
        return self._latest_entry(node_id) is not None

    def latest(self, node_id: str, default: Any = _MISSING) -> Any:
        entry = self._latest_entry(node_id)
        if entry is not None:
            return deepcopy(entry.value)
        if default is not _MISSING:
            return deepcopy(default)
        raise KeyError(f"No output found for node: {node_id}")

    def all(self, node_id: str) -> list[Any]:
        resolved = self._resolve_node_id(node_id)
        return [
            deepcopy(entry.value)
            for entry in self._index._by_node_id.get(resolved, ())
            if entry.sequence <= self._through_sequence
        ]

    def timeline(self, node_ids: list[str] | tuple[str, ...] | None = None) -> list[NodeOutput]:
        allowed = (
            None
            if node_ids is None
            else {self._resolve_node_id(node_id) for node_id in node_ids}
        )
        return [
            NodeOutput(
                node_execution_id=entry.node_execution_id,
                node_id=entry.node_id,
                sequence=entry.sequence,
                execution_scope=entry.execution_scope,
                value=deepcopy(entry.value),
            )
            for entry in self._index._timeline
            if entry.sequence <= self._through_sequence
            and (allowed is None or entry.node_id in allowed)
        ]

    def scoped(self, node_id_aliases: dict[str, str]) -> OutputView:
        """Return a view resolving local child Workflow ids to expanded ids."""

        if not node_id_aliases:
            return self
        aliases = dict(self._node_id_aliases)
        aliases.update(node_id_aliases)
        return OutputView(
            self._index,
            through_sequence=self._through_sequence,
            node_id_aliases=aliases,
        )

    def _resolve_node_id(self, node_id: str) -> str:
        return self._node_id_aliases.get(node_id, node_id)

    def _latest_entry(self, node_id: str) -> _OutputEntry | None:
        resolved = self._resolve_node_id(node_id)
        for entry in reversed(self._index._by_node_id.get(resolved, ())):
            if entry.sequence <= self._through_sequence:
                return entry
        return None

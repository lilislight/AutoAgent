from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from autoagent.runtime.execution import NodeExecution
from autoagent.runtime.readonly import to_read_only

_MISSING = object()


@dataclass(frozen=True)
class NodeOutput:
    """Output produced by one completed NodeExecution.

    This is a read model, not a mutable runtime record. OutputContext rebuilds
    it from Invocation.node_executions whenever input_mapping/condition/output
    binding needs a stable view of completed outputs.
    """

    node_execution_id: UUID
    node_id: str
    sequence: int
    value: Any


class OutputContext:
    """Read-only node output index rebuilt from completed node executions.

    This object is how user hooks read prior node results. It intentionally
    exposes only completed NodeExecution.output values, never internal
    OperatorCall outputs. Map and replication details remain trace data on
    NodeExecution.operator_calls.

    latest(node_id):
        Returns the newest completed output for a node id. This is the default
        read path for loops because a node can execute many times.

    all(node_id):
        Returns every completed output for a node id in execution order.

    timeline(node_ids=None):
        Returns ordered NodeOutput records across nodes. Use this when building
        chat/message history or trace-like input where global order matters.
    """

    def __init__(
        self,
        outputs: list[NodeOutput] | None = None,
        *,
        node_id_aliases: dict[str, str] | None = None,
    ) -> None:
        self._timeline: list[NodeOutput] = list(outputs or [])
        self._node_id_aliases = dict(node_id_aliases or {})
        self._by_node_id: dict[str, list[NodeOutput]] = {}
        for output in self._timeline:
            self._by_node_id.setdefault(output.node_id, []).append(output)

    def has(self, node_id: str) -> bool:
        return bool(self._by_node_id.get(self._resolve_node_id(node_id)))

    def latest(self, node_id: str, default: Any = _MISSING) -> Any:
        outputs = self._by_node_id.get(self._resolve_node_id(node_id))
        if outputs:
            return outputs[-1].value
        if default is not _MISSING:
            return default
        raise KeyError(f"No output found for node: {node_id}")

    def all(self, node_id: str) -> list[Any]:
        resolved = self._resolve_node_id(node_id)
        return [output.value for output in self._by_node_id.get(resolved, [])]

    def timeline(self, node_ids: list[str] | tuple[str, ...] | None = None) -> list[NodeOutput]:
        if node_ids is None:
            return list(self._timeline)
        allowed = {self._resolve_node_id(node_id) for node_id in node_ids}
        return [output for output in self._timeline if output.node_id in allowed]

    def scoped(self, node_id_aliases: dict[str, str]) -> OutputContext:
        """Return a view resolving local child Workflow ids to expanded ids."""

        if not node_id_aliases:
            return self
        return OutputContext(self._timeline, node_id_aliases=node_id_aliases)

    def _resolve_node_id(self, node_id: str) -> str:
        return self._node_id_aliases.get(node_id, node_id)

    @classmethod
    def from_executions(cls, executions: list[NodeExecution]) -> OutputContext:
        outputs = [
            NodeOutput(
                node_execution_id=execution.id,
                node_id=execution.node_id,
                sequence=execution.sequence,
                value=to_read_only(execution.output),
            )
            for execution in sorted(executions, key=lambda item: item.sequence)
            if execution.state == "completed"
        ]
        return cls(outputs)

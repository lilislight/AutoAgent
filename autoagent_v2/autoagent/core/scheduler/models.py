"""Invocation-local scheduling identities."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True, order=True, slots=True)
class LoopIteration:
    loop_region_id: str
    iteration: int

    def to_record(self) -> dict[str, object]:
        return {"loop_region_id": self.loop_region_id, "iteration": self.iteration}

    @classmethod
    def from_record(cls, value: dict[str, object]) -> "LoopIteration":
        return cls(str(value["loop_region_id"]), int(value["iteration"]))


ExecutionScope = tuple[LoopIteration, ...]


def scope_key(scope: ExecutionScope) -> str:
    if not scope:
        return "root"
    return "/".join(f"{item.loop_region_id}:{item.iteration}" for item in scope)


def occurrence_key(node_id: str, scope: ExecutionScope) -> str:
    return node_id if not scope else f"{node_id}@{scope_key(scope)}"


@dataclass(frozen=True, slots=True)
class EdgeActivation:
    edge_id: str
    source_node_id: str
    source_execution_id: UUID

    def to_record(self) -> dict[str, object]:
        return {
            "edge_id": self.edge_id,
            "source_node_id": self.source_node_id,
            "source_execution_id": str(self.source_execution_id),
        }

    @classmethod
    def from_record(cls, value: dict[str, object]) -> "EdgeActivation":
        return cls(
            str(value["edge_id"]),
            str(value["source_node_id"]),
            UUID(str(value["source_execution_id"])),
        )


@dataclass(frozen=True, slots=True)
class NodeExecutionRequest:
    node_id: str
    scope: ExecutionScope = ()
    activations: tuple[EdgeActivation, ...] = ()
    recovery_attempt: int = 0

    @property
    def occurrence(self) -> str:
        return occurrence_key(self.node_id, self.scope)

    def to_record(self) -> dict[str, object]:
        return {
            "node_id": self.node_id,
            "scope": [item.to_record() for item in self.scope],
            "activations": [item.to_record() for item in self.activations],
            "recovery_attempt": self.recovery_attempt,
        }

    @classmethod
    def from_record(cls, value: dict[str, object]) -> "NodeExecutionRequest":
        return cls(
            node_id=str(value["node_id"]),
            scope=tuple(
                LoopIteration.from_record(item)  # type: ignore[arg-type]
                for item in value.get("scope", [])  # type: ignore[union-attr]
            ),
            activations=tuple(
                EdgeActivation.from_record(item)  # type: ignore[arg-type]
                for item in value.get("activations", [])  # type: ignore[union-attr]
            ),
            recovery_attempt=int(value.get("recovery_attempt", 0)),
        )


@dataclass(frozen=True, slots=True)
class EdgeResolution:
    edge_id: str
    target_scope: ExecutionScope
    selected: bool
    activation: EdgeActivation | None = None

    def to_record(self) -> dict[str, object]:
        return {
            "edge_id": self.edge_id,
            "target_scope": [item.to_record() for item in self.target_scope],
            "selected": self.selected,
            "activation": self.activation.to_record() if self.activation else None,
        }

    @classmethod
    def from_record(cls, value: dict[str, object]) -> "EdgeResolution":
        activation = value.get("activation")
        return cls(
            edge_id=str(value["edge_id"]),
            target_scope=tuple(
                LoopIteration.from_record(item)  # type: ignore[arg-type]
                for item in value.get("target_scope", [])  # type: ignore[union-attr]
            ),
            selected=bool(value["selected"]),
            activation=(
                EdgeActivation.from_record(activation)  # type: ignore[arg-type]
                if activation is not None
                else None
            ),
        )

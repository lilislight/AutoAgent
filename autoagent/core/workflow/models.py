"""Authoring definitions and immutable Workflow IR for the V2 Core."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Literal, TypeAlias, Union
from pydantic import BaseModel, ConfigDict, field_validator

from ..operators import Operator, OperatorContract, StreamReducer, ValueContract, Wait
from ..context import ContextOperation, ContextPatch


Executable: TypeAlias = Union[Callable[..., object], Operator, Wait, "Workflow"]


class InvocationRef(BaseModel):
    """Stable external control identity for a Root Invocation."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    session_id: str
    invocation_id: str
    workflow_id: str
    workflow_revision_id: str

    @field_validator(
        "session_id", "invocation_id", "workflow_id", "workflow_revision_id"
    )
    @classmethod
    def _non_empty_identity(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("InvocationRef identity fields cannot be empty.")
        return value


class ChildHandle(BaseModel):
    """Durable identity of an owned Child; never an App control reference."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    child_session_id: str
    child_invocation_id: str
    workflow_revision_id: str

    @field_validator("child_session_id", "child_invocation_id", "workflow_revision_id")
    @classmethod
    def _non_empty_identity(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("ChildHandle identity fields cannot be empty.")
        return value


@dataclass(frozen=True, slots=True)
class Context:
    invocation_context: Mapping[str, object]
    session_context: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class InputMappingContext(Context):
    invocation_input: object
    incoming: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class OutputBindingContext(Context):
    output: object


@dataclass(frozen=True, slots=True)
class ConditionContext(Context):
    source_node_id: str
    output: object | None = None
    error: "ErrorInfo | None" = None


@dataclass(frozen=True, slots=True)
class AggregationContext(Context):
    inputs: tuple[object, ...]
    outputs: tuple[object, ...]


@dataclass(frozen=True, slots=True)
class StreamContext(Context):
    input: object


@dataclass(frozen=True, slots=True)
class ErrorInfo:
    type: str
    message: str


InputMapping = Callable[[InputMappingContext], object | Awaitable[object]]
OutputBinding = Callable[
    [OutputBindingContext], ContextPatch | None | Awaitable[ContextPatch | None]
]
Condition = Callable[[ConditionContext], bool | Awaitable[bool]]
Aggregation = Callable[[AggregationContext], object | Awaitable[object]]
UserEventMapper = Callable[[OutputBindingContext], object | Awaitable[object]]


@dataclass(frozen=True, slots=True)
class UserEventMapping:
    kind: str
    mapper: UserEventMapper = field(compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.kind, str) or not self.kind.strip():
            raise ValueError("User Event kind cannot be empty.")


@dataclass(frozen=True, slots=True)
class UserEventMappingIR:
    kind: str
    mapper: UserEventMapper = field(compare=False)
    output_contract: ValueContract


@dataclass(frozen=True, slots=True)
class Map:
    aggregate: Aggregation | None = field(default=None, compare=False)
    max_parallelism: int | None = None

    def __post_init__(self) -> None:
        if self.max_parallelism is not None and not _positive_int(
            self.max_parallelism
        ):
            raise ValueError("Map max_parallelism must be positive.")


@dataclass(frozen=True, slots=True)
class Recovery:
    """Crash recovery rule for one logical Node occurrence."""

    mode: Literal["never", "replay_safe"] = "never"
    max_attempts: int = 1

    def __post_init__(self) -> None:
        if not isinstance(self.mode, str) or self.mode not in {
            "never",
            "replay_safe",
        }:
            raise ValueError("Recovery mode is invalid.")
        if not _positive_int(self.max_attempts):
            raise ValueError("Recovery max_attempts must be positive.")


@dataclass(frozen=True, slots=True)
class Stream:
    reducer: StreamReducer = field(compare=False)


@dataclass(frozen=True, slots=True)
class Capability:
    id: str
    contract: OperatorContract

    def __post_init__(self) -> None:
        if not isinstance(self.id, str) or not self.id.strip():
            raise ValueError("Capability id cannot be empty.")
        if not isinstance(self.contract, OperatorContract):
            raise TypeError("Capability contract must be OperatorContract.")


@dataclass(slots=True)
class Node:
    id: str
    executable: Executable | Capability
    input_mapping: InputMapping | None = None
    output_binding: OutputBinding | None = None
    execution_mode: Literal["await", "spawn"] = "await"
    map: Map | None = None
    stream: Stream | None = None
    user_events: tuple[UserEventMapping, ...] = ()
    recovery_mode: Recovery = field(default_factory=Recovery)


@dataclass(slots=True)
class Edge:
    source: str
    target: str
    condition: Condition | None = None
    on: Literal["complete", "error"] = "complete"
    id: str | None = None


@dataclass(slots=True)
class SubWorkflow:
    id: str
    workflow: "Workflow"


@dataclass(slots=True)
class Workflow:
    id: str
    nodes: list[Node] = field(default_factory=list)
    edges: list[Edge] = field(default_factory=list)
    sub_workflows: list[SubWorkflow] = field(default_factory=list)
    version: str | int = "1"
    failure_mode: Literal["fail_fast", "continue_active_branches"] = "fail_fast"

    def add_node(self, node: Node) -> Node:
        self.nodes.append(node)
        return node

    def add_edge(self, edge: Edge) -> Edge:
        self.edges.append(edge)
        return edge


@dataclass(frozen=True, slots=True)
class NodeIR:
    id: str
    executable: Operator | Wait | Capability | "WorkflowIR"
    input_contract: ValueContract | None
    output_contract: ValueContract | None
    input_mapping: InputMapping | None = field(default=None, compare=False)
    output_binding: OutputBinding | None = field(default=None, compare=False)
    execution_mode: Literal["await", "spawn"] = "await"
    map: Map | None = field(default=None, compare=False)
    stream: Stream | None = field(default=None, compare=False)
    user_events: tuple[UserEventMappingIR, ...] = field(
        default=(), compare=False
    )
    recovery_mode: Recovery = field(default_factory=Recovery)


@dataclass(frozen=True, slots=True)
class EdgeIR:
    id: str
    source: str
    target: str
    condition: Condition | None = field(default=None, compare=False)
    on: Literal["complete", "error"] = "complete"


@dataclass(frozen=True, slots=True)
class LoopRegionIR:
    id: str
    header_node_id: str
    node_ids: tuple[str, ...]
    entry_edge_ids: tuple[str, ...]
    back_edge_ids: tuple[str, ...]
    exit_edge_ids: tuple[str, ...]
    parent_loop_region_id: str | None = None

    def __post_init__(self) -> None:
        if len(self.back_edge_ids) != 1:
            raise ValueError("A Loop region must own exactly one Back Edge.")

    @property
    def back_edge_id(self) -> str:
        return self.back_edge_ids[0]


@dataclass(frozen=True, slots=True)
class WorkflowIR:
    workflow_id: str
    workflow_revision_id: str
    definition_hash: str
    workflow_version: str
    nodes: tuple[NodeIR, ...]
    edges: tuple[EdgeIR, ...]
    entry_node_ids: tuple[str, ...]
    exit_node_ids: tuple[str, ...]
    failure_mode: Literal["fail_fast", "continue_active_branches"] = "fail_fast"
    loop_regions: tuple[LoopRegionIR, ...] = ()
    _nodes: Mapping[str, NodeIR] = field(init=False, repr=False, compare=False)
    _edges: Mapping[str, EdgeIR] = field(init=False, repr=False, compare=False)
    _incoming: Mapping[str, tuple[EdgeIR, ...]] = field(init=False, repr=False, compare=False)
    _outgoing: Mapping[str, tuple[EdgeIR, ...]] = field(init=False, repr=False, compare=False)
    _loops: Mapping[str, LoopRegionIR] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        nodes = {node.id: node for node in self.nodes}
        incoming: dict[str, list[EdgeIR]] = {key: [] for key in nodes}
        outgoing: dict[str, list[EdgeIR]] = {key: [] for key in nodes}
        for edge in self.edges:
            incoming[edge.target].append(edge)
            outgoing[edge.source].append(edge)
        object.__setattr__(self, "_nodes", MappingProxyType(nodes))
        object.__setattr__(self, "_edges", MappingProxyType({edge.id: edge for edge in self.edges}))
        object.__setattr__(self, "_incoming", MappingProxyType({k: tuple(v) for k, v in incoming.items()}))
        object.__setattr__(self, "_outgoing", MappingProxyType({k: tuple(v) for k, v in outgoing.items()}))
        object.__setattr__(self, "_loops", MappingProxyType({loop.id: loop for loop in self.loop_regions}))

    def node(self, node_id: str) -> NodeIR:
        return self._nodes[node_id]

    def edge(self, edge_id: str) -> EdgeIR:
        return self._edges[edge_id]

    def incoming(self, node_id: str) -> tuple[EdgeIR, ...]:
        return self._incoming[node_id]

    def outgoing(self, node_id: str) -> tuple[EdgeIR, ...]:
        return self._outgoing[node_id]

    def loop(self, loop_id: str) -> LoopRegionIR:
        return self._loops[loop_id]

    def containing_loops(self, node_id: str) -> tuple[LoopRegionIR, ...]:
        values = [loop for loop in self.loop_regions if node_id in loop.node_ids]
        return tuple(sorted(values, key=lambda value: (-len(value.node_ids), value.id)))

    def back_loop(self, edge_id: str) -> LoopRegionIR | None:
        return next((loop for loop in self.loop_regions if edge_id in loop.back_edge_ids), None)

    def entry_loops(self, edge_id: str) -> tuple[LoopRegionIR, ...]:
        return tuple(loop for loop in self.loop_regions if edge_id in loop.entry_edge_ids)

    def exit_loops(self, edge_id: str) -> tuple[LoopRegionIR, ...]:
        return tuple(loop for loop in self.loop_regions if edge_id in loop.exit_edge_ids)


def _positive_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0

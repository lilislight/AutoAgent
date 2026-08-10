"""Small V2 Workflow authoring model and immutable execution IR."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Protocol, TypeAlias

from ..operators import Operator, ValueContract, WaitOperator
from .policy import NodePolicy, WorkflowPolicy


JsonObject: TypeAlias = dict[str, Any]
OperatorCallable: TypeAlias = Callable[..., Any | Awaitable[Any]]


class LoopIterationView(Protocol):
    loop_region_id: str
    iteration: int


ExecutionScope: TypeAlias = tuple[LoopIterationView, ...]


@dataclass(frozen=True, slots=True)
class IncomingActivation:
    """One exact Edge activation visible to the target Node Hook."""

    edge_id: str
    source_node_id: str
    source_execution_id: str
    source_scope: ExecutionScope
    value: Any


@dataclass(frozen=True, slots=True)
class HookContext:
    """Common isolated state visible to every user-defined Workflow Hook."""

    workflow_id: str
    workflow_revision_id: str
    workflow_path: tuple[str, ...]
    session_id: str
    invocation_id: str
    session_context: Mapping[str, Any]
    invocation_context: Mapping[str, Any]
    invocation_input: Any


@dataclass(frozen=True, slots=True)
class NodeHookContext(HookContext):
    node_id: str
    node_execution_id: str
    execution_scope: ExecutionScope
    incoming: Sequence[IncomingActivation]


@dataclass(frozen=True, slots=True)
class InputMappingContext(NodeHookContext):
    pass


@dataclass(frozen=True, slots=True)
class ItemSelectorContext(NodeHookContext):
    input: Any


@dataclass(frozen=True, slots=True)
class AggregationContext(NodeHookContext):
    input: Any
    operator_outputs: list[Any]


@dataclass(frozen=True, slots=True)
class OutputBindingContext(NodeHookContext):
    input: Any
    output: Any


@dataclass(frozen=True, slots=True)
class EdgeConditionContext(HookContext):
    edge_id: str
    source_node_id: str
    source_execution_id: str
    source_scope: ExecutionScope
    output: Any


InputMapping: TypeAlias = Callable[[InputMappingContext], Any | Awaitable[Any]]
OutputBinding: TypeAlias = Callable[
    [OutputBindingContext], "ContextPatch | None | Awaitable[ContextPatch | None]"
]
EdgeCondition: TypeAlias = Callable[[EdgeConditionContext], bool | Awaitable[bool]]
UserEventTransform: TypeAlias = Callable[[Any], Any | Awaitable[Any]]


@dataclass(frozen=True, slots=True)
class ContextPatch:
    """Atomic context changes produced by Output Binding."""

    session: Mapping[str, Any] = field(default_factory=dict)
    invocation: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class UserEventMapping:
    """Map a successfully committed Node output to one User Event."""

    type: str
    transform: UserEventTransform


@dataclass(slots=True)
class Node:
    id: str
    operator: OperatorCallable | Operator | WaitOperator | Workflow
    fallback_operators: tuple[OperatorCallable | Operator, ...] = ()
    input_mapping: InputMapping | None = None
    output_binding: OutputBinding | None = None
    stream_user_event_mappings: tuple[UserEventMapping, ...] = ()
    user_event_mappings: tuple[UserEventMapping, ...] = ()
    name: str | None = None
    hook_version: str | int = 1
    policy: NodePolicy | None = None
    entry: bool | None = None
    child_entry_node_id: str | None = None
    child_exit_node_id: str | None = None
    _workflow_path: tuple[str, ...] = field(default=(), repr=False)
    _local_id: str | None = field(default=None, repr=False)


@dataclass(slots=True)
class Edge:
    source: str
    target: str
    condition: EdgeCondition | None = None
    id: str | None = None
    hook_version: str | int = 1
    _workflow_path: tuple[str, ...] = field(default=(), repr=False)
    _local_id: str | None = field(default=None, repr=False)


@dataclass(slots=True)
class Workflow:
    id: str
    nodes: list[Node] = field(default_factory=list)
    edges: list[Edge] = field(default_factory=list)
    version: str | int = 1
    name: str | None = None
    policy: WorkflowPolicy = WorkflowPolicy()

    def add_node(self, node: Node) -> Node:
        self.nodes.append(node)
        return node

    def add_edge(self, edge: Edge) -> Edge:
        self.edges.append(edge)
        return edge


@dataclass(frozen=True, slots=True)
class NodeIR:
    id: str
    operator: Operator | WaitOperator
    fallback_operators: tuple[Operator, ...]
    input_schema: str
    output_schema: str
    output_contract: ValueContract
    input_mapping: InputMapping | None
    output_binding: OutputBinding | None
    stream_user_event_mappings: tuple[UserEventMapping, ...]
    stream_user_event_contracts: tuple[ValueContract, ...]
    user_event_mappings: tuple[UserEventMapping, ...]
    user_event_contracts: tuple[ValueContract, ...]
    hook_version: str
    name: str | None
    policy: NodePolicy | None
    workflow_path: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class EdgeIR:
    id: str
    source: str
    target: str
    condition: EdgeCondition | None
    hook_version: str
    workflow_path: tuple[str, ...] = ()


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
            raise ValueError("A V2 LoopRegionIR must own exactly one Back Edge.")

    @property
    def back_edge_id(self) -> str:
        return self.back_edge_ids[0]


@dataclass(frozen=True, slots=True)
class SubworkflowIR:
    path: tuple[str, ...]
    workflow_id: str
    workflow_version: str
    name: str | None = None


@dataclass(frozen=True, slots=True)
class WorkflowIR:
    workflow_id: str
    workflow_revision_id: str
    definition_hash: str
    workflow_version: str
    name: str | None
    nodes: tuple[NodeIR, ...]
    edges: tuple[EdgeIR, ...]
    entry_node_ids: tuple[str, ...]
    exit_node_ids: tuple[str, ...]
    loop_regions: tuple[LoopRegionIR, ...] = ()
    subworkflows: tuple[SubworkflowIR, ...] = ()
    policy: WorkflowPolicy = WorkflowPolicy()
    _node_index: Mapping[str, NodeIR] = field(init=False, repr=False, compare=False)
    _edge_index: Mapping[str, EdgeIR] = field(init=False, repr=False, compare=False)
    _outgoing_index: Mapping[str, tuple[EdgeIR, ...]] = field(
        init=False, repr=False, compare=False
    )
    _incoming_index: Mapping[str, tuple[EdgeIR, ...]] = field(
        init=False, repr=False, compare=False
    )
    _loop_index: Mapping[str, LoopRegionIR] = field(
        init=False, repr=False, compare=False
    )
    _containing_loop_index: Mapping[str, tuple[LoopRegionIR, ...]] = field(
        init=False, repr=False, compare=False
    )
    _back_loop_index: Mapping[str, LoopRegionIR] = field(
        init=False, repr=False, compare=False
    )
    _entry_loop_index: Mapping[str, tuple[LoopRegionIR, ...]] = field(
        init=False, repr=False, compare=False
    )
    _exit_loop_index: Mapping[str, tuple[LoopRegionIR, ...]] = field(
        init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        nodes = {node.id: node for node in self.nodes}
        outgoing: dict[str, list[EdgeIR]] = {node_id: [] for node_id in nodes}
        incoming: dict[str, list[EdgeIR]] = {node_id: [] for node_id in nodes}
        for edge in self.edges:
            outgoing[edge.source].append(edge)
            incoming[edge.target].append(edge)
        object.__setattr__(self, "_node_index", MappingProxyType(nodes))
        object.__setattr__(
            self,
            "_edge_index",
            MappingProxyType({edge.id: edge for edge in self.edges}),
        )
        object.__setattr__(
            self,
            "_outgoing_index",
            MappingProxyType({key: tuple(value) for key, value in outgoing.items()}),
        )
        object.__setattr__(
            self,
            "_incoming_index",
            MappingProxyType({key: tuple(value) for key, value in incoming.items()}),
        )
        loops = {region.id: region for region in self.loop_regions}
        containing = {
            node_id: tuple(
                sorted(
                    (
                        region
                        for region in self.loop_regions
                        if node_id in region.node_ids
                    ),
                    key=lambda item: (-len(item.node_ids), item.id),
                )
            )
            for node_id in nodes
        }
        back = {region.back_edge_id: region for region in self.loop_regions}
        entry: dict[str, list[LoopRegionIR]] = {}
        exits: dict[str, list[LoopRegionIR]] = {}
        for region in self.loop_regions:
            for edge_id in region.entry_edge_ids:
                entry.setdefault(edge_id, []).append(region)
            for edge_id in region.exit_edge_ids:
                exits.setdefault(edge_id, []).append(region)
        object.__setattr__(self, "_loop_index", MappingProxyType(loops))
        object.__setattr__(
            self, "_containing_loop_index", MappingProxyType(containing)
        )
        object.__setattr__(self, "_back_loop_index", MappingProxyType(back))
        object.__setattr__(
            self,
            "_entry_loop_index",
            MappingProxyType(
                {
                    edge_id: tuple(
                        sorted(values, key=lambda item: (-len(item.node_ids), item.id))
                    )
                    for edge_id, values in entry.items()
                }
            ),
        )
        object.__setattr__(
            self,
            "_exit_loop_index",
            MappingProxyType(
                {
                    edge_id: tuple(
                        sorted(values, key=lambda item: (len(item.node_ids), item.id))
                    )
                    for edge_id, values in exits.items()
                }
            ),
        )

    def node(self, node_id: str) -> NodeIR:
        return self._node_index[node_id]

    def outgoing(self, node_id: str) -> tuple[EdgeIR, ...]:
        return self._outgoing_index[node_id]

    def edge(self, edge_id: str) -> EdgeIR:
        return self._edge_index[edge_id]

    def incoming(self, node_id: str) -> tuple[EdgeIR, ...]:
        return self._incoming_index[node_id]

    def loop(self, loop_id: str) -> LoopRegionIR:
        return self._loop_index[loop_id]

    def containing_loops(self, node_id: str) -> tuple[LoopRegionIR, ...]:
        return self._containing_loop_index[node_id]

    def back_loop(self, edge_id: str) -> LoopRegionIR | None:
        return self._back_loop_index.get(edge_id)

    def entry_loops(self, edge_id: str) -> tuple[LoopRegionIR, ...]:
        return self._entry_loop_index.get(edge_id, ())

    def exit_loops(self, edge_id: str) -> tuple[LoopRegionIR, ...]:
        return self._exit_loop_index.get(edge_id, ())

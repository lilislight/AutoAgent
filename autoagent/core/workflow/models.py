"""Authoring definitions and immutable Workflow IR for the V2 Core."""

from __future__ import annotations

from ..commands.signals import SignalEndpoint, SignalLimits

from ..commands import Wait
from ..commands import SystemCommand, RuntimeHandle, CommandIR
from ..commands.models import runtime_handles

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
import math
from types import MappingProxyType
from typing import Literal, TypeAlias, Union
from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator

from ..operators import Operator, OperatorContract, StreamReducer, ValueContract
from ..context import ContextPatch


Executable: TypeAlias = Union[Callable[..., object], Operator, SystemCommand]


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


@dataclass(frozen=True, slots=True)
class Context:
    invocation_context: Mapping[str, object]
    session_context: Mapping[str, object]
    self_handle: RuntimeHandle | None = field(default_factory=lambda: runtime_handles.get()[0], kw_only=True)
    owner_handle: RuntimeHandle | None = field(default_factory=lambda: runtime_handles.get()[1], kw_only=True)


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


@dataclass(frozen=True, slots=True)
class UserEventContext(Context):
    """One immutable Chunk record or committed Node output for an opt-in Mapper.

    Output is emitted once per Node occurrence, after Map aggregation and Output
    Binding. Chunk callbacks identify the individual Operator Call/Map unit.
    Values are read-only durable records, just like other Core hook views.
    """

    phase: Literal["chunk", "output"]
    value: object
    session_id: str
    invocation_id: str
    node_id: str
    occurrence_id: str
    operator_id: str | None = None
    operator_call_id: str | None = None
    unit_index: int | None = None


class UserEventSpec(BaseModel):
    """User-selected event content; Core supplies delivery identities and time."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True,
        revalidate_instances="always", allow_inf_nan=False,
    )

    kind: str = Field(min_length=1, pattern=r"\S")
    payload: JsonValue

    @field_validator("payload", mode="before")
    @classmethod
    def _owned_json_payload(cls, value: object) -> object:
        from ..runtime.values import freeze, thaw

        try:
            return thaw(freeze(value))
        except TypeError as error:
            raise ValueError(str(error)) from error


UserEventMapper = Callable[
    [UserEventContext], UserEventSpec | None | Awaitable[UserEventSpec | None]
]


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
    """Crash recovery rule for one Node occurrence; replay is allowed by default."""

    mode: Literal["never", "replay_safe"] = "replay_safe"
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
class TimeoutPolicy:
    """Execution deadline per admitted Operator Call, including its stream.

    Queueing, mappings and bindings are outside this deadline. Each Map unit
    and each recovery attempt receives its own budget. This is not a durable
    Wait deadline and does not forcibly stop synchronous Python code.
    """

    seconds: float

    def __post_init__(self):
        if isinstance(self.seconds, bool) or not isinstance(self.seconds, (int, float)):
            raise ValueError("TimeoutPolicy seconds must be positive and finite.")
        try:
            seconds = float(self.seconds)
        except OverflowError as error:
            raise ValueError("TimeoutPolicy seconds must be positive and finite.") from error
        if not math.isfinite(seconds) or seconds <= 0:
            raise ValueError("TimeoutPolicy seconds must be positive and finite.")
        object.__setattr__(self, "seconds", seconds)


@dataclass(frozen=True, slots=True)
class BackoffPolicy:
    """Delay before retries; jitter never exceeds the capped base delay."""

    mode: Literal["fixed", "linear", "exponential"] = "exponential"
    initial_seconds: float = 0.5
    max_seconds: float = 30.0
    multiplier: float = 2.0
    jitter: Literal["none", "full", "equal"] = "full"

    def __post_init__(self):
        if self.mode not in {"fixed", "linear", "exponential"}:
            raise ValueError("Invalid BackoffPolicy mode.")
        if self.jitter not in {"none", "full", "equal"}:
            raise ValueError("Invalid BackoffPolicy jitter.")
        for name in ("initial_seconds", "max_seconds", "multiplier"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"BackoffPolicy {name} must be finite and nonnegative.")
            try:
                value = float(value)
            except OverflowError as error:
                raise ValueError(f"BackoffPolicy {name} must be finite.") from error
            if not math.isfinite(value) or value < 0 or (name == "multiplier" and value < 1):
                raise ValueError(f"Invalid BackoffPolicy {name}.")
            object.__setattr__(self, name, value)
        if self.max_seconds < self.initial_seconds:
            raise ValueError("BackoffPolicy max_seconds must cover initial_seconds.")

    def delay(self, retry_attempt: int) -> float:
        """Return the delay for an extra attempt numbered from one."""
        import random
        if not _positive_int(retry_attempt):
            raise ValueError("retry_attempt must be positive.")
        if self.initial_seconds == 0:
            return 0.0
        if self.mode == "fixed":
            delay = self.initial_seconds
        elif self.mode == "linear":
            try:
                delay = self.initial_seconds * retry_attempt
            except OverflowError:
                delay = self.max_seconds
        else:
            try:
                delay = self.initial_seconds * self.multiplier ** (retry_attempt - 1)
            except OverflowError:
                delay = self.max_seconds
        delay = min(delay, self.max_seconds)
        if self.jitter == "full":
            return random.uniform(0, delay)
        if self.jitter == "equal":
            return random.uniform(delay / 2, delay)
        return delay


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Extra attempts per Map unit, excluding the initial call.

    retry_on matches exception class names along their inheritance chain.
    Only execution exceptions and deadlines are eligible; contract, reducer,
    cancellation and Runtime failures never qualify. Emitted streams and
    outstanding synchronous work also prevent automatic retry.
    """

    max_attempts: int = 1
    backoff: BackoffPolicy = field(default_factory=BackoffPolicy)
    retry_on: tuple[str, ...] = ("Exception",)

    def __post_init__(self):
        if type(self.max_attempts) is not int or self.max_attempts < 0:
            raise ValueError("RetryPolicy max_attempts must be nonnegative.")
        if not isinstance(self.backoff, BackoffPolicy):
            raise TypeError("RetryPolicy backoff must be BackoffPolicy.")
        if not isinstance(self.retry_on, tuple) or not all(isinstance(x, str) and x.strip() for x in self.retry_on):
            raise TypeError("RetryPolicy retry_on must be a tuple of exception names.")


@dataclass(frozen=True, slots=True)
class ResourcePolicy:
    """Maximum starts of this Node ID in one Invocation, independent of Calls."""

    max_executions_per_invocation: int | None = 1000

    def __post_init__(self):
        value = self.max_executions_per_invocation
        if value is not None and not _positive_int(value):
            raise ValueError("ResourcePolicy max_executions_per_invocation must be positive or None.")


@dataclass(frozen=True, slots=True)
class Stream:
    reducer: StreamReducer = field(compare=False)


@dataclass(frozen=True, slots=True)
class Capability:
    id: str
    contract: OperatorContract
    stream: Stream | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.id, str) or not self.id.strip():
            raise ValueError("Capability id cannot be empty.")
        if not isinstance(self.contract, OperatorContract):
            raise TypeError("Capability contract must be OperatorContract.")
        if self.stream is not None and not isinstance(self.stream, Stream):
            raise TypeError("Capability stream must be Stream or None.")


@dataclass(slots=True)
class Node:
    id: str
    executable: Executable | Capability
    input_mapping: InputMapping | None = None
    output_binding: OutputBinding | None = None
    map: Map | None = None
    stream: Stream | None = None
    user_events: UserEventMapper | None = field(default=None, compare=False)
    recovery_mode: Recovery = field(default_factory=Recovery)
    timeout: TimeoutPolicy | None = None
    retry: RetryPolicy | None = None
    resource: ResourcePolicy = field(default_factory=ResourcePolicy)


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
    signal_endpoints: list[SignalEndpoint] = field(default_factory=list)
    signal_limits: SignalLimits = field(default_factory=SignalLimits)
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
    executable: Operator | Wait | CommandIR | Capability
    input_contract: ValueContract | None
    output_contract: ValueContract | None
    stream_output_contract: ValueContract | None = field(default=None, compare=False)
    input_mapping: InputMapping | None = field(default=None, compare=False)
    output_binding: OutputBinding | None = field(default=None, compare=False)
    map: Map | None = field(default=None, compare=False)
    stream: Stream | None = field(default=None, compare=False)
    user_events: UserEventMapper | None = field(default=None, compare=False)
    recovery_mode: Recovery = field(default_factory=Recovery)
    timeout: TimeoutPolicy | None = None
    retry: RetryPolicy | None = None
    resource: ResourcePolicy = field(default_factory=ResourcePolicy)


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
    signal_endpoints: tuple[tuple[str, ValueContract], ...] = ()
    signal_limits: SignalLimits = field(default_factory=SignalLimits)
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

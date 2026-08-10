"""Workflow and Node execution policies owned by V2 Core."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Generic, Literal, TypeVar

from ..operators import StreamReducer


ChunkT = TypeVar("ChunkT")
ResultT = TypeVar("ResultT")


@dataclass(frozen=True, slots=True)
class FailurePolicy:
    mode: Literal["fail_fast", "continue_active_branches"] = "fail_fast"

    def __post_init__(self) -> None:
        if self.mode not in {"fail_fast", "continue_active_branches"}:
            raise ValueError("Unsupported Workflow failure mode.")


@dataclass(frozen=True, slots=True)
class WorkflowPolicy:
    failure: FailurePolicy = FailurePolicy()


@dataclass(frozen=True, slots=True)
class BackoffPolicy:
    mode: Literal["fixed", "linear", "exponential"] = "exponential"
    initial_delay_ms: int = 1000
    max_delay_ms: int | None = None
    multiplier: float = 2.0
    jitter: Literal["none", "full", "equal"] = "none"

    def __post_init__(self) -> None:
        if self.mode not in {"fixed", "linear", "exponential"}:
            raise ValueError("Unsupported backoff mode.")
        if self.initial_delay_ms < 0:
            raise ValueError("initial_delay_ms cannot be negative.")
        if self.max_delay_ms is not None and self.max_delay_ms < 0:
            raise ValueError("max_delay_ms cannot be negative.")
        if self.multiplier <= 0:
            raise ValueError("multiplier must be positive.")
        if self.jitter not in {"none", "full", "equal"}:
            raise ValueError("Unsupported backoff jitter mode.")
        if self.max_delay_ms is not None and self.max_delay_ms < self.initial_delay_ms:
            raise ValueError("max_delay_ms cannot be less than initial_delay_ms.")


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 1
    backoff: BackoffPolicy | None = None

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least one.")


@dataclass(frozen=True, slots=True)
class RecoveryPolicy:
    mode: Literal["never", "replay_safe", "idempotent"] = "never"
    max_attempts: int = 1

    def __post_init__(self) -> None:
        if self.mode not in {"never", "replay_safe", "idempotent"}:
            raise ValueError("Unsupported recovery mode.")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least one.")


@dataclass(frozen=True, slots=True)
class TimeoutPolicy:
    timeout_ms: int

    def __post_init__(self) -> None:
        if self.timeout_ms <= 0:
            raise ValueError("timeout_ms must be positive.")


@dataclass(frozen=True, slots=True)
class ResourcePolicy:
    max_node_executions_per_invocation: int | None = None
    max_operator_attempts_per_invocation: int | None = None
    max_runtime_ms_per_invocation: int | None = None

    def __post_init__(self) -> None:
        for name, value in (
            ("max_node_executions_per_invocation", self.max_node_executions_per_invocation),
            ("max_operator_attempts_per_invocation", self.max_operator_attempts_per_invocation),
            ("max_runtime_ms_per_invocation", self.max_runtime_ms_per_invocation),
        ):
            if value is not None and value < 1:
                raise ValueError(f"{name} must be positive.")


MapSelector = Callable[
    [Any],
    Iterable[Mapping[str, Any]] | Awaitable[Iterable[Mapping[str, Any]]],
]
Aggregator = Callable[[Any], Any | Awaitable[Any]]


@dataclass(frozen=True, slots=True)
class StreamPolicy(Generic[ChunkT, ResultT]):
    """Consume an Operator stream with one fresh reducer per attempt."""

    reducer: type[StreamReducer[ChunkT, ResultT]]

    def __post_init__(self) -> None:
        if not isinstance(self.reducer, type):
            raise TypeError("StreamPolicy reducer must be a reducer class.")


@dataclass(frozen=True, slots=True)
class MapPolicy:
    item_selector: MapSelector | None = None
    output_aggregator: Aggregator | None = None
    max_parallelism: int | None = None

    def __post_init__(self) -> None:
        if self.max_parallelism is not None and self.max_parallelism < 1:
            raise ValueError("max_parallelism must be positive.")


@dataclass(frozen=True, slots=True)
class ReplicationPolicy:
    count: int
    output_aggregator: Aggregator | None = None
    max_parallelism: int | None = None

    def __post_init__(self) -> None:
        if self.count < 1:
            raise ValueError("Replication count must be positive.")
        if self.max_parallelism is not None and self.max_parallelism < 1:
            raise ValueError("max_parallelism must be positive.")


@dataclass(frozen=True, slots=True)
class NodePolicy:
    retry: RetryPolicy | None = None
    recovery: RecoveryPolicy | None = None
    timeout: TimeoutPolicy | None = None
    resource: ResourcePolicy | None = None
    map: MapPolicy | None = None
    replication: ReplicationPolicy | None = None
    stream: StreamPolicy[Any, Any] | None = None

    def __post_init__(self) -> None:
        if self.map is not None and self.replication is not None:
            raise ValueError("Map and Replication cannot be enabled together.")

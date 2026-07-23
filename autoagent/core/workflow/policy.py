from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable, Mapping
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from autoagent.core.runtime.context import (
        MapAggregationContext,
        MapItemSelectionContext,
        ReplicationAggregationContext,
    )
else:
    MapAggregationContext = Any
    MapItemSelectionContext = Any
    ReplicationAggregationContext = Any


class FailurePolicy(BaseModel):
    """Unhandled branch-failure behavior for one Workflow Invocation."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    mode: Literal["fail_fast", "continue_active_branches"] = "fail_fast"


class WorkflowPolicy(BaseModel):
    """Workflow-wide execution policy."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    failure: FailurePolicy = Field(default_factory=FailurePolicy)


class CapabilitySelectionPolicy(BaseModel):
    """Operator selection rule for abstract capability references."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    mode: Literal[
        "default",
        "priority",
        "first_available",
    ] = Field(
        default="default",
        description="How to choose an operator for a capability.",
    )
    allow_fallback: bool = Field(
        default=True,
        description=(
            "Whether execution may try another operator after an OperatorExecution "
            "failure. Mapping, binding, condition, and aggregation failures do "
            "not enter operator fallback."
        ),
    )
    preferred_operator_ids: tuple[str, ...] = Field(
        default_factory=tuple,
        description="Operators preferred during selection.",
    )
    excluded_operator_ids: tuple[str, ...] = Field(
        default_factory=tuple,
        description="Operators excluded during selection.",
    )


class BackoffPolicy(BaseModel):
    """Delay strategy between retry attempts."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    mode: Literal["fixed", "linear", "exponential"] = Field(
        default="exponential",
        description="Backoff strategy used between retry attempts.",
    )
    initial_delay_ms: int = Field(
        default=1000,
        description="Base delay before retry attempt 1, in milliseconds.",
    )
    max_delay_ms: int | None = Field(
        default=None,
        description="Maximum computed delay in milliseconds, applied before jitter.",
    )
    multiplier: float = Field(
        default=2.0,
        description=(
            "Growth factor. For linear: base * (1 + multiplier * retry_index). "
            "For exponential: base * multiplier ** retry_index."
        ),
    )
    jitter: Literal["none", "full", "equal"] = Field(
        default="none",
        description=(
            "Jitter after max_delay_ms clamp. none uses delay; full samples "
            "[0, delay]; equal samples [delay / 2, delay]."
        ),
    )


class RetryPolicy(BaseModel):
    """Retry rule for OperatorExecution failures inside one NodeExecution."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    max_attempts: int = Field(
        default=1,
        description=(
            "Maximum calls per selected Operator. V1 retries Operator handler "
            "exceptions, timeouts, and invalid Operator outputs. Input mapping, "
            "item selection, condition, output aggregation, and output binding "
            "failures are deterministic framework-stage errors and are never "
            "retried or sent to capability fallback."
        ),
    )
    backoff: BackoffPolicy | None = Field(
        default=None,
        description="Optional delay strategy between attempts.",
    )


class RecoveryPolicy(BaseModel):
    """Crash-recovery rule for one logical Node execution.

    Recovery is deliberately a Node concern: the whole Node phase may be
    replayed after the last durable boundary, including mapping, all Operator
    calls, aggregation, and binding.  It is independent from ``RetryPolicy``,
    which only handles failures observed by a live NodeExecutor.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    mode: Literal["never", "replay_safe", "idempotent"] = Field(
        default="never",
        description=(
            "never interrupts recovery at this Node; replay_safe permits a "
            "whole-Node replay; idempotent additionally requires the executor "
            "to supply the stable logical Node idempotency key to external work."
        ),
    )
    max_attempts: int = Field(
        default=1,
        ge=1,
        description="Maximum crash-recovery replays for one logical Node occurrence.",
    )


class TimeoutPolicy(BaseModel):
    """Timeout rule for one node attempt."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    timeout_ms: int = Field(
        description="Maximum duration for one attempt, in milliseconds.",
    )


class ResourcePolicy(BaseModel):
    """Invocation-scoped resource limits for one workflow node.

    These limits apply to the same node_id inside one workflow Invocation.
    WorkflowExecutor checks node execution count before creating another
    NodeExecution. NodeExecutor checks Operator attempt count and accumulated
    runtime while executing the node.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    max_node_executions_per_invocation: int | None = Field(
        default=None,
        description=(
            "Maximum NodeExecution records allowed for this node_id in one "
            "Invocation. Used to stop runaway loops."
        ),
    )
    max_operator_attempts_per_invocation: int | None = Field(
        default=None,
        description=(
            "Maximum actual Operator handler attempts for this node_id in one "
            "Invocation. Parallel summaries contribute their attempt_count."
        ),
    )
    max_runtime_ms_per_invocation: int | None = Field(
        default=None,
        description=(
            "Maximum accumulated Operator handler time in milliseconds for this "
            "node_id in one Invocation. Mapping, binding, condition, aggregation, "
            "and retry backoff time are excluded."
        ),
    )


class ReplicationPolicy(BaseModel):
    """Run the same logical node input multiple times and aggregate outputs."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    count: int = Field(
        description="How many parallel handler units to run for one NodeExecution.",
    )
    output_aggregator: Callable[
        [ReplicationAggregationContext],
        Any | Awaitable[Any],
    ] | None = Field(
        default=None,
        description=(
            "Required aggregation function for replication. It receives one "
            "ReplicationAggregationContext whose replica_outputs contains all "
            "successful unit outputs, and returns the final "
            "NodeExecution.output consumed by downstream nodes. If any replica "
            "fails, remaining calls are cancelled when possible and this hook is "
            "not called."
        ),
    )
    max_parallelism: int | None = Field(
        default=None,
        description="Maximum replica handler units this node may run concurrently.",
    )


class NodePolicy(BaseModel):
    """Node-level scheduling and execution policy."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    selection: CapabilitySelectionPolicy | None = Field(
        default=None,
        description="Operator selection rule for capability refs.",
    )
    retry: RetryPolicy | None = Field(
        default=None,
        description="Retry rule for failed attempts.",
    )
    recovery: RecoveryPolicy | None = Field(
        default=None,
        description=(
            "Whole-Node crash recovery policy. Omitted is equivalent to "
            "RecoveryPolicy(mode='never')."
        ),
    )
    timeout: TimeoutPolicy | None = Field(
        default=None,
        description="Timeout rule for one attempt.",
    )
    resource: ResourcePolicy | None = Field(
        default=None,
        description="Resource limits for this node.",
    )
    replication: ReplicationPolicy | None = Field(
        default=None,
        description=(
            "Optional self-consistency/multi-sample policy. NodeExecutor runs "
            "the operator multiple times inside one logical NodeExecution and "
            "uses output_aggregator to produce the final output."
        ),
    )
    max_concurrency: int | None = Field(
        default=None,
        description=(
            "Maximum concurrent logical NodeExecutions for this node across "
            "sessions in one App process. MapPolicy/ReplicationPolicy "
            "max_parallelism separately limits internal handler units."
        ),
    )


class MapPolicy(BaseModel):
    """Fan out one selected edge over items derived from source node output."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    item_selector: Callable[
        [MapItemSelectionContext],
        Iterable[Mapping[str, Any]]
        | Awaitable[Iterable[Mapping[str, Any]]],
    ] | None = Field(
        default=None,
        description=(
            "Receives one MapItemSelectionContext and maps its input into "
            "iterable target operator argument mappings. Each selected mapping is copied to a dict and "
            "becomes one map item. When omitted, the source output "
            "must itself be an iterable of argument mappings."
        ),
    )
    output_aggregator: Callable[
        [MapAggregationContext],
        Any | Awaitable[Any],
    ] | None = Field(
        default=None,
        description=(
            "Receives one MapAggregationContext and aggregates item_outputs "
            "into the target NodeExecution.output. "
            "When omitted, outputs are collected into a list ordered by item "
            "index. If any item fails, remaining calls are cancelled when "
            "possible and this hook is not called."
        ),
    )
    max_parallelism: int | None = Field(
        default=None,
        description="Maximum map items to execute concurrently.",
    )


class EdgePolicy(BaseModel):
    """Edge-level data movement policy."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    map: MapPolicy | None = Field(
        default=None,
        description=(
            "Optional map/fan-out behavior. If present, this edge creates one "
            "target NodeExecution whose NodeExecutor performs multiple "
            "map item handler units and aggregates their outputs."
        ),
    )

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class FailurePolicy(BaseModel):
    """Workflow-level behavior for unhandled branch failures."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    mode: Literal["fail_fast", "finish_active"] = Field(
        default="fail_fast",
        description=(
            "fail_fast fails the run immediately. finish_active lets already "
            "active branches finish before the run reaches a final status."
        ),
    )


class WorkflowPolicy(BaseModel):
    """Workflow-level policy shared by the whole run."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    failure: FailurePolicy = Field(
        default_factory=FailurePolicy,
        description="Unhandled branch failure behavior.",
    )


class JoinPolicy(BaseModel):
    """Readiness rule for a node with multiple incoming edges."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    mode: Literal["all", "any", "n"] = Field(
        default="all",
        description="How many incoming dependencies must be satisfied.",
    )
    count: int | None = Field(
        default=None,
        description="Required incoming edge count when mode is n.",
    )


class RoutingPolicy(BaseModel):
    """Selection rule when multiple outgoing edges are satisfied."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    mode: Literal["all_satisfied", "first_satisfied", "exclusive"] = Field(
        default="all_satisfied",
        description="How to select satisfied outgoing edges.",
    )


class CapabilitySelectionPolicy(BaseModel):
    """Operator selection rule for abstract capability references."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    mode: Literal[
        "default",
        "priority",
        "lowest_cost",
        "lowest_latency",
        "highest_reliability",
        "first_available",
    ] = Field(
        default="default",
        description="How to choose an operator for a capability.",
    )
    allow_fallback: bool = Field(
        default=True,
        description="Whether execution may try another operator after failure.",
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
    """Retry rule for failed node attempts."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    max_attempts: int = Field(
        default=1,
        description="Maximum attempts for one node execution.",
    )
    backoff: BackoffPolicy | None = Field(
        default=None,
        description="Optional delay strategy between attempts.",
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
    NodeExecution. NodeExecutor checks operator call count and accumulated
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
    max_operator_calls_per_invocation: int | None = Field(
        default=None,
        description=(
            "Maximum concrete OperatorCall records allowed for this node_id in "
            "one Invocation. Retry, fallback, map, and replication all count."
        ),
    )
    max_runtime_ms_per_invocation: int | None = Field(
        default=None,
        description=(
            "Maximum accumulated runtime in milliseconds for this node_id in "
            "one Invocation."
        ),
    )


class TimerPolicy(BaseModel):
    """Delay rule applied before or after one logical node execution."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    delay_ms: int = Field(
        description="Delay duration in milliseconds.",
    )
    mode: Literal["blocking", "waiting", "auto"] = Field(
        default="auto",
        description=(
            "blocking sleeps inside the executor worker. waiting stores a "
            "WaitingExecution and relies on a timer service to resume later. "
            "auto lets executor/runtime choose, but compiler may warn when a "
            "long delay would block a worker."
        ),
    )
    position: Literal["before", "after"] = Field(
        default="before",
        description=(
            "before delays before operator call. after delays after "
            "operator completion but before the node transition is exposed to "
            "scheduler."
        ),
    )


class ReplicationPolicy(BaseModel):
    """Run the same logical node input multiple times and aggregate outputs."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    count: int = Field(
        description="How many OperatorCalls to create for one NodeExecution.",
    )
    output_aggregator: Callable[[list[Any]], Any] | None = Field(
        default=None,
        description=(
            "Required aggregation function for replication. It receives all "
            "successful OperatorCall outputs and returns the final "
            "NodeExecution.output consumed by downstream nodes."
        ),
    )
    max_parallelism: int | None = Field(
        default=None,
        description="Maximum replica OperatorCalls this node may run concurrently.",
    )


class NodePolicy(BaseModel):
    """Node-level scheduling and execution policy."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    join: JoinPolicy | None = Field(
        default=None,
        description="Readiness rule for incoming edges.",
    )
    routing: RoutingPolicy | None = Field(
        default=None,
        description="Selection rule for outgoing edges.",
    )
    selection: CapabilitySelectionPolicy | None = Field(
        default=None,
        description="Operator selection rule for capability refs.",
    )
    retry: RetryPolicy | None = Field(
        default=None,
        description="Retry rule for failed attempts.",
    )
    timeout: TimeoutPolicy | None = Field(
        default=None,
        description="Timeout rule for one attempt.",
    )
    resource: ResourcePolicy | None = Field(
        default=None,
        description="Resource limits for this node.",
    )
    timer: TimerPolicy | None = Field(
        default=None,
        description="Optional delay before or after this node execution.",
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
            "Maximum concurrent NodeExecutions or internal OperatorCalls "
            "allowed for this node. NodeExecutor combines this with operator "
            "limits and runtime global limits."
        ),
    )


class MapPolicy(BaseModel):
    """Fan out one selected edge over items derived from source node output."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    item_selector: Callable[[Any], Iterable[Any]] | None = Field(
        default=None,
        description=(
            "Maps source NodeExecution.output into iterable target operator "
            "inputs. When omitted, runtime treats the source output itself as "
            "the iterable. Each selected item becomes one map_item "
            "OperatorCall inside the target NodeExecution."
        ),
    )
    output_aggregator: Callable[[list[Any]], Any] | None = Field(
        default=None,
        description=(
            "Aggregates map item outputs into the target NodeExecution.output. "
            "When omitted, outputs are collected into a list ordered by item "
            "index."
        ),
    )
    max_parallelism: int | None = Field(
        default=None,
        description="Maximum map item OperatorCalls to execute concurrently.",
    )


class EdgePolicy(BaseModel):
    """Edge-level data movement policy."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    map: MapPolicy | None = Field(
        default=None,
        description=(
            "Optional map/fan-out behavior. If present, this edge creates one "
            "target NodeExecution whose NodeExecutor performs multiple "
            "map_item OperatorCalls and aggregates their outputs."
        ),
    )

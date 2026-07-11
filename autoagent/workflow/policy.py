from __future__ import annotations

from typing import Literal

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
    """Resource limits for node execution within one run."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    max_invocations: int | None = Field(
        default=None,
        description="Maximum node invocations in one run.",
    )
    max_tokens: int | None = Field(
        default=None,
        description="Maximum token usage in one run.",
    )
    max_cost: float | None = Field(
        default=None,
        description="Maximum cost in one run.",
    )
    max_tool_calls: int | None = Field(
        default=None,
        description="Maximum tool calls made by this node in one run.",
    )
    max_runtime_ms: int | None = Field(
        default=None,
        description="Maximum accumulated node runtime in one run.",
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

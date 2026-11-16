"""Application-owned Operator extensions and ordinary Core execution policies."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Literal, get_args

from autoagent.core import (
    Operator, Recovery, ResourcePolicy, RetryPolicy, TimeoutPolicy, WorkflowResourcePolicy,
)

from .models import (
    AgentResult, CompleteRoundInput, RoundResult,
)


AgentNodeId = Literal[
    "start", "prepare_context", "compact_context", "build_request", "llm_call",
    "tool_branch", "tool_execute", "wait_approval", "tool_execute_approved",
    "complete_round", "finish",
    "ask_user", "prepare_wait", "wait_tools", "collect_wait_results", "route_approved",
    "ask_user_approved", "prepare_wait_approved", "wait_tools_approved", "collect_wait_results_approved",
]

NODE_IDS = frozenset(get_args(AgentNodeId))


@dataclass(frozen=True, slots=True)
class AgentNodePolicy:
    """Override selected policies; None retains that Node's built-in default.

    Core validates which policies each executable supports. In particular,
    Wait Timeout is a durable deadline; Retry must explicitly select wait timeout errors.
    Explicit Retry can repeat side effects; Recovery controls crash replay
    independently and is never inferred from Retry.
    """

    timeout: TimeoutPolicy | None = None
    retry: RetryPolicy | None = None
    resource: ResourcePolicy | None = None
    recovery_mode: Recovery | None = None

    def __post_init__(self):
        for name, expected in (("timeout", TimeoutPolicy), ("retry", RetryPolicy),
                               ("resource", ResourcePolicy), ("recovery_mode", Recovery)):
            value = getattr(self, name)
            if value is not None and not isinstance(value, expected):
                raise TypeError(f"Agent Node {name} must be {expected.__name__} or None.")

    def apply(self, node):
        for name in ("timeout", "retry", "resource", "recovery_mode"):
            value = getattr(self, name)
            if value is not None:
                setattr(node, name, value)


@dataclass(frozen=True, slots=True)
class AgentWorkflowPolicies:
    """Optional Node and Workflow limits; Project limits remain Host settings."""

    nodes: Mapping[AgentNodeId, AgentNodePolicy] = field(default_factory=dict)
    resource: WorkflowResourcePolicy = field(default_factory=WorkflowResourcePolicy)

    def __post_init__(self):
        if not isinstance(self.nodes, Mapping):
            raise TypeError("Agent Node policies must be a mapping.")
        selected = dict(self.nodes)
        if set(selected) - NODE_IDS:
            raise ValueError(f"Unknown Agent Node policy IDs: {sorted(set(selected) - NODE_IDS)!r}.")
        if any(not isinstance(policy, AgentNodePolicy) for policy in selected.values()):
            raise TypeError("Agent Node policies must be AgentNodePolicy values.")
        if not isinstance(self.resource, WorkflowResourcePolicy):
            raise TypeError("Agent Workflow resource must be WorkflowResourcePolicy.")
        object.__setattr__(self, "nodes", MappingProxyType(selected))


@dataclass(frozen=True, slots=True)
class AgentWorkflowExtensions:
    """Replace completion Operators; Context behavior belongs to ContextPolicy.

    Custom Operators retain Core tracing and policies, defaulting to never Recovery.
    """

    complete_round: Operator | None = None
    finish: Operator | None = None

    def __post_init__(self):
        contracts = {
            "complete_round": (CompleteRoundInput, RoundResult),
            "finish": (RoundResult, AgentResult),
        }
        for name, (incoming, outgoing) in contracts.items():
            operator = getattr(self, name)
            if operator is None:
                continue
            if not isinstance(operator, Operator):
                raise TypeError(f"Agent {name} extension must be an Operator.")
            contract = operator.contract
            if (contract.stream_chunk is not None or contract.input.annotation is not incoming
                    or contract.output is None or contract.output.annotation is not outgoing):
                raise TypeError(f"Agent {name} must accept {incoming.__name__} and return {outgoing.__name__}.")


__all__ = ["AgentNodeId", "AgentNodePolicy", "AgentWorkflowPolicies", "AgentWorkflowExtensions"]

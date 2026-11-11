"""Portable Agent settings and a borrowed runtime Context interface."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from contextlib import contextmanager
import json
from typing import Literal, Protocol
from weakref import WeakKeyDictionary

from pydantic import BaseModel, Field, field_validator

from .models._base import Model, NonEmpty
from .tools.permissions import ToolDecision
from .tools.operations import ToolOperationRule
from .workspace import Workspace, WorkspaceBinding


ApprovalMode = Literal["policy", "always_ask", "never_ask", "bypass", "auto_approve"]
ToolAction = Literal["allow", "ask", "deny"]


class AgentModelSettings(Model):
    provider: NonEmpty = "default"
    model: NonEmpty | None = None
    streaming: bool = False
    max_output_tokens: int | None = Field(default=None, gt=0)
    temperature: float | None = Field(default=None, ge=0, le=2)


class AgentAccess(Model):
    """Host startup rules restrict, rather than replace, application Tool policies."""

    default: ToolAction = "allow"
    rules: dict[NonEmpty, ToolAction] = Field(default_factory=dict)
    allowed_tools: tuple[NonEmpty, ...] | None = None
    policy: NonEmpty | None = None
    operation_rules: tuple[ToolOperationRule, ...] = ()

    @field_validator("operation_rules", mode="before")
    @classmethod
    def _operation_rules_from_json(cls, value):
        return tuple(value) if isinstance(value, list) else value


class AgentHostSettings(Model):
    """Startup generation defaults and application-wide access boundaries."""

    model: AgentModelSettings = Field(default_factory=AgentModelSettings)
    approval_mode: ApprovalMode = "policy"
    access: AgentAccess = Field(default_factory=AgentAccess)
    workspace: WorkspaceBinding | None = None

    def session_defaults(self):
        return AgentSessionSettings(model=AgentModelSelection(provider=self.model.provider, model=self.model.model),
            approval_mode=self.approval_mode)


class AgentModelSelection(Model):
    provider: NonEmpty = "default"
    model: NonEmpty | None = None


class AgentSessionSettings(Model):
    """Optional explicit Session overrides; applications choose their storage."""

    model: AgentModelSelection = Field(default_factory=AgentModelSelection)
    approval_mode: ApprovalMode = "policy"


class AgentSettingsRecord(Model):
    session_id: NonEmpty
    version: int = Field(ge=0)
    settings: AgentSessionSettings
    workspace: WorkspaceBinding | None = None


class AgentExecutionSnapshot(Model):
    """Derived call context; only its model binding is immutable and durable.

    Approval and access are read from live Session preferences and Host
    defaults. This object is not a persisted Invocation settings snapshot.
    """

    snapshot_id: NonEmpty
    session_id: NonEmpty
    invocation_id: NonEmpty
    epoch: int = Field(ge=1)
    settings_version: int = Field(ge=0)
    settings: AgentHostSettings
    bindings: dict[str, str] = Field(default_factory=dict)
    reason: Literal["start", "resume", "recover"]
    created_at_us: int = Field(ge=0)


def apply_approval_mode(decision: ToolDecision, mode: ApprovalMode) -> ToolDecision:
    """Transform a combined policy decision, never overriding a Deny."""
    if mode not in ("policy", "always_ask", "never_ask", "bypass", "auto_approve"):
        raise ValueError(f"Unknown approval mode: {mode!r}.")
    if decision.action == "deny":
        return decision
    action = decision.action
    if mode == "always_ask":
        action = "ask"
    elif mode == "never_ask" and action == "ask":
        action = "deny"
    elif mode == "bypass" and action == "ask":
        action = "allow"
    reason = decision.reason
    if mode == "never_ask" and decision.action == "ask":
        reason = "Approval is required, but approval prompts are disabled."
    return ToolDecision(action=action, reason=reason)


def plain(value):
    """Detach immutable Core records before restoring strict JSON models."""
    if isinstance(value, Mapping):
        return {key: plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    return value


def restore(annotation, value):
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    return annotation.model_validate_json(json.dumps(plain(value), allow_nan=False))


class AgentContextProvider(Protocol):
    """Agent Operators borrow this service; Core never imports Host resources."""

    resources: object

    async def snapshot(self) -> AgentExecutionSnapshot: ...
    async def execution_snapshot(self, invocation_id: str) -> AgentExecutionSnapshot: ...
    async def resolve_selection(self, request): ...
    def history_route(self, snapshot: AgentExecutionSnapshot, model: str | None): ...
    def catalog(self, snapshot: AgentExecutionSnapshot): ...
    def finalize_catalog(self, snapshot: AgentExecutionSnapshot, catalog, skills): ...
    def borrow_tool(self, resource_id: str): ...
    async def export_binding(self, invocation_id: str): ...
    def resource_revision(self, snapshot: AgentExecutionSnapshot): ...
    def toolset(self, snapshot: AgentExecutionSnapshot, *, binding=None, catalog=None): ...
    async def prepare_workspace(self, snapshot: AgentExecutionSnapshot): ...
    async def resolve_workspace(self, binding: WorkspaceBinding) -> Workspace: ...


_contexts = WeakKeyDictionary()


@contextmanager
def agent_runtime_context(context: AgentContextProvider):
    loop = asyncio.get_running_loop()
    if loop in _contexts:
        raise RuntimeError("An Agent Context is already installed on this runtime loop.")
    _contexts[loop] = context
    try:
        yield context
    finally:
        _contexts.pop(loop, None)


def current_agent_context() -> AgentContextProvider:
    try:
        context = _contexts[asyncio.get_running_loop()]
        return context.for_call() if hasattr(context, "for_call") else context
    except (KeyError, RuntimeError):
        raise RuntimeError("Agent Operators require an installed Agent runtime Context.") from None


__all__ = [
    "ApprovalMode", "AgentAccess", "AgentModelSettings", "AgentModelSelection", "AgentHostSettings", "AgentSessionSettings",
    "AgentSettingsRecord", "AgentExecutionSnapshot", "AgentContextProvider",
    "apply_approval_mode", "agent_runtime_context", "current_agent_context",
]

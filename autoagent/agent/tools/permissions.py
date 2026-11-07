"""Portable decisions and application-supplied approval replies.

Replies are trusted application input, not credentials or model arguments.
The application authenticates approvers before resuming a Workflow Wait.
Approval does not provide tool idempotency or consume a durable grant.
"""

from __future__ import annotations

from typing import Literal

from pydantic import JsonValue
from pydantic import model_validator

from ..models import LLMToolCall, LLMToolResult
from ..models._base import Model, NonEmpty


class ToolApproval(Model):
    """A human/application reply for one exact approval request."""

    request_id: NonEmpty
    approved: bool


class ToolApprovalRequest(Model):
    """Serializable Wait input, including the normalized arguments to review."""

    id: NonEmpty
    call: LLMToolCall
    arguments: dict[str, JsonValue]
    user_id: str | None = None
    session_id: str | None = None
    reason: str | None = None

    def respond(self, *, approved: bool) -> ToolApproval:
        return ToolApproval(request_id=self.id, approved=approved)


class ToolDecision(Model):
    """Policy result; ToolSet binds an ask decision to an approval request."""

    action: Literal["allow", "deny", "ask"]
    reason: str | None = None
    approval_request: ToolApprovalRequest | None = None


class ToolApprovalRequired(RuntimeError):
    """Execution needs approval; expose the request without waiting in Python."""

    def __init__(self, request: ToolApprovalRequest):
        super().__init__(request.reason or "Tool execution requires approval.")
        self.request = request


class ToolPreflight(Model):
    """Preserve validation observations separately from permission decisions."""

    decision: ToolDecision | None = None
    result: LLMToolResult | None = None
    policy_action: Literal["allow", "ask", "deny"] | None = None

    @model_validator(mode="after")
    def _one_result(self):
        if (self.decision is None) == (self.result is None):
            raise ValueError("Preflight requires a decision or an error result.")
        return self


__all__ = ["ToolDecision", "ToolApproval", "ToolApprovalRequest", "ToolApprovalRequired", "ToolPreflight"]

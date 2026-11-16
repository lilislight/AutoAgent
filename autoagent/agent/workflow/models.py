"""Durable contracts for the general Agent Workflow."""

from typing import Literal
from uuid import uuid4
from pydantic import Field, JsonValue, model_validator

from ..models import LLMItem, LLMRequest, LLMResponse, LLMToolCall, LLMToolResult, LLMToolDefinition
from .history import Conversation, ContextView, HistoryRoute
from ..models._base import Model, NonEmpty
from ..context import AgentModelSelection, AgentHostSettings
from ..tools import ToolApproval, ToolApprovalRequest
from .subagents import SubagentsConfig
from .events import AgentEventMessage


class AgentInput(Model):
    message: NonEmpty
    request_id: NonEmpty | None = None
    model: AgentModelSelection | None = None


class AgentWorkflowOptions(Model):
    workflow_id: NonEmpty = "agent"
    instructions: str = "You are a helpful assistant. Use the available tools when needed."
    llm_options: dict[str, JsonValue] = Field(default_factory=dict)
    runtime: AgentHostSettings | None = None
    max_rounds: int | None = Field(default=10_000, ge=1)
    max_parallelism: int | None = Field(default=None, ge=1)
    include_ask_user: bool = False
    inject_current_time: bool = False
    user_events: bool = False
    user_signal_endpoint: NonEmpty = "agent.user"
    subagents: SubagentsConfig = Field(default_factory=SubagentsConfig)

    @model_validator(mode="after")
    def _endpoints(self):
        if self.user_signal_endpoint == self.subagents.signal_endpoint:
            raise ValueError("User and collaboration Signal Endpoints must have different names.")
        return self


class ToolBinding(Model):
    resource_id: NonEmpty
    name: NonEmpty
    version: NonEmpty
    definition_hash: NonEmpty
    kind: Literal["tool", "ask_user", "agent_builtin"] = "tool"


class ContextPreparation(Model):
    conversation: Conversation
    fixed_input: tuple[LLMItem, ...] = ()
    tools: tuple[LLMToolDefinition, ...] = ()
    bindings: tuple[ToolBinding, ...] = ()
    settings: dict[str, JsonValue] = Field(default_factory=dict)
    generation_options: dict[str, JsonValue] = Field(default_factory=dict)
    compaction_model: NonEmpty | None = None
    compaction_provider: NonEmpty | None = None
    compaction_reasoning_effort: NonEmpty | None = None
    fingerprint: NonEmpty
    snapshot_id: NonEmpty
    replay_target: HistoryRoute | None = None
    inject_current_time: bool = False

    @property
    def model_input(self) -> tuple[LLMItem, ...]:
        from .projection import project_history, project_items
        return (*project_items(self.fixed_input, self.replay_target),
                *project_history(self.conversation, self.replay_target))


class TokenEstimate(Model):
    input_tokens: int = Field(ge=0)
    anchored: bool = False


class ContextPlan(Model):
    preparation: ContextPreparation
    estimate: TokenEstimate
    needs_compaction: bool = False
    base_generation: int = Field(ge=0)
    received_messages: tuple[AgentEventMessage, ...] = ()


class PreparedRequest(Model):
    request_id: NonEmpty = Field(default_factory=lambda: uuid4().hex)
    request: LLMRequest
    tools: tuple[ToolBinding, ...] = ()
    view: ContextView
    fingerprint: NonEmpty
    source_count: int = Field(ge=0)
    base_generation: int = Field(ge=0)
    fixed_input: tuple[LLMItem, ...] = ()
    replay_target: HistoryRoute | None = None
    estimated_input_tokens: int | None = Field(default=None, ge=0)
    runtime_time: NonEmpty | None = None


class CompactionChunk(Model):
    type: Literal["started", "completed"]
    plan: ContextPlan | None = None


class CompactionState(Model):
    plan: ContextPlan | None = None


class ToolTask(Model):
    request_id: NonEmpty
    call: LLMToolCall
    binding: ToolBinding
    approval_request: ToolApprovalRequest | None = None
    approval: ToolApproval | None = None
    observation: LLMToolResult | None = None
    observed_versions: dict[str, str] = Field(default_factory=dict)
    operations: tuple[dict[str, JsonValue], ...] = ()


class ToolBranchInput(Model):
    response: LLMResponse
    prepared: PreparedRequest


class ApprovedTools(Model):
    tasks: tuple[ToolTask, ...]


class AuthorizationObservation(Model):
    call_id: NonEmpty
    approval_mode: str
    policy_action: str | None = None
    effective_action: str


class ToolBranchResult(Model):
    allow: tuple[ToolTask, ...] = ()
    ask: tuple[ToolTask, ...] = ()
    questions: tuple[ToolTask, ...] = ()
    waits: tuple[ToolTask, ...] = ()
    immediate: tuple[LLMToolResult, ...] = ()
    decisions: tuple[AuthorizationObservation, ...] = ()


class AgentApprovalResolution(Model):
    """Host-produced resolution for one member of the original durable batch."""

    request_id: NonEmpty
    source: Literal["user", "policy", "system"]
    approval: ToolApproval | None = None
    result: LLMToolResult | None = None

    @model_validator(mode="after")
    def _one_result(self):
        if self.approval is not None and self.result is not None:
            raise ValueError("An approval resolution cannot also contain a Tool result.")
        return self


class AgentApprovalRequest(Model):
    kind: Literal["agent.tool_approval"] = "agent.tool_approval"
    request_id: NonEmpty
    invocation_id: NonEmpty
    snapshot_id: NonEmpty
    requests: tuple[ToolApprovalRequest, ...]
    bindings: tuple[ToolBinding, ...]
    revision: NonEmpty | None = None
    automatic: tuple[AgentApprovalResolution, ...] = ()

    @model_validator(mode="after")
    def _bindings(self):
        if len(self.bindings) != len(self.requests) or any(
            binding.name != request.call.name for request, binding in zip(self.requests, self.bindings)
        ) or len({request.id for request in self.requests}) != len(self.requests):
            raise ValueError("Approval requests require one matching binding per unique request.")
        return self


class AgentApprovalResponse(Model):
    decisions: tuple[ToolApproval, ...]
    source: Literal["user", "policy", "system"] = "user"
    revision: NonEmpty | None = None
    resolutions: tuple[AgentApprovalResolution, ...] = ()

    @model_validator(mode="after")
    def _unique(self):
        ids = [value.request_id for value in self.decisions]
        if len(ids) != len(set(ids)):
            raise ValueError("Approval replies contain duplicate request IDs.")
        return self

    def validate_request(self, request: AgentApprovalRequest):
        expected = {value.id for value in request.requests}
        received = self.resolutions or self.decisions
        if len(received) != len(expected) or {value.request_id for value in received} != expected:
            raise ValueError("Approval replies must match the complete pending batch.")


class CompleteRoundInput(Model):
    response: LLMResponse
    results: tuple[LLMToolResult, ...] = ()
    previous_rounds: int = 0


class RoundResult(Model):
    response: LLMResponse
    results: tuple[LLMToolResult, ...] = ()
    continue_loop: bool
    rounds: int


class AgentResult(Model):
    """Deliverable content only; execution details belong to records/events."""

    content: tuple[dict[str, JsonValue], ...] = ()

    @property
    def text(self) -> str:
        return "".join(part["text"] for part in self.content if part.get("type") in {"text", "refusal"})

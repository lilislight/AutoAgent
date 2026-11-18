"""Configuration and portable contracts for built-in Agent collaboration."""

from typing import Literal

from pydantic import Field, JsonValue, model_validator

from ..models._base import Model, NonEmpty
from ..models import LLMToolResult
from autoagent.core.commands import SelectRequest
from .message import AgentUserMessage


class AgentSignalMessage(AgentUserMessage):
    """The same payload contract is used by user and collaboration Endpoints.

    Runtime message identity and sender identity come from Signal delivery,
    never from an untrusted payload.
    """

    reply_requested: bool = False
    reply_to: NonEmpty | None = None
    wait_timeout_seconds: float | None = Field(default=None, gt=0)


class SubagentTarget(Model):
    workflow_id: NonEmpty
    entry_node_id: NonEmpty = "start"
    workflow_revision_id: NonEmpty | None = None
    description: NonEmpty = "Execute the delegated task and return its result."


class SubagentsConfig(Model):
    enabled: bool = False
    targets: dict[NonEmpty, SubagentTarget] = Field(default_factory=dict)
    signal_endpoint: NonEmpty = "agent.messages"
    include_parent: bool = True
    include_siblings: bool = False
    default_wait_seconds: float = Field(default=30.0, gt=0)
    max_wait_seconds: float = Field(default=120.0, gt=0)

    @model_validator(mode="after")
    def _limits(self):
        if self.default_wait_seconds > self.max_wait_seconds:
            raise ValueError("Default Agent wait must fit within max_wait_seconds.")
        return self

    def wait_seconds(self, seconds=None):
        value = self.default_wait_seconds if seconds is None else seconds
        if isinstance(value, bool) or value <= 0 or value > self.max_wait_seconds:
            raise ValueError("Wait duration must be positive and within max_wait_seconds.")
        return float(value)


class AgentObservation(Model):
    agent_id: NonEmpty
    relation: Literal["self", "parent", "child", "sibling"]
    workflow_id: NonEmpty
    status: Literal["created", "running", "waiting", "settling", "completed", "failed", "cancelled"]
    result: JsonValue = None
    error: JsonValue = None
    waits: tuple[dict[str, JsonValue], ...] = ()


class DeferredToolWait(Model):
    call_id: NonEmpty
    request: SelectRequest
    receipt: dict[str, JsonValue] = Field(default_factory=dict)
    agent_ids: tuple[NonEmpty, ...] = ()
    relations: dict[str, Literal["parent", "child", "sibling"]] = Field(default_factory=dict)


class WaitPreparation(Model):
    result: LLMToolResult | None = None
    wait: DeferredToolWait | None = None

    @model_validator(mode="after")
    def _one(self):
        if (self.result is None) == (self.wait is None):
            raise ValueError("Preparation produces one Tool result or deferred wait.")
        return self


class WaitPreparationBatch(Model):
    results: tuple[LLMToolResult, ...] = ()
    waits: tuple[DeferredToolWait, ...] = ()


class CompletedToolWait(Model):
    wait: DeferredToolWait
    selected_case: NonEmpty


class CompletedToolWaitBatch(Model):
    waits: tuple[CompletedToolWait, ...]


async def collect_tool_wait(value: CompletedToolWait) -> LLMToolResult:
    """Collect all newly reportable targets after one native Select wakes."""
    from autoagent.core.commands import CommandInvoker, Status
    commands = CommandInvoker.current()
    wait = value.wait
    reason = value.selected_case.split(":", 1)[0]
    agents, versions = [], {}
    for index, agent_id in enumerate(wait.agent_ids):
        case = wait.request.cases["agent:" + agent_id]
        raw = (await commands.execute(Status(), case.handle, key="collect:" + str(index))).target_only()
        reportable = raw.status in {"completed", "failed", "cancelled"} or (
            raw.status in {"waiting", "settling"} and raw.version != case.after)
        if reportable or value.selected_case == "agent:" + agent_id:
            agents.append(project_observation(raw, wait.relations[agent_id]))
            versions[agent_id] = raw.version
    return LLMToolResult(call_id=wait.call_id,
        structured_content={**wait.receipt, "wake_reason": reason, "agents": agents},
        tool_metadata={"agent_observed_versions": versions})


BUILTIN_NAMES = frozenset({"spawn_agent", "run_agent", "get_agents", "send_message", "cancel_agent", "wait_for"})


def project_observation(raw, relation):
    """Project only this target's own waits, without leaking its descendants."""
    result = AgentObservation(agent_id=raw.handle.invocation_id, relation=relation,
        workflow_id=raw.handle.workflow_id, status=raw.status, result=raw.output, error=raw.error,
        waits=tuple(wait.model_dump(mode="json") for wait in raw.waits if wait.handle == raw.handle)).model_dump(mode="json", exclude_none=True)
    if not result["waits"]:
        result.pop("waits")
    return result


__all__ = ["SubagentsConfig", "SubagentTarget", "AgentSignalMessage", "AgentObservation"]

"""LLM-facing collaboration Tools; Runtime resources are borrowed per call."""

from functools import lru_cache
import hashlib
from typing import Annotated

from pydantic import Field, JsonValue

from ..context import current_agent_context
from ..models._base import NonEmpty
from ..resources import AgentResource
from ..tools import Tool
from .questions import with_ask_user


PositiveSeconds = Annotated[float, Field(gt=0)]


async def spawn_agent(type: NonEmpty, task: NonEmpty) -> dict[str, JsonValue]:
    """Start an allowed Agent type with a self-contained task. Return immediately with its agent_id and status."""
    return await current_agent_context().collaboration.spawn(type, task, wait=False)


async def run_agent(type: NonEmpty, task: NonEmpty,
                    wait_timeout_seconds: PositiveSeconds | None = None) -> dict[str, JsonValue]:
    """Start an allowed Agent and wait for its reportable state, an incoming message, or a finite deadline. The Agent may still be running when this returns."""
    return await current_agent_context().collaboration.spawn(type, task, wait=True, seconds=wait_timeout_seconds)


async def get_agents(agent_ids: list[NonEmpty] | None = None) -> dict[str, JsonValue]:
    """Inspect visible parent, direct children and permitted siblings. Omit agent_ids to list all visible Agents. Results always contain an agents list."""
    return await current_agent_context().collaboration.get(agent_ids)


async def send_message(target: NonEmpty, message: NonEmpty, reply_requested: bool = False,
                       reply_to: NonEmpty | None = None,
                       wait_timeout_seconds: PositiveSeconds | None = None) -> dict[str, JsonValue]:
    """Send a message to a visible Agent. Set reply_requested when a reply is needed and state what to answer in message. Use reply_to when replying to a received message_id. Optional waiting ends on any unread incoming message or the deadline; waking does not imply a reply. Messages enter Context automatically."""
    return await current_agent_context().collaboration.send(target, message, reply_requested, reply_to, wait_timeout_seconds)


async def cancel_agent(agent_id: NonEmpty, reason: NonEmpty | None = None) -> dict[str, JsonValue]:
    """Request cancellation of a directly owned Agent. The receipt acknowledges the request, not completion of cancellation."""
    return await current_agent_context().collaboration.cancel(agent_id, reason)


async def wait_for(timeout_seconds: PositiveSeconds, agent_ids: list[NonEmpty] | None = None,
                   wake_on_message: bool = True) -> dict[str, JsonValue]:
    """Wait for any selected Agent's new reportable state, any unread incoming message, or timeout_seconds. Return all selected Agents already completed or newly reportable when waking; remaining Agents may still be running. Does not consume messages. With no Agents and wake_on_message=false this is a finite sleep. Timeout is a normal result."""
    return await current_agent_context().collaboration.wait(timeout_seconds, agent_ids, wake_on_message)


@lru_cache(maxsize=128)
def _resources(config_json):
    from .subagents import SubagentsConfig
    config = SubagentsConfig.model_validate_json(config_json)
    version = "agent.collaboration.3:" + hashlib.sha256(config_json.encode()).hexdigest()
    targets = "\nAllowed Agent types:\n" + "\n".join(
        name + ": " + target.description for name, target in config.targets.items())
    handlers = [get_agents, send_message, cancel_agent, wait_for]
    if config.targets:
        handlers[:0] = [spawn_agent, run_agent]
    result = {}
    for handler in handlers:
        description = handler.__doc__ + (targets if handler in (spawn_agent, run_agent) else "")
        tool = Tool(handler, description=description)
        result[tool.definition.name] = AgentResource(tool.definition.name, version, tool,
            resource_id="builtin:collaboration/" + tool.definition.name)
    return result


def workflow_catalog(service, snapshot, options=None):
    options = options or service.workflow_options(snapshot)
    catalog, skills = service.catalog(snapshot)
    if options.include_ask_user:
        catalog = with_ask_user(catalog)
    if options.subagents.enabled:
        resources = _resources(options.subagents.model_dump_json())
        if set(resources) & set(catalog):
            raise ValueError("A registered Tool collides with an enabled Agent builtin.")
        catalog = {**catalog, **resources}
    return service.finalize_catalog(snapshot, catalog, skills)

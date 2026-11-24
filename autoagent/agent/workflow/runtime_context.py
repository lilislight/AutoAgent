"""Hidden, append-only clock snapshots at model request boundaries."""

from datetime import datetime

from autoagent.core import StreamContext
from ..integration.llm import LLMStreamReducer, LLMStreamState
from ..models import LLMMessage, LLMResponse, TextContent


TIME_KEY = "_autoagent_runtime_time"
PLAN_KEY = "_autoagent_runtime_plan"
_ESTIMATE_TIME = "2000-01-01T00:00:00+00:00"


def server_time():
    """Read the server's local clock with an explicit UTC offset and seconds."""
    return datetime.now().astimezone().isoformat(timespec="seconds")


def time_message(timestamp, target=None):
    role = target.context_role if target is not None else "user"
    return LLMMessage(role=role, content=(TextContent(text=(
        "Runtime context (not a user request):\n"
        f"Current server time: {timestamp}\n"
        "Use the latest runtime time record. Continue the existing task; do not acknowledge this record."
    )),))


def time_characters(preparation):
    from .policy import item_characters
    return item_characters(time_message(_ESTIMATE_TIME, preparation.replay_target)) if preparation.inject_current_time else 0


def runtime_characters(preparation):
    from .policy import item_characters
    return time_characters(preparation) + (item_characters(plan_message(
        preparation.conversation.plan, preparation.replay_target)) if preparation.inject_plan else 0)


def append_time(request, target=None):
    """Append exactly one fresh snapshot without changing any preceding input."""
    timestamp = server_time()
    return request.model_copy(update={
        "input": (*request.input, time_message(timestamp, target)),
        "routing": {**request.routing, TIME_KEY: timestamp},
    })


def plan_message(plan, target=None):
    import json
    value = plan.model_dump(mode="json") if plan is not None else None
    return LLMMessage(role=target.context_role if target is not None else "user", content=(TextContent(text=(
        "Runtime plan snapshot (not a user request):\n"
        + json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        + "\nThis is the current Session plan; it supersedes historical plan snapshots. "
          "Null means no current plan. Continue the user's task; do not acknowledge this record. "
          "Update the plan at meaningful milestones using update_plan. Completed plans remain until replaced."
    )),))


def append_plan(request, plan, target=None):
    return request.model_copy(update={"input": (*request.input, plan_message(plan, target)),
        "routing": {**request.routing, PLAN_KEY: plan.model_dump(mode="json") if plan else None}})


class AgentLLMStreamReducer(LLMStreamReducer):
    """Commit the clock actually sent, including after a pause or Node retry."""

    def finish(self, context: StreamContext, state: LLMStreamState) -> LLMResponse:
        response = super().finish(context, state)
        snapshots = {key: context.input.routing[key] for key in (TIME_KEY, PLAN_KEY) if key in context.input.routing}
        return response.model_copy(update={"provider_metadata": {**response.provider_metadata, **snapshots}}) if snapshots else response

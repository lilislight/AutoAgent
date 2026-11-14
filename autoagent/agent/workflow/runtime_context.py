"""Hidden, append-only clock snapshots at model request boundaries."""

from datetime import datetime

from autoagent.core import StreamContext
from ..integration.llm import LLMStreamReducer, LLMStreamState
from ..models import LLMMessage, LLMResponse, TextContent


TIME_KEY = "_autoagent_runtime_time"
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
    if not preparation.inject_current_time:
        return 0
    from .policy import item_characters
    return item_characters(time_message(_ESTIMATE_TIME, preparation.replay_target))


def append_time(request, target=None):
    """Append exactly one fresh snapshot without changing any preceding input."""
    timestamp = server_time()
    return request.model_copy(update={
        "input": (*request.input, time_message(timestamp, target)),
        "routing": {**request.routing, TIME_KEY: timestamp},
    })


class AgentLLMStreamReducer(LLMStreamReducer):
    """Commit the clock actually sent, including after a pause or Node retry."""

    def finish(self, context: StreamContext, state: LLMStreamState) -> LLMResponse:
        response = super().finish(context, state)
        timestamp = context.input.routing.get(TIME_KEY)
        if timestamp is None:
            return response
        return response.model_copy(update={"provider_metadata": {
            **response.provider_metadata, TIME_KEY: timestamp,
        }})

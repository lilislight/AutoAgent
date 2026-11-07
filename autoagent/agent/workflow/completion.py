"""Default completion stages, reusable by application Operator extensions."""

from ..models import LLMMessage
from .models import AgentResult, CompleteRoundInput, RoundResult


def complete_agent_round(value: CompleteRoundInput) -> RoundResult:
    """Validate one complete batch and restore the model's Tool call order."""
    calls = value.response.tool_calls
    if not calls and value.response.finish_reason != "stop":
        raise ValueError("The model did not return a complete final response.")
    expected = [call.call_id for call in calls]
    received = [result.call_id for result in value.results]
    if len(expected) != len(set(expected)) or len(received) != len(set(received)) or set(expected) != set(received):
        raise ValueError("Every Tool call must have exactly one matching Tool result.")
    by_id = {result.call_id: result for result in value.results}
    if not calls and not any(item.type == "provider_item" or (
        isinstance(item, LLMMessage) and item.role == "assistant" and any(
            content.type != "text" or content.text.strip() for content in item.content))
        for item in value.response.output):
        raise ValueError("The model returned neither Tool calls nor a final response.")
    return RoundResult(response=value.response, results=tuple(by_id[identity] for identity in expected),
        continue_loop=bool(calls), rounds=value.previous_rounds + 1)


def finish_agent_round(value: RoundResult) -> AgentResult:
    """Expose the final model response and completed round count."""
    return AgentResult(response=value.response, rounds=value.rounds)


__all__ = ["complete_agent_round", "finish_agent_round"]

"""Core streaming reduction for Provider-backed LLM Operators."""

from __future__ import annotations

from autoagent.core.workflow import StreamContext

from ..models import LLMResponse
from ..models._base import Model
from ..models.streaming import LLMStreamChunk


class LLMStreamState(Model):
    response: LLMResponse | None = None


class LLMStreamReducer:
    """Use the terminal response as Node output without retaining delta history."""

    def initial(self, context: StreamContext) -> LLMStreamState:
        return LLMStreamState()

    def add(
        self, context: StreamContext, state: LLMStreamState, chunk: LLMStreamChunk,
    ) -> LLMStreamState:
        if state.response is not None:
            raise ValueError("LLM stream emitted data after its terminal response.")
        if chunk.type == "completed":
            return LLMStreamState(response=chunk.response)
        return state

    def finish(self, context: StreamContext, state: LLMStreamState) -> LLMResponse:
        if state.response is None:
            raise ValueError("LLM stream ended without a terminal response.")
        return state.response


__all__ = ["LLMStreamReducer", "LLMStreamState"]

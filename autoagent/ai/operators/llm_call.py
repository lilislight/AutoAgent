from __future__ import annotations

from typing import TYPE_CHECKING

from autoagent.ai.capabilities.llm_call import (
    LLM_CALL_CAPABILITY_ID,
    LLM_CALL_CONTRACT,
    LLMCallMode,
)
from autoagent.ai.models.llm import LLMRequest, LLMResponse, LLMStreamChunk
from autoagent.ai.providers.base import LLMProvider, LLMProviderError
from autoagent.core.operators import (
    Operator,
    StreamingResult,
    streaming_result,
)

if TYPE_CHECKING:
    from autoagent.core.app import AutoAgentApp


def create_llm_call_operator(
    provider: LLMProvider,
    *,
    operator_id: str = "llm_call.default",
) -> Operator:
    """Adapt one LLM Provider to the llm_call Capability."""

    async def handler(
        request: LLMRequest,
        mode: LLMCallMode = "invoke",
    ) -> (
        LLMResponse
        | StreamingResult[LLMStreamChunk, LLMResponse]
    ):
        if mode == "invoke":
            return await provider.ainvoke(request)
        return streaming_result(
            provider.astream(request),
            reducer=_LLMStreamReducer(provider.provider_name),
        )

    return Operator(
        id=operator_id,
        handler=handler,
        capability_id=LLM_CALL_CAPABILITY_ID,
    )


def register_llm_call_operator(
    app: AutoAgentApp,
    provider: LLMProvider,
    *,
    operator_id: str = "llm_call.default",
    default: bool = True,
) -> Operator:
    """Register the llm_call Capability and one Provider-backed Operator."""

    if not app.capability_registry.contains(LLM_CALL_CAPABILITY_ID):
        app.register_capability(
            LLM_CALL_CAPABILITY_ID,
            contract=LLM_CALL_CONTRACT,
            description="Perform one provider-neutral language model call.",
        )
    operator = create_llm_call_operator(
        provider,
        operator_id=operator_id,
    )
    return app.operator_registry.register(operator, default=default)


class _LLMStreamReducer:
    """Extract the Provider-normalized terminal response without retaining deltas."""

    def __init__(self, provider_name: str) -> None:
        self.provider_name = provider_name
        self.completed: LLMResponse | None = None

    def add(self, chunk: LLMStreamChunk) -> None:
        if chunk.type == "completed":
            if self.completed is not None:
                raise LLMProviderError(
                    "LLM Provider stream emitted more than one completed response.",
                    provider=self.provider_name,
                    retryable=False,
                )
            self.completed = chunk.response

    def finish(self) -> LLMResponse:
        if self.completed is None:
            raise LLMProviderError(
                "LLM Provider stream ended without a completed response.",
                provider=self.provider_name,
                retryable=False,
            )
        return self.completed

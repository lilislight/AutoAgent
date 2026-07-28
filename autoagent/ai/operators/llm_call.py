from __future__ import annotations

from typing import TYPE_CHECKING

from autoagent.ai.capabilities.llm_call import (
    LLM_CALL_CAPABILITY_ID,
    LLM_CALL_CONTRACT,
    LLMCallMode,
)
from autoagent.ai.models.llm import LLMRequest, LLMResponse
from autoagent.ai.providers.base import LLMProvider, LLMProviderError
from autoagent.core.operators import Operator

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
    ) -> LLMResponse:
        if mode == "invoke":
            return await provider.ainvoke(request)
        completed: LLMResponse | None = None
        async for chunk in provider.astream(request):
            if chunk.type == "completed":
                completed = chunk.response
        if completed is None:
            raise LLMProviderError(
                "LLM Provider stream ended without a completed response.",
                provider=provider.provider_name,
                retryable=False,
            )
        return completed

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

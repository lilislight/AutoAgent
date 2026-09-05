from __future__ import annotations

from typing import Literal

from autoagent.ai.models.llm import LLMRequest, LLMResponse
from autoagent.core.operators import Capability, OperatorContract
from autoagent.core.operators.contract import callable_contract


LLM_CALL_CAPABILITY_ID = "llm_call"
LLMCallMode = Literal["invoke", "stream"]


def _llm_call_contract(
    request: LLMRequest,
    mode: LLMCallMode = "invoke",
) -> LLMResponse:
    raise NotImplementedError


LLM_CALL_CONTRACT: OperatorContract = callable_contract(_llm_call_contract)[0]
LLM_CALL_CAPABILITY = Capability(
    id=LLM_CALL_CAPABILITY_ID,
    description="Perform one provider-neutral language model call.",
    contract=LLM_CALL_CONTRACT,
)

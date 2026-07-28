from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, AsyncIterator
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


_LLM_STREAM_DELTA_TARGET_CHARS = 32


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
            _coalesce_llm_stream(provider.astream(request)),
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


async def _coalesce_llm_stream(
    source: AsyncIterable[LLMStreamChunk],
) -> AsyncIterator[LLMStreamChunk]:
    """Coalesce adjacent small LLM deltas before generic stream execution.

    Provider streams remain unchanged for direct Provider users. Only the
    framework's ``llm_call`` Operator applies this transport optimization.
    Completed responses and transitions between delta kinds are boundaries, so
    externally observable ordering and final reduction semantics stay intact.
    """

    iterator = source.__aiter__()
    pending: LLMStreamChunk | None = None
    failed = False
    try:
        while True:
            try:
                chunk = await iterator.__anext__()
            except StopAsyncIteration:
                break
            if chunk.type == "completed":
                if pending is not None:
                    yield pending
                    pending = None
                yield chunk
                continue
            if pending is None:
                pending = chunk
            elif _can_merge_llm_deltas(pending, chunk):
                pending = _merge_llm_deltas(pending, chunk)
            else:
                yield pending
                pending = chunk
            if _llm_delta_char_count(pending) >= _LLM_STREAM_DELTA_TARGET_CHARS:
                yield pending
                pending = None
        if pending is not None:
            yield pending
            pending = None
    except asyncio.CancelledError:
        failed = True
        raise
    except Exception:
        failed = True
        if pending is not None:
            yield pending
        raise
    finally:
        aclose = getattr(iterator, "aclose", None)
        if callable(aclose):
            try:
                await aclose()
            except Exception:
                if not failed:
                    raise


def _can_merge_llm_deltas(
    left: LLMStreamChunk,
    right: LLMStreamChunk,
) -> bool:
    if left.type != right.type or left.type == "completed":
        return False
    if left.type != "tool_call_delta":
        return True
    return (
        left.tool_call_index == right.tool_call_index
        and (
            left.tool_call_id is None
            or right.tool_call_id is None
            or left.tool_call_id == right.tool_call_id
        )
        and (
            left.tool_name is None
            or right.tool_name is None
            or left.tool_name == right.tool_name
        )
    )


def _merge_llm_deltas(
    left: LLMStreamChunk,
    right: LLMStreamChunk,
) -> LLMStreamChunk:
    if left.type == "text_delta":
        return LLMStreamChunk(
            type="text_delta",
            text_delta=(left.text_delta or "") + (right.text_delta or ""),
        )
    if left.type == "reasoning_delta":
        return LLMStreamChunk(
            type="reasoning_delta",
            reasoning_delta=(
                (left.reasoning_delta or "") + (right.reasoning_delta or "")
            ),
        )
    return LLMStreamChunk(
        type="tool_call_delta",
        tool_call_index=left.tool_call_index,
        tool_call_id=left.tool_call_id or right.tool_call_id,
        tool_name=left.tool_name or right.tool_name,
        tool_arguments_delta=(
            (left.tool_arguments_delta or "")
            + (right.tool_arguments_delta or "")
        ),
    )


def _llm_delta_char_count(chunk: LLMStreamChunk) -> int:
    if chunk.type == "text_delta":
        return len(chunk.text_delta or "")
    if chunk.type == "reasoning_delta":
        return len(chunk.reasoning_delta or "")
    if chunk.type == "tool_call_delta":
        return len(chunk.tool_arguments_delta or "")
    return 0


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

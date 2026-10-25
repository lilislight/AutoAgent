"""Weather dialogue state and Workflow hooks; no Provider setup or agent loop."""

from collections.abc import Mapping
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field

from autoagent import (ConditionContext, ContextOperation, ContextPatch,
                       InputMappingContext, OutputBindingContext)
from autoagent.agent import LLMMessage, LLMRequest, LLMResponse, LLMToolCall, LLMToolResult, TextContent
from autoagent.host import llm_request
from .tools import weather_tools


class Question(BaseModel):
    model_config = ConfigDict(extra="forbid")
    question: str = Field(min_length=1, pattern=r"\S")


class Answer(BaseModel):
    model_config = ConfigDict(extra="forbid")
    answer: str
    model: str
    model_calls: int
    weather_source: Literal["simulated"] = "simulated"


SYSTEM_PROMPT = """你是天气查询演示助手。请用用户的语言回答。
天气和城市信息只能来自提供的工具。所有天气均为固定模拟数据，回答必须明确说明“模拟天气”，
不能声称查询了真实或实时天气。先用 resolve_city 获取城市 ID，再用 get_weather 查询天气。
可用 list_cities 查看支持的城市。支持今天(today)和明天(tomorrow)，未指定日期时查询今天。
不支持的城市/日期应明确说明限制，工具报错可修正参数或说明原因，不能编造数据。
涉及多个城市时逐一查询后比较。只给出简洁的结论及出行建议，不输出内部推理。"""


MAX_MODEL_CALLS = 6


def record(value):
    """Detach read-only hook records before restoring strict SDK models."""
    if isinstance(value, Mapping):
        return {key: record(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [record(item) for item in value]
    return value


def prepare(value: Question) -> LLMRequest:
    return llm_request(
        input=(LLMMessage(role="system", content=(TextContent(text=SYSTEM_PROMPT),)),
               LLMMessage(role="user", content=(TextContent(text=value.question),))),
        tools=weather_tools().definitions,
        # options={"thinking": {"type": "disabled"}},
    )


def prepared(context: OutputBindingContext) -> ContextPatch:
    return ContextPatch(invocation=(ContextOperation.set("request", record(context.output)),
                                    ContextOperation.set("model_calls", 0)))


def model_input(context: InputMappingContext) -> LLMRequest:
    return LLMRequest.model_validate(record(context.invocation_context["request"]), strict=False)


def model_output(context: OutputBindingContext) -> ContextPatch:
    response = LLMResponse.model_validate(record(context.output), strict=False)
    if response.finish_reason not in {"stop", "tool_calls"}:
        raise RuntimeError(f"Incomplete LLM response: {response.finish_reason}.")
    if not response.tool_calls and not response.text.strip():
        raise RuntimeError("LLM returned neither an answer nor Tool calls.")
    number = context.invocation_context["model_calls"] + 1
    if response.tool_calls and number >= MAX_MODEL_CALLS:
        raise RuntimeError(f"Weather assistant exceeded {MAX_MODEL_CALLS} model calls.")
    request = LLMRequest.model_validate(record(context.invocation_context["request"]), strict=False)
    request = request.model_copy(update={"input": (*request.input, *response.output)})
    return ContextPatch(invocation=(ContextOperation.set("request", request.model_dump(mode="json")),
                                    ContextOperation.set("response", response.model_dump(mode="json")),
                                    ContextOperation.set("model_calls", number)))


def needs_tools(context: ConditionContext) -> bool:
    return bool(LLMResponse.model_validate(record(context.output), strict=False).tool_calls)


def answered(context: ConditionContext) -> bool:
    return not needs_tools(context)


def tool_inputs(context: InputMappingContext) -> list[LLMToolCall]:
    response = LLMResponse.model_validate(record(context.invocation_context["response"]), strict=False)
    return list(response.tool_calls)


def tool_outputs(context: OutputBindingContext) -> ContextPatch:
    results = tuple(LLMToolResult.model_validate(record(item), strict=False) for item in context.output)
    request = LLMRequest.model_validate(record(context.invocation_context["request"]), strict=False)
    request = request.model_copy(update={"input": (*request.input, *results)})
    return ContextPatch(invocation=(ContextOperation.set("request", request.model_dump(mode="json")),))


def final_input(context: InputMappingContext) -> Answer:
    response = LLMResponse.model_validate(record(context.invocation_context["response"]), strict=False)
    return Answer(answer=response.text, model=response.model,
                  model_calls=context.invocation_context["model_calls"])


def finish(value: Answer) -> Answer:
    return value

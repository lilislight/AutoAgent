"""Run with: .venv/bin/python -m examples.agent_skills_demo.run.

Offline mode is a deterministic Provider simulator, not a model quality test.
The bounded Tool loop here belongs to this example application. The Skill
package provides content and Tools, and owns no Agent loop or conversation.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack
import json
import os
from pathlib import Path

from pydantic import JsonValue

from autoagent.agent import (
    LLMMessage, LLMProvider, LLMRequest, LLMResponse, LLMStreamChunk,
    LLMToolCall, LLMToolResult, OpenAIChatCompletionsProvider,
    OpenAIProviderConfig, OpenAIResponsesProvider, SkillSet,
    TextContent, ToolSet, tool,
)


SKILL_ROOT = Path(__file__).resolve().parent / "skills"


@tool
def inventory_get() -> dict[str, JsonValue]:
    """Return this example's fixed inventory dataset, including missing sales data."""
    return {
        "date": "2026-10-14",
        "items": [
            {"sku": "A", "stock": 20, "daily_sales": 5, "replenishment_days": 7},
            {"sku": "B", "stock": 0, "daily_sales": 2, "replenishment_days": 3},
            {"sku": "C", "stock": 8, "replenishment_days": 5},
        ],
    }


def _offline_report(inventory: dict) -> str:
    lines = ["# 库存报告", "", "## 数据范围", f"数据日期：{inventory['date']}。", "", "## 库存现状与缺货风险"]
    replenishment, missing = [], []
    for item in inventory["items"]:
        sku, stock = item["sku"], item["stock"]
        sales, lead = item.get("daily_sales"), item.get("replenishment_days")
        absent = [name for name in ("daily_sales", "replenishment_days") if item.get(name) is None]
        if absent:
            lines.append(f"- {sku}：库存 {stock}，数据不足，无法预测缺货日期。")
            missing.append(f"- {sku} 缺少 {', '.join(absent)}。")
            continue
        days = stock / sales if sales > 0 else None
        risk = "已缺货" if stock == 0 else "存在缺货风险" if days is not None and days < lead else "当前未见缺货风险"
        available = f"预计可售 {days:g} 天" if days is not None else "日销量为零，不计算可售天数"
        lines.append(f"- {sku}：库存 {stock}，{available}，补货周期 {lead} 天，{risk}。")
        replenishment.append(f"- {sku}：最低补货数量 {max(0, sales * lead - stock)}。")
    return "\n".join([*lines, "", "## 补货建议", *replenishment, "以上数量不含安全库存。", "", "## 缺失信息", *(missing or ["无。"])])


class OfflineProvider(LLMProvider):
    """Scripted calls demonstrate real Skill/Tool reads without network access."""

    def __init__(self):
        self.closed = False

    async def ainvoke(self, request: LLMRequest) -> LLMResponse:
        if self.closed:
            raise RuntimeError("Offline Provider is closed.")
        calls = {item.call_id: item for item in request.input if isinstance(item, LLMToolCall)}
        results = {item.call_id: item for item in request.input
                   if isinstance(item, LLMToolResult) and item.call_id in calls}
        completed_names = {calls[call_id].name for call_id in results}
        if any(value.is_error for value in results.values()):
            raise RuntimeError("A demo Tool failed; inspect its error observation.")
        if request.tools and "skill_load" not in completed_names:
            output = (LLMToolCall(name="skill_load", call_id="load", arguments='{"name":"inventory-report"}'),)
        elif request.tools and "inventory_get" not in completed_names:
            output = (
                LLMToolCall(name="skill_read_resource", call_id="rules", arguments='{"name":"inventory-report","path":"references/report-rules.md"}'),
                LLMToolCall(name="skill_read_resource", call_id="template", arguments='{"name":"inventory-report","path":"assets/report-template.md"}'),
                LLMToolCall(name="inventory_get", call_id="inventory", arguments="{}"),
            )
        else:
            inventory = None
            if "inventory_get" in completed_names:
                inventory = next(value.structured_content for call_id, value in results.items()
                                 if calls[call_id].name == "inventory_get")
            else:
                for item in request.input:
                    if not isinstance(item, LLMMessage):
                        continue
                    for content in item.content:
                        if isinstance(content, TextContent):
                            try:
                                payload = json.loads(content.text)
                            except ValueError:
                                continue
                            if isinstance(payload, dict) and "inventory" in payload:
                                inventory = payload["inventory"]
            if not isinstance(inventory, dict):
                raise RuntimeError("The demo needs actual inventory input.")
            output = (LLMMessage(role="assistant", content=(TextContent(text=_offline_report(inventory)),)),)
        return LLMResponse(model=request.model, output=output, finish_reason="tool_calls" if isinstance(output[0], LLMToolCall) else "stop")

    async def astream(self, request: LLMRequest) -> AsyncIterator[LLMStreamChunk]:
        yield LLMStreamChunk(type="completed", response=await self.ainvoke(request))

    async def aclose(self) -> None:
        self.closed = True


def build_request(skills: SkillSet, *, model: str, activation: str) -> tuple[LLMRequest, ToolSet]:
    instructions = LLMMessage(role="developer", content=(TextContent(text=(
        "按任务选择 Skill，使用前先加载正文，再读取需要的资源。"
        "库存事实必须来自输入或 inventory_get 工具。缺失数据明确指出。"
        "按 Skill 的规则和模板生成中文报告。"
    )),))
    if activation == "explicit":
        document = skills.load("inventory-report")
        rules = skills.read_resource("inventory-report", "references/report-rules.md")
        template = skills.read_resource("inventory-report", "assets/report-template.md")
        inputs = (
            instructions, document.to_message(),
            LLMMessage(role="user", content=rules.content),
            LLMMessage(role="user", content=template.content),
            LLMMessage(role="user", content=(TextContent(text=json.dumps({"inventory": inventory_get()}, ensure_ascii=False)),)),
            LLMMessage(role="user", content=(TextContent(text="请生成库存分析和补货报告。"),)),
        )
        tools = ToolSet()
    else:
        tools = ToolSet((*skills.as_tools(), inventory_get))
        catalog = skills.catalog_message()
        inputs = (instructions, *((catalog,) if catalog is not None else ()),
                  LLMMessage(role="user", content=(TextContent(text="请获取当前库存并生成分析和补货报告。"),)))
    return LLMRequest(model=model, input=inputs, tools=tools.definitions), tools


async def run_demo(
    *, mode: str = "offline", activation: str = "explicit", protocol: str = "responses",
    model: str | None = None, skill_root: Path = SKILL_ROOT,
    max_rounds: int = 6, verbose: bool = False,
) -> LLMResponse:
    skills = SkillSet.discover([skill_root]).select(include=("inventory-report",))
    model = model or os.environ.get("LLM_MODEL") or ("offline" if mode == "offline" else None)
    if model is None:
        raise ValueError("Live mode requires --model or LLM_MODEL.")
    request, tools = build_request(skills, model=model, activation=activation)
    async with AsyncExitStack() as stack:
        if mode == "offline":
            provider = OfflineProvider()
            stack.push_async_callback(provider.aclose)
        else:
            key = os.environ.get("OPENAI_API_KEY")
            if not key:
                raise ValueError("Live mode requires OPENAI_API_KEY.")
            config = OpenAIProviderConfig(api_key=key, base_url=os.environ.get("OPENAI_BASE_URL") or "https://api.openai.com/v1")
            provider_type = OpenAIResponsesProvider if protocol == "responses" else OpenAIChatCompletionsProvider
            provider = await stack.enter_async_context(provider_type(config))
        for _ in range(max_rounds):
            response = await provider.ainvoke(request)
            if not response.tool_calls:
                return response
            results = []
            for incoming in response.tool_calls:
                if verbose:
                    print(f"Tool: {incoming.name} {incoming.arguments}")
                results.append(await tools.ainvoke(incoming))
            request = LLMRequest(model=model, input=(*request.input, *response.output, *results), tools=tools.definitions)
        raise RuntimeError("The example reached its configured model-call limit.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("offline", "live"), default="offline")
    parser.add_argument("--activation", choices=("explicit", "tools"), default="explicit")
    parser.add_argument("--protocol", choices=("responses", "chat"), default="responses")
    parser.add_argument("--model")
    parser.add_argument("--skill-root", type=Path, default=SKILL_ROOT)
    parser.add_argument("--max-rounds", type=int, default=6)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    if args.max_rounds <= 0:
        parser.error("--max-rounds must be positive")
    try:
        response = asyncio.run(run_demo(**vars(args)))
    except (ValueError, RuntimeError, ImportError) as error:
        parser.exit(1, f"{error}\n")
    print(response.text)


if __name__ == "__main__":
    main()

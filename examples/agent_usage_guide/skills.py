"""Skill discovery, progressive loading, snapshots, Tools and bound scripts.

Run: .venv/bin/python -m examples.agent_usage_guide.skills
"""

import asyncio
import json
from pathlib import Path
import sys

from autoagent.agent import (
    LLMRequest, LLMToolCall, MemorySkillSource, Skill, SkillLoadRequest,
    SkillOperations, SkillRunScriptRequest, SkillScriptSpec, SkillSet,
    SkillSnapshot, SubprocessSkillRunner, ToolContext, ToolSet,
)
from .support import show, user
from .tools import stock


SKILL_ROOT = Path(__file__).parent / "skills"


def make_runner():
    # This explicit application binding grants execution of exactly one script.
    return SubprocessSkillRunner((SkillScriptSpec(
        skill_name="stock-guide", path="scripts/total.py", command=(sys.executable,),
        resources=("references/rules.md",)),), max_timeout_seconds=5, env={})


async def main():
    discovered = SkillSet.discover([SKILL_ROOT])
    assert not discovered.diagnostics
    show("catalog (metadata only)", discovered.catalog_message())
    document = discovered.load("stock-guide")
    page = discovered.list_resources("stock-guide", limit=1)
    assert page.next_cursor is not None
    next_page = discovered.list_resources("stock-guide", cursor=page.next_cursor, limit=1)
    assert page.resources[0].path != next_page.resources[0].path
    text = discovered.read_text("stock-guide", "references/rules.md", max_lines=1)
    assert text.next_line == 2
    remainder = discovered.read_text("stock-guide", "references/rules.md", start_line=text.next_line)
    show("progressive resource read", {"first": text.text, "rest": remainder.text})

    # Freeze all referenced content before a run; a snapshot carries verifiable bytes.
    snapshot = discovered["stock-guide"].snapshot()
    restored = SkillSnapshot.model_validate_json(snapshot.model_dump_json()).to_skill()
    skills = SkillSet((restored,))
    assert skills.load("stock-guide").snapshot_revision == snapshot.revision
    rules = skills.read_resource("stock-guide", "references/rules.md")
    request = LLMRequest(model="application-selected-model", input=(document.to_message(),
        user("Report stock for apple."), user(text.text)))
    show("explicit activation", {"input_items": len(request.input), "snapshot_revision": snapshot.revision,
                                  "resource_type": rules.content[0].type})

    context = document.restrict_tools(ToolContext(user_id="alice", services={"inventory": {"apple": 3}}))
    stock_tools = ToolSet((stock,), context=context)
    quantity = await stock_tools.ainvoke(LLMToolCall(call_id="read-stock", name="stock", arguments='{"item":"apple"}'))
    assert quantity.structured_content["quantity"] == 3

    runner = make_runner()
    operations = SkillOperations(skills, runner=runner)
    assert (await operations.aload(SkillLoadRequest(name="stock-guide"))).metadata.name == "stock-guide"
    script = await operations.arun_script(SkillRunScriptRequest(
        name="stock-guide", path="scripts/total.py", stdin="[2,3]", timeout_seconds=2))
    assert script.returncode == 0 and json.loads(script.stdout)["total"] == 5
    show("bound script", script)

    # Model-facing activation is optional: expose catalog + read Tools to the LLM.
    read_tools = ToolSet(skills.as_tools(runner=runner))
    loaded = await read_tools.ainvoke(LLMToolCall(call_id="load", name="skill_load", arguments='{"name":"stock-guide"}'))
    assert not loaded.is_error and loaded.tool_metadata["skill"]["snapshot_revision"] == snapshot.revision
    show("model-facing Skill Tools", [definition.name for definition in read_tools.definitions])

    manual = Skill(MemorySkillSource("manual", {"SKILL.md":
        "---\nname: manual\ndescription: Application-only instructions\ndisable-model-invocation: true\n---\nManual content."}))
    manual_set = SkillSet((manual,))
    assert manual_set.catalog_message() is None and not manual_set.as_tools()
    assert (await SkillOperations(manual_set).aload(SkillLoadRequest(name="manual"))).instructions.strip() == "Manual content."
    show("application-only Skill", "Typed operations can read a Skill excluded from model-facing discovery.")


if __name__ == "__main__":
    asyncio.run(main())

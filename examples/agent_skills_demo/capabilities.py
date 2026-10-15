"""Run: .venv/bin/python -m examples.agent_skills_demo.capabilities.

Offline demonstration of diagnostics, manual-only Skills, resource paging,
text ranges, portable sources and explicitly bound script execution.
"""

from __future__ import annotations

import asyncio
import json
import sys

from autoagent.agent import (
    LocalSkillSource, MemorySkillSource, PackageSkillSource, Skill, SkillSet,
    SkillScriptSpec, SubprocessSkillRunner, ToolSet, LLMToolCall,
)
from .run import SKILL_ROOT, inventory_get


async def run_capabilities() -> dict:
    local = LocalSkillSource(SKILL_ROOT / "inventory-report")
    manual = MemorySkillSource("manual-only", {"SKILL.md": (
        "---\nname: manual-only\ndescription: Application-selected guidance.\n"
        "disable-model-invocation: true\n---\nManual instructions."
    )})
    invalid = MemorySkillSource("invalid", {"SKILL.md": "---\nname: invalid\n---\nMissing description."})
    skills = SkillSet.from_sources((local, manual, invalid), errors="skip")
    document = skills.load("inventory-report")
    paths, cursor = [], None
    while True:
        page = skills.list_resources("inventory-report", limit=1, cursor=cursor)
        paths.extend(resource.path for resource in page.resources)
        cursor = page.next_cursor
        if cursor is None:
            break
    chunks, next_line = [], 1
    while next_line is not None:
        chunk = skills.read_text("inventory-report", "references/report-rules.md", start_line=next_line, max_lines=2)
        chunks.append(chunk.text)
        next_line = chunk.next_line
    # A remote SDK may prefetch these bytes on its owning loop. The resulting
    # source then needs neither its connection nor its SDK.
    snapshot = MemorySkillSource("inventory-report", {
        "SKILL.md": skills["inventory-report"].read_bytes("SKILL.md"),
        **{path: skills["inventory-report"].read_bytes(path) for path in paths},
    })
    memory_document = Skill(snapshot).load()
    package_document = Skill(PackageSkillSource("examples.agent_skills_demo", "skills/inventory-report")).load()
    assert memory_document.revision == package_document.revision == document.revision
    runner = SubprocessSkillRunner((SkillScriptSpec(
        skill_name="inventory-report", path="scripts/calculate.py", command=(sys.executable,),
    ),))
    tools = ToolSet(skills.as_tools(runner=runner))
    result = await tools.ainvoke(LLMToolCall(
        name="skill_run_script", call_id="calculate", arguments=json.dumps({
            "name": "inventory-report", "path": "scripts/calculate.py",
            "stdin": json.dumps(inventory_get(), ensure_ascii=False),
        }),
    ))
    if result.is_error:
        raise RuntimeError("The bound calculation script failed.")
    return {
        "diagnostics": [value.model_dump(mode="json") for value in skills.diagnostics],
        "model_skills": [value.name for value in skills.model_metadata],
        "manual_skill": skills.load("manual-only").metadata.name,
        "resources": paths, "rule_text": "".join(chunks), "source_revisions_match": True,
        "calculation": json.loads(result.structured_content["stdout"]),
    }


if __name__ == "__main__":
    print(json.dumps(asyncio.run(run_capabilities()), ensure_ascii=False, indent=2))

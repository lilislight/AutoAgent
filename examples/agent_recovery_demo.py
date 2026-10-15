"""Run .venv/bin/python -m examples.agent_recovery_demo; no external services."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory

from autoagent.agent import (
    LLMToolCall, MCPClient, MCPReconnectPolicy, MCPStdioConfig, MCPToolRegistry,
    Skill, SkillSnapshot, SkillScriptSpec, SubprocessSkillRunner,
)


SERVER = """
from mcp.server import MCPServer
server = MCPServer('recovery-demo')
@server.tool()
def add(a: int, b: int) -> dict[str, int]:
    return {'sum': a + b}
server.run()
"""


async def main():
    config = MCPStdioConfig(server_id="local", command=sys.executable, args=("-c", SERVER))
    async with MCPClient(config, reconnect_policy=MCPReconnectPolicy()) as client:
        registry = MCPToolRegistry(client)
        old = await registry.refresh()
        before = client.generation
        await client.areconnect()
        old_call = LLMToolCall(name="add", call_id="old", arguments='{"a":2,"b":4}')
        old_result = await old.ainvoke(old_call)
        result = await registry.ainvoke(old_call.model_copy(update={"call_id":"new"}))
        mcp = {"before_generation":before, "after_generation":client.generation,
               "old_definition_rejected":old_result.is_error, "result":result.structured_content}

    with TemporaryDirectory() as directory:
        root = Path(directory) / "report"
        (root / "scripts").mkdir(parents=True)
        (root / "assets").mkdir()
        (root / "SKILL.md").write_text("---\nname: report\ndescription: Versioned report\n---\nUse the captured data.\n")
        (root / "assets/value.txt").write_text("original\nsecond line\n")
        (root / "scripts/run.py").write_text("from pathlib import Path\nprint(Path('assets/value.txt').read_text(), end='')\n")

        snapshot = Skill(root).snapshot()
        saved = Path(directory) / "snapshot.json"
        saved.write_text(snapshot.model_dump_json())
        frozen = SkillSnapshot.model_validate_json(saved.read_text()).to_skill()
        (root / "assets/value.txt").write_text("new live data\n")
        (root / "scripts/run.py").write_text("raise RuntimeError('new live code')\n")

        runner = SubprocessSkillRunner([SkillScriptSpec(skill_name="report", path="scripts/run.py",
                                                       command=(sys.executable,), resources=("assets/value.txt",))])
        execution = await runner.arun(frozen, "scripts/run.py")
        skill = {"same_version":execution.snapshot_revision == snapshot.revision,
                 "second_line":frozen.read_text("assets/value.txt", start_line=2).text.strip(),
                 "script_output":execution.stdout.strip()}
        print(json.dumps({"mcp":mcp, "skill":skill}, indent=2))


if __name__ == "__main__":
    asyncio.run(main())

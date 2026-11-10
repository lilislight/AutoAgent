"""Run with .venv/bin/python -m examples.agent_execution_demo (no LLM needed)."""

import asyncio
import json
from pathlib import Path
import sys
import tempfile

from autoagent.agent import (
    CommandRule, ExecutionToolsConfig, LLMToolCall, LocalExecutionService,
    ToolContext, execution_tools,
)


async def main():
    code = "import time; print('started',flush=True); time.sleep(1); print('finished')"
    config = ExecutionToolsConfig(default_decision="ask", rules=[
        CommandRule(program=sys.executable, args=["-u", "-c", code], decision="allow"),
        CommandRule(program="printf", decision="allow"),
    ])
    with tempfile.TemporaryDirectory(prefix="autoagent-execution-demo-") as root:
        service = LocalExecutionService(Path(root), config=config)
        tools = execution_tools(service, context=ToolContext(session_id="demo"))

        async def invoke(name, **arguments):
            result = await tools.ainvoke(LLMToolCall(name=name, call_id=name, arguments=json.dumps(arguments)))
            print(json.dumps(result.structured_content, ensure_ascii=False, indent=2))
            return result.structured_content

        try:
            result = await invoke("execute_command", command={"mode": "exec", "program": sys.executable,
                "args": ["-u", "-c", code]}, yield_seconds=0)
            process_id, cursor = result["process_id"], result["next_cursor"]
            while result["state"] == "running":
                result = await invoke("manage_process", request={"action": "read", "process_id": process_id,
                    "cursor": cursor, "wait_seconds": 2})
                cursor = result["next_cursor"]
            # The same printf rule applies to static commands inside Bash.
            if sys.platform != "win32":
                await invoke("execute_command", command={"mode": "shell", "shell": "bash",
                    "script": "printf '%s' hello && printf '%s' world"})
        finally:
            await service.aclose()


if __name__ == "__main__":
    asyncio.run(main())

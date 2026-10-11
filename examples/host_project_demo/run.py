"""Run from repository root: python -m examples.host_project_demo.run.

Discover a project, run to a Wait, recreate Host, then resume by Invocation id.
"""

import asyncio
import tempfile
from pathlib import Path

from autoagent.host import AutoAgentHost, HostSettings


async def main():
    project = Path(__file__).resolve().parent
    report = AutoAgentHost.check_project(project)
    assert report.ok
    with tempfile.TemporaryDirectory(prefix="autoagent-host-demo-") as temporary:
        settings = HostSettings(
            database_url="sqlite+aiosqlite:///" + str(Path(temporary) / "runtime.db")
        )
        async with AutoAgentHost.from_project(project, settings=settings) as host:
            waiting = await host.join(await host.execute("approval", {"text": "document"}))
            print(
                f"Before restart: {waiting.status}; Invocation: {waiting.invocation_id}"
            )
        async with AutoAgentHost.from_project(project, settings=settings) as host:
            await host.respond(waiting.ref, waiting.waits[0].id, {"text": "document"})
            result = await host.join(waiting.ref)
            print(f"After restart: {result.status}; output: {result.output}")
            print(f"Registered Workflows: {len((await host.list_workflows()).items)}")


if __name__ == "__main__":
    asyncio.run(main())

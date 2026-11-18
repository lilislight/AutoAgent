"""Run a local Workflow with Host Persistence, query it and load its checkpoint.

Install the persistence extra, then run from the repository root:
    .venv/bin/python -m examples.host_persistence_demo --mode buffered
"""

from __future__ import annotations

import argparse
import asyncio
import tempfile
from pathlib import Path

from typing_extensions import TypedDict

from autoagent import Edge, Node, Operator, Workflow
from autoagent.core.app import AutoAgentApp
from autoagent.host.persistence import DatabaseSink, PersistenceConfig


class Document(TypedDict):
    text: str


def identity(value: Document) -> Document:
    return value


async def run(url: str, mode: str):
    async with await DatabaseSink.open(
        url, config=PersistenceConfig(mode=mode)
    ) as persistence:
        operator = Operator(identity, id="document", version="1")
        workflow = Workflow(
            "persistence-demo",
            nodes=[Node("read", operator), Node("done", operator)],
            edges=[Edge("read", "done")],
        )
        app = AutoAgentApp(runtime_event_sink=persistence, user_event_sink=persistence)
        try:
            app.register_workflow(workflow)
            await persistence.register_operator(operator)
            await persistence.register_workflow(
                app.workflow_definition_snapshot(workflow.id)
            )
            result = await app.ainvoke(workflow, {"text": "x" * (2 * 1024 * 1024)})
            checkpoint = app.unload_session(result.ref, capture_checkpoint=True)
            checkpoint_id = await persistence.save_checkpoint(checkpoint)
            nodes = await persistence.list_nodes(result.ref.invocation_id)
            calls = await persistence.list_calls(result.ref.invocation_id)
            detail = await persistence.node_detail(
                result.ref.invocation_id, "read@root"
            )
            restored = await persistence.get_checkpoint(checkpoint_id)
            assert restored.to_record() == checkpoint.to_record()
            assert len(detail["output"]["value"]["text"]) == 2 * 1024 * 1024
            print(f"Invocation: {result.ref.invocation_id} ({result.status})")
            print(f"Historical Nodes: {len(nodes.items)}; Calls: {len(calls.items)}")
            print(f"Checkpoint: {checkpoint_id}; output: 2 MiB")
            print(f"Writer: {await persistence.health()}")
        finally:
            app.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", help="sqlite+aiosqlite or postgresql+asyncpg URL")
    parser.add_argument("--mode", choices=("durable", "buffered"), default="durable")
    args = parser.parse_args()
    if args.url:
        asyncio.run(run(args.url, args.mode))
    else:
        with tempfile.TemporaryDirectory(prefix="autoagent-persistence-") as tmp:
            asyncio.run(
                run("sqlite+aiosqlite:///" + str(Path(tmp) / "runtime.db"), args.mode)
            )


if __name__ == "__main__":
    main()

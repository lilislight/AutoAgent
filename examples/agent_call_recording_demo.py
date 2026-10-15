"""Run .venv/bin/python -m examples.agent_call_recording_demo --database /tmp/llm-demo.sqlite.

Uses the real OpenAI SDK with an offline HTTP transport. No credentials or
external services are needed. Omit --database to use a temporary database.
"""

from __future__ import annotations

import argparse
import asyncio
from contextlib import asynccontextmanager
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory

import httpx
from openai import AsyncOpenAI

from autoagent import Node, Workflow
from autoagent.agent import LLMMessage, LLMRequest, LLMResponse, OpenAIResponsesProvider, TextContent
from autoagent.host import AutoAgentHost, HostSettings


@asynccontextmanager
async def model_provider():
    def respond(request):
        return httpx.Response(200, headers={"x-request-id": "offline-request"}, json={
            "id": "offline-response", "object": "response", "created_at": 1,
            "status": "completed", "model": "offline-model",
            "output": [{"id": "message", "type": "message", "role": "assistant", "status": "completed",
                        "content": [{"type": "output_text", "text": "Hello from the offline model.", "annotations": []}]}],
            "usage": {"input_tokens": 4, "output_tokens": 6, "total_tokens": 10},
        })

    async with AsyncOpenAI(api_key="offline", max_retries=0, http_client=httpx.AsyncClient(
        transport=httpx.MockTransport(respond),
    )) as sdk:
        sdk._platform = "Linux"  # Skip SDK system discovery; transport is entirely offline.
        async with OpenAIResponsesProvider(client=sdk) as provider:
            yield provider


async def run(database: Path):
    request = LLMRequest(model="offline-model", input=(LLMMessage(role="user", content=(TextContent(text="Say hello."),)),))
    # Without Host or an explicit recorder, ordinary Provider use needs no database.
    async with model_provider() as standalone:
        response = await standalone.ainvoke(request)

    active = {}

    @asynccontextmanager
    async def resources(setup):
        async with model_provider() as provider:
            active["provider"] = provider
            yield

    async def generate(value: LLMRequest) -> LLMResponse:
        return await active["provider"].ainvoke(value)

    workflow = Workflow("recording-demo", version="1", nodes=[Node("generate", generate)])
    settings = HostSettings(database_url="sqlite+aiosqlite:///" + str(database.resolve()))
    async with AutoAgentHost(workflows=[workflow], settings=settings, lifespan=resources, root=database.parent) as host:
        result = await host.join(await host.execute(workflow.id, request))
        if result.status != "completed":
            raise RuntimeError(result.error)

    # Inspect physical storage here; there is deliberately no new product query API.
    with sqlite3.connect(database) as conn:
        conn.row_factory = sqlite3.Row
        row = dict(conn.execute("SELECT * FROM llm_calls WHERE invocation_id=?", (result.invocation_id,)).fetchone())
    assert row["workflow_revision_id"] == result.ref.workflow_revision_id
    assert row["operator_call_id"] and row["operator_definition_id"]
    assert row["status"] == "succeeded" and row["total_tokens"] == 10
    print(json.dumps({"standalone_answer": response.text, "database": str(database), "stored_call": row}, indent=2))


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path)
    options = parser.parse_args()
    if options.database is not None:
        await run(options.database)
    else:
        with TemporaryDirectory() as directory:
            await run(Path(directory) / "runtime.sqlite")


if __name__ == "__main__":
    asyncio.run(main())

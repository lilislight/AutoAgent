"""Standalone recording and automatic Host SQLite recording with Core identities.

Run: .venv/bin/python -m examples.agent_usage_guide.recording
"""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory

from autoagent import Node, Workflow
from autoagent.agent import (
    EmbeddingRequest, LLMCallContext, LLMCallRecord, LLMRequest, LLMResponse,
    OpenAIEmbeddingProvider, OpenAIResponsesProvider, llm_recording_context,
)
from autoagent.host import AutoAgentHost, HostSettings
from .support import connection, offline_http, offline_provider, request, show


class MemoryRecorder:
    """A minimal recorder upserts each call's running/terminal snapshots by ID."""

    def __init__(self):
        self.records: dict[str, LLMCallRecord] = {}

    async def record_llm_call(self, record: LLMCallRecord) -> None:
        self.records[record.llm_call_id] = record


async def main():
    # Without a recorder or Host database, calls work normally and store no rows.
    async with offline_provider() as (provider, _):
        assert (await provider.ainvoke(request())).text

    recorder = MemoryRecorder()
    async with offline_provider(recorder=recorder) as (provider, _):
        with llm_recording_context(LLMCallContext(workflow_id="manual-context")):
            await provider.ainvoke(request())
    record = next(iter(recorder.records.values()))
    assert record.status == "succeeded" and record.total_tokens == 14
    assert record.workflow_id == "manual-context"
    show("explicit recorder", record)

    active = {}

    @asynccontextmanager
    async def resources(setup):
        async with OpenAIResponsesProvider(connection()) as provider, OpenAIEmbeddingProvider(connection()) as embeddings:
            active.update(llm=provider, embeddings=embeddings)
            yield

    async def generate(value: LLMRequest) -> LLMResponse:
        await active["embeddings"].ainvoke(EmbeddingRequest(model="demo-embedding", input="record this call too"))
        return await active["llm"].ainvoke(value)

    workflow = Workflow("agent-recording-guide", version="1", nodes=[Node("generate", generate)])
    with TemporaryDirectory() as directory:
        database = Path(directory) / "runtime.sqlite"
        settings = HostSettings(database_url="sqlite+aiosqlite:///" + str(database))
        async with offline_http():
            async with AutoAgentHost(workflows=[workflow], settings=settings, lifespan=resources, root=directory) as host:
                result = await host.join(await host.execute(workflow.id, request()))
                assert result.status == "completed", result.error
        # Example diagnostic SQL, not a newly introduced product query API.
        with sqlite3.connect(database) as db:
            db.row_factory = sqlite3.Row
            rows = [dict(row) for row in db.execute(
                "SELECT * FROM llm_calls WHERE invocation_id=? ORDER BY kind", (result.invocation_id,))]
        assert {row["kind"] for row in rows} == {"generation", "embedding"}
        assert all(row["workflow_revision_id"] == result.ref.workflow_revision_id for row in rows)
        assert all(row["operator_call_id"] and row["status"] == "succeeded" for row in rows)
        show("Host metadata rows", [{key: row[key] for key in (
            "kind", "workflow_id", "workflow_revision_id", "node_id", "operator_call_id", "status", "total_tokens"
        )} for row in rows])


if __name__ == "__main__":
    asyncio.run(main())

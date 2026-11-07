"""Server-facing contracts for the real Agent demo, with an offline test Provider."""

import asyncio
from contextlib import asynccontextmanager
import importlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4

import httpx

from autoagent.agent import (
    AgentApprovalRequest, AgentApprovalResponse, LLM_CALL, LLMMessage, LLMToolResult, llm_call_operator,
)
from autoagent.agent.context import restore
from autoagent.host import AutoAgentHost, HostSettings
from autoagent.server import create_app
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import ScriptedProvider, answer
from tests.test_agent_subagents import call, response, results


PROJECT = Path(__file__).resolve().parents[1] / "examples" / "agent_server_demo"


class DelegatingProvider(ScriptedProvider):
    async def ainvoke(self, request):
        # Skill catalogs may precede the task as user-role fixed Context.
        if any(getattr(part, "text", None) == "child task" for item in request.input
               if isinstance(item, LLMMessage) and item.role == "user" for part in item.content):
            self.requests.append(request)
            return answer("Child complete")
        return await super().ainvoke(request)


class ServerDemoContracts:
    async def database_url(self, directory):
        return "sqlite+aiosqlite:///" + str(Path(directory) / "runtime.db")

    @asynccontextmanager
    async def client(self, provider):
        with tempfile.TemporaryDirectory() as directory:
            @asynccontextmanager
            async def lifespan(setup):
                tools = importlib.import_module("agent_demo.tools")
                with patch.object(tools, "NOTES_DIRECTORY", Path(directory) / "notes"):
                    setup.agent_context.resources.register_provider("default", provider)
                    setup.register_operator(llm_call_operator(provider, id="demo.test.llm"),
                        capability_id=LLM_CALL.id, default=True)
                    yield

            async with AutoAgentHost.from_project(PROJECT, use_env_file=False,
                environment={"AUTOAGENT_LLM_MODEL": "offline"}, lifespan=lifespan,
                settings=HostSettings(database_url=await self.database_url(directory))) as host:
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(host, ui=False)),
                    base_url="http://demo") as client:
                    yield host, client, Path(directory)

    async def start(self, client):
        catalog = await client.get("/api/v2/workflows")
        self.assertEqual(catalog.status_code, 200, catalog.text)
        workflows = {item["workflow_id"]: item for item in catalog.json()["items"]}
        self.assertEqual(set(workflows), {"agent", "researcher", "autoagent.agent"})
        revision = workflows["agent"]["revision_id"]
        spec = await client.get("/api/v2/workflow-run-spec", params={"workflow_id": "agent",
            "revision_id": revision, "entry_node_id": "start"})
        self.assertEqual(spec.status_code, 200, spec.text)
        run = await client.post("/api/v2/executions", json={"workflow_id": "agent", "revision_id": revision,
            "entry_node_id": "start", "input": {"message": "root task"}})
        self.assertEqual(run.status_code, 202, run.text)
        return run.json()["invocation_id"]

    async def action(self, client, invocation_id, wait, payload):
        operation = await client.post("/api/v2/execution-actions", json={"action": "respond",
            "invocation_id": invocation_id, "request_id": uuid4().hex,
            "target_invocation_id": invocation_id, "wait_id": wait.id, "payload": payload})
        self.assertEqual(operation.status_code, 202, operation.text)
        operation_id = operation.json()["operation_id"]
        async with asyncio.timeout(15):
            while True:
                receipt = (await client.get("/api/v2/execution-operations/" + operation_id)).json()
                if receipt["status"] != "running":
                    self.assertEqual(receipt["status"], "succeeded", receipt)
                    return
                await asyncio.sleep(0.01)

    async def test_server_run_question_and_approval_preserve_actual_effect_boundary(self):
        """Run/Respond HTTP contracts expose independent question and approval waits; saving occurs only after approval."""
        provider = ScriptedProvider(response(
            call("ask_user", {"questions": [{"id": "city", "type": "text", "text": "Which city?"}]}, "question"),
            call("save_demo_note", {"title": "demo", "content": "approved note"}, "save")), answer())
        async with self.client(provider) as (host, client, directory):
            invocation_id = await self.start(client)
            waiting = await host.join(invocation_id)
            self.assertEqual(waiting.status, "waiting", waiting.error)
            self.assertEqual({wait.request["kind"] for wait in waiting.waits}, {"agent.ask_user", "agent.tool_approval"})
            self.assertFalse((directory / "notes").exists())
            snapshot = await host.agent_context.get_snapshot(invocation_id)
            _, skills = host.agent_context.catalog(snapshot)
            self.assertIn("demo-guide", [skill.metadata.name for skill in skills])
            self.assertFalse(host.agent_context.resources.entries("tool"))
            self.assertFalse(host.agent_context.resources.entries("skill"))
            for wait in waiting.waits:
                if wait.request["kind"] == "agent.ask_user":
                    payload = {"answers": [{"question_id": "city", "text": "北京"}]}
                else:
                    batch = restore(AgentApprovalRequest, wait.request)
                    payload = AgentApprovalResponse(decisions=tuple(item.respond(approved=True)
                        for item in batch.requests)).model_dump(mode="json")
                await self.action(client, invocation_id, wait, payload)
            result = await host.join(invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            notes = list((directory / "notes").glob("*.json"))
            self.assertEqual(len(notes), 1)
            self.assertEqual(json.loads(notes[0].read_text())["content"], "approved note")
            self.assertEqual([item.call_id for item in provider.requests[-1].input if isinstance(item, LLMToolResult)],
                ["question", "save"])

    async def test_server_registered_subagent_runs_with_inherited_provider(self):
        """The served project registers the target Workflow and delegates through the real native wait branch."""
        def finish(request):
            item = results(request)[-1]
            self.assertFalse(item.is_error, item)
            self.assertEqual(item.structured_content["agents"][0]["status"], "completed")
            return answer()
        provider = DelegatingProvider(response(call("run_agent", {
            "type": "researcher", "task": "child task", "wait_timeout_seconds": 5})), finish)
        async with self.client(provider) as (host, client, _):
            invocation_id = await self.start(client)
            async with asyncio.timeout(30):
                while True:
                    result = await host.join(invocation_id)
                    if result.status != "waiting":
                        break
                    await asyncio.sleep(0.01)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(len(provider.requests), 3)


class SQLiteServerDemoTests(ServerDemoContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLServerDemoTests(ServerDemoContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self, directory):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

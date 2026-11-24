"""Installed authoring guides are product defaults, never SDK-global resources."""

import asyncio
import json
import unittest

from autoagent.agent import AgentInput, LLMResponse, LLMToolCall, LLMToolResult
from autoagent.agent_app import ResourceControl
from autoagent.agent_app.builtin_skills import NAMES
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_builtin_agent import BuiltinApplicationContracts
from tests.test_agent_workflow import ScriptedProvider, answer


def call(name, arguments):
    return LLMResponse(model="offline", finish_reason="tool_calls", output=(
        LLMToolCall(name=name, call_id=name, arguments=json.dumps(arguments)),))


def advertised(request):
    load = next(tool for tool in request.tools if tool.name == "skill_load")
    return load.parameters["properties"]["name"]["enum"]


class BundledSkillContracts:
    project = BuiltinApplicationContracts.project
    database_url = BuiltinApplicationContracts.database_url

    async def test_defaults_load_instructions_and_references_progressively(self):
        """An empty project offers both guides and reads bodies through ordinary Skill Tools only when requested."""
        provider = ScriptedProvider(call("skill_load", {"name": NAMES[0]}),
            call("skill_read_resource", {"name": NAMES[0], "path": "references/live-registration.md"}), answer())
        async with self.project(provider=provider) as (host, _):
            app = host.applications["agent"]
            result = await host.join(await app.start_conversation("author", "Create a Workflow"))
            self.assertEqual(result.status, "completed", result.error)
            self.assertCountEqual(advertised(provider.requests[0]), NAMES)
            self.assertNotIn("Build the smallest Workflow", provider.requests[0].model_dump_json())
            self.assertIn("Build the smallest Workflow", provider.requests[1].model_dump_json())
            resource = next(item for item in provider.requests[2].input if isinstance(item, LLMToolResult)
                and item.call_id == "skill_read_resource")
            self.assertFalse(resource.is_error, str(resource))
            self.assertIn("register_workflow", str(resource.content))

    async def test_inventory_exposes_read_only_builtin_documents_without_database_rows(self):
        """Built-in guides have stable product identities and cannot mutate installed files or create overrides."""
        async with self.project() as (host, client):
            app = host.applications["agent"]
            rows = [row for row in (await app.resource_configs())["resources"] if row["kind"] == "skill"]
            self.assertCountEqual([row["name"] for row in rows], NAMES)
            for row in rows:
                self.assertEqual((row["origin"], row["enabled"], row["enabled_editable"]), ("builtin", True, False))
                document = await client.get("/api/v2/agent/skills/document", params={"management_id": row["management_id"]})
                self.assertEqual(document.status_code, 200, document.text)
                self.assertIn("live-registration.md", document.json()["content"])
                with self.assertRaisesRegex(ValueError, "read-only"):
                    await app.save_resource(ResourceControl(name=row["name"], resource_id=row["management_id"], enabled=False))
            self.assertEqual(await app.storage.context("resource_records"), [])

    async def test_workspace_override_and_disabled_document_do_not_duplicate_or_fall_back(self):
        """A Workspace guide shadows its bundled namesake; disabling it does not silently restore the bundled copy."""
        provider = ScriptedProvider(answer(), answer(), answer())
        async with self.project(provider=provider) as (host, _):
            root = host.root / ".agents" / "skills" / NAMES[0]
            root.mkdir(parents=True)
            (root / "SKILL.md").write_text(f"---\nname: {NAMES[0]}\ndescription: Custom project authoring\n---\nCUSTOM_GUIDE\n")
            app = host.applications["agent"]
            result = await host.join(await app.start_conversation("override", "Hello"))
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(advertised(provider.requests[0]).count(NAMES[0]), 1)
            self.assertIn("Custom project authoring", provider.requests[0].model_dump_json())
            self.assertNotIn("Author AutoAgent V2 Workflows and business", provider.requests[0].model_dump_json())
            rows = [row for row in (await app.resource_configs())["resources"] if row["kind"] == "skill" and row["name"] == NAMES[0]]
            self.assertFalse(next(row for row in rows if row["origin"] == "builtin")["available"])
            custom = next(row for row in rows if row["origin"] == "workspace")
            await app.save_resource(ResourceControl(name=NAMES[0], resource_id=custom["management_id"], enabled=False))
            self.assertEqual((await host.join(await app.start_conversation("override", "Again"))).status, "completed")
            self.assertEqual(advertised(provider.requests[1]), [NAMES[1]])
            self.assertEqual((await host.join(await app.start_conversation("unbound", "Hello", unbound=True))).status, "completed")
            self.assertCountEqual(advertised(provider.requests[2]), NAMES)

    async def test_custom_sdk_agent_does_not_receive_product_guides(self):
        """A custom Agent on the same Host keeps its explicit registry and receives no product Skill Tools."""
        provider = ScriptedProvider(answer())
        async with self.project(custom=True, provider=provider) as (host, _):
            result = await host.join(await host.execute("custom", AgentInput(message="Hello")))
            self.assertEqual(result.status, "completed", result.error)
            self.assertNotIn("skill_load", {tool.name for tool in provider.requests[0].tools})
            self.assertFalse(any(name in provider.requests[0].model_dump_json() for name in NAMES))

    async def test_product_subagent_receives_the_bundled_guides(self):
        """Product delegation uses the same default guides without globally registering Skills in the SDK."""
        provider = ScriptedProvider(call("run_agent", {"type": "agent", "task": "Read the guides", "wait_timeout_seconds": 30}),
            answer("Child done"), answer("Root done"))
        async with self.project(provider=provider) as (host, _):
            result = await host.join(await host.applications["agent"].start_conversation("parent", "Delegate"))
            async with asyncio.timeout(15):
                while result.status == "waiting":
                    await asyncio.sleep(.01)
                    result = await host.join(result.ref)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(len(provider.requests), 3)
            for request in provider.requests:
                self.assertCountEqual(advertised(request), NAMES)


class SQLiteBundledSkillTests(BundledSkillContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLBundledSkillTests(BundledSkillContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self, root):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

"""Workflow resource unions, explicit Host grants, live policies and source isolation."""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import tempfile
import unittest

from autoagent.agent import (
    AgentAccess, AgentApprovalResponse, AgentInput, AgentResourceRegistry, MCPSource, SkillDirectory,
    Skill, MemorySkillSource, Tool, ToolDecision, ToolSet, agent_workflow,
)
from autoagent.host import AutoAgentHost, HostSettings
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import ScriptedProvider, answer, calls, settings
from tests.test_agent_subagents import call, response, results


def skill(name):
    return Skill.from_source(MemorySkillSource(name, {"SKILL.md":
        f"---\nname: {name}\ndescription: {name} helper\n---\nUse {name}.\n"}))


class ResourceScopeTests(unittest.TestCase):
    def test_unscoped_resources_are_closed_and_source_grants_cover_future_publications(self):
        """Registration requires explicit grants and live source grants include later discoveries."""
        def lookup(text: str) -> str:
            return text
        registry = AgentResourceRegistry()
        registry.register_tool(lookup)
        self.assertEqual(registry.catalog("agent"), ((), ()))
        registry.set_workflows("tool", "lookup", {"agent"})
        self.assertEqual([item.name for item in registry.catalog("agent")[0]], ["lookup"])
        self.assertEqual(registry.catalog("other"), ((), ()))
        registry.claim_source("remote", workflows={"agent"})
        registry.replace_source("remote", {("tool", "first"): ("one", Tool(lookup, name="first"))})
        registry.set_source_workflows("remote", {"other"})
        registry.replace_source("remote", {("tool", "first"): ("one", Tool(lookup, name="first")),
            ("tool", "second"): ("two", Tool(lookup, name="second"))})
        self.assertEqual([item.name for item in registry.catalog("other")[0]], ["first", "second"])
        registry.set_enabled("tool", "first", False)
        registry.set_workflows("tool", "second", set())
        registry.replace_source("remote", {})
        registry.replace_source("remote", {("tool", "first"): ("new-one", Tool(lookup, name="first")),
            ("tool", "second"): ("new-two", Tool(lookup, name="second"))})
        self.assertFalse(registry.has("tool", "first"))
        self.assertEqual(registry.catalog("other"), ((), ()))
        for scope in ("agent", {"*"}, {""}):
            with self.assertRaises((TypeError, ValueError)):
                registry.set_workflows("tool", "lookup", scope)


class ResourceBindingContracts:
    async def database_url(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        return "sqlite+aiosqlite:///" + str(Path(temporary.name) / "runtime.db")

    @asynccontextmanager
    async def host(self, workflows, provider, *, setup=None, url=None, on_close=None):
        @asynccontextmanager
        async def lifespan(resources):
            resources.agent_context.resources.register_provider("offline", provider)
            resources.agent_context.set_defaults(settings())
            if setup is not None:
                setup(resources.agent_context.resources)
            try:
                yield
            finally:
                if on_close is not None:
                    on_close()
        async with AutoAgentHost(workflows=workflows, lifespan=lifespan, environment={},
            settings=HostSettings(database_url=url or await self.database_url())) as host:
            yield host

    async def run_agent(self, host, workflow="agent"):
        return await host.join(await host.execute(workflow, AgentInput(message="task")))

    async def test_build_and_host_resources_form_a_scoped_union(self):
        """Each Workflow sees only its Build resources and Host resources explicitly granted to it."""
        def local(text: str) -> str:
            return text
        def shared(text: str) -> str:
            return text
        def hidden(text: str) -> str:
            self.fail("Unscoped Tool executed")
        provider = ScriptedProvider(answer(), answer())
        def setup(registry):
            registry.register_tool(shared, workflows={"agent"})
            registry.register_tool(hidden)
            registry.register_skill(skill("host-skill"), workflows={"other"})
        workflows = [agent_workflow(tools=[local], skills=[skill("local-skill")]), agent_workflow(workflow_id="other")]
        async with self.host(workflows, provider, setup=setup) as host:
            first = await self.run_agent(host)
            second = await self.run_agent(host, "other")
            self.assertEqual(first.status, "completed", first.error)
            self.assertEqual(second.status, "completed", second.error)
            names = {item.name for item in provider.requests[0].tools}
            self.assertTrue({"local", "shared", "skill_load"} <= names)
            self.assertNotIn("hidden", names)
            self.assertNotIn("host-skill", provider.requests[0].model_dump_json())
            self.assertNotIn("local-skill", provider.requests[1].model_dump_json())
            self.assertNotIn("shared", {item.name for item in provider.requests[1].tools})
            self.assertIn("host-skill", provider.requests[1].model_dump_json())

    async def test_same_tool_name_in_different_builds_dispatches_its_own_implementation(self):
        """Local names are scoped by Workflow Revision and cannot resolve another Build's Tool."""
        effects = []
        def first(text: str) -> str:
            effects.append("first")
            return text
        def second(text: str) -> str:
            effects.append("second")
            return text
        provider = ScriptedProvider(calls("lookup"), answer(), calls("lookup"), answer())
        workflows = [agent_workflow(tools=[Tool(first, name="lookup")]),
            agent_workflow(workflow_id="other", tools=[Tool(second, name="lookup")])]
        async with self.host(workflows, provider) as host:
            self.assertEqual((await self.run_agent(host)).status, "completed")
            self.assertEqual((await self.run_agent(host, "other")).status, "completed")
            self.assertEqual(effects, ["first", "second"])

    async def test_live_host_policy_and_revocation_resolve_pending_approval_without_effects(self):
        """A changed Host policy or withdrawn grant applies to an existing Wait before Tool execution."""
        for revoke in (False, True):
            effects = []
            def publish(text: str) -> str:
                effects.append(text)
                return text
            def setup(registry):
                registry.register_tool(publish, workflows={"agent"}, policy=lambda *args: ToolDecision(action="ask"))
            provider = ScriptedProvider(calls("publish"), answer())
            async with self.host([agent_workflow()], provider, setup=setup) as host:
                waiting = await self.run_agent(host)
                self.assertEqual(waiting.status, "waiting", waiting.error)
                if revoke:
                    host.agent_context.resources.set_workflows("tool", "publish", {"other"})
                else:
                    host.agent_context.resources.set_policy("tool", "publish", lambda *args: ToolDecision(action="deny"))
                current = await host.agent_context.get_approval(waiting.invocation_id, waiting.waits[0].id)
                self.assertFalse(current.requests)
                await host.respond(waiting.invocation_id, waiting.waits[0].id,
                    AgentApprovalResponse(decisions=(), revision=current.revision).model_dump(mode="json"))
                result = await host.join(waiting.invocation_id)
                self.assertEqual(result.status, "completed", result.error)
                self.assertFalse(effects)
                self.assertTrue(results(provider.requests[-1])[0].is_error)
        provider = ScriptedProvider(response(call("skill_load", {"name": "restricted"})), answer())
        def setup_skills(registry):
            registry.register_skill(skill("restricted"), workflows={"agent"},
                policy=lambda *args: ToolDecision(action="ask"))
        async with self.host([agent_workflow()], provider, setup=setup_skills) as host:
            waiting = await self.run_agent(host)
            self.assertEqual(waiting.status, "waiting", waiting.error)
            host.agent_context.resources.set_policy("skill", "restricted", lambda *args: ToolDecision(action="deny"))
            current = await host.agent_context.get_approval(waiting.invocation_id, waiting.waits[0].id)
            self.assertFalse(current.requests)
            await host.respond(waiting.invocation_id, waiting.waits[0].id,
                AgentApprovalResponse(decisions=(), revision=current.revision).model_dump(mode="json"))
            self.assertEqual((await host.join(waiting.invocation_id)).status, "completed")
            self.assertTrue(results(provider.requests[-1])[0].is_error)
            self.assertNotIn("Use restricted.", provider.requests[-1].model_dump_json())

    async def test_build_toolset_policy_survives_approval_and_restart(self):
        """Build-local ToolSet policies persist their resource identities through native Wait recovery."""
        effects, url = [], await self.database_url()
        def publish(text: str) -> str:
            effects.append(text)
            return text
        def require_approval(*args):
            return ToolDecision(action="ask")
        def workflow():
            return agent_workflow(tools=ToolSet([Tool(publish)], policy=require_approval))
        async with self.host([workflow()], ScriptedProvider(calls("publish")), url=url) as host:
            waiting = await self.run_agent(host)
            self.assertEqual(waiting.status, "waiting", waiting.error)
        async with self.host([workflow()], ScriptedProvider(answer()), url=url) as host:
            current = await host.agent_context.get_approval(waiting.invocation_id, waiting.waits[0].id)
            self.assertTrue(current.requests)
            self.assertTrue(current.bindings[0].resource_id.startswith("build:"))
            await host.respond(waiting.invocation_id, waiting.waits[0].id, AgentApprovalResponse(
                decisions=tuple(item.respond(approved=True) for item in current.requests), revision=current.revision).model_dump(mode="json"))
            self.assertEqual((await host.join(waiting.invocation_id)).status, "completed")
            self.assertEqual(effects, ["hello"])

    async def test_skill_directory_is_scoped_and_refreshes_without_host_registration(self):
        """Build Skill directories update their own catalog while unrelated Workflows cannot load them."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            visible = root / "visible"
            visible.mkdir()
            (visible / "SKILL.md").write_text("---\nname: visible\ndescription: Visible helper\n---\nOld instructions.\n")
            hidden = root / "hidden"
            hidden.mkdir()
            (hidden / "SKILL.md").write_text("---\nname: hidden\ndescription: Hidden helper\n---\nPrivate instructions.\n")
            source = SkillDirectory(root, interval_seconds=0.02, include=("visible",))
            provider = ScriptedProvider(answer(), response(call("skill_load", {"name": "visible"}, id="visible"),
                call("skill_load", {"name": "hidden"}, id="hidden")), answer(),
                response(call("skill_load", {"name": "visible"})), answer())
            async with self.host([agent_workflow(skills=[source]), agent_workflow(workflow_id="other")], provider) as host:
                first = await self.run_agent(host)
                snapshot = await host.agent_context.get_snapshot(first.invocation_id)
                _, before = host.agent_context.catalog(snapshot)
                (visible / "SKILL.md").write_text("---\nname: visible\ndescription: Visible helper\n---\nNew instructions.\n")
                async with asyncio.timeout(10):
                    while host.agent_context.catalog(snapshot)[1][0].snapshot_revision == before[0].snapshot_revision:
                        await asyncio.sleep(0.02)
                self.assertEqual((await self.run_agent(host)).status, "completed")
                self.assertIn("New instructions", provider.requests[2].model_dump_json())
                self.assertTrue(results(provider.requests[2])[1].is_error)
                self.assertNotIn("Private instructions", provider.requests[2].model_dump_json())
                self.assertEqual((await self.run_agent(host, "other")).status, "completed")
                self.assertNotIn("skill_load", {item.name for item in provider.requests[3].tools})
                self.assertTrue(results(provider.requests[4])[0].is_error)
                self.assertFalse(host.agent_context.resources.entries("skill"))

    async def test_live_same_name_collision_never_overrides_a_build_tool(self):
        """A Host grant introducing a duplicate model name fails before dispatching either executable."""
        def lookup(text: str) -> str:
            self.fail("Colliding Tool executed")
        provider = ScriptedProvider(answer())
        def setup(registry):
            registry.register_tool(lookup, workflows={"agent"})
        async with self.host([agent_workflow(tools=[lookup])], provider, setup=setup) as host:
            result = await self.run_agent(host)
            self.assertEqual(result.status, "failed")
            self.assertIn("collide", result.error.message)
            self.assertFalse(provider.requests)


class SQLiteResourceBindingTests(ResourceBindingContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLResourceBindingTests(ResourceBindingContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()


from tests.test_agent_mcp import WireServer, httpx2


@unittest.skipIf(httpx2 is None, "MCP SDK not installed")
class MCPBuildBindingTests(unittest.IsolatedAsyncioTestCase):
    database_url = ResourceBindingContracts.database_url
    host = ResourceBindingContracts.host
    run_agent = ResourceBindingContracts.run_agent

    async def test_build_mcp_factory_owns_connection_and_scopes_discovered_tools(self):
        """MCP discovery is lazy, scoped to its Build, and closes with the Runtime lifespan."""
        from autoagent.agent import MCPClient, MCPStreamableHTTPConfig
        clients, server = [], WireServer(legacy=True)
        @asynccontextmanager
        async def factory():
            async with httpx2.AsyncClient(transport=httpx2.MockTransport(server.handle)) as http:
                async with MCPClient(MCPStreamableHTTPConfig(server_id="build", url="https://server.test/mcp"),
                                     http_client=http) as client:
                    clients.append(client)
                    yield client
        source = MCPSource(name="remote", client_factory=factory, prefix="remote_", include=("add",))
        workflows = [agent_workflow(mcp=[source]), agent_workflow(workflow_id="other")]
        self.assertFalse(clients)
        provider = ScriptedProvider(response(call("remote_add", {"a": 1, "b": 2})), answer(), answer())
        def closed_before_project_exit():
            self.assertTrue(clients and clients[0]._closed)
        async with self.host(workflows, provider, on_close=closed_before_project_exit) as host:
            result = await self.run_agent(host)
            self.assertEqual(result.status, "completed", result.error)
            self.assertFalse(results(provider.requests[1])[0].is_error, results(provider.requests[1])[0])
            self.assertEqual((await self.run_agent(host, "other")).status, "completed")
            self.assertNotIn("remote_add", {item.name for item in provider.requests[2].tools})
            self.assertFalse(host.agent_context.resources.entries("tool"))
            self.assertEqual(len(clients), 1)
        self.assertTrue(clients[0]._closed)

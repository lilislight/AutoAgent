"""Caller-provided permission Context is fresh, scoped and absent from prompts."""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import tempfile
import unittest

from autoagent.agent import (
    AgentApprovalRequest, AgentApprovalResponse, AgentInput, AgentResources,
    LLMToolCall, LLMToolResult, Tool, ToolContext, ToolContextRequest,
    ToolPolicy, ToolSet, agent_workflow, provide_tool_context,
)
from autoagent.core import WorkflowCompiler
from autoagent.agent.context import restore
from autoagent.host import AutoAgentHost, HostSettings
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import ScriptedProvider, answer, calls, settings


class ToolContextProviderTests(unittest.IsolatedAsyncioTestCase):
    async def test_provider_cannot_replace_execution_boundaries_or_dependencies(self):
        """Injected identities extend facts while SDK restrictions and authorization remain bound."""
        call = LLMToolCall(name="read", call_id="one", arguments="{}")
        base = ToolContext(session_id="session", allowed_tools=frozenset({"read"}),
            services={"runtime": "trusted"}, authorization={"binding": "original"},
            approval_scope={"invocation_id": "invocation"}, attributes={"project": "one"})
        supplied = ToolContext(user_id="alice", allowed_tools=frozenset({"read", "write"}),
            services={"runtime": "replacement", "db": "connection"},
            authorization={"binding": "forged"}, attributes={"roles": ["reader"]})
        result = await provide_tool_context(supplied, ToolContextRequest(call, base, "preflight"))
        self.assertEqual(result.user_id, "alice")
        self.assertEqual(result.session_id, "session")
        self.assertEqual(result.allowed_tools, frozenset({"read"}))
        self.assertEqual(result.services["runtime"], "trusted")
        self.assertEqual(result.services["db"], "connection")
        self.assertEqual(result.authorization, base.authorization)
        self.assertEqual(result.approval_scope, base.approval_scope)
        self.assertEqual(result.attributes["roles"], ("reader",))
        self.assertEqual((result.tool_name, result.call_id), ("read", "one"))

    async def test_sync_async_and_invalid_providers(self):
        """Providers run with either method style; invalid results never become Allow."""
        call = LLMToolCall(name="read", call_id="one", arguments="{}")
        request = ToolContextRequest(call, ToolContext(session_id="session"), "execution")
        class Sync:
            def get_context(self, incoming):
                return ToolContext(user_id=incoming.session_id)
        class Async:
            async def get_context(self, incoming):
                await asyncio.sleep(0)
                return ToolContext(user_id=incoming.session_id)
        for provider in (Sync(), Async()):
            self.assertEqual((await provide_tool_context(provider, request)).user_id, "session")
        class Broken:
            def get_context(self, incoming):
                return {}
        with self.assertRaises(TypeError):
            await provide_tool_context(Broken(), request)
        with self.assertRaises(ValueError):
            await provide_tool_context(ToolContext(session_id="other"), request)

    async def test_changed_context_invalidates_an_existing_approval(self):
        """An Ask approval cannot be reused after the caller's permission facts change."""
        effects = []
        def publish(text: str) -> str:
            effects.append(text)
            return text
        tool = ToolSet([Tool(publish)], policy=ToolPolicy(default="ask"))
        call = LLMToolCall(name="publish", call_id="one", arguments='{"text":"hello"}')
        first = ToolContext(user_id="alice", attributes={"revision": 1})
        decision = await tool.acheck(call, context=first)
        reply = decision.approval_request.respond(approved=True)
        result = await tool.ainvoke(call, context=ToolContext(user_id="alice", attributes={"revision": 2}), approval=reply)
        self.assertEqual(result.structured_content["error"]["kind"], "tool_approval_mismatch")
        self.assertEqual(effects, [])

    async def test_builder_validates_sources_and_versions_fixed_permission_facts(self):
        """Runtime-only sources are typed and their permission identity changes the Revision."""
        with self.assertRaises(TypeError):
            agent_workflow(tool_context={})
        with self.assertRaises(TypeError):
            agent_workflow(tool_context_provider=lambda request: ToolContext())
        class Provider:
            def get_context(self, request):
                return ToolContext()
        with self.assertRaises(ValueError):
            agent_workflow(tool_context=ToolContext(), tool_context_provider=Provider())
        compiler = WorkflowCompiler()
        first = compiler.compile_or_raise(agent_workflow(tool_context=ToolContext(attributes={"role": "reader"})))
        second = compiler.compile_or_raise(agent_workflow(tool_context=ToolContext(attributes={"role": "writer"})))
        self.assertNotEqual(first.workflow_revision_id, second.workflow_revision_id)


class ToolContextWorkflowContracts:
    async def database_url(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        return "sqlite+aiosqlite:///" + str(Path(directory.name) / "runtime.db")

    @asynccontextmanager
    async def host(self, llm, tools, **options):
        @asynccontextmanager
        async def lifespan(setup):
            setup.providers.register("offline", llm)
            setup.extensions["agent"].context.set_defaults(settings())
            yield
        async with AutoAgentHost(workflows=[agent_workflow(resources=AgentResources(tools=tools), **options)],
                lifespan=lifespan, environment={}, settings=HostSettings(database_url=await self.database_url())) as host:
            yield host

    async def test_fixed_context_reaches_tool_policy_and_handler_without_entering_history(self):
        """Fixed caller facts authorize ordinary SDK Tools while prompts and output retain only business data."""
        seen = []
        def publish(text: str, context: ToolContext) -> str:
            seen.append((context.user_id, context.attributes["role"], "agent_snapshot" in context.services))
            return text
        policy = ToolPolicy(default="deny", rules=[dict(when=dict(
            field="context.attributes.role", op="equals", value="writer"), decision="allow")])
        tools = ToolSet([Tool(publish)], policy=policy)
        llm = ScriptedProvider(calls("publish"), answer())
        async with self.host(llm, tools, tool_context=ToolContext(user_id="private-user", attributes={"role": "writer"})) as host:
            result = await host.join(await host.execute("agent", AgentInput(message="Help")))
            self.assertEqual(result.status, "completed", result.error)
        self.assertEqual(seen, [("private-user", "writer", True)])
        self.assertNotIn("private-user", repr(llm.requests))

    async def test_provider_refreshes_between_classification_and_streaming_map_execution(self):
        """A live Context change can deny a previously allowed Map call before side effects."""
        effects, requests = [], []
        def publish(text: str) -> str:
            effects.append(text)
            return text
        class Provider:
            async def get_context(self, request):
                requests.append(request)
                return ToolContext(attributes={"permitted": request.phase == "preflight"})
        policy = ToolPolicy(default="deny", rules=[dict(when=dict(
            field="context.attributes.permitted", op="equals", value=True), decision="allow")])
        llm = ScriptedProvider(calls("publish"), answer())
        tools = ToolSet([Tool(publish)], policy=policy)
        async with self.host(llm, tools, tool_context_provider=Provider(), user_events=True) as host:
            result = await host.join(await host.execute("agent", AgentInput(message="Help")))
            self.assertEqual(result.status, "completed", result.error)
        self.assertEqual(effects, [])
        self.assertEqual({request.phase for request in requests}, {"preflight", "execution"})
        self.assertTrue(all(request.workflow_id == "agent" and request.invocation_id == result.invocation_id
            and request.session_id == result.session_id for request in requests))
        observed = next(item for item in llm.requests[-1].input if isinstance(item, LLMToolResult))
        self.assertEqual(observed.structured_content["error"]["kind"], "tool_access_denied")

    async def test_provider_is_used_during_native_approval_review(self):
        """Approval projections and approved execution obtain caller facts through the same SDK interface."""
        phases, effects = [], []
        class Provider:
            def get_context(self, request):
                phases.append(request.phase)
                return ToolContext(user_id="alice", attributes={"tenant": "one"})
        def publish(text: str) -> str:
            effects.append(text)
            return text
        tools = ToolSet([Tool(publish)], policy=ToolPolicy(default="ask"))
        llm = ScriptedProvider(calls("publish"), answer())
        async with self.host(llm, tools, tool_context_provider=Provider()) as host:
            waiting = await host.join(await host.execute("agent", AgentInput(message="Help")))
            self.assertEqual(waiting.status, "waiting", waiting.error)
            batch = restore(AgentApprovalRequest, waiting.waits[0].request)
            self.assertEqual(batch.requests[0].user_id, "alice")
            reply = AgentApprovalResponse(decisions=tuple(item.respond(approved=True) for item in batch.requests),
                                          revision=batch.revision)
            await host.respond(waiting.invocation_id, waiting.waits[0].id, reply.model_dump(mode="json"))
            result = await host.join(waiting.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
        self.assertEqual(effects, ["hello"])
        self.assertEqual(set(phases), {"preflight", "approval", "execution"})


class SQLiteToolContextWorkflowTests(ToolContextWorkflowContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLToolContextWorkflowTests(ToolContextWorkflowContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

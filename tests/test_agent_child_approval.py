"""Child approval identity boundaries and continuation of durable pending batches."""

import asyncio
from dataclasses import replace
import unittest
from unittest.mock import patch

import httpx

from autoagent.agent import AgentAccess, AgentApprovalRequest, AgentInput, Tool, ToolContext, ToolSet
from autoagent.agent.context import restore
from autoagent.agent.tools.context import tool_context
from autoagent.core import OperatorCallContext
from autoagent.server import create_app
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_subagents import SubagentContracts, RoutedProvider, call, response, results
from tests.test_agent_workflow import answer, settings


class ChildApprovalContracts:
    host = SubagentContracts.host
    database_url = SubagentContracts.database_url

    async def approval_wait(self, host, root):
        async with asyncio.timeout(30):
            while True:
                state = await host.join(root)
                wait = next((wait for wait in state.waits if wait.kind == "external"), None)
                if wait is not None:
                    return state, wait
                self.assertNotIn(state.status, ("failed", "completed", "cancelled"), state.error)
                await asyncio.sleep(0.01)

    async def test_child_tool_uses_its_own_context_and_stable_approval_identity(self):
        """Spawn isolates transient Tool state while child settings and approvals remain consistent."""
        effects, child_requests = [], []

        def publish(text: str, context: ToolContext) -> str:
            effects.append((text, context.session_id, context.approval_scope["invocation_id"]))
            return text

        def child(request):
            child_requests.append(request)
            self.assertIsNone(ToolContext.current())
            return response(call("publish", {"text": "saved"})) if not results(request) else answer("child done")

        provider = RoutedProvider([response(call("spawn_agent", {"type": "worker", "task": "child task"})),
                                   answer("root body done")], child)
        async with self.host(provider, defaults=settings(access=AgentAccess(rules={"publish": "ask"})),
                             tools=(Tool(publish),)) as host:
            root = await host.execute("agent", AgentInput(message="root task"), session_id="parent")
            _, wait = await self.approval_wait(host, root)
            raw = (await host.get_execution_value(wait.request["invocation_id"], "wait", wait.id, "request"))["value"]
            original = restore(AgentApprovalRequest, raw)
            child_id = original.invocation_id
            snapshot = await host.agent_context.get_snapshot(child_id)
            self.assertNotEqual(snapshot.session_id, "parent")
            self.assertEqual(original.requests[0].session_id, snapshot.session_id)
            self.assertFalse(effects)
            # Even a caller carrying another Tool scope must get the same view.
            with tool_context(ToolContext(session_id="unrelated", allowed_tools=frozenset())):
                spec = await host.get_execution_action_spec(root.invocation_id, "respond",
                    target_invocation_id=child_id, wait_id=wait.id)
            self.assertEqual(spec["request"]["requests"][0]["id"], original.requests[0].id)
            reply = spec["response_example"]
            self.assertFalse(reply["decisions"][0]["approved"])
            reply["decisions"][0]["approved"] = True
            await host.perform_execution_action(root.invocation_id, "respond", target_invocation_id=child_id,
                wait_id=wait.id, payload=reply)
            result = await host.join(root)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(effects, [("saved", snapshot.session_id, child_id)])
            self.assertEqual(len(child_requests), 2)

    async def test_existing_polluted_child_wait_resumes_after_restart_without_replaying_parent(self):
        """Current HTTP approval view can resolve an old mismatched batch in the original Invocation."""
        url, effects = await self.database_url(), []

        def publish(text: str, context: ToolContext) -> str:
            effects.append((text, context.session_id))
            return text

        original_request = ToolSet._request

        def legacy_request(call, prepared, reason=None):
            current = OperatorCallContext.current()
            if current is not None and current.workflow_id == "worker" and current.node_id == "tool_branch":
                prepared = replace(prepared, context=ToolContext(session_id="parent").for_call(call, prepared.record))
            return original_request(call, prepared, reason)

        provider = RoutedProvider([response(call("spawn_agent", {"type": "worker", "task": "child task"})),
                                   answer("root body done")], lambda request: response(call("publish", {"text": "saved"})))
        async with self.host(provider, url=url, defaults=settings(access=AgentAccess(rules={"publish": "ask"})),
                             tools=(Tool(publish),)) as host:
            with patch.object(ToolSet, "_request", staticmethod(legacy_request)):
                root = await host.execute("agent", AgentInput(message="root task"), session_id="parent")
                _, wait = await self.approval_wait(host, root)
            raw = (await host.get_execution_value(wait.request["invocation_id"], "wait", wait.id, "request"))["value"]
            old = restore(AgentApprovalRequest, raw)
            self.assertEqual(old.requests[0].session_id, "parent")
            self.assertNotEqual((await host.agent_context.get_snapshot(old.invocation_id)).session_id, "parent")
            self.assertFalse(effects)

        resumed = RoutedProvider([], lambda request: answer("child finished after approval"))
        async with self.host(resumed, url=url, tools=(Tool(publish),)) as host:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(host, ui=False)),
                                         base_url="http://test") as client:
                params = dict(invocation_id=root.invocation_id, action="respond",
                    target_invocation_id=old.invocation_id, wait_id=wait.id)
                fetched = await client.get("/api/v2/execution-action-spec", params=params)
                self.assertEqual(fetched.status_code, 200, fetched.text)
                spec = fetched.json()
                self.assertNotEqual(spec["request"]["requests"][0]["id"], old.requests[0].id)
                self.assertTrue(spec["request"]["revision"])
                self.assertFalse(effects)
                raw_after = (await host.get_execution_value(old.invocation_id, "wait", wait.id, "request"))["value"]
                self.assertEqual(raw_after, raw)
                reply = spec["response_example"]
                reply["decisions"][0]["approved"] = True
                await host.perform_execution_action(root.invocation_id, "respond", target_invocation_id=old.invocation_id,
                    wait_id=wait.id, payload=reply)
                result = await host.join(root.invocation_id)
                self.assertEqual(result.status, "completed", result.error)
                self.assertEqual(result.invocation_id, root.invocation_id)
                self.assertEqual(len(effects), 1)
                snapshot = await host.agent_context.get_snapshot(old.invocation_id)
                self.assertEqual(effects[0], ("saved", snapshot.session_id))
                self.assertEqual(len(resumed.requests), 1)


class SQLiteChildApprovalTests(ChildApprovalContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLChildApprovalTests(ChildApprovalContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

"""Child approval identity boundaries and continuation of durable pending batches."""

import asyncio
import unittest
from unittest.mock import AsyncMock


from autoagent.agent import AgentAccess, AgentApprovalRequest, AgentInput, Tool, ToolContext
from autoagent.agent.context import restore
from autoagent.agent.tools.context import tool_context
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_subagents import SubagentContracts, RoutedProvider, call, response, results
from tests.test_agent_workflow import answer, settings


class ApprovalSchedulingTests(unittest.IsolatedAsyncioTestCase):
    async def test_detached_reevaluation_has_fresh_scope_and_one_root_per_family(self):
        """Notifications outlive their Operator scope and visit a shared Root only once."""
        from types import SimpleNamespace
        from autoagent.core.operators.execution import _sync_execution_scope, run_sync
        from autoagent.agent_app.approvals import AgentApprovals
        ref = SimpleNamespace(invocation_id='root-call', workflow_revision_id='agent:1')
        runtime = SimpleNamespace(resident={'root':ref}, owners={'child-a':'root', 'child-b':'root'},
            app=SimpleNamespace(astatus=AsyncMock(return_value='status')))
        service = SimpleNamespace(services=SimpleNamespace(runtime=runtime))
        service.for_workflow = lambda revision: service
        approvals = AgentApprovals(service)
        async def evaluate(root, status, *, join):
            self.assertIs(root, ref)
            self.assertEqual(status, 'status')
            self.assertFalse(join)
            self.assertEqual(await run_sync(lambda: 42), 42)
        approvals.reconcile = AsyncMock(side_effect=evaluate)
        async with approvals.runtime_scope():
            with _sync_execution_scope(lambda *_: self.fail('Borrowed the notifying Operator executor')):
                await approvals.changed()
            await asyncio.gather(*tuple(approvals.tasks))
            approvals.reconcile.assert_awaited_once()


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
            snapshot = await host.extensions["agent"].context.get_snapshot(child_id)
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


    async def test_child_mode_change_reevaluates_through_resident_root(self):
        """Explicit SDK Root recovery reevaluates a child approval using its current settings."""
        effects = []
        def publish(text: str) -> str:
            effects.append(text)
            return text
        provider = RoutedProvider([response(call('spawn_agent', {'type':'worker', 'task':'child task'})),
            answer('root body done')], lambda request: answer('child done') if results(request)
                else response(call('publish', {'text':'saved'})))
        async with self.host(provider, defaults=settings(access=AgentAccess(rules={'publish':'ask'})),
            tools=(Tool(publish),)) as host:
            root = await host.execute('agent', AgentInput(message='root task'), session_id='parent')
            _, wait = await self.approval_wait(host, root)
            snapshot = await host.extensions["agent"].context.get_snapshot(wait.request['invocation_id'])
            self.assertNotEqual(snapshot.session_id, 'parent')
            await host.pause(root)
            await host.extensions["agent"].context.configure(snapshot.session_id,
                snapshot.settings.session_defaults().model_copy(update={'approval_mode':'auto_approve'}))
            await host.recover(root)
            async with asyncio.timeout(10):
                while not effects:
                    await asyncio.sleep(.02)
            self.assertEqual(effects, ['saved'])
            self.assertEqual((await host.join(root)).status, 'completed')


class SQLiteChildApprovalTests(ChildApprovalContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLChildApprovalTests(ChildApprovalContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

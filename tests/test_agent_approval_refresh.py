"""Approval reevaluation across explicit resume, resource changes and restart."""

from contextlib import asynccontextmanager
import asyncio
import unittest

from autoagent.agent import AgentAccess, AgentApprovalRequest, AgentApprovalResponse, LLMToolResult, Tool
from autoagent.agent.context import restore
from autoagent.agent_app.models import ApplicationSessionSettings
from autoagent.host import HostRunConflict
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import AgentWorkflowContracts, ScriptedProvider, answer, calls, settings


class ApprovalRefreshContracts:
    @asynccontextmanager
    async def host(self, provider, *, defaults=None, tools=(), policies=(), url=None, **options):
        from autoagent.agent import AgentResources
        from autoagent.agent_app import builtin_agent_workflow
        from autoagent.host import AutoAgentHost, HostSettings
        from contextlib import asynccontextmanager
        resources = AgentResources(tools=tools)
        @asynccontextmanager
        async def lifespan(setup):
            setup.providers.register("offline", provider)
            context = setup.applications["agent"].context
            context.set_defaults(defaults or settings())
            for name, policy in policies:
                context.resources.register_policy(name, policy)
            yield
        async with AutoAgentHost(workflows=[builtin_agent_workflow(resources=resources, execution=False)],
                lifespan=lifespan, settings=HostSettings(database_url=url or await self.database_url()), environment={}) as host:
            yield host

    async def run_agent(self, host, **options):
        from autoagent.agent import AgentInput
        return await host.join(await host.execute("autoagent.agent", AgentInput(message="Help"), **options))
    publish = AgentWorkflowContracts.publish
    database_url = AgentWorkflowContracts.database_url

    async def test_action_view_distinguishes_manual_and_automatic_review(self):
        """A paused automatic batch remains durable without requesting a human response."""
        effects = []
        async with self.host(ScriptedProvider(calls("publish"), answer()),
                defaults=settings(access=AgentAccess(default="ask")), tools=(self.publish(effects),)) as host:
            waiting = await self.run_agent(host)
            wait_id = waiting.waits[0].id
            view = await host.get_execution_action_spec(waiting.invocation_id, "respond", wait_id=wait_id)
            self.assertEqual(view["review"], {"reviewer": "user", "requires_user_input": True})
            await host.pause(waiting.invocation_id)
            await host.applications["agent"].context.configure(waiting.session_id,
                settings(mode="auto_approve").session_defaults())
            view = await host.get_execution_action_spec(waiting.invocation_id, "respond", wait_id=wait_id)
            self.assertEqual(view["review"], {"reviewer": "system", "requires_user_input": False})
            self.assertEqual(view["request"]["requests"], [])
            self.assertEqual(view["request"]["automatic"][0]["source"], "system")
            self.assertFalse(effects)
            result = await host.recover(waiting.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(effects, ["hello"])
            with self.assertRaises(HostRunConflict) as resolved:
                await host.get_execution_action_spec(waiting.invocation_id, "respond", wait_id=wait_id)
            self.assertEqual(resolved.exception.code, "wait_unavailable")

    async def test_resume_auto_approval_consumes_wait_without_user_reply(self):
        """Explicit resume automatically resolves Ask after adopting auto approval."""
        effects = []
        provider = ScriptedProvider(calls("publish"), answer())
        async with self.host(provider, defaults=settings(access=AgentAccess(rules={"publish": "ask"})),
                             tools=(self.publish(effects),)) as host:
            waiting = await self.run_agent(host)
            await host.pause(waiting.invocation_id)
            await host.applications["agent"].context.configure(waiting.session_id, ApplicationSessionSettings.from_runtime(settings(mode="auto_approve", model="new-model")))
            result = await host.recover(waiting.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            self.assertFalse(result.waits)
            self.assertEqual(effects, ["hello"])
            self.assertEqual(provider.requests[-1].model, "model-a")
            with self.assertRaises(Exception):
                await host.respond(waiting.invocation_id, waiting.waits[0].id,
                                   AgentApprovalResponse(decisions=()).model_dump(mode="json"))

    async def test_resume_hard_deny_does_not_execute(self):
        """Automatic resolutions distinguish policy denial from a human rejection."""
        for mode, access in (("policy", AgentAccess(rules={"publish": "deny"})),
                             ("auto_approve", AgentAccess(rules={"publish": "deny"}))):
            effects = []
            provider = ScriptedProvider(calls("publish"), answer())
            async with self.host(provider, defaults=settings(access=AgentAccess(rules={"publish": "ask"})),
                                 tools=(self.publish(effects),)) as host:
                waiting = await self.run_agent(host)
                await host.pause(waiting.invocation_id)
                host.applications["agent"].context.set_defaults(settings(access=access))
                await host.applications["agent"].context.configure(waiting.session_id, ApplicationSessionSettings.from_runtime(settings(mode=mode)))
                result = await host.recover(waiting.invocation_id)
                self.assertEqual(result.status, "completed", result.error)
                self.assertFalse(effects)
                observation = next(item for item in provider.requests[-1].input if isinstance(item, LLMToolResult))
                self.assertTrue(observation.is_error)
                self.assertIn("tool_access_denied", str(observation.model_dump()))

    async def test_accepted_user_denial_is_preserved_when_enabling_auto_approval(self):
        """A user rejection accepted while paused cannot become an automatic approval on resume."""
        effects = []
        provider = ScriptedProvider(calls("publish"), answer())
        async with self.host(provider, defaults=settings(access=AgentAccess(default="ask")),
                             tools=(self.publish(effects),)) as host:
            waiting = await self.run_agent(host)
            await host.pause(waiting.invocation_id)
            batch = await host.applications["agent"].context.get_approval(waiting.invocation_id, waiting.waits[0].id)
            reply = AgentApprovalResponse(decisions=(batch.requests[0].respond(approved=False),), revision=batch.revision)
            await host.respond(waiting.invocation_id, waiting.waits[0].id, reply.model_dump(mode="json"))
            await host.applications["agent"].context.configure(waiting.session_id, ApplicationSessionSettings.from_runtime(settings(mode="auto_approve")))
            result = await host.recover(waiting.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            self.assertFalse(effects)
            observation = next(item for item in provider.requests[-1].input if isinstance(item, LLMToolResult))
            self.assertIn("tool_approval_denied", str(observation.model_dump()))

    async def test_mixed_batch_refresh_rejects_old_reply_and_joins_original_order(self):
        """A retained Wait exposes fresh Ask only and merges automatic results durably."""
        effects = []
        def second(text: str) -> str:
            effects.append("second")
            return text
        def third(text: str) -> str:
            effects.append("third")
            return text
        provider = ScriptedProvider(calls("publish", "second", "third"), answer())
        async with self.host(provider, defaults=settings(access=AgentAccess(default="ask")),
                             tools=(self.publish(effects), Tool(second), Tool(third))) as host:
            waiting = await self.run_agent(host)
            original = restore(AgentApprovalRequest, waiting.waits[0].request)
            old_reply = AgentApprovalResponse(decisions=tuple(item.respond(approved=True) for item in original.requests),
                                              revision=original.revision)
            await host.pause(waiting.invocation_id)
            host.applications["agent"].context.set_defaults(settings(access=AgentAccess(
                rules={"publish": "allow", "second": "deny", "third": "ask"})))
            resumed = await host.recover(waiting.invocation_id)
            self.assertEqual(resumed.status, "waiting", resumed.error)
            self.assertEqual(resumed.waits[0].id, waiting.waits[0].id)
            current = await host.applications["agent"].context.get_approval(waiting.invocation_id, waiting.waits[0].id)
            self.assertEqual([item.call.name for item in current.requests], ["third"])
            self.assertEqual(len(current.automatic), 2)
            view = await host.get_execution_action_spec(waiting.invocation_id, "respond", wait_id=waiting.waits[0].id)
            self.assertEqual(view["review"], {"reviewer": "user", "requires_user_input": True})
            self.assertFalse(effects)
            with self.assertRaises(HostRunConflict):
                await host.respond(waiting.invocation_id, waiting.waits[0].id, old_reply.model_dump(mode="json"))
            reply = AgentApprovalResponse(decisions=(current.requests[0].respond(approved=True),), revision=current.revision)
            await host.respond(waiting.invocation_id, waiting.waits[0].id, reply.model_dump(mode="json"))
            result = await host.join(waiting.invocation_id)
            self.assertEqual(result.status, "completed", result.error)
            self.assertCountEqual(effects, ["hello", "third"])
            results = [item for item in provider.requests[-1].input if isinstance(item, LLMToolResult)]
            self.assertEqual([item.call_id for item in results], ["call-0", "call-1", "call-2"])
            self.assertEqual([item.is_error for item in results], [False, True, False])

    async def test_resume_removed_or_changed_tool_generates_observation(self):
        """A resumed approval never silently borrows a removed or replacement executable."""
        for replace in (False, True):
            effects = []
            provider = ScriptedProvider(calls("publish"), answer())
            async with self.host(provider, defaults=settings(access=AgentAccess(default="ask")),
                                 tools=(self.publish(effects),)) as host:
                waiting = await self.run_agent(host)
                await host.pause(waiting.invocation_id)
                if replace:
                    host.applications["agent"].context.resources.register_tool(self.publish(effects), version="2", replace=True, workflows={"agent"})
                else:
                    host.applications["agent"].context.resources.unregister("tool", "publish")
                await host.applications["agent"].context.configure(waiting.session_id, ApplicationSessionSettings.from_runtime(settings(mode="auto_approve")))
                result = await host.recover(waiting.invocation_id)
                self.assertEqual(result.status, "completed", result.error)
                self.assertFalse(effects)
                observation = next(item for item in provider.requests[-1].input if isinstance(item, LLMToolResult))
                self.assertIn("tool_binding_changed" if replace else "tool_unavailable", str(observation.model_dump()))

    async def test_product_restart_cancels_old_approval_wait(self):
        """A product restart stops old tasks instead of resuming an earlier approval."""
        url, effects = await self.database_url(), []
        async with self.host(ScriptedProvider(calls("publish")), url=url,
                             defaults=settings(access=AgentAccess(default="ask")),
                             tools=(self.publish(effects),)) as host:
            waiting = await self.run_agent(host)
            await host.pause(waiting.invocation_id)
            await host.applications["agent"].context.configure(waiting.session_id,
                settings(model="new-model", access=AgentAccess(default="ask")).session_defaults())
            resumed = await host.recover(waiting.invocation_id)
            current = restore(AgentApprovalRequest, resumed.waits[0].request)
            self.assertEqual(current.requests[0].id,
                                restore(AgentApprovalRequest, waiting.waits[0].request).requests[0].id)
        provider = ScriptedProvider(answer())
        async with self.host(provider, url=url, defaults=settings(access=AgentAccess(default="ask")), tools=(self.publish(effects),)) as host:
            stopped = await host.get_execution(waiting.invocation_id)
            self.assertEqual(stopped.status, "cancelled")
            controls = await host.get_execution_controls(waiting.invocation_id)
            self.assertFalse(controls["actions"]["recover"]["available"])
            self.assertEqual(effects, [])

    async def test_external_reply_cannot_supply_policy_resolution(self):
        """Only Host may fill automatic decisions or inject Tool observations into a Wait reply."""
        effects = []
        async with self.host(ScriptedProvider(calls("publish")),
                             defaults=settings(access=AgentAccess(default="ask")), tools=(self.publish(effects),)) as host:
            waiting = await self.run_agent(host)
            request = restore(AgentApprovalRequest, waiting.waits[0].request)
            with self.assertRaises(ValueError):
                await host.respond(waiting.invocation_id, waiting.waits[0].id, {
                    "decisions": [], "source": "policy", "resolutions": [{
                        "request_id": request.requests[0].id, "source": "policy"}]})
            self.assertFalse(effects)
            self.assertEqual((await host.get_execution(waiting.invocation_id)).status, "waiting")

    async def test_concurrent_replies_consume_batch_once(self):
        """Two valid replies cannot dispatch the protected side effect twice."""
        effects = []
        async with self.host(ScriptedProvider(calls("publish"), answer()),
                             defaults=settings(access=AgentAccess(default="ask")), tools=(self.publish(effects),)) as host:
            waiting = await self.run_agent(host)
            request = restore(AgentApprovalRequest, waiting.waits[0].request)
            reply = AgentApprovalResponse(decisions=(request.requests[0].respond(approved=True),), revision=request.revision)
            replies = await asyncio.gather(*(host.respond(waiting.invocation_id, waiting.waits[0].id,
                reply.model_dump(mode="json")) for _ in range(2)), return_exceptions=True)
            self.assertEqual(sum(isinstance(value, Exception) for value in replies), 1)
            self.assertEqual((await host.join(waiting.invocation_id)).status, "completed")
            self.assertEqual(effects, ["hello"])

    async def test_resource_change_during_policy_does_not_consume_wait(self):
        """An asynchronous reevaluation cannot approve a catalog that changes under it."""
        effects, armed, service = [], False, None
        async def policy(call, context):
            nonlocal armed
            if armed:
                armed = False
                service.resources.set_enabled("tool", "publish", True)
                await asyncio.sleep(0)
            return {"action": "ask"}
        async with self.host(ScriptedProvider(calls("publish"), answer()),
                             defaults=settings(access=AgentAccess(policy="dynamic")),
                             policies=(("dynamic", policy),), tools=(self.publish(effects),)) as host:
            service = host.applications["agent"].context
            waiting = await self.run_agent(host)
            request = restore(AgentApprovalRequest, waiting.waits[0].request)
            reply = AgentApprovalResponse(decisions=(request.requests[0].respond(approved=True),), revision=request.revision)
            armed = True
            with self.assertRaises(HostRunConflict):
                await host.respond(waiting.invocation_id, waiting.waits[0].id, reply.model_dump(mode="json"))
            self.assertFalse(effects)
            current = await service.get_approval(waiting.invocation_id, waiting.waits[0].id)
            reply = AgentApprovalResponse(decisions=(current.requests[0].respond(approved=True),), revision=current.revision)
            await host.respond(waiting.invocation_id, waiting.waits[0].id, reply.model_dump(mode="json"))
            self.assertEqual((await host.join(waiting.invocation_id)).status, "completed")
            self.assertEqual(effects, ["hello"])


class SQLiteApprovalRefreshTests(ApprovalRefreshContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLApprovalRefreshTests(ApprovalRefreshContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

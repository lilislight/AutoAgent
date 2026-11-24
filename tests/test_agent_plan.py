"""Session plans commit with results and remain separate from compacted history."""

import asyncio
import json
from threading import Event
import unittest

from autoagent.agent import (AgentInput, Conversation, LLMResponse, LLMToolCall,
    LLMMessage, TextContent, PlanTools, SessionPlan, ToolPolicy)
from autoagent.agent.context import restore
from tests.test_agent_workflow import AgentWorkflowContracts, ScriptedProvider, answer
from tests.test_builtin_agent import BuiltinApplicationContracts
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase


def update(*statuses, call_id="plan", steps=None):
    return LLMResponse(model="offline", finish_reason="tool_calls", output=(LLMToolCall(
        name="update_plan", call_id=call_id, arguments=json.dumps({"plan": [
            {"step": step, "status": status} for step, status in zip(
                steps or ["Inspect", "Implement", "Verify"], statuses)]})),))


class PlanContracts:
    host = AgentWorkflowContracts.host
    database_url = AgentWorkflowContracts.database_url
    run_agent = AgentWorkflowContracts.run_agent

    def conversation(self, host, sid):
        return restore(Conversation, host._runtime.repository.state(sid).session.context["agent_conversation"])

    async def test_retained_completion_next_invocation_and_clear(self):
        """Completed plans remain current across tasks until explicitly replaced or cleared."""
        provider = ScriptedProvider(update("in_progress", "pending"), answer(),
            update("completed", "completed"), answer(), answer(), update(), answer())
        async with self.host(provider, tools=PlanTools().tools()) as host:
            first = await self.run_agent(host)
            self.assertEqual(first.status, "completed", first.error)
            sid = first.session_id
            self.assertEqual(self.conversation(host, sid).plan.plan[0].status, "in_progress")
            finished = await self.run_agent(host, session_id=sid)
            self.assertEqual(finished.status, "completed", finished.error)
            plan = self.conversation(host, sid).plan
            self.assertTrue(all(item.status == "completed" for item in plan.plan))
            await self.run_agent(host, session_id=sid)
            self.assertEqual(self.conversation(host, sid).plan, plan)
            final = await self.run_agent(host, session_id=sid)
            self.assertEqual(final.status, "completed", final.error)
            self.assertIsNone(self.conversation(host, sid).plan)
            self.assertIn("Runtime plan snapshot", provider.requests[1].input[-1].content[0].text)
            self.assertIn('"status":"in_progress"', provider.requests[1].input[-1].content[0].text)
            self.assertIn("\nnull\n", provider.requests[-1].input[-1].content[0].text)
            # Request records are not extra visible messages.
            self.assertFalse(any("Runtime plan snapshot" in getattr(part, "text", "")
                for item in self.conversation(host, sid).items for part in getattr(item, "content", ())))

    async def test_no_plan_resource_no_runtime_record(self):
        """Ordinary SDK Agents do not gain plan tools or runtime records implicitly."""
        provider = ScriptedProvider(answer())
        async with self.host(provider) as host:
            result = await self.run_agent(host)
            self.assertEqual(result.status, "completed", result.error)
            self.assertFalse(any(tool.name == "update_plan" for tool in provider.requests[0].tools))
            self.assertEqual(self.conversation(host, result.session_id).runtime_plans, {})

    async def test_duplicate_and_invalid_updates_are_observations(self):
        """Parallel plan calls cannot race and invalid plans leave current state intact."""
        first = update("in_progress", "pending")
        duplicate = first.model_copy(update={"output": (*first.output,
            update("completed", call_id="duplicate").output[0])})
        provider = ScriptedProvider(duplicate, update("in_progress", "in_progress"), answer())
        async with self.host(provider, tools=PlanTools().tools()) as host:
            result = await self.run_agent(host)
            self.assertEqual(result.status, "completed", result.error)
            history = self.conversation(host, result.session_id)
            self.assertEqual([item.status for item in history.plan.plan], ["in_progress", "pending"])
            errors = [item.structured_content["error"]["kind"] for item in history.items
                if getattr(item, "is_error", False)]
            self.assertEqual(errors, ["duplicate_plan_update", "invalid_plan"])

    async def test_plan_tool_uses_unified_permissions(self):
        """A denied plan update cannot mutate Session plan state."""
        provider = ScriptedProvider(update("in_progress"), answer())
        async with self.host(provider, tools=PlanTools(policy=ToolPolicy(default="deny")).tools()) as host:
            result = await self.run_agent(host)
            self.assertEqual(result.status, "completed", result.error)
            self.assertIsNone(self.conversation(host, result.session_id).plan)

    async def test_restart_preserves_plan_and_request_prefix(self):
        """A fresh Host restores the plan and replays exactly the snapshots previously sent."""
        url = await self.database_url()
        provider = ScriptedProvider(update("in_progress", "pending"), answer(), answer())
        async with self.host(provider, tools=PlanTools().tools(), url=url, inject_current_time=True) as host:
            first = await self.run_agent(host)
            self.assertEqual(first.status, "completed", first.error)
            saved = self.conversation(host, first.session_id).plan
        async with self.host(provider, tools=PlanTools().tools(), url=url, inject_current_time=True) as host:
            second = await self.run_agent(host, session_id=first.session_id)
            self.assertEqual(second.status, "completed", second.error)
            self.assertEqual(self.conversation(host, first.session_id).plan, saved)
        for previous, current in zip(provider.requests, provider.requests[1:]):
            self.assertEqual(current.input[:len(previous.input)], previous.input)

    async def test_cancel_and_failure_keep_committed_plan(self):
        """Cancellation or model failure cannot discard an already committed plan update."""
        for cancel in (True, False):
            entered = Event()
            async def stop(request):
                entered.set()
                if cancel:
                    await asyncio.Event().wait()
                raise RuntimeError("simulated model failure")
            provider = ScriptedProvider(update("in_progress", "pending"), stop, answer())
            async with self.host(provider, tools=PlanTools().tools()) as host:
                execution = await host.execute("agent", AgentInput(message="Work"))
                self.assertTrue(await asyncio.to_thread(entered.wait, 15))
                if cancel:
                    await host.cancel(execution)
                result = await host.join(execution)
                self.assertEqual(result.status, "cancelled" if cancel else "failed")
                saved = self.conversation(host, result.session_id).plan
                self.assertEqual(saved.plan[0].status, "in_progress")
                next_turn = await self.run_agent(host, session_id=result.session_id)
                self.assertEqual(next_turn.status, "completed", next_turn.error)
                self.assertEqual(self.conversation(host, result.session_id).plan, saved)

    async def test_pause_recovery_preserves_current_plan(self):
        """Pausing a live model call and recovering retains the current Session plan."""
        entered, release = Event(), Event()
        async def slow(request):
            entered.set()
            await asyncio.to_thread(release.wait, 15)
            return answer()
        provider = ScriptedProvider(update("in_progress", "pending"), slow)
        async with self.host(provider, tools=PlanTools().tools()) as host:
            execution = await host.execute("agent", AgentInput(message="Work"))
            self.assertTrue(await asyncio.to_thread(entered.wait, 15))
            pause = asyncio.create_task(host.pause(execution, timeout=15))
            try:
                for _ in range(100):
                    controls = await host.get_execution_controls(execution.invocation_id)
                    if controls["control_state"] == "pausing":
                        break
                    await asyncio.sleep(.01)
            finally:
                release.set()
            paused = await pause
            self.assertEqual(paused.control_state, "paused")
            before = self.conversation(host, execution.session_id).plan
            result = await host.recover(execution)
            self.assertEqual(result.status, "completed", result.error)
            self.assertEqual(self.conversation(host, result.session_id).plan, before)


class PlanContextTests(unittest.TestCase):
    def test_compaction_retains_current_plan_and_discards_replay_snapshots(self):
        """Compaction discards injected copies without deleting current Session plan state."""
        plan = SessionPlan.model_validate_json('{"plan":[{"step":"Ship","status":"completed"}]}')
        history = Conversation(items=(LLMMessage(role="user", content=(TextContent(text="Work"),)),),
            plan=plan, runtime_plans={"1": plan})
        replacement = history.replace(0, 1, (LLMMessage(role="assistant", content=(TextContent(text="Summary"),)),))
        self.assertEqual(replacement.plan, plan)
        self.assertEqual(replacement.runtime_plans, {})

    def test_current_plan_counts_toward_input_budget(self):
        """The hidden current plan consumes request budget even without clock injection."""
        from autoagent.agent.workflow.models import ContextPreparation
        from autoagent.agent import UsagePlusCharacters
        from autoagent.agent.workflow.runtime_context import runtime_characters
        plan = SessionPlan.model_validate_json('{"plan":[{"step":"Ship","status":"pending"}]}')
        prep = ContextPreparation(conversation=Conversation(plan=plan), fingerprint="test", snapshot_id="test")
        with_plan = prep.model_copy(update={"inject_plan": True})
        self.assertGreater(runtime_characters(with_plan), 0)
        self.assertGreater(UsagePlusCharacters()(with_plan).input_tokens, UsagePlusCharacters()(prep).input_tokens)


class SQLitePlanTests(PlanContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLPlanTests(PlanContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()


class AppPlanContracts:
    database_url = BuiltinApplicationContracts.database_url
    project = BuiltinApplicationContracts.project

    async def test_api_and_fork_read_historical_plan(self):
        """The read-only API and a draft Fork use the selected reply's plan cut."""
        provider = ScriptedProvider(update("in_progress", "pending"), answer(),
            update("completed", "completed"), answer(), update(), answer())
        async with self.project(provider=provider) as (host, client):
            app = host.applications["agent"]
            first = await host.join(await host.execute("autoagent.agent", AgentInput(message="Work")))
            self.assertEqual(first.status, "completed", first.error)
            second = await host.join(await host.execute("autoagent.agent", AgentInput(message="Finish"), session_id=first.session_id))
            self.assertEqual(second.status, "completed", second.error)
            response = await client.get("/api/v2/agent/plan", params={"session_id": first.session_id})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertTrue(all(item["status"] == "completed" for item in response.json()["plan"]["plan"]))
            await app.fork_conversation(first.session_id, "plan-fork", first.invocation_id)
            inherited = await app.plan("plan-fork")
            self.assertEqual(inherited["plan"]["plan"][0]["status"], "in_progress")
            context = await app.forks.initial_context("plan-fork")
            forked = await host.join(await host.execute("autoagent.agent", AgentInput(message="Clear"),
                session_id="plan-fork", session_context=context))
            self.assertEqual(forked.status, "completed", forked.error)
            self.assertIsNone((await app.plan("plan-fork"))["plan"])
            self.assertIsNotNone((await app.plan(first.session_id))["plan"])


class SQLiteAppPlanTests(AppPlanContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLAppPlanTests(AppPlanContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self, root):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

"""Approval requests/replies use ordinary durable Wait and Runtime Events."""

from pathlib import Path
import tempfile
import unittest

from autoagent.agent import LLMToolCall, ToolApprovalRequest
from autoagent.host import AutoAgentHost, HostSettings
from examples.agent_usage_guide.permissions import build_workflow, plain
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase


class ApprovalStorageContracts:
    async def database_url(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        return "sqlite+aiosqlite:///" + str(Path(temporary.name) / "runtime.sqlite")

    async def test_wait_restore_rechecks_and_records_exact_approval_before_effects(self):
        """A reopened Host restores the approval Wait, then records approval and Tool output."""
        settings = HostSettings(database_url=await self.database_url())
        effects = []
        for approved in (True, False):
            with self.subTest(approved=approved):
                async with AutoAgentHost(workflows=[build_workflow(effects)], settings=settings) as host:
                    execution = await host.execute("tool-approval", LLMToolCall(
                        name="publish", call_id=f"publish-{approved}", arguments='{"text":"Reviewed report"}').model_dump(mode="json"))
                    waiting = await host.join(execution)
                    self.assertEqual(waiting.status, "waiting")
                    wait = waiting.waits[0]
                    request = ToolApprovalRequest.model_validate(plain(wait.request))
                    before = len(effects)
                # Rebuild application-owned tools, do not retain the old ToolSet.
                async with AutoAgentHost(workflows=[build_workflow(effects)], settings=settings) as host:
                    current = await host.get_execution(waiting.invocation_id)
                    self.assertEqual(current.status, "waiting")
                    self.assertEqual(len(effects), before)
                    await host.respond(waiting.invocation_id, wait.id,
                                       request.respond(approved=approved).model_dump(mode="json"))
                    result = await host.join(waiting.invocation_id)
                    self.assertEqual(result.status, "completed", result.error)
                    self.assertEqual(result.output["execute"]["is_error"], not approved)
                    self.assertEqual(len(effects), before + int(approved))
                    detail = await host._persistence.wait_detail(waiting.invocation_id, wait.id)
                    self.assertEqual(detail["status"], "resumed")
                    self.assertEqual(detail["request"]["value"]["id"], request.id)
                    self.assertEqual(detail["response"]["value"], request.respond(approved=approved).model_dump(mode="json"))


class SQLiteApprovalStorageTests(ApprovalStorageContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, "PostgreSQL URL not configured")
class PostgreSQLApprovalStorageTests(ApprovalStorageContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

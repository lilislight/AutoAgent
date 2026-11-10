"""Producer Node history commits, interrupted views and final mailbox inspection."""

import asyncio
import threading
import unittest

from autoagent.agent import (
    AgentAccess, AgentApprovalRequest, AgentApprovalResponse, AgentInput, AgentSignalMessage,
    Conversation, LLMMessage, LLMToolCall, LLMToolResult, Tool,
)
from autoagent.agent.context import restore
from autoagent.agent.workflow.compaction import validate_tool_history
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import AgentWorkflowContracts, ScriptedProvider, answer, calls, settings


class HistoryCommitContracts:
    host = AgentWorkflowContracts.host
    database_url = AgentWorkflowContracts.database_url

    def history(self, host, session_id):
        return restore(Conversation, host._runtime.repository.state(session_id).session.context['agent_conversation'])

    async def test_cancelled_partial_map_keeps_node_commit_boundary(self):
        """One successful Map unit is not saved as a Node result while another unit remains unfinished."""
        effects, entered, release = [], threading.Event(), threading.Event()
        def quick(text: str) -> str:
            effects.append(text)
            return text
        async def slow(text: str) -> str:
            entered.set()
            while not release.is_set():
                await asyncio.sleep(0.01)
            return text
        provider = ScriptedProvider(calls('quick', 'slow'))
        async with self.host(provider, tools=(Tool(quick), Tool(slow))) as host:
            ref = await host.execute('agent', AgentInput(message='parallel work'))
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 20))
                async with asyncio.timeout(20):
                    while not effects:
                        await asyncio.sleep(0.01)
                await host.cancel(ref, reason='stop partial Map')
                self.assertEqual((await host.join(ref)).status, 'cancelled')
                history = self.history(host, ref.session_id)
                self.assertEqual(history.pending.results, {})
                self.assertEqual(len([item for item in history.items if isinstance(item, LLMToolCall)]), 2)
            finally:
                release.set()

    async def test_final_response_checks_mailbox_without_consuming_then_runs_another_round(self):
        """A message accepted during final generation is consumed by Prepare Context and causes one extra round."""
        entered, release = threading.Event(), threading.Event()
        async def first(request):
            entered.set()
            if not await asyncio.to_thread(release.wait, 20):
                raise TimeoutError('generation gate')
            return answer('first answer')
        def second(request):
            text = request.model_dump_json()
            self.assertIn('late requirement', text)
            self.assertIn('first answer', text)
            self.assertEqual(sum(isinstance(item, LLMMessage) and any(
                'late requirement' in getattr(part, 'text', '') for part in item.content) for item in request.input), 1)
            return answer('updated answer')
        provider = ScriptedProvider(first, second)
        async with self.host(provider) as host:
            ref = await host.execute('agent', AgentInput(message='initial'))
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 20))
                await host.signal(ref, 'agent.user', AgentSignalMessage(message='late requirement').model_dump(mode='json'),
                    source_id='user', sequence=1)
            finally:
                release.set()
            result = await host.join(ref)
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual(result.output['content'], [{'type': 'text', 'text': 'updated answer'}])
            self.assertEqual(len(provider.requests), 2)
            history = self.history(host, ref.session_id)
            self.assertEqual(sum(isinstance(item, LLMMessage) and item.content[0].text == 'first answer'
                for item in history.items), 1)

    async def test_committed_tool_history_survives_cancel_restart_and_is_not_reexecuted(self):
        """Completed Node results stay in Session history through cancellation and restart without reading old execution evidence."""
        effects = []
        def write_note(text: str) -> str:
            effects.append(text)
            return 'write was committed'
        def guarded(text: str) -> str:
            self.fail('Approval was never granted')
        provider = ScriptedProvider(calls('write_note', 'guarded'), answer('continued'))
        url = await self.database_url()
        defaults = settings(access=AgentAccess(rules={'guarded': 'ask'}))
        tools = (Tool(write_note), Tool(guarded))
        async with self.host(provider, url=url, defaults=defaults, tools=tools) as host:
            ref = await host.execute('agent', AgentInput(message='write then ask'), session_id='history-session')
            waiting = await host.join(ref)
            self.assertEqual(waiting.status, 'waiting', waiting.error)
            history = self.history(host, ref.session_id)
            self.assertEqual(len([item for item in history.items if isinstance(item, LLMToolCall)]), 2)
            self.assertEqual(list(history.pending.results), ['call-0'])
            self.assertIn('write was committed', history.pending.results['call-0'].model_dump_json())
            await host.cancel(ref, reason='stop this round')
            self.assertEqual((await host.join(ref)).status, 'cancelled')
        async with self.host(provider, url=url, defaults=defaults, tools=tools) as host:
            result = await host.join(await host.execute('agent', AgentInput(message='continue'), session_id='history-session'))
            self.assertEqual(result.status, 'completed', result.error)
            request = provider.requests[-1]
            self.assertIn('write was committed', request.model_dump_json())
            self.assertIn('Interrupted Tool round', request.model_dump_json())
            self.assertIn('unknown outcome', request.model_dump_json())
            validate_tool_history(request.input)
            history = self.history(host, 'history-session')
            self.assertIsNone(history.pending)
            self.assertEqual(sum(isinstance(item, LLMToolResult) for item in history.items), 1)
            self.assertEqual(effects, ['hello'])

    async def test_approval_restore_commits_each_result_once_and_preserves_call_order(self):
        """Restored approval adds only its own Node results and Prepare orders them with earlier results once."""
        effects = []
        def write_note(text: str) -> str:
            effects.append(text)
            return text
        def guarded(text: str) -> str:
            self.fail('Rejected Tool must not run')
        provider = ScriptedProvider(calls('guarded', 'write_note'), answer())
        url = await self.database_url()
        defaults = settings(access=AgentAccess(rules={'guarded':'ask'}))
        tools = (Tool(write_note), Tool(guarded))
        async with self.host(provider, url=url, defaults=defaults, tools=tools) as host:
            ref = await host.execute('agent', AgentInput(message='help'))
            waiting = await host.join(ref)
            self.assertEqual(waiting.status, 'waiting', waiting.error)
            self.assertEqual(list(self.history(host, ref.session_id).pending.results), ['call-1'])
            wait_id = waiting.waits[0].id
            request = restore(AgentApprovalRequest, waiting.waits[0].request)
        async with self.host(provider, url=url, defaults=defaults, tools=tools) as host:
            await host.respond(ref, wait_id, AgentApprovalResponse(decisions=(
                request.requests[0].respond(approved=False),)).model_dump(mode='json'))
            result = await host.join(ref)
            self.assertEqual(result.status, 'completed', result.error)
            recorded = [item for item in self.history(host, ref.session_id).items if isinstance(item, LLMToolResult)]
            self.assertEqual([item.call_id for item in recorded], ['call-0', 'call-1'])
            self.assertTrue(recorded[0].is_error)
            self.assertEqual(effects, ['hello'])


class SQLiteHistoryCommitTests(HistoryCommitContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL URL not configured')
class PostgreSQLHistoryCommitTests(HistoryCommitContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

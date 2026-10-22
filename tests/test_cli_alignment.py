"""Business parity for LLM-facing catalog, inputs and execution evidence."""

import asyncio
from contextlib import AsyncExitStack, redirect_stderr, redirect_stdout
import io
import json
import os
import unittest
from unittest.mock import patch

import httpx
from sqlalchemy import update

from autoagent import Node, Workflow, WorkflowCompiler, WorkflowDefinitionSnapshot
from autoagent.cli import CliError
from autoagent.cli.history import run_query
from autoagent.cli.main import build_parser, main
from autoagent.cli.management import run as run_management
from autoagent.cli.remote import ServerClient
from autoagent.host import AutoAgentHost
from autoagent.host.persistence.reading import SourceReader
from autoagent.host.persistence.schema import meta
from autoagent.server import create_app
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_cli_management import ManagementFixture
from tests.test_exploration import identity


EXTRA = '''
@workflow
def unconditional():
    return Workflow('cli-direct', nodes=[Node('entry',echo),Node('finish',echo)], edges=[Edge('entry','finish',id='direct')])
@workflow
def multiple_entries():
    return Workflow('cli-entries', nodes=[Node('left',echo),Node('right',echo)])
'''


class AlignmentFixture(ManagementFixture):
    def prepare_project(self):
        super().prepare_project()
        path = self.root / (self.module + '.py')
        path.write_text(path.read_text() + EXTRA)

    async def seed(self, host):
        await super().seed(host)
        self.direct = await host.execute('cli-direct', {'text':'route'}, entry_node_id='entry')
        await host.join(self.direct)
        ir = WorkflowCompiler().compile_or_raise(Workflow('database-only', nodes=[Node('entry',identity)]))
        await host._persistence.register_workflow(WorkflowDefinitionSnapshot.from_workflow_ir(ir))
        ir = WorkflowCompiler().compile_or_raise(Workflow('cli-chain', version='saved-later', nodes=[Node('entry',identity)]))
        self.newest_saved = WorkflowDefinitionSnapshot.from_workflow_ir(ir)
        await host._persistence.register_workflow(self.newest_saved)


class LocalAlignmentTests(AlignmentFixture, unittest.TestCase):
    def setUp(self):
        self.prepare_project()
        self.env = patch.dict(os.environ, self.environment(), clear=True)
        self.env.start()
        async def prepare():
            async with AutoAgentHost.from_project(self.root) as host:
                await self.seed(host)
        with redirect_stdout(io.StringIO()):
            asyncio.run(prepare())

    def tearDown(self):
        self.env.stop()
        self.cleanup_project()

    def cli(self, *args):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = main(['--project', str(self.root), *args, '--format', 'json'])
        return code, json.loads(out.getvalue()), err.getvalue()

    def test_catalog_distinguishes_registered_and_database_only_workflows(self):
        """The local Host catalog exposes saved-only IDs and an exact current Revision."""
        code, report, _ = self.cli('workflow', 'list')
        self.assertEqual(code, 0, report)
        rows = {row['workflow_id']:row for row in report['data']['items']}
        self.assertEqual(report['view'], 'live')
        self.assertTrue(report['source']['registration_checked'])
        self.assertEqual(rows['cli-chain']['registration'], 'registered')
        self.assertEqual(rows['cli-chain']['revision_id'], self.current.workflow_revision_id)
        self.assertNotEqual(rows['cli-chain']['workflow_version'], 'saved-later')
        self.assertNotIn('current_revision_id', rows['cli-chain'])
        self.assertEqual(rows['database-only']['registration'], 'unregistered')
        code, report, _ = self.cli('workflow', 'list', '--registered-only', '--search', 'database-only')
        self.assertEqual(code, 0, report)
        self.assertEqual(report['data']['items'], [])

    def test_saved_catalog_and_registration_metadata_do_not_import_broken_code(self):
        """Read-only catalog inspection is available with broken project code and reports unknown registration."""
        (self.root / (self.module + '.py')).write_text('raise AssertionError("No project execution")')
        code, report, _ = self.cli('workflow', 'list', '--saved', '--search', 'dbonl')
        self.assertEqual(code, 0, report)
        self.assertEqual([row['workflow_id'] for row in report['data']['items']], ['database-only'])
        self.assertFalse(report['source']['registration_checked'])
        self.assertEqual(report['data']['items'][0]['registration'], 'unknown')
        code, report, _ = self.cli('session', 'workflows', 'history-conversation')
        self.assertEqual(code, 0, report)
        self.assertTrue(all(row['runnable'] is None and row['registration_checked'] is False for row in report['data']['items']))

    def test_entry_inputs_separate_raw_workflow_json_from_operator_contract(self):
        """Mapping input never advertises an Operator schema as the caller's input contract."""
        for workflow, mode in (('cli-echo','contract'), ('cli-json','raw_json')):
            code, report, _ = self.cli('workflow', 'inputs', workflow, '--entry-node', 'entry')
            self.assertEqual(code, 0, report)
            self.assertEqual(report['data']['selected_entry_node_id'], 'entry')
            entry = next(row for row in report['data']['entries'] if row['node_id'] == 'entry')
            self.assertEqual(entry['input_mode'], mode)
            if mode == 'contract':
                self.assertIn('text', entry['input_schema']['required'])
            else:
                self.assertIsNone(entry['input_schema'])

    def test_multiple_entries_and_busy_sessions_remain_explicit(self):
        """Input discovery does not silently choose an Entry or claim a busy Session is runnable."""
        code, report, _ = self.cli('workflow', 'inputs', 'cli-entries')
        self.assertEqual(code, 0, report)
        self.assertTrue(report['data']['entry_node_required'])
        self.assertIsNone(report['data']['selected_entry_node_id'])
        code, report, _ = self.cli('workflow', 'inputs', 'cli-echo', '--session-id', self.waiting.session_id)
        self.assertEqual(code, 0, report)
        self.assertFalse(report['data']['can_run'])
        self.assertEqual(report['data']['code'], 'session_busy')

    def test_graph_reports_unconditional_route_without_loading_business_values(self):
        """The CLI returns selected routes even when there is no condition-evaluation record."""
        with patch.object(SourceReader, 'read', side_effect=AssertionError('No business values')):
            code, report, _ = self.cli('invocation', 'show', self.direct.invocation_id, '--graph')
        self.assertEqual(code, 0, report)
        self.assertEqual(report['data']['edges'], [dict(edge_id='direct', selected=True, condition_evaluation_count=0)])
        self.assertNotIn('input', report['data']['invocation'])
        self.assertEqual(report['data']['view'], 'durable')
        code, report, _ = self.cli('invocation', 'edges', self.direct.invocation_id)
        self.assertEqual(report['data']['items'], [])

    def test_manual_checkpoints_are_pinned_by_default_with_explicit_opt_out(self):
        """UI and CLI manual capture defaults protect snapshots without changing existing Pins."""
        code, report, _ = self.cli('checkpoint', 'create', self.current.session_id)
        self.assertEqual(code, 0, report)
        self.assertTrue(report['data']['pinned'])
        code, report, _ = self.cli('checkpoint', 'create', self.direct.session_id, '--no-pin')
        self.assertEqual(code, 0, report)
        self.assertFalse(report['data']['pinned'])

    def test_common_local_errors_match_remote_classification(self):
        """Lookup and Entry errors have stable CLI classifications in either mode."""
        for args, expected in (
            (('invocation','show','missing'), 'NOT_FOUND'),
            (('workflow','inputs','cli-echo','--entry-node','missing'), 'INVALID_REQUEST'),
        ):
            code, report, _ = self.cli(*args)
            self.assertEqual(code, 2, report)
            self.assertEqual(report['error']['code'], expected)


class RemoteAlignmentTests(AlignmentFixture, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.prepare_project()
        self.env = patch.dict(os.environ, self.environment(), clear=True)
        self.env.start()
        self.stack = AsyncExitStack()
        self.host = await self.stack.enter_async_context(AutoAgentHost.from_project(self.root))
        await self.seed(self.host)
        self.app = create_app(self.host, ui=False)
        self.parser = build_parser()

    async def asyncTearDown(self):
        await self.stack.aclose()
        self.env.stop()
        self.cleanup_project()

    async def query(self, *args):
        options = self.parser.parse_args(['--server','http://test/api/v2',*args])
        options.request_timeout = 30
        client = lambda url, timeout: ServerClient(url, timeout=timeout, transport=httpx.ASGITransport(app=self.app))
        with patch('autoagent.cli.remote.ServerClient', side_effect=client):
            return await (run_management(options) if getattr(options,'management_query',False) else run_query(options))

    async def test_remote_catalog_search_paging_and_revision_prefix(self):
        """Host catalog pagination discovers saved Workflows and accepts fuzzy IDs and hash prefixes."""
        ids, cursor = [], None
        while True:
            args = ['workflow','list','--limit','2'] + (['--cursor',cursor] if cursor else [])
            with patch.object(SourceReader,'read',side_effect=AssertionError('No definitions')):
                _, report, code = await self.query(*args)
            self.assertEqual(code, 0, report)
            ids.extend(row['workflow_id'] for row in report['data']['items'])
            cursor = report['data']['next_cursor']
            if not cursor: break
        self.assertEqual(len(ids), len(set(ids)))
        self.assertIn('database-only', ids)
        _, report, _ = await self.query('workflow','list','--search','cli-chain')
        row = report['data']['items'][0]
        self.assertEqual(row['revision_id'], self.current.workflow_revision_id)
        self.assertNotEqual(row['workflow_version'], 'saved-later')
        _, report, _ = await self.query('workflow','list','--search','ccn')
        self.assertIn('cli-chain', [row['workflow_id'] for row in report['data']['items']])
        prefix = self.current.workflow_revision_id.split(':')[-1][:12]
        _, report, _ = await self.query('workflow','list','--search',prefix)
        self.assertEqual([row['workflow_id'] for row in report['data']['items']], ['cli-chain'])

    async def test_inputs_match_ui_run_spec_and_do_not_execute_an_invocation(self):
        """CLI input discovery reuses the UI contract and Session admission checks without submitting work."""
        with patch.object(self.host,'execute_json',side_effect=AssertionError('No submission')):
            _, report, code = await self.query('workflow','inputs','cli-echo','--entry-node','entry',
                '--session-id','history-conversation')
        self.assertEqual(code, 0, report)
        spec = await self.host.get_workflow_run_spec('cli-echo', revision_id=report['data']['revision_id'],
            entry_node_id='entry', session_id='history-conversation')
        for key,value in spec.items(): self.assertEqual(report['data'][key], value)
        self.assertTrue(report['data']['can_run'])

    async def test_session_order_uses_events_under_clock_skew_and_keeps_original_ordinals(self):
        """Clock changes and filtering cannot reorder or renumber the Session's Invocation history."""
        async def skew():
            async with self.host._persistence._writer.storage.engine.begin() as conn:
                for index,ref in enumerate(self.refs):
                    await conn.execute(update(meta.tables['invocations']).where(
                        meta.tables['invocations'].c.invocation_id == ref.invocation_id).values(started_at_us=3000000-index*1000000))
        await self.host._persistence._writer.run(skew)
        values, cursor = [], None
        while True:
            args=['session','invocations','history-conversation','--limit','1'] + (['--cursor',cursor] if cursor else [])
            _, report, _ = await self.query(*args)
            values.extend(report['data']['items'])
            cursor=report['data']['next_cursor']
            if not cursor: break
        self.assertEqual([row['invocation_id'] for row in values], [ref.invocation_id for ref in self.refs])
        self.assertEqual([row['ordinal'] for row in values], [1,2,3])
        self.assertTrue(all('values' not in row for row in values))
        _, report, _ = await self.query('session','invocations','history-conversation','--workflow-id','cli-chain')
        self.assertEqual([row['ordinal'] for row in report['data']['items']], [1,3])

    async def test_graph_is_one_consistent_ui_snapshot_with_clear_edge_semantics(self):
        """Graph mode uses one snapshot query and names route selection independently of conditions."""
        with patch.object(self.host,'get_invocation_summary',side_effect=AssertionError('No second snapshot')):
            _, report, code = await self.query('invocation','show',self.direct.invocation_id,'--graph')
        self.assertEqual(code, 0, report)
        self.assertEqual(report['data']['edges'][0]['selected'], True)
        self.assertEqual(report['data']['edges'][0]['condition_evaluation_count'], 0)

    async def test_common_remote_errors_use_cli_classification(self):
        """Object lookup errors retain the same CLI code independently of HTTP transport."""
        with self.assertRaises(CliError) as caught:
            await self.query('invocation','show','missing')
        self.assertEqual(caught.exception.code, 'NOT_FOUND')
        with self.assertRaises(CliError) as caught:
            await self.query('workflow','inputs','cli-echo','--entry-node','missing')
        self.assertEqual(caught.exception.code, 'INVALID_REQUEST')


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL test URL is required.')
class PostgreSQLRemoteAlignmentTests(RemoteAlignmentTests):
    def prepare_project(self):
        super().prepare_project()
        path = self.root / '.env'
        path.write_text(path.read_text().replace('sqlite+aiosqlite:///' + str(self.database), self.url))

    async def asyncSetUp(self):
        self.postgres = PostgreSQLDatabase()
        self.url = await self.postgres.create()
        await super().asyncSetUp()

    async def asyncTearDown(self):
        await super().asyncTearDown()
        await self.postgres.close()

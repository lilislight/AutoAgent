"""CLI reports, live receipts, Revision comparison and Checkpoint management."""

import asyncio
from contextlib import AsyncExitStack, redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch

import httpx

from autoagent.cli import CliError
from autoagent.cli.history import run_query
from autoagent.cli.main import build_parser, main
from autoagent.cli.management import run as run_management
from autoagent.cli.remote import ServerClient
from autoagent.host import AutoAgentHost
from autoagent.host.persistence.reading import SourceReader
from autoagent.server import create_app
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_cli_history import HistoryFixture


EXTRA = '''
import asyncio
from autoagent import Map, RetryPolicy
def map_inputs(ctx: InputMappingContext) -> list[Value]:
    return [{'text':'once'}, {'text':'always'}]
async def flaky_unit(value: Value) -> Value:
    marker = Path(os.environ['CLI_MARKER'] + '.map')
    if value['text'] == 'always':
        # Ensure the other Unit's successful retry has completed before this
        # Unit exhausts the Node. Otherwise fail-fast scheduling can correctly
        # leave both Units failed, which is a different diagnosis scenario.
        async with asyncio.timeout(5):
            while not marker.with_suffix('.done').exists():
                await asyncio.sleep(.005)
        raise ValueError('unit always fails')
    if not marker.exists():
        marker.touch()
        raise ValueError('unit first attempt fails')
    marker.with_suffix('.done').touch()
    return value
@workflow
def partial_map_failure():
    return Workflow('cli-map-retries', nodes=[Node('entry',flaky_unit,input_mapping=map_inputs,
        map=Map(),retry=RetryPolicy(max_attempts=1))])
def broken_mapping(ctx: InputMappingContext) -> Value:
    raise ValueError('mapping failed')
def broken_call(value: Value) -> Value:
    raise RuntimeError('child failed')
@workflow
def mapped_failure():
    return Workflow('cli-mapping-failure', nodes=[Node('entry',echo,input_mapping=broken_mapping)])
@workflow
def child_failure():
    child = Workflow('cli-broken-child', nodes=[Node('entry',broken_call)])
    return Workflow('cli-child-failure', nodes=[Node('spawn',Spawn(child,'entry'))])
@workflow
def diff_current():
    version = os.environ.get('CLI_DIFF_VERSION','1')
    return Workflow('cli-diff', version=version, nodes=[Node('entry',echo),
        Node('old' if version == '1' else 'new',echo)],
        edges=[Edge('entry','old' if version == '1' else 'new',id='route')])
'''


class ManagementFixture(HistoryFixture):
    async def seed(self, host):
        await super().seed(host, large_value=False)

    def prepare_project(self):
        super().prepare_project()
        path = self.root / (self.module + '.py')
        path.write_text(path.read_text() + EXTRA)
        with (self.root / '.env').open('a') as stream:
            stream.write('AUTOAGENT_HOST_MEMORY_BUDGET_BYTES=2147483648\n')


class CliManagementTests(ManagementFixture, unittest.TestCase):
    def test_projection_status_build_rebuild_share_history_reads(self):
        """CLI builds durable projections and historical reads use them without reconstruction."""
        for command in ('status', 'build', 'rebuild'):
            status, stdout, stderr = self.cli('projection', command, '--format', 'json')
            self.assertEqual(status, 0, (stdout, stderr))
            data = json.loads(stdout)['data']
            self.assertIn('processed_order', data)
            if command != 'status':
                self.assertEqual(data['lag_events'], 0)
        from autoagent.host.persistence.read_models import ProjectionCache
        with patch.object(ProjectionCache, 'build', side_effect=AssertionError('CLI ignored database projection')):
            status, stdout, stderr = self.cli('invocation', 'show', self.current.invocation_id, '--format', 'json')
        self.assertEqual(status, 0, (stdout, stderr))

    def test_projection_commands_do_not_import_project_code(self):
        """Offline status/build/rebuild need database facts, without Workflow imports or lifespan."""
        (self.root / (self.module + '.py')).write_text('raise AssertionError("Projection must not import project code")')
        for command in ('status', 'build', 'rebuild'):
            status, stdout, stderr = self.cli('projection', command, '--format', 'json')
            self.assertEqual(status, 0, (stdout, stderr))

    def setUp(self):
        self.prepare_project()
        self.environ_patch = patch.dict(os.environ, self.environment(), clear=True)
        self.environ_patch.start()
        async def prepare():
            async with AutoAgentHost.from_project(self.root) as host:
                await self.seed(host)
                self.base = (await host.get_workflow('cli-diff'))['workflow_revision_id']
        with redirect_stdout(io.StringIO()):
            asyncio.run(prepare())

    def tearDown(self):
        self.environ_patch.stop()
        self.cleanup_project()

    def cli(self, *arguments):
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = main(['--project', str(self.root), *arguments])
        return status, stdout.getvalue(), stderr.getvalue()

    def test_output_infers_json_and_saves_errors_as_well_as_success(self):
        """One report path always receives the structured command outcome, with clean stdout."""
        for arguments, expected in ((('session', 'show', 'history-conversation'), 0),
                                    (('invocation', 'show', 'missing'), 2)):
            status, stdout, stderr = self.cli(*arguments, '--output', 'reports/result.json')
            self.assertEqual(status, expected, stderr)
            self.assertEqual(stdout, '')
            report = json.loads((self.root / 'reports/result.json').read_text())
            self.assertEqual(report['ok'], expected == 0)
        status, _, _ = self.cli('session', 'show', 'history-conversation', '--output', 'reports/result.json', '--format', 'text')
        self.assertEqual(status, 0)
        self.assertTrue((self.root / 'reports/result.json').read_text().startswith('PROJECT '))

    def test_all_exports_every_page_to_one_report_and_preserves_filters(self):
        """Explicit exports follow all seek pages, retain metadata and omit business values."""
        status, stdout, stderr = self.cli('session', 'invocations', 'history-conversation', '--workflow-id', 'cli-chain',
            '--limit', '1', '--all', '--output', 'reports/history.json')
        self.assertEqual(status, 0, (stdout, stderr))
        report = json.loads((self.root / 'reports/history.json').read_text())
        self.assertEqual(report['data']['returned_count'], 2)
        self.assertEqual(report['data']['pages'], 2)
        self.assertIsNone(report['data']['next_cursor'])
        self.assertEqual({row['invocation_id'] for row in report['data']['items']}, {self.refs[0].invocation_id, self.refs[2].invocation_id})
        self.assertTrue(all('input' not in row and 'value_keys' not in row for row in report['data']['items']))
        status, stdout, _ = self.cli('session', 'list', '--all', '--format', 'json')
        self.assertEqual(status, 2)
        self.assertIn('--output', json.loads(stdout)['error']['message'])

    def test_failed_export_keeps_old_report_and_removes_partial_file(self):
        """A later page failure never publishes a truncated document or overwrites an existing export."""
        from autoagent.cli.history import _local_query
        original = _local_query
        async def fail_second(host, options, cursor):
            if cursor is not None:
                raise CliError('READ_FAILED', 'Second page failed.', exit_code=3)
            return await original(host, options, cursor)
        destination = self.root / 'history.json'
        destination.write_text('previous report')
        with patch('autoagent.cli.history._local_query', fail_second):
            status, stdout, _ = self.cli('session', 'invocations', 'history-conversation', '--limit', '1', '--all', '--output', 'history.json')
        self.assertEqual(status, 3)
        self.assertEqual(json.loads(stdout)['error']['code'], 'READ_FAILED')
        self.assertEqual(destination.read_text(), 'previous report')
        self.assertEqual([p.name for p in self.root.iterdir() if p.name.startswith('tmp')], [])

    def test_saved_revision_diff_pages_and_selected_fields_are_directional(self):
        """Comparison reuses saved definitions, scopes cursors and reads details only for an explicit object."""
        async def save():
            async with AutoAgentHost.from_project(self.root, environment={**self.environment(), 'CLI_DIFF_VERSION': '2'}) as host:
                return (await host.get_workflow('cli-diff'))['workflow_revision_id']
        with redirect_stdout(io.StringIO()):
            target = asyncio.run(save())
        for base, other, status_name in ((self.base, target, 'removed'), (target, self.base, 'added')):
            status, stdout, _ = self.cli('workflow', 'diff', base, other, '--format', 'json')
            self.assertEqual(status, 0, stdout)
            changes = json.loads(stdout)['data']
            self.assertEqual(next(row['status'] for row in changes['items'] if row['id'] == 'old'), status_name)
        status, stdout, _ = self.cli('workflow', 'diff', self.base, target, '--node-id', 'old', '--format', 'json')
        self.assertEqual(status, 0, stdout)
        self.assertFalse(json.loads(stdout)['data']['target']['present'])
        status, stdout, _ = self.cli('workflow', 'diff', self.base, target, '--workflow', '--format', 'json')
        self.assertEqual(status, 0, stdout)
        self.assertEqual(json.loads(stdout)['data']['target']['value']['workflow_version'], '2')

    def test_diagnosis_is_metadata_only_and_leads_to_exact_failure_queries(self):
        """Failed execution reports include exact occurrence/call IDs without hydrating their errors or inputs."""
        with patch.object(SourceReader, 'read', side_effect=AssertionError('No payloads')):
            status, stdout, _ = self.cli('invocation', 'diagnose', self.failed.invocation_id, '--format', 'json')
        self.assertEqual(status, 0, stdout)
        report = json.loads(stdout)['data']
        self.assertEqual(report['assessment'], 'failed')
        self.assertEqual(report['failed_nodes']['total_count'], 1)
        self.assertEqual(report['failed_calls']['total_count'], 1)
        self.assertFalse(report['runtime_observed'])
        self.assertTrue(report['next_queries'])
        self.assertNotIn('first attempt failed', stdout)
        status, stdout, _ = self.cli('invocation', 'diagnose', self.current.invocation_id, '--format', 'json')
        self.assertEqual(status, 0, stdout)
        self.assertEqual(json.loads(stdout)['data']['failed_nodes']['total_count'], 0)

    def test_checkpoint_mutations_reuse_versions_and_do_not_resume_waits(self):
        """Local create/pin/unpin/delete mutate snapshots while preserving Events and the waiting execution."""
        status, stdout, _ = self.cli('invocation', 'show', self.waiting.invocation_id, '--format', 'json')
        self.assertEqual(status, 0, stdout)
        before = json.loads(stdout)['data']
        status, stdout, _ = self.cli('checkpoint', 'create', self.waiting.session_id, '--pin', '--format', 'json')
        self.assertEqual(status, 0, stdout)
        identity = json.loads(stdout)['data']['checkpoint_id']
        status, stdout, _ = self.cli('checkpoint', 'create', self.waiting.session_id, '--pin', '--format', 'json')
        self.assertEqual(status, 0, stdout)
        self.assertEqual(json.loads(stdout)['data']['checkpoint_id'], identity)
        for command, pinned in (('unpin', False), ('pin', True)):
            status, stdout, _ = self.cli('checkpoint', command, identity, '--format', 'json')
            self.assertEqual(status, 0, stdout)
            self.assertEqual(json.loads(stdout)['data']['pinned'], pinned)
        status, stdout, _ = self.cli('invocation', 'show', self.waiting.invocation_id, '--format', 'json')
        self.assertEqual(status, 0, stdout)
        after = json.loads(stdout)['data']
        self.assertEqual(after['status'], before['status'])
        self.assertEqual(after['last_sequence'], before['last_sequence'])
        status, stdout, _ = self.cli('checkpoint', 'delete', identity, '--format', 'json')
        self.assertEqual(status, 0, stdout)
        self.assertTrue(json.loads(stdout)['data']['deleted'])

    def test_local_actions_need_no_server(self):
        """Local control discovery uses Host registration and persisted execution metadata."""
        status, stdout, _ = self.cli('invocation', 'actions', self.waiting.invocation_id, '--format', 'json')
        self.assertEqual(status, 0, stdout)
        self.assertTrue(json.loads(stdout)['data']['actions']['respond']['available'])
        self.assertTrue(json.loads(stdout)['data']['actions']['recover']['available'])

    def test_cli_checkpoint_sequence_selects_historical_state(self):
        """CLI passes a Session-scoped position through to historical capture without changing execution."""
        status, stdout, _ = self.cli('invocation', 'events', self.current.invocation_id, '--format', 'json')
        self.assertEqual(status, 0, stdout)
        event = next(row for row in json.loads(stdout)['data']['items'] if row['event_name'] == 'node_occurrence.started')
        status, stdout, _ = self.cli('checkpoint', 'create', self.current.session_id,
            '--sequence', str(event['sequence']), '--pin', '--format', 'json')
        self.assertEqual(status, 0, stdout)
        member = next(row for row in json.loads(stdout)['data']['members'] if row['session_id'] == self.current.session_id)
        self.assertEqual(member['sequence'], event['sequence'])

    def test_mutation_report_write_failure_retains_applied_checkpoint_identity(self):
        """A failed report write exposes the already-applied mutation so an agent need not repeat it."""
        status, stdout, _ = self.cli('session', 'checkpoints', self.current.session_id, '--format', 'json')
        self.assertEqual(status, 0, stdout)
        identity = json.loads(stdout)['data']['items'][0]['checkpoint_id']
        with patch('autoagent.cli.main.write_report', side_effect=OSError('disk full')):
            status, stdout, _ = self.cli('checkpoint', 'pin', identity, '--output', 'reports/pin.json')
        self.assertEqual(status, 3)
        error = json.loads(stdout)['error']
        self.assertEqual(error['code'], 'OUTPUT_WRITE_FAILED')
        self.assertEqual(error['details']['checkpoint_id'], identity)
        self.assertTrue(error['details']['mutation_applied'])

    def test_export_is_page_bounded_and_does_not_render_a_combined_list(self):
        """JSON exports serialize individual records rather than materializing or rendering the whole collection."""
        with patch('autoagent.cli.output.render', side_effect=AssertionError('No full report rendering')):
            status, stdout, stderr = self.cli('invocation', 'events', self.current.invocation_id,
                '--limit', '2', '--all', '--output', 'reports/events.json')
        self.assertEqual(status, 0, (stdout, stderr))
        report = json.loads((self.root / 'reports/events.json').read_text())
        self.assertGreater(report['data']['pages'], 1)
        sequences = [row['sequence'] for row in report['data']['items']]
        self.assertEqual(sequences, sorted(set(sequences)))

    def test_receipt_query_explains_host_memory_without_importing_project(self):
        """Local receipt queries explain the process boundary without importing project code."""
        (self.root / (self.module + '.py')).write_text('raise AssertionError("No project execution")')
        status, stdout, _ = self.cli('operation', 'show', 'receipt', '--format', 'json')
        self.assertEqual(status, 2)
        self.assertEqual(json.loads(stdout)['error']['code'], 'USAGE_ERROR')
        self.assertIn('Host memory', json.loads(stdout)['error']['message'])


class RemoteManagementTests(ManagementFixture, unittest.IsolatedAsyncioTestCase):
    async def test_remote_projection_commands_use_server_database(self):
        """Remote maintenance uses the Server projection route without loading a local project."""
        with patch.object(AutoAgentHost, 'from_project', side_effect=AssertionError('No local Host')):
            for command in ('status', 'build', 'rebuild'):
                _, report, code = await self.query('projection', command)
                self.assertEqual(code, 0, report)
                if command != 'status':
                    self.assertEqual(report['data']['lag_events'], 0)

    async def asyncSetUp(self):
        self.prepare_project()
        self.environ_patch = patch.dict(os.environ, self.environment(), clear=True)
        self.environ_patch.start()
        self.stack = AsyncExitStack()
        self.host = await self.stack.enter_async_context(AutoAgentHost.from_project(self.root))
        await self.seed(self.host)
        self.app = create_app(self.host, ui=False)
        self.parser = build_parser()

    async def asyncTearDown(self):
        await self.stack.aclose()
        self.environ_patch.stop()
        self.cleanup_project()

    async def query(self, *arguments):
        options = self.parser.parse_args(['--server', 'http://test/api/v2', *arguments])
        options.request_timeout = 30
        client = lambda url, timeout: ServerClient(url, timeout=timeout, transport=httpx.ASGITransport(app=self.app))
        with patch('autoagent.cli.remote.ServerClient', side_effect=client):
            return await (run_management(options) if getattr(options, 'management_query', False) else run_query(options))

    async def test_actions_include_child_wait_target_and_selected_response_schema(self):
        """Current action queries use Host authority and retain a Child Wait's exact response contract."""
        children = await self.host.list_child_executions(self.waiting.invocation_id)
        child = children.items[0]['invocation_id']
        wait = (await self.host.list_execution_waits(child)).items[0]
        with patch.object(AutoAgentHost, 'from_project', side_effect=AssertionError('No local Host')):
            _, report, code = await self.query('invocation', 'actions', self.waiting.invocation_id,
                '--target-invocation-id', child, '--wait-id', wait['wait_id'])
        self.assertEqual(code, 0, report)
        self.assertTrue(report['data']['actions']['respond']['available'])
        self.assertIn('text', report['data']['response_schema']['properties'])

    async def test_receipt_query_never_resubmits_and_expiry_is_not_success(self):
        """An independent receipt lookup observes success/failure and an expired lookup never posts another operation."""
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url='http://test') as client:
            receipt = (await client.post('/api/v2/execution-actions', json=dict(invocation_id=self.waiting.invocation_id,
                action='pause', request_id='cli-independent-receipt'))).json()
            for _ in range(200):
                _, report, code = await self.query('operation', 'show', receipt['operation_id'])
                if report['data']['status'] != 'running':
                    break
                await asyncio.sleep(0.01)
            self.assertEqual(code, 0, report)
            self.assertEqual(report['data']['status'], 'succeeded')
        with self.assertRaises(CliError) as missing:
            await self.query('operation', 'show', 'expired')
        self.assertEqual(missing.exception.code, 'operation_unavailable')

    async def test_remote_checkpoint_crud_handles_empty_delete_response(self):
        """Checkpoint mutation transport accepts HTTP 204 and leaves protected operations to Host policy."""
        _, report, code = await self.query('checkpoint', 'create', self.current.session_id, '--pin')
        self.assertEqual(code, 0, report)
        identity = report['data']['checkpoint_id']
        _, report, _ = await self.query('checkpoint', 'unpin', identity)
        self.assertFalse(report['data']['pinned'])
        _, report, _ = await self.query('checkpoint', 'delete', identity)
        self.assertTrue(report['data']['deleted'])

    async def test_recovered_failures_are_not_reported_as_current_faults(self):
        """A successful recovery retains historical failure counts without diagnosing a current failed Call."""
        await self.host.recover(self.failed.invocation_id)
        await self.host.join(self.failed.invocation_id)
        _, report, code = await self.query('invocation', 'diagnose', self.failed.invocation_id)
        self.assertEqual(code, 0, report)
        self.assertEqual(report['data']['assessment'], 'completed')
        self.assertGreater(report['data']['counts']['calls'].get('failed', 0), 0)
        self.assertEqual(report['data']['failed_nodes']['total_count'], 0)
        self.assertEqual(report['data']['failed_calls']['total_count'], 0)

    async def test_map_diagnosis_reports_only_latest_failed_call_per_unit(self):
        """Successful retried Map units are excluded while a different unit still fails the Node."""
        ref = await self.host.execute('cli-map-retries', {'text': 'map'}, entry_node_id='entry')
        await self.host.join(ref)
        from sqlalchemy import select, update
        from autoagent.host.persistence.schema import meta
        async def skew_old_row():
            calls, store = meta.tables['operator_calls'], meta.tables['persistence_meta']
            async with self.host._persistence._writer.storage.engine.begin() as raw:
                from tests.projection_fixtures import EventFixtureConnection
                order = await raw.scalar(select(store.c.last_store_order))
                identity = await raw.scalar(select(store.c.store_id))
                conn = EventFixtureConnection(raw,self.host._persistence._queries.projection_cache,order,identity,scope=ref.invocation_id)
                # Row refresh order and wall-clock adjustments must not turn
                # an old failed attempt into the latest Call for this Unit.
                await conn.execute(update(calls).where(calls.c.invocation_id == ref.invocation_id,
                    calls.c.unit_index == 0, calls.c.status == 'failed').values(
                        updated_store_order=order, started_at_us=9007199254740991))
        await self.host._persistence._writer.run(skew_old_row)
        _, report, code = await self.query('invocation', 'diagnose', ref.invocation_id)
        self.assertEqual(code, 0, report)
        self.assertEqual(report['data']['assessment'], 'failed')
        self.assertGreater(report['data']['counts']['calls'].get('failed', 0), 1)
        self.assertEqual(report['data']['failed_calls']['total_count'], 1)
        self.assertEqual(report['data']['failed_calls']['items'][0]['unit_index'], 1)

    async def test_live_actions_and_diagnostic_waits_are_not_confused(self):
        """Durable diagnosis locates a pending Child Wait, while the live Host decides allowed actions."""
        with patch.object(SourceReader, 'read', side_effect=AssertionError('Metadata only')):
            _, report, code = await self.query('invocation', 'diagnose', self.waiting.invocation_id)
        self.assertEqual(code, 0, report)
        self.assertGreater(report['data']['active_nodes']['total_count'], 0)
        self.assertGreater(report['data']['pending_waits']['total_count'], 0)
        self.assertFalse(report['data']['runtime_observed'])
        _, live, _ = await self.query('invocation', 'actions', self.waiting.invocation_id)
        self.assertEqual(live['view'], 'live')
        self.assertTrue(live['data']['actions']['respond']['available'])

    async def test_remote_revision_comparison_has_same_saved_object_semantics(self):
        """HTTP diff supports directional saved revisions and selected fields without a local project."""
        from autoagent import Node, Workflow
        from tests.test_exploration import identity
        # Register an additional saved Revision through the owned Persistence,
        # without changing the Host's currently registered catalog.
        from autoagent.core.compiler import WorkflowCompiler, WorkflowDefinitionSnapshot
        ir = WorkflowCompiler().compile_or_raise(Workflow('cli-diff', version='2', nodes=[Node('entry', identity)]))
        snapshot = WorkflowDefinitionSnapshot.from_workflow_ir(ir)
        await self.host._persistence.register_workflow(snapshot)
        base = (await self.host.get_workflow('cli-diff'))['workflow_revision_id']
        target = snapshot.workflow_revision_id
        _, report, code = await self.query('workflow', 'diff', base, target, '--limit', '1')
        self.assertEqual(code, 0, report)
        self.assertTrue(report['data']['next_cursor'])
        _, next_page, _ = await self.query('workflow', 'diff', base, target, '--limit', '1', '--cursor', report['data']['next_cursor'])
        self.assertNotEqual(report['data']['items'][0]['id'], next_page['data']['items'][0]['id'])
        with self.assertRaises(CliError):
            await self.query('workflow', 'diff', target, base, '--cursor', report['data']['next_cursor'])
        _, report, _ = await self.query('workflow', 'diff', base, target, '--node-id', 'old')
        self.assertFalse(report['data']['target']['present'])

    async def test_diagnosis_follows_failed_children_and_distinguishes_mapping_faults(self):
        """Root diagnosis finds child errors and mapping failures without claiming that persisted running state is live."""
        for workflow in ('cli-mapping-failure', 'cli-child-failure'):
            ref = await self.host.execute(workflow, {'text': 'broken'}, entry_node_id='entry' if workflow == 'cli-mapping-failure' else 'spawn')
            await self.host.join(ref)
            with patch.object(SourceReader, 'read', side_effect=AssertionError('No payloads')):
                _, report, code = await self.query('invocation', 'diagnose', ref.invocation_id, '--limit', '1')
            self.assertEqual(code, 0, report)
            self.assertLessEqual(len(report['data']['failed_nodes']['items']), 1)
            if workflow == 'cli-mapping-failure':
                self.assertEqual(report['data']['assessment'], 'failed')
                self.assertGreater(report['data']['failed_nodes']['total_count'], 0)
                self.assertEqual(report['data']['failed_calls']['total_count'], 0)
            else:
                self.assertEqual(report['data']['assessment'], 'completed')
                self.assertEqual(report['data']['failed_nodes']['total_count'], 0)
                self.assertGreater(report['data']['descendants']['total_count'], 0)
                child = report['data']['descendants']['items'][0]['invocation_id']
                _, report, _ = await self.query('invocation', 'diagnose', child)
                self.assertEqual(report['data']['assessment'], 'failed')
                self.assertGreater(report['data']['failed_nodes']['total_count'], 0)


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL test URL is required.')
class PostgreSQLRemoteManagementTests(RemoteManagementTests):
    def prepare_project(self):
        super().prepare_project()
        path = self.root / '.env'
        path.write_text(path.read_text().replace('sqlite+aiosqlite:///' + str(self.database), self.postgres_url))

    async def asyncSetUp(self):
        self.postgres = PostgreSQLDatabase()
        self.postgres_url = await self.postgres.create()
        await super().asyncSetUp()

    async def asyncTearDown(self):
        await super().asyncTearDown()
        await self.postgres.close()

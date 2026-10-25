"""The performance harness invokes native HTTP APIs in isolated version processes."""

import json
import importlib.util
from pathlib import Path
import shutil
import tempfile
import unittest

from tests.benchmarks.server_comparison.run import run_case
from tests.postgres import POSTGRES_TEST_URL


class MemoryServerComparisonTests(unittest.TestCase):
    def test_standalone_servers_execute_without_sql_or_database_files(self):
        """Both Core versions agree with no sink, SQL statements or database files."""
        with tempfile.TemporaryDirectory() as root:
            for case in (dict(name='chain', family='chain', nodes=2),
                         dict(name='loop', family='loop', iterations=3, context='append'),
                         dict(name='map', family='map', width=4, parallelism=2)):
                outputs=[]
                for profile in ('v1-full-memory','v1-standard-memory','v2-memory'):
                    folder=Path(root)/case['name']/profile
                    result=run_case(profile,dict(case,calls=2,payload_bytes=1024,
                                                instrument_sql=True),folder)
                    self.assertEqual(result['status'],'completed',result)
                    self.assertEqual(result['backend'],'none')
                    self.assertEqual(result['catalog']['database_mode'],'none')
                    self.assertEqual(result['sql_after']['statements'],0)
                    self.assertEqual(result['closed_storage']['total_bytes'],0)
                    self.assertEqual(list(folder.glob('runtime.db*')),[])
                    records=[json.loads(line)for line in (folder/'calls.jsonl').read_text().splitlines()]
                    outputs.append([r['output']for r in records])
                self.assertEqual(outputs[0],outputs[1])
                self.assertEqual(outputs[0],outputs[2])


class ServerComparisonTests(unittest.TestCase):
    backend = 'sqlite'

    def test_both_servers_produce_identical_results_and_drain_persistence(self):
        """V1 and V2 use separate packages and agree after durable HTTP completion."""
        with tempfile.TemporaryDirectory() as root:
            results=[]
            for profile in ('v1-full', 'v2-artifacts-8k', 'v2-buffered-artifacts-8k'):
                result=run_case(profile,dict(name='chain',family='chain',nodes=2,payload_bytes=16384,calls=2),
                    Path(root)/profile,backend=self.backend)
                self.assertEqual(result['status'],'completed',result)
                self.assertEqual(result['completed'],2)
                self.assertEqual(result['history_query']['status'],200)
                self.assertEqual(result['catalog']['artifact_threshold_bytes'],8192)
                artifacts=[count for name,count in result['database']['rows'].items() if name.endswith('artifacts')]
                self.assertTrue(artifacts and any(count>0 for count in artifacts))
                calls=[json.loads(line) for line in (Path(root)/profile/'calls.jsonl').read_text().splitlines()]
                self.assertTrue(all(row['queued']==0 and row['retained_bytes']==0 for row in calls))
                results.append(result)
            self.assertTrue(all(row['expected_output']==results[0]['expected_output'] for row in results))
            self.assertIn('/v1/autoagent/',results[0]['catalog']['package'])
            self.assertNotIn('/v1/',results[1]['catalog']['package'])

    def test_buffered_context_updates_are_drained_before_timing_completes(self):
        """Buffered Context records accepted during flush are durable on return."""
        with tempfile.TemporaryDirectory() as root:
            result=run_case('v2-buffered-artifacts-8k',dict(name='context-drain',family='loop',
                iterations=20,context='append',payload_bytes=65536,calls=2),root,backend=self.backend)
            self.assertEqual(result['status'],'completed',result)
            records=[json.loads(line) for line in (Path(root)/'calls.jsonl').read_text().splitlines()]
            self.assertTrue(all(row['queued']==0 and row['retained_bytes']==0 for row in records))

    def test_accepted_completion_is_flushed_before_durable_value_read(self):
        """Slow storage cannot turn an accepted terminal result into a null output."""
        with tempfile.TemporaryDirectory() as root:
            result = run_case('v2-buffered-artifacts-8k', dict(name='accepted-value',
                family='chain', nodes=2, payload_bytes=16384, calls=8,
                unique_payload=True, persist_delay_ms=20), root, backend=self.backend)
            self.assertEqual(result['status'], 'completed', result)
            self.assertEqual(result['completed'], 8)

    def test_diagnostics_and_revision_rotation_preserve_native_results(self):
        """Diagnostic counters separate SQL phases while calls rotate real revisions."""
        with tempfile.TemporaryDirectory() as root:
            result = run_case('v2-artifacts-8k', dict(name='diagnostic-rotation',
                family='chain', nodes=2, definitions=3, rotate_definitions=True,
                payload_bytes=16384, calls=3, instrument_sql=True), root, backend=self.backend)
            self.assertEqual(result['status'], 'completed', result)
            self.assertEqual(len(result['catalog']['revisions']), 3)
            before, after = result['sql_before'], result['sql_after']
            self.assertGreater(after['statements'], before['statements'])
            phases = {item['phase'] for item in after['sql'].values()}
            self.assertIn('observation_sql', phases)
            self.assertIn('admission_and_ack', phases)
            self.assertIn('write_transaction', phases)
            self.assertGreater(after['phases']['driver_commit']['calls'], 0)
            self.assertGreater(after['phases']['encoding']['calls'], 0)
            self.assertGreater(after['phases']['admission_lock_wait']['calls'], 0)
            self.assertGreater(after['phases']['admission_lock_hold']['elapsed_ms'], 0)
            self.assertGreater(after['queue']['peak_count'], 0)
            self.assertGreater(after['queue']['peak_retained_bytes'], 0)
            cache = after['writer_metadata_cache']
            self.assertTrue(cache['available'])
            self.assertLessEqual(cache['peak_committed_bytes'], cache['budget'])
            self.assertLessEqual(len(after['sql']), 257)
            self.assertNotIn('xxxxxxxxxxxxxxxx', json.dumps(after))

    def test_concurrent_frontier_barriers_preserve_durable_outputs(self):
        """Concurrent captured frontiers make accepted outputs durable without global drain."""
        with tempfile.TemporaryDirectory() as root:
            result = run_case('v2-buffered-artifacts-8k', dict(name='captured-frontiers',
                family='chain', nodes=2, payload_bytes=16384, calls=8, concurrency=4,
                unique_payload=True, persist_delay_ms=20, instrument_sql=True,
                captured_frontier_barrier=True), root, backend=self.backend)
            self.assertEqual(result['status'], 'completed', result)
            self.assertEqual(result['completed'], 8)
            records = [json.loads(line) for line in (Path(root)/'calls.jsonl').read_text().splitlines()]
            self.assertEqual(len({row['session_id'] for row in records}), 8)
            self.assertTrue(all(row['ended_seconds'] > row['started_seconds'] for row in records))
            health = result['barrier']['health']
            self.assertEqual(health['queued_count'], 0)
            self.assertEqual(health['accepted_frontier'], health['durable_frontier'])
            batches = result['sql_after']['batches']
            self.assertEqual(sum(batches['sessions_per_batch'].values()), batches['count'])
            self.assertEqual(sum(int(size)*count for size, count in batches['runtime_events_per_batch'].items()),
                             batches['runtime_events'])

    def test_duration_churn_runs_past_call_limit_and_records_health(self):
        """Timed churn continues to its deadline with new Sessions, payloads and revisions."""
        with tempfile.TemporaryDirectory() as root:
            result = run_case('v2-buffered-artifacts-8k', dict(name='timed-churn',
                family='chain', nodes=1, payload_bytes=16384, calls=1,
                definitions=3, rotate_definitions=True, unique_payload=True,
                period_seconds=0, sample_health_every=1), root, backend=self.backend, duration=2)
            self.assertEqual(result['status'], 'completed', result)
            self.assertGreater(result['completed'], 1)
            self.assertGreaterEqual(result['run_seconds'], 2)
            self.assertEqual(len(result['catalog']['revisions']), 3)
            calls = [json.loads(line) for line in (Path(root)/'calls.jsonl').read_text().splitlines()]
            self.assertEqual(len({row['session_id'] for row in calls}), result['completed'])
            self.assertEqual(len({row['output']['digest'] for row in calls}), result['completed'])
            samples = [json.loads(line) for line in (Path(root)/'health.jsonl').read_text().splitlines()]
            self.assertEqual(samples[-1]['completed'], result['completed'])
            self.assertTrue(all(cache['bytes'] <= cache['budget'] for row in samples
                                for cache in row['caches'].values()))

    def test_frozen_controller_keeps_native_v1_package_root(self):
        """Copying the benchmark tools cannot redirect the archived V1 package root."""
        with tempfile.TemporaryDirectory() as root:
            folder=Path(root)/'harness';folder.mkdir()
            source=Path(__file__).parent/'benchmarks/server_comparison'
            for name in ('run.py','worker.py','business.py','diagnostics.py'):
                shutil.copy2(source/name,folder/name)
            spec=importlib.util.spec_from_file_location('frozen_test_controller',folder/'run.py')
            controller=importlib.util.module_from_spec(spec);spec.loader.exec_module(controller)
            controller.ROOT=Path(__file__).resolve().parents[1]
            result=controller.run_case('v1-full',dict(name='frozen-v1',family='chain',
                nodes=1,payload_bytes=16384,calls=1),Path(root)/'run',backend=self.backend)
            self.assertEqual(result['status'],'completed',result)
            self.assertIn('/v1/autoagent/',result['catalog']['package'])


@unittest.skipUnless(POSTGRES_TEST_URL,'PostgreSQL URL is required')
class PostgreSQLServerComparisonTests(ServerComparisonTests):
    backend = 'postgres'


class DiagnosticScopeTests(unittest.IsolatedAsyncioTestCase):
    async def test_nested_projection_queries_keep_observation_ownership(self):
        """Accepted overlay work stays attributed to its observer, even on failure."""
        from tests.benchmarks.server_comparison.diagnostics import Diagnostics
        diagnostic = Diagnostics()
        seen = []

        class Probe:
            async def project(self):
                seen.append(diagnostic.phase.get())
                raise RuntimeError('expected')

            async def observe(self):
                await self.project()

        diagnostic.wrap(Probe, 'project', 'projection_compute')
        diagnostic.wrap(Probe, 'observe', 'observation_sql')
        with self.assertRaisesRegex(RuntimeError, 'expected'):
            await Probe().observe()
        self.assertEqual(seen, ['observation_sql'])
        self.assertEqual(diagnostic.phase.get(), 'background')
        self.assertEqual(diagnostic.snapshot()['phases']['projection_compute']['errors'], 1)

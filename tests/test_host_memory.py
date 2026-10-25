"""Bounded memory telemetry across Host lifecycle, HTTP and both databases."""

import asyncio
from dataclasses import replace
import io
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import httpx
from typing_extensions import TypedDict

from autoagent import Node, Workflow
from autoagent.host import AutoAgentHost, HostSettings
from autoagent.host.memory import cgroup_sample, process_sample
from autoagent.server import create_app
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase


class Value(TypedDict):
    text: str


def identity(value: Value) -> Value:
    return value


class HostMemoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.url = 'sqlite+aiosqlite:///' + str(self.root / 'runtime.db')
        self.database = None
        if type(self).__name__.startswith('PostgreSQL'):
            self.database = PostgreSQLDatabase()
            self.url = await self.database.create()
        self.settings = HostSettings(database_url=self.url, memory_monitor_enabled=True,
            memory_monitor_interval_seconds=.05, memory_monitor_history_samples=3,
            memory_monitor_top_sessions=2, memory_budget_bytes=2 * 1024**3)

    async def asyncTearDown(self):
        if self.database is not None:
            await self.database.close()
        self.temp.cleanup()

    def host(self, *, settings=None, read_only=False):
        return AutoAgentHost(workflows=[Workflow('memory', nodes=[Node('work', identity)])],
            root=self.root, settings=settings or self.settings, environment={}, read_only=read_only)

    async def test_bounded_sampling_survives_blocked_host_loop_and_closes_thread(self):
        """Process sampling continues independently and all workers stop on close."""
        async with self.host() as host:
            monitor = host._memory_monitor
            thread = monitor.thread
            with (patch.object(host._runtime.app, 'acapture_checkpoint', side_effect=AssertionError('No checkpoint')),
                  patch.object(host._runtime.app, 'aresidency_info', side_effect=AssertionError('No State walk'))):
                time.sleep(.25)
                snapshot = await host.memory_snapshot(history=3)
            self.assertEqual(len(snapshot['history']), 3)
            self.assertFalse(snapshot['stale'])
            self.assertGreater(snapshot['latest']['process']['rss_bytes'], 0)
            self.assertEqual(snapshot['latest']['host']['resident_graphs'], 0)
            snapshot['latest']['host']['resident_graphs'] = 999
            self.assertEqual((await host.memory_snapshot())['latest']['host']['resident_graphs'], 0)
        self.assertFalse(thread.is_alive())
        self.assertIsNone(monitor.task)

    async def test_http_metrics_and_cli_read_the_running_server(self):
        """HTTP and CLI expose bounded telemetry without importing project code."""
        from autoagent.cli.main import main
        async with self.host() as host:
            app = create_app(host, ui=False)
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                response = await client.get('/api/v2/memory', params={'history': 3})
                self.assertEqual(response.status_code, 200, response.text)
                self.assertTrue(response.json()['enabled'])
                self.assertEqual((await client.get('/api/v2/memory', params={'history': 4})).status_code, 400)
                self.assertEqual((await client.get('/api/v2/memory', params={'history': 3601})).status_code, 422)
                metrics = await client.get('/api/v2/memory/metrics')
                self.assertIn('autoagent_memory_rss_bytes', metrics.text)
                self.assertNotIn('session_id', metrics.text)
                snapshot = response.json()
            async def request(*args, **kwargs):
                self.assertEqual(args[2], '/memory')
                return snapshot
            stdout, stderr = io.StringIO(), io.StringIO()
            with patch('autoagent.cli.remote.ServerClient.request', request):
                with patch('sys.stdout', stdout), patch('sys.stderr', stderr):
                    result = await asyncio.to_thread(main, ['--server', 'http://test/api/v2', 'memory', '--format', 'json'])
            self.assertEqual(result, 0, stderr.getvalue())
            self.assertTrue(json.loads(stdout.getvalue())['data']['enabled'])
            from autoagent.cli.output import render
            self.assertIn('RSS ', render(json.loads(stdout.getvalue()), 'text'))
            self.assertIn('not an RSS breakdown', render(json.loads(stdout.getvalue()), 'text'))

    async def test_disabled_read_only_and_query_boundaries(self):
        """Disabled sampling creates no thread and read-only telemetry starts no Core."""
        async with self.host(settings=replace(self.settings, memory_monitor_enabled=False)) as host:
            self.assertIsNone(host._memory_monitor.thread)
            self.assertFalse((await host.memory_snapshot())['enabled'])
            self.assertIsNone((await host.memory_snapshot())['latest'])
        async with self.host(read_only=True) as host:
            self.assertIsNone(host._runtime)
            self.assertTrue((await host.memory_snapshot())['enabled'])
            with self.assertRaises(ValueError):
                await host.memory_snapshot(history=-1)

    async def test_counters_reuse_cached_residency_and_do_not_write_events(self):
        """Telemetry observes cached estimates and does not append persistent Events."""
        async with self.host() as host:
            result = await host.join(await host.execute('memory', {'text': 'ok'}, entry_node_id='work'))
            self.assertEqual(result.output, {'text': 'ok'})
            before = (await host._persistence.health()).accepted_frontier
            with patch.object(host._runtime.app, 'aresidency_info', side_effect=AssertionError('No State walk')):
                await host._memory_monitor.refresh()
                host._memory_monitor._sample()
                data = (await host.memory_snapshot())['latest']['host']
                self.assertEqual(data['resident_graphs'], 1)
            self.assertEqual((await host._persistence.health()).accepted_frontier, before)
            self.assertLessEqual(len(data['residency']['top_sessions']), 2)
            self.assertEqual(data['persistence']['queued_count'], 0)

    async def test_telemetry_failure_does_not_fail_host_startup(self):
        """A failed initial counter read leaves process monitoring and execution usable."""
        with patch('autoagent.host.memory.MemoryMonitor.refresh', side_effect=RuntimeError('unavailable')):
            async with self.host() as host:
                self.assertIsNotNone((await host.memory_snapshot())['latest'])
                result = await host.join(await host.execute('memory', {'text': 'ok'}, entry_node_id='work'))
                self.assertEqual(result.output, {'text': 'ok'})

    async def test_multiple_hosts_share_sampler_and_close_independently(self):
        """Closing one Host keeps the shared sampler alive for another Host."""
        from autoagent.host.memory import MemoryMonitor
        from types import SimpleNamespace
        async with self.host() as host:
            other = MemoryMonitor(SimpleNamespace(settings=replace(self.settings), _runtime=None,
                _persistence=host._persistence, _operations=set(), _request_bytes=0))
            await other.start()
            self.assertIs(other.thread, host._memory_monitor.thread)
            try:
                await host._memory_monitor.stop()
                before = other.latest['monotonic_seconds']
                await asyncio.sleep(.1)
                self.assertGreater(other.latest['monotonic_seconds'], before)
            finally:
                await other.stop()

    async def test_last_sampler_can_stop_while_new_generation_starts(self):
        """A new Host cannot clear the previous generation's shutdown notification."""
        from autoagent.host.memory import MemoryMonitor, _sampler
        from types import SimpleNamespace
        entered, release = threading.Event(), threading.Event()
        async with self.host() as host:
            first = host._memory_monitor
            first.settings = replace(first.settings, memory_monitor_interval_seconds=3600)
            original = first._sample
            def slow_sample(**kwargs):
                entered.set()
                release.wait(3)
                original(**kwargs)
            first._sample = slow_sample
            first.next_sample = 0
            _sampler.wake.set()
            self.assertTrue(await asyncio.to_thread(entered.wait, 2))
            closing = asyncio.create_task(first.stop())
            other = MemoryMonitor(SimpleNamespace(settings=replace(self.settings, memory_monitor_interval_seconds=3600),
                _runtime=None, _persistence=host._persistence, _operations=set(), _request_bytes=0))
            try:
                for _ in range(100):
                    if _sampler.thread is None:
                        break
                    await asyncio.sleep(.01)
                else:
                    self.fail('Previous generation did not unregister')
                await other.start()
                release.set()
                await asyncio.wait_for(asyncio.shield(closing), .5)
                self.assertTrue(other.thread.is_alive())
            finally:
                release.set()
                await other.stop()
                await closing

    async def test_sampling_covers_runtime_shutdown_checkpoint_capture(self):
        """Closing Core and saving shutdown Checkpoints do not end process sampling early."""
        async with self.host() as host:
            monitor = host._memory_monitor
            shutdown = host._runtime.shutdown
            async def delayed_shutdown():
                before = monitor.latest['monotonic_seconds']
                await asyncio.sleep(.15)
                after = monitor.latest['monotonic_seconds']
                await shutdown()
                self.assertGreater(after, before)
            host._runtime.shutdown = delayed_shutdown
        self.assertIsNone(monitor.thread)

    async def test_sampling_starts_before_workflow_compilation(self):
        """Compilation can consume memory before Core and Persistence are ready."""
        from autoagent import WorkflowCompiler
        original = WorkflowCompiler.compile_or_raise
        host = self.host()
        observed = []
        def compile_with_observation(compiler, *args, **kwargs):
            monitor = host._memory_monitor
            if monitor is not None and host._persistence is None:
                before = monitor.latest['monotonic_seconds']
                time.sleep(.1)
                observed.append(monitor.latest['monotonic_seconds'] > before)
            return original(compiler, *args, **kwargs)
        with patch.object(WorkflowCompiler, 'compile_or_raise', compile_with_observation):
            async with host:
                self.assertTrue(observed)
                self.assertTrue(all(observed))


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL URL is required')
class PostgreSQLHostMemoryTests(HostMemoryTests):
    pass


class MemoryConfigurationTests(unittest.TestCase):
    def test_environment_and_limits(self):
        """Strict booleans and bounded sampling settings reject invalid deployment values."""
        settings = HostSettings.from_environment({'AUTOAGENT_MEMORY_MONITOR_ENABLED': 'true',
            'AUTOAGENT_MEMORY_MONITOR_INTERVAL_SECONDS': '.5', 'AUTOAGENT_MEMORY_MONITOR_HISTORY_SAMPLES': '60'})
        self.assertTrue(settings.memory_monitor_enabled)
        self.assertEqual(settings.memory_monitor_interval_seconds, .5)
        self.assertEqual(settings.memory_monitor_history_samples, 60)
        for changes in ({'memory_monitor_enabled': 1}, {'memory_monitor_interval_seconds': 0},
                        {'memory_monitor_history_samples': 3601}, {'memory_monitor_top_sessions': 0}):
            with self.assertRaises(ValueError):
                replace(settings, **changes)
        with self.assertRaises(ValueError):
            HostSettings.from_environment({'AUTOAGENT_MEMORY_MONITOR_ENABLED': 'sometimes'})

    def test_unavailable_measurements_and_pressure_hysteresis(self):
        """Unavailable RSS stays null; pressure logs change only across hysteresis boundaries."""
        from types import SimpleNamespace
        from autoagent.host.memory import MemoryMonitor
        with patch('pathlib.Path.read_text', side_effect=OSError('unsupported')):
            self.assertIsNone(process_sample()['rss_bytes'])
            self.assertIsNone(cgroup_sample(None))
        host = SimpleNamespace(settings=HostSettings(memory_budget_bytes=100), _runtime=None)
        monitor = MemoryMonitor(host)
        with patch('autoagent.host.memory.process_sample', side_effect=[{'rss_bytes': x} for x in (81, 78, 64, 91, 85)]), patch('autoagent.host.memory.log') as logger:
            for _ in range(5):
                monitor._sample()
            self.assertEqual(logger.warning.call_count, 3)
            self.assertEqual(logger.info.call_count, 1)
            self.assertEqual(monitor.pressure, 'high')

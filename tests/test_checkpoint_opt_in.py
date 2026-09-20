"""Optional lifecycle capture must bypass checkpoint construction, not cleanup."""

from tests.graph_fixtures import (
    load_graph,
    resume_graph_wait,
)
import asyncio
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

from autoagent import AutoAgentApp, Node, Wait, Workflow
from autoagent.core import AppCheckpoint, SessionCheckpoint, RuntimeGraphCheckpoint
from autoagent.core.errors import RuntimeInfrastructureError
from tests.benchmarks.benchmark_core_audit import Value, identity
from tests.test_phase13_lifecycle_recovery import _CoordinatedCloseApp, _release_runtime_gate


def forbid_capture(app):
    stack = ExitStack()
    for name in ('_capture_checkpoint', '_capture_checkpoint_locked'):
        stack.enter_context(patch.object(app, name, new=AsyncMock(side_effect=AssertionError('capture reached'))))
    stack.enter_context(patch.object(app._repository, 'capture_checkpoint', side_effect=AssertionError('capture reached')))
    stack.enter_context(patch('autoagent.core.app.app.AppCheckpoint', side_effect=AssertionError('bundle constructed')))
    return stack


class CheckpointOptInTests(unittest.TestCase):
    def test_default_close_skips_all_capture_even_when_repeated(self):
        """Default close returns None for empty/populated Apps and never constructs a checkpoint."""
        for populated in (False, True):
            with self.subTest(populated=populated):
                app = AutoAgentApp()
                if populated:
                    app.invoke(Workflow('default-close', nodes=[Node('work', identity)]), {'value': 1})
                with forbid_capture(app):
                    self.assertIsNone(app.close())
                    self.assertIsNone(app.close())
                    self.assertIsNone(app.close(capture_checkpoint=True))
                self.assertTrue(app._closed)
                self.assertIsNone(app._closed_checkpoint)

    def test_default_unload_skips_capture_and_releases_state(self):
        """Default unload still discards the Session while bypassing capture at every layer."""
        app = AutoAgentApp()
        try:
            result = app.invoke(Workflow('default-unload', nodes=[Node('wait', Wait(Value, Value))]), {'value': 1})
            with forbid_capture(app):
                self.assertIsNone(app.unload_session(result.ref))
                self.assertEqual(app.resident_invocations(), ())
                self.assertIsNone(app.close())
        finally:
            app.close()

    def test_async_defaults_skip_capture(self):
        """Asynchronous unload and close use the same capture-free default path."""
        async def run():
            app = AutoAgentApp()
            try:
                result = await app.ainvoke(Workflow('async-default', nodes=[Node('work', identity)]), {'value': 1})
                with forbid_capture(app):
                    self.assertIsNone(await app.aunload_session(result.ref))
                    self.assertIsNone(await app.aclose())
                    self.assertIsNone(await app.aclose(capture_checkpoint=True))
            finally:
                await app.aclose()
        asyncio.run(run())

    def test_explicit_unload_checkpoint_restores_wait(self):
        """Opted-in Session capture preserves the load/resume round trip."""
        workflow = Workflow('explicit-unload', nodes=[Node('wait', Wait(Value, Value))])
        app, restored = AutoAgentApp(), AutoAgentApp()
        try:
            result = app.invoke(workflow, {'value': 1})
            checkpoint = app.unload_session(result.ref, capture_checkpoint=True)
            self.assertIsInstance(checkpoint, RuntimeGraphCheckpoint)
            restored.register_workflow(workflow)
            ref = load_graph(restored, checkpoint).invocations[0]
            self.assertEqual(resume_graph_wait(restored, ref, result.waits[0].id, {'value': 2}).output, {'value': 2})
        finally:
            app.close()
            restored.close()

    def test_explicit_close_caches_one_checkpoint(self):
        """Opted-in close returns one cached bundle even when later callers use defaults."""
        app = AutoAgentApp()
        app.invoke(Workflow('explicit-close', nodes=[Node('work', identity)]), {'value': 1})
        with patch.object(app._repository, 'capture_checkpoint', wraps=app._repository.capture_checkpoint) as capture:
            checkpoint = app.close(capture_checkpoint=True)
            self.assertIsInstance(checkpoint, AppCheckpoint)
            self.assertEqual(len(checkpoint.graphs), 1)
            self.assertIs(app.close(), checkpoint)
            self.assertIs(app.close(capture_checkpoint=True), checkpoint)
            self.assertEqual(capture.call_count, 1)

    def test_default_close_still_retries_pending_ack(self):
        """Skipping checkpoints cannot drop a captured Event whose first ACK failed."""
        class Sink:
            event = None
            attempts = 0
            async def append(self, event):
                if event.event_name == 'invocation.completed':
                    self.attempts += 1
                    if self.event is None:
                        self.event = event
                        raise OSError('ACK lost')
                    assert event is self.event
        sink = Sink()
        app = AutoAgentApp(runtime_event_sink=sink)
        with self.assertRaises(RuntimeInfrastructureError):
            app.invoke(Workflow('pending-close', nodes=[Node('work', identity)]), {'value': 1})
        with forbid_capture(app):
            self.assertIsNone(app.close())
        self.assertEqual(sink.attempts, 2)
        self.assertFalse(app._repository._pending)

    def test_concurrent_close_uses_first_call_capture_option(self):
        """Concurrent mixed options share the first admitted close operation and result."""
        async def run(capture_first):
            app = _CoordinatedCloseApp()
            await app.ainvoke(Workflow('mixed-close', nodes=[Node('work', identity)]), {'value': 1})
            first = asyncio.create_task(app.aclose(capture_checkpoint=capture_first))
            self.assertTrue(await app.close_operation_entered.wait_async())
            second = asyncio.create_task(app.aclose(capture_checkpoint=not capture_first))
            try:
                self.assertTrue(await app.both_close_callers_entered.wait_async())
            finally:
                await _release_runtime_gate(app._runtime_loop, app.release_close_operations)
            results = await asyncio.wait_for(asyncio.gather(first, second), 2)
            self.assertIs(results[0], results[1])
            self.assertEqual(isinstance(results[0], AppCheckpoint), capture_first)
            self.assertEqual(app.close_entries, 1)
        for capture_first in (False, True):
            with self.subTest(capture_first=capture_first):
                asyncio.run(run(capture_first))

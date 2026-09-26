"""Observable terminal boundaries include cancellation cleanup and owned children."""
import asyncio
import json
import threading
import unittest
from dataclasses import replace

from autoagent import AutoAgentApp, Edge, Node, Workflow, Wait, RuntimeInfrastructureError
from autoagent.core import RuntimeEvent, RuntimeState, StateReducer, SessionCheckpoint
from tests.graph_fixtures import graph_bundle
from tests.benchmarks.benchmark_core_audit import Value, identity
from tests.test_runtime_graph import Collector


class InvocationSettlingTests(unittest.TestCase):
    def exercise_cleanup(self, outcome, child, repeat_cancel=False):
        started, cleaning, release, cleaned = (threading.Event() for _ in range(4))

        async def slow(value: Value) -> Value:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaning.set()
                while not release.is_set():
                    await asyncio.sleep(.001)
                cleaned.set()
            return value

        async def fail(value: Value) -> Value:
            while not started.is_set():
                await asyncio.sleep(.001)
            raise ValueError('original failure')

        work = Node('slow', Workflow('child', nodes=[Node('slow', slow)]), execution_mode='spawn') if child else Node('slow', slow)
        if outcome == 'failed':
            workflow = Workflow('root', nodes=[Node('start', identity), work, Node('fail', fail)],
                                edges=[Edge('start', 'slow'), Edge('start', 'fail')])
        else:
            workflow = Workflow('root', nodes=[work])
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        cancellations = []
        try:
            ref = app.submit_invoke(workflow, {'value': 1}, session_id='root').ref
            self.assertTrue(started.wait(2))
            if outcome == 'cancelled':
                if child:
                    async def wait_settling():
                        while app._repository.state('root').invocation.status != 'settling':
                            await asyncio.sleep(.001)
                    app._runtime_loop.run(asyncio.wait_for(wait_settling(), 2))
                    self.assertEqual(app.status(ref).pending_outcome, 'completed')
                cancellations.append(app._submit(app._cancel(ref, 'stop')))
            self.assertTrue(cleaning.wait(2))
            if repeat_cancel:
                cancellations.append(app._submit(app._cancel(ref, 'again')))
                # Let the second coordinator reach its task barrier.
                app._runtime_loop.run(asyncio.sleep(.02))
            result = app.status(ref)
            self.assertEqual(result.status, 'settling')
            self.assertEqual(result.pending_outcome, outcome)
            state = app._repository.state('root')
            self.assertIsNone(state.invocation.completed_at_us)
            self.assertIsNone(result.output)
            self.assertFalse(cleaned.is_set())
            self.assertFalse(any(e.session_id == 'root' and e.payload.kind in {
                'invocation.completed', 'invocation.failed', 'invocation.cancelled'} for e in sink.events))
            RuntimeState.from_record(json.loads(json.dumps(state.to_record())))
            with self.assertRaises(TimeoutError):
                app.join(ref, timeout=.01)
            release.set()
            for future in cancellations:
                self.assertEqual(future.result(2).status, outcome)
            result = app.join(ref, timeout=2)
            self.assertEqual(result.status, outcome)
            self.assertIsNone(result.pending_outcome)
            self.assertTrue(cleaned.is_set())
            if outcome == 'failed':
                self.assertIn('original failure', result.error.message)
            # Replay the actual stream and validate all Session states at every boundary.
            states = {}
            for e in sink.events:
                e = RuntimeEvent.from_record(json.loads(json.dumps(e.to_record())))
                states[e.session_id] = StateReducer().apply(states.get(e.session_id, RuntimeState()), e)
                RuntimeState.from_record(states[e.session_id].to_record())
            self.assertEqual(states['root'], app._repository.state('root'))
        finally:
            release.set()
            app.close(timeout=3)

    def test_cancel_waits_for_own_cleanup(self):
        """A leaf remains settling until its cancelled Operator finishes cleanup."""
        self.exercise_cleanup('cancelled', False)

    def test_failure_waits_for_own_cleanup(self):
        """Fail-fast waits for a sibling Operator's cleanup before publishing failure."""
        self.exercise_cleanup('failed', False)

    def test_cancel_successful_parent_waits_for_child_cleanup(self):
        """Cancellation replaces pending success and waits for Child cleanup."""
        self.exercise_cleanup('cancelled', True)

    def test_failure_waits_for_child_cleanup(self):
        """Parent failure stays pending until its owned Child finishes cancellation."""
        self.exercise_cleanup('failed', True)

    def test_repeated_cancel_does_not_interrupt_cleanup(self):
        """A second cancel request neither re-cancels cleanup nor publishes early."""
        self.exercise_cleanup('cancelled', True, repeat_cancel=True)

    def test_cancel_preserves_failure_and_cleanup(self):
        """Cancel during failure preserves the original error and cleanup barrier."""
        self.exercise_cleanup('failed', True, repeat_cancel=True)

    def test_no_child_success_avoids_extra_event(self):
        """Successful leaf execution still commits its terminal boundary directly."""
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(Workflow('leaf', nodes=[Node('n', identity)]), {'value': 1})
            self.assertEqual(result.status, 'completed')
            self.assertNotIn('invocation.settling', [e.payload.kind for e in sink.events])
            state = app._repository.state(result.session_id)
            with self.assertRaises(ValueError):
                RuntimeState.from_record(replace(state, invocation=replace(
                    state.invocation, pending_outcome='completed')).to_record())
        finally:
            app.close()

    def test_every_cancellation_prefix_recovers_nested_graph(self):
        """Every stop/terminal prefix resumes convergence without replaying business work."""
        sink = Collector()
        leaf = Workflow('leaf-wait', nodes=[Node('wait', Wait(Value, Value))])
        child = Workflow('child-wait', nodes=[Node('spawn', leaf, execution_mode='spawn')])
        root = Workflow('root-wait', nodes=[Node('spawn', child, execution_mode='spawn')])
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(root, {'value': 1}, session_id='root')
            app.cancel(result.ref, 'stop')
        finally:
            app.close()
        states = {}
        stop_seen = False
        prefixes = []
        for event in sink.events:
            states[event.session_id] = StateReducer().apply(states.get(event.session_id, RuntimeState()), event)
            if event.payload.kind == 'invocation.settling' and event.payload.outcome == 'cancelled':
                stop_seen = True
            if stop_seen:
                prefixes.append(dict(states))
        self.assertGreater(len(prefixes), 3)
        for prefix in prefixes:
            with self.subTest(sequences={sid: state.sequence for sid, state in prefix.items()}):
                restored_sink = Collector()
                restored = AutoAgentApp(runtime_event_sink=restored_sink)
                try:
                    restored.register_workflow(root)
                    bundle = graph_bundle(tuple(SessionCheckpoint.from_state(state) for state in prefix.values()))
                    ref = restored.load_checkpoint(bundle).invocations[0]
                    self.assertEqual(restored.recover(ref).status, 'cancelled')
                    self.assertTrue(all(restored._repository.state(sid).invocation.terminal
                                        for sid in restored._repository.session_ids()))
                    self.assertFalse(any(e.payload.kind == 'node_occurrence.started' for e in restored_sink.events))
                finally:
                    restored.close()

    def test_terminal_ack_failure_retries_exact_event(self):
        """A lost final ACK leaves settling visible and recovery retries the same identity."""
        class LostAck(Collector):
            fail = True
            async def append(self, event):
                await super().append(event)
                if self.fail and event.session_id == 'root' and event.payload.kind == 'invocation.cancelled':
                    raise OSError('lost ACK')
        sink = LostAck()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            child = Workflow('wait-child', nodes=[Node('wait', Wait(Value, Value))])
            root = Workflow('wait-root', nodes=[Node('spawn', child, execution_mode='spawn')])
            result = app.invoke(root, {'value': 1}, session_id='root')
            with self.assertRaises(RuntimeInfrastructureError):
                app.cancel(result.ref)
            inv = app._repository.state('root').invocation
            self.assertEqual((inv.status, inv.pending_outcome), ('settling', 'cancelled'))
            sink.fail = False
            self.assertEqual(app.recover(result.ref).status, 'cancelled')
            events = [e for e in sink.events if e.session_id == 'root' and e.payload.kind == 'invocation.cancelled']
            self.assertGreaterEqual(len(events), 2)
            self.assertEqual(len({e.id for e in events}), 1)
        finally:
            sink.fail = False
            app.close()

    def test_cancel_restored_success_intent_after_children_settle(self):
        """Cancellation can replace a saved success intent even with no work remaining."""
        from autoagent.core import InvocationCompleted, RuntimeRepository, RuntimeTransitionError
        app = AutoAgentApp()
        workflow = Workflow('success', nodes=[Node('n', identity)])
        try:
            result = app.invoke(workflow, {'value': 1})
            state = app._repository.state(result.session_id)
            saved = replace(state, invocation=replace(state.invocation, status='settling',
                            pending_outcome='completed', completed_at_us=None))
            repository = RuntimeRepository()
            repository.install_states({result.session_id: saved})
            with self.assertRaisesRegex(RuntimeTransitionError, 'OUTCOME_CONFLICT'):
                asyncio.run(repository.commit(session_id=result.session_id,
                    invocation_id=result.invocation_id, payload=InvocationCompleted({'value': 2})))
            app.unload_session(result.ref)
            ref = app.load_checkpoint(graph_bundle((SessionCheckpoint.from_state(saved),))).invocations[0]
            self.assertEqual(app.cancel(ref).status, 'cancelled')
        finally:
            app.close()

"""Root-only lifecycle, structured completion and graph recovery contracts."""

from tests.graph_fixtures import (
    graph_bundle,
)
import asyncio
import json
import threading
import unittest
from dataclasses import replace
from autoagent import (AutoAgentApp, AppCheckpoint, ChildHandle, InvocationRef, Map,
    Node, Workflow, Wait, RuntimeGraphCheckpoint, RuntimeTransitionError, Recovery)
from autoagent.core import SessionCheckpoint, StateReducer, RuntimeState
from tests.benchmarks.benchmark_core_audit import Value, identity, items


class Collector:
    def __init__(self):
        self.events = []
    async def append(self, event):
        self.events.append(event)


class RuntimeGraphTests(unittest.TestCase):
    def test_spawn_barrier_and_release_payloads(self):
        """Parent body finishes once and releases its payloads while a Child runs."""
        started, release = threading.Event(), threading.Event()
        async def slow(v: Value) -> Value:
            started.set()
            while not release.is_set():
                await asyncio.sleep(.001)
            return v
        app = AutoAgentApp()
        try:
            ref = app.submit_invoke(Workflow('root', nodes=[Node('spawn', Workflow('child', nodes=[Node('work', slow)]), execution_mode='spawn')]), {'value': 1}).ref
            self.assertTrue(started.wait(1))
            async def inspect():
                for _ in range(1000):
                    inv = app._repository.state(ref.session_id).invocation
                    if inv.status == 'joining_children':
                        self.assertTrue(all(o.output is None for o in inv.scheduler.occurrences.values()))
                        return
                    await asyncio.sleep(.001)
                self.fail('Parent never reached its completion barrier')
            app._runtime_loop.run(inspect())
            with self.assertRaises(TimeoutError):
                app.join(ref, timeout=.01)
            self.assertEqual(app.status(ref).status, 'joining_children')
            release.set()
            result = app.join(ref, timeout=2)
            self.assertEqual(result.status, 'completed')
            self.assertIsInstance(result.output, ChildHandle)
        finally:
            release.set()
            app.close()

    def test_nested_wait_routes_through_root_and_unloads_graph(self):
        """Nested external Waits resume by Root identity and unload as one graph."""
        app = AutoAgentApp()
        try:
            leaf = Workflow('leaf', nodes=[Node('wait', Wait(Value, Value))])
            child = Workflow('child', nodes=[Node('spawn', leaf, execution_mode='spawn')])
            root = Workflow('root', nodes=[Node('spawn', child, execution_mode='spawn')])
            result = app.invoke(root, {'value': 1})
            self.assertEqual(result.status, 'joining_children')
            self.assertEqual(len(result.waits), 1)
            self.assertEqual(app.resident_invocations(), (result.ref,))
            cp = app.unload_session(result.ref, capture_checkpoint=True)
            self.assertEqual(len(cp.sessions), 3)
            self.assertEqual(app._repository.session_ids(), ())
            self.assertFalse(app._child_owners)
            refs = app.load_checkpoint(RuntimeGraphCheckpoint.from_record(json.loads(json.dumps(cp.to_record())))).invocations
            self.assertEqual(refs, (result.ref,))
            result = app.recover(refs[0])
            final = app.resume(refs[0], result.waits[0].id, {'value': 9})
            self.assertEqual(final.status, 'completed')
            self.assertTrue(all(app._repository.state(sid).invocation.terminal for sid in app._repository.session_ids()))
        finally:
            app.close()

    def test_forged_child_control_and_root_admission_rejected(self):
        """Neither a Handle nor a forged control reference grants Child control."""
        app = AutoAgentApp()
        try:
            c = Workflow('child', nodes=[Node('work', identity)])
            r = app.invoke(Workflow('root', nodes=[Node('spawn', c, execution_mode='spawn')]), {'value': 1})
            h = r.output
            forged = InvocationRef(session_id=h.child_session_id, invocation_id=h.child_invocation_id,
                                   workflow_id=c.id, workflow_revision_id=h.workflow_revision_id)
            for method in (app.status, app.join, app.cancel, app.recover, app.unload_session):
                with self.subTest(method=method.__name__):
                    with self.assertRaises(TypeError): method(h)
                    with self.assertRaisesRegex(RuntimeTransitionError, 'CHILD_CONTROL_FORBIDDEN'): method(forged)
            with self.assertRaisesRegex(RuntimeTransitionError, 'SESSION_OWNED_BY_CHILD'):
                app.invoke(c, {'value': 2}, session_id=h.child_session_id)
        finally:
            app.close()

    def test_spawn_failure_does_not_fail_parent(self):
        """A failed Spawn subtree settles without changing the Parent business result."""
        def fail(v: Value) -> Value: raise ValueError('expected')
        app = AutoAgentApp()
        try:
            r = app.invoke(Workflow('root', nodes=[Node('spawn', Workflow('bad', nodes=[Node('bad', fail)]), execution_mode='spawn')]), {'value': 1})
            self.assertEqual(r.status, 'completed')
            self.assertEqual(app._repository.state(r.output.child_session_id).invocation.status, 'failed')
        finally: app.close()

    def test_cancel_waiting_nested_graph(self):
        """Cancellation converges every descendant and its Parent phase before return."""
        app = AutoAgentApp()
        try:
            leaf = Workflow('leaf', nodes=[Node('wait', Wait(Value, Value))])
            child = Workflow('child', nodes=[Node('leaf', leaf, execution_mode='spawn')])
            result = app.invoke(Workflow('root', nodes=[Node('child', child, execution_mode='spawn')]), {'value': 1})
            result = app.cancel(result.ref)
            self.assertEqual(result.status, 'cancelled')
            for sid in app._repository.session_ids():
                inv = app._repository.state(sid).invocation
                self.assertTrue(inv.terminal)
                self.assertTrue(all(u.phase == 'terminal' for p in inv.child_plans.values() for u in p.units))
            app.unload_session(result.ref)
            self.assertFalse(app._repository.session_ids())
        finally: app.close()

    def test_graph_checkpoint_rejects_missing_or_forged_child(self):
        """Partial ownership and identity changes cannot form a valid graph bundle."""
        app = AutoAgentApp()
        try:
            r = app.invoke(Workflow('root', nodes=[Node('child', Workflow('child', nodes=[Node('n', identity)]))]), {'value': 1})
            cp = app.unload_session(r.ref, capture_checkpoint=True)
            parent = next(s for s in cp.sessions if s.session_id == r.session_id)
            child = next(s for s in cp.sessions if s.session_id != r.session_id)
            with self.assertRaises(ValueError): RuntimeGraphCheckpoint(r.session_id, (parent,))
            forged = SessionCheckpoint.from_state(replace(child.state, invocation=replace(child.state.invocation, workflow_revision_id='wrong')))
            with self.assertRaises(ValueError): RuntimeGraphCheckpoint(r.session_id, (parent, forged))
            with self.assertRaises(TypeError): app.load_checkpoint(parent)
            self.assertFalse(app._repository.session_ids())
        finally: app.close()

    def test_joining_recovery_does_not_repeat_parent_body(self):
        """A checkpointed joining Parent recovers children without rerunning its body."""
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        root = Workflow('root', nodes=[Node('child', Workflow('child', nodes=[Node('w', Wait(Value, Value))]), execution_mode='spawn')])
        try:
            r = app.invoke(root, {'value': 1})
            checkpoint = app.close(capture_checkpoint=True)
        finally: app.close()
        restored = AutoAgentApp(runtime_event_sink=sink)
        try:
            restored.register_workflow(root)
            ref = restored.load_checkpoint(checkpoint).invocations[0]
            before = sum(e.session_id == ref.session_id and e.payload.kind == 'node_occurrence.started' for e in sink.events)
            self.assertEqual(before, 1)
            r = restored.recover(ref)
            r = restored.resume(ref, r.waits[0].id, {'value': 2})
            self.assertEqual(r.status, 'completed')
            self.assertEqual(sum(e.session_id == ref.session_id and e.payload.kind == 'node_occurrence.started' for e in sink.events), before)
        finally: restored.close()

    def test_planned_checkpoint_restores_unopened_children(self):
        """A durable plan alone is sufficient to reconstruct unopened Child Sessions."""
        sink = Collector()
        root = Workflow('root', nodes=[Node('child', Workflow('child', nodes=[Node('w', identity)]), recovery_mode=Recovery('replay_safe', max_attempts=2))])
        app = AutoAgentApp(runtime_event_sink=sink)
        try: app.invoke(root, {'value': 1}, session_id='root')
        finally: app.close()
        states = {}
        for e in sink.events:
            states[e.session_id] = StateReducer().apply(states.get(e.session_id, RuntimeState()), e)
            if e.payload.kind == 'child_invocation.planned': break
        checkpoint = graph_bundle(tuple(SessionCheckpoint.from_state(s) for s in states.values()))
        restored = AutoAgentApp()
        try:
            restored.register_workflow(root)
            ref = restored.load_checkpoint(checkpoint).invocations[0]
            self.assertEqual(restored.recover(ref).status, 'completed')
        finally: restored.close()

    def test_every_admission_prefix_preserves_child_sequence(self):
        """Graph recovery keeps ACKed SessionOpened even before Child InvocationStarted."""
        source_sink = Collector()
        root = Workflow('prefix-root', nodes=[Node('child', Workflow('prefix-child', nodes=[Node('work', identity)]), recovery_mode=Recovery('replay_safe'))])
        app = AutoAgentApp(runtime_event_sink=source_sink)
        try: app.invoke(root, {'value': 3}, session_id='root')
        finally: app.close()
        states = {}
        prefixes = []
        for event in source_sink.events:
            states[event.session_id] = StateReducer().apply(states.get(event.session_id, RuntimeState()), event)
            if event.payload.kind in {'child_invocation.planned', 'session.opened', 'invocation.started', 'child_invocation.phase_changed'} and states['root'].invocation is not None:
                prefixes.append(dict(states))
        for prefix in prefixes:
            with self.subTest(sequences={sid: s.sequence for sid, s in prefix.items()}):
                saved = graph_bundle(tuple(SessionCheckpoint.from_state(s) for s in prefix.values()))
                sink = Collector()
                restored = AutoAgentApp(runtime_event_sink=sink)
                try:
                    restored.register_workflow(root)
                    ref = restored.load_checkpoint(saved).invocations[0]
                    self.assertEqual(restored.recover(ref).status, 'completed')
                    for sid, state in prefix.items():
                        events = [e for e in sink.events if e.session_id == sid]
                        if events:
                            self.assertEqual(events[0].sequence, state.sequence + 1)
                finally: restored.close()

    def test_cancel_planned_child_without_running_operator(self):
        """Cancellation abandons an unopened plan without fabricating a terminal Child."""
        from autoagent.core import RuntimeRepository, InvocationCancelled
        sink = Collector()
        root = Workflow('cancel-planned-root', nodes=[Node('child', Workflow('cancel-planned-child', nodes=[Node('work', identity)]))])
        app = AutoAgentApp(runtime_event_sink=sink)
        try: app.invoke(root, {'value': 1}, session_id='root')
        finally: app.close()
        state = RuntimeState()
        for e in sink.events:
            if e.session_id == 'root': state = StateReducer().apply(state, e)
            if e.payload.kind == 'child_invocation.planned': break
        repository = RuntimeRepository()
        repository.install_states({'root': state})
        asyncio.run(repository.commit(session_id='root', invocation_id=state.invocation.id, payload=InvocationCancelled('stop')))
        saved = RuntimeGraphCheckpoint('root', (repository.capture_checkpoint('root'),))
        restored = AutoAgentApp()
        try:
            ref = restored.load_checkpoint(saved).invocations[0]
            self.assertEqual(restored.recover(ref).status, 'cancelled')
            inv = restored._repository.state('root').invocation
            self.assertEqual(next(iter(inv.child_plans.values())).units[0].phase, 'abandoned')
            self.assertEqual(restored._repository.session_ids(), ('root',))
            cp = restored.unload_session(ref, capture_checkpoint=True)
            self.assertEqual(RuntimeGraphCheckpoint.from_record(cp.to_record()), cp)
        finally: restored.close()

    def test_graph_digest_and_multiple_roots(self):
        """Independent graph bundles preserve root identity and reject altered membership."""
        app = AutoAgentApp()
        workflow = Workflow('root', nodes=[Node('work', identity)])
        try:
            refs = tuple(app.invoke(workflow, {'value': i}).ref for i in range(2))
            saved = app.close(capture_checkpoint=True)
        finally: app.close()
        self.assertEqual(len(saved.graphs), 2)
        record = saved.to_record()
        rebuilt = AppCheckpoint.from_record(json.loads(json.dumps(record)))
        restored = AutoAgentApp()
        try:
            self.assertEqual(set(r.session_id for r in restored.load_checkpoint(rebuilt).invocations), set(r.session_id for r in refs))
            corrupted = saved.graphs[0].to_record()
            corrupted['root_session_id'] = 'another-root'
            with self.assertRaises(ValueError): RuntimeGraphCheckpoint.from_record(corrupted)
            with self.assertRaises(ValueError): AppCheckpoint((saved.graphs[0], saved.graphs[0]))
        finally: restored.close()

    def test_load_cannot_adopt_an_existing_root_as_child(self):
        """Loading a second graph cannot silently change a resident Root's ownership."""
        source = AutoAgentApp()
        try:
            r = source.invoke(Workflow('root', nodes=[Node('child', Workflow('child', nodes=[Node('n', identity)]))]), {'value': 1})
            graph = source.unload_session(r.ref, capture_checkpoint=True)
        finally: source.close()
        child = next(s for s in graph.sessions if s.session_id != graph.root_session_id)
        target = AutoAgentApp()
        try:
            standalone = RuntimeGraphCheckpoint(child.session_id, (child,))
            target.load_checkpoint(standalone)
            before = target._repository.session_ids()
            with self.assertRaisesRegex(RuntimeTransitionError, 'CHECKPOINT_SESSION_CONFLICT'):
                target.load_checkpoint(graph)
            self.assertEqual(target._repository.session_ids(), before)
            self.assertFalse(target._child_owners)
        finally: target.close()

    def test_failed_graph_cannot_omit_a_terminal_admitted_child(self):
        """Cancellation does not authorize dropping an admitted Child from a bundle."""
        app = AutoAgentApp()
        try:
            r = app.invoke(Workflow('root', nodes=[Node('child', Workflow('child', nodes=[Node('w', Wait(Value, Value))]))]), {'value': 1})
            app.cancel(r.ref)
            cp = app.unload_session(r.ref, capture_checkpoint=True)
            root = next(s for s in cp.sessions if s.session_id == cp.root_session_id)
            with self.assertRaises(ValueError): RuntimeGraphCheckpoint(cp.root_session_id, (root,))
        finally: app.close()

    def test_close_keeps_acknowledged_uninitialized_child_session(self):
        """Close retains a partial Child Session so recovery cannot reuse sequence one."""
        from autoagent.core import RuntimeRepository
        sink = Collector()
        workflow = Workflow('root', nodes=[Node('child', Workflow('child', nodes=[Node('n', identity)]), recovery_mode=Recovery('replay_safe'))])
        source = AutoAgentApp(runtime_event_sink=sink)
        try: source.invoke(workflow, {'value': 1}, session_id='root')
        finally: source.close()
        states = {}
        for event in sink.events:
            states[event.session_id] = StateReducer().apply(states.get(event.session_id, RuntimeState()), event)
            if event.session_id != 'root' and event.payload.kind == 'session.opened': break
        repository = RuntimeRepository()
        repository.install_states(states)
        app = AutoAgentApp(runtime_repository=repository)
        saved = app.close(capture_checkpoint=True)
        self.assertEqual(len(saved.graphs[0].sessions), 2)
        self.assertTrue(any(s.state.invocation is None for s in saved.graphs[0].sessions))
        resumed = AutoAgentApp()
        try:
            resumed.register_workflow(workflow)
            ref = resumed.load_checkpoint(saved).invocations[0]
            self.assertEqual(resumed.recover(ref).status, 'completed')
        finally: resumed.close()

    def test_child_handle_strict_round_trip(self):
        """Durable Handle contracts restore identity without allowing extra control fields."""
        from autoagent import ValueContract
        handle = ChildHandle(child_session_id='session', child_invocation_id='invocation', workflow_revision_id='revision')
        contract = ValueContract.create(ChildHandle, location='handle test')
        self.assertEqual(contract.restore(contract.to_record(handle)), handle)
        with self.assertRaises(Exception):
            contract.restore({**contract.to_record(handle), 'workflow_id': 'unexpected'})

    def test_cancel_after_child_sink_failure_clears_resolved_drive_error(self):
        """A successful cancellation resolves old infrastructure errors after exact ACK retry."""
        from autoagent import RuntimeInfrastructureError
        class FailOnce:
            def __init__(self): self.failed = False
            async def append(self, event):
                if event.session_id != 'root' and event.payload.kind == 'invocation.completed' and not self.failed:
                    self.failed = True
                    raise OSError('ambiguous child completion')
        app = AutoAgentApp(runtime_event_sink=FailOnce())
        try:
            workflow = Workflow('root', nodes=[Node('child', Workflow('child', nodes=[Node('n', identity)]), execution_mode='spawn')])
            with self.assertRaises(RuntimeInfrastructureError): app.invoke(workflow, {'value': 1}, session_id='root')
            ref = app.resident_invocations()[0]
            self.assertEqual(app.cancel(ref).status, 'cancelled')
            self.assertEqual(app.join(ref).status, 'cancelled')
        finally: app.close()

"""Clean graph pause, durable continuation, and concurrent control boundaries."""
import asyncio
from dataclasses import replace
import json
import threading
import time
import unittest

from autoagent import (
    Await, AwaitSignal, Edge, InputMappingContext, Map, Node,
    OutputBindingContext, Recovery, Select, SignalCase,
    SignalEndpoint, Spawn, Timer, TimerCase, TimerRequest, Wait, Workflow,
)
from autoagent.core.app import AutoAgentApp
from autoagent.core.errors import RuntimeInfrastructureError, RuntimeTransitionError
from autoagent.core import ContextPatch, RuntimeEvent, RuntimeGraphCheckpoint
from autoagent.core.runtime import GraphPaused, GraphPauseRequested, GraphResumed
from tests.test_system_commands import Collector, Value, identity


def wait_until(predicate, timeout=2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.001)
    raise AssertionError('Condition did not become true')


def roundtrip(checkpoint):
    return RuntimeGraphCheckpoint.from_record(json.loads(json.dumps(checkpoint.to_record())))


class GraphPauseTests(unittest.TestCase):
    def setUp(self):
        self.releases = []
        self.apps = []

    def tearDown(self):
        for release in self.releases:
            release.set()
        for app in reversed(self.apps):
            app.close()

    def app(self, **options):
        app = AutoAgentApp(**options)
        self.apps.append(app)
        return app

    def slow(self, *, fail=False):
        started, release = threading.Event(), threading.Event()
        self.releases.append(release)
        calls = []
        async def run(value: Value) -> Value:
            calls.append(value.value)
            started.set()
            while not release.is_set():
                await asyncio.sleep(.001)
            if fail:
                raise ValueError('in-flight failure')
            return value
        return run, started, release, calls

    def begin_pause(self, app, ref):
        with self.assertRaisesRegex(RuntimeTransitionError, 'Pause remains requested'):
            app.pause(ref, timeout=.01)
        self.assertEqual(app.status(ref).control_state, 'pausing')

    def test_running_node_finishes_and_ready_successor_is_not_dispatched(self):
        """Pause drains the active Node, retains ready work, and never replays it."""
        slow, started, release, calls = self.slow()
        after = []
        def finish(value: Value) -> Value:
            after.append(value.value)
            return value
        workflow = Workflow('boundary', nodes=[Node('slow', slow, recovery_mode=Recovery(mode='never')),
            Node('finish', finish, recovery_mode=Recovery(mode='never'))], edges=[Edge('slow', 'finish')])
        app = self.app()
        submission = app.submit_invoke(workflow, {'value': 7})
        self.assertTrue(started.wait(2))
        self.begin_pause(app, submission.ref)
        self.assertFalse(app.suspension_info(submission.ref).can_unload)
        release.set()
        result = app.pause(submission.ref, timeout=2)
        self.assertEqual((result.status, result.control_state), ('running', 'paused'))
        self.assertEqual(calls, [7])
        self.assertEqual(after, [])
        self.assertTrue(app.suspension_info(submission.ref).can_unload)
        checkpoint = roundtrip(app.unload_session(submission.ref, capture_checkpoint=True))
        app.load_checkpoint(checkpoint)
        self.assertEqual(app.status(submission.ref).control_state, 'paused')
        self.assertEqual(app.join(submission.ref).control_state, 'paused')
        result = app.recover(submission.ref)
        self.assertEqual((result.status, result.control_state), ('completed', 'active'))
        self.assertEqual(calls, [7])
        self.assertEqual(after, [7])
        self.assertTrue(all(o.recovery_attempts == 0 for o in app._repository.state(submission.ref.session_id).invocation.scheduler.occurrences.values()))

    def test_terminal_success_wins_over_pause(self):
        """A finishing exit returns terminal success rather than artificial pause."""
        slow, started, release, calls = self.slow()
        app = self.app()
        ref = app.submit_invoke(Workflow('exit', nodes=[Node('work', slow)]), {'value': 1}).ref
        self.assertTrue(started.wait(2))
        self.begin_pause(app, ref)
        release.set()
        result = app.pause(ref, timeout=2)
        self.assertEqual((result.status, result.control_state), ('completed', 'active'))
        self.assertEqual(calls, [1])

    def test_terminal_failure_wins_over_pause(self):
        """An in-flight failure retains its error while pause drains the graph."""
        slow, started, release, _ = self.slow(fail=True)
        app = self.app()
        ref = app.submit_invoke(Workflow('failure', nodes=[Node('work', slow)]), {'value': 1}).ref
        self.assertTrue(started.wait(2))
        self.begin_pause(app, ref)
        release.set()
        result = app.pause(ref, timeout=2)
        self.assertEqual((result.status, result.control_state), ('failed', 'active'))
        self.assertIn('in-flight failure', result.error.message)

    def test_parallel_nodes_both_drain_before_pause(self):
        """Pause waits for every active sibling and preserves their ready joins."""
        left, ls, lr, lc = self.slow()
        right, rs, rr, rc = self.slow()
        app = self.app()
        workflow = Workflow('parallel', nodes=[Node('entry', identity), Node('left', left),
            Node('right', right), Node('left_exit', identity), Node('right_exit', identity)],
            edges=[Edge('entry', 'left'), Edge('entry', 'right'), Edge('left', 'left_exit'), Edge('right', 'right_exit')])
        ref = app.submit_invoke(workflow, {'value': 3}).ref
        self.assertTrue(ls.wait(2) and rs.wait(2))
        self.begin_pause(app, ref)
        lr.set()
        wait_until(lambda: app._repository.state(ref.session_id).invocation.scheduler.occurrences[next(o.id for o in app._repository.state(ref.session_id).invocation.scheduler.occurrences.values() if o.node_id == 'left')].status == 'completed')
        self.assertEqual(app.status(ref).control_state, 'pausing')
        rr.set()
        self.assertEqual(app.pause(ref, timeout=2).control_state, 'paused')
        self.assertEqual(app.recover(ref).status, 'completed')
        self.assertEqual((lc, rc), ([3], [3]))

    def test_map_finishes_all_calls_inside_the_started_node(self):
        """Node-boundary pause drains queued Map calls without splitting the Node."""
        slow, started, release, calls = self.slow()
        def inputs(ctx: InputMappingContext) -> list[Value]:
            return [Value(value=i) for i in range(5)]
        def approval_input(ctx: InputMappingContext) -> Value:
            return Value(value=5)
        app = self.app(max_operator_concurrency=2)
        workflow = Workflow('map', nodes=[Node('map', slow, map=Map(max_parallelism=2), input_mapping=inputs),
            Node('after', Wait(Value, Value), input_mapping=approval_input)], edges=[Edge('map', 'after')])
        ref = app.submit_invoke(workflow, None).ref
        self.assertTrue(started.wait(2))
        self.begin_pause(app, ref)
        release.set()
        result = app.pause(ref, timeout=2)
        self.assertEqual(result.control_state, 'paused')
        self.assertCountEqual(calls, range(5))
        self.assertFalse(result.waits)
        self.assertEqual(app.recover(ref).status, 'waiting')
        self.assertEqual(len(calls), 5)

    def test_repeated_pause_is_idempotent(self):
        """Repeated pause joins one request and emits one pair of control Events."""
        sink = Collector()
        app = self.app(runtime_event_sink=sink)
        ref = app.invoke(Workflow('wait', nodes=[Node('wait', Wait(Value, Value))]), {'value': 1}).ref
        for _ in range(3):
            self.assertEqual(app.pause(ref, timeout=2).control_state, 'paused')
            self.assertFalse(app._task_runtime._wake_events)
        names = [e.event_name for e in sink.events]
        self.assertEqual(names.count('graph.pause_requested'), 1)
        self.assertEqual(names.count('graph.paused'), 1)

    def test_recover_rejects_a_live_pause_request(self):
        """Recovery cannot open admission before an in-flight pause has settled."""
        slow, started, release, _ = self.slow()
        app = self.app()
        ref = app.submit_invoke(Workflow('busy', nodes=[Node('work', slow), Node('after', identity)], edges=[Edge('work', 'after')]), {'value': 1}).ref
        self.assertTrue(started.wait(2))
        self.begin_pause(app, ref)
        with self.assertRaises(RuntimeTransitionError) as caught:
            app.recover(ref)
        self.assertEqual(caught.exception.code, 'GRAPH_PAUSING')
        release.set()
        self.assertEqual(app.pause(ref, timeout=2).control_state, 'paused')

    def test_cancel_during_pause_settles_the_graph(self):
        """Cancel remains effective during pausing and releases the pause worker."""
        slow, started, _, _ = self.slow()
        app = self.app()
        ref = app.submit_invoke(Workflow('busy', nodes=[Node('work', slow)]), {'value': 1}).ref
        self.assertTrue(started.wait(2))
        self.begin_pause(app, ref)
        self.assertEqual(app.cancel(ref).status, 'cancelled')
        self.assertEqual(app.pause(ref, timeout=2).status, 'cancelled')
        self.assertTrue(app.suspension_info(ref).can_unload)

    def test_cancel_paused_graph_does_not_execute_ready_work(self):
        """Cancel of a paused graph never restarts its pending business Nodes."""
        app = self.app()
        ref = app.invoke(Workflow('wait', nodes=[Node('wait', Wait(Value, Value)), Node('after', identity)], edges=[Edge('wait', 'after')]), {'value': 1}).ref
        app.pause(ref)
        self.assertEqual(app.cancel(ref).status, 'cancelled')
        self.assertTrue(app.suspension_info(ref).can_unload)

    def test_external_approval_records_response_but_defers_hooks(self):
        """Approval during pause survives unload and executes never-replay hooks once."""
        bound = []
        def bind(ctx: OutputBindingContext) -> ContextPatch:
            bound.append(ctx.output['value'])
            return ContextPatch()
        workflow = Workflow('approval', nodes=[Node('wait', Wait(Value, Value), output_binding=bind,
            recovery_mode=Recovery(mode='never')), Node('after', identity)], edges=[Edge('wait', 'after')])
        app = self.app()
        result = app.invoke(workflow, {'value': 1})
        app.pause(result.ref)
        accepted = app.resume(result.ref, result.waits[0].id, {'value': 9})
        self.assertEqual(accepted.control_state, 'paused')
        self.assertEqual(bound, [])
        cp = roundtrip(app.unload_session(result.ref, capture_checkpoint=True))
        app.load_checkpoint(cp)
        result = app.recover(result.ref)
        self.assertEqual(result.status, 'completed')
        self.assertEqual(result.output, {'value': 9})
        self.assertEqual(bound, [9])

    def test_recover_preserves_unapproved_external_wait(self):
        """Recover releases pause without treating an unanswered Wait as approved."""
        app = self.app()
        result = app.invoke(Workflow('wait', nodes=[Node('wait', Wait(Value, Value))]), {'value': 1})
        app.pause(result.ref)
        recovered = app.recover(result.ref)
        self.assertEqual((recovered.status, recovered.control_state), ('waiting', 'active'))
        self.assertEqual(recovered.waits[0].id, result.waits[0].id)

    def test_signal_is_buffered_while_paused_and_consumed_on_recover(self):
        """Paused Signal waits retain accepted messages until graph recovery."""
        sink = Collector()
        app = self.app(runtime_event_sink=sink)
        workflow = Workflow('signal', nodes=[Node('message', AwaitSignal('inbox'))], signal_endpoints=[SignalEndpoint('inbox', Value)])
        result = app.invoke(workflow, None)
        app.pause(result.ref)
        app.signal(result.ref, 'inbox', {'value': 4}, source_id='client', sequence=1)
        self.assertEqual(app.status(result.ref).control_state, 'paused')
        self.assertFalse(any(e.event_name == 'command.awakened' for e in sink.events))
        cp = roundtrip(app.unload_session(result.ref, capture_checkpoint=True))
        app.load_checkpoint(cp)
        final = app.recover(result.ref)
        self.assertEqual(final.status, 'completed')
        self.assertEqual(final.output.messages[0].payload, {'value': 4})
        self.assertEqual(sum(e.event_name == 'command.awakened' for e in sink.events), 1)

    def test_timer_expiry_does_not_wake_paused_graph(self):
        """Timer expiry while paused preserves its deadline and defers output hooks."""
        now = [100]
        bound = []
        def bind(ctx: OutputBindingContext) -> ContextPatch:
            bound.append(ctx.output['deadline_at_us'])
            return ContextPatch()
        app = self.app(clock_us=lambda: now[0])
        workflow = Workflow('timer', nodes=[Node('timer', Timer(), output_binding=bind, recovery_mode=Recovery(mode='never'))])
        result = app.invoke(workflow, TimerRequest(delay_us=1_000_000).model_dump())
        app.pause(result.ref)
        self.assertFalse(app._wait_timers)
        now[0] = 2_000_000
        self.assertEqual(app.join(result.ref).control_state, 'paused')
        self.assertEqual(bound, [])
        cp = roundtrip(app.unload_session(result.ref, capture_checkpoint=True))
        app.load_checkpoint(cp)
        final = app.recover(result.ref)
        self.assertEqual(final.status, 'completed')
        self.assertEqual(final.output.deadline_at_us, 1_000_100)
        self.assertEqual(bound, [1_000_100])

    def test_select_defers_choice_and_keeps_declaration_order(self):
        """Signal and Timer ready during pause are resolved only after recovery."""
        now = [100]
        app = self.app(clock_us=lambda: now[0])
        workflow = Workflow('select', nodes=[Node('select', Select({'message': SignalCase('inbox'), 'timeout': TimerCase(delay_us=1_000_000)}))], signal_endpoints=[SignalEndpoint('inbox', Value)])
        result = app.invoke(workflow, None)
        app.pause(result.ref)
        now[0] = 2_000_000
        app.signal(result.ref, 'inbox', {'value': 5}, source_id='client', sequence=1)
        self.assertTrue(app.status(result.ref).waits)
        final = app.recover(result.ref)
        self.assertEqual(final.output.case, 'message')
        self.assertFalse(app._wait_timers)

    def test_nested_children_resume_approval_through_root(self):
        """Three-level Await ownership survives pause, approval, and JSON reload."""
        leaf = Workflow('leaf', nodes=[Node('approval', Wait(Value, Value)), Node('finish', identity)], edges=[Edge('approval', 'finish')])
        middle = Workflow('middle', nodes=[Node('leaf', Await(leaf, 'approval'), recovery_mode=Recovery(mode='never'))])
        root = Workflow('root', nodes=[Node('middle', Await(middle, 'leaf'), recovery_mode=Recovery(mode='never'))])
        app = self.app()
        result = app.invoke(root, {'value': 1})
        before = set(app._repository.session_ids())
        self.assertEqual(len(before), 3)
        paused = app.pause(result.ref)
        external = next(w for w in paused.waits if w.kind == 'external')
        app.resume(result.ref, external.id, {'value': 8})
        self.assertEqual(set(app._repository.session_ids()), before)
        cp = roundtrip(app.unload_session(result.ref, capture_checkpoint=True))
        self.assertEqual(len(cp.sessions), 3)
        restored = self.app()
        restored.register_workflow(root)
        restored.load_checkpoint(cp)
        final = restored.recover(result.ref)
        self.assertEqual(final.status, 'completed')
        self.assertEqual(set(restored._repository.session_ids()), before)
        self.assertTrue(restored.suspension_info(result.ref).can_unload)

    def test_parent_with_two_spawn_children_drains_all_running_branches(self):
        """Pause includes detached siblings and grandchild work, not only Root Tasks."""
        slow, started, release, calls = self.slow()
        leaf = Workflow('leaf', nodes=[Node('slow', slow), Node('finish', identity)], edges=[Edge('slow', 'finish')])
        child = Workflow('child', nodes=[Node('leaf', Await(leaf, 'slow'))])
        def inputs(ctx: InputMappingContext) -> list[Value]:
            return [Value(value=1), Value(value=2)]
        root = Workflow('root', nodes=[Node('spawn', Spawn(child, 'leaf'), map=Map(max_parallelism=2), input_mapping=inputs)])
        app = self.app()
        ref = app.submit_invoke(root, None).ref
        self.assertTrue(started.wait(2))
        wait_until(lambda: len(calls) == 2)
        self.begin_pause(app, ref)
        release.set()
        paused = app.pause(ref, timeout=2)
        self.assertEqual((paused.status, paused.control_state), ('settling', 'paused'))
        self.assertEqual(len(app._repository.session_ids()), 5)
        self.assertTrue(app.suspension_info(ref).can_unload)
        cp = roundtrip(app.unload_session(ref, capture_checkpoint=True))
        app.load_checkpoint(cp)
        self.assertEqual(app.recover(ref).status, 'completed')
        self.assertCountEqual(calls, [1, 2])

    def test_pause_control_events_roundtrip_and_replay(self):
        """Control Events reconstruct the same canonical graph pause state."""
        sink = Collector()
        app = self.app(runtime_event_sink=sink)
        result = app.invoke(Workflow('wait', nodes=[Node('wait', Wait(Value, Value))]), {'value': 1})
        app.pause(result.ref)
        from autoagent.core.runtime import StateReducer, RuntimeState
        state = RuntimeState()
        for event in sink.events:
            decoded = RuntimeEvent.from_record(json.loads(json.dumps(event.to_record())))
            state = StateReducer().apply(state, decoded)
        self.assertEqual(state, app._repository.state(result.ref.session_id))
        self.assertEqual(state.invocation.control_state, 'paused')
        self.assertIsInstance(sink.events[-2].payload, GraphPauseRequested)
        self.assertIsInstance(sink.events[-1].payload, GraphPaused)
        app.recover(result.ref)
        self.assertTrue(any(isinstance(e.payload, GraphResumed) for e in sink.events))

    def test_pause_rejects_child_handle_and_invalid_timeouts(self):
        """Only exact Root references and finite non-negative timeouts are allowed."""
        app = self.app()
        result = app.invoke(Workflow('root', nodes=[Node('child', Await(Workflow('child', nodes=[Node('wait', Wait(Value, Value))]), 'wait'))]), {'value': 1})
        child_sid = next(s for s in app._repository.session_ids() if s != result.ref.session_id)
        child_ref = app._ref_for_invocation(child_sid, app._repository.state(child_sid).invocation)
        with self.assertRaises(RuntimeTransitionError):
            app.pause(child_ref)
        for timeout in (-1, True, float('nan'), float('inf'), 'x'):
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                app.pause(result.ref, timeout=timeout)

    def test_clean_recovery_validates_all_workflows_before_enabling(self):
        """A missing child revision leaves the whole loaded graph paused."""
        app = self.app()
        root = Workflow('root', nodes=[Node('child', Await(Workflow('child', nodes=[Node('wait', Wait(Value, Value))]), 'wait'))])
        result = app.invoke(root, {'value': 1})
        app.pause(result.ref)
        cp = roundtrip(app.unload_session(result.ref, capture_checkpoint=True))
        restored = self.app()
        restored.register_workflow(root)
        restored.load_checkpoint(cp)
        child = next(s.state.invocation for s in cp.sessions if s.session_id != result.ref.session_id)
        revision = restored._workflows.pop(child.workflow_revision_id)
        with self.assertRaises(RuntimeTransitionError):
            restored.recover(result.ref)
        self.assertEqual(restored.status(result.ref).control_state, 'paused')
        self.assertFalse(restored._task_runtime.active_sessions())
        restored._workflows[child.workflow_revision_id] = revision
        self.assertIn(restored.recover(result.ref).status, {'waiting', 'settling'})

    def test_repeated_pause_recovery_does_not_spend_crash_attempts(self):
        """Repeated clean continuation keeps never-replay Nodes and attempt counters intact."""
        app = self.app()
        result = app.invoke(Workflow('wait', nodes=[Node('wait', Wait(Value, Value), recovery_mode=Recovery(mode='never'))]), {'value': 1})
        for _ in range(10):
            self.assertEqual(app.pause(result.ref).control_state, 'paused')
            self.assertEqual(app.recover(result.ref).status, 'waiting')
        self.assertEqual(app.resume(result.ref, result.waits[0].id, {'value': 2}).status, 'completed')
        self.assertTrue(all(o.recovery_attempts == 0 for o in app._repository.state(result.ref.session_id).invocation.scheduler.occurrences.values()))

    def test_timer_remaining_time_is_not_restarted_on_recovery(self):
        """A future Timer recovered from pause retains its original absolute deadline."""
        now = [100]
        app = self.app(clock_us=lambda: now[0])
        result = app.invoke(Workflow('timer', nodes=[Node('timer', Timer())]), TimerRequest(delay_us=1_000_000).model_dump())
        app.pause(result.ref)
        now[0] = 500_100
        final = app.recover(result.ref)
        self.assertEqual(final.status, 'waiting')
        self.assertEqual(app.suspension_info(result.ref).next_deadline_at_us, 1_000_100)
        self.assertTrue(app._wait_timers)

    def test_pause_does_not_stop_an_unrelated_root(self):
        """Pausing one Root leaves another Root's Signal delivery executable."""
        app = self.app()
        workflow = Workflow('message', nodes=[Node('message', AwaitSignal('inbox'))], signal_endpoints=[SignalEndpoint('inbox', Value)])
        first, second = app.invoke(workflow, None), app.invoke(workflow, None)
        app.pause(first.ref)
        app.signal(second.ref, 'inbox', {'value': 2}, source_id='client', sequence=1)
        wait_until(lambda: app.status(second.ref).status == 'completed')
        self.assertEqual(app.status(first.ref).control_state, 'paused')

    def test_control_ack_loss_retries_the_same_event(self):
        """Lost pause/resume acknowledgements retain Event identity and a contiguous prefix."""
        for boundary in ('graph.pause_requested', 'graph.paused', 'graph.resumed'):
            with self.subTest(boundary=boundary):
                class Sink(Collector):
                    failed = None
                    retries = 0
                    async def append(self, event):
                        if event is self.failed:
                            self.retries += 1
                        if all(e.id != event.id for e in self.events):
                            self.events.append(event)
                        if event.event_name == boundary and self.failed is None:
                            self.failed = event
                            raise OSError('lost acknowledgement')
                sink = Sink()
                app = self.app(runtime_event_sink=sink)
                result = app.invoke(Workflow('wait', nodes=[Node('wait', Wait(Value, Value))]), {'value': 1})
                if boundary == 'graph.resumed':
                    app.pause(result.ref)
                    with self.assertRaises(RuntimeInfrastructureError):
                        app.recover(result.ref)
                    self.assertEqual(app.recover(result.ref).control_state, 'active')
                else:
                    with self.assertRaises(RuntimeInfrastructureError):
                        app.pause(result.ref)
                    self.assertEqual(app.pause(result.ref).control_state, 'paused')
                self.assertEqual(sink.retries, 1)
                self.assertEqual(sum(e.event_name == boundary for e in sink.events), 1)
                self.assertEqual([e.sequence for e in sink.events], list(range(1, len(sink.events) + 1)))

    def test_interrupted_pause_keeps_crash_recovery_policy(self):
        """Shutdown during pausing cannot grant clean continuation to an interrupted Operator."""
        for mode in ('never', 'replay_safe'):
            with self.subTest(mode=mode):
                slow, started, release, calls = self.slow()
                workflow = Workflow('crash', nodes=[Node('work', slow, recovery_mode=Recovery(mode=mode)), Node('after', identity)], edges=[Edge('work', 'after')])
                app = self.app()
                ref = app.submit_invoke(workflow, {'value': 1}).ref
                self.assertTrue(started.wait(2))
                self.begin_pause(app, ref)
                bundle = app.close(capture_checkpoint=True)
                cp = roundtrip(bundle.graphs[0])
                self.assertEqual(next(s.state.invocation for s in cp.sessions if s.session_id == ref.session_id).control_state, 'pausing')
                release.set()
                restored = self.app()
                restored.register_workflow(workflow)
                restored.load_checkpoint(cp)
                result = restored.recover(ref)
                self.assertEqual(result.status, 'failed' if mode == 'never' else 'completed')
                if mode == 'never':
                    self.assertEqual(result.error.type, 'RecoveryNotAllowed')
                    self.assertEqual(calls, [1])
                else:
                    self.assertEqual(calls, [1, 1])

    def test_checkpoint_cannot_claim_clean_pause_for_interrupted_operator(self):
        """Checkpoint validation rejects paused flags that hide uncertain business work."""
        slow, started, _, _ = self.slow()
        app = self.app()
        ref = app.submit_invoke(Workflow('busy', nodes=[Node('work', slow)]), {'value': 1}).ref
        self.assertTrue(started.wait(2))
        cp = app.close(capture_checkpoint=True).graphs[0]
        from autoagent.core import SessionCheckpoint
        session = cp.sessions[0]
        forged = replace(session.state, invocation=replace(session.state.invocation, control_state='paused'))
        with self.assertRaisesRegex(ValueError, 'interrupted work'):
            RuntimeGraphCheckpoint(ref.session_id, (SessionCheckpoint.from_state(forged),))

    def test_node_start_ack_and_task_ownership_precede_pause_cut(self):
        """A pending NodeStarted ACK cannot leave an unowned running Node at the pause cut."""
        entered, ack = threading.Event(), threading.Event()
        self.releases.append(ack)
        class Sink(Collector):
            async def append(self, event):
                if event.event_name == 'node_occurrence.started' and not entered.is_set():
                    entered.set()
                    while not ack.is_set():
                        await asyncio.sleep(.001)
                await super().append(event)
        slow, started, release, calls = self.slow()
        app = self.app(runtime_event_sink=Sink())
        workflow = Workflow('start_cut', nodes=[Node('work', slow), Node('after', identity)], edges=[Edge('work', 'after')])
        ref = app.submit_invoke(workflow, {'value': 1}).ref
        self.assertTrue(entered.wait(2))
        with self.assertRaises(RuntimeTransitionError):
            app.pause(ref, timeout=.01)
        ack.set()
        self.assertTrue(started.wait(2))
        release.set()
        self.assertEqual(app.pause(ref, timeout=2).control_state, 'paused')
        self.assertEqual(calls, [1])
        self.assertEqual(app.recover(ref).status, 'completed')

    def test_inflight_parent_can_admit_child_without_executing_it(self):
        """A Spawn already in input mapping may record new ownership while child dispatch stays paused."""
        entered, release = threading.Event(), threading.Event()
        self.releases.append(release)
        async def prepare(ctx: InputMappingContext) -> Value:
            entered.set()
            while not release.is_set():
                await asyncio.sleep(.001)
            return Value(value=6)
        calls = []
        def work(value: Value) -> Value:
            calls.append(value.value)
            return value
        child = Workflow('child', nodes=[Node('work', work, recovery_mode=Recovery(mode='never'))])
        root = Workflow('root', nodes=[Node('spawn', Spawn(child, 'work'), input_mapping=prepare)])
        app = self.app()
        ref = app.submit_invoke(root, None).ref
        self.assertTrue(entered.wait(2))
        self.begin_pause(app, ref)
        release.set()
        result = app.pause(ref, timeout=2)
        self.assertEqual((result.status, result.control_state), ('settling', 'paused'))
        self.assertEqual(calls, [])
        self.assertEqual(len(app._repository.session_ids()), 2)
        cp = roundtrip(app.unload_session(ref, capture_checkpoint=True))
        app.load_checkpoint(cp)
        self.assertEqual(app.recover(ref).status, 'completed')
        self.assertEqual(calls, [6])

    def test_cancel_paused_three_level_graph_settles_all_ownership(self):
        """Cancel retires all descendants and wait registrations of a paused nested graph."""
        leaf = Workflow('leaf', nodes=[Node('timer', Timer())])
        child = Workflow('child', nodes=[Node('leaf', Await(leaf, 'timer'))])
        root = Workflow('root', nodes=[Node('child', Await(child, 'leaf'))])
        app = self.app()
        ref = app.invoke(root, TimerRequest(delay_us=60_000_000).model_dump()).ref
        app.pause(ref)
        result = app.cancel(ref)
        self.assertEqual((result.status, result.control_state), ('cancelled', 'active'))
        self.assertTrue(all(app._repository.state(sid).invocation.terminal for sid in app._repository.session_ids()))
        self.assertFalse(app._wait_timers)
        self.assertFalse(app._task_runtime.active_sessions())
        cp = roundtrip(app.unload_session(ref, capture_checkpoint=True))
        self.assertEqual(len(cp.sessions), 3)

    def test_close_preserves_completed_pause_without_starting_work(self):
        """App shutdown captures paused state, and loading it alone does not activate pending work."""
        app = self.app()
        workflow = Workflow('wait', nodes=[Node('wait', Wait(Value, Value)), Node('after', identity)], edges=[Edge('wait', 'after')])
        result = app.invoke(workflow, {'value': 1})
        app.pause(result.ref)
        app.resume(result.ref, result.waits[0].id, {'value': 2})
        cp = roundtrip(app.close(capture_checkpoint=True).graphs[0])
        restored = self.app()
        restored.register_workflow(workflow)
        restored.load_checkpoint(cp)
        self.assertEqual(restored.status(result.ref).control_state, 'paused')
        self.assertFalse(restored._task_runtime.active_sessions())
        self.assertEqual(restored.recover(result.ref).output, {'value': 2})


class AsyncGraphPauseTests(unittest.IsolatedAsyncioTestCase):
    async def test_harness_parent_child_signals_approval_and_failures_across_pause(self):
        """Parallel agents retain identities, reciprocal Signals and tool gating across repeated reloads."""
        from examples.core_agent_harness_demo import (
            ApprovalRequest, ApprovalResponse, Draft, DraftInput, MockServices, RunSpec, parent_workflow,
        )
        for scenario in ('approved', 'rejected', 'llm_failure', 'tool_failure'):
            with self.subTest(scenario=scenario):
                release = threading.Event()
                class Services(MockServices):
                    def __init__(self):
                        super().__init__()
                        self.entered = set()
                    async def llm(self, value: DraftInput) -> Draft:
                        with self.lock:
                            self.entered.add(value.task.task_id)
                        while not release.is_set():
                            await asyncio.sleep(.001)
                        return await super().llm(value)
                services = Services()
                app = AutoAgentApp(runtime_event_sink=services)
                try:
                    workflow = parent_workflow(services)
                    ref = (await app.asubmit_invoke(workflow, RunSpec(run_id='pause-' + scenario, scenario=scenario).model_dump())).ref
                    deadline = time.monotonic() + 10
                    while len(services.entered) != 2 and time.monotonic() < deadline:
                        await asyncio.sleep(.001)
                    self.assertEqual(len(services.entered), 2)
                    pause = asyncio.create_task(app.apause(ref))
                    while (await app.astatus(ref)).control_state != 'pausing' and time.monotonic() < deadline:
                        await asyncio.sleep(.001)
                    release.set()
                    self.assertEqual((await asyncio.wait_for(pause, 3)).control_state, 'paused')
                    identities = {sid: app._repository.state(sid).invocation.id for sid in app._repository.session_ids()}
                    self.assertEqual(len(identities), 3)
                    self.assertFalse(services.snapshot()['effects'])
                    self.assertFalse(any(e['kind'] == 'parent.review' for e in services.snapshot()['timeline']))
                    cp = roundtrip(await app.aunload_session(ref, capture_checkpoint=True))
                    await app.aload_checkpoint(cp)
                    await app.arecover(ref)
                    handled = set()
                    paused_approvals = 0
                    while time.monotonic() < deadline:
                        status = await app.astatus(ref)
                        if status.status in {'completed', 'failed', 'cancelled'}:
                            break
                        for wait in status.waits:
                            if wait.kind != 'external' or wait.id in handled:
                                continue
                            request = ApprovalRequest.model_validate(wait.request)
                            await app.apause(ref)
                            before = services.snapshot()
                            self.assertNotIn(request.operation_key, before['effects'])
                            await app.asignal(ref, 'instructions', {'text': 'Audit approved calls'}, source_id='human', sequence=paused_approvals + 1)
                            approved = not (scenario == 'rejected' and request.task_id == 'publish')
                            if approved:
                                with services.lock:
                                    services.approved.add(request.operation_key)
                            await app.aresume(ref, wait.id, ApprovalResponse(operation_key=request.operation_key, approved=approved, reason='reviewed').model_dump())
                            self.assertEqual(services.snapshot()['effects'], before['effects'])
                            cp = roundtrip(await app.aunload_session(ref, capture_checkpoint=True))
                            await app.aload_checkpoint(cp)
                            await app.arecover(ref)
                            handled.add(wait.id)
                            paused_approvals += 1
                        await asyncio.sleep(.002)
                    else:
                        self.fail('Paused agent graph did not settle')
                    self.assertEqual(status.status, 'completed', status)
                    self.assertGreater(paused_approvals, 0)
                    self.assertEqual({sid: app._repository.state(sid).invocation.id for sid in app._repository.session_ids()}, identities)
                    snapshot = services.snapshot()
                    self.assertEqual(len(snapshot['effects']), 2 if scenario == 'approved' else 1)
                    for task_id in ('research', 'publish'):
                        calls = [e for e in snapshot['timeline'] if e['kind'] == 'child.llm.started' and e['task_id'] == task_id]
                        self.assertEqual([e['attempt'] for e in calls], [1] if scenario == 'llm_failure' and task_id == 'research' else [1, 2])
                    self.assertTrue((await app.asuspension_info(ref)).can_unload)
                finally:
                    release.set()
                    await app.aclose()

    async def test_cancelled_pause_caller_does_not_cancel_business_work(self):
        """Disconnecting a pause caller leaves the shared pause request running."""
        started, release = threading.Event(), threading.Event()
        async def slow(value: Value) -> Value:
            started.set()
            while not release.is_set():
                await asyncio.sleep(.001)
            return value
        app = AutoAgentApp()
        try:
            ref = (await app.asubmit_invoke(Workflow('busy', nodes=[Node('work', slow), Node('after', identity)], edges=[Edge('work', 'after')]), {'value': 1})).ref
            deadline = time.monotonic() + 2
            while not started.is_set() and time.monotonic() < deadline:
                await asyncio.sleep(.001)
            self.assertTrue(started.is_set())
            caller = asyncio.create_task(app.apause(ref))
            while (await app.astatus(ref)).control_state != 'pausing':
                await asyncio.sleep(.001)
            caller.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await caller
            self.assertEqual((await app.astatus(ref)).status, 'running')
            release.set()
            results = await asyncio.gather(app.apause(ref), app.apause(ref))
            self.assertTrue(all(r.control_state == 'paused' for r in results))
            self.assertEqual((await app.arecover(ref)).status, 'completed')
        finally:
            release.set()
            await app.aclose()

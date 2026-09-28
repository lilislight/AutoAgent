"""Durable message, time, Child and composite wait lifecycle boundaries."""
import asyncio
import json
import time
import unittest
from dataclasses import replace

from autoagent import (
    AutoAgentApp, Await, AwaitSignal, ChildCase, Edge, InputMappingContext, Node,
    ReceiveSignal, RuntimeHandle, RuntimeObservation, Select, SelectResult, SendSignal,
    SelfHandle, SignalCase, SignalEndpoint, Spawn, Timer, TimerCase, TimerRequest,
    Wait, Workflow, RuntimeTransitionError,
)
from autoagent.core import RuntimeEvent, RuntimeGraphCheckpoint
from tests.test_system_commands import Value, Collector, identity


def terminal(app, ref, timeout=3):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = app.join(ref, timeout=timeout)
        if result.status in {'completed', 'failed', 'cancelled'}:
            return result
        time.sleep(.002)
    raise AssertionError(f'Invocation did not finish: {result}')


def signal_workflow(command=None):
    return Workflow('inbox', nodes=[Node('receive', command or AwaitSignal('message'))],
        signal_endpoints=[SignalEndpoint('message', Value)])


def child_handle(ctx: InputMappingContext) -> RuntimeHandle:
    return RuntimeHandle.model_validate(next(iter(ctx.incoming.values())))


class DurableWaitTests(unittest.TestCase):
    def test_signal_wait_wakes_without_resume(self):
        """A delivered message completes a suspended receiver without an external Resume."""
        app = AutoAgentApp()
        try:
            result = app.invoke(signal_workflow(), None)
            self.assertEqual(result.status, 'waiting')
            self.assertEqual(result.waits[0].kind, 'signal')
            self.assertIsNone(result.waits[0].request)
            self.assertFalse(app._task_runtime.is_live(result.ref.session_id))
            info = app.suspension_info(result.ref)
            self.assertTrue(info.can_unload)
            self.assertEqual(info.waits[0].condition, {'endpoint': 'message', 'limit': 1})
            app.signal(result.ref, 'message', {'value': 3}, source_id='client', sequence=1)
            final = terminal(app, result.ref)
            self.assertEqual(final.status, 'completed', final)
            self.assertEqual(final.output.messages[0].payload, {'value': 3})
        finally:
            app.close()

    def test_buffered_message_and_batch_limit(self):
        """An existing mailbox is consumed immediately up to the declared batch limit."""
        def payload(ctx: InputMappingContext) -> Value:
            return Value(value=9)
        workflow = Workflow('buffer', nodes=[Node('send', SendSignal(SelfHandle(), 'message'), input_mapping=payload),
            Node('get', AwaitSignal('message', limit=2))], edges=[Edge('send', 'get')],
            signal_endpoints=[SignalEndpoint('message', Value)])
        app = AutoAgentApp()
        try:
            result = app.invoke(workflow, None)
            self.assertEqual(result.status, 'completed', result)
            self.assertEqual(len(result.output.messages), 1)
        finally:
            app.close()

    def test_signal_checkpoint_roundtrip(self):
        """An unloaded Signal wait survives JSON roundtrip and recovery before delivery."""
        app = AutoAgentApp()
        try:
            result = app.invoke(signal_workflow(), None)
            cp = app.unload_session(result.ref, capture_checkpoint=True)
            self.assertFalse(app._wait_sessions)
            cp = RuntimeGraphCheckpoint.from_record(json.loads(json.dumps(cp.to_record(), sort_keys=True)))
            app.load_checkpoint(cp)
            recovered = app.recover(result.ref)
            self.assertEqual(recovered.status, 'waiting')
            app.signal(result.ref, 'message', {'value': 5}, source_id='x', sequence=1)
            self.assertEqual(terminal(app, result.ref).output.messages[0].payload, {'value': 5})
        finally:
            app.close()

    def test_timer_unloads_and_recovers_original_deadline(self):
        """Long timers release their Task and resume from the recorded absolute deadline."""
        now = [100]
        app = AutoAgentApp(clock_us=lambda: now[0])
        try:
            workflow = Workflow('timer', nodes=[Node('timer', Timer())])
            result = app.invoke(workflow, TimerRequest(deadline_at_us=1_000_000).model_dump())
            self.assertEqual(result.status, 'waiting')
            self.assertFalse(app._task_runtime.is_live(result.ref.session_id))
            self.assertEqual(app.suspension_info(result.ref).next_deadline_at_us, 1_000_000)
            cp = app.unload_session(result.ref, capture_checkpoint=True)
            self.assertFalse(app._wait_timers)
            now[0] = 1_000_001
            app.load_checkpoint(cp)
            final = app.recover(result.ref)
            self.assertEqual(final.status, 'completed', final)
            self.assertEqual(final.output.deadline_at_us, 1_000_000)
        finally:
            app.close()

    def test_timer_auto_wakes_while_resident(self):
        """Resident timer waits resume automatically without recover or external scheduler calls."""
        app = AutoAgentApp()
        try:
            result = app.invoke(Workflow('timer', nodes=[Node('timer', Timer())]), TimerRequest(delay_us=20_000).model_dump())
            self.assertEqual(terminal(app, result.ref).status, 'completed')
        finally:
            app.close()

    def test_resume_rejects_internal_wait_without_mutation(self):
        """External Resume cannot complete a Signal, Timer or Select wait."""
        for workflow, data in ((signal_workflow(), None), (signal_workflow(Select({'message': SignalCase('message')})), None),
                (Workflow('timer', nodes=[Node('timer', Timer())]), TimerRequest(delay_us=60_000_000).model_dump())):
            app = AutoAgentApp()
            try:
                result = app.invoke(workflow, data)
                before = app._repository.state(result.ref.session_id)
                with self.assertRaises(RuntimeTransitionError):
                    app.resume(result.ref, result.waits[0].id, {'value': 1})
                self.assertIs(app._repository.state(result.ref.session_id), before)
            finally:
                app.close()

    def test_cancel_signal_wait(self):
        """Cancellation retires a suspended receiver and subsequent delivery cannot revive it."""
        app = AutoAgentApp()
        try:
            result = app.invoke(signal_workflow(), None)
            cancelled = app.cancel(result.ref)
            self.assertEqual(cancelled.status, 'cancelled')
            with self.assertRaises(RuntimeTransitionError):
                app.signal(result.ref, 'message', {'value': 1}, source_id='x', sequence=1)
            self.assertFalse(app._wait_sessions)
            self.assertIsNotNone(app.unload_session(result.ref, capture_checkpoint=True))
        finally:
            app.close()

    def test_multiple_receivers_fifo(self):
        """Parallel receivers consume each message once in registration order."""
        workflow = Workflow('parallel', nodes=[Node('start', identity), Node('one', AwaitSignal('message')),
            Node('two', AwaitSignal('message'))], edges=[Edge('start', 'one'), Edge('start', 'two')],
            signal_endpoints=[SignalEndpoint('message', Value)])
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(workflow, {'value': 0})
            registered = [e.payload.wait_id for e in sink.events if e.event_name == 'node_occurrence.waiting']
            app.signal(result.ref, 'message', {'value': 1}, source_id='x', sequence=1)
            app.signal(result.ref, 'message', {'value': 2}, source_id='x', sequence=2)
            self.assertEqual(terminal(app, result.ref).status, 'completed')
            awakened = [e for e in sink.events if e.event_name == 'command.awakened']
            self.assertEqual([e.payload.wait_id for e in awakened], registered)
            self.assertEqual([e.payload.output['messages'][0]['payload']['value'] for e in awakened], [1, 2])
        finally:
            app.close()

    def test_signal_event_prefixes_recover_once(self):
        """Every accepted delivery/awakening prefix recovers without lost or duplicate consumption."""
        from tests.test_child_recovery_integrity import _checkpoint_from_prefix
        from tests.graph_fixtures import load_graph
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        workflow = signal_workflow()
        try:
            result = app.invoke(workflow, None, session_id='root')
            app.signal(result.ref, 'message', {'value': 8}, source_id='x', sequence=1)
            self.assertEqual(terminal(app, result.ref).status, 'completed')
            events = tuple(RuntimeEvent.from_record(e.to_record()) for e in sink.events)
        finally:
            app.close()
        first = next(i for i, e in enumerate(events) if e.event_name == 'signal.accepted')
        for cut in range(first, len(events)):
            with self.subTest(cut=events[cut].event_name):
                app = AutoAgentApp()
                try:
                    app.register_workflow(workflow)
                    ref = load_graph(app, _checkpoint_from_prefix(events[:cut+1])).invocations[0]
                    final = app.recover(ref)
                    self.assertEqual(final.status, 'completed', final)
                    self.assertEqual(len(final.output.messages), 1)
                    self.assertEqual(final.output.messages[0].payload, {'value': 8})
                finally:
                    app.close()


class SelectTests(unittest.TestCase):
    def test_message_wins_and_timer_registration_is_removed(self):
        """A message wins Select and removes the unused Timer without a later completion."""
        app = AutoAgentApp()
        try:
            workflow = signal_workflow(Select({'message': SignalCase('message'), 'timeout': TimerCase(delay_us=60_000_000)}))
            result = app.invoke(workflow, None)
            self.assertEqual(result.waits[0].kind, 'any')
            app.signal(result.ref, 'message', {'value': 6}, source_id='x', sequence=1)
            final = terminal(app, result.ref)
            self.assertEqual(final.output.case, 'message')
            self.assertEqual(final.output.value['messages'][0]['payload'], {'value': 6})
            self.assertFalse(app._wait_timers)
        finally:
            app.close()

    def test_ready_cases_use_declaration_order(self):
        """Already-ready cases are selected in author order, including after sorted JSON reload."""
        app = AutoAgentApp()
        try:
            result = app.invoke(signal_workflow(Select({'z': TimerCase(deadline_at_us=0), 'a': TimerCase(deadline_at_us=0)})), None)
            self.assertEqual(result.status, 'completed', result)
            self.assertEqual(result.output.case, 'z')
        finally:
            app.close()

    def test_unselected_message_is_not_consumed(self):
        """A Timer winner leaves buffered messages available to the subsequent receiver."""
        def payload(ctx: InputMappingContext) -> Value:
            return Value(value=11)
        workflow = Workflow('preserve', nodes=[Node('send', SendSignal(SelfHandle(), 'message'), input_mapping=payload),
            Node('select', Select({'timer': TimerCase(deadline_at_us=0), 'message': SignalCase('message')})),
            Node('receive', ReceiveSignal('message'))], edges=[Edge('send', 'select'), Edge('select', 'receive')],
            signal_endpoints=[SignalEndpoint('message', Value)])
        app = AutoAgentApp()
        try:
            result = app.invoke(workflow, None)
            self.assertEqual(result.status, 'completed', result)
            self.assertEqual(result.output.messages[0].payload, {'value': 11})
        finally:
            app.close()

    def test_child_wait_is_stable_and_not_cancelled(self):
        """A direct Child entering Wait wins Select and remains resumable."""
        child = Workflow('child', nodes=[Node('wait', Wait(Value, Value))])
        workflow = Workflow('parent', nodes=[Node('spawn', Spawn(child, 'wait')),
            Node('select', Select({'child': ChildCase(child_handle), 'timeout': TimerCase(delay_us=60_000_000)}))],
            edges=[Edge('spawn', 'select')])
        app = AutoAgentApp()
        try:
            result = app.invoke(workflow, {'value': 1})
            self.assertEqual(result.status, 'settling', result)
            self.assertEqual(result.output.case, 'child')
            self.assertEqual(result.output.value['status'], 'waiting')
            self.assertEqual(app.resume(result.ref, result.waits[0].id, {'value': 2}).status, 'completed')
        finally:
            app.close()

    def test_child_and_timer_validate_all_cases(self):
        """A ready Timer cannot hide a forbidden self or cross-graph Child condition."""
        def self_target(ctx: InputMappingContext) -> RuntimeHandle:
            return ctx.self_handle
        app = AutoAgentApp()
        try:
            result = app.invoke(signal_workflow(Select({'ready': TimerCase(deadline_at_us=0), 'invalid': ChildCase(self_target)})), None)
            self.assertEqual(result.status, 'failed')
            self.assertIn('AWAIT_NOT_DIRECT_CHILD', result.error.message)
        finally:
            app.close()

    def test_definitions_and_compiler_reject_invalid_configuration(self):
        """Invalid cases and unsupported mapping/Map configuration fail before execution."""
        from autoagent import Map, WorkflowCompiler
        for factory in (lambda: Select({}), lambda: Select({'x': object()}), lambda: AwaitSignal('', 1),
                lambda: SignalCase('x', 0), lambda: TimerCase(), lambda: ChildCase(None)):
            with self.assertRaises((ValueError, TypeError)):
                factory()
        for command in (AwaitSignal('message'), Select({'x': SignalCase('message')})):
            self.assertFalse(WorkflowCompiler().compile(Workflow('bad', nodes=[Node('n', command, map=Map())])).ok)


class WaitRecoveryBoundaryTests(unittest.TestCase):
    def test_child_after_version_waits_for_a_new_boundary(self):
        """An already observed Child Wait does not win again until that Child changes state."""
        def handle(ctx: InputMappingContext) -> RuntimeHandle:
            return RuntimeObservation.model_validate(next(iter(ctx.incoming.values()))).handle
        def version(ctx: InputMappingContext) -> str:
            return RuntimeObservation.model_validate(next(iter(ctx.incoming.values()))).version
        child = Workflow('child', nodes=[Node('wait', Wait(Value, Value))])
        workflow = Workflow('parent', nodes=[Node('first', Await(child, 'wait')),
            Node('next', Select({'child': ChildCase(handle, after=version)}))], edges=[Edge('first', 'next')])
        app = AutoAgentApp()
        try:
            result = app.invoke(workflow, {'value': 1})
            self.assertEqual(result.status, 'waiting', result)
            self.assertEqual({w.kind for w in result.waits}, {'any', 'external'})
            info = app.suspension_info(result.ref)
            self.assertTrue(info.can_unload)
            cp = app.unload_session(result.ref, capture_checkpoint=True)
            app.load_checkpoint(RuntimeGraphCheckpoint.from_record(json.loads(json.dumps(cp.to_record(), sort_keys=True))))
            result = app.recover(result.ref)
            self.assertEqual(result.status, 'waiting')
            external = next(w for w in result.waits if w.kind == 'external')
            final = app.resume(result.ref, external.id, {'value': 12})
            self.assertEqual(final.status, 'completed', final)
            self.assertEqual(final.output.case, 'child')
            self.assertEqual(final.output.value['output'], {'value': 12})
        finally:
            app.close()

    def test_select_resolver_and_deadline_are_not_replayed(self):
        """Recorded Select arguments survive recovery without rerunning a dynamic resolver."""
        from tests.test_child_recovery_integrity import _checkpoint_from_prefix
        from tests.graph_fixtures import load_graph
        resolutions = []
        async def handle(ctx: InputMappingContext) -> RuntimeHandle:
            resolutions.append(1)
            if len(resolutions) > 1:
                raise AssertionError('resolver replayed')
            return child_handle(ctx)
        child = Workflow('child', nodes=[Node('wait', Wait(Value, Value))])
        workflow = Workflow('parent', nodes=[Node('spawn', Spawn(child, 'wait')),
            Node('choose', Select({'z_child': ChildCase(handle), 'a_timer': TimerCase(delay_us=60_000_000)}))],
            edges=[Edge('spawn', 'choose')])
        sink = Collector()
        source = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = source.invoke(workflow, {'value': 1}, session_id='root')
            self.assertEqual(result.status, 'settling')
            events = tuple(RuntimeEvent.from_record(e.to_record()) for e in sink.events)
        finally:
            source.close()
        cut = next(i for i, e in enumerate(events) if e.event_name == 'operator_call.started' and e.payload.operator_id == 'system_command:select')
        app = AutoAgentApp()
        try:
            app.register_workflow(workflow)
            ref = load_graph(app, _checkpoint_from_prefix(events[:cut+1])).invocations[0]
            result = app.recover(ref)
            self.assertEqual(result.status, 'settling', result)
            self.assertEqual(result.output.case, 'z_child')
            self.assertEqual(len(resolutions), 1)
        finally:
            app.close()

    def test_awakening_ack_failure_retries_exact_event(self):
        """A lost awakening acknowledgement cannot consume twice or replace its result."""
        from autoagent import RuntimeInfrastructureError
        class Sink(Collector):
            failed = None
            retried = False
            async def append(self, event):
                if event.event_name == 'command.awakened' and self.failed is None:
                    self.failed = event
                    raise OSError('lost acknowledgement')
                if event is self.failed:
                    self.retried = True
                await super().append(event)
        sink = Sink()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(signal_workflow(), None)
            app.signal(result.ref, 'message', {'value': 3}, source_id='x', sequence=1)
            with self.assertRaises(RuntimeInfrastructureError):
                app.join(result.ref)
            recovered = app.recover(result.ref)
            self.assertEqual(recovered.status, 'completed', recovered)
            self.assertEqual(len(recovered.output.messages), 1)
            self.assertTrue(sink.retried)
            self.assertEqual(sum(e.event_name == 'command.awakened' for e in sink.events), 1)
        finally:
            app.close()

    def test_select_all_event_prefixes_preserve_winner(self):
        """Each prefix after a winning Signal restores exactly that Select result."""
        from tests.test_child_recovery_integrity import _checkpoint_from_prefix
        from tests.graph_fixtures import load_graph
        workflow = signal_workflow(Select({'message': SignalCase('message'), 'timeout': TimerCase(delay_us=60_000_000)}))
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(workflow, None, session_id='root')
            app.signal(result.ref, 'message', {'value': 4}, source_id='x', sequence=1)
            self.assertEqual(terminal(app, result.ref).output.case, 'message')
            events = tuple(RuntimeEvent.from_record(e.to_record()) for e in sink.events)
        finally:
            app.close()
        first = next(i for i, e in enumerate(events) if e.event_name == 'signal.accepted')
        for cut in range(first, len(events)):
            with self.subTest(cut=events[cut].event_name):
                app = AutoAgentApp()
                try:
                    app.register_workflow(workflow)
                    ref = load_graph(app, _checkpoint_from_prefix(events[:cut+1])).invocations[0]
                    result = app.recover(ref)
                    self.assertEqual(result.status, 'completed', result)
                    self.assertEqual(result.output.case, 'message')
                    self.assertEqual(result.output.value['messages'][0]['payload'], {'value': 4})
                finally:
                    app.close()

    def test_cancel_and_delivery_race_never_revives_cancelled_graph(self):
        """Concurrent delivery and cancellation settle without duplicate completion or resurrection."""
        async def exercise():
            for _ in range(10):
                app = AutoAgentApp()
                try:
                    result = await app.ainvoke(signal_workflow(), None)
                    await asyncio.gather(app.asignal(result.ref, 'message', {'value': 1}, source_id='x', sequence=1),
                        app.acancel(result.ref), return_exceptions=True)
                    final = await app.ajoin(result.ref)
                    self.assertIn(final.status, {'cancelled', 'completed'})
                    self.assertFalse(app._wait_sessions)
                    self.assertFalse(app._task_runtime.is_live(result.ref.session_id))
                finally:
                    await app.aclose()
        asyncio.run(exercise())

    def test_reject_tampered_wait_condition(self):
        """Checkpoint validation rejects altered deadlines and Command identities."""
        app = AutoAgentApp()
        try:
            result = app.invoke(Workflow('timer', nodes=[Node('timer', Timer())]), TimerRequest(delay_us=60_000_000).model_dump())
            state = app._repository.state(result.ref.session_id)
            record = state.to_record()
            wait = next(iter(record['invocation']['scheduler']['waits'].values()))
            wait['request']['deadline_at_us'] += 1
            from autoagent.core import RuntimeState
            with self.assertRaisesRegex(ValueError, 'deadline'):
                RuntimeState.from_record(record)
        finally:
            app.close()

    def test_observation_version_ignores_mailbox_only_updates(self):
        """Unconsumed mailbox updates do not repeatedly wake an unchanged external Wait observation."""
        app = AutoAgentApp()
        try:
            workflow = Workflow('external', nodes=[Node('wait', Wait(Value, Value))], signal_endpoints=[SignalEndpoint('message', Value)])
            result = app.invoke(workflow, {'value': 0})
            handle = RuntimeHandle(**result.ref.model_dump())
            before = app._runtime_loop.run(app._status_runtime(result.ref.session_id, handle))
            app.signal(result.ref, 'message', {'value': 1}, source_id='x', sequence=1)
            after = app._runtime_loop.run(app._status_runtime(result.ref.session_id, handle))
            self.assertEqual(before.version, after.version)
            app.resume(result.ref, result.waits[0].id, {'value': 2})
            completed = app._runtime_loop.run(app._status_runtime(result.ref.session_id, handle))
            self.assertNotEqual(after.version, completed.version)
        finally:
            app.close()


class WaitResourceTests(unittest.TestCase):
    def test_cancel_timer_retires_clock_callback(self):
        """Cancelling the last timed wait removes its resident clock callback immediately."""
        app = AutoAgentApp()
        try:
            result = app.invoke(Workflow('timer', nodes=[Node('timer', Timer())]), TimerRequest(delay_us=60_000_000).model_dump())
            self.assertTrue(app._wait_timers)
            self.assertEqual(app.cancel(result.ref).status, 'cancelled')
            self.assertFalse(app._wait_timers)
            self.assertFalse(app._wait_roots)
        finally:
            app.close()

    def test_awakening_reuses_accepted_payload(self):
        """Mailbox consumption and its completed Call reuse the accepted immutable payload."""
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(signal_workflow(), None)
            app.signal(result.ref, 'message', {'value': 1}, source_id='x', sequence=1)
            self.assertEqual(terminal(app, result.ref).status, 'completed')
            accepted = next(e for e in sink.events if e.event_name == 'signal.accepted')
            awakened = next(e for e in sink.events if e.event_name == 'command.awakened')
            call_output = next(o.value for o in awakened.delta.operations if o.path[-1] == 'output')
            self.assertIs(awakened.payload.output['messages'][0]['payload'], accepted.payload.payload)
            self.assertIs(call_output['messages'][0]['payload'], accepted.payload.payload)
        finally:
            app.close()

    def test_delivery_during_registration_does_not_lose_wakeup(self):
        """Delivery immediately after admission works whether registration has happened or not."""
        async def exercise():
            app = AutoAgentApp()
            try:
                for index in range(15):
                    submission = await app.asubmit_invoke(signal_workflow(), None)
                    await app.asignal(submission.ref, 'message', {'value': index}, source_id='x', sequence=1)
                    result = await app.ajoin(submission.ref)
                    self.assertEqual(result.status, 'completed', result)
                    self.assertEqual(result.output.messages[0].payload['value'], index)
                    await app.aunload_session(submission.ref)
                self.assertFalse(app._wait_sessions)
                self.assertFalse(app._wait_roots)
                self.assertFalse(app._wait_timers)
            finally:
                await app.aclose()
        asyncio.run(exercise())

    def test_running_branch_prevents_graph_unload(self):
        """A paused message receiver does not make another active business branch unloadable."""
        import threading
        entered = threading.Event()
        async def work(value: Value) -> Value:
            entered.set()
            await asyncio.Event().wait()
            return value
        workflow = Workflow('mixed', nodes=[Node('entry', identity), Node('receive', AwaitSignal('message')),
            Node('work', work)], edges=[Edge('entry', 'receive'), Edge('entry', 'work')],
            signal_endpoints=[SignalEndpoint('message', Value)])
        app = AutoAgentApp()
        try:
            submission = app.submit_invoke(workflow, {'value': 1})
            self.assertTrue(entered.wait(1))
            self.assertFalse(app.suspension_info(submission.ref).can_unload)
            with self.assertRaisesRegex(RuntimeTransitionError, 'NOT_UNLOADABLE'):
                app.unload_session(submission.ref)
            self.assertEqual(app.cancel(submission.ref).status, 'cancelled')
        finally:
            app.close()

    def test_example_runs(self):
        """The public example demonstrates unload/recovery and message-versus-timeout selection."""
        import contextlib
        import io
        from examples.durable_waits_demo import run_demo
        with contextlib.redirect_stdout(io.StringIO()):
            received, selected = run_demo()
        self.assertEqual(received.status, 'completed')
        self.assertEqual(selected.output.case, 'message')

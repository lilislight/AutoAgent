"""Cancellation, first-ready observation and durable Timer boundaries."""
import asyncio
import unittest

from autoagent import (
    AutoAgentApp, Await, AwaitAny, AwaitAnyRequest, Cancel, CancelRequest, CancelReceipt,
    Edge, InputMappingContext, Map, Node, RuntimeHandle, RuntimeInfrastructureError,
    RuntimeObservation, Spawn, Timer, TimerRequest, TimerResult, Wait, Workflow,
)
from autoagent.core import RuntimeEvent, ChildResult
from tests.test_system_commands import Value, Collector, identity
from tests.test_status_command import observed_handle


def cancel_request(context: InputMappingContext) -> CancelRequest:
    return CancelRequest(handle=observed_handle(context), reason='no longer needed')


def cancel_workflow():
    child = Workflow('child', nodes=[Node('wait', Wait(Value, Value))])
    return Workflow('root', nodes=[Node('await', Await(child, 'wait')),
        Node('cancel', Cancel(), input_mapping=cancel_request),
        Node('result', Await(), input_mapping=observed_handle)],
        edges=[Edge('await', 'cancel'), Edge('cancel', 'result')])


class CancelTests(unittest.TestCase):
    def test_cancel_waiting_child(self):
        """Cancel acknowledges the request and Await observes the cancelled compact Child."""
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(cancel_workflow(), {'value': 1}, session_id='root')
            self.assertEqual(result.status, 'completed', result)
            self.assertEqual(result.output.status, 'cancelled')
            self.assertEqual(result.output.cancel_reason, 'no longer needed')
            inv = app._repository.state(result.output.handle.session_id).invocation
            self.assertIsInstance(inv, ChildResult)
            self.assertIsNotNone(inv.cancel_origin)
            self.assertEqual(sum(e.event_name == 'invocation.cancellation_requested' for e in sink.events), 1)
            self.assertIsNotNone(app.unload_session(result.ref, capture_checkpoint=True))
        finally:
            app.close()

    def test_self_and_parent_cancellation(self):
        """Cancelling self or owner stops the caller without waiting on itself."""
        for owner in (False, True):
            def request(context: InputMappingContext) -> CancelRequest:
                return CancelRequest(handle=context.owner_handle if owner else context.self_handle, reason='stop')
            child = Workflow('child', nodes=[Node('entry', identity), Node('cancel', Cancel(), input_mapping=request)],
                edges=[Edge('entry', 'cancel')])
            workflow = Workflow('root', nodes=[Node('await', Await(child, 'entry'))]) if owner else child
            app = AutoAgentApp()
            try:
                result = app.invoke(workflow, {'value': 1})
                self.assertEqual(result.status, 'cancelled', result)
                self.assertIsNotNone(app.unload_session(result.ref, capture_checkpoint=True))
            finally:
                app.close()

    def test_terminal_and_cross_graph(self):
        """Terminal targets are unchanged and cross-graph cancellation is rejected."""
        app = AutoAgentApp()
        try:
            child = Workflow('child', nodes=[Node('work', identity)])
            workflow = Workflow('root', nodes=[Node('await', Await(child, 'work')),
                Node('cancel', Cancel(), input_mapping=cancel_request)], edges=[Edge('await', 'cancel')])
            result = app.invoke(workflow, {'value': 1})
            self.assertEqual(result.status, 'completed', result)
            self.assertIsInstance(result.output, CancelReceipt)
            self.assertEqual(result.output.disposition, 'already_terminal')
            target = app._repository.state(result.output.handle.session_id)
            other = app.invoke(Workflow('other', nodes=[Node('cancel', Cancel())]),
                CancelRequest(handle=result.output.handle).model_dump())
            self.assertEqual(other.status, 'failed')
            self.assertIs(app._repository.state(result.output.handle.session_id), target)
        finally:
            app.close()

    def test_cancel_event_prefix_recovery(self):
        """Cancellation survives every effect/result prefix, including compact target retries."""
        from tests.test_child_recovery_integrity import _checkpoint_from_prefix
        from tests.graph_fixtures import load_graph
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(cancel_workflow(), {'value': 1}, session_id='root')
            self.assertEqual(result.status, 'completed', result)
            events = tuple(RuntimeEvent.from_record(e.to_record()) for e in sink.events)
        finally:
            app.close()
        start = next(i for i, e in enumerate(events) if e.event_name == 'operator_call.started'
            and e.payload.operator_id == 'system_command:cancel')
        for index in range(start, len(events)):
            with self.subTest(cut=events[index].event_name):
                restored_sink = Collector()
                restored = AutoAgentApp(runtime_event_sink=restored_sink)
                try:
                    restored.register_workflow(cancel_workflow())
                    ref = load_graph(restored, _checkpoint_from_prefix(events[:index+1])).invocations[0]
                    result = restored.recover(ref)
                    self.assertEqual(result.status, 'completed', result)
                    self.assertEqual(result.output.status, 'cancelled')
                    self.assertEqual(sum(e.event_name == 'invocation.cancellation_requested'
                        for e in (*events[:index+1], *restored_sink.events)), 1)
                finally:
                    restored.close()


class AwaitAnyTests(unittest.TestCase):
    def test_ready_order_and_validation(self):
        """Multiple ready Children select input order and empty or duplicate sets are invalid."""
        def inputs(context: InputMappingContext) -> list[Value]:
            return [Value(value=1), Value(value=2)]
        def targets(context: InputMappingContext) -> AwaitAnyRequest:
            observations = next(iter(context.incoming.values()))
            return AwaitAnyRequest(handles=[RuntimeHandle.model_validate(o['handle']) for o in reversed(observations)])
        child = Workflow('child', nodes=[Node('work', identity)])
        workflow = Workflow('root', nodes=[Node('await', Await(child, 'work'), map=Map(), input_mapping=inputs),
            Node('any', AwaitAny(), input_mapping=targets)], edges=[Edge('await', 'any')])
        app = AutoAgentApp()
        try:
            result = app.invoke(workflow, {'value': 1})
            self.assertEqual(result.status, 'completed', result)
            self.assertEqual(result.output.output, {'value': 2})
            with self.assertRaises(ValueError):
                AwaitAnyRequest(handles=[])
            with self.assertRaises(ValueError):
                AwaitAnyRequest(handles=[result.output.handle, result.output.handle])
            root = RuntimeHandle(**result.ref.model_dump())
            with self.assertRaises(Exception):
                app._validate_wait_target(result.ref.session_id, 'any', {'select': False, 'cases': [
                    {'name': str(i), 'kind': 'child', 'condition': {'handle': h.model_dump(), 'after': None}}
                    for i, h in enumerate([result.output.handle, root])]})
        finally:
            app.close()

    def test_waiting_child_returns_without_waiting_for_slow_sibling(self):
        """AwaitAny observes a waiting Child without joining a running sibling."""
        release = asyncio.Event()
        async def slow(value: Value) -> Value:
            await asyncio.wait_for(release.wait(), 2)
            return value
        def inputs(context: InputMappingContext) -> Value:
            return Value(value=1)
        def targets(context: InputMappingContext) -> AwaitAnyRequest:
            return AwaitAnyRequest(handles=[RuntimeHandle.model_validate(v) for v in context.incoming.values()])
        async def finish(value: RuntimeObservation) -> RuntimeObservation:
            release.set()
            return value
        root = Workflow('root', nodes=[Node('entry', identity),
            Node('slow', Spawn(Workflow('slow', nodes=[Node('work', slow)]), 'work'), input_mapping=inputs),
            Node('wait', Spawn(Workflow('wait', nodes=[Node('wait', Wait(Value, Value))]), 'wait'), input_mapping=inputs),
            Node('any', AwaitAny(), input_mapping=targets), Node('finish', finish)],
            edges=[Edge('entry', 'slow'), Edge('entry', 'wait'), Edge('slow', 'any'), Edge('wait', 'any'), Edge('any', 'finish')])
        app = AutoAgentApp()
        try:
            result = app.invoke(root, {'value': 1})
            self.assertEqual(result.status, 'settling', result)
            self.assertEqual(result.output.status, 'waiting')
            self.assertEqual(result.output.handle.workflow_id, 'wait')
        finally:
            app.close()


class TimerTests(unittest.TestCase):
    def test_timer_input_and_result(self):
        """Timers accept relative or absolute microseconds and reject ambiguous inputs."""
        for bad in ({}, {'delay_us': -1}, {'delay_us': 1, 'deadline_at_us': 2}, {'delay_us': True}):
            with self.assertRaises(ValueError):
                TimerRequest(**bad)
        app = AutoAgentApp()
        try:
            for request in (TimerRequest(delay_us=0), TimerRequest(deadline_at_us=0), TimerRequest(delay_us=1000)):
                result = app.invoke(Workflow('timer', nodes=[Node('timer', Timer())]), request.model_dump())
                from tests.test_durable_waits import terminal
                result = terminal(app, result.ref)
                self.assertEqual(result.status, 'completed', result)
                self.assertIsInstance(result.output, TimerResult)
        finally:
            app.close()

    def test_recovery_keeps_original_deadline(self):
        """A recovered Timer keeps its recorded deadline rather than restarting its delay."""
        from tests.test_child_recovery_integrity import _checkpoint_from_prefix
        from tests.graph_fixtures import load_graph
        workflow = Workflow('timer', nodes=[Node('timer', Timer())])
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(workflow, TimerRequest(delay_us=1000).model_dump(), session_id='root')
            from tests.test_durable_waits import terminal
            result = terminal(app, result.ref)
            self.assertEqual(result.status, 'completed', result)
            events = tuple(RuntimeEvent.from_record(e.to_record()) for e in sink.events)
        finally:
            app.close()
        start = next(i for i, e in enumerate(events) if e.event_name == 'operator_call.started')
        deadline = events[start].payload.input['deadline_at_us']
        self.assertIsNone(events[start].payload.input['delay_us'])
        for cut in range(start, len(events)):
            restored = AutoAgentApp()
            try:
                restored.register_workflow(workflow)
                ref = load_graph(restored, _checkpoint_from_prefix(events[:cut+1])).invocations[0]
                result = restored.recover(ref)
                self.assertEqual(result.status, 'completed', result)
                self.assertEqual(result.output.deadline_at_us, deadline)
            finally:
                restored.close()

class CommandFailureBoundaryTests(unittest.TestCase):
    def test_cancel_ack_failures(self):
        """Lost effect and result ACKs retry the same cancellation without replacing its origin."""
        for boundary in ('invocation.cancellation_requested', 'operator_call.completed'):
            class Sink(Collector):
                failed = None
                call_id = None
                retries = 0
                async def append(self, event):
                    if event.event_name == 'operator_call.started' and event.payload.operator_id == 'system_command:cancel':
                        self.call_id = event.payload.call_id
                    if event is self.failed:
                        self.retries += 1
                    match = event.event_name == boundary and (boundary != 'operator_call.completed' or event.payload.call_id == self.call_id)
                    if match and self.failed is None:
                        self.failed = event
                        raise OSError('lost ACK')
                    await super().append(event)
            sink = Sink()
            app = AutoAgentApp(runtime_event_sink=sink)
            try:
                with self.assertRaises(RuntimeInfrastructureError):
                    app.invoke(cancel_workflow(), {'value': 1}, session_id='root')
                async def settled():
                    tasks = [app._task_runtime.task(s) for s in app._repository.session_ids()]
                    await asyncio.gather(*(t for t in tasks if t is not None), return_exceptions=True)
                    await asyncio.gather(*tuple(app._command_cancel_tasks.values()), return_exceptions=True)
                app._runtime_loop.run(settled())
                inv = app._repository.state('root').invocation
                result = app.recover(app._ref_for_invocation('root', inv))
                self.assertEqual(result.status, 'completed', (boundary, result))
                self.assertEqual(result.output.status, 'cancelled')
                self.assertEqual(sink.retries, 1)
            finally:
                app.close()

    def test_competing_cancels_keep_first_reason(self):
        """Multiple Cancel calls accept one intent and retain the first reason."""
        def requests(context: InputMappingContext) -> list[CancelRequest]:
            handle = observed_handle(context)
            return [CancelRequest(handle=handle, reason='first'), CancelRequest(handle=handle, reason='second')]
        child = Workflow('child', nodes=[Node('wait', Wait(Value, Value))])
        root = Workflow('root', nodes=[Node('await', Await(child, 'wait')),
            Node('cancel', Cancel(), map=Map(), input_mapping=requests)], edges=[Edge('await', 'cancel')])
        app = AutoAgentApp()
        try:
            result = app.invoke(root, {'value': 1})
            self.assertEqual(result.status, 'completed', result)
            self.assertEqual(result.output[0].disposition, 'requested')
            self.assertIn(result.output[1].disposition, {'already_stopping', 'already_terminal'})
            inv = app._repository.state(result.output[0].handle.session_id).invocation
            self.assertEqual(inv.cancel_reason, 'first')
        finally:
            app.close()

    def test_cancel_does_not_wait_for_cleanup(self):
        """Target cleanup may depend on the caller consuming the cancellation receipt."""
        started, release = asyncio.Event(), asyncio.Event()
        async def work(value: Value) -> Value:
            started.set()
            try:
                await asyncio.Future()
            finally:
                await asyncio.wait_for(release.wait(), 2)
        async def request(context: InputMappingContext) -> CancelRequest:
            await asyncio.wait_for(started.wait(), 2)
            return CancelRequest(handle=RuntimeHandle.model_validate(next(iter(context.incoming.values()))))
        async def finish(value: CancelReceipt) -> CancelReceipt:
            release.set()
            return value
        root = Workflow('root', nodes=[Node('spawn', Spawn(Workflow('child', nodes=[Node('work', work)]), 'work')),
            Node('cancel', Cancel(), input_mapping=request), Node('finish', finish)],
            edges=[Edge('spawn', 'cancel'), Edge('cancel', 'finish')])
        app = AutoAgentApp()
        try:
            result = app.invoke(root, {'value': 1})
            self.assertEqual(result.status, 'completed', result)
            self.assertEqual(result.output.disposition, 'requested')
            self.assertFalse(app._command_cancel_tasks)
        finally:
            app.close()

    def test_timer_cancel_and_close_recovery(self):
        """A long Timer cancels promptly and close/load retains its absolute deadline."""
        from unittest.mock import patch
        from tests.graph_fixtures import load_graph
        class Sink(Collector):
            failed = False
            async def append(self, event):
                await super().append(event)
                if event.event_name == 'operator_call.started' and not self.failed:
                    self.failed = True
                    raise OSError('timer start ACK lost')
        sink = Sink()
        workflow = Workflow('timer', nodes=[Node('timer', Timer())])
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            with self.assertRaises(RuntimeInfrastructureError):
                app.invoke(workflow, TimerRequest(delay_us=3_600_000_000).model_dump(), session_id='root')
            checkpoint = app.close(capture_checkpoint=True)
        finally:
            app.close()
        start = next(e for e in sink.events if e.event_name == 'operator_call.started')
        deadline = start.payload.input['deadline_at_us']
        restored = AutoAgentApp()
        try:
            restored.register_workflow(workflow)
            ref = load_graph(restored, checkpoint).invocations[0]
            with patch.object(restored, '_clock_us', return_value=deadline):
                result = restored.recover(ref)
            self.assertEqual(result.status, 'completed', result)
            self.assertEqual(result.output.deadline_at_us, deadline)
        finally:
            restored.close()
        def long_timer(context: InputMappingContext) -> TimerRequest:
            return TimerRequest(delay_us=3_600_000_000)
        def request(context: InputMappingContext) -> CancelRequest:
            return CancelRequest(handle=RuntimeHandle.model_validate(next(iter(context.incoming.values()))))
        root = Workflow('root', nodes=[Node('spawn', Spawn(workflow, 'timer'), input_mapping=long_timer),
            Node('cancel', Cancel(), input_mapping=request)], edges=[Edge('spawn', 'cancel')])
        app = AutoAgentApp()
        try:
            result = app.invoke(root, {'value': 1})
            self.assertEqual(result.status, 'completed', result)
            self.assertEqual(app._repository.state(result.output.handle.session_id).invocation.status, 'cancelled')
        finally:
            app.close()

    def test_await_any_prefix_recovery(self):
        """AwaitAny preserves its recorded selection across event-prefix recovery."""
        from tests.test_child_recovery_integrity import _checkpoint_from_prefix
        from tests.graph_fixtures import load_graph
        def request(context: InputMappingContext) -> AwaitAnyRequest:
            return AwaitAnyRequest(handles=[RuntimeHandle.model_validate(next(iter(context.incoming.values())))])
        root = Workflow('root', nodes=[Node('spawn', Spawn(Workflow('child', nodes=[Node('wait', Wait(Value, Value))]), 'wait')),
            Node('any', AwaitAny(), input_mapping=request)], edges=[Edge('spawn', 'any')])
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(root, {'value': 1}, session_id='root')
            self.assertEqual(result.status, 'settling', result)
            events = tuple(RuntimeEvent.from_record(e.to_record()) for e in sink.events)
        finally:
            app.close()
        start = next(i for i, e in enumerate(events) if e.event_name == 'operator_call.started'
            and e.payload.operator_id == 'system_command:await_any')
        for cut in range(start, len(events)):
            restored = AutoAgentApp()
            try:
                restored.register_workflow(root)
                ref = load_graph(restored, _checkpoint_from_prefix(events[:cut+1])).invocations[0]
                result = restored.recover(ref)
                self.assertEqual(result.status, 'settling', result)
                self.assertEqual(result.output.status, 'waiting')
            finally:
                restored.close()

    def test_parallel_cancel_calls_and_sibling_target(self):
        """Parallel callers establish one cancellation origin and a sibling can cancel by explicit handle."""
        child = Workflow('child', nodes=[Node('wait', Wait(Value, Value))])
        root = Workflow('root', nodes=[Node('await', Await(child, 'wait')),
            Node('first', Cancel(), input_mapping=cancel_request),
            Node('second', Cancel(), input_mapping=cancel_request)],
            edges=[Edge('await', 'first'), Edge('await', 'second')])
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(root, {'value': 1})
            self.assertEqual(result.status, 'completed', result)
            self.assertEqual(sum(e.event_name == 'invocation.cancellation_requested' for e in sink.events), 1)
            self.assertIsNotNone(app.unload_session(result.ref, capture_checkpoint=True))
        finally:
            app.close()
        def sibling_request(context: InputMappingContext) -> CancelRequest:
            return CancelRequest(handle=RuntimeHandle.model_validate(next(iter(context.incoming.values()))))
        controller = Workflow('controller', nodes=[Node('cancel', Cancel())])
        root = Workflow('root', nodes=[Node('spawn', Spawn(child, 'wait')),
            Node('controller', Await(controller, 'cancel'), input_mapping=sibling_request)],
            edges=[Edge('spawn', 'controller')])
        app = AutoAgentApp()
        try:
            result = app.invoke(root, {'value': 1})
            self.assertEqual(result.status, 'completed', result)
            self.assertEqual(result.output.status, 'completed')
            self.assertEqual(result.output.output['disposition'], 'requested')
            self.assertIsNotNone(app.unload_session(result.ref, capture_checkpoint=True))
        finally:
            app.close()

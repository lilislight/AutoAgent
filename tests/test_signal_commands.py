"""Bounded Signal delivery, atomic receive and recovery contracts."""
import asyncio
import json
import unittest
from dataclasses import replace
from pydantic import BaseModel, ConfigDict

from autoagent import (
    AutoAgentApp, Await, Edge, InputMappingContext, Map, Node, RuntimeHandle,
    RuntimeInfrastructureError, RuntimeTransitionError, RuntimeGraphCheckpoint,
    ReceiveSignal, SendSignal, SelfHandle, OwnerHandle,
    SignalBatch, SignalEndpoint, SignalLimits, SignalReceipt, Wait, Workflow,
)
from autoagent.core import RuntimeEvent
from tests.test_system_commands import Collector, Value, identity


class LargeMessage(BaseModel):
    model_config = ConfigDict(extra='forbid')
    blob: str


def self_send(context: InputMappingContext) -> Value:
    return Value(value=7)


def self_workflow():
    return Workflow('self', nodes=[Node('send', SendSignal(handle=SelfHandle(), endpoint='message'), input_mapping=self_send),
        Node('receive', ReceiveSignal(endpoint='message', limit=100))], edges=[Edge('send', 'receive')],
        signal_endpoints=[SignalEndpoint('message', Value)])


def waiting_workflow(limits=None):
    return Workflow('waiting', nodes=[Node('wait', Wait(Value, Value)),
        Node('receive', ReceiveSignal(endpoint='message', limit=100))], edges=[Edge('wait', 'receive')],
        signal_endpoints=[SignalEndpoint('message', Value)], signal_limits=limits or SignalLimits())


class SignalCommandTests(unittest.TestCase):
    def test_internal_self_delivery_and_atomic_receive(self):
        """Receive moves accepted messages into its Call result in one event and frees the mailbox."""
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(self_workflow(), {'value': 1}, session_id='root')
            self.assertEqual(result.status, 'completed', result)
            self.assertIsInstance(result.output, SignalBatch)
            self.assertEqual([m.payload for m in result.output.messages], [{'value': 7}])
            state = app._repository.state('root').invocation.signals
            self.assertEqual(dict(state['messages']), {})
            self.assertEqual(state['bytes'], 0)
            self.assertEqual(dict(state['receipts']), {})
            self.assertEqual(len(state['frontiers']), 1)
            accepted = next(e for e in sink.events if e.event_name == 'signal.accepted')
            received = next(e for e in sink.events if e.event_name == 'signal.received')
            self.assertEqual(result.output.messages[0].accepted_sequence, accepted.sequence)
            self.assertTrue(any(o.path[-1] == 'output' for o in received.delta.operations))
            self.assertTrue(any(o.op == 'remove' and 'messages' in o.path for o in received.delta.operations))
            mailbox_entry = next(o.value for o in accepted.delta.operations if o.op == 'add' and 'messages' in o.path)
            received_output = next(o.value for o in received.delta.operations if o.path[-1] == 'output')
            self.assertIs(mailbox_entry['message']['payload'], accepted.payload.payload)
            self.assertIs(received_output['messages'][0]['payload'], accepted.payload.payload)
            self.assertIsNotNone(app.unload_session(result.ref, capture_checkpoint=True))
        finally:
            app.close()

    def test_external_delivery_does_not_resume_wait_and_sequences_are_bounded(self):
        """External retries reuse receipts, reject conflicts/gaps and retain one record per source."""
        app = AutoAgentApp()
        try:
            result = app.invoke(waiting_workflow(), {'value': 1})
            first = app.signal(result.ref, 'message', {'value': 2}, source_id='client', sequence=1)
            self.assertIsInstance(first, SignalReceipt)
            self.assertEqual(first, app.signal(result.ref, 'message', {'value': 2}, source_id='client', sequence=1))
            for sequence, payload, code in ((1, {'value': 3}, 'SIGNAL_SEQUENCE_CONFLICT'), (3, {'value': 3}, 'SIGNAL_SEQUENCE_GAP')):
                with self.assertRaisesRegex(RuntimeTransitionError, code):
                    app.signal(result.ref, 'message', payload, source_id='client', sequence=sequence)
            second = app.signal(result.ref, 'message', {'value': 3}, source_id='client', sequence=2)
            with self.assertRaisesRegex(RuntimeTransitionError, 'SIGNAL_SEQUENCE_RETIRED'):
                app.signal(result.ref, 'message', {'value': 2}, source_id='client', sequence=1)
            inv = app._repository.state(result.ref.session_id).invocation
            self.assertEqual(inv.status, 'waiting')
            self.assertEqual(len(inv.signals['messages']), 2)
            self.assertEqual(len(inv.signals['sources']), 1)
            completed = app.resume(result.ref, result.waits[0].id, {'value': 0})
            self.assertEqual([m.payload['value'] for m in completed.output.messages], [2, 3])
            self.assertEqual(second, app.signal(result.ref, 'message', {'value': 3}, source_id='client', sequence=2))
            with self.assertRaisesRegex(RuntimeTransitionError, 'SIGNAL_TARGET_UNAVAILABLE'):
                app.signal(result.ref, 'message', {'value': 4}, source_id='client', sequence=3)
        finally:
            app.close()

    def test_capacity_contract_and_unknown_endpoint_do_not_mutate(self):
        """Invalid payloads, endpoints, oversized messages and capacity overflow leave state unchanged."""
        limits = SignalLimits(max_message_bytes=12, max_mailbox_bytes=24, max_messages=1, max_external_sources=1)
        app = AutoAgentApp()
        try:
            result = app.invoke(waiting_workflow(limits), {'value': 1})
            for endpoint, payload in (('unknown', {'value': 1}), ('message', {'value': 'bad'}), ('message', {'value': 123456789})):
                before = app._repository.state(result.ref.session_id)
                with self.assertRaises((TypeError, RuntimeTransitionError)):
                    app.signal(result.ref, endpoint, payload, source_id='client', sequence=1)
                self.assertIs(before, app._repository.state(result.ref.session_id))
            first = app.signal(result.ref, 'message', {'value': 1}, source_id='client', sequence=1)
            self.assertEqual(first, app.signal(result.ref, 'message', {'value': 1}, source_id='client', sequence=1))
            with self.assertRaisesRegex(RuntimeTransitionError, 'MAILBOX_FULL'):
                app.signal(result.ref, 'message', {'value': 2}, source_id='client', sequence=2)
        finally:
            app.close()

    def test_checkpoint_roundtrip_preserves_fifo_independent_of_json_key_order(self):
        """Mailbox FIFO uses acceptance sequence rather than JSON object key ordering."""
        workflow = waiting_workflow()
        app = AutoAgentApp()
        try:
            result = app.invoke(workflow, {'value': 1})
            for index, source in enumerate(('z', 'a', 'm')):
                app.signal(result.ref, 'message', {'value': index}, source_id=source, sequence=1)
            checkpoint = app.unload_session(result.ref, capture_checkpoint=True)
            checkpoint = RuntimeGraphCheckpoint.from_record(json.loads(json.dumps(checkpoint.to_record(), sort_keys=True)))
            app.load_checkpoint(checkpoint)
            result = app.resume(result.ref, result.waits[0].id, {'value': 0})
            self.assertEqual([m.payload['value'] for m in result.output.messages], [0, 1, 2])
        finally:
            app.close()

    def test_competing_receives_never_deliver_one_message_twice(self):
        """Concurrent ReceiveSignal Nodes partition messages through the target lane."""
        workflow = Workflow('parallel', nodes=[Node('wait', Wait(Value, Value)),
            Node('left', ReceiveSignal(endpoint='message')), Node('right', ReceiveSignal(endpoint='message'))],
            edges=[Edge('wait', 'left'), Edge('wait', 'right')], signal_endpoints=[SignalEndpoint('message', Value)])
        app = AutoAgentApp()
        try:
            result = app.invoke(workflow, {'value': 1})
            app.signal(result.ref, 'message', {'value': 1}, source_id='client', sequence=1)
            result = app.resume(result.ref, result.waits[0].id, {'value': 0})
            self.assertEqual(result.status, 'completed', result)
            messages = [m for batch in result.output.values() for m in batch['messages']]
            self.assertEqual(len(messages), 1)
        finally:
            app.close()

    def test_internal_event_prefix_recovery(self):
        """Every Send/Receive prefix recovers without redelivering or consuming twice."""
        from tests.test_child_recovery_integrity import _checkpoint_from_prefix
        from tests.graph_fixtures import load_graph
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(self_workflow(), {'value': 1}, session_id='root')
            self.assertEqual(result.status, 'completed', result)
            events = tuple(RuntimeEvent.from_record(e.to_record()) for e in sink.events)
        finally:
            app.close()
        start = next(i for i, e in enumerate(events) if e.event_name == 'operator_call.started')
        for cut in range(start, len(events)):
            with self.subTest(cut=events[cut].event_name):
                recovery_sink = Collector()
                restored = AutoAgentApp(runtime_event_sink=recovery_sink)
                try:
                    restored.register_workflow(self_workflow())
                    ref = load_graph(restored, _checkpoint_from_prefix(events[:cut+1])).invocations[0]
                    result = restored.recover(ref)
                    self.assertEqual(result.status, 'completed', result)
                    self.assertEqual([m.payload for m in result.output.messages], [{'value': 7}])
                    for kind in ('signal.accepted', 'signal.received'):
                        self.assertEqual(sum(e.event_name == kind for e in (*events[:cut+1], *recovery_sink.events)), 1)
                finally:
                    restored.close()

    def test_ack_failure_retries_exact_event(self):
        """Acceptance, receipt retirement and atomic receive ACK losses retry the original event."""
        for boundary in ('signal.accepted', 'signal.receipt_released', 'signal.received'):
            class Sink(Collector):
                failed = None
                retries = 0
                async def append(self, event):
                    if event is self.failed:
                        self.retries += 1
                    if event.event_name == boundary and self.failed is None:
                        self.failed = event
                        raise OSError('lost ACK')
                    await super().append(event)
            sink = Sink()
            app = AutoAgentApp(runtime_event_sink=sink)
            try:
                with self.assertRaises(RuntimeInfrastructureError):
                    app.invoke(self_workflow(), {'value': 1}, session_id='root')
                inv = app._repository.state('root').invocation
                result = app.recover(app._ref_for_invocation('root', inv))
                self.assertEqual(result.status, 'completed', (boundary, result))
                self.assertEqual(len(result.output.messages), 1)
                self.assertEqual(sink.retries, 1)
            finally:
                app.close()

    def test_terminal_mailbox_releases_unreceived_payloads(self):
        """Completed and cancelled invocations clear unread mailbox payloads."""
        workflow = Workflow('sender', nodes=[Node('send', SendSignal(handle=SelfHandle(), endpoint='message'), input_mapping=self_send)],
            signal_endpoints=[SignalEndpoint('message', Value)])
        app = AutoAgentApp()
        try:
            result = app.invoke(workflow, {'value': 1})
            self.assertEqual(result.status, 'completed', result)
            self.assertEqual(app._repository.state(result.ref.session_id).invocation.signals['bytes'], 0)
            result = app.invoke(waiting_workflow(), {'value': 1})
            app.signal(result.ref, 'message', {'value': 1}, source_id='client', sequence=1)
            app.cancel(result.ref)
            self.assertEqual(app._repository.state(result.ref.session_id).invocation.signals['bytes'], 0)
        finally:
            app.close()

    def test_async_external_concurrency_respects_capacity(self):
        """Concurrent external producers cannot overfill the mailbox or race sequence acceptance."""
        app = AutoAgentApp()
        async def run():
            result = await app.ainvoke(waiting_workflow(SignalLimits(max_messages=1)), {'value': 1})
            outcomes = await asyncio.gather(
                app.asignal(result.ref, 'message', {'value': 1}, source_id='a', sequence=1),
                app.asignal(result.ref, 'message', {'value': 2}, source_id='b', sequence=1), return_exceptions=True)
            self.assertEqual(sum(isinstance(v, SignalReceipt) for v in outcomes), 1)
            self.assertEqual(sum(isinstance(v, RuntimeTransitionError) and v.code == 'MAILBOX_FULL' for v in outcomes), 1)
        try:
            asyncio.run(run())
        finally:
            app.close()

    def test_external_ack_retry_and_source_limit(self):
        """A lost acceptance ACK is idempotent and the external source ledger has a hard bound."""
        class Sink(Collector):
            failed = None
            async def append(self, event):
                if event.event_name == 'signal.accepted' and self.failed is None:
                    self.failed = event
                    raise OSError('lost external ACK')
                await super().append(event)
        sink = Sink()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(waiting_workflow(SignalLimits(max_external_sources=1)), {'value': 1})
            with self.assertRaises(RuntimeInfrastructureError):
                app.signal(result.ref, 'message', {'value': 1}, source_id='a', sequence=1)
            receipt = app.signal(result.ref, 'message', {'value': 1}, source_id='a', sequence=1)
            self.assertEqual(receipt.accepted_sequence, sink.failed.sequence)
            self.assertEqual(sum(e.event_name == 'signal.accepted' for e in sink.events), 1)
            with self.assertRaisesRegex(RuntimeTransitionError, 'SIGNAL_SOURCES_FULL'):
                app.signal(result.ref, 'message', {'value': 1}, source_id='b', sequence=1)
            self.assertEqual(len(app._repository.state(result.ref.session_id).invocation.signals['messages']), 1)
        finally:
            app.close()

    def test_cross_graph_and_forged_handle_rejection(self):
        """Foreign and stale internal handles cannot publish to a target mailbox."""
        app = AutoAgentApp()
        try:
            result = app.invoke(waiting_workflow(), {'value': 1})
            handle = RuntimeHandle(**result.ref.model_dump())
            before = app._repository.state(result.ref.session_id)
            sender = Workflow('sender', nodes=[Node('send', SendSignal(handle=handle, endpoint='message'))])
            sent = app.invoke(sender, {'value': 1})
            self.assertEqual(sent.status, 'failed')
            self.assertIs(app._repository.state(result.ref.session_id), before)
            def forged(context: InputMappingContext) -> RuntimeHandle:
                return context.self_handle.model_copy(update={'invocation_id': 'stale'})
            sent = app.invoke(Workflow('forged', nodes=[Node('send', SendSignal(handle=forged, endpoint='message'))],
                signal_endpoints=[SignalEndpoint('message', Value)]), {'value': 1})
            self.assertEqual(sent.status, 'failed')
            self.assertFalse(app._repository.state(sent.ref.session_id).invocation.signals['messages'])
        finally:
            app.close()

    def test_map_send_and_empty_receive(self):
        """Map sends preserve acceptance order and receiving from an empty Endpoint returns an empty batch."""
        def requests(context: InputMappingContext) -> list[Value]:
            return [Value(value=i) for i in range(5)]
        workflow = Workflow('map', nodes=[Node('send', SendSignal(handle=SelfHandle(), endpoint='message'), map=Map(), input_mapping=requests),
            Node('receive', ReceiveSignal(endpoint='message', limit=100))], edges=[Edge('send', 'receive')],
            signal_endpoints=[SignalEndpoint('message', Value)])
        app = AutoAgentApp()
        try:
            result = app.invoke(workflow, {'value': 1})
            self.assertEqual(result.status, 'completed', result)
            self.assertEqual([m.payload['value'] for m in result.output.messages], list(range(5)))
            result = app.invoke(Workflow('empty', nodes=[Node('receive', ReceiveSignal(endpoint='message', limit=100))],
                signal_endpoints=[SignalEndpoint('message', Value)]), {'value': 1})
            self.assertEqual(result.output.messages, [])
        finally:
            app.close()

    def test_compact_target_before_sender_ack_prefixes(self):
        """A consumed message and compact target retain proof until the sender result is acknowledged."""
        from autoagent import Spawn, Recovery
        from autoagent.core import ChildResult
        from tests.test_child_recovery_integrity import _checkpoint_from_prefix
        from tests.graph_fixtures import load_graph
        delivered, compacted = asyncio.Event(), asyncio.Event()
        async def wait_delivery(value: Value) -> Value:
            await asyncio.wait_for(delivered.wait(), 2)
            return value
        def child_handle(context: InputMappingContext) -> RuntimeHandle:
            return RuntimeHandle.model_validate(next(iter(context.incoming.values())))
        def request(context: InputMappingContext) -> Value:
            return Value(value=9)
        child = Workflow('child', nodes=[Node('work', wait_delivery, recovery_mode=Recovery('replay_safe', 3)),
            Node('receive', ReceiveSignal(endpoint='message', limit=100))], edges=[Edge('work', 'receive')],
            signal_endpoints=[SignalEndpoint('message', Value)])
        root = Workflow('root', nodes=[Node('spawn', Spawn(child, 'work')), Node('send', SendSignal(handle=child_handle, endpoint='message'), input_mapping=request)],
            edges=[Edge('spawn', 'send')])
        class Sink(Collector):
            sender_call = None
            async def append(self, event):
                if event.event_name == 'operator_call.started' and event.payload.operator_id == 'system_command:send_signal':
                    self.sender_call = event.payload.call_id
                if event.event_name == 'operator_call.completed' and event.payload.call_id == self.sender_call:
                    await asyncio.wait_for(compacted.wait(), 2)
                await super().append(event)
                if event.event_name == 'signal.accepted':
                    delivered.set()
                if event.event_name == 'child_invocation.compacted':
                    compacted.set()
        sink = Sink()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(root, {'value': 1}, session_id='root')
            self.assertEqual(result.status, 'completed', result)
            events = tuple(RuntimeEvent.from_record(e.to_record()) for e in sink.events)
        finally:
            app.close()
        start = next(i for i, e in enumerate(events) if e.event_name == 'signal.accepted')
        checked = False
        for cut in range(start, len(events)):
            with self.subTest(cut=events[cut].event_name):
                cp = _checkpoint_from_prefix(events[:cut+1])
                if events[cut].event_name == 'child_invocation.compacted':
                    target = next(s.state.invocation for g in cp.graphs for s in g.sessions if s.session_id != 'root')
                    self.assertIsInstance(target, ChildResult)
                    self.assertTrue(target.signals['receipts'])
                    self.assertFalse(target.signals['messages'])
                    checked = True
                restored_sink = Collector()
                restored = AutoAgentApp(runtime_event_sink=restored_sink)
                try:
                    restored.register_workflow(root)
                    ref = load_graph(restored, cp).invocations[0]
                    result = restored.recover(ref)
                    self.assertEqual(result.status, 'completed', result)
                    target = restored._repository.state(result.output.handle.session_id).invocation
                    self.assertEqual(target.output['messages'][0]['payload'], {'value': 9})
                    self.assertEqual(sum(e.event_name == 'signal.accepted' for e in (*events[:cut+1], *restored_sink.events)), 1)
                    self.assertFalse(target.signals['receipts'])
                finally:
                    restored.close()
        self.assertTrue(checked)

    def test_loop_releases_payload_and_keeps_constant_receipt_metadata(self):
        """Repeated send/receive cycles keep one caller frontier and release payloads from live State."""
        from autoagent import ConditionContext, OutputBindingContext, ContextPatch, ContextOperation
        def count(context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(invocation=(ContextOperation.set('count', context.invocation_context.get('count', 0) + 1),))
        def again(context: ConditionContext) -> bool:
            return context.invocation_context['count'] < 100
        def done(context: ConditionContext) -> bool:
            return not again(context)
        def finish(context: InputMappingContext) -> Value:
            return Value(value=context.invocation_context['count'])
        def large_send(context: InputMappingContext) -> LargeMessage:
            return LargeMessage(blob='payload-marker:' + str(context.invocation_context.get('count', 0)) + ':' + 'x' * 65536)
        workflow = Workflow('loop', nodes=[Node('entry', identity), Node('send', SendSignal(handle=SelfHandle(), endpoint='message'), input_mapping=large_send),
            Node('receive', ReceiveSignal(endpoint='message', limit=100), output_binding=count),
            Node('finish', identity, input_mapping=finish)], edges=[Edge('entry', 'send'), Edge('send', 'receive'),
                Edge('receive', 'send', again), Edge('receive', 'finish', done)], signal_endpoints=[SignalEndpoint('message', LargeMessage)])
        app = AutoAgentApp()
        try:
            result = app.invoke(workflow, {'value': 0})
            self.assertEqual(result.status, 'completed', result)
            self.assertEqual(result.output, {'value': 100})
            state = app._repository.state(result.ref.session_id).invocation
            self.assertEqual(len(state.signals['frontiers']), 1)
            self.assertFalse(state.signals['receipts'])
            self.assertFalse(state.signals['messages'])
            self.assertEqual(state.signals['bytes'], 0)
            self.assertFalse(app._signal_receipt_targets)
            self.assertNotIn('payload-marker:', json.dumps(app._repository.state(result.ref.session_id).to_record()))
        finally:
            app.close()

    def test_internal_receipt_and_source_capacity(self):
        """Concurrent Child senders cannot exceed target receipt or source limits."""
        for kind in ('receipt', 'source'):
            release = asyncio.Event()
            class Sink(Collector):
                first = None
                async def append(self, event):
                    if event.event_name == 'operator_call.started' and event.payload.operator_id == 'system_command:send_signal' and self.first is None:
                        self.first = event.payload.call_id
                    if kind == 'receipt' and event.event_name == 'operator_call.completed' and event.payload.call_id == self.first:
                        await asyncio.wait_for(release.wait(), 2)
                    if event.event_name == 'operator_call.failed':
                        release.set()
                    await super().append(event)
            def inputs(context: InputMappingContext) -> list[Value]:
                return [Value(value=i) for i in range(2)]
            limits = SignalLimits(max_pending_receipts=1) if kind == 'receipt' else SignalLimits(max_internal_sources=1)
            child = Workflow('sender_child', nodes=[Node('entry', identity), Node('send', SendSignal(handle=OwnerHandle(), endpoint='message'))], edges=[Edge('entry', 'send')])
            root = Workflow('root', nodes=[Node('children', Await(child, 'entry'), map=Map(), input_mapping=inputs)],
                signal_endpoints=[SignalEndpoint('message', Value)], signal_limits=limits)
            sink = Sink()
            app = AutoAgentApp(runtime_event_sink=sink)
            try:
                result = app.invoke(root, {'value': 1})
                self.assertEqual(result.status, 'completed', result)
                self.assertEqual(sorted(x.status for x in result.output), ['completed', 'failed'])
                error = next(x.error for x in result.output if x.status == 'failed')
                self.assertIn('SIGNAL_RECEIPTS_FULL' if kind == 'receipt' else 'SIGNAL_SOURCES_FULL', error['message'])
                inv = app._repository.state(result.ref.session_id).invocation
                self.assertFalse(inv.signals['receipts'])
                self.assertEqual(len(inv.signals['frontiers']), 1)
            finally:
                app.close()

    def test_rejected_external_requests_keep_sequence_available(self):
        """Payload failures do not reserve a sequence, and invalid source IDs or sequence types are rejected."""
        app = AutoAgentApp()
        try:
            result = app.invoke(waiting_workflow(), {'value': 1})
            for source, sequence in (('', 1), ('x' * 129, 1), ('x', 0), ('x', True)):
                with self.assertRaises(ValueError):
                    app.signal(result.ref, 'message', {'value': 1}, source_id=source, sequence=sequence)
            with self.assertRaises(TypeError):
                app.signal(result.ref, 'message', {'value': 'bad'}, source_id='x', sequence=1)
            receipt = app.signal(result.ref, 'message', {'value': 1}, source_id='x', sequence=1)
            self.assertEqual(receipt.endpoint, 'message')
        finally:
            app.close()

    def test_endpoint_and_limit_definitions_affect_revision(self):
        """Endpoint contracts and capacity limits are part of the portable Workflow definition."""
        from autoagent.core import WorkflowCompiler, WorkflowDefinitionSnapshot
        compiler = WorkflowCompiler()
        first = compiler.compile_or_raise(waiting_workflow())
        changed = compiler.compile_or_raise(waiting_workflow(SignalLimits(max_messages=2)))
        self.assertNotEqual(first.workflow_revision_id, changed.workflow_revision_id)
        other = waiting_workflow()
        other.signal_endpoints = [SignalEndpoint('message', LargeMessage)]
        self.assertNotEqual(first.workflow_revision_id, compiler.compile_or_raise(other).workflow_revision_id)
        duplicate = waiting_workflow()
        duplicate.signal_endpoints.append(SignalEndpoint('message', LargeMessage))
        self.assertFalse(compiler.compile(duplicate).ok)
        for invalid in ({'max_messages': 0}, {'max_pending_receipts': True}, {'max_message_bytes': 100, 'max_mailbox_bytes': 10}):
            with self.assertRaises(ValueError):
                SignalLimits(**invalid)
        snapshot = WorkflowDefinitionSnapshot.from_workflow_ir(first)
        self.assertEqual(WorkflowDefinitionSnapshot.from_record(snapshot.to_record()), snapshot)

    def test_checkpoint_rejects_retirement_without_sender_ack(self):
        """Independently cut Session histories cannot retire proof while erasing sender acknowledgement."""
        from autoagent import Spawn
        from tests.test_child_recovery_integrity import _checkpoint_from_prefix
        from tests.graph_fixtures import session_checkpoints, graph_bundle
        def request(context: InputMappingContext) -> Value:
            return Value(value=1)
        child = Workflow('child', nodes=[Node('entry', identity), Node('send', SendSignal(handle=OwnerHandle(), endpoint='message'), input_mapping=request)], edges=[Edge('entry', 'send')])
        root = Workflow('root', nodes=[Node('await', Await(child, 'entry'))], signal_endpoints=[SignalEndpoint('message', Value)])
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(root, {'value': 1}, session_id='root')
            self.assertEqual(result.status, 'completed', result)
            events = tuple(sink.events)
        finally:
            app.close()
        accepted = next(i for i, e in enumerate(events) if e.event_name == 'signal.accepted')
        released = next(i for i, e in enumerate(events) if e.event_name == 'signal.receipt_released')
        before = session_checkpoints(_checkpoint_from_prefix(events[:accepted+1]))
        after = session_checkpoints(_checkpoint_from_prefix(events[:released+1]))
        old_sender = next(s for s in before if s.session_id != 'root')
        new_target = next(s for s in after if s.session_id == 'root')
        with self.assertRaisesRegex(ValueError, 'Signal receipt retirement'):
            graph_bundle((new_target, old_sender))

    def test_usage_example(self):
        """The standalone example exercises Parent/Child and external checkpoint delivery end to end."""
        from examples.signal_workflow_demo import run_demo
        result = run_demo()
        self.assertTrue(result['duplicate_returned_same_receipt'])
        self.assertEqual(result['checkpoint_roundtrip'], 'ok')
        self.assertEqual(len(result['internal_messages']), 1)
        self.assertEqual(len(result['external_messages']), 2)

    def test_mailbox_total_bytes_and_checkpoint_accounting(self):
        """Byte capacity is enforced independently of message count and corrupt accounting cannot load."""
        from autoagent.core import SessionCheckpoint
        from autoagent.core.runtime.values import freeze, thaw
        limits = SignalLimits(max_message_bytes=12, max_mailbox_bytes=12, max_messages=10)
        app = AutoAgentApp()
        try:
            result = app.invoke(waiting_workflow(limits), {'value': 1})
            app.signal(result.ref, 'message', {'value': 1}, source_id='client', sequence=1)
            with self.assertRaisesRegex(RuntimeTransitionError, 'MAILBOX_FULL'):
                app.signal(result.ref, 'message', {'value': 2}, source_id='client', sequence=2)
            state = app._repository.state(result.ref.session_id)
            signals = thaw(state.invocation.signals)
            signals['bytes'] += 1
            with self.assertRaisesRegex(ValueError, 'accounting'):
                SessionCheckpoint.from_state(replace(state, invocation=replace(state.invocation, signals=freeze(signals))))
        finally:
            app.close()

    def test_signal_cancel_race(self):
        """Cancellation serializes with delivery and never leaves payloads in a terminal mailbox."""
        app = AutoAgentApp()
        async def run():
            result = await app.ainvoke(waiting_workflow(), {'value': 1})
            outcomes = await asyncio.gather(
                app.asignal(result.ref, 'message', {'value': 1}, source_id='client', sequence=1),
                app.acancel(result.ref, reason='stop'), return_exceptions=True)
            self.assertEqual(outcomes[1].status, 'cancelled')
            if isinstance(outcomes[0], Exception):
                self.assertIsInstance(outcomes[0], RuntimeTransitionError)
                self.assertEqual(outcomes[0].code, 'SIGNAL_TARGET_UNAVAILABLE')
            else:
                self.assertEqual(outcomes[0], await app.asignal(result.ref, 'message', {'value': 1}, source_id='client', sequence=1))
            inv = app._repository.state(result.ref.session_id).invocation
            self.assertFalse(inv.signals['messages'])
            self.assertEqual(inv.signals['bytes'], 0)
        try:
            asyncio.run(run())
        finally:
            app.close()

    def test_signal_configuration_and_snapshot(self):
        """Signal targets and endpoints are visible in snapshots and invalid authoring fails early."""
        import autoagent
        from autoagent.core import WorkflowCompiler, WorkflowDefinitionSnapshot
        self.assertFalse(hasattr(autoagent, 'SendSignalRequest'))
        self.assertFalse(hasattr(autoagent, 'ReceiveSignalRequest'))
        for factory in (lambda: SendSignal(), lambda: ReceiveSignal(),
                lambda: SendSignal(handle=None, endpoint='message'),
                lambda: SendSignal(handle=SelfHandle(), endpoint=''),
                lambda: ReceiveSignal(endpoint='message', limit=0),
                lambda: ReceiveSignal(endpoint='message', limit=True)):
            with self.assertRaises((TypeError, ValueError)):
                factory()
        compiler = WorkflowCompiler()
        first = compiler.compile_or_raise(self_workflow())
        changed = self_workflow()
        changed.nodes[0] = Node('send', SendSignal(handle=OwnerHandle(), endpoint='message'), input_mapping=self_send)
        self.assertNotEqual(first.workflow_revision_id, compiler.compile_or_raise(changed).workflow_revision_id)
        changed = self_workflow()
        changed.nodes[0] = Node('send', SendSignal(handle=SelfHandle(), endpoint='other'), input_mapping=self_send)
        self.assertNotEqual(first.workflow_revision_id, compiler.compile_or_raise(changed).workflow_revision_id)
        changed = self_workflow()
        changed.nodes[1] = Node('receive', ReceiveSignal(endpoint='message', limit=1))
        self.assertNotEqual(first.workflow_revision_id, compiler.compile_or_raise(changed).workflow_revision_id)
        snapshot = WorkflowDefinitionSnapshot.from_workflow_ir(first).to_record()
        send = snapshot['definition']['nodes'][0]['executable']
        self.assertEqual(send['endpoint'], 'message')
        self.assertEqual(send['handle'], {'kind': 'self'})
        invalid = Workflow('invalid', nodes=[Node('receive', ReceiveSignal(endpoint='message'), input_mapping=self_send)])
        self.assertFalse(compiler.compile(invalid).ok)
        def wrong(context: InputMappingContext) -> Value:
            return Value(value=1)
        self.assertFalse(compiler.compile(Workflow('invalid', nodes=[Node('send', SendSignal(handle=wrong, endpoint='message'))])).ok)

    def test_async_handle_resolver_is_not_replayed_after_call_started(self):
        """A recorded SendSignal reuses its target even if the resolver would now fail or choose another target."""
        from tests.test_child_recovery_integrity import _checkpoint_from_prefix
        from tests.graph_fixtures import load_graph
        calls = []
        forbidden = False
        async def target(context: InputMappingContext) -> RuntimeHandle:
            if forbidden:
                raise AssertionError('Handle must not be resolved twice after CallStarted')
            calls.append(context.self_handle)
            return context.self_handle
        workflow = self_workflow()
        workflow.nodes[0] = Node('send', SendSignal(handle=target, endpoint='message'), input_mapping=self_send)
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(workflow, {'value': 1}, session_id='root')
            self.assertEqual(result.status, 'completed', result)
            events = tuple(RuntimeEvent.from_record(e.to_record()) for e in sink.events)
            self.assertEqual(len(calls), 1)
        finally:
            app.close()
        forbidden = True
        start = next(i for i, e in enumerate(events) if e.event_name == 'operator_call.started')
        for cut in range(start, len(events)):
            restored = AutoAgentApp()
            try:
                restored.register_workflow(workflow)
                ref = load_graph(restored, _checkpoint_from_prefix(events[:cut+1])).invocations[0]
                outcome = restored.recover(ref)
                self.assertEqual(outcome.status, 'completed', (cut, outcome))
                self.assertEqual(outcome.output.messages[0].payload, {'value': 7})
            finally:
                restored.close()
        self.assertEqual(len(calls), 1)

    def test_direct_payload_and_missing_owner(self):
        """SendSignal accepts upstream payloads directly and rejects OwnerHandle on a Root."""
        workflow = Workflow('direct', nodes=[Node('entry', identity),
            Node('send', SendSignal(handle=SelfHandle(), endpoint='message')),
            Node('receive', ReceiveSignal(endpoint='message'))],
            edges=[Edge('entry', 'send'), Edge('send', 'receive')], signal_endpoints=[SignalEndpoint('message', Value)])
        app = AutoAgentApp()
        try:
            result = app.invoke(workflow, {'value': 42})
            self.assertEqual(result.status, 'completed', result)
            self.assertEqual(result.output.messages[0].payload, {'value': 42})
            result = app.invoke(Workflow('no_owner', nodes=[Node('send', SendSignal(handle=OwnerHandle(), endpoint='message'))]), {'value': 1})
            self.assertEqual(result.status, 'failed')
        finally:
            app.close()

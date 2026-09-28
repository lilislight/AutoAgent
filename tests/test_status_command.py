"""Status observes acknowledged graph state without advancing the target."""
import asyncio
import unittest

from autoagent import (
    AutoAgentApp, Await, Edge, InputMappingContext, Map, Node, Operator,
    RuntimeHandle, RuntimeInfrastructureError, RuntimeObservation,
    RuntimeTransitionError, Spawn, Status, Wait, Workflow,
)
from autoagent.core import ChildResult, RuntimeEvent, RuntimeGraphCheckpoint
from tests.test_system_commands import Collector, Value, identity


def observed_handle(context: InputMappingContext) -> RuntimeHandle:
    return RuntimeHandle.model_validate(next(iter(context.incoming.values()))['handle'])


def self_handle(context: InputMappingContext) -> RuntimeHandle:
    return context.self_handle


def owner_handle(context: InputMappingContext) -> RuntimeHandle:
    return context.owner_handle


def status_workflow(child):
    return Workflow('root', nodes=[Node('await', Await(child, child.nodes[0].id)),
        Node('status', Status(), input_mapping=observed_handle)], edges=[Edge('await', 'status')])


class StatusCommandTests(unittest.TestCase):
    def test_self_and_parent_are_immediate_observations(self):
        """Self and owner queries observe current running or suspended status without joining."""
        with self.assertRaises(ValueError):
            Operator(identity, id='system_command:status')
        self.assertEqual(Status().id, 'system_command:status')
        for mapping in (self_handle, owner_handle):
            child = Workflow('child', nodes=[Node('entry', identity), Node('status', Status(), input_mapping=mapping)], edges=[Edge('entry', 'status')])
            workflow = child if mapping is self_handle else Workflow('root', nodes=[Node('await', Await(child, 'entry'))])
            app = AutoAgentApp()
            try:
                result = app.invoke(workflow, {'value': 1})
                self.assertEqual(result.status, 'completed', result)
                observation = result.output if mapping is self_handle else RuntimeObservation.model_validate(result.output.output)
                self.assertIn(observation.status, {'running'} if mapping is self_handle else {'running', 'waiting'})
                self.assertEqual(observation.handle.session_id, result.ref.session_id)
            finally:
                app.close()

    def test_waiting_and_settling_include_descendant_waits(self):
        """Status exposes the concrete Wait handle even while an ancestor is settling."""
        leaf = Workflow('leaf', nodes=[Node('wait', Wait(Value, Value))])
        for child, expected in ((leaf, 'waiting'),
                (Workflow('child', nodes=[Node('spawn', Spawn(leaf, 'wait'))]), 'settling')):
            app = AutoAgentApp()
            try:
                result = app.invoke(status_workflow(child), {'value': 3})
                self.assertEqual(result.status, 'settling', result)
                self.assertEqual(result.output.status, expected)
                self.assertEqual(result.output.waits[0].handle.workflow_id, 'leaf')
                self.assertEqual(result.output.waits[0].request, {'value': 3})
                self.assertEqual(result.output.waits[0].wait_id, result.waits[0].id)
                if expected == 'settling':
                    self.assertEqual(result.output.pending_outcome, 'completed')
            finally:
                app.close()

    def test_terminal_compact_results_survive_load_and_are_detached(self):
        """Completed and failed compact Children remain queryable after checkpoint load."""
        def broken(value: Value) -> Value:
            raise ValueError('child failure')
        for function, expected in ((identity, 'completed'), (broken, 'failed')):
            workflow = status_workflow(Workflow('child', nodes=[Node('work', function)]))
            app = AutoAgentApp()
            try:
                result = app.invoke(workflow, {'value': 8})
                self.assertEqual(result.status, 'completed', result)
                handle = result.output.handle
                self.assertEqual(result.output.status, expected)
                self.assertIsInstance(app._repository.state(handle.session_id).invocation, ChildResult)
                cp = app.unload_session(result.ref, capture_checkpoint=True)
                app.load_checkpoint(RuntimeGraphCheckpoint.from_record(cp.to_record()))
                observed = app._runtime_loop.run(app._status_runtime(result.ref.session_id, handle))
                if expected == 'completed':
                    self.assertEqual(observed.output, {'value': 8})
                    observed.output['value'] = 99
                else:
                    self.assertEqual(observed.error['message'], 'child failure')
                    observed.error['message'] = 'changed'
                again = app._runtime_loop.run(app._status_runtime(result.ref.session_id, handle))
                self.assertEqual(again, result.output)
            finally:
                app.close()

    def test_cancelled_child_observation(self):
        """Cancelled compact Children retain their cancellation reason for Status."""
        app = AutoAgentApp()
        try:
            result = app.invoke(Workflow('root', nodes=[Node('spawn', Spawn(
                Workflow('child', nodes=[Node('wait', Wait(Value, Value))]), 'wait'))]), {'value': 1})
            app.cancel(result.ref, reason='stop graph')
            observed = app._runtime_loop.run(app._status_runtime(result.ref.session_id, result.output))
            self.assertEqual(observed.status, 'cancelled')
            self.assertIsNotNone(observed.cancel_reason)
            self.assertEqual(observed.waits, [])
        finally:
            app.close()

    def test_sibling_and_map_order(self):
        """Explicit sibling handles are readable and Map preserves input order including empty lists."""
        for values in ([], [3, 1, 2]):
            def batch(context: InputMappingContext) -> list[Value]:
                return [Value(value=v) for v in values]
            def handles(context: InputMappingContext) -> list[RuntimeHandle]:
                return [RuntimeHandle.model_validate(v['handle']) for v in next(iter(context.incoming.values()))]
            child = Workflow('child', nodes=[Node('work', identity)])
            workflow = Workflow('root', nodes=[Node('await', Await(child, 'work'), map=Map(), input_mapping=batch),
                Node('status', Status(), map=Map(), input_mapping=handles)], edges=[Edge('await', 'status')])
            app = AutoAgentApp()
            try:
                result = app.invoke(workflow, {'value': 1})
                self.assertEqual(result.status, 'completed', result)
                self.assertEqual([v.output['value'] for v in result.output], values)
                if values:
                    observed = app._runtime_loop.run(app._status_runtime(result.output[0].handle.session_id, result.output[1].handle))
                    self.assertEqual(observed, result.output[1])
            finally:
                app.close()

    def test_invalid_and_cross_graph_handles_fail_without_target_changes(self):
        """Unknown, stale, forged and cross-graph handles fail before target mutation."""
        app = AutoAgentApp()
        try:
            result = app.invoke(Workflow('target', nodes=[Node('wait', Wait(Value, Value))]), {'value': 1})
            handle = RuntimeHandle(**result.ref.model_dump())
            before = app._repository.state(handle.session_id)
            for field in ('session_id', 'invocation_id', 'workflow_id', 'workflow_revision_id'):
                with self.subTest(field=field), self.assertRaises(RuntimeTransitionError):
                    app._runtime_loop.run(app._status_runtime(handle.session_id, handle.model_copy(update={field: 'missing'})))
            other = app.invoke(Workflow('other', nodes=[Node('status', Status())]), handle.model_dump())
            self.assertEqual(other.status, 'failed')
            self.assertIn('exact Handle', other.error.message)
            self.assertIs(app._repository.state(handle.session_id), before)
        finally:
            app.close()

    def test_running_child_does_not_block_status(self):
        """A Child that needs the Status result to continue cannot deadlock the query."""
        started, release = asyncio.Event(), asyncio.Event()
        async def slow(value: Value) -> Value:
            started.set()
            await asyncio.wait_for(release.wait(), 2)
            return value
        async def target(context: InputMappingContext) -> RuntimeHandle:
            await asyncio.wait_for(started.wait(), 2)
            return RuntimeHandle.model_validate(next(iter(context.incoming.values())))
        async def finish(value: RuntimeObservation) -> RuntimeObservation:
            release.set()
            return value
        workflow = Workflow('root', nodes=[Node('spawn', Spawn(Workflow('child', nodes=[Node('slow', slow)]), 'slow')),
            Node('status', Status(), input_mapping=target), Node('finish', finish)],
            edges=[Edge('spawn', 'status'), Edge('status', 'finish')])
        app = AutoAgentApp()
        try:
            result = app.invoke(workflow, {'value': 1})
            self.assertEqual(result.status, 'completed', result)
            self.assertEqual(result.output.status, 'running')
        finally:
            app.close()

    def test_pending_target_ack_is_not_retried(self):
        """Status reads the last acknowledged state without settling a pending target event."""
        class Sink(Collector):
            attempts = 0
            async def append(self, event):
                if event.event_name == 'node_occurrence.waiting':
                    self.attempts += 1
                    if self.attempts == 1:
                        raise OSError('Wait ACK unavailable')
                await super().append(event)
        sink = Sink()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            with self.assertRaises(RuntimeInfrastructureError):
                app.invoke(Workflow('target', nodes=[Node('wait', Wait(Value, Value))]), {'value': 1}, session_id='target')
            handle = app._runtime_identity('target')[0]
            before = app._repository.state('target')
            observed = app._runtime_loop.run(app._status_runtime('target', handle))
            self.assertEqual(observed.status, before.invocation.status)
            self.assertEqual(observed.waits, [])
            self.assertEqual(sink.attempts, 1)
            self.assertIs(app._repository.state('target'), before)
        finally:
            app.close()

    def test_event_prefix_recovery_reuses_recorded_observation(self):
        """Every Status prefix recovers, and completed queries never reread a later target state."""
        from tests.test_child_recovery_integrity import _checkpoint_from_prefix
        from tests.graph_fixtures import load_graph
        workflow = Workflow('root', nodes=[Node('status', Status(), input_mapping=self_handle)])
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(workflow, {'value': 1}, session_id='root')
            self.assertEqual(result.status, 'completed', result)
            events = tuple(RuntimeEvent.from_record(e.to_record()) for e in sink.events)
        finally:
            app.close()
        start = next(i for i, e in enumerate(events) if e.event_name == 'operator_call.started')
        for index in range(start, len(events)):
            with self.subTest(cut=events[index].event_name):
                prefix = events[:index+1]
                completed = any(e.event_name == 'operator_call.completed' for e in prefix)
                recovery_sink = Collector()
                restored = AutoAgentApp(runtime_event_sink=recovery_sink)
                try:
                    restored.register_workflow(workflow)
                    ref = load_graph(restored, _checkpoint_from_prefix(prefix)).invocations[0]
                    if completed:
                        async def unexpected(*args):
                            raise AssertionError('completed query must not execute again')
                        restored._workflow_executor._status_runtime = unexpected
                    outcome = restored.recover(ref)
                    self.assertEqual(outcome.status, 'completed', outcome)
                    self.assertEqual(outcome.output.status, 'running')
                    self.assertFalse(any(e.event_name == 'operator_call.started' for e in recovery_sink.events))
                finally:
                    restored.close()

    def test_status_to_resume_uses_reported_wait(self):
        """A Status observation supplies the exact target and Wait needed by Resume."""
        from autoagent import Resume, ResumeRequest
        def response(context: InputMappingContext) -> ResumeRequest:
            observation = next(iter(context.incoming.values()))
            wait = observation['waits'][0]
            return ResumeRequest(handle=RuntimeHandle.model_validate(wait['handle']),
                wait_id=wait['wait_id'], response={'value': 12})
        child = Workflow('child', nodes=[Node('wait', Wait(Value, Value))])
        workflow = Workflow('root', nodes=[Node('await', Await(child, 'wait')),
            Node('status', Status(), input_mapping=observed_handle),
            Node('resume', Resume(), input_mapping=response),
            Node('result', Await(), input_mapping=observed_handle)],
            edges=[Edge('await', 'status'), Edge('status', 'resume'), Edge('resume', 'result')])
        app = AutoAgentApp()
        try:
            result = app.invoke(workflow, {'value': 1})
            self.assertEqual(result.status, 'completed', result)
            self.assertEqual(result.output.output, {'value': 12})
        finally:
            app.close()

    def test_lost_status_ack_retries_same_event_without_querying_again(self):
        """An unresolved Status result ACK is retried verbatim without replacing its snapshot."""
        class Sink(Collector):
            failed = None
            attempts = []
            async def append(self, event):
                if event.event_name == 'operator_call.completed':
                    self.attempts.append(event)
                    if self.failed is None:
                        self.failed = event
                        raise OSError('Status result ACK lost')
                await super().append(event)
        sink = Sink()
        app = AutoAgentApp(runtime_event_sink=sink)
        workflow = Workflow('root', nodes=[Node('status', Status(), input_mapping=self_handle)])
        try:
            with self.assertRaises(RuntimeInfrastructureError):
                app.invoke(workflow, {'value': 1}, session_id='root')
            async def unexpected(*args):
                raise AssertionError('pending result must settle without a new query')
            app._workflow_executor._status_runtime = unexpected
            handle = app._runtime_identity('root')[0]
            from autoagent import InvocationRef
            result = app.recover(InvocationRef(**handle.model_dump()))
            self.assertEqual(result.status, 'completed', result)
            self.assertEqual(result.output.status, 'running')
            self.assertEqual(len(sink.attempts), 2)
            self.assertIs(sink.attempts[0], sink.attempts[1])
        finally:
            app.close()

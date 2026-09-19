"""Differential checks for committed counts and Planner ACK boundaries."""
import random
import unittest
from dataclasses import replace
from types import MappingProxyType
from autoagent.core.runtime import TransitionPlanner, SchedulerDelta
from autoagent.core.runtime._execution_index import ExecutionIndex
from autoagent.core.runtime.events import WaitRequested, NodeCompleted, ChildAwaitSuspended, NodeFailed, RuntimeErrorInfo
from autoagent.core.runtime.scheduling import OccurrencePlan
from tests.benchmarks.benchmark_core_planner_topology import seed_state, retained_state


class PlannerIndexTests(unittest.TestCase):
    def test_random_statuses_produce_identical_deltas_without_mutating_index(self):
        """Indexed count decisions exactly match scans for completion and both waiting boundaries."""
        rng = random.Random(7)
        seed = seed_state()
        planner = TransitionPlanner()
        statuses = ['ready', 'running', 'waiting', 'completed', 'failed', 'skipped', 'cancelled']
        for _ in range(150):
            state = retained_state(seed, rng.randrange(2, 40))
            occurrences = {key: replace(value, status=rng.choice(statuses)) if key != '0@root' else value
                           for key, value in state.invocation.scheduler.occurrences.items()}
            state = replace(state, invocation=replace(state.invocation,
                scheduler=replace(state.invocation.scheduler, occurrences=MappingProxyType(occurrences))))
            index = ExecutionIndex(state)
            counts = dict(index.occurrence_counts)
            scheduler_delta = SchedulerDelta()
            if rng.random() < .3:
                scheduler_delta = SchedulerDelta(ready=(OccurrencePlan('new@root', 'new', ()),))
            for payload in (WaitRequested('0@root', 'wait', {}), NodeCompleted('0@root', {}),
                            NodeFailed('0@root', RuntimeErrorInfo('test', 'failure')),
                            ChildAwaitSuspended('p', '0@root')):
                kwargs = dict(occurred_at_us=123, session_id=state.session.id,
                              invocation_id=state.invocation.id, scheduler_delta=scheduler_delta)
                expected = planner.plan(state, payload, **kwargs)
                actual = planner.plan(state, payload, _execution_index=index, **kwargs)
                self.assertEqual(actual, expected)
                self.assertEqual(index.occurrence_counts, counts)

    def test_child_waiting_occurrence_does_not_require_wait_record(self):
        """Occurrence waiting counts stay distinct from user Wait record counts."""
        state = retained_state(seed_state(), 3)
        state = replace(state, invocation=replace(state.invocation,
            scheduler=replace(state.invocation.scheduler, waits=MappingProxyType({}))))
        index = ExecutionIndex(state)
        self.assertEqual(index.waiting_count, 0)
        planner = TransitionPlanner()
        delta = planner.plan(state, NodeCompleted('0@root', {}), occurred_at_us=1,
            session_id=state.session.id, invocation_id=state.invocation.id,
            scheduler_delta=SchedulerDelta(), _execution_index=index)
        self.assertTrue(any(op.path == ('invocation', 'status') and op.value == 'waiting' for op in delta.operations))

    def test_wait_index_remains_unpublished_until_ack_and_timestamp_delta_is_kept(self):
        """A failed indexed Wait commit preserves State/counts and the existing time Delta."""
        from autoagent import AutoAgentApp, Node, Wait, Workflow
        from autoagent.core.errors import RuntimeInfrastructureError
        from autoagent.core.runtime import RuntimeRepository
        from tests.benchmarks.benchmark_core_execution import Value
        class Sink:
            pending = None
            async def append(self, event):
                if isinstance(event.payload, WaitRequested):
                    if self.pending is None:
                        self.pending = event
                        raise OSError('lost ACK')
                    assert self.pending is event
        sink = Sink()
        repository = RuntimeRepository(sink=sink)
        app = AutoAgentApp(runtime_repository=repository)
        try:
            with self.assertRaises(RuntimeInfrastructureError):
                app.invoke(Workflow('indexed-wait', nodes=[Node('approval', Wait(Value, Value))]),
                           {'value': 1}, session_id='wait')
            before = repository.state('wait')
            index = repository.execution_index('wait')
            self.assertEqual(index.occurrence_counts.get('running'), 1)
            self.assertEqual(index.occurrence_counts.get('waiting', 0), 0)
            app._runtime_loop.run(repository.settle('wait'))
            self.assertEqual(index.occurrence_counts.get('waiting'), 1)
            self.assertEqual(before.invocation.status, 'running')
            self.assertEqual(repository.state('wait').session.updated_at_us, sink.pending.occurred_at_us)
            self.assertTrue(any(op.path == ('session', 'updated_at_us') for op in sink.pending.delta.operations))
        finally:
            app.close()

    def test_custom_planner_keeps_original_signature(self):
        """Repository does not require custom Planners to accept the private index argument."""
        from autoagent import AutoAgentApp, Node, Workflow
        from autoagent.core.runtime import RuntimeRepository
        from tests.benchmarks.benchmark_core_execution import identity
        class CustomPlanner(TransitionPlanner):
            def plan(self, state, payload, *, occurred_at_us, session_id=None,
                     invocation_id=None, scheduler_delta=None):
                return super().plan(state, payload, occurred_at_us=occurred_at_us,
                    session_id=session_id, invocation_id=invocation_id, scheduler_delta=scheduler_delta)
        app = AutoAgentApp(runtime_repository=RuntimeRepository(planner=CustomPlanner()))
        try:
            result = app.invoke(Workflow('custom', nodes=[Node('entry', identity)]), {'value': 1})
            self.assertEqual(result.status, 'completed', result.error)
        finally:
            app.close()

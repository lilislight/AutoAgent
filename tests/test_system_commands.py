"""Builtin commands share call identity and observe durable Child boundaries."""
import unittest
from pydantic import BaseModel, ConfigDict
from autoagent import AutoAgentApp, Workflow, Node, Edge, Spawn, Await, Wait, RuntimeHandle, RuntimeObservation, SystemCommand
from autoagent.core import RuntimeGraphCheckpoint


class Value(BaseModel):
    model_config = ConfigDict(extra="forbid")
    value: int


def identity(value: Value) -> Value:
    return value


class Collector:
    def __init__(self):
        self.events = []

    async def append(self, event):
        self.events.append(event)


class SystemCommandTests(unittest.TestCase):
    def test_definitions(self):
        """Builtin IDs are reserved and creating commands require explicit entries."""
        from autoagent import Operator
        with self.assertRaises(TypeError):
            SystemCommand()
        with self.assertRaises(ValueError):
            Await(Workflow('child', nodes=[Node('work', identity)]))
        with self.assertRaises(ValueError):
            Await(entry_node_id='work')
        with self.assertRaises(ValueError):
            Operator(identity, id='system_command:wait')
        self.assertEqual(Wait(Value, Value).id, 'system_command:wait')

    def test_spawn_and_handle_await(self):
        """Spawn returns a RuntimeHandle and Await observes its retained terminal result."""
        child = Workflow('child', nodes=[Node('work', identity)])
        workflow = Workflow('root', nodes=[Node('spawn', Spawn(child, 'work')), Node('await', Await())],
            edges=[Edge('spawn', 'await')])
        app = AutoAgentApp()
        try:
            result = app.invoke(workflow, {'value': 7})
            self.assertEqual(result.status, 'completed', result)
            self.assertIsInstance(result.output, RuntimeObservation)
            self.assertEqual(result.output.status, 'completed')
            self.assertEqual(result.output.output, {'value': 7})
            self.assertIsInstance(result.output.handle, RuntimeHandle)
        finally:
            app.close()

    def test_creating_await_returns_wait_and_recovers(self):
        """Await returns at external Wait and Root resume works after checkpoint reload."""
        child = Workflow('child', nodes=[Node('wait', Wait(Value, Value))])
        workflow = Workflow('root', nodes=[Node('await', Await(child, 'wait'))])
        app = AutoAgentApp()
        try:
            result = app.invoke(workflow, {'value': 3})
            self.assertEqual(result.status, 'settling', result)
            inv = app._repository.state(result.ref.session_id).invocation
            self.assertEqual(inv.output['status'], 'waiting')
            self.assertEqual(len(result.waits), 1)
            cp = app.unload_session(result.ref, capture_checkpoint=True)
            app.load_checkpoint(RuntimeGraphCheckpoint.from_record(cp.to_record()))
            recovered = app.recover(result.ref)
            final = app.resume(result.ref, recovered.waits[0].id, {'value': 9})
            self.assertEqual(final.status, 'completed', final)
            # Observation is the original boundary, not a subscription to later output.
            self.assertEqual(final.output.status, 'waiting')
        finally:
            app.close()

    def test_wait_shares_call_id(self):
        """Wait uses one call_id through suspend and resume, also used as wait_id."""
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(Workflow('root', nodes=[Node('wait', Wait(Value, Value))]), {'value': 1})
            starts = [e.payload for e in sink.events if e.payload.kind == 'operator_call.started']
            self.assertEqual(len(starts), 1)
            self.assertEqual(starts[0].call_id, result.waits[0].id)
            final = app.resume(result.ref, result.waits[0].id, {'value': 2})
            self.assertEqual(final.status, 'completed')
            ends = [e.payload for e in sink.events if e.payload.kind == 'operator_call.completed']
            self.assertEqual([e.call_id for e in ends], [starts[0].call_id])
        finally:
            app.close()

    def test_selected_entry_and_context_identities(self):
        """Commands choose one of multiple entries and hooks see self/owner identities."""
        from autoagent import InputMappingContext
        seen = []
        def chosen(context: InputMappingContext) -> Value:
            seen.append((context.self_handle, context.owner_handle))
            return Value(value=22)
        child = Workflow('multi', nodes=[Node('first', identity), Node('second', identity, input_mapping=chosen)])
        for command in (Spawn(child, 'second'), Await(child, 'second')):
            app = AutoAgentApp()
            try:
                result = app.invoke(Workflow('root', nodes=[Node('command', command)]), {'value': 1})
                self.assertEqual(result.status, 'completed', result)
                handle = result.output if isinstance(result.output, RuntimeHandle) else result.output.handle
                inv = app._repository.state(handle.session_id).invocation
                self.assertEqual(inv.entry_node_id, 'second')
                self.assertEqual(inv.output, {'second': {'value': 22}})
                self.assertEqual(seen[-1][0], handle)
                self.assertEqual(seen[-1][1].invocation_id, result.ref.invocation_id)
            finally:
                app.close()

    def test_failed_child_is_observation(self):
        """Await returns Child failure details instead of failing the Parent Node."""
        def broken(value: Value) -> Value:
            raise ValueError('child failed')
        app = AutoAgentApp()
        try:
            result = app.invoke(Workflow('root', nodes=[Node('await', Await(
                Workflow('child', nodes=[Node('broken', broken)]), 'broken'))]), {'value': 1})
            self.assertEqual(result.status, 'completed', result)
            self.assertEqual(result.output.status, 'failed')
            self.assertEqual(result.output.error['message'], 'child failed')
        finally:
            app.close()

    def test_relationship_validation(self):
        """Possessing a Handle never authorizes self, owner, sibling or forged Await."""
        from autoagent import Map, InputMappingContext, RuntimeTransitionError
        def batch(context: InputMappingContext) -> list[Value]:
            return [Value(value=1), Value(value=2)]
        app = AutoAgentApp()
        try:
            result = app.invoke(Workflow('root', nodes=[Node('spawn', Spawn(
                Workflow('child', nodes=[Node('work', identity)]), 'work'), map=Map(), input_mapping=batch)]), {'value': 1})
            self.assertEqual(result.status, 'completed', result)
            first, second = result.output
            root = RuntimeHandle(**result.ref.model_dump())
            for caller, target in ((first.session_id, first), (first.session_id, root),
                (first.session_id, second), (root.session_id, first.model_copy(update={'workflow_id': 'forged'})),
                (root.session_id, first.model_copy(update={'invocation_id': 'stale'}))):
                with self.subTest(caller=caller, target=target):
                    with self.assertRaises(RuntimeTransitionError):
                        app._validate_wait_target(caller, 'child', {'handle': target.model_dump(), 'after': None})
        finally:
            app.close()

    def test_map_await_order_and_empty(self):
        """Map Await keeps unit order and handles empty input without creating Children."""
        from autoagent import Map, InputMappingContext
        for values in ([1, 2, 3], []):
            def batch(context: InputMappingContext) -> list[Value]:
                return [Value(value=v) for v in values]
            app = AutoAgentApp()
            try:
                result = app.invoke(Workflow('root', nodes=[Node('await', Await(
                    Workflow('child', nodes=[Node('work', identity)]), 'work'), map=Map(), input_mapping=batch)]), {'value': 1})
                self.assertEqual(result.status, 'completed', result)
                self.assertEqual([x.output['value'] for x in result.output], values)
            finally:
                app.close()

    def test_command_event_prefix_recovery(self):
        """Recovering every durable prefix keeps command call IDs and Child identity."""
        from tests.test_child_recovery_integrity import _checkpoint_from_prefix
        from tests.graph_fixtures import load_graph
        from autoagent.core import RuntimeEvent
        child = Workflow('child', nodes=[Node('wait', Wait(Value, Value))])
        workflow = Workflow('root', nodes=[Node('await', Await(child, 'wait'))])
        sink = Collector()
        source = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = source.invoke(workflow, {'value': 3}, session_id='root')
            events = tuple(RuntimeEvent.from_record(e.to_record()) for e in sink.events)
        finally:
            source.close()
        # Test every root-command durable cut; child-only admission cuts may be incomplete graph bundles.
        for index, event in enumerate(events):
            if event.session_id != 'root' or event.event_name not in {
                'operator_call.started', 'child_invocation.planned', 'child_invocation.phase_changed',
                'operator_call.completed', 'node_occurrence.completed'}:
                continue
            with self.subTest(cut=event.event_name, index=index):
                cp = _checkpoint_from_prefix(events[:index+1])
                recovery_sink = Collector()
                restored = AutoAgentApp(runtime_event_sink=recovery_sink)
                try:
                    restored.register_workflow(workflow)
                    ref = load_graph(restored, cp).invocations[0]
                    outcome = restored.recover(ref)
                    self.assertEqual(outcome.status, 'settling', outcome)
                    self.assertEqual(len(outcome.waits), 1)
                    old_calls = [e.payload.call_id for e in events[:index+1]
                        if e.event_name == 'operator_call.started' and e.session_id == 'root']
                    new_calls = [e.payload.call_id for e in recovery_sink.events
                        if e.event_name == 'operator_call.started' and e.session_id == 'root']
                    self.assertEqual(new_calls, [])
                    root = restored._repository.state('root').invocation
                    self.assertEqual(len(root.child_plans), 1)
                    calls = list(root.scheduler.operator_calls)
                    self.assertEqual(calls, old_calls)
                finally:
                    restored.close()

    def test_nested_wait_boundary(self):
        """Await reports a settling Child and the descendant Wait that blocks it."""
        leaf = Workflow('leaf', nodes=[Node('wait', Wait(Value, Value))])
        child = Workflow('child', nodes=[Node('spawn', Spawn(leaf, 'wait'))])
        app = AutoAgentApp()
        try:
            result = app.invoke(Workflow('root', nodes=[Node('await', Await(child, 'spawn'))]), {'value': 4})
            self.assertEqual(result.status, 'settling', result)
            self.assertEqual(result.output.status, 'settling')
            self.assertEqual(result.output.pending_outcome, 'completed')
            self.assertEqual(result.output.waits[0].handle.workflow_id, 'leaf')
            self.assertEqual(result.output.waits[0].wait_id, result.waits[0].id)
            self.assertEqual(app.resume(result.ref, result.waits[0].id, {'value': 8}).status, 'completed')
        finally:
            app.close()

    def test_compiler_rejects_unregistered_commands_and_invalid_entries(self):
        """Only builtin Commands execute and entry identity affects the Workflow revision."""
        from autoagent import WorkflowCompileError
        class UserCommand(SystemCommand):
            id = 'system_command:custom'
        app = AutoAgentApp()
        try:
            with self.assertRaises(WorkflowCompileError):
                app.register_workflow(Workflow('bad', nodes=[Node('custom', UserCommand())]))
            child = Workflow('child', nodes=[Node('first', identity), Node('second', identity)])
            with self.assertRaises(WorkflowCompileError):
                app.register_workflow(Workflow('bad', nodes=[Node('spawn', Spawn(child, 'absent'))]))
            revisions = [app.register_workflow(Workflow('root', nodes=[Node('spawn', Spawn(child, entry))])).workflow_revision_id
                for entry in ('first', 'second')]
            self.assertNotEqual(*revisions)
        finally:
            app.close()

    def test_map_aggregation_gets_observations(self):
        """The command Map aggregator receives observations and runs once after unit calls."""
        from autoagent import Map, InputMappingContext, AggregationContext
        def inputs(context: InputMappingContext) -> list[Value]:
            return [Value(value=2), Value(value=3)]
        def aggregate(context: AggregationContext) -> Value:
            self.assertTrue(all(isinstance(v, RuntimeObservation) for v in context.outputs))
            self.assertIsNotNone(context.self_handle)
            return Value(value=sum(v.output['value'] for v in context.outputs))
        app = AutoAgentApp()
        try:
            result = app.invoke(Workflow('root', nodes=[Node('await', Await(
                Workflow('child', nodes=[Node('work', identity)]), 'work'),
                map=Map(aggregate=aggregate), input_mapping=inputs)]), {'value': 0})
            self.assertEqual(result.status, 'completed', result)
            self.assertEqual(result.output, {'value': 5})
        finally:
            app.close()

    def test_wait_event_prefix_preserves_call_identity(self):
        """Wait prefixes recover with the same call_id before and after response acceptance."""
        from tests.test_child_recovery_integrity import _checkpoint_from_prefix
        from tests.graph_fixtures import load_graph
        workflow = Workflow('root', nodes=[Node('wait', Wait(Value, Value))])
        sink = Collector()
        source = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = source.invoke(workflow, {'value': 3}, session_id='root')
            source.resume(result.ref, result.waits[0].id, {'value': 6})
            events = tuple(sink.events)
        finally:
            source.close()
        for index, event in enumerate(events):
            if event.event_name not in {'operator_call.started', 'wait.requested', 'wait.resumed', 'operator_call.completed'}:
                continue
            with self.subTest(cut=event.event_name):
                restored = AutoAgentApp()
                try:
                    restored.register_workflow(workflow)
                    ref = load_graph(restored, _checkpoint_from_prefix(events[:index+1])).invocations[0]
                    result = restored.recover(ref)
                    if result.waits:
                        self.assertEqual(result.waits[0].id, next(e.payload.call_id for e in events if e.event_name == 'operator_call.started'))
                        result = restored.resume(ref, result.waits[0].id, {'value': 6})
                    self.assertEqual(result.status, 'completed', result)
                    self.assertEqual(result.output, {'value': 6})
                    calls = restored._repository.state('root').invocation.scheduler.operator_calls
                    self.assertEqual(len(calls), 1)
                finally:
                    restored.close()

    def test_cancelled_child_observation(self):
        """A compact cancelled Child keeps its cancellation outcome readable by its owner."""
        app = AutoAgentApp()
        try:
            result = app.invoke(Workflow('root', nodes=[Node('spawn', Spawn(
                Workflow('child', nodes=[Node('wait', Wait(Value, Value))]), 'wait'))]), {'value': 1})
            handle = result.output
            app.cancel(result.ref, reason='stop graph')
            observed = app._runtime_loop.run(app._status_runtime(result.ref.session_id, handle))
            self.assertEqual(observed.status, 'cancelled')
            self.assertIsNotNone(observed.cancel_reason)
            self.assertEqual(observed.waits, [])
        finally:
            app.close()

    def test_command_failure_with_error_route_has_no_running_calls(self):
        """A rejected Await settles every Map call before a handled error completes the graph."""
        from autoagent import InputMappingContext, Map
        def targets(context: InputMappingContext) -> list[RuntimeHandle]:
            return [context.self_handle, context.self_handle]
        def handled_input(context: InputMappingContext) -> Value:
            return Value(value=9)
        app = AutoAgentApp()
        try:
            result = app.invoke(Workflow('root', nodes=[
                Node('await', Await(), map=Map(), input_mapping=targets),
                Node('handled', identity, input_mapping=handled_input),
            ], edges=[Edge('await', 'handled', on='error')]), {'value': 1})
            self.assertEqual(result.status, 'completed', result)
            calls = app._repository.state(result.ref.session_id).invocation.scheduler.operator_calls.values()
            self.assertFalse(any(c.status == 'running' for c in calls))
            self.assertIsNotNone(app.unload_session(result.ref, capture_checkpoint=True))
        finally:
            app.close()

    def test_removed_workflow_executable_is_rejected(self):
        """Bare Workflow executables and the removed execution_mode argument cannot compile."""
        from autoagent import WorkflowCompileError
        import autoagent
        from autoagent.core.workflow import NodeIR
        from dataclasses import fields
        child = Workflow('child', nodes=[Node('work', identity)])
        app = AutoAgentApp()
        try:
            with self.assertRaisesRegex(WorkflowCompileError, 'WORKFLOW_EXECUTABLE_UNSUPPORTED'):
                app.register_workflow(Workflow('root', nodes=[Node('child', child)]))
            with self.assertRaisesRegex(TypeError, 'execution_mode'):
                Node('child', Spawn(child, 'work'), execution_mode='spawn')
            self.assertNotIn('execution_mode', {item.name for item in fields(NodeIR)})
            self.assertFalse(hasattr(autoagent, 'ChildHandle'))
        finally:
            app.close()

    def test_command_plan_contains_entry_without_legacy_mode(self):
        """Command plans persist the exact entry and no retired await/spawn mode."""
        from autoagent.core import RuntimeEvent
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            result = app.invoke(Workflow('root', nodes=[Node('await', Await(
                Workflow('child', nodes=[Node('work', identity)]), 'work'))]), {'value': 7})
            event = next(e for e in sink.events if e.event_name == 'child_invocation.planned')
            record = event.to_record()
            self.assertEqual(record['payload']['entry_node_id'], 'work')
            self.assertNotIn('mode', record['payload'])
            self.assertEqual(RuntimeEvent.from_record(record).payload, event.payload)
            state = app._repository.state(result.session_id).to_record()
            plan = next(iter(state['invocation']['child_plans'].values()))
            self.assertNotIn('mode', plan)
            self.assertEqual(plan['entry_node_id'], 'work')
            self.assertFalse(any(e.event_name.startswith('child_await.') for e in sink.events))
        finally:
            app.close()

    def test_partial_map_command_recovery_reuses_children_and_call_ids(self):
        """A partial Map prefix retains remaining inputs and reuses completed Child observations."""
        from autoagent import InputMappingContext, Map, Recovery
        from tests.test_child_recovery_integrity import _checkpoint_from_prefix
        from tests.graph_fixtures import load_graph
        def inputs(context: InputMappingContext) -> list[Value]:
            return [Value(value=1), Value(value=2)]
        workflow = Workflow('root', nodes=[Node('await', Await(
            Workflow('child', nodes=[Node('work', identity, recovery_mode=Recovery('replay_safe'))]), 'work'),
            map=Map(), input_mapping=inputs)])
        sink = Collector()
        source = AutoAgentApp(runtime_event_sink=sink)
        try:
            source.invoke(workflow, {'value': 0}, session_id='root')
            cut = next(i for i, event in enumerate(sink.events)
                if event.session_id == 'root' and event.event_name == 'command.awakened')
            checkpoint = _checkpoint_from_prefix(tuple(sink.events[:cut + 1]))
            parent = next(s.state.invocation for g in checkpoint.graphs for s in g.sessions if s.session_id == 'root')
            plan = next(iter(parent.child_plans.values()))
            self.assertTrue(all(not unit.input_released for unit in plan.units))
            original_children = tuple(unit.session_id for unit in plan.units)
            original_calls = set(parent.scheduler.operator_calls)
        finally:
            source.close()
        recovered_sink = Collector()
        restored = AutoAgentApp(runtime_event_sink=recovered_sink)
        try:
            restored.register_workflow(workflow)
            ref = load_graph(restored, checkpoint).invocations[0]
            result = restored.recover(ref)
            self.assertEqual(result.status, 'completed', result)
            self.assertEqual([item.output for item in result.output], [{'value': 1}, {'value': 2}])
            parent = restored._repository.state('root').invocation
            self.assertEqual(set(parent.scheduler.operator_calls), original_calls)
            self.assertEqual(tuple(u.session_id for p in parent.child_plans.values() for u in p.units), original_children)
            self.assertFalse(any(e.event_name == 'child_invocation.planned' for e in recovered_sink.events))
        finally:
            restored.close()

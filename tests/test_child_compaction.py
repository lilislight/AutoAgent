"""Terminal Child results survive execution workspace reclamation and graph recovery."""
import asyncio
import json
import unittest
from dataclasses import replace
from typing_extensions import TypedDict

from autoagent import (AutoAgentApp, ChildHandle, Map, Node, Workflow, Wait, RuntimeInfrastructureError,
                       RuntimeGraphCheckpoint, AggregationContext, ContextPatch, ContextOperation, OutputBindingContext)
from autoagent.core import ChildResult, RuntimeState, StateReducer, RuntimeEvent, SessionCheckpoint
from tests.benchmarks.benchmark_core_audit import Value, identity, items
from tests.graph_fixtures import child_refs, resume_graph_wait, graph_bundle
from tests.test_runtime_graph import Collector


class AggregatedValues(TypedDict):
    items: list[Value]


class ChildCompactionTests(unittest.TestCase):
    def mapped_wait(self, mode):
        child = Workflow('child', nodes=[Node('wait', Wait(Value, Value))])
        def aggregate(ctx: AggregationContext) -> AggregatedValues:
            return {"items": [{'value': original['value'] + output['value']}
                    for original, output in zip(ctx.inputs, ctx.outputs)]}
        return Workflow('root', nodes=[Node('children', child, execution_mode=mode,
            input_mapping=items, map=Map(aggregate=aggregate if mode == 'await' else None, max_parallelism=4))])

    def test_spawn_keeps_waits_and_reclaims_finished_inputs(self):
        """Spawn results remain readable while unfinished siblings retain full Wait State."""
        app = AutoAgentApp()
        try:
            result = app.invoke(self.mapped_wait('spawn'), {'value': 3})
            refs = child_refs(app, result.ref)
            plan = next(iter(app._repository.state(result.session_id).invocation.child_plans.values()))
            self.assertTrue(all(u.input_released and u.input is None for u in plan.units))
            child = app._repository.state(refs[0].session_id).invocation
            self.assertNotIsInstance(child, ChildResult)
            wait = next(iter(child.scheduler.waits))
            resume_graph_wait(app, refs[0], wait, {'value': 9})
            compact = app._repository.state(refs[0].session_id).invocation
            self.assertIsInstance(compact, ChildResult)
            self.assertEqual(compact.output, {'value': 9})
            self.assertNotIn(refs[0].session_id, app._repository._execution_indexes)
            self.assertNotIsInstance(app._repository.state(refs[1].session_id).invocation, ChildResult)
            for _ in range(3):
                self.assertEqual(app._repository.state(refs[0].session_id).invocation.output, {'value': 9})
            checkpoint = app.unload_session(result.ref, capture_checkpoint=True)
            record = json.loads(json.dumps(checkpoint.to_record()))
            entry = next(s for s in record['sessions'] if s['session_id'] == refs[0].session_id)['state']['invocation']
            for name in ('input', 'context', 'scheduler'):
                self.assertNotIn(name, entry)
            app.load_checkpoint(RuntimeGraphCheckpoint.from_record(record))
            result = app.recover(result.ref)
            self.assertEqual(len(result.waits), 2)
            self.assertNotIn(refs[0].session_id, app._repository._execution_indexes)
            self.assertEqual(app._root_session_id(refs[0].session_id), result.session_id)
        finally:
            app.close()

    def test_await_map_preserves_unconsumed_inputs_and_compact_outputs(self):
        """A partial await Map keeps aggregation inputs while completed Child State shrinks."""
        app = AutoAgentApp()
        try:
            result = app.invoke(self.mapped_wait('await'), {'value': 3})
            refs = child_refs(app, result.ref)
            first = app._repository.state(refs[0].session_id).invocation
            resume_graph_wait(app, refs[0], next(iter(first.scheduler.waits)), {'value': 10})
            plan = next(iter(app._repository.state(result.session_id).invocation.child_plans.values()))
            self.assertTrue(all(not u.input_released for u in plan.units))
            self.assertIsInstance(app._repository.state(refs[0].session_id).invocation, ChildResult)
            cp = app.unload_session(result.ref, capture_checkpoint=True)
            app.load_checkpoint(RuntimeGraphCheckpoint.from_record(json.loads(json.dumps(cp.to_record()))))
            app.recover(result.ref)
            for i, ref in enumerate(refs[1:], 11):
                inv = app._repository.state(ref.session_id).invocation
                resume_graph_wait(app, ref, next(iter(inv.scheduler.waits)), {'value': i})
            final = app.join(result.ref, timeout=2)
            self.assertEqual(final.output, {'items': [{'value': 10}, {'value': 12}, {'value': 14}]})
            plan = next(iter(app._repository.state(result.session_id).invocation.child_plans.values()))
            self.assertTrue(all(u.input_released and u.input is None for u in plan.units))
        finally:
            app.close()

    def test_nested_results_keep_descendant_handle_ownership(self):
        """Compacting a Parent Child does not discard results addressed by nested Handles."""
        app = AutoAgentApp()
        try:
            leaf = Workflow('leaf', nodes=[Node('n', identity)])
            child = Workflow('child', nodes=[Node('spawn', leaf, execution_mode='spawn')])
            root = Workflow('root', nodes=[Node('spawn', child, execution_mode='spawn')])
            result = app.invoke(root, {'value': 7})
            child_state = app._repository.state(result.output.child_session_id).invocation
            self.assertIsInstance(child_state, ChildResult)
            handle = ChildHandle.model_validate(dict(child_state.output))
            leaf_result = app._repository.state(handle.child_session_id).invocation
            self.assertIsInstance(leaf_result, ChildResult)
            self.assertEqual(leaf_result.output, {'value': 7})
            checkpoint = app.unload_session(result.ref, capture_checkpoint=True)
            app.load_checkpoint(RuntimeGraphCheckpoint.from_record(checkpoint.to_record()))
            self.assertEqual(app._root_session_id(handle.child_session_id), result.session_id)
            self.assertEqual(app._repository.state(handle.child_session_id).invocation.output, {'value': 7})
            self.assertEqual(app.recover(result.ref).status, 'completed')
        finally:
            app.close()

    def test_compaction_ack_failure_preserves_full_state_and_retries(self):
        """A failed compact ACK retains full State and retries the identical compact event."""
        class Sink(Collector):
            fail = True
            async def append(self, event):
                await super().append(event)
                if self.fail and event.payload.kind == 'child_invocation.compacted':
                    raise OSError('lost ACK')
        sink = Sink()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            child = Workflow('child', nodes=[Node('n', identity)])
            root = Workflow('root', nodes=[Node('child', child)])
            with self.assertRaises(RuntimeInfrastructureError):
                app.invoke(root, {'value': 3}, session_id='root')
            ref = app.resident_invocations()[0]
            sid = next(s for s in app._repository.session_ids() if s != 'root')
            self.assertNotIsInstance(app._repository.state(sid).invocation, ChildResult)
            self.assertEqual(app._repository.state(sid).invocation.input, {'value': 3})
            sink.fail = False
            self.assertEqual(app.recover(ref).output, {'value': 3})
            events = [e for e in sink.events if e.payload.kind == 'child_invocation.compacted']
            self.assertGreaterEqual(len(events), 2)
            self.assertEqual(len({e.id for e in events}), 1)
            self.assertIsInstance(app._repository.state(sid).invocation, ChildResult)
        finally:
            sink.fail = False
            app.close()

    def test_compaction_event_prefixes_recover_without_repeating_child(self):
        """Both sides of the compact ACK reconstruct results without rerunning the Child."""
        calls = 0
        def work(value: Value) -> Value:
            nonlocal calls
            calls += 1
            return value
        root = Workflow('root', nodes=[Node('child', Workflow('child', nodes=[Node('n', work)]))])
        sink = Collector()
        source = AutoAgentApp(runtime_event_sink=sink)
        try:
            source.invoke(root, {'value': 4}, session_id='root')
        finally:
            source.close()
        states, prefixes = {}, []
        for event in sink.events:
            event = RuntimeEvent.from_record(json.loads(json.dumps(event.to_record())))
            states[event.session_id] = StateReducer().apply(states.get(event.session_id, RuntimeState()), event)
            if event.payload.kind in {'child_invocation.compacted', 'invocation.completed'} and event.session_id != 'root':
                prefixes.append(dict(states))
        self.assertEqual(len(prefixes), 2)
        for prefix in prefixes:
            app = AutoAgentApp()
            try:
                app.register_workflow(root)
                checkpoint = graph_bundle(tuple(SessionCheckpoint.from_state(s) for s in prefix.values()))
                ref = app.load_checkpoint(checkpoint).invocations[0]
                self.assertEqual(app.recover(ref).output, {'value': 4})
                self.assertEqual(calls, 1)
            finally:
                app.close()

    def test_compacted_checkpoint_rejects_missing_result_and_forged_owner(self):
        """Compact results remain mandatory and cannot be transferred to a different owner."""
        app = AutoAgentApp()
        try:
            r = app.invoke(Workflow('root', nodes=[Node('child', Workflow('child', nodes=[Node('n', identity)]))]), {'value': 1})
            cp = app.unload_session(r.ref, capture_checkpoint=True)
            parent = next(s for s in cp.sessions if s.session_id == cp.root_session_id)
            child = next(s for s in cp.sessions if s.session_id != cp.root_session_id)
            with self.assertRaises(ValueError):
                RuntimeGraphCheckpoint(cp.root_session_id, (parent,))
            with self.assertRaises(ValueError):
                RuntimeGraphCheckpoint(child.session_id, (child,))
            forged = SessionCheckpoint.from_state(replace(child.state, invocation=replace(
                child.state.invocation, parent_invocation_id='wrong')))
            with self.assertRaises(ValueError):
                RuntimeGraphCheckpoint(cp.root_session_id, (parent, forged))
        finally:
            app.close()

    def test_compaction_releases_contexts_without_copying_final_output(self):
        """Session and Invocation Context disappear while the terminal output stays shared."""
        def bind(ctx: OutputBindingContext) -> ContextPatch:
            return ContextPatch(
                invocation=(ContextOperation.set('large', 'x' * 1024 * 1024),),
                session=(ContextOperation.set('large', 'y' * 1024 * 1024),))
        sink = Collector()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            child = Workflow('context-child', nodes=[Node('n', identity, output_binding=bind)])
            r = app.invoke(Workflow('context-root', nodes=[Node('spawn', child, execution_mode='spawn')]), {'value': 1})
            sid = r.output.child_session_id
            result = app._repository.state(sid)
            self.assertIsInstance(result.invocation, ChildResult)
            self.assertEqual(dict(result.session.context), {})
            before = RuntimeState()
            for event in sink.events:
                if event.session_id != sid:
                    continue
                if event.payload.kind == 'child_invocation.compacted':
                    self.assertIn('large', before.session.context)
                    self.assertIn('large', before.invocation.context)
                    self.assertIs(result.invocation.output, before.invocation.output)
                    break
                before = StateReducer().apply(before, event)
            else:
                self.fail('Missing compaction boundary')
        finally:
            app.close()

    def test_partial_await_result_rejects_forged_input(self):
        """The compact admission digest validates inputs still needed by an await Node."""
        app = AutoAgentApp()
        try:
            r = app.invoke(self.mapped_wait('await'), {'value': 2})
            refs = child_refs(app, r.ref)
            first = app._repository.state(refs[0].session_id).invocation
            resume_graph_wait(app, refs[0], next(iter(first.scheduler.waits)), {'value': 5})
            cp = app.unload_session(r.ref, capture_checkpoint=True)
            parent = next(s for s in cp.sessions if s.session_id == cp.root_session_id)
            inv = parent.state.invocation
            plan = next(iter(inv.child_plans.values()))
            units = tuple(replace(u, input={'value': 999}) if u.unit_index == 0 else u for u in plan.units)
            forged = SessionCheckpoint.from_state(replace(parent.state, invocation=replace(
                inv, child_plans={plan.creation_id: replace(plan, units=units)})))
            with self.assertRaisesRegex(ValueError, 'input does not match'):
                RuntimeGraphCheckpoint(cp.root_session_id, tuple(
                    forged if s.session_id == cp.root_session_id else s for s in cp.sessions))
        finally:
            app.close()

    def test_await_child_settling_on_descendant_wait_remains_resumable(self):
        """Await treats successful Child settling as pending, then consumes its compact result."""
        app = AutoAgentApp()
        try:
            leaf = Workflow('wait-leaf', nodes=[Node('wait', Wait(Value, Value))])
            child = Workflow('spawn-child', nodes=[Node('spawn', leaf, execution_mode='spawn')])
            root = Workflow('await-root', nodes=[Node('await', child)])
            result = app.invoke(root, {'value': 1})
            self.assertEqual(result.status, 'waiting')
            self.assertEqual(len(result.waits), 1)
            child_ref = child_refs(app, result.ref)[0]
            pending = app._repository.state(child_ref.session_id).invocation
            self.assertEqual(pending.status, 'settling')
            self.assertNotIsInstance(pending, ChildResult)
            cp = app.unload_session(result.ref, capture_checkpoint=True)
            app.load_checkpoint(RuntimeGraphCheckpoint.from_record(cp.to_record()))
            waiting = app.recover(result.ref)
            final = app.resume(result.ref, waiting.waits[0].id, {'value': 9})
            self.assertEqual(final.status, 'completed')
            self.assertIsInstance(app._repository.state(child_ref.session_id).invocation, ChildResult)
            self.assertIsInstance(final.output, ChildHandle)
            self.assertEqual(app._repository.state(final.output.child_session_id).invocation.output, {'value': 9})
        finally:
            app.close()

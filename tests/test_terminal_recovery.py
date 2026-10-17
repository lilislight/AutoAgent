"""Unified in-place recovery of failed, cancelled and interrupted execution graphs."""
import unittest
from collections import Counter
from typing_extensions import TypedDict

from autoagent import AutoAgentApp, Node, Edge, Workflow, RecoverySource, Recovery, Map, InputMappingContext
from autoagent.core.runtime.reducer import StateReducer


class Value(TypedDict):
    value: int


class Events:
    def __init__(self):
        self.events = []
    async def append(self, event):
        if all(old.id != event.id for old in self.events):
            self.events.append(event)


class TerminalRecoveryTests(unittest.TestCase):
    def test_failed_node_recovers_in_place_without_replaying_success(self):
        """A failed Node gets a new occurrence while the successful prefix and Invocation identity survive."""
        calls = Counter()
        def first(value: Value) -> Value:
            calls['first'] += 1
            return value
        def flaky(value: Value) -> Value:
            calls['flaky'] += 1
            if calls['flaky'] == 1:
                raise ValueError('temporary')
            return {'value':value['value']+1}
        def last(value: Value) -> Value:
            calls['last'] += 1
            return value
        wf = Workflow('recover-terminal', nodes=[Node('a',first),Node('b',flaky),Node('c',last)],edges=[Edge('a','b'),Edge('b','c')])
        sink = Events()
        app = AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close, timeout=2)
        failed = app.invoke(wf,{'value':1},entry_node_id='a')
        self.assertEqual(failed.status,'failed')
        source = RecoverySource.from_events(tuple(sink.events))
        recovered = app.recover(failed.ref,source=source,request_id='recover-once',expected_version=source.states[failed.session_id].sequence)
        self.assertEqual(recovered.status,'completed')
        self.assertEqual(recovered.ref,failed.ref)
        self.assertEqual(recovered.output,{'value':2})
        self.assertEqual(calls,{'first':1,'flaky':2,'last':1})
        duplicate = app.recover(failed.ref,source=source,request_id='recover-once')
        self.assertEqual(duplicate.status,'completed')
        self.assertEqual(calls['flaky'],2)
        state = StateReducer().reduce(tuple(e for e in sink.events if e.session_id==failed.session_id))
        self.assertEqual(state.invocation.recovery_count,1)
        self.assertIsNone(state.invocation.pending_recovery)
        self.assertTrue(any('~recover:1' in oid for oid in state.invocation.scheduler.occurrences))

    def test_map_recovers_only_failed_units_and_preserves_successful_results(self):
        """Accepted Map Unit outputs are reused without inventing another Operator Call."""
        calls = Counter()
        def mapped(ctx: InputMappingContext) -> list[Value]:
            return [{'value':i} for i in range(4)]
        def unit(value: Value) -> Value:
            index=value['value']; calls[index]+=1
            if index==2 and calls[index]==1:
                raise ValueError('unit failed')
            return value
        sink=Events(); app=AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close,timeout=2)
        failed=app.invoke(Workflow('recover-map',nodes=[Node('map',unit,input_mapping=mapped,map=Map(max_parallelism=1))]),{'value':0})
        self.assertEqual(failed.status,'failed')
        recovered=app.recover(failed.ref,source=RecoverySource.from_events(tuple(sink.events)))
        self.assertEqual(recovered.status,'completed')
        self.assertEqual(calls[0],1); self.assertEqual(calls[1],1); self.assertEqual(calls[2],2)
        self.assertEqual(recovered.output,[{'value':i} for i in range(4)])

    def test_cancelled_wait_recovers_with_new_wait_identity_and_original_invocation(self):
        """Recovering a cancelled external Wait creates a fresh response target in the same Invocation."""
        from autoagent import Wait
        sink=Events(); app=AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close,timeout=2)
        waiting=app.invoke(Workflow('recover-wait',nodes=[Node('approval',Wait(Value,Value))]),{'value':3})
        old=waiting.waits[0].id
        app.cancel(waiting.ref,'stop')
        restored=app.recover(waiting.ref,source=RecoverySource.from_events(tuple(sink.events)))
        self.assertEqual(restored.status,'waiting')
        self.assertEqual(restored.ref,waiting.ref)
        self.assertNotEqual(restored.waits[0].id,old)
        result=app.resume(restored.ref,restored.waits[0].id,{'value':4})
        self.assertEqual(result.output,{'value':4})

    def test_failed_await_child_and_parent_recover_without_new_child_identity(self):
        """A Parent's failed Await and compacted Child are jointly reopened under the original ownership."""
        from autoagent import Await
        calls=Counter()
        def flaky(value:Value)->Value:
            calls['child']+=1
            if calls['child']==1:raise ValueError('child failed')
            return value
        child=Workflow('recover-child',nodes=[Node('work',flaky)])
        from autoagent.core.commands.models import RuntimeObservation
        def check(value:RuntimeObservation)->Value:
            if value.status != 'completed':
                raise ValueError('child unavailable')
            return value.output
        parent=Workflow('recover-parent',nodes=[Node('child',Await(child,'work')),Node('check',check)],edges=[Edge('child','check')])
        sink=Events(); app=AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close,timeout=2)
        failed=app.invoke(parent,{'value':7})
        self.assertEqual(failed.status,'failed')
        before=app._repository.state(failed.session_id).invocation
        ids=[u.invocation_id for p in before.child_plans.values() for u in p.units]
        result=app.recover(failed.ref,source=RecoverySource.from_events(tuple(sink.events)))
        self.assertEqual(result.status,'completed')
        self.assertEqual(result.output,{'value':7})
        after=app._repository.state(failed.session_id).invocation
        self.assertEqual(ids,[u.invocation_id for p in after.child_plans.values() for u in p.units])
        self.assertEqual(calls['child'],2)

    def test_never_policy_and_stale_version_reject_before_any_new_work(self):
        """Policy and compare-and-set failures leave terminal state and side effects untouched."""
        def bad(value:Value)->Value:
            raise ValueError('failure')
        sink=Events(); app=AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close,timeout=2)
        failed=app.invoke(Workflow('recover-never',nodes=[Node('bad',bad,recovery_mode=Recovery('never'))]),{'value':0})
        source=RecoverySource.from_events(tuple(sink.events))
        before=len(sink.events)
        from autoagent import RuntimeTransitionError
        with self.assertRaisesRegex(RuntimeTransitionError,'changed'):
            app.recover(failed.ref,source=source,expected_version=0)
        with self.assertRaisesRegex(RuntimeTransitionError,'recomputation'):
            app.recover(failed.ref,source=source)
        self.assertEqual(len(sink.events),before)

    def test_cancelled_parallel_children_restore_same_tree_and_new_waits(self):
        """A cancelled Spawn Map reuses all Child IDs and recovers twenty external Waits."""
        from autoagent import Spawn, Wait
        def units(ctx:InputMappingContext)->list[Value]:
            return [{'value':i} for i in range(20)]
        child=Workflow('recover-parallel-child',nodes=[Node('ask',Wait(Value,Value))])
        parent=Workflow('recover-parallel-root',nodes=[Node('children',Spawn(child,'ask'),input_mapping=units,map=Map())])
        sink=Events(); app=AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close,timeout=2)
        waiting=app.invoke(parent,{'value':0})
        before={w.id for w in waiting.waits}
        ids=set(app._repository.session_ids())
        app.cancel(waiting.ref,'stop all')
        restored=app.recover(waiting.ref,source=RecoverySource.from_events(tuple(sink.events)))
        self.assertIn(restored.status,('waiting','settling'))
        self.assertEqual(set(app._repository.session_ids()),ids)
        self.assertEqual(len(restored.waits),20)
        self.assertFalse(before & {w.id for w in restored.waits})
        for wait in restored.waits:
            app.resume(restored.ref,wait.id,{'value':1})
        self.assertEqual(app.status(restored.ref).status,'completed')

    def test_partial_graph_preparation_survives_lost_ack_and_process_replacement(self):
        """A persisted Root plan finishes after a Child reopen ACK is lost, before running any Operator."""
        from autoagent import Spawn, Wait, RuntimeInfrastructureError
        from autoagent.core import SessionCheckpoint
        from tests.graph_fixtures import graph_bundle, load_graph
        class Broken(Events):
            broken=False
            async def append(self,event):
                await super().append(event)
                if self.broken and event.event_name=='invocation.reopened':
                    raise RuntimeError('lost recovery ACK')
        child=Workflow('recover-crash-child',nodes=[Node('ask',Wait(Value,Value))])
        parent=Workflow('recover-crash-parent',nodes=[Node('spawn',Spawn(child,'ask'))])
        sink=Broken(); app=AutoAgentApp(runtime_event_sink=sink)
        waiting=app.invoke(parent,{'value':1})
        app.cancel(waiting.ref,'stop')
        source=RecoverySource.from_events(tuple(sink.events)); sink.broken=True
        with self.assertRaises(RuntimeInfrastructureError):
            app.recover(waiting.ref,source=source,request_id='crash-recovery',expected_version=source.states[waiting.session_id].sequence)
        prefix=tuple(sink.events)
        sink.broken=False
        app.close(timeout=2)
        states=RecoverySource.from_events(prefix).states
        checkpoint=graph_bundle(tuple(SessionCheckpoint.from_state(s) for s in states.values()))
        target=AutoAgentApp()
        self.addCleanup(target.close,timeout=2)
        target.register_workflow(parent)
        load_graph(target,checkpoint)
        recovered=target.recover(waiting.ref,request_id='crash-recovery',expected_version=source.states[waiting.session_id].sequence)
        self.assertEqual(recovered.ref,waiting.ref)
        self.assertEqual(len(recovered.waits),1)
        self.assertEqual(target._repository.state(waiting.session_id).invocation.recovery_count,1)

    def test_loop_failure_recovers_current_iteration_without_repeating_earlier_iterations(self):
        """A Loop reopens its failed scope and continues through subsequent iterations."""
        from autoagent import ConditionContext
        calls=Counter()
        def identity(value:Value)->Value:
            return value
        def step(value:Value)->Value:
            calls[value['value']]+=1
            if value['value']==1 and calls[1]==1:
                raise ValueError('second iteration failed')
            return {'value':value['value']+1}
        def more(ctx:ConditionContext)->bool:
            return ctx.output['value']<3
        def done(ctx:ConditionContext)->bool:
            return ctx.output['value']>=3
        wf=Workflow('recover-loop',nodes=[Node('start',identity),Node('step',step),Node('finish',identity)],edges=[Edge('start','step'),Edge('step','step',more),Edge('step','finish',done)])
        sink=Events(); app=AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close,timeout=2)
        failed=app.invoke(wf,{'value':0},entry_node_id='start')
        self.assertEqual(failed.status,'failed')
        result=app.recover(failed.ref,source=RecoverySource.from_events(tuple(sink.events)))
        self.assertEqual(result.status,'completed')
        self.assertEqual(result.output,{'value':3})
        self.assertEqual(calls,{0:1,1:2,2:1})

    def test_repeated_failed_recovery_and_recovery_after_unload_keep_distinct_attempts(self):
        """Repeated recovery survives Checkpoint round trips without resetting identities or redoing the prefix."""
        calls=Counter()
        def first(value:Value)->Value:
            calls['first']+=1
            return value
        def flaky(value:Value)->Value:
            calls['flaky']+=1
            if calls['flaky']<3:
                raise ValueError('again')
            return value
        wf=Workflow('recover-twice',nodes=[Node('first',first),Node('flaky',flaky)],edges=[Edge('first','flaky')])
        sink=Events(); app=AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close,timeout=2)
        failed=app.invoke(wf,{'value':1},entry_node_id='first')
        again=app.recover(failed.ref,source=RecoverySource.from_events(tuple(sink.events)),request_id='attempt-one')
        self.assertEqual(again.status,'failed')
        checkpoint=app.unload_session(failed.ref,capture_checkpoint=True)
        app.load_checkpoint(checkpoint)
        final=app.recover(failed.ref,source=RecoverySource.from_events(tuple(sink.events)),request_id='attempt-two')
        self.assertEqual(final.status,'completed')
        self.assertEqual(calls,{'first':1,'flaky':3})
        self.assertEqual(app._repository.state(failed.session_id).invocation.recovery_count,2)

    def test_await_map_refreshes_failed_child_units_and_reuses_successful_siblings(self):
        """A mapped Await refreshes only reopened Child observations before retrying its aggregation."""
        from autoagent import Await, AggregationContext
        calls=Counter()
        def units(ctx:InputMappingContext)->list[Value]:
            return [{'value':i} for i in range(3)]
        def work(value:Value)->Value:
            calls[value['value']]+=1
            if value['value']==1 and calls[1]==1:
                raise ValueError('child transient')
            return value
        def aggregate(ctx:AggregationContext)->Value:
            if any(item.status!='completed' for item in ctx.outputs):
                raise ValueError('one child failed')
            return {'value':sum(item.output['value'] for item in ctx.outputs)}
        child=Workflow('recover-map-child',nodes=[Node('work',work)])
        parent=Workflow('recover-map-parent',nodes=[Node('children',Await(child,'work'),input_mapping=units,map=Map(aggregate=aggregate,max_parallelism=1))])
        sink=Events(); app=AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close,timeout=2)
        failed=app.invoke(parent,{'value':0})
        self.assertEqual(failed.status,'failed')
        result=app.recover(failed.ref,source=RecoverySource.from_events(tuple(sink.events)))
        self.assertEqual(result.status,'completed')
        self.assertEqual(result.output,{'value':3})
        self.assertEqual(calls,{0:1,1:2,2:1})

    def test_completed_sibling_observation_is_retained_when_await_refreshes(self):
        """Refreshing an Await preserves a completed sibling's old activation proof and committed work."""
        from autoagent import Await
        from autoagent.core.commands.models import RuntimeObservation
        import asyncio
        calls=Counter()
        def work(value:Value)->Value:
            calls['child']+=1
            if calls['child']==1:
                raise ValueError('transient')
            return value
        def log(value:RuntimeObservation)->Value:
            calls['log']+=1
            return {'value':0}
        async def check(value:RuntimeObservation)->Value:
            await asyncio.sleep(.01)
            if value.status!='completed':
                raise ValueError('child failed')
            return value.output
        child=Workflow('recover-branch-child',nodes=[Node('work',work)])
        parent=Workflow('recover-branch-parent',nodes=[Node('child',Await(child,'work')),Node('log',log),Node('check',check)],edges=[Edge('child','log'),Edge('child','check')])
        sink=Events(); app=AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close,timeout=2)
        failed=app.invoke(parent,{'value':2},entry_node_id='child')
        self.assertEqual(failed.status,'failed')
        result=app.recover(failed.ref,source=RecoverySource.from_events(tuple(sink.events)))
        self.assertEqual(result.status,'completed')
        self.assertEqual(calls,{'child':2,'log':1})
        checkpoint=app.unload_session(failed.ref,capture_checkpoint=True)
        app.load_checkpoint(checkpoint)
        self.assertEqual(app.status(failed.ref).status,'completed')

    def test_three_level_await_tree_recovers_leaf_before_recomputing_parent_observations(self):
        """Nested compacted Parents and a failed grandchild reopen together with unchanged identities."""
        from autoagent import Await
        from autoagent.core.commands.models import RuntimeObservation
        calls=Counter()
        def work(value:Value)->Value:
            calls['leaf']+=1
            if calls['leaf']==1:
                raise ValueError('temporary leaf')
            return value
        def output(value:RuntimeObservation)->Value:
            if value.status!='completed':
                raise ValueError('child failed')
            return value.output
        leaf=Workflow('recover-grandchild',nodes=[Node('work',work)])
        middle=Workflow('recover-middle',nodes=[Node('leaf',Await(leaf,'work')),Node('check',output)],edges=[Edge('leaf','check')])
        root=Workflow('recover-tree',nodes=[Node('middle',Await(middle,'leaf')),Node('check',output)],edges=[Edge('middle','check')])
        sink=Events(); app=AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close,timeout=2)
        failed=app.invoke(root,{'value':9},entry_node_id='middle')
        identities=set(app._repository.session_ids())
        self.assertEqual(failed.status,'failed')
        result=app.recover(failed.ref,source=RecoverySource.from_events(tuple(sink.events)))
        self.assertEqual(result.status,'completed')
        self.assertEqual(result.output,{'value':9})
        self.assertEqual(set(app._repository.session_ids()),identities)
        self.assertEqual(calls['leaf'],2)

    def test_cancelled_timer_keeps_original_absolute_deadline(self):
        """Recovering a Timer does not turn its relative delay into another full wait."""
        from autoagent import Timer, TimerRequest
        now=[100]
        sink=Events(); app=AutoAgentApp(runtime_event_sink=sink,clock_us=lambda:now[0])
        self.addCleanup(app.close,timeout=2)
        waiting=app.invoke(Workflow('recover-timer',nodes=[Node('timer',Timer())]),TimerRequest(delay_us=1_000_000).model_dump())
        deadline=app.suspension_info(waiting.ref).next_deadline_at_us
        app.cancel(waiting.ref,'stop')
        now[0]=200
        app.recover(waiting.ref,source=RecoverySource.from_events(tuple(sink.events)))
        self.assertEqual(app.suspension_info(waiting.ref).next_deadline_at_us,deadline)
        self.assertTrue(all(event.occurred_at_us in (100,200) for event in sink.events))

    def test_cancelled_signal_wait_can_receive_new_messages_after_recovery(self):
        """Recovering an internal Signal Wait restores the mailbox endpoint without restoring cancelled messages."""
        from autoagent import AwaitSignal, SignalEndpoint
        import time
        sink=Events(); app=AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close,timeout=2)
        wf=Workflow('recover-signal',nodes=[Node('message',AwaitSignal('feedback'))],signal_endpoints=[SignalEndpoint('feedback',Value)])
        waiting=app.invoke(wf,{})
        app.cancel(waiting.ref,'stop')
        restored=app.recover(waiting.ref,source=RecoverySource.from_events(tuple(sink.events)))
        self.assertEqual(restored.status,'waiting')
        app.signal(waiting.ref,'feedback',{'value':4},source_id='browser',sequence=1)
        for _ in range(200):
            result=app.status(waiting.ref)
            if result.status=='completed':break
            time.sleep(.005)
        self.assertEqual(result.status,'completed')
        self.assertEqual(result.output.messages[0].payload,{'value':4})

    def test_completed_parallel_waits_are_not_requested_again(self):
        """Parallel Wait recovery keeps accepted responses and replaces only outstanding Wait IDs."""
        from autoagent import Wait, RuntimeTransitionError
        def start(value:Value)->Value:
            return value
        sink=Events(); app=AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close,timeout=2)
        wf=Workflow('recover-parallel-waits',nodes=[Node('start',start),*[Node(f'ask{i}',Wait(Value,Value)) for i in range(3)]],edges=[Edge('start',f'ask{i}') for i in range(3)])
        initial=app.invoke(wf,{'value':0},entry_node_id='start')
        first=app.resume(initial.ref,initial.waits[0].id,{'value':10})
        old_ids={w.id for w in first.waits}
        app.cancel(initial.ref,'stop remaining')
        recovered=app.recover(initial.ref,source=RecoverySource.from_events(tuple(sink.events)))
        self.assertEqual(len(recovered.waits),2)
        self.assertFalse(old_ids & {w.id for w in recovered.waits})
        with self.assertRaises(RuntimeTransitionError):
            app.resume(initial.ref,initial.waits[0].id,{'value':999})
        for wait in recovered.waits:
            result=app.resume(initial.ref,wait.id,{'value':20})
        self.assertEqual(result.status,'completed')
        self.assertIn({'value':10},result.output.values())

    def test_stale_or_incomplete_history_never_reopens_an_execution(self):
        """Canonical history must be contiguous and match every acknowledged Session frontier."""
        from autoagent import RuntimeTransitionError
        def work(value:Value)->Value:
            raise ValueError('failure')
        sink=Events(); app=AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close,timeout=2)
        failed=app.invoke(Workflow('recover-stale',nodes=[Node('work',work)]),{'value':0})
        count=len(sink.events)
        source=RecoverySource.from_events(tuple(sink.events[:-1]))
        with self.assertRaisesRegex(RuntimeTransitionError,'does not match'):
            app.recover(failed.ref,source=source)
        with self.assertRaises(RuntimeTransitionError):
            RecoverySource.from_events(tuple(sink.events[1:]))
        self.assertEqual(len(sink.events),count)

    def test_lost_root_reopen_ack_continues_the_accepted_round(self):
        """Retrying a lost final reopen ACK starts prepared work once without deadlocking Root admission."""
        from autoagent import RuntimeInfrastructureError
        class Broken(Events):
            broken=False
            async def append(self,event):
                await super().append(event)
                if self.broken and event.event_name=='invocation.reopened':
                    raise RuntimeError('Root reopen ACK lost')
        calls=Counter()
        def work(value:Value)->Value:
            calls['work']+=1
            if calls['work']==1:raise ValueError('transient')
            return value
        sink=Broken(); app=AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close,timeout=2)
        initial=app.invoke(Workflow('recover-root-ack',nodes=[Node('work',work)]),{'value':3})
        source=RecoverySource.from_events(tuple(sink.events))
        sink.broken=True
        with self.assertRaises(RuntimeInfrastructureError):
            app.recover(initial.ref,source=source,request_id='root-ack')
        self.assertEqual(calls['work'],1)
        sink.broken=False
        result=app.recover(initial.ref,request_id='root-ack')
        self.assertEqual(result.status,'completed')
        self.assertEqual(calls['work'],2)
        self.assertEqual(app._repository.state(initial.session_id).invocation.recovery_count,1)

    def test_duplicate_accepted_request_does_not_restart_a_later_interruption(self):
        """After dispatch begins, repeating its old request ID observes state; a new request recovers interruption."""
        from autoagent import RuntimeInfrastructureError
        class Broken(Events):
            broken=False
            async def append(self,event):
                await super().append(event)
                if self.broken and event.event_name=='operator_call.started':
                    raise RuntimeError('interrupted after recovery dispatch')
        calls=Counter()
        def work(value:Value)->Value:
            calls['work']+=1
            if calls['work']==1:raise ValueError('transient')
            return value
        sink=Broken(); app=AutoAgentApp(runtime_event_sink=sink)
        self.addCleanup(app.close,timeout=2)
        failed=app.invoke(Workflow('recover-dispatched-request',nodes=[Node('work',work)]),{'value':3})
        source=RecoverySource.from_events(tuple(sink.events)); sink.broken=True
        with self.assertRaises(RuntimeInfrastructureError):
            app.recover(failed.ref,source=source,request_id='accepted-request')
        sink.broken=False
        version=app._repository.state(failed.session_id).sequence
        duplicate=app.recover(failed.ref,request_id='accepted-request')
        self.assertEqual(duplicate.status,'running')
        self.assertEqual(app._repository.state(failed.session_id).sequence,version)
        self.assertEqual(calls['work'],1)
        recovered=app.recover(failed.ref,request_id='recover-new-interruption')
        self.assertEqual(recovered.status,'completed')
        self.assertEqual(calls['work'],2)

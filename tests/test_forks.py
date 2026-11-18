"""Historical Root branching, including independently owned Child waits."""
import asyncio
from dataclasses import replace
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from typing_extensions import TypedDict
from autoagent import Await, Edge, Node, Spawn, Wait, Workflow, Map, Recovery, InputMappingContext, ConditionContext
from autoagent import (
    SignalEndpoint,
    ReceiveSignal,
    SendSignal,
    OwnerHandle,
    UserEventContext,
    UserEventSpec,
)
from autoagent.core.app import AutoAgentApp
from autoagent.host import AutoAgentHost, HostSettings, HostRunConflict
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase


class Value(TypedDict):
    text: str


def echo(value: Value) -> Value:
    return value


def observed(ctx: UserEventContext) -> UserEventSpec | None:
    if ctx.phase != "output":
        return None
    return UserEventSpec(kind=ctx.node_id, payload=ctx.value)


def selected(ctx: ConditionContext) -> bool:
    return True


def two_children(ctx: InputMappingContext) -> list[Value]:
    return [{'text':'first'},{'text':'second'}]


def fail(value: Value) -> Value:
    raise ValueError('expected failure')


class SignalQuery(TypedDict):
    endpoint: str
    limit: int


def inbox(ctx: InputMappingContext) -> SignalQuery:
    return {'endpoint':'inbox','limit':2}


def no_request(ctx: InputMappingContext) -> None:
    return None


def mapped_value(ctx: InputMappingContext) -> Value:
    return ctx.invocation_input


class Notice(TypedDict):
    text: str
    child: dict[str,str]


def notify_input(ctx: InputMappingContext) -> Notice:
    return {'text':ctx.invocation_input['text'],'child':ctx.self_handle.model_dump(mode='json')}


def two_notices(ctx: InputMappingContext) -> list[Notice]:
    return [{'text':'first','child':{}},{'text':'second','child':{}}]


GATE = threading.Event()


async def blocked(value: Value) -> Value:
    while not GATE.is_set():
        await asyncio.sleep(.005)
    return value


async def partial_map(value: Value) -> Value:
    if value['text'] == 'second':
        return await blocked(value)
    return value


def null_units(ctx: InputMappingContext) -> list[None]:
    return [None,None]


async def partial_null_map() -> None:
    from autoagent.core.operators.context import OperatorCallContext
    if OperatorCallContext.current().unit_index == 1:
        await blocked({'text':'second'})
    return None


class ForkTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.settings = HostSettings(database_url='sqlite+aiosqlite:///' + str(Path(self.temp.name)/'fork.db'))

    async def asyncTearDown(self):
        self.temp.cleanup()

    def host(self, workflows):
        return AutoAgentHost(workflows=workflows, settings=self.settings, environment={})

    async def event(self, host, ref, name):
        await host._persistence.flush()
        rows = (await host.list_execution_events(ref.invocation_id, names=[name], limit=100)).items
        return rows[-1]

    async def dataset(self,host,iid,**options):
        manifest=await host.get_trace_dataset(iid,**options)
        sections={}
        for kind,section in manifest['sections'].items():
            sections[kind]=[]
            for page in range(section['pages']):
                sections[kind].extend((await host.get_trace_dataset_page(manifest['snapshot_id'],kind,page))['items'])
        return manifest,sections

    async def test_timeline_excludes_inherited_work_and_keeps_wait_continuation(self):
        """Fork history stays inspectable while Timeline counts and rows show only continued or new branch work."""
        wf=Workflow('fork-timing',nodes=[Node('z_before',echo),Node('a_wait',Wait(Value,Value)),Node('m_after',echo)],
            edges=[Edge('z_before','a_wait',id='wait'),Edge('a_wait','m_after',id='after')])
        async with self.host([wf]) as host:
            ref=await host.execute(wf.id,{'text':'source'},entry_node_id='z_before')
            await host.join(ref)
            event=await self.event(host,ref,'node_occurrence.waiting')
            fork=await host.fork(ref.invocation_id,sequence=event['sequence'])
            result=await host.join(fork['invocation_id'])
            before,rows=await self.dataset(host,fork['invocation_id'])
            self.assertEqual({r['node_id']:r['timeline_visible'] for r in rows['node']},{'z_before':False,'a_wait':True})
            self.assertEqual(before['overview']['counts'],{'node':1,'call':1,'wait':1})
            start=before['overview']['start_at_us']
            timeline=await host.list_timeline_items(fork['invocation_id'],start_us=start,end_us=before['overview']['end_at_us']+1)
            self.assertEqual([r['node_id'] for r in timeline.items],['a_wait'])
            self.assertEqual(timeline.items[0]['call_count'],1)
            self.assertEqual(len(timeline.items[0]['preview_calls']),1)
            inherited=await host.get_execution_record(fork['invocation_id'],'node','z_before@root')
            self.assertIsNotNone(inherited['inherited_from'])
            await host.respond(fork['invocation_id'],result.waits[0].id,{'text':'branch'})
            await host.join(fork['invocation_id'])
            await host._persistence.flush()
            after,rows=await self.dataset(host,fork['invocation_id'])
            self.assertEqual(after['overview']['counts'],{'node':2,'call':2,'wait':1})
            page=await host.list_timeline_items(fork['invocation_id'],start_us=start,end_us=after['overview']['end_at_us']+1)
            self.assertEqual([r['node_id'] for r in page.items],['a_wait','m_after'])
            self.assertGreater(page.items[0]['end_at_us'],page.items[0]['start_at_us'])
            _,delta=await self.dataset(host,fork['invocation_id'],since=before['snapshot_id'])
            merged={r['id']:r for r in rows['node']}
            merged.update({r['id']:r for r in delta['node']})
            self.assertEqual({r['node_id'] for r in merged.values() if r['timeline_visible']},{'a_wait','m_after'})

    async def test_timeline_omits_reused_map_calls_and_abandoned_inflight_call(self):
        """A partially completed Map shows its new replayed Call but omits inherited results and lost source Calls."""
        GATE.clear()
        wf=Workflow('fork-map-timing',nodes=[Node('map',partial_map,input_mapping=two_children,map=Map(max_parallelism=2), recovery_mode=Recovery("replay_safe"))])
        try:
            async with self.host([wf]) as host:
                ref=await host.execute(wf.id,{'text':'source'},entry_node_id='map')
                async with asyncio.timeout(5):
                    while not (events:=(await host.list_execution_events(ref.invocation_id,names=['operator_call.completed'])).items):
                        await asyncio.sleep(.01)
                fork=await host.fork(ref.invocation_id,sequence=events[0]['sequence'])
                async with asyncio.timeout(5):
                    while True:
                        _,rows=await self.dataset(host,fork['invocation_id'])
                        active=[r for r in rows['call'] if r['timeline_visible']]
                        if active:
                            break
                        await asyncio.sleep(.01)
                self.assertEqual(len(active),1)
                self.assertEqual(active[0]['unit_index'],1)
                self.assertTrue(any(not r['timeline_visible'] and r['unit_index']==0 for r in rows['call']))
                GATE.set()
                await host.join(fork['invocation_id'])
                await host.join(ref)
                await host._persistence.flush()
                manifest,rows=await self.dataset(host,fork['invocation_id'])
                self.assertEqual(manifest['overview']['counts']['call'],1)
                self.assertEqual([r['unit_index'] for r in rows['call'] if r['timeline_visible']],[1])
        finally:
            GATE.set()

    async def test_timeline_excludes_inherited_waits_with_null_requests(self):
        """A completed null-request Wait stays inherited while its pending successor continues in the branch."""
        wf=Workflow('fork-null-requests',nodes=[Node('before',Wait(None,Value),input_mapping=no_request),
            Node('ask',Wait(None,Value),input_mapping=no_request)],edges=[Edge('before','ask',id='next')])
        async with self.host([wf]) as host:
            ref=await host.execute(wf.id,{'text':'source'},entry_node_id='before')
            waiting=await host.join(ref)
            await host.respond(ref,waiting.waits[0].id,{'text':'first'})
            await host.join(ref)
            event=await self.event(host,ref,'node_occurrence.waiting')
            fork=await host.fork(ref.invocation_id,sequence=event['sequence'])
            result=await host.join(fork['invocation_id'])
            manifest,rows=await self.dataset(host,fork['invocation_id'])
            self.assertEqual(manifest['overview']['counts'],{'node':1,'call':1,'wait':1})
            self.assertEqual(sum(r['timeline_visible'] for r in rows['call']),1)
            self.assertEqual(sum(r['timeline_visible'] for r in rows['wait']),1)
            await host.respond(fork['invocation_id'],result.waits[0].id,{'text':'second'})
            await host.join(fork['invocation_id'])
            await host._persistence.flush()
            manifest,_=await self.dataset(host,fork['invocation_id'])
            self.assertEqual(manifest['overview']['counts'],{'node':1,'call':1,'wait':1})

    async def test_timeline_hides_inherited_null_map_results(self):
        """Imported null Input/Output Calls need no value refs to be excluded from branch timing."""
        GATE.clear()
        wf=Workflow('fork-null-map',nodes=[Node('map',partial_null_map,input_mapping=null_units,map=Map(max_parallelism=2), recovery_mode=Recovery("replay_safe"))])
        try:
            async with self.host([wf]) as host:
                ref=await host.execute(wf.id,None,entry_node_id='map')
                async with asyncio.timeout(5):
                    while not (events:=(await host.list_execution_events(ref.invocation_id,names=['operator_call.completed'])).items):
                        await asyncio.sleep(.01)
                fork=await host.fork(ref.invocation_id,sequence=events[0]['sequence'])
                GATE.set()
                await host.join(fork['invocation_id'])
                await host.join(ref)
                await host._persistence.flush()
                manifest,rows=await self.dataset(host,fork['invocation_id'])
                self.assertEqual(manifest['overview']['counts']['call'],1)
                self.assertEqual([row['unit_index'] for row in rows['call'] if row['timeline_visible']],[1])
        finally:
            GATE.set()

    async def test_wait_fork_is_independent_and_can_restart(self):
        """A fork preserves prefix state while new Wait IDs and persistence isolate both executions."""
        wf = Workflow('fork-wait', nodes=[Node('before',echo),Node('ask',Wait(Value,Value))],
            edges=[Edge('before','ask',id='next')])
        async with self.host([wf]) as host:
            ref = await host.execute(wf.id, {'text':'original'}, entry_node_id='before')
            before = await host.join(ref)
            event = await self.event(host,ref,'node_occurrence.waiting')
            plan = await host.plan_fork(ref.invocation_id, sequence=event['sequence'])
            self.assertTrue(plan['available'], plan)
            fork = await host.fork(ref.invocation_id, sequence=event['sequence'])
            result = await host.join(fork['invocation_id'])
            self.assertEqual(result.status,'waiting')
            self.assertNotEqual(result.waits[0].id,before.waits[0].id)
            self.assertEqual((await host.get_execution(ref)).status,'waiting')
            branch_id = fork['invocation_id']
        async with self.host([wf]) as host:
            result = await host.join(branch_id)
            await host.respond(branch_id,result.waits[0].id,{'text':'branch'})
            self.assertEqual((await host.join(branch_id)).output,{'text':'branch'})
            self.assertEqual((await host.get_execution(ref)).status,'waiting')

    async def test_fork_completed_source_at_earlier_event(self):
        """A terminal source can branch from an earlier exact cut without importing its future outcome."""
        wf = Workflow('fork-echo', nodes=[Node('entry',echo)])
        async with self.host([wf]) as host:
            ref = await host.execute(wf.id, {'text':'value'}, entry_node_id='entry')
            await host.join(ref)
            event = await self.event(host,ref,'invocation.started')
            fork = await host.fork(ref.invocation_id,sequence=event['sequence'])
            self.assertEqual((await host.join(fork['invocation_id'])).output,{'text':'value'})
            self.assertFalse((await host.plan_fork(ref.invocation_id,
                sequence=(await self.event(host,ref,'invocation.completed'))['sequence']))['available'])

    async def test_child_event_forks_owning_root_and_waits(self):
        """A Child cut clones its Root and related ownership rather than detaching the selected Child."""
        child = Workflow('fork-child', nodes=[Node('ask',Wait(Value,Value))])
        parent = Workflow('fork-parent',nodes=[Node('spawn',Spawn(child,'ask')),Node('await',Await())],
            edges=[Edge('spawn','await',id='join')])
        async with self.host([parent]) as host:
            ref = await host.execute(parent.id,{'text':'child'},entry_node_id='spawn')
            await host.join(ref)
            children = await host.list_child_executions(ref.invocation_id)
            cid = children.items[0]['invocation_id']
            from types import SimpleNamespace
            event = await self.event(host,SimpleNamespace(invocation_id=cid),'node_occurrence.waiting')
            fork = await host.fork(cid,sequence=event['sequence'])
            self.assertNotEqual(fork['selected_invocation_id'],fork['root_invocation_id'])
            self.assertEqual(len(fork['members']),2)
            waits = (await host.list_execution_waits(fork['selected_invocation_id'])).items
            self.assertEqual(len(waits),1)
            self.assertNotEqual(waits[0]['wait_id'],(await host.list_execution_waits(cid)).items[0]['wait_id'])

    async def test_grandchild_fork_keeps_complete_ancestry_and_independent_response(self):
        """Selecting a grandchild preserves both owners and responds only inside the new three-level graph."""
        leaf=Workflow('fork-leaf',nodes=[Node('ask',Wait(Value,Value))])
        middle=Workflow('fork-middle',nodes=[Node('spawn',Spawn(leaf,'ask'))])
        root=Workflow('fork-grandparent',nodes=[Node('spawn',Spawn(middle,'spawn'))])
        async with self.host([root]) as host:
            ref=await host.execute(root.id,{'text':'family'},entry_node_id='spawn')
            await host.join(ref)
            await host._persistence.flush()
            child=(await host.list_child_executions(ref.invocation_id)).items[0]
            grandchild=(await host.list_child_executions(child['invocation_id'])).items[0]
            event=(await host.list_execution_events(grandchild['invocation_id'],names=['node_occurrence.waiting'])).items[0]
            fork=await host.fork(grandchild['invocation_id'],sequence=event['sequence'])
            self.assertEqual(len(fork['members']),3)
            self.assertEqual(len((await host.get_execution_family(fork['selected_invocation_id']))['ancestors']),2)
            result=await host.join(fork['invocation_id'])
            current=await host.get_execution_trace(fork['invocation_id'])
            second=await host.fork(fork['invocation_id'],sequence=current['last_event']['sequence'])
            result=await host.join(second['invocation_id'])
            await host.respond(second['invocation_id'],result.waits[0].id,{'text':'new'})
            self.assertEqual((await host.join(second['invocation_id'])).status,'completed')
            self.assertTrue((await host.get_execution(fork['invocation_id'])).waits)
            source=await host.get_execution(ref)
            self.assertNotIn(source.status,('completed','failed','cancelled'))
            self.assertEqual(len(source.waits),1)
            self.assertNotEqual(source.waits[0].id,result.waits[0].id)

    async def test_concurrent_explicit_session_admission_has_one_winner(self):
        """Two concurrent Fork requests using one explicit new Session admit exactly one graph."""
        wf=Workflow('fork-race',nodes=[Node('ask',Wait(Value,Value))])
        async with self.host([wf]) as host:
            ref=await host.execute(wf.id,{'text':'source'},entry_node_id='ask')
            await host.join(ref)
            event=await self.event(host,ref,'node_occurrence.waiting')
            results=await asyncio.gather(*(host.fork(ref.invocation_id,sequence=event['sequence'],session_id='one-winner') for _ in range(2)),return_exceptions=True)
            accepted=[result for result in results if isinstance(result,dict)]
            rejected=[result for result in results if isinstance(result,HostRunConflict)]
            self.assertEqual(len(accepted),1)
            self.assertEqual([result.code for result in rejected],['FORK_SESSION_EXISTS'])
            self.assertEqual((await host.join(accepted[0]['invocation_id'])).status,'waiting')

    async def test_terminal_failure_reopens_only_in_branch(self):
        """Terminal Fork uses Recovery's reopening rules without modifying failed source history."""
        wf=Workflow('fork-failure',nodes=[Node('entry',fail, recovery_mode=Recovery("replay_safe"))])
        async with self.host([wf]) as host:
            ref=await host.execute(wf.id,{'text':'bad'},entry_node_id='entry')
            await host.join(ref)
            event=await self.event(host,ref,'invocation.failed')
            fork=await host.fork(ref.invocation_id,sequence=event['sequence'])
            self.assertEqual((await host.join(fork['invocation_id'])).status,'failed')
            self.assertEqual((await host.get_execution(ref)).status,'failed')
            trace=await host.get_execution_trace(fork['invocation_id'])
            self.assertEqual(trace['fork']['source_sequence'],event['sequence'])

    async def test_completed_sibling_is_reused_in_mapped_child_family(self):
        """Completed compacted siblings remain completed when another mapped Child is forked at a Wait."""
        child=Workflow('fork-many-child',nodes=[Node('ask',Wait(Value,Value))])
        parent=Workflow('fork-many-parent',nodes=[Node('spawn',Spawn(child,'ask'),input_mapping=two_children,map=Map(max_parallelism=2))])
        async with self.host([parent]) as host:
            ref=await host.execute(parent.id,{'text':'children'},entry_node_id='spawn')
            await host.join(ref)
            await host._persistence.flush()
            children=(await host.list_child_executions(ref.invocation_id,order='asc')).items
            first,second=children
            wait=(await host.list_execution_waits(first['invocation_id'])).items[0]
            await host.respond(ref,wait['wait_id'],{'text':'done'})
            # The compaction Event belongs to the completed Child Session.
            async with asyncio.timeout(5):
                while True:
                    await host._persistence.flush()
                    compacted=(await host.list_execution_events(first['invocation_id'],names=['child_invocation.compacted'],limit=100)).items
                    if compacted:
                        break
                    await asyncio.sleep(.01)
            sequence=compacted[-1]['sequence']
            fork=await host.fork(first['invocation_id'],sequence=sequence)
            result=await host.join(fork['invocation_id'])
            self.assertEqual(len(result.waits),1)
            members=await host.list_child_executions(fork['invocation_id'])
            self.assertEqual(sorted(m['status'] for m in members.items),['completed','waiting'])

    async def test_duplicate_session_and_wrong_invocation_sequence_are_rejected(self):
        """Explicit existing Session IDs and Events from another Invocation cannot change source ownership."""
        wf=Workflow('fork-identity',nodes=[Node('entry',echo)])
        async with self.host([wf]) as host:
            first=await host.execute(wf.id,{'text':'a'},entry_node_id='entry',session_id='shared')
            await host.join(first)
            event=await self.event(host,first,'invocation.started')
            with self.assertRaises(HostRunConflict):
                await host.fork(first.invocation_id,sequence=event['sequence'],session_id='shared')
            second=await host.execute(wf.id,{'text':'b'},entry_node_id='entry',session_id='shared')
            await host.join(second)
            plan=await host.plan_fork(second.invocation_id,sequence=event['sequence'])
            self.assertFalse(plan['available'])
            self.assertEqual(plan['code'],'FORK_POSITION_INVALID')

    async def test_source_projection_and_event_values_survive_fork(self):
        """Inherited Call inputs and outputs remain readable and initial graph replay is complete."""
        wf=Workflow('fork-values',nodes=[Node('echo',echo),Node('ask',Wait(Value,Value))],edges=[Edge('echo','ask',selected,id='next')])
        async with self.host([wf]) as host:
            ref=await host.execute(wf.id,{'text':'original'},entry_node_id='echo')
            await host.join(ref)
            event=await self.event(host,ref,'node_occurrence.waiting')
            fork=await host.fork(ref.invocation_id,sequence=event['sequence'])
            await host.join(fork['invocation_id'])
            calls=(await host.list_operator_calls(fork['invocation_id'])).items
            call=next(c for c in calls if c['operator_id']=='echo')
            self.assertIsNotNone(call['operator_definition_id'])
            result=await host.get_execution_value(fork['invocation_id'],'call',call['call_id'],'output')
            self.assertEqual(result['value'],{'text':'original'})
            frame=await host.get_timeline_replay(fork['invocation_id'],sequence=1)
            self.assertTrue(frame['history_complete'])
            trace=await host.get_execution_trace(fork['invocation_id'])
            self.assertEqual(next(e for e in trace['edges'] if e['edge_id']=='next')['evaluations'],1)
            edges=(await host.list_edge_evaluations(fork['invocation_id'],'next')).items
            self.assertEqual(len(edges),1)
            self.assertTrue(edges[0]['inherited'])
            identity=f"{edges[0]['event_id']}:{edges[0]['result_index']}"
            record=await host.get_execution_record(fork['invocation_id'],'edge',identity)
            self.assertEqual(record['ordinal'],1)
            self.assertEqual(record['invocation_id'],fork['invocation_id'])
            inherited=next(e for e in frame['events'] if e['sequence']==1)['initial_edges']
            self.assertEqual(inherited[0]['evaluations'],1)

    async def test_fork_of_fork_references_original_stage_values(self):
        """A second branch reads inherited stage values through both cuts without copying source Events."""
        wf=Workflow('fork-stages',nodes=[Node('ask',Wait(Value,Value),input_mapping=mapped_value)])
        async with self.host([wf]) as host:
            ref=await host.execute(wf.id,{'text':'original'},entry_node_id='ask')
            await host.join(ref)
            event=await self.event(host,ref,'node_occurrence.waiting')
            first=await host.fork(ref.invocation_id,sequence=event['sequence'])
            await host.join(first['invocation_id'])
            second=await host.fork(first['invocation_id'],sequence=1)
            await host.join(second['invocation_id'])
            node=await host.get_execution_record(second['invocation_id'],'node','ask@root')
            self.assertIn('input',node['values'])
            self.assertEqual((await host.get_execution_value(second['invocation_id'],'node','ask@root','input'))['value'],{'text':'original'})
            stages=(await host.list_execution_stages(second['invocation_id'],'ask@root')).items
            mapped=next(s for s in stages if s['event_name']=='input.mapped')
            self.assertTrue(mapped['inherited'])
            self.assertEqual(mapped['invocation_id'],ref.invocation_id)
            value=await host.get_execution_stage_value(mapped['invocation_id'],'ask@root',mapped['event_id'],'mapped_input')
            self.assertEqual(value['value'],{'text':'original'})
            self.assertFalse((await host.list_execution_events(second['invocation_id'],names=['input.mapped'])).items)

    async def test_fork_keeps_timer_deadline_and_shutdown_drains_running_fork(self):
        """Timers keep their absolute deadline and closing Host settles a replaying Fork's wrapper task."""
        from autoagent import Timer, TimerRequest
        timer=Workflow('fork-timer',nodes=[Node('timer',Timer())])
        async with self.host([timer]) as host:
            ref=await host.execute(timer.id,TimerRequest(delay_us=60_000_000).model_dump(),entry_node_id='timer')
            source=await host.join(ref)
            event=await self.event(host,ref,'node_occurrence.waiting')
            fork=await host.fork(ref.invocation_id,sequence=event['sequence'])
            result=await host.join(fork['invocation_id'])
            self.assertEqual(result.waits[0].condition['deadline_at_us'],source.waits[0].condition['deadline_at_us'])
        GATE.clear()
        running=Workflow('fork-shutdown',nodes=[Node('blocked',blocked, recovery_mode=Recovery("replay_safe"))])
        try:
            async with self.host([running]) as host:
                runtime=host._runtime
                ref=await host.execute(running.id,{'text':'running'},entry_node_id='blocked')
                async with asyncio.timeout(5):
                    while not (rows:=(await host.list_execution_events(ref.invocation_id,names=['operator_call.started'])).items):
                        await asyncio.sleep(.01)
                fork=await host.fork(ref.invocation_id,sequence=rows[-1]['sequence'])
                self.assertTrue(fork['accepted'])
            self.assertFalse(runtime.fork_tasks)
        finally:
            GATE.set()

    async def test_mailbox_order_and_external_source_frontier_survive_fork(self):
        """Multiple inherited Signals preserve FIFO and deduplication frontiers under new identities."""
        wf=Workflow('fork-signals',nodes=[Node('gate',Wait(Value,Value)),Node('inbox',ReceiveSignal('inbox',limit=2))],
            edges=[Edge('gate','inbox',id='receive')],signal_endpoints=[SignalEndpoint('inbox',Value)])
        async with self.host([wf]) as host:
            ref=await host.execute(wf.id,{'text':'start'},entry_node_id='gate')
            await host.join(ref)
            await host.signal(ref,'inbox',{'text':'first'},source_id='source',sequence=1)
            await host.signal(ref,'inbox',{'text':'second'},source_id='source',sequence=2)
            event=await self.event(host,ref,'signal.accepted')
            fork=await host.fork(ref.invocation_id,sequence=event['sequence'])
            waiting=await host.join(fork['invocation_id'])
            await host.signal(fork['invocation_id'],'inbox',{'text':'third'},source_id='source',sequence=3)
            await host.respond(fork['invocation_id'],waiting.waits[0].id,{'text':'continue'})
            result=await host.join(fork['invocation_id'])
            self.assertEqual([m['payload']['text'] for m in result.output['messages']],['first','second'])
            self.assertEqual((await host.get_execution(ref)).status,'waiting')

    async def test_parent_child_signals_keep_ownership_without_resending(self):
        """Internal Signal identities, retired receipts and two queued messages survive a whole-family Fork."""
        child=Workflow('fork-notify-child',nodes=[Node('notify',SendSignal(OwnerHandle(),'notice'),input_mapping=notify_input),Node('wait',Wait(None,Value),input_mapping=no_request)],
            edges=[Edge('notify','wait',id='wait')])
        parent=Workflow('fork-notify-parent',nodes=[Node('spawn',Spawn(child,'notify'),input_mapping=two_notices,map=Map(max_parallelism=2)),Node('wait',Wait(None,Value),input_mapping=no_request)],
            edges=[Edge('spawn','wait',id='wait')],signal_endpoints=[SignalEndpoint('notice',Notice)])
        async with self.host([parent]) as host:
            ref=await host.execute(parent.id,{'text':'family'},entry_node_id='spawn')
            await host.join(ref)
            event=await self.event(host,ref,'signal.accepted')
            fork=await host.fork(ref.invocation_id,sequence=event['sequence'])
            await host.join(fork['invocation_id'])
            await host._persistence.flush()
            events=(await host.list_execution_events(fork['invocation_id'],names=['signal.accepted'],limit=100)).items
            self.assertEqual(len(events),0)
            new=host._runtime.repository.state(fork['session_id']).invocation
            self.assertEqual(len(new.signals['messages']),2)
            self.assertEqual(len(fork['members']),3)

    async def test_pause_is_not_inherited_as_source_control(self):
        """A paused source remains paused while the new Root can receive its own response."""
        wf=Workflow('fork-paused',nodes=[Node('wait',Wait(Value,Value))])
        async with self.host([wf]) as host:
            ref=await host.execute(wf.id,{'text':'wait'},entry_node_id='wait')
            await host.join(ref)
            await host.pause(ref)
            event=await self.event(host,ref,'graph.paused')
            fork=await host.fork(ref.invocation_id,sequence=event['sequence'])
            result=await host.join(fork['invocation_id'])
            self.assertEqual(result.control_state,'active')
            self.assertEqual((await host.get_execution(ref)).control_state,'paused')
            await host.respond(fork['invocation_id'],result.waits[0].id,{'text':'new'})
            self.assertEqual((await host.join(fork['invocation_id'])).status,'completed')

    async def test_cancelled_source_reopens_as_new_wait(self):
        """Cancelled graphs reopen under new identities and leave the cancellation audit untouched."""
        wf=Workflow('fork-cancelled',nodes=[Node('wait',Wait(Value,Value), recovery_mode=Recovery("replay_safe"))])
        async with self.host([wf]) as host:
            ref=await host.execute(wf.id,{'text':'wait'},entry_node_id='wait')
            await host.join(ref)
            await host.cancel(ref,reason='source only')
            event=await self.event(host,ref,'invocation.cancelled')
            fork=await host.fork(ref.invocation_id,sequence=event['sequence'])
            self.assertEqual((await host.join(fork['invocation_id'])).status,'waiting')
            self.assertEqual((await host.get_execution(ref)).status,'cancelled')

    async def test_non_replayable_inflight_call_is_rejected_before_admission(self):
        """Never-replay Calls prevent Fork without admitting a new Session or disturbing the live source."""
        GATE.clear()
        wf=Workflow('fork-never',nodes=[Node('blocked',blocked,recovery_mode=Recovery(mode='never'))])
        try:
            async with self.host([wf]) as host:
                ref=await host.execute(wf.id,{'text':'wait'},entry_node_id='blocked')
                for _ in range(100):
                    await host._persistence.flush()
                    rows=(await host.list_execution_events(ref.invocation_id,names=['operator_call.started'])).items
                    if rows:break
                    await asyncio.sleep(.005)
                plan=await host.plan_fork(ref.invocation_id,sequence=rows[0]['sequence'])
                self.assertFalse(plan['available'])
                self.assertEqual(plan['code'],'FORK_REPLAY_NOT_ALLOWED')
                self.assertEqual(len((await host.list_execution_sessions()).items),1)
                GATE.set()
                await host.join(ref)
        finally:
            GATE.set()

    async def test_fork_initialization_is_atomic_when_storage_fails(self):
        """An error after a transactional insert rolls back every new identity and preserves the source."""
        wf=Workflow('fork-atomic',nodes=[Node('wait',Wait(Value,Value))])
        async with self.host([wf]) as host:
            ref=await host.execute(wf.id,{'text':'wait'},entry_node_id='wait')
            await host.join(ref)
            event=await self.event(host,ref,'node_occurrence.waiting')
            await host._persistence.flush()
            write=host._persistence._writer.storage.write_transaction
            async def rejected(conn,entries):
                await write(conn,entries)
                raise ValueError('injected storage rejection')
            with patch.object(host._persistence._writer.storage,'write_transaction',side_effect=rejected):
                with self.assertRaisesRegex(ValueError,'injected storage'):
                    await host.fork(ref.invocation_id,sequence=event['sequence'],session_id='rolled-back')
            self.assertIsNone(await host._persistence.get_session('rolled-back'))
            self.assertEqual((await host.get_execution(ref)).status,'waiting')

    async def test_source_history_is_protected_until_fork_is_removed(self):
        """A referenced Runtime prefix cannot disappear while a branch still exposes its provenance."""
        from autoagent.host.persistence.models import ConflictError
        wf=Workflow('fork-source',nodes=[Node('work',echo)])
        async with self.host([wf]) as host:
            ref=await host.execute(wf.id,{'text':'source'},entry_node_id='work')
            await host.join(ref)
            event=await self.event(host,ref,'invocation.started')
            fork=await host.fork(ref.invocation_id,sequence=event['sequence'])
            await host.join(fork['invocation_id'])
            with self.assertRaises(ConflictError):
                await host._persistence.purge_root_graph(ref.invocation_id)

    async def test_user_events_start_fresh_and_large_artifacts_are_shared(self):
        """Fork references Runtime history, emits only new User Events, and deduplicates large inherited strings."""
        wf=Workflow('fork-observations',nodes=[Node('before',echo,user_events=observed),
            Node('after',Wait(Value,Value),user_events=observed)],edges=[Edge('before','after',id='next')])
        async with self.host([wf]) as host:
            ref=await host.execute(wf.id,{'text':'x'*(2*1024*1024)},entry_node_id='before')
            await host.join(ref)
            event=await self.event(host,ref,'node_occurrence.waiting')
            fork=await host.fork(ref.invocation_id,sequence=event['sequence'])
            waiting=await host.join(fork['invocation_id'])
            self.assertEqual((await host._persistence.user_events(fork['invocation_id'])).items,())
            await host.respond(fork['invocation_id'],waiting.waits[0].id,{'text':'new'})
            await host.join(fork['invocation_id'])
            await host._persistence.flush()
            events=(await host._persistence.user_events(fork['invocation_id'])).items
            self.assertEqual([(e['sequence'],e['kind']) for e in events],[(1,'after')])
            from sqlalchemy import select,func
            from autoagent.host.persistence.schema import meta
            async def count():
                async with host._persistence._queries.view() as (conn,_,_,_):
                    table=meta.tables['artifacts']
                    return await conn.scalar(select(func.count()).select_from(table).where(table.c.value_type=='string',table.c.logical_bytes>=2*1024*1024))
            self.assertEqual(await host._persistence._writer.run(count),1)


class CoreForkTests(unittest.IsolatedAsyncioTestCase):
    async def test_core_fork_continues_new_graph_and_keeps_source_wait(self):
        """Core-only callers can prepare and execute a new branch without involving a Host."""
        app=AutoAgentApp()
        self.addAsyncCleanup(app.aclose)
        wf=Workflow('core-fork',nodes=[Node('wait',Wait(Value,Value))])
        app.register_workflow(wf)
        ref=(await app.asubmit_invoke(wf.id,{'text':'core'},entry_node_id='wait')).ref
        original=await app.ajoin(ref)
        graph=await app.acapture_checkpoint(ref)
        result=await app.afork(graph)
        self.assertEqual(result.status,'waiting')
        self.assertNotEqual(result.invocation_id,ref.invocation_id)
        self.assertNotEqual(result.waits[0].id,original.waits[0].id)


@unittest.skipUnless(POSTGRES_TEST_URL,'PostgreSQL test URL is required.')
class PostgreSQLForkTests(ForkTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.database = PostgreSQLDatabase()
        self.settings = replace(self.settings,database_url=await self.database.create())

    async def asyncTearDown(self):
        await self.database.close()
        await super().asyncTearDown()

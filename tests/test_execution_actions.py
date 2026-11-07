"""Root control admission, bounded HTTP receipts and parent/child interaction."""
import asyncio
from dataclasses import replace
import tempfile
import threading
import time
import unittest
from pathlib import Path
from uuid import uuid4
from unittest.mock import patch
import httpx
from pydantic import BaseModel,ConfigDict
from autoagent import Edge,Node,ReceiveSignal,Recovery,SignalEndpoint,Spawn,Wait,Workflow,Map,InputMappingContext
from autoagent.host import AutoAgentHost,HostSettings,HostCapacityError
from autoagent.host.persistence.reading import SourceReader
from autoagent.server import create_app,ServerSettings
from autoagent.host.operations import ExecutionOperations
from tests.postgres import POSTGRES_TEST_URL,PostgreSQLDatabase
from tests.test_server import Value,identity

GATE=threading.Event()


async def gated(value:Value)->Value:
    while not GATE.is_set():
        await asyncio.sleep(.01)
    return value


def many_children(ctx:InputMappingContext)->list[Value]:
    return [{'text':str(index)} for index in range(25)]


class Approval(BaseModel):
    model_config=ConfigDict(extra='forbid')
    approved: bool
    note: str=''


class ExecutionActionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        GATE.clear()
        self.temp=tempfile.TemporaryDirectory()
        self.url='sqlite+aiosqlite:///'+str(Path(self.temp.name)/'controls.db')

    async def asyncTearDown(self):
        GATE.set()
        self.temp.cleanup()

    def host(self,workflows):
        return AutoAgentHost(workflows=workflows,settings=HostSettings(database_url=self.url),environment={})

    def client(self,host):
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(host,ui=False)),base_url='http://test')

    async def test_operation_settings_are_host_owned_and_environment_configurable(self):
        """Operation budgets apply equally to embedded and HTTP Hosts, with strict environment validation."""
        settings = HostSettings.from_environment(dict(AUTOAGENT_HOST_OPERATION_CONCURRENCY='2',
            AUTOAGENT_HOST_OPERATION_RECEIPTS='4', AUTOAGENT_HOST_OPERATION_TTL_SECONDS='600'))
        self.assertEqual((settings.operation_concurrency, settings.operation_receipts, settings.operation_ttl_seconds), (2, 4, 600))
        for values in (dict(operation_concurrency=0), dict(operation_receipts=1), dict(operation_ttl_seconds=0), dict(operation_ttl_seconds=float('nan'))):
            with self.assertRaises(ValueError):
                replace(HostSettings(), **values)

    async def test_operation_admission_does_not_charge_payload_twice(self):
        """A response within the Host request budget survives the asynchronous receipt adapter."""
        from autoagent.core.app._residency import retained_size
        wf = Workflow('receipt-payload-budget', nodes=[Node('ask', Wait(Value, Value))])
        async with self.host([wf]) as host:
            root = await host.execute(wf.id, {'text': 'ask'}, entry_node_id='ask')
            waiting = await host.join(root)
            payload = {'text': 'x' * 256}
            host.settings = replace(host.settings, max_request_bytes=retained_size((payload,)) + 1)
            receipt = await host.submit_execution_operation(dict(invocation_id=root.invocation_id,
                action='respond', request_id=uuid4().hex, target_invocation_id=root.invocation_id,
                wait_id=waiting.waits[0].id, payload=payload))
            await asyncio.gather(*tuple(host._execution_operations.tasks))
            self.assertEqual((await host.get_execution_operation(receipt['operation_id']))['status'], 'succeeded')
            self.assertEqual((await host.join(root)).output, payload)

    async def operation(self,client,body):
        if body['action']=='recover':
            controls=(await client.get('/api/v2/execution-controls',params={'invocation_id':body['invocation_id']})).json()
            body.setdefault('expected_version',controls['expected_version'])
        response=await client.post('/api/v2/execution-actions',json=body)
        self.assertEqual(response.status_code,202,response.text)
        return response.json()

    async def finished(self,client,operation):
        for _ in range(500):
            result=(await client.get('/api/v2/execution-operations/'+operation['operation_id'])).json()
            if result['status']!='running':
                self.assertEqual(result['status'],'succeeded',result)
                return result
            await asyncio.sleep(.01)
        self.fail('Control operation did not reach its boundary.')

    async def test_pause_is_asynchronous_idempotent_and_continue_reaches_wait_then_cancel(self):
        """Pause accepts immediately, shares duplicate receipts, and exposes failure-free boundary transitions."""
        wf=Workflow('control-pause',nodes=[Node('work',gated),Node('ask',Wait(Value,Value))],edges=[Edge('work','ask')])
        async with self.host([wf]) as host:
            root=await host.execute(wf.id,{'text':'start'},entry_node_id='work')
            async with self.client(host) as client:
                body=dict(invocation_id=root.invocation_id,action='pause',request_id=uuid4().hex)
                before=time.monotonic()
                operation=await self.operation(client,body)
                self.assertLess(time.monotonic()-before,1)
                self.assertEqual(operation['status'],'running')
                duplicate=await self.operation(client,body)
                self.assertEqual(duplicate['operation_id'],operation['operation_id'])
                conflict=await client.post('/api/v2/execution-actions',json=body|{'action':'cancel'})
                self.assertEqual(conflict.status_code,409)
                GATE.set()
                await self.finished(client,operation)
                controls=(await client.get('/api/v2/execution-controls',params={'invocation_id':root.invocation_id})).json()
                self.assertEqual(controls['control_state'],'paused')
                self.assertTrue(controls['actions']['recover']['available'])
                continued=await self.operation(client,dict(invocation_id=root.invocation_id,action='recover',request_id=uuid4().hex))
                await self.finished(client,continued)
                self.assertEqual((await host.join(root)).status,'waiting')
                cancelled=await self.operation(client,dict(invocation_id=root.invocation_id,action='cancel',request_id=uuid4().hex,reason='review stop'))
                await self.finished(client,cancelled)
                self.assertEqual((await host.join(root)).status,'cancelled')

    async def test_child_wait_response_is_root_scoped_and_decodes_registered_response_model(self):
        """A Child approval is submitted through Root with its own response contract and foreign Waits are rejected."""
        child=Workflow('control-child',nodes=[Node('approval',Wait(Value,Approval))])
        parent=Workflow('control-parent',nodes=[Node('spawn',Spawn(child,'approval'))])
        async with self.host([parent]) as host:
            root=await host.execute(parent.id,{'text':'ask'},entry_node_id='spawn')
            await host.join(root)
            iid=(await host.list_child_executions(root.invocation_id)).items[0]['invocation_id']
            with patch.object(SourceReader,'read',side_effect=AssertionError('Metadata only')):
                controls=await host.get_execution_controls(iid)
            self.assertEqual(controls['root_invocation_id'],root.invocation_id)
            wait=controls['pending_responses'][0]
            self.assertEqual(wait['node_id'], 'approval')
            async with self.client(host) as client:
                options=dict(invocation_id=iid,action='respond',target_invocation_id=iid,wait_id=wait['wait_id'])
                spec=await client.get('/api/v2/execution-action-spec',params=options)
                self.assertEqual(spec.status_code,200,spec.text)
                self.assertEqual(spec.json()['input_schema']['required'],['approved'])
                foreign=await client.post('/api/v2/execution-actions',json=options|dict(request_id=uuid4().hex,target_invocation_id='foreign',payload={'approved':True}))
                self.assertEqual(foreign.status_code,409)
                operation=await self.operation(client,options|dict(request_id=uuid4().hex,payload={'approved':True}))
                await self.finished(client,operation)
                self.assertEqual((await host.join(root)).status,'completed')
                duplicate=await self.operation(client,options|dict(request_id=operation['operation_id'],payload={'approved':True}))
                self.assertEqual(duplicate['status'],'succeeded')

    async def test_signal_uses_declared_endpoint_and_durable_source_sequence(self):
        """Signal submission validates the payload, preserves its receipt and advances the next source sequence."""
        wf=Workflow('control-signal',nodes=[Node('ask',Wait(Value,Value))],signal_endpoints=[SignalEndpoint('message',Approval)])
        async with self.host([wf]) as host:
            root=await host.execute(wf.id,{'text':'hold'},entry_node_id='ask');await host.join(root)
            async with self.client(host) as client:
                params=dict(invocation_id=root.invocation_id,action='signal',endpoint='message',source_id='browser-tab')
                spec=await client.get('/api/v2/execution-action-spec',params=params)
                self.assertEqual(spec.status_code,200,spec.text)
                self.assertEqual(spec.json()['next_sequence'],1)
                operation=await self.operation(client,params|dict(request_id=uuid4().hex,sequence=1,payload={'approved':True}))
                await self.finished(client,operation)
                next_spec=await client.get('/api/v2/execution-action-spec',params=params)
                self.assertEqual(next_spec.status_code,200,next_spec.text)
                self.assertEqual(next_spec.json()['next_sequence'],2)

    async def test_unregistered_and_superseded_executions_cannot_be_controlled(self):
        """Control eligibility checks exact registration and latest Session identity without restoring old executions."""
        wf=Workflow('control-registered',nodes=[Node('entry',identity)])
        async with self.host([wf]) as host:
            first=await host.execute(wf.id,{'text':'first'},entry_node_id='entry');await host.join(first)
            second=await host.execute(wf.id,{'text':'second'},entry_node_id='entry',session_id=first.session_id);await host.join(second)
            controls=await host.get_execution_controls(first.invocation_id)
            self.assertFalse(controls['actions']['cancel']['available'])
            self.assertTrue(controls['actions']['run_again']['available'])
        other=Workflow('other',nodes=[Node('entry',identity)])
        async with self.host([other]) as host:
            controls=await host.get_execution_controls(second.invocation_id)
            self.assertFalse(any(action['available'] for action in controls['actions'].values()))
            self.assertFalse(host._runtime.resident)

    async def test_operation_receipts_are_bounded_and_failed_payloads_remain_visible(self):
        """Bad response contracts produce queryable errors, while receipt retention stays bounded."""
        wf=Workflow('control-errors',nodes=[Node('ask',Wait(Value,Approval))],signal_endpoints=[SignalEndpoint('message',Approval)])
        async with self.host([wf]) as host:
            root=await host.execute(wf.id,{'text':'ask'},entry_node_id='ask');await host.join(root)
            wait=(await host.get_execution_controls(root.invocation_id))['pending_responses'][0]
            async with self.client(host) as client:
                rejected=await client.post('/api/v2/execution-actions',json=dict(invocation_id=root.invocation_id,action='respond',request_id=uuid4().hex,
                    target_invocation_id=root.invocation_id,wait_id=wait['wait_id'],payload={'approved':'wrong'}))
                self.assertIn(rejected.status_code,(400,422),rejected.text)
            host.settings=replace(host.settings,operation_concurrency=1,operation_receipts=2,operation_ttl_seconds=.05)
            manager=ExecutionOperations(host)
            try:
                op=await manager.submit(dict(invocation_id=root.invocation_id,action='signal',request_id=uuid4().hex,
                    endpoint='message',source_id='bad-sequence',sequence=2,payload={'approved':True}))
                await asyncio.gather(*tuple(manager.tasks))
                failed=manager.get(op['operation_id'])
                self.assertEqual(failed['status'],'failed')
                self.assertEqual(failed['error']['code'],'SIGNAL_SEQUENCE_GAP')
                await asyncio.sleep(.06)
                manager.expire()
                self.assertEqual(len(manager.entries),0)
            finally:
                await manager.close()

    async def test_cancel_can_finish_a_pending_pause_and_observer_close_does_not_cancel_host_work(self):
        """Root cancellation stays available during pause, and closing HTTP observers cannot undo an admitted pause."""
        wf=Workflow('control-detach',nodes=[Node('work',gated),Node('ask',Wait(Value,Value))],edges=[Edge('work','ask')])
        async with self.host([wf]) as host:
            first=await host.execute(wf.id,{'text':'first'},entry_node_id='work')
            host.settings=replace(host.settings,operation_concurrency=1,operation_receipts=2)
            manager=ExecutionOperations(host)
            pause=await manager.submit(dict(invocation_id=first.invocation_id,action='pause',request_id=uuid4().hex))
            for _ in range(100):
                if (await host.get_execution_controls(first.invocation_id))['control_state']=='pausing':break
                await asyncio.sleep(.01)
            with self.assertRaises(HostCapacityError):
                await manager.submit(dict(invocation_id=first.invocation_id,action='cancel',request_id=uuid4().hex))
            await manager.close()
            GATE.set()
            for _ in range(200):
                if (await host.get_execution_controls(first.invocation_id))['control_state']=='paused':break
                await asyncio.sleep(.01)
            self.assertEqual((await host.get_execution_controls(first.invocation_id))['control_state'],'paused')
            # The separately admitted Host task survived cancellation of its HTTP observer.
            await host.perform_execution_action(first.invocation_id,'recover')
            self.assertEqual((await host.join(first)).status,'waiting')
            host.settings=replace(host.settings,operation_concurrency=32,operation_receipts=256)
            GATE.clear()
            second=await host.execute(wf.id,{'text':'second'},entry_node_id='work')
            async with self.client(host) as client:
                pausing=await self.operation(client,dict(invocation_id=second.invocation_id,action='pause',request_id=uuid4().hex))
                for _ in range(100):
                    if (await host.get_execution_controls(second.invocation_id))['control_state']=='pausing':break
                    await asyncio.sleep(.01)
                cancel=await self.operation(client,dict(invocation_id=second.invocation_id,action='cancel',request_id=uuid4().hex))
                await self.finished(client,cancel)
                await self.finished(client,pausing)
                self.assertEqual((await host.join(second)).status,'cancelled')

    async def test_interrupted_wait_can_be_recovered_after_host_restart(self):
        """A nonresident current Root is offered Continue, then restores without losing its external Wait."""
        wf=Workflow('control-restart',nodes=[Node('ask',Wait(Value,Value))])
        async with self.host([wf]) as host:
            root=await host.execute(wf.id,{'text':'persist'},entry_node_id='ask');await host.join(root)
        async with self.host([wf]) as host:
            controls=await host.get_execution_controls(root.invocation_id)
            self.assertEqual(controls['recovery_reason'],'interrupted')
            self.assertTrue(controls['actions']['recover']['available'])
            result=await host.perform_execution_action(root.invocation_id,'recover')
            self.assertEqual(result['status'],'waiting')
            self.assertFalse((await host.get_execution_controls(root.invocation_id))['actions']['recover']['available'])

    async def test_pending_child_responses_are_paged_and_require_registered_dependencies(self):
        """Many Child Waits remain metadata-only and controllable only while every required Revision is registered."""
        child=Workflow('control-many-child',nodes=[Node('approval',Wait(Value,Value))])
        parent=Workflow('control-many-parent',nodes=[Node('spawn',Spawn(child,'approval'),input_mapping=many_children,map=Map(max_parallelism=4))])
        async with self.host([parent]) as host:
            root=await host.execute(parent.id,{'text':'many'},entry_node_id='spawn');await host.join(root)
            with patch.object(SourceReader,'read',side_effect=AssertionError('Metadata only')):
                first=await host.get_execution_controls(root.invocation_id)
                self.assertEqual(len(first['pending_responses']),20)
                self.assertIsNotNone(first['next_cursor'])
                second=await host.get_execution_controls(root.invocation_id,cursor=first['next_cursor'])
                self.assertEqual(len(second['pending_responses']),5)
            rows=first['pending_responses']+second['pending_responses']
            self.assertEqual(len({(row['invocation_id'],row['wait_id']) for row in rows}),25)
            self.assertTrue(all(row['node_id'] == 'approval' for row in rows))
            self.assertTrue(first['actions']['respond']['available'])
            revision=host._runtime.workflow_ids[child.id]
            definition=host._runtime.revisions.pop(revision)
            try:
                controls=await host.get_execution_controls(root.invocation_id)
                self.assertFalse(controls['actions']['cancel']['available'])
                self.assertIn('Child Revision',controls['actions']['cancel']['reason'])
            finally:
                host._runtime.revisions[revision]=definition
            await host.cancel(root)

    async def test_failed_recovery_after_restart_preserves_history_and_deduplicates_across_receipts(self):
        """Recover restores a terminal Root from SQL history, preserves its prefix and survives receipt loss."""
        calls={'first':0,'flaky':0}
        def first(value:Value)->Value:
            calls['first']+=1
            return value
        def flaky(value:Value)->Value:
            calls['flaky']+=1
            if calls['flaky']==1:
                raise ValueError('temporary')
            return value
        wf=Workflow('recover-http-failed',nodes=[Node('first',first),Node('flaky',flaky,recovery_mode=Recovery('replay_safe'))],edges=[Edge('first','flaky')])
        async with self.host([wf]) as host:
            root=await host.execute(wf.id,{'text':'persist'},entry_node_id='first')
            self.assertEqual((await host.join(root)).status,'failed')
        async with self.host([wf]) as host:
            controls=await host.get_execution_controls(root.invocation_id)
            self.assertTrue(controls['actions']['recover']['available'])
            request=dict(invocation_id=root.invocation_id,action='recover',request_id=uuid4().hex,expected_version=controls['expected_version'])
            async with self.client(host) as client:
                stale=await client.post('/api/v2/execution-actions',json=request|{'expected_version':0})
                self.assertEqual(stale.status_code,409,stale.text)
                op=await self.operation(client,request)
                await self.finished(client,op)
            self.assertEqual((await host.join(root)).status,'completed')
            self.assertEqual(calls,{'first':1,'flaky':2})
            controls=await host.get_execution_controls(root.invocation_id)
            self.assertEqual(controls['recovery_count'],1)
            self.assertEqual(controls['last_recovery_id'],request['request_id'])
            self.assertFalse(controls['actions']['recover']['available'])
            # A new router reads the same temporary Host receipt and cannot repeat business work.
            async with self.client(host) as client:
                missing=await client.get('/api/v2/execution-operations/'+request['request_id'])
                self.assertEqual(missing.status_code,200)
                restored=(await client.get('/api/v2/execution-controls',params={'invocation_id':root.invocation_id})).json()
                self.assertEqual(restored['last_recovery_id'],request['request_id'])
                op=await self.operation(client,request)
                await self.finished(client,op)
            self.assertEqual(calls['flaky'],2)
            await host._persistence.flush()
            page=await host._persistence.list_nodes(root.invocation_id)
            self.assertEqual(len(page.items),3)
            self.assertEqual({row['status'] for row in page.items},{'completed','failed'})
            recovered=next(row for row in page.items if '~recover:' in row['occurrence_id'])
            detail=await host._persistence.node_detail(root.invocation_id,recovered['occurrence_id'])
            self.assertEqual(detail['input']['value'],{'text':'persist'})
            await host._persistence.rebuild_projections()
            after=await host._persistence.node_detail(root.invocation_id,recovered['occurrence_id'])
            self.assertEqual(after['input'],detail['input'])
            self.assertEqual(after['output'],detail['output'])
            self.assertEqual((await host.get_execution_controls(root.invocation_id))['recovery_count'],1)

    async def test_lost_response_receipt_reconciles_exact_child_wait_beyond_first_page(self):
        """Receipt loss exposes exact Wait state without reading payloads or confusing pagination with acceptance."""
        child=Workflow('receipt-child',nodes=[Node('ask',Wait(Value,Value))])
        parent=Workflow('receipt-parent',nodes=[Node('spawn',Spawn(child,'ask'),input_mapping=many_children,map=Map(max_parallelism=4))])
        async with self.host([parent]) as host:
            root=await host.execute(parent.id,{'text':'many'},entry_node_id='spawn');await host.join(root)
            first=await host.get_execution_controls(root.invocation_id)
            last=(await host.get_execution_controls(root.invocation_id,cursor=first['next_cursor']))['pending_responses'][-1]
            params=dict(invocation_id=root.invocation_id,target_invocation_id=last['invocation_id'],wait_id=last['wait_id'])
            async with self.client(host) as client:
                with patch.object(SourceReader,'read',side_effect=AssertionError('Metadata only')):
                    before=await client.get('/api/v2/execution-controls',params=params)
                self.assertEqual(before.status_code,200,before.text)
                self.assertEqual(before.json()['response_target']['status'],'waiting')
                self.assertNotIn(last['wait_id'],[row['wait_id'] for row in before.json()['pending_responses']])
                operation=await self.operation(client,params|dict(action='respond',request_id=uuid4().hex,payload={'text':'approved'}))
                await self.finished(client,operation)
            # Routers share Host receipts; changing HTTP adapters does not lose admission history.
            async with self.client(host) as client:
                self.assertEqual((await client.get('/api/v2/execution-operations/'+operation['operation_id'])).status_code,200)
                with patch.object(SourceReader,'read',side_effect=AssertionError('Metadata only')):
                    after=(await client.get('/api/v2/execution-controls',params=params)).json()
                    unknown=(await client.get('/api/v2/execution-controls',params=params|{'wait_id':'missing'})).json()
                    foreign=(await client.get('/api/v2/execution-controls',params=params|{'target_invocation_id':'foreign'})).json()
                self.assertEqual(after['response_target'],dict(invocation_id=last['invocation_id'],wait_id=last['wait_id'],status='resumed'))
                self.assertIsNone(unknown['response_target'])
                self.assertIsNone(foreign['response_target'])
                await host.cancel(root)
                cancelled=(await client.get('/api/v2/execution-controls',params=dict(invocation_id=root.invocation_id,target_invocation_id=first['pending_responses'][0]['invocation_id'],wait_id=first['pending_responses'][0]['wait_id']))).json()
                self.assertNotEqual(cancelled['response_target']['status'],'resumed')

    async def test_cancelled_wait_recovers_through_http_in_the_original_invocation(self):
        """Cancel and Recover create a new actionable Wait without replacing the Session or Invocation."""
        wf=Workflow('recover-http-wait',nodes=[Node('ask',Wait(Value,Value),recovery_mode=Recovery('replay_safe'))])
        async with self.host([wf]) as host:
            root=await host.execute(wf.id,{'text':'ask'},entry_node_id='ask')
            old=(await host.join(root)).waits[0].id
            await host.cancel(root,reason='try later')
            async with self.client(host) as client:
                op=await self.operation(client,dict(invocation_id=root.invocation_id,action='recover',request_id=uuid4().hex))
                await self.finished(client,op)
            restored=await host.join(root)
            reason=await host.get_execution_value(root.invocation_id,'invocation',root.invocation_id,'cancel_reason')
            self.assertFalse(reason['available'])
            self.assertNotEqual(restored.waits[0].id,old)
            controls=await host.get_execution_controls(root.invocation_id)
            self.assertEqual(len(controls['pending_responses']),1)
            await host.respond(root,restored.waits[0].id,{'text':'approved'})
            self.assertEqual((await host.join(root)).status,'completed')

    async def test_recovery_owns_single_flight_before_loading_history(self):
        """Concurrent identical Host requests share SQL reconstruction and reject a competing recovery request."""
        from autoagent.host import HostRunConflict
        count=0
        def work(value:Value)->Value:
            nonlocal count
            count+=1
            if count==1:
                raise ValueError('transient')
            return value
        wf=Workflow('recover-admission-race',nodes=[Node('work',work,recovery_mode=Recovery('replay_safe'))])
        async with self.host([wf]) as host:
            root=await host.execute(wf.id,{'text':'race'},entry_node_id='work')
            await host.join(root)
            version=(await host.get_execution_controls(root.invocation_id))['expected_version']
            entered,release=asyncio.Event(),asyncio.Event()
            original=host._persistence.read_recovery_bundle
            reads=0
            async def delayed(*args,**kwargs):
                nonlocal reads
                reads+=1
                entered.set()
                await release.wait()
                return await original(*args,**kwargs)
            with patch.object(host._persistence,'read_recovery_bundle',side_effect=delayed):
                first=asyncio.create_task(host.recover(root,request_id='same-request',expected_version=version))
                await asyncio.wait_for(entered.wait(),2)
                second=asyncio.create_task(host.recover(root,request_id='same-request',expected_version=version))
                try:
                    with self.assertRaises(HostRunConflict):
                        await host.recover(root,request_id='competing-request',expected_version=version)
                finally:
                    release.set()
                results=await asyncio.gather(first,second)
            self.assertTrue(all(result.status=='completed' for result in results))
            self.assertEqual(reads,1)
            self.assertEqual(count,2)


@unittest.skipUnless(POSTGRES_TEST_URL,'PostgreSQL test database is not configured.')
class PostgreSQLExecutionActionTests(ExecutionActionTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.database=PostgreSQLDatabase();self.url=await self.database.create()

    async def asyncTearDown(self):
        await self.database.close();await super().asyncTearDown()

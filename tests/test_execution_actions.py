"""Root control admission, bounded HTTP receipts and parent/child interaction."""
import asyncio
import tempfile
import threading
import time
import unittest
from pathlib import Path
from uuid import uuid4
from unittest.mock import patch
import httpx
from pydantic import BaseModel,ConfigDict
from autoagent import Edge,Node,ReceiveSignal,SignalEndpoint,Spawn,Wait,Workflow,Map,InputMappingContext
from autoagent.host import AutoAgentHost,HostSettings,HostCapacityError
from autoagent.host.persistence.reading import SourceReader
from autoagent.server import create_app,ServerSettings
from autoagent.server.operations import ExecutionOperations
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

    async def operation(self,client,body):
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
                self.assertTrue(controls['actions']['continue_execution']['available'])
                continued=await self.operation(client,dict(invocation_id=root.invocation_id,action='continue',request_id=uuid4().hex))
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
            manager=ExecutionOperations(host,ServerSettings(operation_concurrency=1,operation_receipts=2,operation_ttl_seconds=.05))
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
            manager=ExecutionOperations(host,ServerSettings(operation_concurrency=1,operation_receipts=2))
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
            await host.perform_execution_action(first.invocation_id,'continue')
            self.assertEqual((await host.join(first)).status,'waiting')
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
            self.assertEqual(controls['continue_reason'],'interrupted')
            self.assertTrue(controls['actions']['continue_execution']['available'])
            result=await host.perform_execution_action(root.invocation_id,'continue')
            self.assertEqual(result['status'],'waiting')
            self.assertFalse((await host.get_execution_controls(root.invocation_id))['actions']['continue_execution']['available'])

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


@unittest.skipUnless(POSTGRES_TEST_URL,'PostgreSQL test database is not configured.')
class PostgreSQLExecutionActionTests(ExecutionActionTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.database=PostgreSQLDatabase();self.url=await self.database.create()

    async def asyncTearDown(self):
        await self.database.close();await super().asyncTearDown()

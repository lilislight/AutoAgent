"""Registered Workflow execution through Host and its JSON HTTP boundary."""
import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from pydantic import BaseModel, ConfigDict
from autoagent import InputMappingContext, Node, Spawn, Wait, Workflow
from autoagent.host import AutoAgentHost, HostRunConflict, HostSettings
from autoagent.host.persistence import PersistenceConfig
from autoagent.host.persistence.reading import SourceReader
from autoagent.server import create_app
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_server import Value, identity


class RunValue(BaseModel):
    model_config=ConfigDict(extra='forbid')
    text: str
    count: int=2


def model_echo(value: RunValue) -> RunValue:
    assert isinstance(value,RunValue)
    return value


def raw_input(ctx: InputMappingContext) -> Value:
    return {'text':ctx.invocation_input['payload']}


def session_label(ctx: InputMappingContext) -> Value:
    return {'text':ctx.session_context['label']}


class WorkflowRunTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.url='sqlite+aiosqlite:///'+str(Path(self.temp.name)/'run.db')

    async def asyncTearDown(self):
        self.temp.cleanup()

    def host(self,workflows,**settings):
        return AutoAgentHost(workflows=workflows,settings=HostSettings(database_url=self.url,**settings),environment={})

    def client(self,host):
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(host,ui=False)),base_url='http://test')

    async def revision(self,host,wid):
        return (await host.list_workflows(workflow_id=wid)).items[0]['revision_id']

    async def test_run_spec_is_exact_and_does_not_read_business_values_or_infer_mapping_input(self):
        """Run metadata contains declared Entries and distinguishes raw mapping input from Operator contracts."""
        wf=Workflow('run-spec',nodes=[Node('model',model_echo),Node('mapped',identity,input_mapping=raw_input)])
        async with self.host([wf]) as host:
            revision=await self.revision(host,wf.id)
            with patch.object(SourceReader,'read',side_effect=AssertionError('No business payloads')):
                listing=await host.get_workflow_run_spec(wf.id,revision_id=revision)
                self.assertTrue(all(row['input_schema'] is None for row in listing['entries']))
                spec=await host.get_workflow_run_spec(wf.id,revision_id=revision,entry_node_id='model')
            entries={row['node_id']:row for row in spec['entries']}
            self.assertTrue(spec['can_run'])
            self.assertEqual(set(entries),{'model','mapped'})
            self.assertEqual(entries['model']['input_schema']['required'],['text'])
            self.assertEqual(entries['mapped']['input_mode'],'raw_json')
            self.assertIsNone(entries['mapped']['input_schema'])

    async def test_http_run_restores_models_defaults_and_admits_selected_entry(self):
        """JSON is decoded to the registered Python model before Core receives the selected Entry input."""
        wf=Workflow('run-model',nodes=[Node('model',model_echo),Node('mapped',identity,input_mapping=raw_input)])
        async with self.host([wf]) as host:
            revision=await self.revision(host,wf.id)
            async with self.client(host) as client:
                response=await client.post('/api/v2/executions',json=dict(workflow_id=wf.id,revision_id=revision,entry_node_id='model',input={'text':'typed'}))
                self.assertEqual(response.status_code,202,response.text)
                first=response.json()
                result=await host.join(first['invocation_id'])
                self.assertEqual(result.status,'completed')
                self.assertEqual(result.output,{'model':{'text':'typed','count':2}})
                response=await client.post('/api/v2/executions',json=dict(workflow_id=wf.id,revision_id=revision,entry_node_id='mapped',input={'payload':'raw'}))
                self.assertEqual(response.status_code,202,response.text)
                self.assertEqual((await host.join(response.json()['invocation_id'])).output,{'mapped':{'text':'raw'}})

    async def test_unregistered_revision_stays_readable_but_cannot_run_even_with_registered_workflow_id(self):
        """An old same-named Revision cannot be silently replaced by the Host's registered Revision."""
        old=Workflow('run-version',version='1',nodes=[Node('entry',identity)])
        async with self.host([old]) as host:
            revision=await self.revision(host,old.id)
        current=Workflow('run-version',version='2',nodes=[Node('entry',identity)])
        async with self.host([current]) as host:
            spec=await host.get_workflow_run_spec(current.id,revision_id=revision)
            self.assertFalse(spec['can_run'])
            self.assertEqual(spec['code'],'workflow_unregistered')
            self.assertNotEqual(spec['current_revision_id'],revision)
            async with self.client(host) as client:
                response=await client.post('/api/v2/executions',json=dict(workflow_id=current.id,revision_id=revision,entry_node_id='entry',input={'text':'old'}))
                self.assertEqual(response.status_code,409,response.text)
                self.assertEqual(response.json()['detail']['code'],'workflow_unregistered')

    async def test_existing_session_reuse_rejects_busy_missing_and_child_sessions(self):
        """Eligibility and final admission forbid replacing active executions or using Child Sessions as Roots."""
        child=Workflow('run-child',nodes=[Node('ask',Wait(Value,Value))])
        parent=Workflow('run-parent',nodes=[Node('spawn',Spawn(child,'ask'))])
        echo=Workflow('run-echo',nodes=[Node('entry',identity)])
        async with self.host([parent,echo]) as host:
            revision=await self.revision(host,echo.id)
            root=await host.execute(parent.id,{'text':'child'},entry_node_id='spawn')
            await host.join(root)
            child_row=(await host.list_child_executions(root.invocation_id)).items[0]
            for sid,code in ((root.session_id,'session_busy'),(child_row['session_id'],'child_session'),('missing','session_not_found')):
                spec=await host.get_workflow_run_spec(echo.id,revision_id=revision,session_id=sid)
                self.assertFalse(spec['can_run']);self.assertEqual(spec['code'],code)
                with self.assertRaises(HostRunConflict) as error:
                    await host.execute_json(echo.id,{'text':'cannot'},revision_id=revision,entry_node_id='entry',session_id=sid,require_existing_session=True)
                self.assertEqual(error.exception.code,code)
            await host.cancel(root)
            self.assertTrue((await host.get_workflow_run_spec(echo.id,revision_id=revision,session_id=root.session_id))['can_run'])
            second=await host.execute_json(echo.id,{'text':'same session, different workflow'},revision_id=revision,entry_node_id='entry',session_id=root.session_id,require_existing_session=True)
            self.assertEqual(second.session_id,root.session_id)
            self.assertNotEqual(second.invocation_id,root.invocation_id)
            self.assertEqual((await host.join(second)).status,'completed')

    async def test_invalid_input_entry_and_session_parameters_are_rejected_before_execution(self):
        """Bad JSON contracts, Entry IDs and ambiguous Session parameters do not start a Workflow."""
        wf=Workflow('run-invalid',nodes=[Node('entry',model_echo)])
        async with self.host([wf]) as host:
            revision=await self.revision(host,wf.id)
            body=dict(workflow_id=wf.id,revision_id=revision,entry_node_id='entry',input={'text':'ok'})
            async with self.client(host) as client:
                for override in ({'entry_node_id':'not-an-entry'},{'input':{'text':1}},{'session_mode':'existing'},{'session_mode':'new','session_id':'not-new'},{'session_mode':'existing','session_id':'some','session_context':{}}):
                    response=await client.post('/api/v2/executions',json=body|override)
                    self.assertIn(response.status_code,(400,422),response.text)
            self.assertEqual((await host.list_executions()).items,())

    async def test_session_context_is_preserved_and_concurrent_reuse_has_one_admission(self):
        """New Session context survives reuse, and simultaneous Runs cannot both replace its Invocation."""
        contextual=Workflow('run-context',nodes=[Node('entry',identity,input_mapping=session_label)])
        waiting=Workflow('run-context-wait',nodes=[Node('ask',Wait(Value,Value))])
        async with self.host([contextual,waiting]) as host:
            revision=await self.revision(host,contextual.id)
            async with self.client(host) as client:
                body=dict(workflow_id=contextual.id,revision_id=revision,entry_node_id='entry',input={})
                response=await client.post('/api/v2/executions',json=body|dict(session_context={'label':'preserved'}))
                self.assertEqual(response.status_code,202,response.text)
                first=response.json()
                self.assertEqual((await host.join(first['invocation_id'])).output,{'text':'preserved'})
                response=await client.post('/api/v2/executions',json=body|dict(session_mode='existing',session_id=first['session_id']))
                self.assertEqual(response.status_code,202,response.text)
                self.assertEqual((await host.join(response.json()['invocation_id'])).output,{'text':'preserved'})
                waiting_revision=await self.revision(host,waiting.id)
                request=dict(workflow_id=waiting.id,revision_id=waiting_revision,entry_node_id='ask',input={'text':'ask'},session_mode='existing',session_id=first['session_id'])
                attempts=await asyncio.gather(*(client.post('/api/v2/executions',json=request) for _ in range(2)))
                self.assertEqual(sorted(r.status_code for r in attempts),[202,409])

    async def test_buffered_run_returns_accepted_before_trace_commit_without_forcing_flush(self):
        """A successful Run can precede its durable trace; viewing and admission must not flush the queue."""
        wf=Workflow('run-buffer',nodes=[Node('ask',Wait(Value,Value))])
        async with self.host([wf],persistence=PersistenceConfig(mode='buffered',batch_delay_ms=5000)) as host:
            revision=await self.revision(host,wf.id)
            async with self.client(host) as client:
                with patch.object(host._persistence,'flush',side_effect=AssertionError('Run must not force flush')):
                    response=await client.post('/api/v2/executions',json=dict(workflow_id=wf.id,revision_id=revision,entry_node_id='ask',input={'text':'pending'}))
                    self.assertEqual(response.status_code,202,response.text)
                    iid=response.json()['invocation_id']
                    trace=await client.get('/api/v2/execution-trace',params={'invocation_id':iid})
                    self.assertEqual(trace.status_code,404)
                await host._persistence.flush()
                trace=await client.get('/api/v2/execution-trace',params={'invocation_id':iid})
                self.assertEqual(trace.status_code,200,trace.text)


@unittest.skipUnless(POSTGRES_TEST_URL,'PostgreSQL test database is not configured.')
class PostgreSQLWorkflowRunTests(WorkflowRunTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.database=PostgreSQLDatabase()
        self.url=await self.database.create()

    async def asyncTearDown(self):
        await self.database.close()
        await super().asyncTearDown()

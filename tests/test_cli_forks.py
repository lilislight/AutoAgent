"""LLM-facing Fork commands preserve source identity and report submission uncertainty."""
import asyncio
from contextlib import AsyncExitStack, redirect_stdout, redirect_stderr
import io
import json
import os
import unittest
from unittest.mock import patch

import httpx

from autoagent.cli import CliError
from autoagent.cli.main import build_parser, main
from autoagent.cli.management import run
from autoagent.cli.remote import ServerClient
from autoagent.host import AutoAgentHost
from autoagent.server import create_app
from tests.test_cli_execution import ProjectFixture
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase


class LocalForkCliTests(ProjectFixture,unittest.TestCase):
    def setUp(self):
        self.prepare_project()
        self.environ=patch.dict(os.environ,self.environment(),clear=True)
        self.environ.start()
        async def seed():
            async with AutoAgentHost.from_project(self.root) as host:
                self.ref=await host.execute('cli-approval',{'text':'source'},entry_node_id='ask')
                await host.join(self.ref)
                await host._persistence.flush()
                self.sequence=(await host.list_execution_events(self.ref.invocation_id,names=['node_occurrence.waiting'])).items[0]['sequence']
        with redirect_stdout(io.StringIO()):
            asyncio.run(seed())

    def tearDown(self):
        self.environ.stop()
        self.cleanup_project()

    def cli(self,*args):
        out,err=io.StringIO(),io.StringIO()
        with redirect_stdout(out),redirect_stderr(err):
            code=main(['--project',str(self.root),*args,'--format','json'])
        return code,json.loads(out.getvalue())

    def test_plan_and_local_fork_use_exact_source_and_new_session(self):
        """One concise command checks replay requirements or creates an independently waiting Root."""
        args=('invocation','fork',self.ref.invocation_id,'--sequence',str(self.sequence))
        code,report=self.cli(*args,'--plan')
        self.assertEqual(code,0,report)
        self.assertTrue(report['data']['available'])
        code,report=self.cli(*args,'--session-id','cli-fork')
        self.assertEqual(code,0,report)
        self.assertEqual(report['data']['session_id'],'cli-fork')
        self.assertEqual(report['data']['source_invocation_id'],self.ref.invocation_id)
        self.assertEqual(report['data']['execution']['status'],'waiting')
        self.assertNotEqual(report['data']['invocation_id'],self.ref.invocation_id)

    def test_invalid_positions_and_plan_only_options_are_explicit(self):
        """Argument mistakes never silently select another position or create another Session."""
        args=('invocation','fork',self.ref.invocation_id)
        for extra in (('--sequence','0'),('--sequence',str(self.sequence),'--plan','--session-id','unused')):
            code,report=self.cli(*args,*extra)
            self.assertEqual(code,2,report)
            self.assertEqual(report['error']['code'],'USAGE_ERROR')


class RemoteForkCliTests(ProjectFixture,unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.prepare_project()
        self.environ=patch.dict(os.environ,self.environment(),clear=True)
        self.environ.start()
        self.stack=AsyncExitStack()
        self.host=await self.stack.enter_async_context(AutoAgentHost.from_project(self.root))
        self.ref=await self.host.execute('cli-approval',{'text':'source'},entry_node_id='ask')
        await self.host.join(self.ref)
        await self.host._persistence.flush()
        self.sequence=(await self.host.list_execution_events(self.ref.invocation_id,names=['node_occurrence.waiting'])).items[0]['sequence']
        self.app=create_app(self.host,ui=False)

    async def asyncTearDown(self):
        await self.stack.aclose()
        self.environ.stop()
        self.cleanup_project()

    async def query(self,*args,transport=None):
        options=build_parser().parse_args(['--server','http://test/api/v2','invocation','fork',self.ref.invocation_id,
            '--sequence',str(self.sequence),*args])
        options.request_timeout=30
        factory=lambda url,timeout:ServerClient(url,timeout=timeout,transport=transport or httpx.ASGITransport(app=self.app))
        with patch('autoagent.cli.remote.ServerClient',side_effect=factory):
            return await run(options)

    async def test_server_plan_and_cli_submission_share_host_semantics(self):
        """HTTP plan and execution use the same Root cut, admission rules and result fields as local mode."""
        _,plan,code=await self.query('--plan')
        self.assertEqual(code,0)
        self.assertTrue(plan['data']['available'])
        _,report,code=await self.query('--session-id','server-fork')
        self.assertEqual(code,0,report)
        self.assertEqual(report['data']['execution']['status'],'waiting')
        trace=await self.host.get_execution_trace(report['data']['invocation_id'])
        self.assertEqual(trace['fork']['source_sequence'],self.sequence)
        self.assertEqual((await self.host.get_execution(self.ref)).status,'waiting')
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app),base_url='http://test/api/v2') as client:
            response=await client.post('/execution-forks',json=dict(invocation_id=self.ref.invocation_id,sequence=self.sequence,session_id='server-fork'))
        self.assertEqual(response.status_code,409,response.text)

    async def test_mutation_connection_loss_is_unknown_and_is_never_retried(self):
        """Fork transport failure performs one POST and reports an unknown outcome instead of resubmission."""
        requests=[]
        def fail(request):
            requests.append(request)
            raise httpx.ReadError('lost response')
        with self.assertRaises(CliError) as caught:
            await self.query(transport=httpx.MockTransport(fail))
        self.assertEqual(caught.exception.code,'OUTCOME_UNKNOWN')
        self.assertEqual(len(requests),1)
        self.assertTrue(json.loads(requests[0].content)['session_id'])

    async def test_server_rejects_invalid_sequence_without_creating_work(self):
        """Request validation rejects an invalid event boundary before Host mutation."""
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app),base_url='http://test/api/v2') as client:
            response=await client.post('/execution-forks',json=dict(invocation_id=self.ref.invocation_id,sequence=0))
        self.assertEqual(response.status_code,422)
        self.assertEqual(len((await self.host.list_execution_sessions()).items),1)


@unittest.skipUnless(POSTGRES_TEST_URL,'PostgreSQL test URL is required.')
class PostgreSQLRemoteForkCliTests(RemoteForkCliTests):
    def prepare_project(self):
        super().prepare_project()
        path=self.root/'.env'
        path.write_text(path.read_text().replace('sqlite+aiosqlite:///'+str(self.database),self.url))

    async def asyncSetUp(self):
        self.pg=PostgreSQLDatabase()
        self.url=await self.pg.create()
        await super().asyncSetUp()

    async def asyncTearDown(self):
        await super().asyncTearDown()
        await self.pg.close()

"""Agent product preferences are small, live, and independent of model-bound submissions."""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import tempfile
import threading
import unittest
from uuid import uuid4

import httpx

from autoagent.agent_app import BUILTIN_AGENT_WORKFLOW_ID, builtin_agent_workflow
from autoagent.agent import (
    AgentAccess, AgentHostSettings, AgentModelSettings, AgentSessionSettings, Tool,
)
from autoagent.host import AutoAgentHost, HostSettings
from autoagent.server import create_app
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import ScriptedProvider, answer, calls


class SessionControlContracts:
    async def database_url(self, root):
        return 'sqlite+aiosqlite:///' + str(root / 'runtime.db')

    @asynccontextmanager
    async def product(self, provider, *, access=None, tools=(), extra_workflows=()):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            @asynccontextmanager
            async def lifespan(setup):
                setup.agent_context.resources.register_provider('offline', provider)
                setup.agent_context.set_defaults(AgentHostSettings(model=AgentModelSettings(provider='offline',
                    model='A', streaming=False, max_output_tokens=512), access=access or AgentAccess()))
                for handler in tools:
                    setup.agent_context.resources.register_tool(handler, workflows={BUILTIN_AGENT_WORKFLOW_ID})
                yield
            async with AutoAgentHost(root=root, workflows=[builtin_agent_workflow(execution=False), *extra_workflows],
                settings=HostSettings(database_url=await self.database_url(root)), lifespan=lifespan, environment={}) as host:
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(host, ui=False)), base_url='http://test') as client:
                    yield host, client

    async def test_conversation_preferences_exclude_host_parameters_and_titles_are_stable(self):
        """Only model and approval are writable; first-message titles survive later turns and rename independently."""
        provider = ScriptedProvider(answer(), answer())
        async with self.product(provider) as (host, client):
            sent = await client.post('/api/v2/agent/messages', json={'session_id':'chat', 'message':'First title'})
            self.assertEqual(sent.status_code, 202, sent.text)
            self.assertEqual((await host.join(sent.json()['invocation_id'])).status, 'completed')
            record = (await client.get('/api/v2/agent/settings', params={'session_id':'chat'})).json()
            self.assertEqual(set(record['settings']), {'model','approval_mode'})
            invalid = await client.put('/api/v2/agent/settings', json={'session_id':'chat', 'expected_version':record['version'],
                'settings':{**record['settings'], 'access':{'default':'allow'}}})
            self.assertEqual(invalid.status_code, 422)
            invalid = await client.put('/api/v2/agent/settings', json={'session_id':'chat', 'expected_version':record['version'],
                'settings':{**record['settings'], 'model':{**record['settings']['model'], 'streaming':True}}})
            self.assertEqual(invalid.status_code, 422)
            second = await client.post('/api/v2/agent/messages', json={'session_id':'chat','message':'Different second title'})
            await host.join(second.json()['invocation_id'])
            recent = (await client.get('/api/v2/agent/sessions')).json()['items']
            self.assertEqual(recent[0]['title'], 'First title')
            renamed = await client.patch('/api/v2/agent/sessions/chat/title', json={
                'title':'Renamed conversation', 'expected_version':record['metadata_version']})
            self.assertEqual(renamed.status_code, 200, renamed.text)
            self.assertEqual(renamed.json()['version'], record['version'])
            self.assertEqual(renamed.json()['title'], 'Renamed conversation')
            self.assertEqual(provider.requests[0].max_output_tokens, 512)
            self.assertFalse(provider.requests[0].streaming)

    async def test_queue_captures_models_and_message_keeps_the_running_invocation_model(self):
        """Queued submissions retain their model despite later Session changes and steering uses the active model."""
        started, release = threading.Event(), threading.Event()
        async def first(request):
            started.set()
            await asyncio.to_thread(release.wait, 10)
            return answer('First answer')
        provider = ScriptedProvider(first, answer('Steered answer'), answer('B answer'), answer('C answer'))
        async with self.product(provider) as (host, client):
            async def submit(message, model=None, **extra):
                value = {'session_id':'chat','message':message,'request_id':uuid4().hex,**extra}
                if model:
                    value['model']={'provider':'offline','model':model}
                result = await client.post('/api/v2/agent/queue', json=value)
                self.assertEqual(result.status_code, 202, result.text)
                return result.json()
            first_item = await submit('First question', 'A')
            self.assertTrue(await asyncio.to_thread(started.wait, 10))
            b, c = await submit('Second question', 'B'), await submit('Third question', 'C')
            await host.agent_app.context.configure('chat', AgentSessionSettings(model={'provider':'offline','model':'D'}))
            retried = await client.post('/api/v2/agent/queue', json={
                'session_id':'chat','message':b['message'],'request_id':b['id'],
                'initial_settings':{'model':{'provider':'offline','model':'A'},'approval_mode':'policy'}})
            self.assertEqual(retried.status_code,202,retried.text)
            self.assertEqual(retried.json()['model']['model'],'B')
            conflicting = await client.post('/api/v2/agent/queue', json={
                'session_id':'chat','message':b['message'],'request_id':b['id'],
                'model':{'provider':'offline','model':'different'}})
            self.assertEqual(conflicting.status_code,409)
            message = await submit('Steering message', send_to=first_item['invocation_id'])
            self.assertEqual(message['state'], 'sent')
            release.set()
            self.assertEqual((await host.join(first_item['invocation_id'])).status, 'completed')
            async with asyncio.timeout(20):
                while True:
                    current = await host.agent_app.context.messages.storage('rows', 'chat', item_id=c['id'])
                    if current and current[0]['state'] == 'started':
                        await host.join(current[0]['invocation_id'])
                        break
                    await asyncio.sleep(.02)
            self.assertEqual([request.model for request in provider.requests], ['A','A','B','C'])
            self.assertEqual(b['model']['model'], 'B')

    async def test_approval_mode_update_resolves_an_existing_wait_without_recovery(self):
        """A committed auto-approve preference reevaluates the current approval Wait without pause or recovery."""
        effects=[]
        def publish(text: str) -> str:
            """Publish a test record."""
            effects.append(text)
            return text
        async with self.product(ScriptedProvider(calls('publish'), answer()),
            access=AgentAccess(rules={'publish':'ask'}), tools=[publish]) as (host, client):
            sent = await client.post('/api/v2/agent/messages',json={'session_id':'approval','message':'Publish'})
            iid=sent.json()['invocation_id']
            self.assertEqual((await host.join(iid)).status, 'waiting')
            record=await host.agent_app.context.get_settings('approval')
            await host.agent_app.context.configure('approval', record.settings.model_copy(update={'approval_mode':'auto_approve'}), expected_version=record.version)
            async with asyncio.timeout(10):
                while True:
                    result=await host.join(iid)
                    if result.status in {'completed', 'failed', 'cancelled'}:
                        break
                    await asyncio.sleep(.02)
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual(effects, ['hello'])

    async def test_restart_keeps_preferences_but_reloads_host_generation_defaults(self):
        """Restart retains Session model choice while generation parameters come exclusively from startup."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            url = await self.database_url(root)
            provider = ScriptedProvider(answer(),answer())
            maximum = 512
            @asynccontextmanager
            async def lifespan(setup):
                setup.agent_context.resources.register_provider('offline',provider)
                setup.agent_context.set_defaults(AgentHostSettings(model=AgentModelSettings(
                    provider='offline',model='startup',max_output_tokens=maximum)))
                yield
            options = dict(root=root, workflows=[builtin_agent_workflow(execution=False)],
                settings=HostSettings(database_url=url),lifespan=lifespan,environment={})
            async with AutoAgentHost(**options) as host:
                ref = await host.agent_app.context.start_conversation('saved','First message',unbound=True)
                await host.join(ref)
                await host.agent_app.context.configure('saved',AgentSessionSettings(model={'provider':'offline','model':'selected'}))
            maximum = 1024
            async with AutoAgentHost(**options) as host:
                saved = await host.agent_app.context.get_settings('saved')
                self.assertEqual(saved.settings.model.model,'selected')
                ref = await host.agent_app.context.start_conversation('saved','Continue')
                self.assertEqual((await host.join(ref)).status,'completed')
                self.assertEqual(provider.requests[-1].max_output_tokens,1024)
                self.assertEqual(provider.requests[-1].model,'selected')
                self.assertEqual((await host.agent_app.context.get_settings('saved')).title,'First message')

    async def test_auto_approval_records_authorization_but_does_not_answer_questions(self):
        """System approval resolves only Tool authorization; a subsequent user question stays waiting."""
        from tests.test_agent_ask_user import ask_call, tool_response
        from autoagent.agent_app.models import ApplicationSessionSettings
        effects = []
        def publish(text: str) -> str:
            effects.append(text)
            return text
        provider = ScriptedProvider(calls('publish'), tool_response(ask_call()))
        async with self.product(provider, access=AgentAccess(rules={'publish':'ask'}), tools=[publish]) as (host, client):
            await host.agent_app.context.configure('auto', ApplicationSessionSettings(
                model={'provider':'offline','model':'A'}, approval_mode='auto_approve'))
            sent = await client.post('/api/v2/agent/messages', json={'session_id':'auto','message':'Begin'})
            iid = sent.json()['invocation_id']
            async with asyncio.timeout(10):
                while True:
                    result = await host.join(iid)
                    if result.status == 'waiting' and result.waits and result.waits[0].request.get('kind') == 'agent.ask_user':
                        break
                    self.assertNotEqual(result.status, 'failed', result.error)
                    await asyncio.sleep(.02)
            self.assertEqual(effects, ['hello'])
            await host._persistence.flush()
            events = await host.list_execution_events(iid, names=('wait.resumed',))
            self.assertEqual(len(events.items), 1)
            event = await host.get_execution_event(iid, events.items[0]['event_id'], field='payload')
            self.assertEqual(event['value']['response']['source'], 'system')

    async def test_product_allow_replaces_project_and_nested_policy(self):
        """A product override is final even when the original Project ToolSet asks or denies."""
        from autoagent.agent import Tool, ToolSet
        from autoagent.agent_app import ResourceControl, ResourcePermissions
        effects = []
        def publish(text: str) -> str:
            effects.append(text)
            return text
        protected = ToolSet([Tool(publish)], policy=lambda call, context: {'action':'deny'})['publish']
        provider = ScriptedProvider(calls('publish'), answer())
        async with self.product(provider, access=AgentAccess(rules={'publish':'ask'}), tools=[protected]) as (host, client):
            manager = host.agent_app.context.shared_resources
            entry = next(row for row in (await manager.describe())['resources'] if row['name'] == 'publish')
            self.assertEqual(entry['default_decision'], 'ask')
            self.assertTrue(entry['parameter_policy'])
            await manager.save(ResourceControl(name='Publish', resource_id=entry['management_id'],
                permissions=ResourcePermissions(default='allow')))
            effective = next(row for row in (await manager.describe())['resources'] if row['name'] == 'publish')
            self.assertEqual(effective['default_decision'], 'allow')
            self.assertEqual(effective['permission_source'], 'tool')
            sent = await client.post('/api/v2/agent/messages', json={'session_id':'override','message':'Begin'})
            result = await host.join(sent.json()['invocation_id'])
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual(effects, ['hello'])


class SQLiteSessionControlTests(SessionControlContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL URL not configured')
class PostgreSQLSessionControlTests(SessionControlContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self, root):
        database=PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

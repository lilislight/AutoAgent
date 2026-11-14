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
                setup.providers.register('offline', provider)
                setup.applications["agent"].context.set_defaults(AgentHostSettings(model=AgentModelSettings(provider='offline',
                    model='A', streaming=False, max_output_tokens=512), access=access or AgentAccess()))
                for handler in tools:
                    setup.applications["agent"].context.resources.register_tool(handler, workflows={BUILTIN_AGENT_WORKFLOW_ID})
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

    async def test_product_continues_past_twenty_tool_rounds(self):
        """The built-in Agent has no default round cutoff for a longer tool-based task."""
        completed = []
        def step(text: str) -> str:
            completed.append(text)
            return text
        provider = ScriptedProvider(*(calls('step') for _ in range(21)), answer())
        async with self.product(provider, tools=[step]) as (host, client):
            sent = await client.post('/api/v2/agent/messages', json={'session_id':'long-task','message':'Complete all steps'})
            self.assertEqual(sent.status_code, 202, sent.text)
            result = await host.join(sent.json()['invocation_id'])
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual(len(completed), 21)
            self.assertEqual(len(provider.requests), 22)

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
            await host.applications["agent"].context.configure('chat', AgentSessionSettings(model={'provider':'offline','model':'D'}))
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
                    current = await host.applications["agent"].context.messages.storage('rows', 'chat', item_id=c['id'])
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
            record=await host.applications["agent"].context.get_settings('approval')
            await host.applications["agent"].context.configure('approval', record.settings.model_copy(update={'approval_mode':'auto_approve'}), expected_version=record.version)
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
                setup.providers.register('offline',provider)
                setup.applications["agent"].context.set_defaults(AgentHostSettings(model=AgentModelSettings(
                    provider='offline',model='startup',max_output_tokens=maximum)))
                yield
            options = dict(root=root, workflows=[builtin_agent_workflow(execution=False)],
                settings=HostSettings(database_url=url),lifespan=lifespan,environment={})
            async with AutoAgentHost(**options) as host:
                ref = await host.applications["agent"].context.start_conversation('saved','First message',unbound=True)
                await host.join(ref)
                await host.applications["agent"].context.configure('saved',AgentSessionSettings(model={'provider':'offline','model':'selected'}))
            maximum = 1024
            async with AutoAgentHost(**options) as host:
                saved = await host.applications["agent"].context.get_settings('saved')
                self.assertEqual(saved.settings.model.model,'selected')
                ref = await host.applications["agent"].context.start_conversation('saved','Continue')
                self.assertEqual((await host.join(ref)).status,'completed')
                self.assertEqual(provider.requests[-1].max_output_tokens,1024)
                self.assertEqual(provider.requests[-1].model,'selected')
                self.assertEqual((await host.applications["agent"].context.get_settings('saved')).title,'First message')

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
            await host.applications["agent"].context.configure('auto', ApplicationSessionSettings(
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
            view = await host.get_execution_action_spec(iid, 'respond', wait_id=result.waits[0].id)
            self.assertEqual(view['review'], {'reviewer':'user','requires_user_input':True})
            self.assertEqual(view['request']['kind'], 'agent.ask_user')
            history = await host.get_user_events(iid, limit=1000)
            approval = next(row['payload'] for row in history.items if row['kind'] == 'agent.interaction'
                and row['payload']['type'] == 'approval')
            self.assertEqual(approval['state'], 'resolved')
            self.assertEqual(approval['resolution']['response']['resolutions'][0]['source'], 'system')

    async def test_product_allow_replaces_project_and_nested_policy(self):
        """A product override is final even when the original Project ToolSet asks or denies."""
        from autoagent.agent import Tool, ToolSet
        from autoagent.agent_app import ResourceControl
        from autoagent.agent import ToolPolicy
        effects = []
        def publish(text: str) -> str:
            effects.append(text)
            return text
        protected = ToolSet([Tool(publish)], policy=lambda call, context: {'action':'deny'})['publish']
        provider = ScriptedProvider(calls('publish'), answer())
        async with self.product(provider, access=AgentAccess(rules={'publish':'ask'}), tools=[protected]) as (host, client):
            manager = host.applications["agent"].context.shared_resources
            entry = next(row for row in (await manager.describe())['resources'] if row['name'] == 'publish')
            self.assertEqual(entry['default_decision'], 'ask')
            self.assertTrue(entry['parameter_policy'])
            await manager.save(ResourceControl(name='Publish', resource_id=entry['management_id'],
                permissions=ToolPolicy(default='allow')))
            effective = next(row for row in (await manager.describe())['resources'] if row['name'] == 'publish')
            self.assertEqual(effective['default_decision'], 'allow')
            self.assertEqual(effective['permission_source'], 'tool')
            sent = await client.post('/api/v2/agent/messages', json={'session_id':'override','message':'Begin'})
            result = await host.join(sent.json()['invocation_id'])
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual(effects, ['hello'])

    async def test_cancelled_task_is_not_recoverable_and_new_turn_keeps_its_history(self):
        """Cancellation ends the product task while the next Invocation retains its recorded conversation."""
        from autoagent.host import HostRunConflict
        def publish(text: str) -> str:
            """Publish a record."""
            return text
        provider = ScriptedProvider(calls('publish'), answer('Next task'))
        async with self.product(provider, access=AgentAccess(rules={'publish':'ask'}), tools=[publish]) as (host, client):
            first = await host.applications["agent"].start_conversation('cancelled-chat', 'Keep this original goal')
            self.assertEqual((await host.join(first)).status, 'waiting')
            await host.cancel(first)
            controls = await host.get_execution_controls(first.invocation_id)
            self.assertFalse(controls['actions']['recover']['available'])
            with self.assertRaises(HostRunConflict):
                await host.recover(first)
            second = await host.applications["agent"].start_conversation('cancelled-chat', 'Continue with a new task')
            result = await host.join(second)
            self.assertEqual(result.status, 'completed', result.error)
            self.assertNotEqual(first.invocation_id, second.invocation_id)
            self.assertIn('Keep this original goal', str(provider.requests[-1].input))
            self.assertIn('unknown outcome', str(provider.requests[-1].input))

    async def test_pause_finishes_inflight_call_and_resume_keeps_the_bound_model(self):
        """Pause leaves the in-flight LLM intact and resumption ignores later Session model selections."""
        started, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        async def first(request):
            started.set()
            while not release.is_set():
                await asyncio.sleep(.01)
            return answer('Recorded before pause')
        provider = ScriptedProvider(first)
        async with self.product(provider) as (host, client):
            ref = await host.applications["agent"].start_conversation('pause-chat', 'Run this task')
            self.assertTrue(await asyncio.to_thread(started.wait, 5))
            pause = asyncio.create_task(host.pause(ref))
            try:
                async with asyncio.timeout(5):
                    while (await host.get_execution_controls(ref.invocation_id))['control_state'] != 'pausing':
                        await asyncio.sleep(.01)
                self.assertFalse(pause.done())
                release.set()
                self.assertEqual((await pause).control_state, 'paused')
                record = await host.applications["agent"].context.get_settings('pause-chat')
                await host.applications["agent"].configure('pause-chat', record.settings.model_copy(update={
                    'model':record.settings.model.model_copy(update={'model':'B','reasoning_effort':'high'})}), expected_version=record.version)
                self.assertTrue((await host.get_execution_controls(ref.invocation_id))['actions']['recover']['available'])
                await host.recover(ref)
                self.assertEqual((await host.join(ref)).status, 'completed')
                self.assertEqual(len(provider.requests), 1)
                self.assertEqual((await host.applications["agent"].context.get_snapshot(ref.invocation_id)).settings.model.model, 'A')
                self.assertEqual((await host.applications["agent"].context.get_settings('pause-chat')).settings.model.model, 'B')
            finally:
                release.set()
                await asyncio.gather(pause, return_exceptions=True)

    async def test_restart_cancels_product_waits_without_resuming_the_llm(self):
        """A Host restart ends pending product tasks and leaves their next user turn available."""
        from autoagent.host import HostRunConflict
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            url = await self.database_url(root)
            provider = ScriptedProvider(calls('publish'), answer('After restart'))
            def publish(text: str) -> str:
                """Publish a record."""
                return text
            @asynccontextmanager
            async def lifespan(setup):
                setup.providers.register('offline', provider)
                setup.applications["agent"].context.set_defaults(AgentHostSettings(model=AgentModelSettings(provider='offline', model='A'), access=AgentAccess(rules={'publish':'ask'})))
                setup.applications["agent"].context.resources.register_tool(publish, workflows={BUILTIN_AGENT_WORKFLOW_ID})
                yield
            options = dict(root=root, workflows=[builtin_agent_workflow(execution=False)], settings=HostSettings(database_url=url), lifespan=lifespan, environment={})
            async with AutoAgentHost(**options) as host:
                ref = await host.applications["agent"].start_conversation('restart-chat', 'Original task')
                self.assertEqual((await host.join(ref)).status, 'waiting')
            async with AutoAgentHost(**options) as host:
                self.assertEqual((await host.get_execution(ref)).status, 'cancelled')
                self.assertEqual(len(provider.requests), 1)
                with self.assertRaises(HostRunConflict):
                    await host.recover(ref)
                current = await host.applications["agent"].start_conversation('restart-chat', 'Continue')
                self.assertEqual((await host.join(current)).status, 'completed')
                self.assertIn('Original task', str(provider.requests[-1].input))

    async def test_submission_updates_latest_model_without_old_queue_activation_overwriting_it(self):
        """Each submitted model is saved, but starting an older queued item cannot reset that choice."""
        release, started = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        async def first(request):
            started.set()
            while not release.is_set():
                await asyncio.sleep(.01)
            return answer()
        async with self.product(ScriptedProvider(first, answer(), answer())) as (host, client):
            queue = host.applications["agent"].context.messages
            one = await queue.submit('models', 'First', 'first', model={'provider':'offline','model':'A','reasoning_effort':'low'})
            self.assertTrue(await asyncio.to_thread(started.wait, 5))
            await queue.submit('models', 'Second', 'second', model={'provider':'offline','model':'B','reasoning_effort':'medium'})
            await queue.submit('models', 'Third', 'third', model={'provider':'offline','model':'C','reasoning_effort':'high'})
            saved = await host.applications["agent"].context.get_settings('models')
            self.assertEqual((saved.settings.model.model, saved.settings.model.reasoning_effort), ('C','high'))
            release.set()
            await host.join(one['invocation_id'])
            async with asyncio.timeout(10):
                while True:
                    rows = await host.applications["agent"].storage.queue('rows','models',item_id='third')
                    if rows[0]['state'] == 'started':
                        await host.join(rows[0]['invocation_id'])
                        break
                    await asyncio.sleep(.02)
            self.assertEqual((await host.applications["agent"].context.get_settings('models')).settings.model.model, 'C')

    async def test_cancelled_map_keeps_completed_tool_results_in_the_next_turn(self):
        """A successful Map unit survives cancellation before the whole Node can bind its output."""
        from autoagent.agent import LLMToolResult
        finished = threading.Event()
        async def fast(text: str) -> str:
            """Produce a quick result."""
            finished.set()
            return 'FAST_RESULT_PRESERVED'
        async def slow(text: str) -> str:
            """Wait until cancelled."""
            await asyncio.sleep(30)
            return 'Unexpected completion'
        provider = ScriptedProvider(calls('fast', 'slow'), answer('Continue safely'))
        async with self.product(provider, tools=[fast, slow]) as (host, client):
            ref = await host.applications["agent"].start_conversation('partial-map', 'Run both tools')
            self.assertTrue(await asyncio.to_thread(finished.wait, 5))
            async with asyncio.timeout(5):
                while True:
                    page = await host.list_operator_calls(ref.invocation_id, status='completed', limit=100)
                    if any(row['operator_id'].endswith('.tool.dispatch') for row in page.items):
                        break
                    await asyncio.sleep(.02)
            await host.cancel(ref)
            next_ref = await host.applications["agent"].start_conversation('partial-map', 'Check the previous results')
            result = await host.join(next_ref)
            self.assertEqual(result.status, 'completed', result.error)
            self.assertIn('FAST_RESULT_PRESERVED', str(provider.requests[-1].input))
            self.assertIn('unknown outcome', str(provider.requests[-1].input))

    async def test_evicted_approval_wait_remains_usable_without_exposing_recovery(self):
        """Normal Session unloading can rehydrate a live approval Wait without reopening ended tasks."""
        effects = []
        def publish(text: str) -> str:
            """Publish an approved record."""
            effects.append(text)
            return text
        async with self.product(ScriptedProvider(calls('publish'), answer()),
                access=AgentAccess(rules={'publish':'ask'}), tools=[publish]) as (host, client):
            ref = await host.applications["agent"].start_conversation('evicted', 'Publish')
            self.assertEqual((await host.join(ref)).status, 'waiting')
            async with host._runtime.coordinated(ref.session_id):
                checkpoint = await host._runtime.app.acapture_checkpoint(ref)
                await host._runtime.save_checkpoint(checkpoint, reason='unload')
                await host._runtime.app.aunload_session(ref, expected_version=checkpoint.version)
                host._runtime.forget(ref.session_id)
            self.assertFalse((await host.get_execution_controls(ref.invocation_id))['actions']['recover']['available'])
            saved = await host.applications["agent"].context.get_settings('evicted')
            await host.applications["agent"].configure('evicted', saved.settings.model_copy(update={'approval_mode':'auto_approve'}))
            async def rehydrate():
                async with host._execution(ref, load=True) as loaded:
                    await host._runtime.activate(loaded)
            await host._perform(rehydrate)
            async with asyncio.timeout(10):
                while True:
                    result = await host.join(ref)
                    if result.status == 'completed':
                        break
                    if result.status in {'failed','cancelled'}:
                        self.fail(str(result.error))
                    await asyncio.sleep(.02)
            self.assertEqual(effects, ['hello'])


class SQLiteSessionControlTests(SessionControlContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL URL not configured')
class PostgreSQLSessionControlTests(SessionControlContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self, root):
        database=PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

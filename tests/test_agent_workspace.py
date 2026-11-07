"""Agent App APIs wrap Host controls and User Events without Trace payloads."""
import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException

from autoagent.agent import (AgentInput, LLMStreamChunk, SubagentsConfig, SubagentTarget,
                             Tool, agent_workflow, builtin_agent_workflow, BUILTIN_AGENT_WORKFLOW_ID)
from autoagent.host import AutoAgentHost, HostSettings
from autoagent.server import ServerSettings, create_app
from autoagent.server.agent import agent_router
from autoagent.server.app import _query
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import ScriptedProvider, answer, settings
from tests.test_agent_ask_user import ask_call, reply, tool_response
from tests.test_agent_resource_bindings import skill
from tests.test_agent_subagents import RoutedProvider, call, response, incoming


def echo_note(text: str) -> str:
    """Echo a note."""
    return text


class WorkspaceContracts:
    async def database_url(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        return 'sqlite+aiosqlite:///' + str(Path(directory.name) / 'agent.db')

    @asynccontextmanager
    async def host(self, provider=None, workflows=None):
        @asynccontextmanager
        async def lifespan(setup):
            setup.agent_context.resources.register_provider('offline', provider or ScriptedProvider(answer()))
            setup.agent_context.set_defaults(settings(streaming=True))
            setup.agent_context.resources.register_tool(Tool(echo_note), workflows=(BUILTIN_AGENT_WORKFLOW_ID,))
            setup.agent_context.resources.register_skill(skill('workspace-skill'), workflows=(BUILTIN_AGENT_WORKFLOW_ID,))
            yield
        async with AutoAgentHost(workflows=[builtin_agent_workflow(), *(workflows or [agent_workflow(user_events=True)])],
            lifespan=lifespan, settings=HostSettings(database_url=await self.database_url()), environment={}) as host:
            yield host

    def client(self, host):
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(host, ui=False)), base_url='http://test')

    async def test_catalog_scopes_resources_and_settings_apply_at_next_start(self):
        """Safe catalogs expose explicit bindings and versioned Session settings control future stages."""
        builds = [agent_workflow(user_events=True, tools=[Tool(echo_note)], skills=[skill('workspace-skill')]), agent_workflow(workflow_id='other')]
        async with self.host(ScriptedProvider(answer(), answer()), builds) as host, self.client(host) as client:
            catalog = (await client.get('/api/v2/agent/catalog')).json()
            self.assertNotIn('workflows', catalog)
            revision = catalog['agent']['revision_id']
            self.assertIn('offline', catalog['providers'])
            resources = await client.get('/api/v2/agent/resources', params={'revision_id': revision})
            self.assertEqual(resources.status_code, 200, resources.text)
            self.assertIn('echo_note', {tool['name'] for tool in resources.json()['tools']})
            self.assertEqual(resources.json()['skills'][0]['name'], 'workspace-skill')
            self.assertEqual(resources.json()['skills'][0]['origin'], 'Host')
            other_revision = host._runtime.workflow_ids['other']
            other = await client.get('/api/v2/agent/resources', params={'revision_id': other_revision})
            self.assertEqual(other.status_code, 404)
            sid = str(uuid4())
            initial = (await client.get('/api/v2/agent/settings', params={'session_id': sid})).json()
            desired = initial['settings']; desired['model']['model'] = 'chosen'
            body = dict(session_id=sid, expected_version=0, settings=desired)
            saved = await client.put('/api/v2/agent/settings', json=body)
            self.assertEqual(saved.status_code, 200, saved.text)
            self.assertEqual((await client.put('/api/v2/agent/settings', json=body)).status_code, 409)
            started = await client.post('/api/v2/agent/messages', json=dict(message='Hello workspace', session_id=sid))
            self.assertEqual(started.status_code, 202, started.text)
            iid = started.json()['invocation_id']
            self.assertEqual((await host.join(iid)).status, 'completed')
            execution = await client.get('/api/v2/agent/execution', params={'invocation_id': iid})
            self.assertEqual(execution.status_code, 200, execution.text)
            self.assertEqual(execution.json()['snapshot']['settings']['model']['model'], 'chosen')
            desired['model']['model'] = 'later'
            await client.put('/api/v2/agent/settings', json=dict(body, expected_version=1))
            self.assertEqual((await host.agent_context.get_snapshot(iid)).settings.model.model, 'chosen')
            sessions = await client.get('/api/v2/agent/sessions')
            self.assertEqual(sessions.status_code, 200, sessions.text)
            self.assertEqual(sessions.json()['items'][0]['title'], 'Hello workspace')
            usage = await client.get('/api/v2/agent/usage', params={'session_id': sid, 'invocation_id': iid})
            self.assertEqual(usage.status_code, 200, usage.text)
            self.assertEqual(usage.json()['session'], (await host.token_usage(session_id=sid)).model_dump(mode='json'))

    async def test_history_pages_include_final_snapshots_and_stream_is_session_scoped(self):
        """Session subscribers reject unrelated traffic before buffering; reload reads complete events."""
        async with self.host(ScriptedProvider(answer('One'), answer('Two'))) as host, self.client(host) as client:
            sid = str(uuid4())
            async with host.subscribe_user_events(session_id=sid, capacity=10) as stream:
                unrelated = await host.execute('agent', AgentInput(message='unrelated'))
                await host.join(unrelated)
                ref = await host.execute('agent', AgentInput(message='related'), session_id=sid)
                await host.join(ref)
                event = await asyncio.wait_for(anext(stream), 5)
                self.assertEqual(event.session_id, sid)
            events, after = [], 0
            while True:
                page = (await client.get('/api/v2/user-events', params=dict(invocation_id=ref.invocation_id, after=after, limit=1))).json()
                events.extend(page['items'])
                if page['next_after'] is None:
                    break
                after = page['next_after']
            self.assertEqual([e['sequence'] for e in events], list(range(1, len(events) + 1)))
            self.assertEqual(sum(e['kind'] == 'agent.response' for e in events), 1)
            self.assertFalse(any(e['kind'] == 'agent.response.delta' for e in events))
            self.assertEqual((await client.get('/api/v2/user-events/watch')).status_code, 400)
            self.assertEqual((await client.get('/api/v2/agent/sessions', params={'cursor': 'invalid'})).status_code, 400)

    async def test_sse_reserves_capacity_and_releases_on_overflow_or_disconnect(self):
        """Live overflow explicitly requests reconciliation and always frees the subscription slot."""
        async with self.host() as host:
            config = replace(ServerSettings(), user_event_max_subscribers=1, user_event_buffer=1)
            router = agent_router(host, config, _query)
            watch = next(route.endpoint for route in router.routes if route.path == '/user-events/watch')
            sid = str(uuid4())
            result = await watch(invocation_id=None, session_id=sid)
            stream = result.body_iterator
            self.assertIn('event: ready', await anext(stream))
            with self.assertRaises(HTTPException) as rejected:
                await watch(invocation_id=None, session_id=sid)
            self.assertEqual(rejected.exception.status_code, 503)
            ref = await host.execute('agent', AgentInput(message='overflow'), session_id=sid)
            await host.join(ref)
            self.assertIn('event: reset', await anext(stream))
            with self.assertRaises(StopAsyncIteration):
                await anext(stream)
            self.assertFalse(host._user_events.subscriptions)
            reopened = await watch(invocation_id=None, session_id=sid)
            await anext(reopened.body_iterator)
            await reopened.body_iterator.aclose()
            self.assertFalse(host._user_events.subscriptions)

    async def test_router_shutdown_closes_user_streams_and_preserves_borrowed_host(self):
        """Router shutdown wakes idle SSE consumers while other Host subscriptions keep working."""
        async with self.host() as host:
            config = ServerSettings()
            router = agent_router(host, config, _query)
            watch = next(route.endpoint for route in router.routes if route.path == '/user-events/watch')
            app = FastAPI()
            async with host.subscribe_user_events(session_id='other') as external:
                async with router.lifespan_context(app):
                    response = await watch(invocation_id=None, session_id='workspace')
                    self.assertIn('event: ready', await anext(response.body_iterator))
                    idle = asyncio.create_task(anext(response.body_iterator))
                    await asyncio.sleep(0)
                    self.assertFalse(idle.done())
                with self.assertRaises(StopAsyncIteration):
                    await asyncio.wait_for(idle, 1)
                self.assertEqual(app.state._autoagent_user_event_hubs, set())
                self.assertEqual(len(host._user_events.subscriptions), 1)
                ref = await host.execute('agent', AgentInput(message='still available'), session_id='other')
                self.assertEqual((await host.join(ref)).status, 'completed')
                self.assertEqual((await asyncio.wait_for(anext(external), 3)).kind, 'agent.messages')
                with self.assertRaises(HTTPException) as stopped:
                    await watch(invocation_id=None, session_id='workspace')
                self.assertEqual(stopped.exception.status_code, 503)

    async def test_pending_requests_page_through_the_existing_host_control_api(self):
        """Large question batches retain usable scoped cursors in the Agent execution facade."""
        provider = ScriptedProvider(tool_response(*(ask_call(str(i)) for i in range(25))))
        async with self.host(provider, [agent_workflow(user_events=True, include_ask_user=True)]) as host, self.client(host) as client:
            ref = await host.execute(BUILTIN_AGENT_WORKFLOW_ID, AgentInput(message='many questions'))
            self.assertEqual((await host.join(ref)).status, 'waiting')
            view = (await client.get('/api/v2/agent/execution', params={'invocation_id': ref.invocation_id})).json()
            controls = view['controls']
            self.assertEqual(len(controls['pending_responses']), 20)
            self.assertIsInstance(controls['next_cursor'], str)
            rest = await client.get('/api/v2/execution-controls', params={'invocation_id': ref.invocation_id, 'cursor': controls['next_cursor']})
            self.assertEqual(rest.status_code, 200, rest.text)
            self.assertEqual(len(rest.json()['pending_responses']), 5)
            ids = [item['wait_id'] for item in controls['pending_responses'] + rest.json()['pending_responses']]
            self.assertEqual(len(set(ids)), 25)

    async def test_http_signal_targets_child_and_uses_child_source_frontier(self):
        """A UI update reaches the selected Child through Root control without polluting the Root inbox."""
        child_steps = iter([tool_response(ask_call()), answer('child done')])
        provider = RoutedProvider([response(call('run_agent', {'type': 'worker', 'task': 'child task', 'wait_timeout_seconds': 30})), answer('root done')],
            lambda request: next(child_steps))
        workflows = [agent_workflow(user_events=True, subagents=SubagentsConfig(enabled=True,
            targets={'worker': SubagentTarget(workflow_id='worker')})),
            agent_workflow(workflow_id='worker', user_events=True, include_ask_user=True)]
        async with self.host(provider, workflows) as host, self.client(host) as client:
            root = await host.execute('agent', AgentInput(message='root task'))
            child = None
            async with asyncio.timeout(10):
                while not child:
                    children = (await host.list_child_executions(root.invocation_id)).items
                    if children:
                        waits = await host.get_execution_controls(root.invocation_id)
                        if waits['pending_responses']:
                            child = children[0]; break
                    await asyncio.sleep(.01)
            iid = child['invocation_id']
            source = 'workspace'
            args = dict(invocation_id=iid, action='signal', endpoint='agent.user', source_id=source)
            spec = await client.get('/api/v2/execution-action-spec', params=args)
            self.assertEqual(spec.status_code, 200, spec.text)
            self.assertEqual(spec.json()['next_sequence'], 1)
            accepted = await client.post('/api/v2/execution-actions', json=dict(args, sequence=1, request_id=uuid4().hex,
                payload={'message': 'Child update'}))
            self.assertEqual(accepted.status_code, 202, accepted.text)
            operation_id = accepted.json()['operation_id']
            async with asyncio.timeout(10):
                while (operation := await host.get_execution_operation(operation_id))['status'] == 'running':
                    await asyncio.sleep(.01)
            self.assertEqual(operation['status'], 'succeeded', operation)
            spec = await client.get('/api/v2/execution-action-spec', params=args)
            self.assertEqual(spec.json()['next_sequence'], 2)
            wait = (await host.get_execution_controls(root.invocation_id))['pending_responses'][0]
            await host.perform_execution_action(root.invocation_id, 'respond', target_invocation_id=iid,
                wait_id=wait['wait_id'], payload=reply())
            async with asyncio.timeout(10):
                while (await host.join(root)).status == 'waiting':
                    await asyncio.sleep(.01)
            child_requests = [request for request in provider.requests if incoming(request)]
            self.assertEqual(len(child_requests), 1)
            self.assertEqual(incoming(child_requests[0])[0][1], 'Child update')
            records = (await host.get_user_events(iid)).items
            self.assertTrue(any(e['kind'] == 'agent.messages' and any(m['content'][0].get('text') == 'Child update' for m in e['payload']['messages']) for e in records))


class SQLiteWorkspaceTests(WorkspaceContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL URL not configured')
class PostgreSQLWorkspaceTests(WorkspaceContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

"""Agent App APIs wrap Host controls and User Events without Trace payloads."""

from autoagent.agent import AgentResources, CommandTools
import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
import tempfile
import threading
import unittest
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException

from autoagent.agent_app import builtin_agent_workflow, BUILTIN_AGENT_WORKFLOW_ID
from autoagent.agent import (
    AgentInput, LLMStreamChunk, SubagentsConfig, SubagentTarget, Tool, agent_workflow,
    LocalTokenPricing, TokenPrice, LLMMessage,
)
from autoagent.host import AutoAgentHost, HostSettings
from autoagent.server import ServerSettings, create_app
from autoagent.server.user_events import user_event_routes
from autoagent.server.app import _query
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import ScriptedProvider, answer, settings
from tests.test_agent_ask_user import ask_call, reply, tool_response
from tests.test_agent_resource_bindings import skill
from tests.test_agent_subagents import RoutedProvider, call, response, incoming
from tests.test_llm_call_storage import running, completed


def echo_note(text: str) -> str:
    """Echo a note."""
    return text


class WorkspaceContracts:
    async def test_session_lists_expose_running_then_completed_activity(self):
        """Both directory lists expose the latest run and ordered state without loading execution bodies."""
        entered, release = threading.Event(), threading.Event()
        async def blocked(request):
            entered.set()
            await asyncio.to_thread(release.wait, 10)
            return answer()
        async with self.host(ScriptedProvider(blocked)) as host, self.client(host) as client:
            sent = await client.post('/api/v2/agent/messages', json={'session_id': 'activity', 'message': 'Wait for release'})
            self.assertEqual(sent.status_code, 202, sent.text)
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 10))
                await host._persistence.flush()
                running = (await client.get('/api/v2/agent/sessions')).json()['items'][0]
                self.assertEqual(running['session_id'], 'activity')
                self.assertEqual(running['status'], 'running')
                self.assertEqual(running['invocation_id'], sent.json()['invocation_id'])
            finally:
                release.set()
            await host.join(sent.json()['invocation_id'])
            await host._persistence.flush()
            recent = (await client.get('/api/v2/agent/sessions')).json()['items'][0]
            scoped = (await client.get('/api/v2/agent/sessions', params={
                'workspace_id': host.applications["agent"].context.project_workspace_id})).json()['items'][0]
            self.assertEqual(recent['status'], 'completed')
            self.assertEqual(recent['last_sequence'], scoped['last_sequence'])
            self.assertGreater(recent['last_sequence'], running['last_sequence'])

    async def test_unbound_draft_preview_and_first_message_keep_workspace_choice(self):
        """A draft can select no Workspace, save its settings on first send, and then cannot rebind."""
        provider = ScriptedProvider(answer(), answer())
        async with self.host(provider) as host, self.client(host) as client:
            preview = await client.get('/api/v2/agent/settings', params={'session_id': 'loose', 'unbound': 'true'})
            self.assertEqual(preview.status_code, 200, preview.text)
            chosen = preview.json()['settings']
            self.assertIsNone(preview.json()['workspace'])
            self.assertEqual((await host.applications["agent"].context.get_settings('loose')).version, 0)
            catalog = (await client.get('/api/v2/agent/catalog')).json()
            resources = await client.get('/api/v2/agent/resources', params={
                'session_id': 'loose', 'unbound': 'true', 'revision_id': catalog['agent']['revision_id']})
            self.assertEqual(resources.status_code, 200, resources.text)
            self.assertNotIn('read_file', {tool['name'] for tool in resources.json()['tools']})
            self.assertIn('execute_command', {tool['name'] for tool in resources.json()['tools']})
            chosen['model'].update(model='draft-model')
            sent = await client.post('/api/v2/agent/queue', json={'session_id': 'loose', 'message': 'General question',
                'item_id': uuid4().hex, 'workspace_id': None, 'initial_settings': chosen})
            self.assertEqual(sent.status_code, 202, sent.text)
            result = await host.join(sent.json()['invocation_id'])
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual(provider.requests[0].model, 'draft-model')
            record = await host.applications["agent"].context.get_settings('loose')
            self.assertIsNone(record.workspace)
            self.assertEqual(record.settings.model.model, 'draft-model')
            rebind = record.settings.model_dump(mode='json')
            rebind['workspace'] = {'workspace_id': host.applications["agent"].context.project_workspace_id, 'working_directory': '.'}
            denied = await client.put('/api/v2/agent/settings', json={
                'session_id': 'loose', 'expected_version': record.version, 'settings': rebind})
            self.assertEqual(denied.status_code, 422, denied.text)
            denied = await client.get('/api/v2/agent/settings', params={
                'session_id': 'loose', 'workspace_id': host.applications["agent"].context.project_workspace_id})
            self.assertEqual(denied.status_code, 409, denied.text)

    async def test_recent_lists_bound_and_unbound_chats_with_stable_workspace_metadata(self):
        """The global Recent page includes every root chat while scoped pages retain their filters."""
        async with self.host(ScriptedProvider(answer(), answer())) as host, self.client(host) as client:
            loose = await client.post('/api/v2/agent/messages', json={
                'session_id': 'loose', 'message': 'No workspace question', 'workspace_id': None})
            self.assertEqual(loose.status_code, 202, loose.text)
            await host.join(loose.json()['invocation_id'])
            bound = await client.post('/api/v2/agent/messages', json={'session_id': 'bound', 'message': 'Project question'})
            self.assertEqual(bound.status_code, 202, bound.text)
            await host.join(bound.json()['invocation_id'])
            await host._persistence.flush()
            recent = (await client.get('/api/v2/agent/sessions')).json()['items']
            self.assertEqual({row['session_id']: row['workspace_id'] for row in recent}, {
                'loose': None, 'bound': host.applications["agent"].context.project_workspace_id})
            self.assertEqual(recent[0]['session_id'], 'bound')
            self.assertEqual(recent[0]['status'], 'completed')
            self.assertGreater(recent[0]['last_sequence'], 0)
            self.assertIn('control_state', recent[0])
            scoped = (await client.get('/api/v2/agent/sessions', params={
                'workspace_id': host.applications["agent"].context.project_workspace_id})).json()['items']
            self.assertEqual([row['session_id'] for row in scoped], ['bound'])
            self.assertEqual(scoped[0]['last_sequence'], recent[0]['last_sequence'])
            self.assertEqual(scoped[0]['status'], recent[0]['status'])
            unbound = (await client.get('/api/v2/agent/sessions', params={'unbound': 'true'})).json()['items']
            self.assertEqual([row['session_id'] for row in unbound], ['loose'])

    async def database_url(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        return 'sqlite+aiosqlite:///' + str(Path(directory.name) / 'agent.db')

    @asynccontextmanager
    async def host(self, provider=None, workflows=None):
        @asynccontextmanager
        async def lifespan(setup):
            setup.providers.register('offline', provider or ScriptedProvider(answer()))
            setup.applications["agent"].context.set_defaults(settings(streaming=True))
            setup.extensions["agent"].context.set_defaults(settings(streaming=True))
            setup.applications["agent"].context.resources.register_tool(Tool(echo_note), workflows=(BUILTIN_AGENT_WORKFLOW_ID,))
            setup.applications["agent"].context.resources.register_skill(skill('workspace-skill'), workflows=(BUILTIN_AGENT_WORKFLOW_ID,))
            yield
        async with AutoAgentHost(workflows=[builtin_agent_workflow(), *(workflows or [agent_workflow(user_events=True)])],
            lifespan=lifespan, settings=HostSettings(database_url=await self.database_url()), environment={}) as host:
            yield host

    def client(self, host):
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(host, ui=False)), base_url='http://test')

    async def test_catalog_scopes_resources_and_settings_apply_at_next_start(self):
        """Safe catalogs expose explicit bindings and versioned Session settings control future stages."""
        builds = [agent_workflow(user_events=True, resources=AgentResources(tools=[Tool(echo_note)], skills=[skill('workspace-skill')]),  ), agent_workflow(workflow_id='other')]
        async with self.host(ScriptedProvider(answer(), answer()), builds) as host, self.client(host) as client:
            catalog = (await client.get('/api/v2/agent/catalog')).json()
            self.assertNotIn('workflows', catalog)
            revision = catalog['agent']['revision_id']
            self.assertNotIn('offline', catalog['providers'])
            resources = await client.get('/api/v2/agent/resources', params={'revision_id': revision})
            self.assertEqual(resources.status_code, 200, resources.text)
            self.assertIn('echo_note', {tool['name'] for tool in resources.json()['tools']})
            self.assertEqual(resources.json()['skills'][0]['name'], 'workspace-skill')
            self.assertEqual(resources.json()['skills'][0]['origin'], 'Project')
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
            self.assertEqual((await host.applications["agent"].context.get_snapshot(iid)).settings.model.model, 'chosen')
            sessions = await client.get('/api/v2/agent/sessions')
            self.assertEqual(sessions.status_code, 200, sessions.text)
            self.assertEqual(sessions.json()['items'][0]['title'], 'Hello workspace')
            usage = await client.get('/api/v2/agent/usage', params={'session_id': sid, 'invocation_id': iid})
            self.assertEqual(usage.status_code, 200, usage.text)
            self.assertEqual(usage.json()['session'], (await host.token_usage(session_id=sid)).model_dump(mode='json'))

    async def test_turn_usage_sums_nested_agents_without_other_turns_or_siblings(self):
        """Session usage and subagent browsing include descendants; turn totals stay isolated."""
        steps = iter([response(call('run_agent', {'type': 'agent', 'task': 'child leaf', 'wait_timeout_seconds': 30})),
                      answer('worker done')])
        class Provider(RoutedProvider):
            async def ainvoke(self, request):
                self.requests.append(request)
                messages = [getattr(part, 'text', '') for item in request.input
                            if isinstance(item, LLMMessage) and item.role == 'user' for part in item.content]
                if 'child worker' in messages:
                    return next(steps)
                if 'child leaf' in messages or 'child sibling' in messages:
                    return answer('leaf done')
                return next(self.root)
        provider = Provider([response(
            call('run_agent', {'type': 'agent', 'task': 'child worker', 'wait_timeout_seconds': 30}, id='worker')),
            response(call('run_agent', {'type': 'agent', 'task': 'child sibling', 'wait_timeout_seconds': 30}, id='sibling')),
            answer('root done'), answer('another turn')])
        async with self.host(provider) as host, self.client(host) as client:
            root = await host.execute(BUILTIN_AGENT_WORKFLOW_ID, AgentInput(message='root task'))
            async with asyncio.timeout(60):
                while (result := await host.join(root)).status == 'waiting':
                    await asyncio.sleep(.01)
            self.assertEqual(result.status, 'completed', result)
            children = (await host.list_child_executions(root.invocation_id, order='asc')).items
            self.assertEqual(len(children), 2)
            worker, sibling = children
            leaf = (await host.list_child_executions(worker['invocation_id'])).items[0]
            other = await host.execute(BUILTIN_AGENT_WORKFLOW_ID, AgentInput(message='another turn'), session_id=root.session_id)
            self.assertEqual((await host.join(other)).status, 'completed')
            host._persistence.pricing = LocalTokenPricing(overrides={'test/model': TokenPrice('1', '3')},
                                                         use_litellm_catalog=False)
            for index, (iid, sid) in enumerate([(root.invocation_id, root.session_id), (root.invocation_id, root.session_id),
                (worker['invocation_id'], worker['session_id']), (sibling['invocation_id'], sibling['session_id']),
                (leaf['invocation_id'], leaf['session_id']), (other.invocation_id, other.session_id)]):
                record = completed(running(llm_call_id=f'usage-{index}', invocation_id=iid, session_id=sid))
                await host._persistence.record_llm_call(record)
                await host._persistence.record_llm_call(record)
            async def usage(iid, sid):
                params = dict(session_id=sid)
                if iid is not None:
                    params['invocation_id'] = iid
                result = await client.get('/api/v2/agent/usage', params=params)
                self.assertEqual(result.status_code, 200, result.text)
                return result.json()
            totals = await usage(root.invocation_id, root.session_id)
            self.assertEqual((totals['invocation']['call_count'], totals['invocation']['total_tokens']), (5, 25))
            self.assertEqual(totals['invocation']['cost_usd'], '0.000045')
            session = (await usage(None, root.session_id))['session']
            self.assertEqual(totals['session'], session)
            self.assertEqual((session['call_count'], session['total_tokens'], session['cost_usd']), (6, 30, '0.000054'))
            own = await host.token_usage(invocation_id=root.invocation_id)
            self.assertEqual((own.call_count, own.total_tokens), (2, 10))
            subtree = (await usage(worker['invocation_id'], worker['session_id']))['invocation']
            self.assertEqual((subtree['call_count'], subtree['total_tokens'], subtree['cost_usd']), (2, 10, '0.000018'))
            self.assertEqual((await usage(None, worker['session_id']))['session'], subtree)
            agents, cursor = [], None
            while True:
                params = dict(session_id=root.session_id, limit=1)
                if cursor:
                    params['cursor'] = cursor
                page = await client.get('/api/v2/agent/subagents', params=params)
                self.assertEqual(page.status_code, 200, page.text)
                payload = page.json()
                agents.extend(payload['items'])
                cursor = payload['next_cursor']
                if not cursor:
                    break
                self.assertLess(len(agents), 4)
            self.assertCountEqual([item['invocation_id'] for item in agents],
                                  [item['invocation_id'] for item in (worker, sibling, leaf)])
            self.assertCountEqual([item['task'] for item in agents], ['child worker', 'child sibling', 'child leaf'])
            branch = await client.get('/api/v2/agent/subagents', params=dict(session_id=worker['session_id']))
            self.assertEqual(branch.status_code, 200, branch.text)
            self.assertEqual([item['invocation_id'] for item in branch.json()['items']], [leaf['invocation_id']])
            await host._persistence.record_llm_call(completed(running(llm_call_id='unpriced', requested_model='unknown',
                invocation_id=leaf['invocation_id'], session_id=leaf['session_id'])))
            partial = (await usage(root.invocation_id, root.session_id))['invocation']
            self.assertEqual((partial['call_count'], partial['total_tokens'], partial['missing_cost_calls']), (6, 30, 0))
            self.assertEqual(partial['cost_usd'], '0.000045')
            self.assertEqual((await client.get('/api/v2/agent/usage', params=dict(
                invocation_id=worker['invocation_id'], session_id=root.session_id))).status_code, 400)

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
            endpoints = user_event_routes(host, config, _query)
            router = endpoints.router
            watch = endpoints.watch
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
            endpoints = user_event_routes(host, config, _query)
            router = endpoints.router
            watch = endpoints.watch
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

"""SDK chat registration and the application boundary over generic Host APIs."""

from contextlib import asynccontextmanager
import json
from pathlib import Path
import tempfile
import unittest
from uuid import uuid4

import httpx

from autoagent.agent import AgentInput, BUILTIN_AGENT_WORKFLOW_ID, builtin_agent_workflow, ExecutionToolsConfig, CommandRule
from autoagent.host import AutoAgentHost, HostSettings, ProjectError
from autoagent.host.project import Project
from autoagent.server import create_app
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import ScriptedProvider, answer, settings


class BuiltinRegistrationTests(unittest.TestCase):
    def test_sdk_context_defaults_to_200k_and_accepts_execution_policy(self):
        """The product Agent uses a 200k Context window and explicit command configuration."""
        config = ExecutionToolsConfig(default_decision="deny", rules=[CommandRule(program="git", decision="ask")])
        workflow = builtin_agent_workflow(execution=config)
        start = next(node for node in workflow.nodes if node.id == "start")
        self.assertEqual(start.executable.handler.__autoagent_context_policy__.context_window, 200_000)
        self.assertEqual(start.executable.handler.__autoagent_resources__.execution, config)

    def test_empty_directory_and_empty_manifest_register_sdk_agent(self):
        """Both discovery without factories and an explicit empty list include the SDK Agent."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for manifest in (False, True):
                if manifest:
                    (root / 'auto-agent.toml').write_text('schema_version=1\nworkflows=[]\n')
                workflows, lifespan = Project.find(root).load({})
                self.assertEqual([wf.id for wf in workflows], [BUILTIN_AGENT_WORKFLOW_ID])
                self.assertIsNone(lifespan)
                self.assertEqual(Project.find(root).load({'AUTOAGENT_BUILTIN_AGENT_ENABLED': 'false'})[0], ())

    def test_flag_validation_and_reserved_id(self):
        """Invalid switches and user definitions taking the SDK ID fail during project loading."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project = Project(root, 'test')
            with self.assertRaisesRegex(ProjectError, 'AUTOAGENT_BUILTIN_AGENT_ENABLED'):
                project.load({'AUTOAGENT_BUILTIN_AGENT_ENABLED': 'sometimes'})
            module = 'reserved_' + uuid4().hex
            (root / (module + '.py')).write_text('from autoagent.agent import agent_workflow\n'
                'workflow = agent_workflow(workflow_id="autoagent.agent")\n')
            project = Project(root, 'test', (module + ':workflow',))
            for flag in ('true', 'false'):
                with self.assertRaisesRegex(ProjectError, 'reserved'):
                    project.load({'AUTOAGENT_BUILTIN_AGENT_ENABLED': flag})


class BuiltinApplicationContracts:
    async def database_url(self, root):
        return 'sqlite+aiosqlite:///' + str(root / 'runtime.db')

    @asynccontextmanager
    async def project(self, *, disabled=False, custom=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            if custom:
                module = 'custom_' + uuid4().hex
                (root / (module + '.py')).write_text('from autoagent.agent import agent_workflow\n'
                    'workflow = agent_workflow(workflow_id="custom", user_events=True)\n')
                manifest = f'schema_version=1\n[[workflows]]\nentrypoint="{module}:workflow"\n'
            else:
                manifest = 'schema_version=1\nworkflows=[]\n'
            (root / 'auto-agent.toml').write_text(manifest)
            provider = ScriptedProvider(*(answer(f'Answer {i}') for i in range(8)))
            @asynccontextmanager
            async def lifespan(setup):
                setup.agent_context.resources.register_provider('offline', provider)
                setup.agent_context.set_defaults(settings())
                yield
            async with AutoAgentHost.from_project(root, use_env_file=False,
                environment={'AUTOAGENT_BUILTIN_AGENT_ENABLED': 'false' if disabled else 'true'},
                lifespan=lifespan, settings=HostSettings(database_url=await self.database_url(root))) as host:
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(host)),
                    base_url='http://test') as client:
                    yield host, client

    async def test_agent_only_project_runs_multiple_turns_and_defaults_to_chat(self):
        """An empty project boots into Agents and persists messages through the fixed chat endpoint."""
        async with self.project() as (host, client):
            catalog = (await client.get('/api/v2/agent/catalog')).json()
            self.assertTrue(catalog['standalone'])
            self.assertTrue(catalog['agent']['include_ask_user'])
            self.assertTrue(catalog['agent']['subagents'])
            self.assertEqual(catalog['agent']['context_policy']['context_window'], 200_000)
            page = (await client.get('/')).text
            config = json.loads(page.split('<script id="autoagent-config" type="application/json">')[1].split('</script>')[0])
            self.assertEqual(config['defaultView'], 'agents')
            sid = str(uuid4())
            for message in ('First message', 'Next message'):
                run = await client.post('/api/v2/agent/messages', json=dict(session_id=sid, message=message))
                self.assertEqual(run.status_code, 202, run.text)
                result = await host.join(run.json()['invocation_id'])
                self.assertEqual(result.status, 'completed', result.error)
                history = await client.get('/api/v2/agent/history', params={'invocation_id': result.ref.invocation_id})
                self.assertEqual(history.status_code, 200, history.text)
                self.assertTrue(any(item['kind'] == 'agent.response' for item in history.json()['items']))
            turns = await client.get('/api/v2/agent/invocations', params={'session_id': sid})
            self.assertEqual(len(turns.json()['items']), 2)
            self.assertEqual({item['workflow_id'] for item in turns.json()['items']}, {BUILTIN_AGENT_WORKFLOW_ID})
            rows = (await client.get('/api/v2/agent/sessions')).json()['items']
            self.assertEqual(len(rows), 1)
            self.assertNotIn('workflow_id', rows[0])
            self.assertNotIn('status', rows[0])

    async def test_disabled_agent_allows_empty_host_and_returns_clear_chat_state(self):
        """Disabling the SDK Agent still permits project startup with zero Workflows."""
        async with self.project(disabled=True) as (host, client):
            self.assertEqual((await client.get('/api/v2/workflows')).json()['items'], [])
            self.assertIsNone((await client.get('/api/v2/agent/catalog')).json()['agent'])
            self.assertEqual((await client.get('/api/v2/agent/sessions')).json()['items'], [])
            denied = await client.post('/api/v2/agent/messages', json={'session_id': 'new', 'message': 'Hi'})
            self.assertEqual(denied.status_code, 404)

    async def test_custom_agent_builds_and_mixed_sessions_cannot_enter_chat(self):
        """Chat rejects custom execution/history/settings without restricting generic Host execution."""
        async with self.project(custom=True) as (host, client):
            custom = await host.execute('custom', AgentInput(message='Custom'))
            await host.join(custom)
            sdk = await host.agent_context.start_conversation('sdk-only', 'SDK')
            await host.join(sdk)
            for route, params in (
                ('execution', {'invocation_id': custom.invocation_id}),
                ('history', {'invocation_id': custom.invocation_id}),
                ('resources', {'revision_id': host._runtime.workflow_ids['custom']}),
                ('invocations', {'session_id': custom.session_id}),
                ('settings', {'session_id': custom.session_id}),
                ('usage', {'session_id': custom.session_id}),
                ('events/watch', {'session_id': custom.session_id}),
            ):
                result = await client.get('/api/v2/agent/' + route, params=params)
                self.assertEqual(result.status_code, 404, (route, result.text))
            denied = await client.post('/api/v2/agent/messages', json={'session_id': custom.session_id, 'message': 'Hi'})
            self.assertEqual(denied.status_code, 404)
            extra = await client.post('/api/v2/agent/messages', json={'session_id': 'new', 'message': 'Hi', 'workflow_id': 'custom'})
            self.assertEqual(extra.status_code, 422)
            generic = await client.get('/api/v2/user-events', params={'invocation_id': custom.invocation_id})
            self.assertEqual(generic.status_code, 200)
            self.assertEqual(len((await client.get('/api/v2/agent/sessions')).json()['items']), 1)
            mixed = await host.execute('custom', AgentInput(message='Mixed'), session_id=sdk.session_id)
            await host.join(mixed)
            self.assertEqual((await client.get('/api/v2/agent/sessions')).json()['items'], [])
            self.assertEqual((await client.get('/api/v2/agent/history', params={'invocation_id': sdk.invocation_id})).status_code, 404)


class SQLiteBuiltinApplicationTests(BuiltinApplicationContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL URL not configured')
class PostgreSQLBuiltinApplicationTests(BuiltinApplicationContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self, root):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

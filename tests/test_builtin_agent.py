"""SDK chat registration and the application boundary over generic Host APIs."""

from contextlib import asynccontextmanager
import json
from pathlib import Path
import tempfile
import unittest
from uuid import uuid4

import httpx

from autoagent.agent_app import AgentApplication, BUILTIN_AGENT_WORKFLOW_ID, builtin_agent_workflow
from autoagent.agent import AgentInput, ExecutionToolsConfig, LLMReasoning, LLMResponse, LLMToolCall, ToolPolicy
from autoagent.host import AutoAgentHost, HostSettings, ProjectError
from autoagent.host.project import Project
from autoagent.server import create_app
from autoagent.server.settings import ServerSettings
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import ScriptedProvider, answer, settings


class BuiltinRegistrationTests(unittest.TestCase):
    def test_sdk_context_defaults_to_200k_and_accepts_execution_policy(self):
        """The product Agent uses a 200k Context window and explicit command configuration."""
        config = ExecutionToolsConfig(max_running_processes=2)
        policy = ToolPolicy(default="deny")
        workflow = builtin_agent_workflow(execution=config, command_policy=policy)
        start = next(node for node in workflow.nodes if node.id == "start")
        self.assertEqual(start.executable.handler.__autoagent_context_policy__.context_window, 200_000)
        self.assertEqual(start.executable.handler.__agent_app_execution__, config)
        self.assertEqual(start.executable.handler.__agent_app_command_policy__, policy)

    def test_empty_directory_and_empty_manifest_register_sdk_agent(self):
        """Empty projects leave product registration to explicit application composition."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for manifest in (False, True):
                if manifest:
                    (root / 'auto-agent.toml').write_text('schema_version=1\nworkflows=[]\n')
                workflows, lifespan = Project.find(root).load({})
                self.assertEqual(workflows, ())
                product = AgentApplication.project_workflows(workflows, {})
                self.assertEqual([wf.id for wf in product], [BUILTIN_AGENT_WORKFLOW_ID])
                self.assertIsNone(lifespan)
                self.assertEqual(Project.find(root).load({'AUTOAGENT_BUILTIN_AGENT_ENABLED': 'false'})[0], ())

    def test_flag_validation_and_reserved_id(self):
        """Invalid switches and user definitions taking the SDK ID fail during project loading."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project = Project(root, 'test')
            with self.assertRaisesRegex(ValueError, 'AUTOAGENT_BUILTIN_AGENT_ENABLED'):
                AgentApplication.project_workflows(project.load({})[0], {'AUTOAGENT_BUILTIN_AGENT_ENABLED': 'sometimes'})
            module = 'reserved_' + uuid4().hex
            (root / (module + '.py')).write_text('from autoagent.agent import agent_workflow\n'
                'workflow = agent_workflow(workflow_id="autoagent.agent")\n')
            project = Project(root, 'test', (module + ':workflow',))
            for flag in ('true', 'false'):
                with self.assertRaisesRegex(ValueError, 'reserved'):
                    AgentApplication.project_workflows(project.load({})[0], {'AUTOAGENT_BUILTIN_AGENT_ENABLED': flag})


class BuiltinApplicationContracts:
    async def database_url(self, root):
        return 'sqlite+aiosqlite:///' + str(root / 'runtime.db')

    @asynccontextmanager
    async def project(self, *, disabled=False, custom=False, provider=None):
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
            provider = provider or ScriptedProvider(*(answer(f'Answer {i}') for i in range(8)))
            @asynccontextmanager
            async def lifespan(setup):
                setup.providers.register('offline', provider)
                if "agent" in setup.extensions:
                    setup.extensions["agent"].context.set_defaults(settings())
                setup.applications["agent"].context.set_defaults(settings())
                yield
            async with AutoAgentHost.from_project(root, use_env_file=False, applications=(AgentApplication,),
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
            self.assertEqual(rows[0]['status'], 'completed')

    async def test_conversation_history_projects_final_answers_and_jumps_by_invocation(self):
        """Summary and index omit execution payloads while an anchored page and raw history remain accessible."""
        intermediate = LLMResponse(model="offline", finish_reason="tool_calls", output=(
            LLMReasoning(text="Hidden reasoning"),
            LLMToolCall(name="get_agents", call_id="list-agents", arguments="{}"),
        ))
        final = answer("First final answer")
        final = final.model_copy(update={"output": (LLMReasoning(text="Final hidden reasoning"), *final.output)})
        async with self.project(provider=ScriptedProvider(intermediate, final, answer("Second final answer"))) as (host, client):
            sid = "history-session"
            ids = []
            for text in ("First question", "Second question"):
                response = await client.post("/api/v2/agent/messages", json=dict(session_id=sid, message=text))
                self.assertEqual(response.status_code, 202, response.text)
                ids.append(response.json()["invocation_id"])
                outcome = await host.join(ids[-1])
                self.assertEqual(outcome.status, "completed", outcome.error)
            application = host.applications["agent"]
            first_page = await application.conversation_page(sid, limit=1)
            self.assertEqual([row["invocation_id"] for row in first_page["items"]], [ids[1]])
            previous = await application.conversation_page(sid, limit=1, cursor=first_page["next_cursor"])
            response = await client.get("/api/v2/agent/invocations", params={"session_id": sid, "anchor": ids[0]})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json(), previous)
            self.assertIn(ids[0], previous["details"])
            self.assertEqual([event["kind"] for event in previous["events"]], [
                "agent.messages", "agent.completed", "agent.response"])
            answer_blocks = previous["events"][-1]["payload"]["blocks"]
            self.assertEqual([(block["type"], block["text"]) for block in answer_blocks], [("text", "First final answer")])
            index = (await client.get("/api/v2/agent/turn-index", params={"session_id": sid})).json()["items"]
            self.assertEqual([(row["invocation_id"], row["question"], row["answer"]) for row in index], [
                (ids[0], "First question", "First final answer"), (ids[1], "Second question", "Second final answer")])
            details = (await client.get("/api/v2/agent/history", params={"invocation_id": ids[0]})).json()["items"]
            self.assertTrue(any(event["kind"] == "agent.tools" for event in details))
            self.assertIn("Final hidden reasoning", json.dumps(details))
            self.assertEqual((await client.get("/api/v2/agent/invocations",
                params={"session_id": sid, "anchor": "missing"})).status_code, 404)

    async def test_disabled_agent_allows_empty_host_and_returns_clear_chat_state(self):
        """Disabling the Agent application still permits startup with zero Workflows."""
        async with self.project(disabled=True) as (host, client):
            self.assertEqual((await client.get('/api/v2/workflows')).json()['items'], [])
            self.assertIsNone((await client.get('/api/v2/agent/catalog')).json()['agent'])
            self.assertEqual((await client.get('/api/v2/agent/sessions')).json()['items'], [])
            denied = await client.post('/api/v2/agent/messages', json={'session_id': 'new', 'message': 'Hi'})
            self.assertEqual(denied.status_code, 404)

    async def test_workflow_ui_can_hide_product_history_without_filtering_sdk_or_chat(self):
        """Workflow visibility filters before paging and aggregation while generic APIs and Agent chat retain access."""
        header = {"X-AutoAgent-UI": "workflow"}
        async with self.project(custom=True) as (host, client):
            app = host.applications["agent"]
            hidden = await app.start_conversation("product-only", "Hidden task")
            await host.join(hidden)
            mixed = await app.start_conversation("mixed-history", "First product task")
            await host.join(mixed)
            custom = await host.execute("custom", AgentInput(message="Custom task"), session_id="mixed-history")
            await host.join(custom)
            last = await host.execute(BUILTIN_AGENT_WORKFLOW_ID, AgentInput(message="Last product task"), session_id="mixed-history")
            await host.join(last)
            other = await host.execute("custom", AgentInput(message="Other custom task"), session_id="custom-only")
            await host.join(other)
            revision = hidden.workflow_revision_id
            for route, params, field in (
                ("workflow-catalog", {"limit": 1}, "workflow_id"),
                ("workflows", {"limit": 1, "include_history": "true"}, "workflow_id"),
                ("executions", {"limit": 1}, "workflow_id"),
                ("session-invocations", {"session_id": "mixed-history", "limit": 1}, "workflow_id"),
                ("session-history", {"session_id": "mixed-history", "limit": 1}, "workflow_id"),
                ("session-workflows", {"session_id": "mixed-history", "limit": 1}, "workflow_id"),
            ):
                cursor, found = None, []
                while True:
                    result = await client.get("/api/v2/" + route, params={**params, **({"cursor": cursor} if cursor else {})}, headers=header)
                    self.assertEqual(result.status_code, 200, result.text)
                    page = result.json()
                    found.extend(page["items"])
                    if page["next_cursor"] is None:
                        break
                    self.assertTrue(page["items"], route)
                    cursor = page["next_cursor"]
                self.assertTrue(found, route)
                self.assertEqual({row[field] for row in found}, {"custom"}, route)
            listed = (await client.get("/api/v2/execution-sessions", headers=header)).json()
            self.assertEqual(listed["total_count"], 2)
            rows = {row["session_id"]: row for row in listed["items"]}
            self.assertEqual(set(rows), {"mixed-history", "custom-only"})
            self.assertEqual(rows["mixed-history"]["invocation_count"], 1)
            self.assertEqual(rows["mixed-history"]["workflow_count"], 1)
            info = (await client.get("/api/v2/session-information", params={"session_id": "mixed-history"}, headers=header)).json()
            self.assertEqual(info["latest_invocation_id"], custom.invocation_id)
            self.assertEqual(info["invocation_count"], 1)
            for route, params in (
                ("workflow-graph", {"revision_id": revision}),
                ("workflow-revisions", {"workflow_id": BUILTIN_AGENT_WORKFLOW_ID}),
                ("execution-trace", {"invocation_id": hidden.invocation_id}),
                ("trace-dataset", {"invocation_id": hidden.invocation_id}),
                ("session-information", {"session_id": "product-only"}),
            ):
                result = await client.get("/api/v2/" + route, params=params, headers=header)
                self.assertEqual(result.status_code, 404, (route, result.text))
                self.assertEqual(result.json()["detail"]["code"], "agent_app_hidden")
            generic = (await client.get("/api/v2/workflow-catalog")).json()
            self.assertEqual({row["workflow_id"] for row in generic["items"]}, {"custom", BUILTIN_AGENT_WORKFLOW_ID})
            self.assertEqual((await client.get("/api/v2/agent/history",
                params={"invocation_id": hidden.invocation_id})).status_code, 200)
            self.assertEqual((await host.get_execution_trace(hidden.invocation_id))["invocation"]["status"], "completed")
            async with httpx.AsyncClient(transport=httpx.ASGITransport(
                    app=create_app(host, settings=ServerSettings(workflow_ui_show_agent_app=True))),
                    base_url="http://test", headers=header) as visible:
                self.assertEqual((await visible.get("/api/v2/workflow-graph", params={"revision_id": revision})).status_code, 200)
                shown = (await visible.get("/api/v2/execution-sessions")).json()["items"]
                self.assertEqual({row["session_id"] for row in shown}, {"product-only", "mixed-history", "custom-only"})

    async def test_custom_agent_builds_and_mixed_sessions_cannot_enter_chat(self):
        """Chat rejects custom execution/history/settings without restricting generic Host execution."""
        async with self.project(custom=True) as (host, client):
            custom = await host.execute('custom', AgentInput(message='Custom'))
            await host.join(custom)
            sdk = await host.applications["agent"].context.start_conversation('sdk-only', 'SDK')
            await host.join(sdk)
            for route, params in (
                ('execution', {'invocation_id': custom.invocation_id}),
                ('history', {'invocation_id': custom.invocation_id}),
                ('resources', {'revision_id': host._runtime.workflow_ids['custom']}),
                ('invocations', {'session_id': custom.session_id}),
                ('turn-index', {'session_id': custom.session_id}),
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

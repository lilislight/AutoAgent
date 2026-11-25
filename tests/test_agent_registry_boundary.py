"""Resource ownership, ordinary Tool groups and product-only Skill switches."""

from contextlib import asynccontextmanager
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from sqlalchemy import inspect
from autoagent.agent import (AgentInput, AgentResources, FileTools, Skill,
    Tool, agent_workflow)
from autoagent.agent_app import builtin_agent_workflow, ResourceControl
from autoagent.agent_app.skills import enabled, set_enabled
from autoagent.host import AutoAgentHost, HostSettings
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import ScriptedProvider, answer, settings


def make_skill(root, extra=''):
    directory = root / 'sample'
    directory.mkdir(parents=True, exist_ok=True)
    (directory / 'SKILL.md').write_text('---\nname: sample\ndescription: Example\n' + extra + '---\n# Body\nKeep this text.\n')
    return Skill(directory).freeze()


class RegistryBoundaryTests(unittest.TestCase):

    def test_file_group_selection_keeps_instance_and_schema(self):
        """Selected file Tools retain their own backend and configured bounds."""
        with tempfile.TemporaryDirectory() as directory:
            group = FileTools(directory)
            resources = AgentResources(tools=group.tools(include=('read_file',)))
            self.assertEqual([item.name for item in resources.entries('tool')], ['read_file'])
            self.assertIs(group['read_file']._owner._tools['read_file'].group, group)
            self.assertNotIn('self', group['read_file'].definition.parameters['properties'])

    def test_skill_switch_preserves_other_metadata_and_markdown(self):
        """App writes only its extension while preserving comments and body text."""
        for extra in ('', 'metadata:\n  author: Alice # keep\n', 'metadata: {author: Alice}\n',
                      'metadata:\n  autoagent-app-enabled: "true" # keep\n'):
            with self.subTest(extra=extra), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                skill = make_skill(root, extra).freeze()
                set_enabled(skill, False, roots=(root,))
                current = Skill(root / 'sample').freeze()
                self.assertFalse(enabled(current))
                text = (root / 'sample' / 'SKILL.md').read_text()
                self.assertTrue(text.endswith('# Body\nKeep this text.\n'))
                if '# keep' in extra:
                    self.assertIn('# keep', text)
                set_enabled(current, True, roots=(root,))
                self.assertTrue(enabled(Skill(root / 'sample')))

    def test_skill_write_rejects_stale_or_readonly_source(self):
        """Stale snapshots and read-only documents cannot silently overwrite changes."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            skill = make_skill(root)
            path = root / 'sample' / 'SKILL.md'
            path.write_text(path.read_text() + 'Changed\n')
            with self.assertRaisesRegex(ValueError, 'changed'):
                set_enabled(skill, False, roots=(root,))
            current = Skill(root / 'sample').freeze()
            path.chmod(0o444)
            try:
                with self.assertRaisesRegex(ValueError, 'read-only'):
                    set_enabled(current, False, roots=(root,))
            finally:
                path.chmod(0o644)

    def test_skill_switch_rejects_a_directory_swapped_for_a_symlink(self):
        """A concurrent symlink swap cannot redirect a Skill edit to another directory."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            managed, external = root / 'managed', root / 'external'
            skill = make_skill(managed)
            make_skill(external)
            target = external / 'sample' / 'SKILL.md'
            original = target.read_bytes()
            parent = managed / 'sample'
            replaced = managed / 'old-sample'
            fsync = os.fsync

            def swap_after_staging(fd):
                fsync(fd)
                parent.rename(replaced)
                parent.symlink_to(target.parent, target_is_directory=True)

            with patch('autoagent.agent_app.skills.os.fsync', side_effect=swap_after_staging):
                with self.assertRaises(OSError):
                    set_enabled(skill, False, roots=(managed,))
            self.assertEqual(target.read_bytes(), original)
            self.assertEqual((replaced / 'SKILL.md').read_bytes(), original)
            self.assertEqual(list(replaced.glob('.SKILL.*')), [])


class RegistryExecutionContracts:
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.url = await self.database_url()

    async def database_url(self):
        return 'sqlite+aiosqlite:///' + str(self.root / 'db')

    @asynccontextmanager
    async def running(self, workflows, provider):
        @asynccontextmanager
        async def lifespan(setup):
            setup.providers.register('offline', provider)
            setup.extensions['agent'].context.set_defaults(settings())
            if 'agent' in setup.applications:
                setup.applications['agent'].context.set_defaults(settings())
            yield
        async with AutoAgentHost(root=self.root, environment={}, workflows=workflows,
                lifespan=lifespan, settings=HostSettings(database_url=self.url)) as host:
            yield host

    async def test_sdk_ignores_app_skill_flag_and_has_no_product_tables(self):
        """Explicit SDK Skills remain available even when the App flag disables them."""
        skill = make_skill(self.root, 'metadata:\n  autoagent-app-enabled: "false"\n')
        resources = AgentResources(skills=(skill,))
        provider = ScriptedProvider(answer())
        async with self.running([agent_workflow(resources=resources)], provider) as host:
            result = await host.join(await host.execute('agent', AgentInput(message='Hello')))
            self.assertEqual(result.status, 'completed', result.error)
            self.assertIn('skill_load', [tool.name for tool in provider.requests[0].tools])
            self.assertEqual(host.applications, {})
            self.assertFalse(hasattr(host, 'agent_context'))
            self.assertFalse(hasattr(host.extensions['agent'].context, 'resources'))
            self.assertIs(host.extensions['agent'].context.resources_for(result.ref.workflow_revision_id), resources)
            async def read_tables():
                async with host._persistence._writer.storage.engine.connect() as connection:
                    return await connection.run_sync(lambda sync: inspect(sync).get_table_names())
            tables = await host._persistence._writer.run(read_tables)
            self.assertFalse(any(name.startswith('agent_app_') for name in tables))

    async def test_app_toggle_changes_file_not_resource_records(self):
        """App toggles the Skill file immediately without storing a per-Skill override."""
        make_skill(self.root / '.agents' / 'skills')
        provider = ScriptedProvider(answer(), answer())
        async with self.running([builtin_agent_workflow(execution=False)], provider) as host:
            app = host.applications['agent']
            inventory = await app.resource_configs()
            skill = next(item for item in inventory['resources'] if item['kind'] == 'skill')
            self.assertTrue(skill['enabled_editable'])
            before = await app.context.storage('resource_records')
            await app.save_resource(ResourceControl(name='sample', resource_id=skill['management_id'], enabled=False))
            self.assertEqual(await app.context.storage('resource_records'), before)
            self.assertFalse(enabled(Skill(self.root / '.agents' / 'skills' / 'sample')))
            inventory = await app.resource_configs()
            self.assertFalse(next(item for item in inventory['resources'] if item['kind'] == 'skill')['enabled'])
            await app.save_resource(ResourceControl(name='sample', resource_id=skill['management_id'], enabled=True))
            self.assertTrue(enabled(Skill(self.root / '.agents' / 'skills' / 'sample')))

    async def test_cli_available_without_workspace_and_files_absent(self):
        """Product unbound chats retain command Tools while omitting Workspace file Tools."""
        provider = ScriptedProvider(answer())
        async with self.running([builtin_agent_workflow()], provider) as host:
            app = host.applications['agent']
            result = await host.join(await app.context.start_conversation('unbound', 'Hello', unbound=True))
            self.assertEqual(result.status, 'completed', result.error)
            tools = {tool.name for tool in provider.requests[0].tools}
            self.assertIn('execute_command', tools)
            self.assertNotIn('read_file', tools)

    async def test_registered_local_skill_switch_refreshes_its_snapshot(self):
        """Editing a directly registered Skill refreshes its immutable App snapshot."""
        skill = make_skill(self.root / 'declared')
        resources = AgentResources(skills=(skill,))
        async with self.running([builtin_agent_workflow(resources=resources, execution=False)], ScriptedProvider(answer())) as host:
            app = host.applications['agent']
            inventory = await app.resource_configs()
            entry = next(item for item in inventory['resources'] if item['kind'] == 'skill')
            await app.save_resource(ResourceControl(name='sample', resource_id=entry['management_id'], enabled=False))
            self.assertFalse(enabled(app.context.resources.get('skill', 'sample').value))
            self.assertEqual(await app.context.storage('resource_records'), [])

    async def test_static_sdk_auto_approval_needs_no_product_monitor(self):
        """SDK auto approval resolves native Waits without installing the application."""
        import asyncio
        from autoagent.agent import LLMToolCall, ToolSet, ToolDecision
        from tests.test_agent_ask_user import tool_response
        def publish(text: str) -> str:
            return text
        resources = AgentResources(tools=ToolSet((Tool(publish),), policy=lambda *_: ToolDecision(action='ask')))
        provider = ScriptedProvider(tool_response(LLMToolCall(name='publish', call_id='p', arguments='{"text":"saved"}')), answer())
        workflow = agent_workflow(resources=resources, runtime=settings(mode='auto_approve'))
        async with self.running([workflow], provider) as host:
            ref = await host.execute('agent', AgentInput(message='Hello'))
            async with asyncio.timeout(15):
                while (result := await host.join(ref)).status not in {'completed', 'failed', 'cancelled'}:
                    await asyncio.sleep(.01)
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual(host.applications, {})

    async def test_product_change_does_not_resume_sdk_approval(self):
        """Application resource notifications cannot advance an SDK-owned Wait."""
        import asyncio
        from autoagent.agent import LLMToolCall, ToolDecision
        from tests.test_agent_ask_user import tool_response
        def publish(text: str) -> str:
            self.fail('Product notification executed an SDK Tool')
        resources = AgentResources()
        resources.register_tool(publish, policy=lambda *_: ToolDecision(action='ask'))
        workflow = agent_workflow(workflow_id='custom', resources=resources)
        provider = ScriptedProvider(tool_response(LLMToolCall(name='publish', call_id='p', arguments='{"text":"saved"}')))
        async with self.running([builtin_agent_workflow(execution=False), workflow], provider) as host:
            ref = await host.execute('custom', AgentInput(message='Hello'))
            self.assertEqual((await host.join(ref)).status, 'waiting')
            controller = host.extensions['agent'].context.approvals
            async def drain(controller):
                await asyncio.gather(*tuple(controller.tasks))
            await controller._run(lambda: drain(controller))
            resources.set_policy('tool', 'publish', lambda *_: ToolDecision(action='allow'))
            app_controller = host.applications['agent'].context.approvals
            await app_controller.changed()
            await app_controller._run(lambda: drain(app_controller))
            self.assertEqual((await host.join(ref)).status, 'waiting')
            self.assertEqual(len(provider.requests), 1)

    async def test_embedded_agent_installs_runtime_and_validates_child_waits(self):
        """An ordinary parent can embed an Agent and resume its typed approval Wait."""
        from autoagent.core import Workflow, Node, Spawn
        from tests.test_agent_ask_user import tool_response
        from autoagent.agent import LLMToolCall, ToolDecision, ToolSet
        effects = []
        def publish(text: str) -> str:
            effects.append(text)
            return text
        resources = AgentResources(tools=ToolSet((Tool(publish),), policy=lambda *_: ToolDecision(action='ask')))
        child = agent_workflow(workflow_id='embedded', resources=resources)
        parent = Workflow('ordinary', nodes=[Node('spawn', Spawn(child, 'start'))])
        provider = ScriptedProvider(tool_response(LLMToolCall(name='publish', arguments='{"text":"saved"}', call_id='p')), answer())
        async with self.running([parent], provider) as host:
            ref = await host.execute('ordinary', AgentInput(message='Hello'))
            result = await host.join(ref)
            self.assertIsNone(result.error)
            wait = next(item for item in result.waits if item.kind == 'external')
            view = await host.extensions['agent'].action_view(result.ref, wait.id)
            reply = view['response_example']
            reply['decisions'][0]['approved'] = True
            await host.respond(ref, wait.id, reply)
            finished = await host.join(ref)
            self.assertIsNone(finished.error)
            self.assertEqual(effects, ['saved'])
            self.assertEqual(len(provider.requests), 2)


class SQLiteRegistryExecutionTests(RegistryExecutionContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL is not configured')
class PostgreSQLRegistryExecutionTests(RegistryExecutionContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()


class RegistryMCPLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_disconnect_withdraws_tools_before_draining_borrowed_client(self):
        """MCP removal hides Tools immediately while a borrowed connection remains usable."""
        import asyncio
        from tests.test_agent_mcp import httpx2 as httpx
        from autoagent.agent import MCPSource, MCPClient, MCPStreamableHTTPConfig
        from tests.test_agent_mcp import WireServer
        wire, clients = WireServer(legacy=True), []
        @asynccontextmanager
        async def factory():
            async with httpx.AsyncClient(transport=httpx.MockTransport(wire.handle)) as http:
                async with MCPClient(MCPStreamableHTTPConfig(server_id='registry', url='https://server.test/mcp'),
                        http_client=http) as client:
                    clients.append(client)
                    yield client
        source = MCPSource(name='remote', client_factory=factory)
        resources = AgentResources(mcp=(source,))
        async with resources.lifespan():
            entry = resources.entries('tool')[0]
            async with resources.borrow(entry.resource_id):
                stopping = asyncio.create_task(resources.disconnect_mcp('remote'))
                for _ in range(50):
                    if not resources.entries('tool'):
                        break
                    await asyncio.sleep(.01)
                self.assertFalse(resources.entries('tool'))
                self.assertFalse(stopping.done())
                self.assertFalse(clients[0]._closed)
            await stopping
            self.assertTrue(clients[0]._closed)
            await resources.connect_mcp(source)
            self.assertTrue(resources.entries('tool'))
        self.assertTrue(clients[-1]._closed)

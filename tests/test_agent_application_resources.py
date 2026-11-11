"""Stable product resource controls, groups, and Workspace Skill views."""

from contextlib import asynccontextmanager
import unittest

from autoagent.agent import LocalWorkspace, Tool
from autoagent.agent_app import builtin_agent_workflow, BUILTIN_AGENT_WORKFLOW_ID, GroupControl, ResourceControl
from autoagent.host import AutoAgentHost, HostSettings
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_application_isolation import ApplicationIsolationContracts
from tests.test_agent_workflow import ScriptedProvider, settings


class ApplicationResourceContracts:
    asyncSetUp = ApplicationIsolationContracts.asyncSetUp
    database_url = ApplicationIsolationContracts.database_url

    @asynccontextmanager
    async def product(self, *, execution=False, root=None, environment=None, provider=None):
        @asynccontextmanager
        async def lifespan(setup):
            setup.agent_context.resources.register_provider('offline', provider or ScriptedProvider())
            setup.agent_context.set_defaults(settings())
            def project_echo(text: str) -> str:
                return text
            setup.agent_context.resources.register_tool(Tool(project_echo), workflows={BUILTIN_AGENT_WORKFLOW_ID})
            yield
        async with AutoAgentHost(root=root or self.root, workflows=[builtin_agent_workflow(execution=execution)],
            lifespan=lifespan, settings=HostSettings(database_url=self.url), environment=environment or {}) as host:
            yield host

    async def test_builtin_control_survives_workflow_revision_changes(self):
        """A saved Tool switch follows its management key rather than a generated binding ID."""
        identities = []
        for execution in (False, True):
            async with self.product(execution=execution) as host:
                manager = host.agent_app.context.shared_resources
                inventory = await manager.describe()
                entry = next(value for value in inventory['resources'] if value['name'] == 'read_file')
                identities.append(entry)
                if not execution:
                    saved = await manager.save(ResourceControl(name='Read files', resource_id=entry['resource_id'], enabled=False))
                    self.assertEqual(saved['configuration']['resource_id'], entry['management_id'])
                else:
                    self.assertFalse(entry['enabled'])
        self.assertEqual(identities[0]['management_id'], identities[1]['management_id'])
        self.assertNotEqual(identities[0]['resource_id'], identities[1]['resource_id'])

    async def test_group_switch_filters_tools_without_changing_sdk_registration(self):
        """A disabled product group hides every member while the Host's explicit registrations remain enabled."""
        async with self.product() as host:
            manager = host.agent_app.context.shared_resources
            entry = next(row for row in (await manager.describe())['resources'] if row['name'] == 'project_echo')
            await manager.save(GroupControl(name='Project', group_id=entry['group_id'], enabled=False))
            inventory = await manager.describe()
            self.assertFalse(next(row for row in inventory['resources'] if row['name'] == 'project_echo')['enabled'])
            self.assertTrue(host.agent_context.resources.has('tool', 'project_echo'))
            self.assertTrue(any(group['id'] == 'builtin/files' for group in inventory['groups']))

    async def test_workspace_skill_switch_and_document_are_shared_by_workspace(self):
        """Workspace selection is a management view; disabling one Skill does not affect another Workspace."""
        first = self.root / '.agents' / 'skills' / 'sample'
        second_root = self.root / 'other'
        second = second_root / '.agents' / 'skills' / 'sample'
        for directory, content in ((first, 'First instructions'), (second, 'Second instructions')):
            directory.mkdir(parents=True)
            (directory / 'SKILL.md').write_text('---\nname: sample\ndescription: Sample\n---\n' + content)
        async with self.product() as host:
            context = host.agent_app.context
            await context.register_workspace(LocalWorkspace('other', second_root))
            manager = context.shared_resources
            one = next(row for row in (await manager.describe())['resources'] if row['kind'] == 'skill')
            two = next(row for row in (await manager.describe('other'))['resources'] if row['kind'] == 'skill')
            self.assertNotEqual(one['management_id'], two['management_id'])
            self.assertTrue(one['location'].startswith(str(first)), one['location'])
            self.assertTrue(two['location'].startswith(str(second)), two['location'])
            await manager.save(ResourceControl(name='First sample', resource_id=one['management_id'], enabled=False))
            self.assertFalse(next(row for row in (await manager.describe())['resources'] if row['kind'] == 'skill')['enabled'])
            self.assertTrue(next(row for row in (await manager.describe('other'))['resources'] if row['kind'] == 'skill')['enabled'])
            document = await manager.skill_document(one['management_id'])
            self.assertIn('First instructions', document['content'])
            self.assertNotIn('other', {workspace.info.id for workspace in host.agent_context.workspaces.entries()})

    async def test_project_tool_controls_do_not_cross_host_roots_in_one_database(self):
        """Same-named Project Tools keep different settings when a shared database serves another project root."""
        async with self.product() as host:
            manager = host.agent_app.context.shared_resources
            one = next(row for row in (await manager.describe())['resources'] if row['name'] == 'project_echo')
            await manager.save(ResourceControl(name='First project', resource_id=one['management_id'], enabled=False))
        other = self.root / 'another-project'
        other.mkdir()
        async with self.product(root=other) as host:
            two = next(row for row in (await host.agent_app.context.shared_resources.describe())['resources'] if row['name'] == 'project_echo')
            self.assertNotEqual(one['management_id'], two['management_id'])
            self.assertTrue(two['enabled'])

    async def test_code_permission_lock_ignores_existing_overrides_and_rejects_edits(self):
        """Deployment locking disables stored code-policy overrides without disabling resource switches."""
        from autoagent.agent_app import ResourcePermissions
        async with self.product() as host:
            manager = host.agent_app.context.shared_resources
            entry = next(row for row in (await manager.describe())['resources'] if row['name'] == 'project_echo')
            saved = await manager.save(ResourceControl(name='Project', resource_id=entry['management_id'], permissions=ResourcePermissions(default='deny')))
        async with self.product(environment={'AUTOAGENT_AGENT_EDIT_CODE_PERMISSIONS':'false'}) as host:
            manager = host.agent_app.context.shared_resources
            entry = next(row for row in (await manager.describe())['resources'] if row['name'] == 'project_echo')
            self.assertFalse(entry['permissions_editable'])
            self.assertIsNone(entry['permissions'])
            actual = host.agent_context.resources.get('tool','project_echo')
            self.assertIsNone(manager.filter([actual])[0].policy)
            with self.assertRaisesRegex(ValueError, 'disabled'):
                await manager.save(ResourceControl(name='Project', resource_id=entry['management_id'], permissions=ResourcePermissions(default='allow')),
                    resource_id=saved['id'], expected_version=saved['version'])

    async def test_file_policy_override_reaches_the_backend_recheck(self):
        """Allowing the file Tool replaces the default secret-path Ask at planning and execution."""
        from autoagent.agent import LLMResponse, LLMToolCall
        from autoagent.agent_app import ResourcePermissions
        from tests.test_agent_workflow import answer
        (self.root / '.env').write_text('SMOKE_TEST=example')
        provider = ScriptedProvider(LLMResponse(model='offline', finish_reason='tool_calls', output=(
            LLMToolCall(name='read_file', call_id='read', arguments='{"path":".env"}'),)), answer())
        async with self.product(provider=provider) as host:
            manager = host.agent_app.context.shared_resources
            entry = next(row for row in (await manager.describe())['resources'] if row['name'] == 'read_file')
            await manager.save(ResourceControl(name='Read files', resource_id=entry['management_id'], permissions=ResourcePermissions(default='allow')))
            result = await host.join(await host.agent_app.start_conversation('read', 'Read the test file'))
            self.assertEqual(result.status, 'completed', result.error)
            self.assertIn('SMOKE_TEST=example', str(provider.requests[-1].input))

    async def test_legacy_mcp_credentials_move_out_of_database_on_restart(self):
        """Old plaintext Header values move to the dedicated file while preserving the MCP connection."""
        async with self.product() as host:
            await host.agent_app.storage.context('save_resource', 'legacy-mcp', dict(kind='mcp', name='Legacy', enabled=False,
                transport=dict(type='streamable_http', server_id='legacy', url='https://example.test/mcp',
                    headers={'Authorization':'LEGACY_PRIVATE_TEST'}), permissions={'decision':'ask'}), 0)
        async with self.product() as host:
            rows = await host.agent_app.storage.context('resource_records')
            self.assertNotIn('LEGACY_PRIVATE_TEST', str(rows))
            configuration = rows[0]['configuration']
            self.assertEqual(configuration['permissions']['default'], 'ask')
            from autoagent.agent_app.credentials import CredentialReference
            reference = CredentialReference.model_validate_json(__import__('json').dumps(configuration['headers']['Authorization']))
            self.assertEqual(host.agent_app.credentials.resolve(reference), 'LEGACY_PRIVATE_TEST')
            self.assertEqual(host.agent_app.credentials.path.stat().st_mode & 0o777, 0o600)

    async def test_provider_configuration_survives_restart_without_changing_host_registry(self):
        """Restart restores an app-only Provider and its enabled state from public configuration."""
        from autoagent.agent_app.providers import ApplicationProviderConfig
        async with self.product() as host:
            saved = await host.agent_app.save_provider(ApplicationProviderConfig(name='Local', backend='litellm',
                provider_id='lm_studio', model_prefix='lm_studio', base_url='http://localhost:1234/v1',
                models=('tiny',), enabled=False))
        async with self.product() as host:
            restored = next(row for row in (await host.agent_app.list_providers())['items'] if row['id'] == saved['id'])
            self.assertEqual(restored['configuration'], saved['configuration'])
            self.assertFalse(host.agent_app.context.resources.has('provider', saved['alias']))
            self.assertFalse(host.agent_context.resources.has('provider', saved['alias']))


class SQLiteApplicationResourceTests(ApplicationResourceContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL test database is not configured')
class PostgreSQLApplicationResourceTests(ApplicationResourceContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

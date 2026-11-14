"""Stable product resource controls, groups, and Workspace Skill views."""

from contextlib import asynccontextmanager
import json
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
            setup.providers.register('offline', provider or ScriptedProvider())
            setup.applications["agent"].context.set_defaults(settings())
            def project_echo(text: str) -> str:
                return text
            setup.applications["agent"].context.resources.register_tool(Tool(project_echo), workflows={BUILTIN_AGENT_WORKFLOW_ID})
            yield
        async with AutoAgentHost(root=root or self.root, workflows=[builtin_agent_workflow(execution=execution)],
            lifespan=lifespan, settings=HostSettings(database_url=self.url), environment=environment or {}) as host:
            yield host

    async def test_builtin_control_survives_workflow_revision_changes(self):
        """A saved Tool switch follows its management key rather than a generated binding ID."""
        identities = []
        for execution in (False, True):
            async with self.product(execution=execution) as host:
                manager = host.applications["agent"].context.shared_resources
                inventory = await manager.describe()
                entry = next(value for value in inventory['resources'] if value['name'] == 'read_file')
                identities.append(entry)
                if not execution:
                    saved = await manager.save(ResourceControl(name='Read files', resource_id=entry['resource_id'], enabled=False))
                    self.assertEqual(saved['configuration']['resource_id'], entry['management_id'])
                else:
                    self.assertFalse(entry['enabled'])
        self.assertEqual(identities[0]['management_id'], identities[1]['management_id'])
        self.assertEqual(identities[0]['resource_id'], identities[1]['resource_id'])

    async def test_nested_policy_persists_and_tool_override_replaces_group_policy(self):
        """Nested policies survive reopening and a Tool Allow replaces its group's Ask."""
        from autoagent.agent import ToolPolicy, ToolContext, LLMToolCall
        policy = ToolPolicy.from_dict({'rules': [dict(id='protected', when={'all': [
            dict(field='args.text', op='regex', value='secret', flags='i'),
            {'not': dict(field='context.user_id', op='equals', value='owner')},
        ]}, decision='deny')]})
        async with self.product() as host:
            manager = host.applications['agent'].context.shared_resources
            entry = next(row for row in (await manager.describe())['resources'] if row['name'] == 'project_echo')
            await manager.save(GroupControl(name='Project', group_id=entry['group_id'], permissions=ToolPolicy(default='ask')))
            await manager.save(ResourceControl(name='Echo', resource_id=entry['management_id'], permissions=policy))
        async with self.product() as host:
            manager = host.applications['agent'].context.shared_resources
            row = next(row for row in (await manager.describe())['resources'] if row['name'] == 'project_echo')
            self.assertEqual(row['permissions'], policy.to_dict())
            self.assertEqual(row['permission_source'], 'tool')
            bound = manager.effective_permissions(host.applications['agent'].context.resources.get('tool', 'project_echo'))
            call = LLMToolCall(name='project_echo', call_id='one', arguments='{"text":"normal"}')
            context = ToolContext(user_id='guest').for_call(call, {'text': 'normal'})
            self.assertEqual(bound(call, context).action, 'allow')
            self.assertEqual(bound(call, context.for_call(call, {'text': 'SECRET'})).action, 'deny')
            self.assertEqual(bound(call, ToolContext(user_id='owner').for_call(call, {'text': 'SECRET'})).action, 'allow')

    async def test_registered_declarative_rules_are_visible_before_product_overrides(self):
        """The editor receives the registered Policy instead of an empty approximation of its default."""
        from autoagent.agent import ToolPolicy
        configured = ToolPolicy(default='ask', rules=[dict(id='hello', when=dict(
            field='args.text', op='equals', value='hello'), decision='allow')])
        async with self.product() as host:
            context = host.applications['agent'].context
            context.resources.set_policy('tool', 'project_echo', configured)
            row = next(row for row in (await context.shared_resources.describe())['resources'] if row['name'] == 'project_echo')
            self.assertEqual(row['permissions'], configured.to_dict())
            self.assertEqual(row['permission_source'], 'registration')
            self.assertEqual(row['default_decision'], 'ask')

    async def test_group_switch_filters_tools_without_changing_sdk_registration(self):
        """A disabled product group hides every member while the Host's explicit registrations remain enabled."""
        async with self.product() as host:
            manager = host.applications["agent"].context.shared_resources
            entry = next(row for row in (await manager.describe())['resources'] if row['name'] == 'project_echo')
            await manager.save(GroupControl(name='Project', group_id=entry['group_id'], enabled=False))
            inventory = await manager.describe()
            self.assertFalse(next(row for row in inventory['resources'] if row['name'] == 'project_echo')['enabled'])
            self.assertTrue(host.applications["agent"].context.resources.has('tool', 'project_echo'))
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
            context = host.applications["agent"].context
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
            self.assertNotIn('other', {workspace.info.id for workspace in host.extensions["agent"].context.workspaces.entries()})

    async def test_project_tool_controls_do_not_cross_host_roots_in_one_database(self):
        """Same-named Project Tools keep different settings when a shared database serves another project root."""
        async with self.product() as host:
            manager = host.applications["agent"].context.shared_resources
            one = next(row for row in (await manager.describe())['resources'] if row['name'] == 'project_echo')
            await manager.save(ResourceControl(name='First project', resource_id=one['management_id'], enabled=False))
        other = self.root / 'another-project'
        other.mkdir()
        async with self.product(root=other) as host:
            two = next(row for row in (await host.applications["agent"].context.shared_resources.describe())['resources'] if row['name'] == 'project_echo')
            self.assertNotEqual(one['management_id'], two['management_id'])
            self.assertTrue(two['enabled'])

    async def test_code_permission_lock_ignores_existing_overrides_and_rejects_edits(self):
        """Deployment locking disables stored code-policy overrides without disabling resource switches."""
        from autoagent.agent import ToolPolicy
        async with self.product() as host:
            manager = host.applications["agent"].context.shared_resources
            entry = next(row for row in (await manager.describe())['resources'] if row['name'] == 'project_echo')
            saved = await manager.save(ResourceControl(name='Project', resource_id=entry['management_id'], permissions=ToolPolicy(default='deny')))
        async with self.product(environment={'AUTOAGENT_AGENT_EDIT_CODE_PERMISSIONS':'false'}) as host:
            manager = host.applications["agent"].context.shared_resources
            entry = next(row for row in (await manager.describe())['resources'] if row['name'] == 'project_echo')
            self.assertFalse(entry['permissions_editable'])
            self.assertIsNone(entry['permissions'])
            actual = host.applications["agent"].context.resources.get('tool','project_echo')
            self.assertIsNone(manager.filter([actual])[0].policy)
            with self.assertRaisesRegex(ValueError, 'disabled'):
                await manager.save(ResourceControl(name='Project', resource_id=entry['management_id'], permissions=ToolPolicy(default='allow')),
                    resource_id=saved['id'], expected_version=saved['version'])

    async def test_builtin_permission_settings_are_locked_and_native_file_policy_still_asks(self):
        """Built-in Tool and group overrides are rejected while secret reads retain native approval."""
        from autoagent.agent import LLMResponse, LLMToolCall
        from autoagent.agent import ToolPolicy
        from tests.test_agent_workflow import answer
        (self.root / '.env').write_text('SMOKE_TEST=example')
        provider = ScriptedProvider(LLMResponse(model='offline', finish_reason='tool_calls', output=(
            LLMToolCall(name='read_file', call_id='read', arguments='{"path":".env"}'),)), answer())
        async with self.product(provider=provider) as host:
            manager = host.applications["agent"].context.shared_resources
            inventory = await manager.describe()
            builtin = [row for row in inventory['resources'] if row['origin'] == 'builtin']
            self.assertTrue(builtin)
            self.assertTrue(all(not row['permissions_editable'] for row in builtin))
            groups = [row for row in inventory['groups'] if row['origin'] == 'builtin']
            self.assertTrue(all(not row['permissions_editable'] for row in groups))
            entry = next(row for row in builtin if row['name'] == 'read_file')
            self.assertFalse(entry['approval_exempt'])
            with self.assertRaisesRegex(ValueError, 'disabled'):
                await manager.save(ResourceControl(name='Read files', resource_id=entry['management_id'], permissions=ToolPolicy(default='allow')))
            with self.assertRaisesRegex(ValueError, 'disabled'):
                await manager.save(GroupControl(name='Files', group_id=entry['group_id'], permissions=ToolPolicy(default='allow')))
            result = await host.join(await host.applications["agent"].start_conversation('read', 'Read the test file'))
            self.assertEqual(result.status, 'waiting', result.error)
            self.assertNotIn('SMOKE_TEST=example', str(provider.requests[-1].input))

    async def test_stored_builtin_overrides_cannot_change_native_permissions(self):
        """Stored Built-in overrides cannot bypass native policies or block Enable switches."""
        from autoagent.agent import ToolPolicy
        async with self.product(execution=True) as host:
            manager = host.applications["agent"].context.shared_resources
            inventory = await manager.describe()
            entry = next(row for row in inventory['resources'] if row['name'] == 'read_file')
            for key, configuration in (
                ('stored-file', ResourceControl(name='Read files', resource_id=entry['management_id'], permissions=ToolPolicy(default='allow'))),
                ('stored-group', GroupControl(name='Files', group_id=entry['group_id'], permissions=ToolPolicy(default='allow'))),
            ):
                row = await manager.service.storage('save_resource', key, configuration.model_dump(mode='json'), 0)
                manager._publish(key, row)
            current = await manager.describe()
            shown = next(row for row in current['resources'] if row['name'] == 'read_file')
            self.assertIsNone(shown['permissions'])
            self.assertIsNone(next(row for row in current['groups'] if row['id'] == entry['group_id'])['permissions'])
            actual = manager._known_resources[entry['management_id']]
            self.assertIsNone(manager.effective_permissions(actual))
            self.assertIs(manager.filter([actual])[0], actual)
            for key in ('stored-file', 'stored-group'):
                old, configuration = manager.records[key]
                saved = await manager.save(configuration.model_copy(update={'enabled':False}), resource_id=key, expected_version=old['version'])
                self.assertIsNone(saved['configuration']['permissions'])
            self.assertFalse(next(row for row in (await manager.describe())['resources'] if row['name'] == 'read_file')['enabled'])

    async def test_mcp_only_accepts_environment_credential_references(self):
        """Credential-file references are rejected and environment values remain outside storage."""
        from autoagent.agent_app.credentials import CredentialReference
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            CredentialReference(source="file", name="MCP_TOKEN")
        async with self.product(environment={"MCP_TOKEN": "PRIVATE_TEST"}) as host:
            self.assertEqual(host.applications["agent"].credentials.resolve(CredentialReference(name="MCP_TOKEN")), "PRIVATE_TEST")
            self.assertFalse(hasattr(host.applications["agent"].credentials, "path"))

    async def test_host_json_defaults_survive_restart_without_a_preferences_table(self):
        """Both defaults come from project JSON and no product preferences table is created."""
        from sqlalchemy import inspect
        (self.root / 'providers.json').write_text(json.dumps({
            'default_model':{'provider':'local','model':'tiny'},
            'compaction_model':{'provider':'local','model':'tiny'},
            'providers':{'local':{'models':[{'id':'tiny'}]}}}))
        async with self.product():
            pass
        async with self.product() as host:
            self.assertEqual(host.providers.default_model.model, 'tiny')
            self.assertEqual(host.applications["agent"].context.compaction_model, 'tiny')
            self.assertEqual(host.extensions["agent"].context.compaction_model, 'tiny')
            async def table_names():
                async with host._persistence._writer.storage.engine.connect() as conn:
                    return await conn.run_sync(lambda sync: inspect(sync).get_table_names())
            tables = await host._persistence._writer.run(table_names)
            self.assertNotIn('agent_app_preferences', tables)


class SQLiteApplicationResourceTests(ApplicationResourceContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL test database is not configured')
class PostgreSQLApplicationResourceTests(ApplicationResourceContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

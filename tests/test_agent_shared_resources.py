"""Shared Agent resources persist independently of Session preferences."""

import asyncio
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from autoagent.agent_app import ResourceControl, SkillResourceConfig, MCPResourceConfig
from autoagent.agent import Tool, ToolOperation
from autoagent.host import AutoAgentHost, HostSettings
from autoagent.agent_app import builtin_agent_workflow
from autoagent.agent import agent_workflow
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_session_controls import SessionControlContracts
from tests.test_agent_workflow import ScriptedProvider, answer, calls


class SharedResourceContracts:
    product = SessionControlContracts.product
    database_url = SessionControlContracts.database_url

    async def eventually(self, operation, predicate):
        async with asyncio.timeout(10):
            while True:
                result = await operation()
                if predicate(result):
                    return result
                await asyncio.sleep(.02)

    async def test_shared_skills_apply_to_existing_and_new_sessions_without_opt_in(self):
        """Shared Skills are discovered live, disabled everywhere, and isolated from ungranted Workflows."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            skill = root / 'sample'
            skill.mkdir()
            document = skill / 'SKILL.md'
            document.write_text('---\nname: sample\ndescription: Shared sample\n---\nInitial instructions')
            async with self.product(ScriptedProvider(answer(), answer()),
                extra_workflows=[agent_workflow(workflow_id='private',files=False,execution=False)]) as (host, client):
                first = await client.post('/api/v2/agent/messages', json={'session_id':'old','message':'Hello','workspace_id':None})
                await host.join(first.json()['invocation_id'])
                created = await client.post('/api/v2/agent/resource-configs', json={'configuration':{
                    'kind':'skills','name':'Shared skills','path':str(root),'interval_seconds':.02}})
                self.assertEqual(created.status_code, 201, created.text)
                config = created.json()
                catalog = (await client.get('/api/v2/agent/catalog')).json()
                async def resources(session='old'):
                    return (await client.get('/api/v2/agent/resources', params={
                        'revision_id':catalog['agent']['revision_id'],'session_id':session,'unbound':True})).json()
                await self.eventually(resources, lambda result: bool(result['skills']))
                self.assertEqual((await resources('new'))['skills'][0]['name'], 'sample')
                private = await host.agent_context.workspace_resources(host._runtime.workflow_ids['private'])
                self.assertEqual(private['skills'],[])
                self.assertNotIn('skill_load',{tool['name'] for tool in private['tools']})
                document.write_text('---\nname: sample\ndescription: Updated sample\n---\nNew instructions')
                await self.eventually(resources, lambda result: bool(result['skills']) and result['skills'][0]['description'] == 'Updated sample')
                disabled = await client.put('/api/v2/agent/resource-configs/'+config['id'], json={
                    'configuration':{**config['configuration'],'enabled':False},'expected_version':config['version']})
                self.assertEqual(disabled.status_code, 200, disabled.text)
                self.assertEqual((await resources())['skills'], [])
                self.assertNotIn('skill_load', {tool['name'] for tool in (await resources('new'))['tools']})
                stale = await client.delete('/api/v2/agent/resource-configs/'+config['id'], params={'expected_version':1})
                self.assertEqual(stale.status_code, 409)
                deleted = await client.delete('/api/v2/agent/resource-configs/'+config['id'], params={'expected_version':2})
                self.assertEqual(deleted.status_code, 200, deleted.text)

    async def test_shared_permission_change_resolves_pending_wait_without_session_update(self):
        """A shared Deny resolves existing approvals and prevents Tool effects in every Session."""
        effects = []
        def publish(text: str) -> str:
            """Record a protected effect."""
            effects.append(text)
            return text
        provider = ScriptedProvider(calls('publish'), answer())
        async with self.product(provider, tools=[Tool(publish, operations=lambda arguments, context: (
            ToolOperation(action='note.write',resource=arguments['text']),))]) as (host, client):
            entry = host.agent_context.resources.get('tool','publish')
            created = await host.agent_app.context.shared_resources.save(ResourceControl(
                name='Publish permission',resource_id=entry.resource_id,permissions={
                    'rules':[{'conditions':[{'source':'operation','path':'/action','value':'note.write'}, {'source':'operation','path':'/resource','value':'hello'}],'decision':'ask'}]}))
            first = await client.post('/api/v2/agent/messages',json={'session_id':'old','message':'Publish'})
            iid = first.json()['invocation_id']
            self.assertEqual((await host.join(iid)).status,'waiting')
            settings = await host.agent_app.context.get_settings('old')
            updated = await host.agent_app.context.shared_resources.save(ResourceControl(
                name='Publish permission',resource_id=entry.resource_id,permissions={
                    'rules':[{'conditions':[{'source':'operation','path':'/action','value':'note.write'}, {'source':'operation','path':'/resource','value':'hello'}],'decision':'deny'}]}),
                resource_id=created['id'],expected_version=created['version'])
            result = await self.eventually(lambda: host.join(iid),lambda result: result.status == 'completed')
            self.assertFalse(effects)
            self.assertEqual((await host.agent_app.context.get_settings('old')).version,settings.version)
            self.assertIn('tool_access_denied',str(provider.requests[-1].input))
            self.assertEqual(updated['version'],2)

    async def test_mcp_credentials_use_references_and_survive_edit(self):
        """MCP configuration stores only credential names and never accepts raw Header secrets."""
        async with self.product(ScriptedProvider()) as (host, client):
            # Disabled configuration exercises storage without opening any real network connection.
            created = await client.post('/api/v2/agent/resource-configs',json={'configuration':{
                'kind':'mcp','name':'Remote','enabled':False,'transport':{
                    'type':'streamable_http','server_id':'remote','url':'https://example.test/mcp',
                    },'headers':{'Authorization':{'source':'env','name':'MCP_AUTHORIZATION'}}}})
            self.assertEqual(created.status_code,201,created.text)
            self.assertNotIn('PRIVATE_VALUE',created.text)
            config = created.json()
            self.assertEqual(config['credential_fields'],['headers.Authorization'])
            updated = await client.put('/api/v2/agent/resource-configs/'+config['id'],json={
                'configuration':{**config['configuration'],'name':'Renamed'},'expected_version':config['version']})
            self.assertEqual(updated.status_code,200,updated.text)
            rows = await host.agent_app.storage.context('resource_records')
            self.assertEqual(rows[0]['configuration']['headers']['Authorization'],{'source':'env','name':'MCP_AUTHORIZATION'})
            self.assertNotIn('PRIVATE_VALUE',(await client.get('/api/v2/agent/resource-configs')).text)

    async def test_registered_tool_controls_hide_and_restore_without_rebinding(self):
        """Shared visibility controls retain the registered implementation and need no per-Session opt in."""
        def echo(text: str) -> str:
            """Echo text."""
            return text
        async with self.product(ScriptedProvider(answer()),tools=[echo]) as (host, client):
            entry = host.agent_context.resources.get('tool','echo')
            control = await host.agent_app.context.shared_resources.save(ResourceControl(
                name='Echo',resource_id=entry.resource_id,enabled=False))
            first = await client.post('/api/v2/agent/messages',json={'session_id':'one','message':'Hello'})
            self.assertEqual((await host.join(first.json()['invocation_id'])).status,'completed')
            self.assertNotIn('echo',{value.name for value in host.agent_context.resources.get('provider','offline').value.requests[0].tools})
            await host.agent_app.context.shared_resources.save(None,resource_id=control['id'],expected_version=control['version'])
            self.assertEqual(host.agent_context.resources.get('tool','echo').version,entry.version)
            listed = (await client.get('/api/v2/agent/resource-configs')).json()
            self.assertTrue(next(row for row in listed['resources'] if row['name']=='echo')['enabled'])

    async def test_shared_resources_restore_on_host_restart(self):
        """A persisted Skill source is restored independently of Session settings and startup models."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            skills = root / 'shared'
            skill = skills / 'sample'
            skill.mkdir(parents=True)
            (skill / 'SKILL.md').write_text('---\nname: sample\ndescription: Shared sample\n---\nInstructions')
            url = await self.database_url(root)
            options = dict(root=root, workflows=[builtin_agent_workflow(execution=False)],
                settings=HostSettings(database_url=url), environment={})
            async with AutoAgentHost(**options) as host:
                created = await host.agent_app.context.shared_resources.save(SkillResourceConfig(name='Shared',path=str(skills)))
                await self.eventually(host.agent_app.context.shared_resources.describe,
                    lambda value: value['items'][0]['state'] == 'ready')
            async with AutoAgentHost(**options) as host:
                restored = await self.eventually(host.agent_app.context.shared_resources.describe,
                    lambda value: bool(value['resources']))
                self.assertEqual(restored['items'][0]['id'],created['id'])
                self.assertEqual(restored['resources'][0]['name'],'sample')
            async with AutoAgentHost(**{**options,'workflows':[agent_workflow(workflow_id='private')]}) as host:
                hidden = await host.agent_app.context.shared_resources.describe()
                self.assertEqual(hidden['items'], [])
                self.assertFalse(host.agent_app.context.shared_resources.sources)
                self.assertFalse(hidden['resources'])
                self.assertIsNone(host.agent_app.context.shared_resources.monitor)

    async def test_mcp_refresh_and_delete_drain_existing_borrow_without_leaking_catalog(self):
        """MCP reconnect republishes Tools; deletion hides immediately while existing borrowers finish."""
        from tests.test_agent_mcp import MCPHTTPTests, WireServer
        wire = WireServer(legacy=True)
        clients = []
        def factory(config, **options):
            client, http = MCPHTTPTests.client(self, wire, sse=True)
            clients.append(client)
            return client
        with patch('autoagent.agent.MCPClient',side_effect=factory):
            async with self.product(ScriptedProvider()) as (host, client):
                managed = host.agent_app.context.shared_resources
                created = await managed.save(MCPResourceConfig(name='Local wire',transport={
                    'type':'sse','server_id':'wire','url':'https://server.test/sse'}))
                ready = await self.eventually(managed.describe,lambda value: bool(value['resources']) and value['items'][0]['state'] == 'ready' or value['items'][0]['state'] == 'error')
                self.assertEqual(ready['items'][0]['state'],'ready',(ready,repr(getattr(managed.sources[created['id']].source,'last_error',None))))
                version = ready['resources'][0]['version']
                await host.agent_context.approvals._run(clients[0].areconnect)
                refreshed = await managed.describe()
                self.assertNotEqual(refreshed['resources'][0]['version'],version)
                state = managed.sources[created['id']]
                revised = await managed.save(MCPResourceConfig.model_validate_json(
                    json.dumps({**created['configuration'],'permissions':{'default':'ask'}})),
                    resource_id=created['id'],expected_version=created['version'])
                self.assertIs(managed.sources[created['id']],state)
                self.assertEqual((await managed.describe())['resources'][0]['version'],refreshed['resources'][0]['version'])
                entered, release = asyncio.Event(), asyncio.Event()
                async def borrow():
                    async with managed.borrow(refreshed['resources'][0]['resource_id']):
                        entered.set()
                        await release.wait()
                async def begin():
                    task = asyncio.create_task(borrow())
                    await entered.wait()
                    return task
                task = await host.agent_context.approvals._run(begin)
                await managed.save(None,resource_id=created['id'],expected_version=revised['version'])
                self.assertFalse([entry for entry in (await managed.describe())['resources'] if entry['origin'] == 'mcp'])
                self.assertFalse(state.task.done())
                self.assertTrue(clients[0].is_connected)
                async def finish():
                    release.set()
                    await task
                    await state.task
                await host.agent_context.approvals._run(finish)
                self.assertFalse(clients[0].is_connected)


class SQLiteSharedResourceTests(SharedResourceContracts,unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL,'PostgreSQL URL not configured')
class PostgreSQLSharedResourceTests(SharedResourceContracts,unittest.IsolatedAsyncioTestCase):
    async def database_url(self, root):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

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

    async def test_mcp_tool_name_conflict_matches_the_model_inventory(self):
        """A colliding MCP Tool is marked unavailable while other Tools on that server remain usable."""
        from contextlib import asynccontextmanager
        from tests.test_agent_mcp import MCPHTTPTests, WireServer
        from tests.test_agent_workflow import settings

        provider = ScriptedProvider(answer())

        def factory(config, **options):
            wire = WireServer(legacy=True)
            wire.rpc_override = lambda message: {"result": {"tools": [
                wire.tool, {**wire.tool, "name": "multiply"}], "resultType": "complete"}} if message["method"] == "tools/list" else None
            return MCPHTTPTests.client(self, wire, sse=True)[0]

        @asynccontextmanager
        async def setup(runtime):
            def add(a: int, b: int) -> int:
                return a + b
            app = runtime.applications["agent"]
            runtime.providers.register("offline", provider)
            app.context.set_defaults(settings())
            app.context.resources.register_tool(Tool(add), workflows={"autoagent.agent"})
            yield

        with tempfile.TemporaryDirectory() as directory, patch("autoagent.agent.MCPClient", side_effect=factory):
            root = Path(directory)
            url = await self.database_url(root)
            async with AutoAgentHost(root=root, workflows=[builtin_agent_workflow(execution=False)],
                    settings=HostSettings(database_url=url), environment={}, lifespan=setup) as host:
                app = host.applications["agent"]
                connection = await app.context.shared_resources.save(MCPResourceConfig(name="Wire", transport={
                    "type": "sse", "server_id": "wire", "url": "https://server.test/sse"}))
                inventory = await self.eventually(lambda: app.resource_configs(scope="mcp"), lambda data:
                    any(tool["mcp_status"] == "conflict" for tool in data["resources"]))
                rows = {row["remote_name"]: row for row in inventory["resources"]}
                self.assertFalse(rows["add"]["available"])
                self.assertTrue(rows["multiply"]["available"])
                record = next(item for item in inventory["items"] if item["id"] == connection["id"])
                self.assertEqual(record["state"], "conflict")
                self.assertIn("conflict", record["error"])
                result = await host.join(await app.start_conversation("chat", "Check tool catalog"))
                self.assertEqual(result.status, "completed", result.error)
                model_tools = {tool.name: tool for tool in provider.requests[0].tools}
                self.assertIn("multiply", model_tools)
                self.assertNotEqual(model_tools["add"].description, "Remote addition")

    async def test_mcp_catalog_survives_offline_restart_and_requires_schema_review(self):
        """A reconnected tool keeps custom rules and stays unpublished until its changed schema is reviewed."""
        from tests.test_agent_mcp import MCPHTTPTests, WireServer
        from autoagent.agent import ToolPolicy

        online = True
        argument_type = "integer"
        def factory(config, **options):
            if not online:
                raise RuntimeError("Offline fixture")
            wire = WireServer(legacy=True)
            wire.tool["inputSchema"]["properties"]["a"]["type"] = argument_type
            client, _ = MCPHTTPTests.client(self, wire, sse=True)
            return client

        with tempfile.TemporaryDirectory() as directory, patch("autoagent.agent.MCPClient", side_effect=factory):
            root = Path(directory)
            url = await self.database_url(root)
            options = dict(root=root, workflows=[builtin_agent_workflow(execution=False)],
                settings=HostSettings(database_url=url), environment={})
            async with AutoAgentHost(**options) as host:
                app = host.applications["agent"]
                managed = app.context.shared_resources
                config = MCPResourceConfig(name="Wire", transport={
                    "type": "sse", "server_id": "wire", "url": "https://server.test/sse"})
                created = await managed.save(config)
                inventory = await self.eventually(app.resource_configs, lambda value: any(
                    row.get("mcp_status") == "ready" for row in value["resources"]))
                tool = next(row for row in inventory["resources"] if row["origin"] == "mcp")
                await managed.save(ResourceControl(name="Add", resource_id=tool["management_id"],
                    permissions=ToolPolicy(default="deny")))
                self.assertFalse(any(row["origin"] == "mcp" for row in (await app.resource_configs(scope="tools"))["resources"]))
                original = tool["schema_version"]
            online = False
            async with AutoAgentHost(**options) as host:
                app = host.applications["agent"]
                managed = app.context.shared_resources
                async def inventory():
                    return await app.resource_configs(scope="mcp")
                offline = await self.eventually(inventory, lambda value: any(
                    row["id"] == created["id"] and row["state"] == "error" for row in value["items"]))
                self.assertEqual(offline["resources"][0]["mcp_status"], "offline")
                self.assertEqual(offline["resources"][0]["custom_policy"]["default"], "deny")
                online, argument_type = True, "string"
                await managed.save(config, resource_id=created["id"], expected_version=1)
                review = await self.eventually(inventory, lambda value: any(
                    row["mcp_status"] == "review" for row in value["resources"]))
                changed = review["resources"][0]
                self.assertEqual(changed["approved_schema"]["properties"]["a"]["type"], "integer")
                self.assertEqual(changed["input_schema"]["properties"]["a"]["type"], "string")
                self.assertFalse(changed["available"])
                self.assertFalse(managed.catalog()[0])
                from autoagent.host import HostRunConflict
                with self.assertRaises(HostRunConflict):
                    await app.accept_mcp_schema(created["id"], "add", original)
                await app.accept_mcp_schema(created["id"], "add", changed["schema_version"])
                restored = (await inventory())["resources"][0]
                self.assertTrue(restored["available"])
                self.assertEqual(restored["custom_policy"], ToolPolicy(default="deny").to_dict())

    async def test_policy_editor_parses_yaml_and_previews_real_matching_without_execution(self):
        """The product parser, preview and saved Policy share nested SDK semantics on both databases."""
        effects = []
        def echo(text: str) -> str:
            effects.append(text)
            return text
        async with self.product(ScriptedProvider(), tools=[Tool(echo)]) as (host, client):
            text = '''rules:
  - id: private
    when:
      all:
        - field: args.text
          op: regex
          value: secret
          flags: i
        - not:
            field: context.user_id
            op: equals
            value: owner
    decision: deny
'''
            parsed = await client.post('/api/v2/agent/policies/parse', json={'format': 'yaml', 'text': text})
            self.assertEqual(parsed.status_code, 200, parsed.text)
            value = parsed.json()
            self.assertNotIn('not_', value['json'])
            for user, expected in (('owner', 'allow'), ('guest', 'deny')):
                preview = await client.post('/api/v2/agent/policies/preview', json={
                    'policy': value['policy'], 'tool_name': 'echo', 'arguments': {'text': 'SECRET'},
                    'context': {'user_id': user}})
                self.assertEqual(preview.status_code, 200, preview.text)
                self.assertEqual(preview.json()['decision']['action'], expected)
                self.assertEqual(preview.json()['operations'][0]['default_used'], user == 'owner')
            invalid = await client.post('/api/v2/agent/policies/parse', json={'format': 'yaml', 'text': 'rules: ['})
            self.assertEqual(invalid.status_code, 400, invalid.text)
            legacy = await client.post('/api/v2/agent/policies/parse', json={'format': 'json', 'text':
                '{"rules":[{"conditions":[],"decision":"allow"}]}'})
            self.assertEqual(legacy.status_code, 422, legacy.text)
            self.assertEqual(effects, [])
            self.assertFalse(host.applications['agent'].context.shared_resources.records)

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
                extra_workflows=[agent_workflow(workflow_id='private',)]) as (host, client):
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
                await self.eventually(resources, lambda result: any(row['name'] == 'sample' for row in result['skills']))
                self.assertEqual((await resources('new'))['skills'][0]['name'], 'sample')
                private = await host.extensions["agent"].context.resource_catalog(host._runtime.workflow_ids['private'])
                self.assertEqual(private['skills'],[])
                self.assertNotIn('skill_load',{tool['name'] for tool in private['tools']})
                document.write_text('---\nname: sample\ndescription: Updated sample\n---\nNew instructions')
                await self.eventually(resources, lambda result: bool(result['skills']) and result['skills'][0]['description'] == 'Updated sample')
                disabled = await client.put('/api/v2/agent/resource-configs/'+config['id'], json={
                    'configuration':{**config['configuration'],'enabled':False},'expected_version':config['version']})
                self.assertEqual(disabled.status_code, 200, disabled.text)
                self.assertNotIn('sample', {row['name'] for row in (await resources())['skills']})
                self.assertNotIn('sample', {row['name'] for row in (await resources('new'))['skills']})
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
            entry = host.applications["agent"].context.resources.get('tool','publish')
            created = await host.applications["agent"].context.shared_resources.save(ResourceControl(
                name='Publish permission',resource_id=entry.resource_id,permissions={
                    'rules':[{'when':{'all':[{'field':'operation.action','op':'equals','value':'note.write'}, {'field':'operation.resource','op':'equals','value':'hello'}]},'decision':'ask'}]}))
            first = await client.post('/api/v2/agent/messages',json={'session_id':'old','message':'Publish'})
            iid = first.json()['invocation_id']
            self.assertEqual((await host.join(iid)).status,'waiting')
            settings = await host.applications["agent"].context.get_settings('old')
            updated = await host.applications["agent"].context.shared_resources.save(ResourceControl(
                name='Publish permission',resource_id=entry.resource_id,permissions={
                    'rules':[{'when':{'all':[{'field':'operation.action','op':'equals','value':'note.write'}, {'field':'operation.resource','op':'equals','value':'hello'}]},'decision':'deny'}]}),
                resource_id=created['id'],expected_version=created['version'])
            result = await self.eventually(lambda: host.join(iid),lambda result: result.status == 'completed')
            self.assertFalse(effects)
            self.assertEqual((await host.applications["agent"].context.get_settings('old')).version,settings.version)
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
            rows = await host.applications["agent"].storage.context('resource_records')
            self.assertEqual(rows[0]['configuration']['headers']['Authorization'],{'source':'env','name':'MCP_AUTHORIZATION'})
            self.assertNotIn('PRIVATE_VALUE',(await client.get('/api/v2/agent/resource-configs')).text)

    async def test_registered_tool_controls_hide_and_restore_without_rebinding(self):
        """Shared visibility controls retain the registered implementation and need no per-Session opt in."""
        def echo(text: str) -> str:
            """Echo text."""
            return text
        async with self.product(ScriptedProvider(answer()),tools=[echo]) as (host, client):
            entry = host.applications["agent"].context.resources.get('tool','echo')
            control = await host.applications["agent"].context.shared_resources.save(ResourceControl(
                name='Echo',resource_id=entry.resource_id,enabled=False))
            first = await client.post('/api/v2/agent/messages',json={'session_id':'one','message':'Hello'})
            self.assertEqual((await host.join(first.json()['invocation_id'])).status,'completed')
            self.assertNotIn('echo',{value.name for value in host.providers.get('offline').requests[0].tools})
            await host.applications["agent"].context.shared_resources.save(None,resource_id=control['id'],expected_version=control['version'])
            self.assertEqual(host.applications["agent"].context.resources.get('tool','echo').version,entry.version)
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
                created = await host.applications["agent"].context.shared_resources.save(SkillResourceConfig(name='Shared',path=str(skills)))
                await self.eventually(host.applications["agent"].context.shared_resources.describe,
                    lambda value: value['items'][0]['state'] == 'ready')
            async with AutoAgentHost(**options) as host:
                restored = await self.eventually(host.applications["agent"].context.shared_resources.describe,
                    lambda value: any(row['kind'] == 'skill' and row['name'] == 'sample' for row in value['resources']))
                self.assertEqual(restored['items'][0]['id'],created['id'])
                self.assertEqual([row['name'] for row in restored['resources'] if row['kind'] == 'skill' and row['origin'] == 'manual'], ['sample'])
            async with AutoAgentHost(**{**options,'workflows':[agent_workflow(workflow_id='private')]}) as host:
                self.assertNotIn("agent", host.applications)
                private = await host.extensions["agent"].context.resource_catalog(host._runtime.workflow_ids["private"])
                self.assertEqual(private["skills"], [])


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
                managed = host.applications["agent"].context.shared_resources
                created = await managed.save(MCPResourceConfig(name='Local wire',transport={
                    'type':'sse','server_id':'wire','url':'https://server.test/sse'}))
                ready = await self.eventually(managed.describe,lambda value:
                    any(row['origin'] == 'mcp' for row in value['resources']) and value['items'][0]['state'] == 'ready'
                    or value['items'][0]['state'] == 'error')
                self.assertEqual(ready['items'][0]['state'],'ready',(ready,repr(getattr(managed.sources[created['id']].source,'last_error',None))))
                remote = next(row for row in ready['resources'] if row['origin'] == 'mcp')
                version = remote['version']
                await host.applications["agent"].context.approvals._run(clients[0].areconnect)
                refreshed = await managed.describe()
                remote = next(row for row in refreshed['resources'] if row['origin'] == 'mcp')
                self.assertNotEqual(remote['version'],version)
                state = managed.sources[created['id']]
                revised = await managed.save(MCPResourceConfig.model_validate_json(
                    json.dumps({**created['configuration'],'permissions':{'default':'ask'}})),
                    resource_id=created['id'],expected_version=created['version'])
                self.assertIs(managed.sources[created['id']],state)
                current = next(row for row in (await managed.describe())['resources'] if row['origin'] == 'mcp')
                self.assertEqual(current['version'], remote['version'])
                entered, release = asyncio.Event(), asyncio.Event()
                async def borrow():
                    async with managed.borrow(remote['resource_id']):
                        entered.set()
                        await release.wait()
                async def begin():
                    task = asyncio.create_task(borrow())
                    await entered.wait()
                    return task
                task = await host.applications["agent"].context.approvals._run(begin)
                await managed.save(None,resource_id=created['id'],expected_version=revised['version'])
                self.assertFalse([entry for entry in (await managed.describe())['resources'] if entry['origin'] == 'mcp'])
                self.assertFalse(state.task.done())
                self.assertTrue(clients[0].is_connected)
                async def finish():
                    release.set()
                    await task
                    await state.task
                await host.applications["agent"].context.approvals._run(finish)
                self.assertFalse(clients[0].is_connected)


class SQLiteSharedResourceTests(SharedResourceContracts,unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL,'PostgreSQL URL not configured')
class PostgreSQLSharedResourceTests(SharedResourceContracts,unittest.IsolatedAsyncioTestCase):
    async def database_url(self, root):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

"""Agent product defaults and read-only Host model discovery remain separate from SDK bindings."""
import json
import unittest
from unittest.mock import AsyncMock, patch

from tests.test_agent_session_controls import SessionControlContracts
from tests.test_agent_workflow import ScriptedProvider, answer
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase


class ProviderContracts:
    product = SessionControlContracts.product
    database_url = SessionControlContracts.database_url

    async def test_providers_are_read_only_and_defaults_are_host_owned(self):
        """The product lists Host metadata, rejects connection writes, and reads defaults from project JSON."""
        async with self.product(ScriptedProvider()) as (host, client):
            path = host.root / 'providers.json'
            path.write_text(json.dumps({'providers':{'local':{'api_key':'PRIVATE_TEST_VALUE',
                'models':[{'id':'tiny','name':'Tiny'}]}}}))
            response = await client.post('/api/v2/providers/reload')
            self.assertEqual(response.status_code, 200, response.text)
            self.assertNotIn('PRIVATE_TEST_VALUE', response.text)
            data = (await client.get('/api/v2/agent/catalog')).json()
            self.assertEqual(data['provider_options'][0]['models'], ['tiny'])
            self.assertEqual((await client.post('/api/v2/agent/providers', json={})).status_code, 404)
            data = json.loads(path.read_text())
            data['default_model'] = data['compaction_model'] = {'provider':'local','model':'tiny'}
            path.write_text(json.dumps(data))
            response = await client.post('/api/v2/providers/reload')
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual((await client.get('/api/v2/agent/preferences')).status_code, 404)
            self.assertEqual(host.providers.default_model.model, 'tiny')
            new_chat = (await client.get('/api/v2/agent/settings', params={'session_id':'new-chat'})).json()
            self.assertEqual(new_chat['settings']['model']['model'], 'tiny')
            from autoagent.host.providers import CatalogProvider
            provider = ScriptedProvider(answer('New connection is usable'))
            provider.aclose = AsyncMock()
            with patch.object(CatalogProvider, '_make_transport', return_value=provider):
                result = await host.join(await host.applications["agent"].start_conversation('new-chat', 'Hello', unbound=True))
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual(len(provider.requests), 1)

    async def test_prepared_compaction_keeps_its_captured_provider(self):
        """The shared Host selection honors the prepared compaction route for code-supplied main Providers."""
        from tests.test_agent_providers import request
        async with self.product(ScriptedProvider(answer())) as (host, client):
            ref = await host.applications["agent"].start_conversation('compaction-route', 'First')
            await host.join(ref)
            snapshot = await host.applications["agent"].context.get_snapshot(ref.invocation_id)
            summarizer = ScriptedProvider()
            prepared = request(model='summary-model', routing={'agent_snapshot_id':snapshot.snapshot_id,
                'agent_role':'compaction','compaction_provider':'summary'})
            with patch.object(host.applications["agent"].context, 'snapshot', AsyncMock(return_value=snapshot)), \
                    patch.object(host.providers, 'get', return_value=summarizer) as lookup:
                selected = await host.applications["agent"].context.resolve_selection(prepared)
            self.assertIs(selected.provider, summarizer)
            lookup.assert_called_once_with('summary')

    async def test_mcp_connection_preview_lists_tools_without_saving_or_executing(self):
        """A full SDK preview registers privately without invoking or persisting remote Tools."""
        from tests.test_agent_mcp import MCPHTTPTests, WireServer
        server = WireServer()
        connection, _ = MCPHTTPTests.client(self, server)
        async with self.product(ScriptedProvider()) as (host, client):
            with patch('autoagent.agent.MCPClient', return_value=connection):
                response = await client.post('/api/v2/agent/mcp/test', json={'configuration':{
                    'kind':'mcp','name':'Preview','prefix':'remote_',
                    'transport':{'type':'streamable_http','server_id':'preview','url':'https://example.test/mcp'}}})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertTrue(response.json()['connected'], response.text)
            self.assertEqual(response.json()['tools'][0]['name'], 'add')
            self.assertEqual(await host.applications["agent"].storage.context('resource_records'), [])
            self.assertEqual(server.calls, [])
            self.assertFalse(connection._message_observers)

    async def test_mcp_preview_rejects_unusable_catalog_listener(self):
        """Preview catches actual registration failures even when tools/list succeeds."""
        from tests.test_agent_mcp import MCPHTTPTests, WireServer
        server = WireServer(list_changed=True)
        server.rpc_override = lambda message: {'error':{'code':-32603, 'message':'PRIVATE_SERVER_DETAIL'}} if message['method'] == 'subscriptions/listen' else None
        connection, _ = MCPHTTPTests.client(self, server)
        async with self.product(ScriptedProvider()) as (host, client):
            with patch('autoagent.agent.MCPClient', return_value=connection):
                response = await client.post('/api/v2/agent/mcp/test', json={'configuration':{
                    'kind':'mcp','name':'Preview','transport':{
                        'type':'streamable_http','server_id':'preview','url':'https://example.test/mcp'}}})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertFalse(response.json()['connected'])
            self.assertEqual(response.json()['tools'], [])
            self.assertNotIn('PRIVATE_SERVER_DETAIL', response.text)
            self.assertEqual(await host.applications["agent"].storage.context('resource_records'), [])
            self.assertEqual(server.calls, [])

    async def test_saved_mcp_without_notifications_registers_tools(self):
        """Saving a server without subscriptions exposes its catalog through product resources."""
        import asyncio
        from tests.test_agent_mcp import MCPHTTPTests, WireServer
        server = WireServer()
        connection, _ = MCPHTTPTests.client(self, server)
        async with self.product(ScriptedProvider()) as (host, client):
            with patch('autoagent.agent.MCPClient', return_value=connection):
                response = await client.post('/api/v2/agent/resource-configs', json={'configuration':{
                    'kind':'mcp','name':'Saved connection','transport':{
                        'type':'streamable_http','server_id':'saved','url':'https://example.test/mcp'}}})
                self.assertEqual(response.status_code, 201, response.text)
                async with asyncio.timeout(5):
                    while True:
                        inventory = await host.applications["agent"].resource_configs(scope='mcp')
                        remote = inventory['resources']
                        if remote and all(entry['mcp_status'] == 'ready' for entry in remote):
                            break
                        await asyncio.sleep(0.01)
                self.assertEqual([entry['name'] for entry in remote], ['add'])
                self.assertTrue(remote[0]['enabled'])
                self.assertEqual(len(await host.applications["agent"].storage.context('resource_records')), 1)
                self.assertEqual(server.calls, [])

    async def test_real_mcp_preview_uses_sdk_tool_schema(self):
        """A real local MCP preview returns JSON schemas without persisting the connection."""
        import sys
        from pathlib import Path
        async with self.product(ScriptedProvider()) as (host, client):
            response = await client.post('/api/v2/agent/mcp/test', json={'configuration':{
                'kind':'mcp', 'name':'Local preview', 'transport':{
                    'type':'stdio', 'server_id':'local-preview', 'command':sys.executable,
                    'args':[str(Path(__file__).parent / 'fixtures' / 'mcp_tool_server.py')],
                    'connect_timeout_seconds':5}}})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertTrue(response.json()['connected'], response.text)
            add = next(tool for tool in response.json()['tools'] if tool['name'] == 'add')
            self.assertIn('a', add['input_schema']['properties'])
            self.assertEqual(await host.applications["agent"].storage.context('resource_records'), [])


class SQLiteProviderTests(ProviderContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL URL not configured')
class PostgreSQLProviderTests(ProviderContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self, root):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

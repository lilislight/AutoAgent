"""Product connections persist public configuration and resolve credentials outside SQL."""

import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from autoagent.agent_app.providers import ApplicationProviderConfig
from autoagent.agent_app.credentials import CredentialReference
from autoagent.agent_app.models import ApplicationSessionSettings
from tests.test_agent_session_controls import SessionControlContracts
from tests.test_agent_workflow import ScriptedProvider, answer
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase


class ProviderFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_closing_the_application_stream_closes_its_transport(self):
        """Core cancellation closes the borrowed transport stream immediately rather than waiting for garbage collection."""
        from autoagent.agent_app.providers import _Provider
        from types import SimpleNamespace
        closed = []
        async def stream(request):
            try:
                yield 'first'
                yield 'second'
            finally:
                closed.append(True)
        provider = _Provider(ApplicationProviderConfig(name='Test'), SimpleNamespace())
        provider.client = SimpleNamespace(astream=stream)
        source = provider.astream(None)
        self.assertEqual(await anext(source), 'first')
        await source.aclose()
        self.assertEqual(closed, [True])

    async def test_usage_error_record_is_redacted_before_persistence(self):
        """The underlying SDK records a sanitized transport failure before the outer wrapper sees it."""
        from autoagent.agent_app.providers import _Provider
        from autoagent.agent import LLMRequest, LLMProviderError, LLMMessage, TextContent, llm_recording_context
        from types import SimpleNamespace
        records = []
        class Recorder:
            async def record_llm_call(self, record):
                records.append(record)
        client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(
            create=AsyncMock(side_effect=RuntimeError('Context exceeded PRIVATE_TEST_VALUE')))))
        provider = _Provider(ApplicationProviderConfig(name='Test', api_key=CredentialReference(name='KEY')),
            SimpleNamespace(resolve=lambda reference:'PRIVATE_TEST_VALUE'))
        with patch('autoagent.agent.providers._openai_client.OpenAIClientOwner._get_client', return_value=client):
            with llm_recording_context(recorder=Recorder()):
                with self.assertRaises(LLMProviderError) as captured:
                    await provider.ainvoke(LLMRequest(model='small', input=(LLMMessage(role='user', content=(TextContent(text='Hello'),)),)))
        self.assertTrue(records)
        self.assertNotIn('PRIVATE_TEST_VALUE', str(records))
        self.assertEqual(records[-1].status, 'failed')
        self.assertIn('Context exceeded', str(captured.exception))
        self.assertNotIn('PRIVATE_TEST_VALUE', str(captured.exception))
        await provider.aclose()

    async def test_transport_error_keeps_the_cause_but_redacts_credentials(self):
        """Provider diagnostics preserve upstream failures without copying connection secrets into Runtime errors."""
        from autoagent.agent_app.providers import _Provider
        from types import SimpleNamespace
        from autoagent.agent import LLMProviderError
        config = ApplicationProviderConfig(name='Test', api_key=CredentialReference(name='KEY'))
        credentials = SimpleNamespace(resolve=lambda reference:'PRIVATE_TEST_VALUE')
        upstream = SimpleNamespace(ainvoke=AsyncMock(side_effect=RuntimeError('Context exceeded; token=PRIVATE_TEST_VALUE')))
        provider = _Provider(config, credentials)
        with patch.object(_Provider, '_make_transport', return_value=upstream):
            with self.assertRaises(LLMProviderError) as captured:
                await provider.ainvoke(None)
        self.assertIn('Context exceeded', str(captured.exception))
        self.assertNotIn('PRIVATE_TEST_VALUE', str(captured.exception))

    async def test_structured_response_failure_keeps_its_contract(self):
        """Transport wrapping never turns a structured-output validation failure into a retryable Provider error."""
        from autoagent.agent_app.providers import _Provider
        from autoagent.agent import LLMStructuredOutputError
        from types import SimpleNamespace
        failure = LLMStructuredOutputError('Invalid JSON', kind='invalid_json', response=answer())
        provider = _Provider(ApplicationProviderConfig(name='Test'), SimpleNamespace())
        provider.client = SimpleNamespace(ainvoke=AsyncMock(side_effect=failure))
        with self.assertRaises(LLMStructuredOutputError) as captured:
            await provider.ainvoke(None)
        self.assertIs(captured.exception, failure)


class ProviderContracts:
    product = SessionControlContracts.product
    database_url = SessionControlContracts.database_url

    async def test_provider_configuration_is_private_and_credentials_never_enter_sql(self):
        """The app connection resolves its file reference at use and leaves Host defaults untouched."""
        upstream = ScriptedProvider(answer())
        upstream.aclose = AsyncMock()
        captured = []
        def factory(kind, config, **options):
            captured.append(config)
            return upstream
        async with self.product(ScriptedProvider()) as (host, client):
            credentials = host.agent_app.credentials
            credentials.path.parent.mkdir(parents=True, exist_ok=True)
            credentials.path.write_text("PRIVATE_KEY='PRIVATE_TEST_VALUE'\n")
            response = await client.post('/api/v2/agent/providers', json={'configuration':{
                'name':'Remote', 'base_url':'http://localhost:1234/v1', 'models':['small'],
                'api_key':{'source':'file','name':'PRIVATE_KEY'}}})
            self.assertEqual(response.status_code, 201, response.text)
            created = response.json()
            catalog = (await client.get('/api/v2/agent/catalog')).json()
            choice = next(item for item in catalog['provider_options'] if item['id'] == created['alias'])
            self.assertEqual(choice['name'], 'Remote')
            self.assertEqual(choice['models'], ['small'])
            self.assertFalse(host.agent_context.resources.has('provider', created['alias']))
            self.assertNotIn('PRIVATE_TEST_VALUE', json.dumps(await host.agent_app.storage.context('provider_records')))
            await host.agent_app.configure('provider-chat', ApplicationSessionSettings(model={'provider':created['alias'],'model':'small'}))
            from autoagent.agent_app.providers import _Provider
            with patch.object(_Provider, '_make_transport', side_effect=factory):
                ref = await host.agent_app.start_conversation('provider-chat','Hello',unbound=True)
                result = await host.join(ref)
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual(captured[0].api_key.get_secret_value(), 'PRIVATE_TEST_VALUE')
            self.assertNotIn('PRIVATE_TEST_VALUE', (await client.get('/api/v2/agent/providers')).text)
            conflict = await client.put('/api/v2/agent/providers/'+created['id'], json={'configuration':created['configuration'], 'expected_version':0})
            self.assertEqual(conflict.status_code, 409)
        upstream.aclose.assert_awaited_once()

    async def test_raw_secrets_are_rejected_without_echoing_them(self):
        """Neither provider keys nor MCP Header values are accepted as plaintext or reflected in errors."""
        async with self.product(ScriptedProvider()) as (host, client):
            response = await client.post('/api/v2/agent/providers', json={'configuration':{'name':'Bad','api_key':'PRIVATE_TEST_VALUE'}})
            self.assertEqual(response.status_code, 422)
            self.assertNotIn('PRIVATE_TEST_VALUE', response.text)
            response = await client.post('/api/v2/agent/resource-configs', json={'configuration':{'kind':'mcp','name':'Bad','enabled':False,
                'transport':{'type':'streamable_http','server_id':'bad','url':'https://example.test','headers':{'Authorization':'PRIVATE_TEST_VALUE'}}}})
            self.assertEqual(response.status_code, 422)
            self.assertNotIn('PRIVATE_TEST_VALUE', response.text)
            self.assertEqual(await host.agent_app.storage.context('resource_records'), [])

    async def test_missing_credential_reference_has_safe_failure(self):
        """A missing credential fails locally without exposing credentials or contacting a Provider."""
        async with self.product(ScriptedProvider()) as (host, client):
            created = await host.agent_app.save_provider(ApplicationProviderConfig(name='Missing',
                api_key=CredentialReference(name='AUTOAGENT_NONEXISTENT_SECRET')))
            await host.agent_app.configure('missing', ApplicationSessionSettings(model={'provider':created['alias'],'model':'small'}))
            ref = await host.agent_app.start_conversation('missing','Hello',unbound=True)
            result = await host.join(ref)
            self.assertEqual(result.status, 'failed')
            self.assertIn('Credential', str(result.error))

    async def test_mcp_connection_preview_lists_tools_without_saving_or_executing(self):
        """Testing an MCP configuration discovers definitions but never installs or invokes its Tools."""
        from types import SimpleNamespace
        from mcp_types import Tool as RemoteTool
        remote = SimpleNamespace(list_tools=AsyncMock(return_value=(RemoteTool(name='lookup',
            description='Read a record', input_schema={'type':'object'}),)), call_tool=AsyncMock())
        connection = MagicMock()
        connection.__aenter__.return_value = remote
        connection.__aexit__.return_value = None
        async with self.product(ScriptedProvider()) as (host, client):
            with patch('autoagent.agent.MCPClient', return_value=connection):
                response = await client.post('/api/v2/agent/mcp/test', json={'configuration':{
                    'kind':'mcp','name':'Preview','transport':{'type':'streamable_http','server_id':'preview','url':'https://example.test/mcp'}}})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()['tools'][0]['name'], 'lookup')
            self.assertEqual(await host.agent_app.storage.context('resource_records'), [])
            remote.call_tool.assert_not_awaited()

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
            self.assertEqual(await host.agent_app.storage.context('resource_records'), [])


class SQLiteProviderTests(ProviderContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL URL not configured')
class PostgreSQLProviderTests(ProviderContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self, root):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

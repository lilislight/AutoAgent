"""Shared Host JSON catalog, refresh, explicit embedding setup, and accounted calls."""
import asyncio
from contextlib import aclosing, asynccontextmanager
import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from openai import AsyncOpenAI

from autoagent import Node, Workflow, InputMappingContext
from autoagent.agent import LLM_CALL, EMBEDDING, embedding_operator, LLMRequest, EmbeddingRequest
from autoagent.host import AutoAgentHost, HostSettings
from autoagent.host.providers import CatalogProvider, HostProviders, ModelInfo
from tests.test_agent_providers import request, chat_body
from tests.test_agent_capabilities import DemoEmbedding
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase


class CatalogSchemaTests(unittest.TestCase):
    def test_only_three_prices_and_two_limits_are_configurable(self):
        """Unsupported billing fields and invalid prices cannot enter the project catalog."""
        for data in ({'cache_creation_input_token_cost':1}, {'input_cost_per_token':-1}, {'max_input_tokens':0}):
            with self.subTest(data=data), self.assertRaises(ValueError):
                ModelInfo.model_validate_json(json.dumps(data))


class CatalogRetirementTests(unittest.IsolatedAsyncioTestCase):
    async def test_reload_drains_old_stream_and_closes_only_retired_transports(self):
        """Reload publishes the new provider while the old stream drains safely."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def write(model_name):
                (root / "providers.json").write_text(json.dumps({"providers": {
                    "local": {"models": [{"id": "tiny", "name": model_name}]}}}))

            async def chunks(_):
                yield "first"
                yield "last"

            transport = SimpleNamespace(astream=chunks, aclose=AsyncMock())
            write("Before")
            providers = HostProviders(SimpleNamespace(root=root, environment={}))
            with patch.object(CatalogProvider, "_make_transport", return_value=transport):
                async with providers.lifespan(None):
                    previous = providers.get("local")
                    async with aclosing(previous.astream(request(model="tiny"))) as stream:
                        self.assertEqual(await anext(stream), "first")
                        write("After")
                        await providers._reload()
                        self.assertIsNot(providers.get("local"), previous)
                        with self.assertRaises(KeyError):
                            providers.registration("local", version=previous.version)
                        transport.aclose.assert_not_awaited()
                        self.assertEqual(await anext(stream), "last")
                    await asyncio.gather(*tuple(providers._draining))
                    transport.aclose.assert_awaited_once()
                    self.assertEqual(previous.clients, {})
                    self.assertEqual(len(providers.instances), 1)


class HostCatalogContracts:
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.url = await self.database_url()

    async def database_url(self):
        return 'sqlite+aiosqlite:///' + str(self.root / 'runtime.db')

    def write(self, *, backend='openai', rate='0.000001', **changes):
        provider = dict(backend=backend, api_key='${TEST_MODEL_KEY}', models=[dict(id='tiny', name='Tiny',
            reasoning_effort=dict(values=['low','high'], default='low'),
            model_info=dict(max_input_tokens=100000, max_output_tokens=4096, input_cost_per_token=rate,
                output_cost_per_token='0.000003', cache_read_input_token_cost=0))])
        provider.update(changes)
        (self.root / 'providers.json').write_text(json.dumps(dict(providers={'local':provider})))

    def host(self, workflows=(), **kwargs):
        from autoagent.host.persistence import PersistenceConfig
        return AutoAgentHost(root=self.root, workflows=workflows,
            settings=HostSettings(database_url=self.url, persistence=PersistenceConfig()),
            environment={'TEST_MODEL_KEY':'PRIVATE_MODEL_KEY'}, **kwargs)

    async def test_catalog_is_shared_without_enabling_agent_app_and_keys_are_private(self):
        """Host-only projects expose model metadata and resolve environment keys without returning secrets."""
        self.write()
        async with self.host() as host:
            result = await host.providers.describe()
            self.assertNotIn("agent", host.applications)
            self.assertNotIn('PRIVATE_MODEL_KEY', json.dumps(result))
            model = result['items'][0]['models'][0]
            self.assertEqual(model['model_info']['max_input_tokens'], 100000)
            self.assertEqual(model['model_info']['max_output_tokens'], 4096)
            self.assertEqual(host.providers.get('local').config.api_key.get_secret_value(), 'PRIVATE_MODEL_KEY')

    async def test_reload_is_atomic_and_replaces_only_valid_complete_configuration(self):
        """Invalid reloads preserve usable connections, while disable and removal take effect on successful reload."""
        self.write()
        async with self.host() as host:
            before = await host.providers.describe()
            (self.root / 'providers.json').write_text('{"api_key":"PRIVATE_MODEL_KEY",')
            with self.assertRaises(ValueError) as caught:
                await host.providers.reload()
            self.assertNotIn('PRIVATE_MODEL_KEY', str(caught.exception))
            self.assertEqual((await host.providers.describe())['version'], before['version'])
            self.write(enabled=False)
            await host.providers.reload()
            with self.assertRaises(ValueError):
                host.providers.get('local')
            (self.root / 'providers.json').write_text('{"providers":{}}')
            await host.providers.reload()
            self.assertEqual((await host.providers.describe())['items'], [])

    async def test_reload_preserves_unchanged_provider_instances_and_versions(self):
        """Editing another connection does not replace an unchanged client or invalidate its history affinity."""
        self.write()
        async with self.host() as host:
            original = host.providers.get('local')
            configuration = json.loads((self.root / 'providers.json').read_text())
            configuration['providers']['other'] = {'enabled':False, 'models':[{'id':'other-model'}]}
            (self.root / 'providers.json').write_text(json.dumps(configuration))
            await host.providers.reload()
            self.assertIs(host.providers.get('local'), original)
            self.assertEqual(host.providers.registration('local').version, original.version)
            count = len(host.providers.instances)
            await host.providers.reload()
            self.assertEqual(len(host.providers.instances), count)

    async def test_litellm_metadata_is_local_and_json_overrides_only_selected_fields(self):
        """LiteLLM fills missing metadata offline; native connections use only JSON prices."""
        self.write(backend='litellm')
        catalog = {'tiny':dict(max_input_tokens=5, max_output_tokens=7, input_cost_per_token='9',
            output_cost_per_token='8', cache_read_input_token_cost='6', cache_creation_input_token_cost='99')}
        with patch('autoagent.host.providers.load_litellm_catalog', return_value=(catalog, 'local')), \
                patch('autoagent.agent.providers._litellm_sdk.load_litellm_sdk'):
            async with self.host() as host:
                info = (await host.providers.describe())['items'][0]['models'][0]['model_info']
                self.assertEqual(info['input_cost_per_token'], '0.000001')
                self.assertEqual(info['max_output_tokens'], 4096)
                self.assertNotIn('cache_creation_input_token_cost', info)
                self.write()
                await host.providers.reload()
                self.assertEqual((await host.providers.describe())['items'][0]['backend'], 'openai')

    async def test_enabled_litellm_is_initialized_before_catalog_use(self):
        """Host startup prepares enabled SDKs before the project can accept model requests."""
        self.write(backend='litellm')
        with patch('autoagent.host.providers.load_litellm_catalog', return_value=({}, 'local')), \
                patch('autoagent.agent.providers._litellm_sdk.load_litellm_sdk') as prepare:
            async with self.host() as host:
                prepare.assert_called_once_with()
                self.assertEqual(host.providers.get('local').config.backend, 'litellm')

    async def test_disabled_litellm_does_not_initialize_optional_sdk(self):
        """Disabled connections and models do not require loading the optional LiteLLM SDK."""
        with patch('autoagent.host.providers.load_litellm_catalog', return_value=({}, 'local')), \
                patch('autoagent.agent.providers._litellm_sdk.load_litellm_sdk') as prepare:
            self.write(backend='litellm', enabled=False)
            async with self.host() as host:
                prepare.assert_not_called()
                self.write(backend='litellm', models=[{'id':'tiny', 'enabled':False}])
                await host.providers.reload()
                prepare.assert_not_called()

    async def test_sdk_initialization_failure_preserves_published_catalog(self):
        """Reload cannot publish a LiteLLM connection until its SDK is successfully initialized."""
        self.write()
        async with self.host() as host:
            original = host.providers.get('local')
            before = await host.providers.describe()
            self.write(backend='litellm')
            with patch('autoagent.host.providers.load_litellm_catalog', return_value=({}, 'local')), \
                    patch('autoagent.agent.providers._litellm_sdk.load_litellm_sdk') as prepare:
                prepare.side_effect = ImportError('SDK initialization failed')
                with self.assertRaisesRegex(ImportError, 'SDK initialization failed'):
                    await host.providers.reload()
                self.assertIs(host.providers.get('local'), original)
                self.assertEqual((await host.providers.describe())['version'], before['version'])
                prepare.side_effect = None
                await host.providers.reload()
                self.assertEqual(host.providers.get('local').config.backend, 'litellm')

    async def test_custom_workflow_calls_are_recorded_once_and_prices_are_pinned_across_reload(self):
        """A refresh during a real SDK request cannot reprice its usage or duplicate its call record."""
        self.write()
        entered, release = threading.Event(), threading.Event()
        async def respond(http_request):
            entered.set()
            await asyncio.to_thread(release.wait, 10)
            body = chat_body()
            body['model'] = 'tiny'
            return httpx.Response(200, json=body)
        original = CatalogProvider._make_transport
        clients = []
        def factory(owner, kind, config, **kwargs):
            client = AsyncOpenAI(api_key=config.api_key.get_secret_value(), http_client=httpx.AsyncClient(transport=httpx.MockTransport(respond)))
            clients.append(client)
            return original(owner, kind, config, client=client, **kwargs)
        def model_input(ctx: InputMappingContext) -> LLMRequest:
            return request(model='tiny', routing={'provider':'local'})
        workflow = Workflow('custom', nodes=[Node('call', LLM_CALL, input_mapping=model_input)])
        with patch.object(CatalogProvider, '_make_transport', factory):
            async with self.host([workflow]) as host:
                ref = await host.execute('custom', {})
                self.assertTrue(await asyncio.to_thread(entered.wait, 10))
                self.write(rate='1')
                await host.providers.reload()
                release.set()
                result = await host.join(ref)
                self.assertEqual(result.status, 'completed', result.error)
                usage = await host.token_usage(invocation_id=ref.invocation_id)
                self.assertEqual(usage.call_count, 1)
                self.assertEqual(usage.cost_usd, '0.000009')
        for client in clients:
            await client.close()

    async def test_json_default_routes_generic_calls_and_preserves_default_effort(self):
        """A normal Workflow with no request model uses the Host JSON selection, including its effort."""
        from unittest.mock import AsyncMock
        from autoagent.agent import LLMResponse, LLMMessage, TextContent
        from autoagent.host.providers import CatalogProvider
        self.write()
        path = self.root / 'providers.json'
        config = json.loads(path.read_text())
        config['default_model'] = {'provider':'local','model':'tiny','reasoning_effort':'high'}
        path.write_text(json.dumps(config))
        def llm_input(context: InputMappingContext) -> LLMRequest:
            return LLMRequest(input=(LLMMessage(role='user', content=(TextContent(text='Hello'),)),))
        workflow = Workflow('default-model', nodes=[Node('llm', LLM_CALL, input_mapping=llm_input)])
        response = LLMResponse(model='tiny', finish_reason='stop', output=(LLMMessage(role='assistant', content=(TextContent(text='Done'),)),))
        provider = AsyncMock(return_value=response)
        with patch.object(CatalogProvider, 'ainvoke', provider):
            async with self.host([workflow]) as host:
                result = await host.join(await host.execute('default-model', {}))
                self.assertEqual(result.status, 'completed', result.error)
        self.assertEqual(provider.await_args.args[0].model, 'tiny')
        self.assertEqual(provider.await_args.args[0].reasoning_effort, 'high')

    async def test_embedding_has_only_explicit_code_registration(self):
        """Embedding remains available through code even when legacy environment configuration is absent."""
        provider = DemoEmbedding()
        @asynccontextmanager
        async def lifespan(setup):
            setup.register_operator(embedding_operator(provider), capability_id=EMBEDDING.id, default=True)
            yield
        def model_input(ctx: InputMappingContext) -> EmbeddingRequest:
            return EmbeddingRequest(model='vectors', input='hello')
        workflow = Workflow('vectors', nodes=[Node('embed', EMBEDDING, input_mapping=model_input)])
        async with self.host([workflow], lifespan=lifespan) as host:
            result = await host.join(await host.execute('vectors', {}))
            self.assertEqual(result.status, 'completed', result.error)


class SQLiteHostCatalogTests(HostCatalogContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL URL not configured')
class PostgreSQLHostCatalogTests(HostCatalogContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

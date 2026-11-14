"""SDK execution never depends on Agent application configuration or tables."""

from contextlib import asynccontextmanager
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from sqlalchemy import inspect, select

from autoagent import Node, Operator, Workflow
from autoagent.agent import AgentInput, AgentResources, Tool, agent_workflow
from autoagent.agent_app import BUILTIN_AGENT_WORKFLOW_ID, builtin_agent_workflow
from autoagent.host import AutoAgentHost, HostSettings
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_application import echo
from tests.test_agent_workflow import ScriptedProvider, answer, settings


class ApplicationIsolationContracts:
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.url = await self.database_url()

    async def database_url(self):
        return 'sqlite+aiosqlite:///' + str(self.root / 'runtime.db')

    async def product_tables(self, host):
        async def read():
            async with host._persistence._writer.storage.engine.connect() as connection:
                names = await connection.run_sync(lambda sync: inspect(sync).get_table_names())
                return [name for name in names if name.startswith('agent_')]
        return await host._persistence._writer.run(read)

    async def test_sdk_and_ordinary_workflows_do_not_initialize_product_storage(self):
        """An SDK Agent uses explicit runtime defaults and creates no Agent product tables."""
        provider = ScriptedProvider(answer())

        @asynccontextmanager
        async def lifespan(setup):
            setup.providers.register('offline', provider)
            yield

        custom = agent_workflow(workflow_id='custom', runtime=settings(model='explicit-model'),  )
        ordinary = Workflow('ordinary', nodes=[Node('echo', Operator(echo))])
        async with AutoAgentHost(root=self.root, workflows=[custom, ordinary], lifespan=lifespan,
            settings=HostSettings(database_url=self.url), environment={}) as host:
            self.assertNotIn("agent", host.applications)
            result = await host.join(await host.execute('custom', AgentInput(message='Hello')))
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual(provider.requests[0].model, 'explicit-model')
            self.assertEqual((await host.join(await host.execute('ordinary', {'text':'Hello'}))).output, {'text':'Hello'})
            self.assertEqual(await self.product_tables(host), [])

    async def test_product_preferences_and_controls_do_not_change_custom_workflow(self):
        """Product model selection and Tool disabling stay private in a mixed Host."""
        provider = ScriptedProvider(answer(), answer())

        def shared(text: str) -> str:
            return text

        @asynccontextmanager
        async def lifespan(setup):
            setup.providers.register('offline', provider)
            setup.extensions["agent"].context.set_defaults(settings(model='sdk-model'))
            setup.applications["agent"].context.resources.register_tool(Tool(shared))
            yield

        workflows = [builtin_agent_workflow(execution=False),
            agent_workflow(workflow_id='custom', runtime=settings(model='sdk-model'), resources=AgentResources(tools=[shared]))]
        async with AutoAgentHost(root=self.root, workflows=workflows, lifespan=lifespan,
            settings=HostSettings(database_url=self.url), environment={}) as host:
            from autoagent.agent_app import ResourceControl
            app = host.applications["agent"].context
            await app.configure('chat', settings(model='product-model').session_defaults())
            entry = app.resources.get('tool', 'shared')
            await app.shared_resources.save(ResourceControl(name='Hide shared', resource_id=entry.resource_id, enabled=False))
            custom = await host.join(await host.execute('custom', AgentInput(message='SDK'), session_id='custom-session'))
            product = await host.join(await app.start_conversation('chat', 'Product', unbound=True))
            self.assertEqual(custom.status, 'completed', custom.error)
            self.assertEqual(product.status, 'completed', product.error)
            self.assertEqual([request.model for request in provider.requests], ['sdk-model', 'product-model'])
            self.assertIn('shared', {tool.name for tool in provider.requests[0].tools})
            self.assertNotIn('shared', {tool.name for tool in provider.requests[1].tools})
            self.assertIsNone(await app.storage('snapshot', custom.invocation_id))

    async def test_disabled_product_does_not_validate_unused_product_configuration(self):
        """Invalid product edit flags cannot break a custom-only Host."""
        workflow = Workflow('ordinary', nodes=[Node('echo', Operator(echo))])
        async with AutoAgentHost(root=self.root, workflows=[workflow],
            settings=HostSettings(database_url=self.url), environment={
                'AUTOAGENT_AGENT_EDIT_CODE_PERMISSIONS': 'unused-invalid-value',
            }) as host:
            result = await host.join(await host.execute('ordinary', {'text': 'SDK only'}))
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual(result.output, {'text': 'SDK only'})
            self.assertEqual(await self.product_tables(host), [])


class SQLiteApplicationIsolationTests(ApplicationIsolationContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL test database is not configured')
class PostgreSQLApplicationIsolationTests(ApplicationIsolationContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

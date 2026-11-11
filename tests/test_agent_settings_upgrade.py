"""One-time settings conversion preserves conversations and accepted model identity."""

from contextlib import asynccontextmanager
from pathlib import Path
import tempfile
import unittest

from sqlalchemy import Column, BigInteger, Integer, JSON, MetaData, String, Table, insert, inspect, select
from sqlalchemy.ext.asyncio import create_async_engine

from autoagent.agent_app import builtin_agent_workflow
from autoagent.agent_app.persistence import AgentApplicationStorage
from autoagent.agent_app import ResourceControl
from autoagent.agent import AgentHostSettings, AgentModelSettings
from autoagent.host import AutoAgentHost, HostSettings
from autoagent.host.persistence import DatabaseSink
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_agent_workflow import ScriptedProvider, answer


class SettingsUpgradeContracts:
    async def database_url(self, root):
        return 'sqlite+aiosqlite:///' + str(root / 'runtime.db')

    async def test_previous_product_tables_move_without_copying_sdk_ownership(self):
        """Renaming retains chats, queued submissions and Core history while excluding old SDK settings."""
        from sqlalchemy import text
        from autoagent.agent import AgentInput, agent_workflow
        from autoagent.agent_app.persistence.context import sessions, invocation_bindings, context_meta
        from autoagent.agent_app.persistence.queue import messages
        from autoagent.host.persistence.schema import meta
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            url = await self.database_url(root)
            @asynccontextmanager
            async def lifespan(setup):
                setup.agent_context.resources.register_provider('offline', ScriptedProvider(answer(), answer()))
                setup.agent_context.set_defaults(AgentHostSettings(model=AgentModelSettings(provider='offline', model='test')))
                yield
            options = dict(root=root, settings=HostSettings(database_url=url), environment={}, lifespan=lifespan,
                workflows=[builtin_agent_workflow(execution=False), agent_workflow(workflow_id='custom', files=False, execution=False)])
            async with AutoAgentHost(**options) as host:
                product = await host.agent_app.context.start_conversation('chat', 'Preserved title', unbound=True)
                self.assertEqual((await host.join(product)).status, 'completed')
                custom = await host.execute('custom', AgentInput(message='SDK'), session_id='custom')
                self.assertEqual((await host.join(custom)).status, 'completed')
                await host.agent_app.context.messages.close()
                await host.agent_app.storage.queue('add', 'chat', 'saved', 'Later task')
                await host.agent_app.storage.queue('change', 'chat', 'saved', ('queued',), state='cancelled')
                await host._persistence.flush()
                before = [(row['event_id'], row['sequence']) for row in (await host.list_execution_events(product.invocation_id)).items]
            engine = create_async_engine(url)
            try:
                async with engine.begin() as conn:
                    session = dict((await conn.execute(select(sessions).where(sessions.c.session_id == 'chat'))).mappings().one())
                    await conn.execute(insert(sessions).values(**{**session, 'session_id':'custom'}))
                    binding = dict((await conn.execute(select(invocation_bindings))).mappings().one())
                    event_id = await conn.scalar(select(meta.tables['runtime_events'].c.event_id).where(
                        meta.tables['runtime_events'].c.invocation_id == custom.invocation_id,
                        meta.tables['runtime_events'].c.event_name == 'invocation.started'))
                    await conn.execute(insert(invocation_bindings).values(**{**binding, 'binding_id':'legacy-sdk',
                        'session_id':'custom', 'invocation_id':custom.invocation_id, 'activation_id':event_id}))
                    for table in (*context_meta.tables.values(), messages):
                        await conn.execute(text(f'ALTER TABLE "{table.name}" RENAME TO "{table.name.replace("agent_app_", "agent_", 1)}"'))
            finally:
                await engine.dispose()
            async with AutoAgentHost(**options) as host:
                self.assertEqual((await host.agent_app.context.get_settings('chat')).title, 'Preserved title')
                self.assertEqual((await host.agent_app.context.get_settings('custom')).version, 0)
                self.assertIsNone(await host.agent_app.storage.context('snapshot', custom.invocation_id))
                self.assertEqual((await host.agent_app.storage.queue('rows', 'chat', item_id='saved'))[0]['state'], 'cancelled')
                after = [(row['event_id'], row['sequence']) for row in (await host.list_execution_events(product.invocation_id)).items]
                self.assertEqual(after, before)
                self.assertEqual((await host.get_execution(custom)).status, 'completed')

    async def test_purge_removes_agent_metadata_and_core_session_together(self):
        """Purging a stopped conversation deletes its preferences and Core facts while retaining shared resources."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            url = await self.database_url(root)
            @asynccontextmanager
            async def lifespan(setup):
                setup.agent_context.resources.register_provider('offline',ScriptedProvider(answer()))
                setup.agent_context.set_defaults(AgentHostSettings(model=AgentModelSettings(provider='offline',model='test')))
                yield
            async with AutoAgentHost(root=root,workflows=[builtin_agent_workflow(execution=False)],
                settings=HostSettings(database_url=url),lifespan=lifespan,environment={}) as host:
                ref = await host.agent_app.context.start_conversation('purged','Original title',unbound=True)
                self.assertEqual((await host.join(ref)).status,'completed')
                await host.agent_app.context.shared_resources.save(ResourceControl(name='Saved resource',resource_id='host/tool/future'))
            sink = await DatabaseSink.open(url)
            try:
                await AgentApplicationStorage(sink).context("prepare")
                await sink.purge_root_graph(ref.invocation_id)
                self.assertIsNone(await sink.get_session('purged'))
                self.assertIsNone(await sink.get_invocation(ref.invocation_id))
                record = await AgentApplicationStorage(sink).context('settings','purged',AgentHostSettings())
                self.assertEqual(record.version,0)
                self.assertEqual(len(await AgentApplicationStorage(sink).context('resource_records')),1)
            finally:
                await sink.close()

    async def test_full_settings_are_converted_once_without_losing_history_or_queue(self):
        """Obsolete tables convert to small preferences and immutable model bindings, retaining titles and queued messages."""
        from autoagent.agent_app.persistence.context import sessions, invocation_bindings
        from autoagent.agent_app.persistence.queue import messages
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            url = await self.database_url(root)
            provider = ScriptedProvider(answer(),answer())
            @asynccontextmanager
            async def lifespan(setup):
                setup.agent_context.resources.register_provider('offline',provider)
                setup.agent_context.set_defaults(AgentHostSettings(model=AgentModelSettings(provider='offline',model='startup')))
                yield
            options = dict(root=root,workflows=[builtin_agent_workflow(execution=False)],
                lifespan=lifespan,settings=HostSettings(database_url=url),environment={})
            async with AutoAgentHost(**options) as host:
                ref = await host.agent_app.context.start_conversation('chat','Original first message',unbound=True)
                self.assertEqual((await host.join(ref)).status,'completed')
            engine = create_async_engine(url)
            try:
                async with engine.begin() as conn:
                    binding = dict((await conn.execute(select(invocation_bindings))).mappings().one())
                    await conn.run_sync(sessions.drop)
                    await conn.run_sync(invocation_bindings.drop)
                    metadata = MetaData()
                    old_sessions = Table('agent_session_settings',metadata,
                        Column('session_id',String,primary_key=True),Column('version',Integer),
                        Column('settings',JSON),Column('updated_at_us',Integer))
                    old_executions = Table('agent_execution_settings',metadata,
                        Column('snapshot_id',String,primary_key=True),Column('session_id',String),Column('invocation_id',String),
                        Column('epoch',Integer),Column('settings_version',Integer),Column('settings',JSON),Column('bindings',JSON),
                        Column('reason',String),Column('activation_id',String),Column('created_at_us',BigInteger))
                    old_queue = Table('agent_message_queue',metadata,
                        *(Column(column.name,column.type,primary_key=column.primary_key,nullable=column.nullable)
                            for column in messages.columns if column.name != 'model'))
                    await conn.run_sync(metadata.create_all)
                    full = AgentHostSettings(model=AgentModelSettings(provider='offline',model='chosen',max_output_tokens=999)).model_dump(mode='json')
                    await conn.execute(insert(old_sessions).values(session_id='chat',version=7,settings=full,updated_at_us=1))
                    full['model']['model'] = 'startup'
                    await conn.execute(insert(old_executions).values(snapshot_id=binding['binding_id'],session_id='chat',
                        invocation_id=ref.invocation_id,epoch=1,settings_version=0,settings=full,bindings=binding['bindings'],
                        reason='start',activation_id=binding['activation_id'],created_at_us=binding['created_at_us']))
                    await conn.execute(insert(old_queue).values(id='pending',session_id='chat',position=1,message='Queued message',
                        state='blocked',created_at_us=2))
                async with AutoAgentHost(**options) as host:
                    record = await host.agent_app.context.get_settings('chat')
                    self.assertEqual(record.version,7)
                    self.assertEqual(record.settings.model.model,'chosen')
                    self.assertEqual(record.title,'Original first message')
                    self.assertEqual(set(record.settings.model_dump()),{'model','approval_mode'})
                    snapshot = await host.agent_app.context.get_snapshot(ref.invocation_id)
                    self.assertEqual(snapshot.settings.model.model,'startup')
                    self.assertEqual(snapshot.snapshot_id,binding['binding_id'])
                    queue = await host.agent_app.context.messages.storage('rows','chat',item_id='pending')
                    self.assertEqual(queue[0]['model']['model'],'chosen')
                    self.assertEqual((await host.get_execution(ref.invocation_id)).status,'completed')
                async with engine.connect() as conn:
                    tables = await conn.run_sync(lambda sync: set(inspect(sync).get_table_names()))
                    self.assertNotIn('agent_session_settings',tables)
                    self.assertNotIn('agent_execution_settings',tables)
            finally:
                await engine.dispose()


class SQLiteSettingsUpgradeTests(SettingsUpgradeContracts,unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL,'PostgreSQL URL not configured')
class PostgreSQLSettingsUpgradeTests(SettingsUpgradeContracts,unittest.IsolatedAsyncioTestCase):
    async def database_url(self, root):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()

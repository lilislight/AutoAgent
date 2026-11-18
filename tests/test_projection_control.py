"""Read-model loss cannot affect control, facts, value reads or snapshot cuts."""
import asyncio
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from sqlalchemy import event as sql_event, inspect, select

from autoagent import Edge, Node, Operator, Wait, Workflow
from autoagent.core.app import AutoAgentApp
from autoagent.host.persistence import DatabaseSink, PersistenceConfig
from autoagent.host.persistence.schema import DETAIL_TABLES, meta
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_host_persistence import PersistenceContracts, Value, identity


class ProjectionCacheTests(unittest.IsolatedAsyncioTestCase):
    async def test_expiration_preserves_refreshed_entries_and_byte_accounting(self):
        """An LRU hit extends its TTL while older entries expire in access order."""
        from autoagent.host.persistence import read_models
        cache = read_models.ProjectionCache(SimpleNamespace(
            config=PersistenceConfig(projection_cache_ttl_seconds=10),
            storage=SimpleNamespace(source=None, workflow_graph=None)))
        batch = SimpleNamespace(rows={name: {} for name in read_models.PROJECTION_TABLES})
        now = 0
        try:
            with patch.object(read_models, 'time', SimpleNamespace(monotonic=lambda: now)):
                cache._save(('store', 'first'), batch, 10)
                now = 1
                cache._save(('store', 'second'), batch, 10)
                now = 2
                cache._save(('store', 'third'), batch, 10)
                now = 5
                await cache.build(None, 'first', 10, 'store')
                now = 11
                cache.expire()
                self.assertEqual(list(cache.entries), [('store', 'third'), ('store', 'first')])
                self.assertEqual(cache.evictions, 1)
                self.assertEqual(cache.bytes, sum(entry['size'] for entry in cache.entries.values()))
                now = 15
                cache.expire()
                self.assertEqual(cache.bytes, 0)
                self.assertEqual(cache.evictions, 3)
        finally:
            await cache.close()


class ProjectionContracts:
    async def asyncSetUp(self):
        self.directory=tempfile.TemporaryDirectory()
        self.apps=[];self.sinks=[]
        self.url=await self.database_url()

    async def asyncTearDown(self):
        for app in self.apps: app.close()
        for sink in self.sinks: await sink.close()
        await self.remove_database()
        self.directory.cleanup()

    sink=PersistenceContracts.sink
    workflow=PersistenceContracts.workflow
    registered=PersistenceContracts.registered

    async def test_artifact_reference_chunks_bind_values_and_restore_exact_events(self):
        """Reference batches cross 128 rows without interpolating or losing values."""
        from autoagent.core.runtime import RuntimeEvent
        from autoagent.core.runtime.events import SessionOpened
        sink = await self.sink(mode='buffered', batch_count=1024, batch_delay_ms=60000)
        value = "quotes' ; -- 中文 " + 'x' * 16384
        events = [RuntimeEvent(f'reference-{index}', 1, SessionOpened({'blob': value})) for index in range(257)]
        for item in events:
            await sink.append(item)
        statements = []

        def collect(conn, cursor, statement, parameters, context, executemany):
            if statement.startswith('INSERT INTO artifact_refs'):
                self.assertNotIn('中文', statement)
                self.assertLessEqual(len(parameters), 640)
                statements.append(statement)

        engine = sink._writer.storage.engine.sync_engine
        sql_event.listen(engine, 'before_cursor_execute', collect)
        try:
            await sink.flush()
        finally:
            sql_event.remove(engine, 'before_cursor_execute', collect)
        self.assertEqual(len(statements), 3)
        for item in (events[0], events[127], events[128], events[-1]):
            restored = await sink.get_runtime_event(item.id)
            self.assertEqual(restored.to_record(), item.to_record())

    async def test_batch_local_active_work_never_issues_sql_deletes(self):
        """Active Nodes created and ended in one buffered batch leave no SQL work."""
        sink = await self.sink(mode='buffered', batch_delay_ms=60000)
        app, wf, _ = await self.registered(sink)
        result = await app.ainvoke(wf, {'blob': 'local-active-work'})
        statements = []

        def collect(conn, cursor, statement, parameters, context, executemany):
            if 'active_nodes' in statement and statement.lstrip().upper().startswith(('INSERT', 'DELETE')):
                statements.append(statement)

        engine = sink._writer.storage.engine.sync_engine
        sql_event.listen(engine, 'before_cursor_execute', collect)
        try:
            await sink.flush()
        finally:
            sql_event.remove(engine, 'before_cursor_execute', collect)
        self.assertEqual(statements, [])
        self.assertTrue((await sink.execution_state(result.invocation_id))['settled'])
        self.assertEqual(len((await sink.list_nodes(result.invocation_id)).items), 2)

    async def test_sparse_execution_fields_preserve_running_node_phase(self):
        """Node inspection reflects the executing phase from a field-level delta."""
        entered, release = threading.Event(), threading.Event()

        async def gated(value: Value) -> Value:
            entered.set()
            while not release.is_set():
                await asyncio.sleep(.005)
            return value

        sink = await self.sink()
        app, wf, _ = await self.registered(sink, handler=gated)
        invocation = asyncio.create_task(app.ainvoke(wf, {'blob': 'phase'}))
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait, 2))
            async def identity():
                async with sink._queries.view() as (conn, _, _, _):
                    return await conn.scalar(select(meta.tables['invocations'].c.invocation_id))
            iid = await sink._writer.run(identity)
            detail = await sink.node_detail(iid, 'a@root')
            self.assertEqual(detail['phase'], 'executing')
        finally:
            release.set()
            await invocation

    async def test_projection_batches_artifact_references_per_event_page(self):
        """Cold multi-page views batch references without hydrating business Artifacts."""
        sink = await self.sink(artifact_threshold_bytes=8192)
        operator = Operator(identity, id='echo')
        names = [f'node-{index}' for index in range(20)]
        wf = Workflow('paged-projection', nodes=[Node(name, operator) for name in names],
                      edges=[Edge(left, right) for left, right in zip(names, names[1:])])
        app = AutoAgentApp(runtime_event_sink=sink)
        self.apps.append(app)
        app.register_workflow(wf)
        await sink.register_operator(operator)
        await sink.register_workflow(app.workflow_definition_snapshot(wf.id))
        value = {'blob': 'x' * 16384}
        result = await app.ainvoke(wf, value)
        cache = sink._queries.projection_cache
        storage = sink._writer.storage
        reference_queries = []

        def collect(conn, cursor, statement, parameters, context, executemany):
            values = parameters.values() if isinstance(parameters, dict) else parameters
            if (statement.lstrip().upper().startswith('SELECT') and 'FROM artifact_refs' in statement
                    and 'runtime_event' in values):
                reference_queries.append(statement)

        async def check():
            cache.clear()
            before = cache.built_events
            engine = storage.query_engine.sync_engine
            sql_event.listen(engine, 'before_cursor_execute', collect)
            original = storage.row

            async def metadata_only(conn, name, **identity):
                self.assertNotEqual(name, 'artifacts', 'Projection hydrated a business value.')
                return await original(conn, name, **identity)

            try:
                with patch.object(storage, 'row', side_effect=metadata_only):
                    nodes = await sink.list_nodes(result.invocation_id)
            finally:
                sql_event.remove(engine, 'before_cursor_execute', collect)
            count = cache.built_events - before
            self.assertGreater(count, 64)
            self.assertEqual(len(nodes.items), len(names))
            self.assertEqual(len(reference_queries), (count + 63) // 64)

        await sink._writer.run(check)
        detail = await sink.node_detail(result.invocation_id, f'{names[-1]}@root')
        self.assertEqual(detail['output']['value'], value)

    async def test_cold_projection_reuses_prefetched_event_rows(self):
        """Rebuilding a view never fetches batch-selected Runtime Events by ID again."""
        sink = await self.sink()
        app, wf, _ = await self.registered(sink)
        result = await app.ainvoke(wf, {'blob': 'prefetched-metadata'})
        expected = (await sink.list_nodes(result.invocation_id)).items
        sink._queries.projection_cache.clear()
        storage = sink._writer.storage
        original = storage.row
        event_reads = []

        async def track(conn, name, **identity):
            if name == 'runtime_events':
                event_reads.append(identity)
            return await original(conn, name, **identity)

        with patch.object(storage, 'row', side_effect=track):
            actual = (await sink.list_nodes(result.invocation_id)).items
        self.assertEqual(actual, expected)
        self.assertEqual(len(event_reads), 0, 'Projection reread prefetched Event bodies.')

    async def test_metadata_without_prefetch_loads_event_once(self):
        """A metadata read shares one stored Event row between its header and body."""
        from autoagent.host.persistence.projector_source import metadata_record
        sink = await self.sink()
        app, wf, _ = await self.registered(sink)
        result = await app.ainvoke(wf, {'blob': 'single-metadata-read'})
        storage = sink._writer.storage

        async def check():
            async with sink._queries.view(invocation_id=result.invocation_id) as (conn, _, _, _):
                events = meta.tables['runtime_events']
                row = (await conn.execute(select(events).where(
                    events.c.invocation_id == result.invocation_id).order_by(events.c.sequence).limit(1))).mappings().one()
                expected = await metadata_record(storage, conn, 'runtime_event', row['event_id'], row=row)
                with patch.object(storage, 'row', wraps=storage.row) as lookup:
                    actual = await metadata_record(storage, conn, 'runtime_event', row['event_id'])
                self.assertEqual(actual, expected)
                self.assertEqual(lookup.await_count, 1)

        await sink._writer.run(check)

    async def test_accepted_scope_skips_other_roots_without_serializing_business_values(self):
        """A pending Root read folds its own family without materializing either Root's bodies."""
        from autoagent.core.runtime.events import RuntimeEvent
        from autoagent.host.persistence import queries
        sink=await self.sink(mode='buffered',batch_delay_ms=60000)
        app,wf,_=await self.registered(sink)
        first=await app.ainvoke(wf,{'blob':'first-'+'x'*16384})
        second=await app.ainvoke(wf,{'blob':'second-'+'y'*16384})
        original=queries.pending_metadata
        selected=[]
        def track(entries):
            selected.extend(entry.value for entry in entries)
            return original(entries)
        with patch.object(queries,'pending_metadata',side_effect=track),patch.object(
            RuntimeEvent,'to_record',side_effect=AssertionError('Observation serialized a body')):
            state=await sink.execution_state(first.invocation_id)
        self.assertTrue(state['settled'])
        self.assertTrue(selected)
        self.assertTrue(all(event.session_id==first.session_id for event in selected))
        self.assertFalse(any(event.invocation_id==second.invocation_id for event in selected))

    async def test_common_detail_pages_do_not_send_projections_back_to_sql(self):
        """Warm Node/Call/Wait lookups and filtered pages read the memory model directly."""
        from sqlalchemy.sql import visitors
        from sqlalchemy.sql.schema import Table
        from autoagent.host.persistence.read_models import ProjectionConnection
        sink=await self.sink()
        app,wf,_=await self.registered(sink)
        result=await app.ainvoke(wf,{'blob':'pages'})
        nodes=await sink.list_nodes(result.invocation_id)
        original=ProjectionConnection._statement
        async def facts_only(connection,statement):
            self.assertFalse(any(isinstance(item,Table) and item.name in DETAIL_TABLES
                                 for item in visitors.iterate(statement)))
            return await original(connection,statement)
        with patch.object(ProjectionConnection,'_statement',facts_only):
            self.assertEqual((await sink.list_nodes(result.invocation_id)).items,nodes.items)
            calls=await sink.list_calls(result.invocation_id)
            await sink.list_waits(result.invocation_id)
            await sink.node_detail(result.invocation_id,nodes.items[0]['occurrence_id'])
            await sink.call_detail(result.invocation_id,calls.items[0]['call_id'])
            page=await sink.trace_page('node',result.invocation_id,limit=1,order='asc')
            self.assertEqual(len(page.items),1)
            self.assertIsNotNone(page.next_cursor)
            following=await sink.trace_page('node',result.invocation_id,limit=1,order='asc',cursor=page.next_cursor)
            self.assertNotEqual(page.items[0]['occurrence_id'],following.items[0]['occurrence_id'])

    async def test_committed_artifact_hashes_skip_repeat_reads_and_gc_invalidates_them(self):
        """A bounded committed hash cache avoids repeat lookups and cannot survive Artifact deletion."""
        from sqlalchemy import event
        sink=await self.sink()
        app,wf,_=await self.registered(sink)
        value={'blob':'x'*16384}
        first=await app.ainvoke(wf,value)
        lookups=[]
        engine=sink._writer.storage.engine.sync_engine
        def collect(conn,cursor,statement,parameters,context,executemany):
            if statement.lstrip().upper().startswith('SELECT') and 'FROM artifacts' in statement:
                lookups.append(statement)
        event.listen(engine,'before_cursor_execute',collect)
        try:
            second=await app.ainvoke(wf,value)
            self.assertEqual(lookups,[])
        finally:event.remove(engine,'before_cursor_execute',collect)
        await sink.purge_root_graph(first.invocation_id)
        await sink.purge_root_graph(second.invocation_id)
        third=await app.ainvoke(wf,value)
        detail=await sink.node_detail(third.invocation_id,'a@root')
        self.assertEqual(detail['output']['value'],value)

    async def test_control_survives_a_completely_broken_projector(self):
        """Execution, Core observation, and external responses never call the UI projector."""
        sink=await self.sink()
        wf=Workflow('control-independent',nodes=[Node('approval',Wait(Value,Value))])
        app=AutoAgentApp(runtime_event_sink=sink);self.apps.append(app)
        app.register_workflow(wf)
        await sink.register_workflow(app.workflow_definition_snapshot(wf.id))
        with patch.object(sink._queries.projection_cache,'build',side_effect=AssertionError('UI unavailable')):
            result=await app.ainvoke(wf,{'blob':'request'},entry_node_id='approval')
            state=await sink.execution_state(result.invocation_id)
            self.assertTrue(state['at_boundary'])
            snapshot=await app.acontrol_snapshot(result.ref)
            self.assertTrue(snapshot.at_boundary)
            self.assertFalse(snapshot.settled)
            self.assertFalse(hasattr(snapshot,'output'))
            controls=await sink.control_metadata(result.invocation_id,(app.workflow_definition_snapshot(wf.id).workflow_revision_id,))
            self.assertEqual(controls['waits'][0]['node_id'],'approval')
            done=await app.aresume(result.ref,result.waits[0].id,{'blob':'approved'})
            self.assertEqual(done.output,{'blob':'approved'})
            self.assertTrue((await sink.execution_state(result.invocation_id))['settled'])
            self.assertTrue((await app.acontrol_snapshot(result.ref)).settled)
        async def rows():
            async with sink._writer.storage.engine.connect() as conn:
                for name in ('active_nodes','active_waits'):
                    self.assertEqual((await conn.execute(select(meta.tables[name]))).all(),[])
                self.assertTrue(set(DETAIL_TABLES).issubset(await conn.run_sync(lambda c:inspect(c).get_table_names())))
        await sink._writer.run(rows)

    async def test_cold_host_recovers_and_responds_without_any_ui_projector(self):
        """A cold Host restores from Checkpoint/Events and responds while projection building fails."""
        from autoagent.host import AutoAgentHost,HostSettings
        wf=Workflow('cold-control-independent',nodes=[Node('approval',Wait(Value,Value))])
        settings=HostSettings(database_url=self.url,memory_budget_bytes=2*1024*1024*1024)
        async with AutoAgentHost(workflows=[wf],settings=settings,root=self.directory.name) as host:
            with patch.object(host._persistence._queries.projection_cache,'build',side_effect=AssertionError('UI unavailable')):
                ref=await host.execute(wf.id,{'blob':'request'},entry_node_id='approval')
                waiting=await host.join(ref)
        async with AutoAgentHost(workflows=[wf],settings=settings,root=self.directory.name) as host:
            with patch.object(host._persistence._queries.projection_cache,'build',side_effect=AssertionError('UI unavailable')):
                controls=await host.get_execution_controls(ref.invocation_id)
                self.assertTrue(controls['actions']['recover']['available'])
                await host.recover(ref.invocation_id)
                await host.respond(ref.invocation_id,waiting.waits[0].id,{'blob':'approved'})
                result=await host.join(ref.invocation_id)
                self.assertEqual(result.output,{'blob':'approved'})

    async def test_eviction_rebuilds_value_references_without_loading_business_artifacts(self):
        """Cold metadata never loads large bodies; Node and Call values survive cache loss."""
        sink=await self.sink(artifact_threshold_bytes=8192,projection_cache_entry_bytes=1)
        app,wf,_=await self.registered(sink)
        value={'blob':'projection-business-sentinel-'+'x'*(2*1024*1024)}
        result=await app.ainvoke(wf,value)
        storage=sink._writer.storage;original=storage.row
        async def no_artifacts(conn,name,**identity):
            self.assertNotEqual(name,'artifacts','Metadata hydrated a business Artifact')
            return await original(conn,name,**identity)
        with patch.object(storage,'row',side_effect=no_artifacts):
            nodes=(await sink.list_nodes(result.invocation_id)).items
            calls=(await sink.list_calls(result.invocation_id)).items
        self.assertEqual(sink._queries.projection_cache.entries,{})
        for row in nodes:
            detail=await sink.node_detail(result.invocation_id,row['occurrence_id'])
            self.assertEqual(detail['input']['value'],value);self.assertEqual(detail['output']['value'],value)
        for row in calls:
            detail=await sink.call_detail(result.invocation_id,row['call_id'])
            self.assertEqual(detail['input']['value'],value);self.assertEqual(detail['output']['value'],value)

    async def test_expiration_releases_cache_without_waiting_for_another_query(self):
        """Idle projections expire on their own while immutable Events remain readable."""
        sink=await self.sink(projection_cache_ttl_seconds=.01)
        app,wf,_=await self.registered(sink)
        result=await app.ainvoke(wf,{'blob':'ttl'})
        expected=(await sink.list_nodes(result.invocation_id)).items
        self.assertGreater(sink._queries.projection_cache.bytes,0)
        await asyncio.sleep(.15)
        self.assertEqual(sink._queries.projection_cache.bytes,0)
        self.assertEqual((await sink.list_nodes(result.invocation_id)).items,expected)

    async def test_live_projection_advances_only_over_new_events(self):
        """A hot view folds the new tail and preserves stable Node and Wait identities."""
        sink=await self.sink()
        wf=Workflow('incremental-view',nodes=[Node('approval',Wait(Value,Value))])
        app=AutoAgentApp(runtime_event_sink=sink);self.apps.append(app)
        app.register_workflow(wf);await sink.register_workflow(app.workflow_definition_snapshot(wf.id))
        result=await app.ainvoke(wf,{'blob':'request'},entry_node_id='approval')
        first=(await sink.list_nodes(result.invocation_id)).items
        cache=sink._queries.projection_cache;initial=cache.built_events
        await sink.list_nodes(result.invocation_id)
        self.assertEqual(cache.built_events,initial)
        await app.aresume(result.ref,result.waits[0].id,{'blob':'approved'})
        last=(await sink.list_nodes(result.invocation_id)).items
        delta=cache.built_events-initial
        self.assertGreater(delta,0)
        self.assertLess(delta,cache.built_events)
        self.assertEqual(first[0]['occurrence_id'],last[0]['occurrence_id'])
        self.assertEqual(last[0]['status'],'completed')
        self.assertLessEqual(cache.bytes,cache.config.projection_cache_bytes)
        self.assertNotIn(b'approved',b''.join(e['data'] for e in cache.entries.values()))

    async def test_old_sql_snapshot_cannot_use_a_newer_cached_projection(self):
        """A newer cache generation never leaks completed state into an older SQL cut."""
        sink=await self.sink()
        wf=Workflow('snapshot-cut',nodes=[Node('approval',Wait(Value,Value))])
        app=AutoAgentApp(runtime_event_sink=sink);self.apps.append(app)
        app.register_workflow(wf);await sink.register_workflow(app.workflow_definition_snapshot(wf.id))
        result=await app.ainvoke(wf,{'blob':'request'},entry_node_id='approval')
        await sink.list_nodes(result.invocation_id)
        ready,release=threading.Event(),threading.Event()
        async def old_view():
            async with sink._queries.view(invocation_id=result.invocation_id) as (conn,_,_,frontier):
                ready.set()
                await asyncio.to_thread(release.wait)
                table=meta.tables['node_executions']
                return [dict(r) for r in (await conn.execute(select(table).where(table.c.invocation_id==result.invocation_id))).mappings()],frontier
        task=asyncio.create_task(sink._writer.run(old_view))
        self.assertTrue(await asyncio.to_thread(ready.wait,2))
        try:
            await app.aresume(result.ref,result.waits[0].id,{'blob':'approved'})
            latest=await sink.list_nodes(result.invocation_id)
            self.assertEqual(latest.items[0]['status'],'completed')
        finally:release.set()
        rows,frontier=await asyncio.wait_for(task,3)
        self.assertEqual(rows[0]['status'],'waiting')
        self.assertLess(frontier,latest.frontier)

    async def test_ui_connection_pressure_cannot_block_execution_or_control(self):
        """Exhausting every UI reader connection leaves the Writer/control pool usable."""
        from contextlib import AsyncExitStack
        sink=await self.sink()
        app,wf,_=await self.registered(sink)
        ready,release=threading.Event(),threading.Event()
        async def hold_readers():
            async with AsyncExitStack() as stack:
                for _ in range(4):
                    await stack.enter_async_context(sink._writer.storage.query_engine.connect())
                ready.set()
                await asyncio.to_thread(release.wait)
        holder=asyncio.create_task(sink._writer.run(hold_readers))
        self.assertTrue(await asyncio.to_thread(ready.wait,3))
        try:
            result=await asyncio.wait_for(app.ainvoke(wf,{'blob':'independent'}),3)
            state=await asyncio.wait_for(sink.execution_state(result.invocation_id),3)
            directory=await asyncio.wait_for(sink.get_invocation(result.invocation_id),3)
            self.assertTrue(state['settled'])
            self.assertEqual(directory['status'],'completed')
        finally:
            release.set()
            await holder

    async def test_accepted_snapshot_survives_concurrent_commit_releasing_queue_entries(self):
        """An accepted read keeps its Event values after the Writer clears committed entries."""
        sink=await self.sink(mode='buffered',batch_delay_ms=60000)
        app,wf,_=await self.registered(sink)
        result=await app.ainvoke(wf,{'blob':'pending'})
        ready,release=threading.Event(),threading.Event()
        original=sink._writer.cpu
        first=True
        async def gated(function,*args):
            nonlocal first
            if first and getattr(function,'__name__','')=='pending_metadata':
                first=False
                ready.set()
                await asyncio.to_thread(release.wait)
            return await original(function,*args)
        with patch.object(sink._writer,'cpu',side_effect=gated):
            query=asyncio.create_task(sink.execution_state(result.invocation_id))
            self.assertTrue(await asyncio.to_thread(ready.wait,2))
            try:
                self.assertFalse(sink._writer.mutation.locked())
                await asyncio.wait_for(sink.flush(),3)
                self.assertEqual((await sink.health()).queued_count,0)
            finally:release.set()
            state=await asyncio.wait_for(query,3)
            self.assertEqual(state['status'],'completed')
            self.assertTrue(state['settled'])

    async def test_metadata_readers_release_without_waiting_for_cycle_collection(self):
        """Completed metadata queries cannot retain Event bodies in recursive closure cycles."""
        import gc
        import weakref
        from autoagent.host.persistence.reading import MetadataReader
        sink=await self.sink()
        app,wf,_=await self.registered(sink)
        result=await app.ainvoke(wf,{'blob':'metadata'})
        original=MetadataReader.open
        references=[]
        async def track(*args,**kwargs):
            reader=await original(*args,**kwargs)
            references.append(weakref.ref(reader))
            return reader
        enabled=gc.isenabled()
        gc.disable()
        try:
            with patch.object(MetadataReader,'open',side_effect=track):
                await sink.execution_trace(result.invocation_id)
            self.assertTrue(references)
            self.assertTrue(all(reference() is None for reference in references))
        finally:
            if enabled:gc.enable()

    async def test_sql_compilation_does_not_retain_read_model_parameters(self):
        """Compiled SQL cannot retain the serialized projection outside its byte budget."""
        sink=await self.sink()
        app,wf,_=await self.registered(sink)
        result=await app.ainvoke(wf,{'blob':'metadata'})
        await sink.execution_trace(result.invocation_id)
        async def parameters():
            return [value for compiled in sink._writer.storage.engine.sync_engine._compiled_cache.values()
                    for value in compiled.params.values() if isinstance(value,str)]
        retained=await sink._writer.run(parameters)
        self.assertFalse(any('"occurrence_id"' in value and '"node_id"' in value for value in retained))

    async def test_dataset_pages_remain_fixed_after_cache_eviction_and_execution_progress(self):
        """Immutable Dataset pages survive projection eviction and later Wait completion."""
        sink=await self.sink()
        wf=Workflow('dataset-cut',nodes=[Node('approval',Wait(Value,Value))])
        app=AutoAgentApp(runtime_event_sink=sink);self.apps.append(app)
        app.register_workflow(wf);await sink.register_workflow(app.workflow_definition_snapshot(wf.id))
        result=await app.ainvoke(wf,{'blob':'request'},entry_node_id='approval')
        manifest=await sink.trace_dataset(result.invocation_id)
        expected=await sink.trace_dataset_page(manifest['snapshot_id'],'node',0)
        async def clear(): sink._queries.projection_cache.clear()
        await sink._writer.run(clear)
        await app.aresume(result.ref,result.waits[0].id,{'blob':'approved'})
        await sink.list_nodes(result.invocation_id)
        self.assertEqual(await sink.trace_dataset_page(manifest['snapshot_id'],'node',0),expected)


class SQLiteProjectionTests(ProjectionContracts,unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        return 'sqlite+aiosqlite:///'+str(Path(self.directory.name)/'runtime.db')
    async def remove_database(self): pass


@unittest.skipUnless(POSTGRES_TEST_URL,'PostgreSQL not configured')
class PostgreSQLProjectionTests(ProjectionContracts,unittest.IsolatedAsyncioTestCase):
    async def database_url(self):
        self.database=PostgreSQLDatabase()
        return await self.database.create()
    async def remove_database(self): await self.database.close()

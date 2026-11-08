"""Optional SQL read models derived from canonical facts.

The reducer never owns execution control. Background computation happens on a
read snapshot; only bounded dirty rows and their frontier enter the publish
transaction. Epoch/frontier comparison rejects stale work after maintenance.
"""
from __future__ import annotations

from dataclasses import dataclass
import time
import weakref

from sqlalchemy import delete, func, select, tuple_
from sqlalchemy.sql import visitors
from sqlalchemy.sql.schema import Table

from .projections import ProjectionBatch
from .codec import retained_objects
from .models import CapacityError
from .schema import DETAIL_TABLES, meta

STATE = meta.tables['query_projection_state']
ENTITIES = meta.tables['query_projection_entities']
FRONTIER_KEY = ('query_projection_frontier',)
STATEFUL_TABLES = ('sessions', 'invocations', 'node_executions', 'operator_calls', 'waits')


async def reset(conn):
    """Invalidate detailed read models atomically with a fact/history change."""
    for name in reversed(DETAIL_TABLES):
        await conn.execute(delete(meta.tables[name]))
    await conn.execute(delete(ENTITIES))
    await conn.execute(delete(STATE))


async def progress(conn, store_id):
    row = (await conn.execute(select(STATE).where(STATE.c.id == 1))).mappings().first()
    return row['processed_order'] if row and row['store_id'] == store_id else None


async def anchor_ids(conn, invocation_ids=None):
    """Earliest available imported Checkpoint per incomplete Session history."""
    members, events = meta.tables['checkpoint_sessions'], meta.tables['runtime_events']
    ranked = select(members.c.checkpoint_id, func.row_number().over(
        partition_by=members.c.session_id,
        order_by=(members.c.sequence, members.c.checkpoint_id)).label('position')).where(
        ~members.c.session_id.in_(select(events.c.session_id).where(events.c.sequence == 1)))
    if invocation_ids is not None:
        ranked = ranked.where(members.c.invocation_id.in_(invocation_ids))
    ranked = ranked.subquery()
    return (await conn.scalars(select(ranked.c.checkpoint_id).where(ranked.c.position == 1)
                              .distinct().order_by(ranked.c.checkpoint_id))).all()


class BatchConnection:
    """SQL history plus uncommitted append-only Edge/Graph metadata."""
    def __init__(self, raw, batch):
        self.raw, self.batch = raw, weakref.proxy(batch)

    def __getattr__(self, name):
        return getattr(self.raw, name)

    async def detail_get(self, name, identity):
        row = (await self.raw.execute(select(meta.tables[name]).filter_by(**identity))).mappings().first()
        return dict(row) if row else None

    async def detail_ordinal(self, invocation_id, node_id):
        cached = self.batch.cached_ordinal((invocation_id, node_id))
        if cached is not None:
            return cached
        table = meta.tables['node_executions']
        return await self.raw.scalar(select(func.coalesce(func.max(table.c.ordinal), 0)).where(
            table.c.invocation_id == invocation_id, table.c.node_id == node_id))

    async def execute(self, statement, *args, **kwargs):
        used = {item.name for item in visitors.iterate(statement)
                if isinstance(item, Table) and item.name in ('graph_changes', 'edge_evaluations')
                and self.batch.dirty[item.name]}
        if used:
            # Reuse the same typed JSON relation as the query fallback. These
            # tables are append-only, so the durable prefix and dirty rows are
            # disjoint; no per-Event persistence is needed during computation.
            from .read_models import ProjectionConnection
            adapter = ProjectionConnection(self.raw, None, 0, '', fixed=self.batch.rows)
            adapter.cache = self.batch.cache_limits
            replacements = {}
            for name in used:
                table = meta.tables[name]
                dirty = await adapter._statement(select(table))
                relation = select(table).union_all(dirty).cte('pending_' + name)
                replacements[table] = relation
                replacements.update({col: relation.c[col.name] for col in table.c})
            statement = visitors.replacement_traverse(statement, {}, lambda item: replacements.get(item))
            kwargs['execution_options'] = {**kwargs.get('execution_options', {}), 'compiled_cache': None}
        return await self.raw.execute(statement, *args, **kwargs)

    async def scalar(self, statement, *args, **kwargs):
        return (await self.execute(statement, *args, **kwargs)).scalar()


class SQLProjector(ProjectionBatch):
    def __init__(self, conn, storage, config, *, cache=None, store_id=None):
        from types import SimpleNamespace
        super().__init__(None, storage.source, graph_loader=storage.workflow_graph)
        self.connection = BatchConnection(conn, self)
        self.cache_limits = SimpleNamespace(config=config)
        self.metadata_cache, self.store_id = cache, store_id

    def cached_row(self, name, key):
        return self.metadata_cache.get(('query_projection_row', self.store_id, name, key)) if self.metadata_cache is not None else None

    def cached_ordinal(self, scope):
        return self.metadata_cache.get(('query_projection_ordinal', self.store_id, scope)) if self.metadata_cache is not None else None

    async def get(self, name, **identity):
        table = meta.tables[name]
        key = tuple(identity[col.name] for col in table.primary_key)
        if key not in self.rows[name]:
            cached = self.cached_row(name, key)
            if cached is not None:
                self.rows[name][key] = cached
            elif name in ('sessions', 'invocations'):
                self.rows[name][key] = await self.connection.raw.scalar(select(ENTITIES.c.data).where(
                    ENTITIES.c.kind == name, ENTITIES.c.identity == key[0]))
            elif name in ('graph_changes', 'edge_evaluations'):
                self.rows[name][key] = None
            else:
                self.rows[name][key] = await self.connection.detail_get(name, identity)
        return self.rows[name][key]

    async def prefetch(self, records):
        """One indexed lookup per relation for ordinary node/call/wait updates."""
        keys = {name: set() for name in ('sessions', 'invocations', 'node_executions', 'operator_calls', 'waits')}
        scopes = set()
        for record in records:
            sid, iid = record['session_id'], record['invocation_id']
            keys['sessions'].add((sid,))
            if iid:
                keys['invocations'].add((iid,))
                if record['payload'].get('node_id'):
                    scopes.add((iid, record['payload']['node_id']))
                for name, field in (('node_executions', 'occurrence_id'), ('operator_calls', 'call_id'), ('waits', 'wait_id')):
                    value = record['payload'].get(field)
                    if value:
                        keys[name].add((iid, value))
                for op in (record.get('delta') or {}).get('operations', ()):
                    path = op['path']
                    names = {'occurrences': 'node_executions', 'operator_calls': 'operator_calls', 'waits': 'waits'}
                    if len(path) >= 4 and path[:2] == ['invocation', 'scheduler'] and path[2] in names:
                        keys[names[path[2]]].add((iid, path[3]))
                        if path[2] == 'occurrences' and len(path) == 4 and isinstance(op.get('value'), dict):
                            node_id = op['value'].get('node_id')
                            if node_id:
                                scopes.add((iid, node_id))
        for name, identities in keys.items():
            values = list(identities - self.rows[name].keys())
            missing = []
            for key in values:
                cached = self.cached_row(name, key)
                if cached is None:
                    missing.append(key)
                else:
                    self.rows[name][key] = cached
            values = missing
            table = meta.tables[name]
            for start in range(0, len(values), 300):
                group = values[start:start + 300]
                self.rows[name].update(dict.fromkeys(group))
                if name in ('sessions', 'invocations'):
                    rows = await self.connection.raw.execute(select(ENTITIES.c.identity, ENTITIES.c.data).where(
                        ENTITIES.c.kind == name, ENTITIES.c.identity.in_([key[0] for key in group])))
                    self.rows[name].update(((identity,), data) for identity, data in rows)
                else:
                    rows = (await self.connection.raw.execute(select(table).where(
                        tuple_(*table.primary_key.columns).in_(group)))).mappings()
                    self.rows[name].update((tuple(row[col.name] for col in table.primary_key), dict(row)) for row in rows)
        scopes.update((row['invocation_id'], row['node_id']) for row in self.rows['node_executions'].values() if row)
        table = meta.tables['node_executions']
        missing_scopes = []
        for scope in scopes:
            cached = self.cached_ordinal(scope)
            if cached is None:
                missing_scopes.append(scope)
            else:
                self.ordinals[scope] = max(self.ordinals.get(scope, 0), cached)
        scopes = missing_scopes
        for start in range(0, len(scopes), 300):
            group = scopes[start:start + 300]
            for scope in group:
                self.ordinals.setdefault(scope, 0)
            for iid, node_id, ordinal in await self.connection.raw.execute(select(
                table.c.invocation_id, table.c.node_id, func.max(table.c.ordinal)).where(
                    tuple_(table.c.invocation_id, table.c.node_id).in_(group))
                    .group_by(table.c.invocation_id, table.c.node_id)):
                self.ordinals[iid, node_id] = max(self.ordinals[iid, node_id], ordinal)


@dataclass(slots=True)
class PreparedProjection:
    store_id: str
    after: int | None
    order: int
    batch: SQLProjector
    count: int
    transactional: bool = False

    def publish_cache(self, cache):
        """Only the owner of the committed SQL transaction may call this."""
        for name in STATEFUL_TABLES:
            for key, row in self.batch.rows[name].items():
                if row is not None:
                    cache.put(('query_projection_row', self.store_id, name, key), row)
        for scope, ordinal in self.batch.ordinals.items():
            cache.put(('query_projection_ordinal', self.store_id, scope), ordinal)
        cache.put(FRONTIER_KEY, (self.store_id, self.order))

    def invalidate_cache(self, cache):
        # Earlier chunks of the same SQL transaction may not be retained for
        # publication. Their old committed rows must not survive a newer cut.
        for name in STATEFUL_TABLES:
            for key in self.batch.dirty[name]:
                cache.discard(('query_projection_row', self.store_id, name, key))
        for scope in self.batch.ordinals:
            cache.discard(('query_projection_ordinal', self.store_id, scope))


def check_capacity(batch, config):
    if sum(size for _, size in retained_objects(batch.rows).values()) > config.projection_build_bytes:
        raise CapacityError('Database projection batch exceeds projection_build_bytes.')


async def transactional(storage, conn, config, records, before):
    """Fold this transaction's validated new facts without reading them back.

    The Writer owns the fact frontier and already locks persistence_meta before
    entering here. A cold/mismatched cursor uses SQL catch-up, including imported
    Checkpoint anchors. Cached reducer rows belong only to a proven same epoch.
    """
    frontier = storage.metadata_cache.get(FRONTIER_KEY)
    if frontier is None or frontier[1] != before:
        store_id = await conn.scalar(select(meta.tables['persistence_meta'].c.store_id))
        after = await progress(conn, store_id)
        if after != before:
            return await catch_up(storage, conn, config)
        frontier = (store_id, after)
        cache = None  # No proof that older cached rows describe this SQL cut.
    else:
        cache = storage.metadata_cache
    for start in range(0, len(records), config.projection_batch_count):
        group = records[start:start + config.projection_batch_count]
        batch = SQLProjector(conn, storage, config, cache=cache, store_id=frontier[0])
        await batch.prefetch([record for record, _, _ in group])
        for index, (record, event_order, binding) in enumerate(group):
            await batch.runtime(record, event_order, binding)
            if index % 64 == 63:
                check_capacity(batch, config)
        check_capacity(batch, config)
        prepared = PreparedProjection(frontier[0], before, group[-1][1], batch, len(group), transactional=True)
        await publish(storage, conn, prepared)
        prepared.invalidate_cache(storage.metadata_cache)
        storage._committing_projection = prepared
        before, cache = prepared.order, None
    return len(records)


async def prepare(storage, conn, config):
    """Read one consistent fact page without decoding business Artifacts."""
    from .projector_source import metadata_record
    header = (await conn.execute(select(meta.tables['persistence_meta']))).mappings().one()
    after = await progress(conn, header['store_id'])
    batch = SQLProjector(conn, storage, config)
    if after is None:
        # Use the earliest imported anchor per Session, never a later recovery
        # snapshot which could hide the original detail value references.
        for identity in await anchor_ids(conn):
            record = await metadata_record(storage, conn, 'checkpoint', identity, availability=True)
            await batch.checkpoint(record, identity, anchor=True)
    events, refs = meta.tables['runtime_events'], meta.tables['artifact_refs']
    rows = (await conn.execute(select(events).where(events.c.store_order > (after or 0),
        events.c.store_order <= header['last_store_order']).order_by(events.c.store_order)
        .limit(config.projection_batch_count))).mappings().all()
    page_refs = {row['event_id']: {} for row in rows}
    if rows:
        for owner, path, artifact_hash in await conn.execute(select(refs.c.owner_id, refs.c.path, refs.c.artifact_hash)
            .where(refs.c.owner_kind == 'runtime_event', refs.c.owner_id.in_(page_refs))):
            page_refs[owner][path] = artifact_hash
    records = [await metadata_record(storage, conn, 'runtime_event', row['event_id'], row=row,
        refs=page_refs[row['event_id']], availability=row['event_name'] in ('invocation.forked', 'fork.state_imported'))
        for row in rows]
    # Anchor rows already loaded must not be replaced by a prefetch miss.
    await batch.prefetch(records)
    for index, (record, row) in enumerate(zip(records, rows)):
        session = await batch.get('sessions', session_id=record['session_id'])
        if not session or record['sequence'] > session['durable_sequence']:
            await batch.runtime(record, row['store_order'], row['operator_definition_id'])
        if index % 64 == 63:
            check_capacity(batch, config)
    check_capacity(batch, config)
    order = rows[-1]['store_order'] if rows else header['last_store_order']
    return PreparedProjection(header['store_id'], after, order, batch, len(rows))


async def publish(storage, conn, prepared):
    """Publish rows and frontier together, rejecting a changed history/worker cut."""
    header = meta.tables['persistence_meta']
    # Shared lock order with maintenance. PostgreSQL's main Writer updates this
    # row only at commit; the background lock timeout keeps contention bounded.
    current = prepared.store_id
    if not prepared.transactional:
        current = await conn.scalar(select(header.c.store_id).with_for_update())
        if current != prepared.store_id or await progress(conn, current) != prepared.after:
            return False
    for name in DETAIL_TABLES:
        rows = prepared.batch.changed(name)
        if name in ('graph_changes', 'edge_evaluations'):
            await storage.insert_rows(conn, name, rows)
        else:
            await storage.upsert(conn, name, rows)
    entities = []
    for name in ('sessions', 'invocations'):
        for row in prepared.batch.changed(name):
            entities.append(dict(kind=name, identity=row['session_id' if name == 'sessions' else 'invocation_id'],
                                 data=row))
    await storage.upsert(conn, ENTITIES.name, entities)
    await storage.upsert(conn, STATE.name, [dict(id=1, store_id=current,
        processed_order=prepared.order, updated_at_us=time.time_ns() // 1000)])
    return True


async def catch_up(storage, conn, config):
    """Transactional mode folds its database facts before the Writer commits."""
    count = 0
    while True:
        prepared = await prepare(storage, conn, config)
        if prepared.after == prepared.order or (
                prepared.after is None and not prepared.count and not any(prepared.batch.dirty.values())):
            return count
        await publish(storage, conn, prepared)
        prepared.invalidate_cache(storage.metadata_cache)
        storage._committing_projection = prepared
        count += prepared.count
        if prepared.count < config.projection_batch_count:
            return count

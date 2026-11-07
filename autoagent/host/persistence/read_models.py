"""Byte-bounded read models over immutable Runtime Events.

SQL relations adapt the existing read queries to cached metadata. A query uses one fixed database snapshot and
never advertises a frontier beyond the Events it has actually folded.
"""
from __future__ import annotations

import asyncio
from collections import OrderedDict
import json
import time

from sqlalchemy import JSON, Text, bindparam, cast, func, select, type_coerce
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import visitors
from sqlalchemy.sql.schema import Table

from .codec import retained_objects
from .models import CapacityError
from .projections import ProjectionBatch, PROJECTION_TABLES
from .schema import DETAIL_TABLES, meta

_UNREAD = object()


class MemoryProjector(ProjectionBatch):
    """The shared Event projector with query-local row storage and no SQL writes."""
    async def get(self, name, **identity):
        key = tuple(identity[c.name] for c in meta.tables[name].primary_key)
        return self.rows[name].get(key)

    async def put(self, name, identity, values):
        if name == 'node_executions' and 'node_id' in values:
            self.ordinals.setdefault((identity['invocation_id'], values['node_id']), 0)
        return await super().put(name, identity, values)

    def reset_changes(self):
        for values in self.dirty.values():
            values.clear()


class ProjectionCache:
    def __init__(self, writer):
        self.writer = writer
        self.config = writer.config
        self.entries = OrderedDict()
        self.bytes = 0
        self.lock = asyncio.Lock()
        self.hits = self.misses = self.evictions = self.built_events = 0
        self.cleaner = None

    async def statistics(self):
        self.expire()
        return dict(bytes=self.bytes, budget_bytes=self.config.projection_cache_bytes,
                    entries=len(self.entries), hits=self.hits, misses=self.misses,
                    evictions=self.evictions, built_events=self.built_events)

    async def close(self):
        if self.cleaner is not None:
            self.cleaner.cancel()
            await asyncio.gather(self.cleaner,return_exceptions=True)
            self.cleaner=None
        self.clear()

    async def _clean(self):
        while True:
            await asyncio.sleep(max(.05,min(1,self.config.projection_cache_ttl_seconds)))
            self.expire()

    def clear(self):
        self.entries.clear()
        self.bytes = 0

    def expire(self):
        now = time.monotonic()
        # Hits and saves move entries to the end, so expiry follows LRU order.
        while self.entries:
            entry = next(iter(self.entries.values()))
            if now - entry['used'] < self.config.projection_cache_ttl_seconds:
                break
            self.entries.popitem(last=False)
            self.bytes -= entry['size']
            self.evictions += 1

    def _save(self, key, batch, order):
        if not self.config.projection_cache_enabled:
            return
        if self.cleaner is None:
            self.cleaner=asyncio.create_task(self._clean())
        data = json.dumps({name:list(rows.values()) for name, rows in batch.rows.items()},
                          separators=(',', ':'), ensure_ascii=False).encode()
        size=len(data)+sum(n for _,n in retained_objects(key).values())+512
        previous = self.entries.pop(key, None)
        if previous:
            self.bytes -= previous['size']
        if size > min(self.config.projection_cache_entry_bytes, self.config.projection_cache_bytes):
            return
        while self.entries and self.bytes + size > self.config.projection_cache_bytes:
            _, old = self.entries.popitem(last=False)
            self.bytes -= old['size']
            self.evictions += 1
        self.entries[key] = dict(data=data, size=size, order=order, used=time.monotonic())
        self.bytes += size

    async def build(self, connection, root, frontier, store_id, *, tails=None):
        # Bound construction concurrency and coalesce callers at the same cut.
        async with self.lock:
            self.expire()
            key = (store_id, root, 'live') if tails is not None else (store_id, root)
            previous = self.entries.get(key)
            entry, entry_key = previous, key
            if tails is None:
                live_key = (store_id, root, 'live')
                live = self.entries.get(live_key)
                if live and live['order'] <= frontier and (entry is None or entry['order'] > frontier
                                                          or live['order'] > entry['order']):
                    entry, entry_key = live, live_key
            batch = MemoryProjector(connection, self.writer.storage.source,
                                    graph_loader=self.writer.storage.workflow_graph)
            after = 0
            if entry and entry['order'] <= frontier:
                self.hits += 1
                entry['used'] = time.monotonic()
                self.entries.move_to_end(entry_key)
                for name, rows in json.loads(entry['data']).items():
                    table = meta.tables[name]
                    batch.rows[name] = {tuple(row[c.name] for c in table.primary_key):row for row in rows if row}
                for row in batch.rows['node_executions'].values():
                    scope=(row['invocation_id'],row['node_id'])
                    batch.ordinals[scope]=max(batch.ordinals.get(scope,0),row['ordinal'])
                after = entry['order']
            else:
                self.misses += 1
            if after == frontier and entry is not None:
                return batch.rows
            persisted = await connection.database_order()
            if persisted is not None and persisted > after and persisted <= frontier:
                # A database prefix avoids replaying old facts after TTL expiry
                # or a fresh CLI process. Business values stay in Event/Artifact.
                invs = meta.tables['invocations']
                ids = select(invs.c.invocation_id).where(invs.c.root_invocation_id == root)
                sessions = select(invs.c.session_id).where(invs.c.root_invocation_id == root)
                batch.rows = {name: {} for name in PROJECTION_TABLES}
                charged = 0
                def retain(name, row):
                    nonlocal charged
                    charged += sum(size for _, size in retained_objects(row).values()) + 128
                    if charged > self.config.projection_build_bytes:
                        raise CapacityError('Database projection prefix exceeds projection_build_bytes.')
                    table = meta.tables[name]
                    batch.rows[name][tuple(row[col.name] for col in table.primary_key)] = row
                for name in DETAIL_TABLES:
                    table = meta.tables[name]
                    async with connection.raw.stream(select(table).where(table.c.invocation_id.in_(ids))
                                                     .execution_options(yield_per=128)) as rows:
                        async for row in rows.mappings():
                            retain(name, dict(row))
                entities = meta.tables['query_projection_entities']
                from sqlalchemy import or_, and_
                async with connection.raw.stream(select(entities.c.kind, entities.c.data).where(or_(
                    and_(entities.c.kind == 'invocations', entities.c.identity.in_(ids)),
                    and_(entities.c.kind == 'sessions', entities.c.identity.in_(sessions))))
                    .execution_options(yield_per=128)) as rows:
                    async for name, row in rows:
                        retain(name, row)
                batch.ordinals.clear()
                for row in batch.rows['node_executions'].values():
                    scope = (row['invocation_id'], row['node_id'])
                    batch.ordinals[scope] = max(batch.ordinals.get(scope, 0), row['ordinal'])
                after = persisted
            # SQL needed by conditional routes and Fork lineage reads this very
            # prefix, not another cache generation or latest persisted details.
            projected = ProjectionConnection(connection.raw, self, frontier, store_id,
                                              fixed=batch.rows, scope=root)
            batch.connection = projected
            await self._fold(projected, batch, root, after, frontier, set(), tails or {})
            if previous is None or previous["order"] <= frontier:
                self._save(key, batch, frontier)
            return batch.rows

    async def _fold(self, connection, batch, root, after, frontier, visiting, tails):
        if root in visiting:
            raise ValueError('Cyclic Fork projection lineage.')
        visiting.add(root)
        invs, events = (meta.tables[n] for n in ('invocations', 'runtime_events'))
        ids = select(invs.c.invocation_id).where(invs.c.root_invocation_id==root)
        if after == 0:
            # Imported Checkpoints are anchors only when the original prefix is
            # absent. Their full current State must not seed complete histories.
            from .query_projections import anchor_ids
            anchors = await anchor_ids(connection.raw, ids)
            for identity in anchors:
                from .projector_source import metadata_record
                record = await metadata_record(self.writer.storage,connection.raw,'checkpoint',identity,availability=True)
                await batch.checkpoint(record, identity, anchor=True)
        # Cursor pages bound Event bodies; historical lists never retain them.
        order = after
        charged = 0
        sizes = {}
        for name, rows in batch.rows.items():
            for key, row in rows.items():
                size = sum(n for _, n in retained_objects(row).values()) + 128
                sizes[(name,key)] = size; charged += size
        async def apply(light, event_order, binding):
            nonlocal charged
            if light['event_name'] in ('invocation.forked','fork.state_imported'):
                source_id=light['payload']['source_root_invocation_id']
                if source_id != root and source_id not in visiting:
                    source = batch.rows['invocations'].get((light['payload']['source_invocation_id'],))
                    if source is None:
                        await self._fold(connection,batch,source_id,0,frontier,visiting,tails)
            session = batch.rows['sessions'].get((light['session_id'],))
            if session and light['sequence'] <= session['durable_sequence']:
                return
            await batch.runtime(light,event_order,binding)
            self.built_events += 1
            for name, keys in batch.dirty.items():
                for key in keys:
                    value=batch.rows[name].get(key)
                    size=sum(n for _,n in retained_objects(value).values())+128
                    charged += size-sizes.get((name,key),0)
                    sizes[(name,key)]=size
            if charged > self.config.projection_build_bytes:
                raise CapacityError('Projection construction exceeds projection_build_bytes.')
            batch.reset_changes()

        while True:
            rows = (await connection.raw.execute(select(events).where(events.c.invocation_id.in_(ids),
                events.c.store_order>order, events.c.store_order<=frontier)
                .order_by(events.c.store_order).limit(64))).mappings().all()
            if not rows:
                break
            references = meta.tables['artifact_refs']
            page_refs = {row['event_id']: {} for row in rows}
            for ref in await connection.raw.execute(select(
                references.c.owner_id, references.c.path, references.c.artifact_hash
            ).where(references.c.owner_kind == 'runtime_event',
                    references.c.owner_id.in_(page_refs))):
                page_refs[ref.owner_id][ref.path] = ref.artifact_hash
            for row in rows:
                from .projector_source import metadata_record
                light = await metadata_record(self.writer.storage,connection.raw,'runtime_event',row['event_id'],
                    row=row,refs=page_refs.pop(row['event_id']),
                    availability=row['event_name'] in ('invocation.forked','fork.state_imported'))
                await apply(light,row['store_order'],row['operator_definition_id'])
            order=rows[-1]['store_order']
            await asyncio.sleep(0)
        for index, (light, event_order, binding) in enumerate(tails.get(root, ())):
            if after < event_order <= frontier:
                await apply(light,event_order,binding)
            if index % 64 == 63:
                await asyncio.sleep(0)
        visiting.remove(root)


class ProjectionConnection:
    """Expose typed read relations while facts stay on the original SQL snapshot."""
    def __init__(self, raw, cache, frontier, store_id, *, scope=None, fixed=None, persisted_order=_UNREAD):
        self.raw, self.cache, self.frontier, self.store_id = raw, cache, frontier, store_id
        self.scope, self.fixed = scope, fixed
        self.persisted_order = persisted_order
        self.sql_scopes = {}
        self.loaded = {}
        self.merged = None
        self.relations = {}
        self.roots = {}
        self.working_bytes = self.relation_bytes = 0

    async def database_order(self):
        if self.fixed is not None:
            return None
        if self.persisted_order is _UNREAD:
            from .query_projections import progress
            self.persisted_order = await progress(self.raw, self.store_id)
        return self.persisted_order

    @property
    def dialect(self):
        return self.raw.dialect

    def __getattr__(self, name):
        return getattr(self.raw, name)

    def _candidates(self, statement):
        candidates = {self.scope} if self.scope else set()
        # All detail endpoints are Invocation-scoped. Also recognize explicit
        # predicates used by source Fork queries and generic metadata readers.
        from sqlalchemy.sql.elements import BinaryExpression, BindParameter
        for element in visitors.iterate(statement):
            if isinstance(element, BinaryExpression) and getattr(element.left,'name',None) in ('invocation_id','root_invocation_id') and isinstance(element.right,BindParameter):
                value=element.right.value
                if isinstance(value,str): candidates.add(value)
                elif isinstance(value,(tuple,list)): candidates.update(v for v in value if isinstance(v,str))
        return candidates

    async def _rows(self, statement):
        return await self._load(self._candidates(statement))

    async def _can_sql(self, candidates):
        if self.fixed is not None:
            return False
        key = frozenset(candidates)
        if key in self.sql_scopes:
            return self.sql_scopes[key]
        invs = meta.tables['invocations']
        roots = await self._resolve_roots(candidates)
        # Memory wins when a valid generation exists. Otherwise an up-to-date
        # persisted relation can be queried with indexed SQL and pagination.
        self.cache.expire()
        cached = any(entry['order'] <= self.frontier for cache_key, entry in self.cache.entries.items()
                     if cache_key[0] == self.store_id and (not candidates or cache_key[1] in roots))
        if cached:
            self.sql_scopes[key] = False
            return False
        persisted = await self.database_order()
        if persisted is None:
            self.sql_scopes[key] = False
            return False
        covered = persisted >= self.frontier
        if not covered and roots:
            last = await self.raw.scalar(select(func.max(invs.c.updated_store_order)).where(invs.c.root_invocation_id.in_(roots)))
            covered = last is not None and last <= persisted
        result = covered
        self.sql_scopes[key] = result
        return result

    async def _resolve_roots(self, candidates):
        invs=meta.tables['invocations']
        missing=list(candidates-self.roots.keys())
        for start in range(0,len(missing),500):
            group=missing[start:start+500]
            self.roots.update(dict.fromkeys(group))
            self.roots.update((await self.raw.execute(select(invs.c.invocation_id,invs.c.root_invocation_id)
                .where(invs.c.invocation_id.in_(group)))).all())
        return {self.roots[iid] for iid in candidates if self.roots[iid]}

    async def _load(self, candidates):
        if self.fixed is not None:
            return self.fixed
        invs=meta.tables['invocations']
        roots=await self._resolve_roots(candidates)
        if not roots and candidates:
            return {name:{} for name in PROJECTION_TABLES}
        if not roots:
            # Unscoped administrative statistics still have complete semantics;
            # individual roots are built under the same bounded cache policy.
            roots=set((await self.raw.scalars(select(invs.c.root_invocation_id).distinct())).all())
        for root in roots:
            if root not in self.loaded:
                self.loaded[root]=await self.cache.build(self,root,self.frontier,self.store_id)
                self.working_bytes=sum(n for _,n in retained_objects(self.loaded).values())
                if self.working_bytes>self.cache.config.projection_build_bytes:
                    raise CapacityError('Projection query exceeds projection_build_bytes.')
                self.merged=None
                self.relations.clear()
                self.relation_bytes=0
        if self.merged is None:
            self.merged={name:{} for name in PROJECTION_TABLES}
            for data in self.loaded.values():
                for name, records in data.items(): self.merged[name].update(records)
        return self.merged

    async def detail_get(self, name, identity):
        candidates={identity['invocation_id']} if 'invocation_id' in identity else {self.scope} if self.scope else set()
        if await self._can_sql(candidates):
            row = (await self.raw.execute(select(meta.tables[name]).filter_by(**identity))).mappings().first()
            return dict(row) if row else None
        data=await self._load(candidates)
        table=meta.tables[name]
        key=tuple(identity[col.name] for col in table.primary_key)
        row=data[name].get(key)
        return {col.name:row.get(col.name) for col in table.c} if row else None

    async def detail_rows(self, name, filters, *, order=(), cursor=None, limit=None):
        candidates={filters['invocation_id']} if filters.get('invocation_id') else {self.scope} if self.scope else set()
        if await self._can_sql(candidates):
            from sqlalchemy import tuple_
            table = meta.tables[name]
            statement = select(table).filter_by(**filters)
            if cursor is not None:
                statement = statement.where(tuple_(*(table.c[key] for key in order)) > tuple(cursor))
            if order:
                statement = statement.order_by(*(table.c[key] for key in order))
            if limit is not None:
                statement = statement.limit(limit)
            return [dict(row) for row in (await self.raw.execute(statement)).mappings()]
        data=await self._load(candidates)
        table=meta.tables[name]
        return [{col.name:row.get(col.name) for col in table.c}
                for row in data[name].values() if row and all(row.get(k)==v for k,v in filters.items())]

    async def detail_ordinal(self, invocation_id, node_id):
        rows=await self.detail_rows('node_executions',dict(invocation_id=invocation_id,node_id=node_id))
        return max((row['ordinal'] for row in rows),default=0)

    async def _statement(self, statement):
        used={element.name for element in visitors.iterate(statement)
              if isinstance(element,Table) and element.name in DETAIL_TABLES}
        if not used:
            return statement
        if await self._can_sql(self._candidates(statement)):
            return statement
        data=await self._rows(statement)
        replacements={}
        for name in sorted(used):
            table=meta.tables[name]
            relation=self.relations.get(name) if self.fixed is None else None
            if relation is not None:
                replacements[table]=relation
                replacements.update({col:relation.c[col.name] for col in table.c})
                continue
            payload=json.dumps(list(data[name].values()),ensure_ascii=False,separators=(',',':'))
            if self.working_bytes+self.relation_bytes+len(payload.encode())>self.cache.config.projection_build_bytes:
                raise CapacityError('Projection SQL relations exceed projection_build_bytes.')
            if self.dialect.name=='postgresql':
                items=func.jsonb_array_elements(cast(bindparam(None,payload,type_=Text),JSONB)).table_valued('value').alias()
                cols=[]
                for col in table.c:
                    value=items.c.value.op('->')(col.name) if isinstance(col.type,JSON) else cast(items.c.value.op('->>')(col.name),col.type)
                    cols.append(type_coerce(value,col.type).label(col.name))
            else:
                items=func.json_each(bindparam(None,payload)).table_valued('value').alias()
                cols=[type_coerce(func.json_extract(items.c.value,'$.'+col.name),col.type).label(col.name) for col in table.c]
            relation=select(*cols).select_from(items).cte('read_'+name)
            if self.fixed is None:
                self.relations[name]=relation
                self.relation_bytes+=len(payload.encode())
            replacements[table]=relation
            replacements.update({col:relation.c[col.name] for col in table.c})
        return visitors.replacement_traverse(statement,{},lambda element:replacements.get(element))

    async def execute(self, statement, *args, **kwargs):
        # Compiled SQL must not retain the JSON parameter containing an entire
        # query-local read model beyond its bounded cache lifetime.
        kwargs['execution_options']={**kwargs.get('execution_options',{}),'compiled_cache':None}
        return await self.raw.execute(await self._statement(statement),*args,**kwargs)

    async def scalar(self, statement, *args, **kwargs):
        return (await self.execute(statement, *args, **kwargs)).scalar()

    async def scalars(self, statement, *args, **kwargs):
        return (await self.execute(statement, *args, **kwargs)).scalars()

    def stream(self, statement, *args, **kwargs):
        from sqlalchemy.ext.asyncio.base import GeneratorStartableContext
        async def generate():
            kwargs['execution_options']={**kwargs.get('execution_options',{}),'compiled_cache':None}
            async with self.raw.stream(await self._statement(statement),*args,**kwargs) as result:
                yield result
        # AsyncConnection.stream supports both await and async-with.
        return GeneratorStartableContext(generate, (), {})

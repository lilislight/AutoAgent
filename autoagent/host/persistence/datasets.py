"""Immutable, paged trace exports. Only metadata is spooled, never business values."""
from collections import OrderedDict
import asyncio
import json
import tempfile
import time
from uuid import uuid4

from sqlalchemy import and_, func, select

from .models import CapacityError
from .replay import replay_events
from .schema import meta
from .timeline import TimelineQueries, timing_item
from .tracing import TraceQueries, summary
from .trace_cursor import encode_trace_cursor, decode_trace_cursor

PAGE_SIZE = 200


class TraceDatasets:
    def __init__(self, queries, config, source):
        self.queries, self.config = queries, config
        self.cleaner = None
        self.entries = OrderedDict()
        self.bytes = 0
        self.lock = asyncio.Lock()

    def _drop(self, key):
        entry = self.entries.pop(key)
        self.bytes -= entry['bytes']
        entry['file'].close()

    def _expire(self):
        now = time.monotonic()
        for key, entry in list(self.entries.items()):
            if now - entry['used'] > self.config.trace_snapshot_ttl_seconds:
                self._drop(key)

    async def _clean(self):
        while True:
            await asyncio.sleep(max(.05, min(1, self.config.trace_snapshot_ttl_seconds)))
            async with self.lock:
                self._expire()

    async def release(self, snapshot_id):
        async with self.lock:
            if snapshot_id in self.entries:
                self._drop(snapshot_id)

    async def close(self):
        if self.cleaner is not None:
            self.cleaner.cancel()
            await asyncio.gather(self.cleaner, return_exceptions=True)
            self.cleaner = None
        for key in list(self.entries):
            self._drop(key)

    async def create(self, iid, *, replay=False, snapshot_id=None, since=None, through_sequence=None, cursor=None):
        # One consistent read transaction is materialized to a private temporary
        # file, then released before any HTTP pagination. PostgreSQL uses MVCC;
        # SQLite's explicit read transaction is held only during this export.
        async with self.lock:
            self._expire()
            if self.cleaner is None:
                self.cleaner = asyncio.create_task(self._clean())
            base = self.entries.get(snapshot_id) if snapshot_id else None
            old = self.entries.get(since) if since else None
            if base and base['manifest']['invocation_id'] != iid:
                raise ValueError('Trace snapshot belongs to another execution.')
            if through_sequence is not None and (type(through_sequence) is not int or not 0 <= through_sequence <= 9007199254740991):
                raise ValueError('Invalid Replay boundary.')
            if old and (old['manifest']['invocation_id'] != iid or old['manifest']['replay']):
                raise ValueError('Incremental cursor belongs to another execution.')
            timeline, traces = TimelineQueries(self.queries), TraceQueries(self.queries)
            file = tempfile.TemporaryFile(mode='w+b')
            offsets, counts, size = {}, {}, 0

            async def write(kind, rows):
                nonlocal size
                payload = json.dumps(rows, ensure_ascii=False, separators=(',', ':')).encode()
                size += len(payload)
                if size > self.config.trace_snapshot_bytes:
                    raise CapacityError('Trace export exceeds the configured temporary snapshot budget.')
                offsets.setdefault(kind, []).append((file.tell(), len(payload)))
                counts[kind] = counts.get(kind, 0) + len(rows)
                await asyncio.to_thread(file.write, payload)

            try:
                async with self.queries.view() as (conn, _, _, frontier):
                    store_id = await conn.scalar(select(meta.tables['persistence_meta'].c.store_id))
                    previous = decode_trace_cursor(cursor, store_id, iid, frontier) if cursor is not None else old['manifest']['frontier'] if old else None
                    if replay and cursor is not None:
                        raise ValueError('Replay uses a fixed sequence boundary, not an incremental cursor.')
                    if previous is None or replay:
                        trace = await traces._execution(conn, frontier, iid)
                    else:
                        trace = await self._trace_header(conn, frontier, iid)
                    if trace is None:
                        file.close()
                        return None
                    inv = await self.queries.storage.row(conn, 'invocations', invocation_id=iid)
                    trace['invocation_version'] = inv['updated_store_order']
                    trace['source_id'] = store_id
                    events, changes = meta.tables['runtime_events'], meta.tables['graph_changes']
                    upper = base['manifest']['through_sequence'] if replay and base else through_sequence if replay and through_sequence is not None else (trace['last_event'] or {}).get('sequence', 0)
                    if replay:
                        # Initial state and all deltas share the original fixed
                        # boundary even if execution has continued since capture.
                        if base:
                            trace = base['manifest']['trace']
                        elif upper:
                            frame = await timeline._frame(conn, frontier, iid, sequence=upper)
                            if frame['trace'] is not None:
                                trace = dict(frame['trace'], last_event=frame['boundary'], observed_at_us=frame['boundary']['occurred_at_us'])
                        initial = dict(trace, invocation=dict(trace['invocation'], status='planned', control_state='active', last_sequence=0, completed_at_us=None),
                                       nodes=[], edges=[], active_routes=[], child_count=0, last_event=None)
                        first = (await conn.execute(select(events.c.event_name).where(events.c.invocation_id == iid,
                            events.c.sequence <= upper).order_by(events.c.sequence).limit(1))).scalar()
                        history_complete = first == 'invocation.started'
                        overview = base['manifest']['overview'] if base else await timeline._overview(conn, frontier, iid)
                    else:
                        initial, history_complete = None, None
                        overview = await timeline._overview(conn, frontier, iid) if previous is None else self._overview_header(trace)
                        trace['observed_at_us'] = overview['observed_at_us']
                        for kind in ('node', 'call', 'wait'):
                            spans = timeline.spans(kind, iid, after_order=previous)
                            statement = select(spans).order_by(spans.c.id)
                            stream = await conn.stream(statement)
                            async for rows in stream.mappings().partitions(PAGE_SIZE):
                                await write(kind, [timing_item(row, kind) for row in rows])
                        if previous is not None:
                            for rows in await self._edges(conn, iid, previous):
                                await write('edge', rows)
                    headers = select(events.c.event_id, events.c.event_name, events.c.sequence, events.c.occurred_at_us).where(
                        events.c.invocation_id == iid, events.c.sequence <= upper).order_by(events.c.sequence)
                    if not replay:
                        # Small control boundaries, never Runtime Event payloads.
                        # Scalar subqueries retain one header per Event even when
                        # the same commit changes several Invocation fields.
                        for field, label in (('control_state', 'control_state'), ('status', 'invocation_status')):
                            value = changes.c.change[field].as_string()
                            boundary = select(value).where(changes.c.event_id == events.c.event_id,
                                changes.c.entity_type == 'invocation', changes.c.entity_id == iid,
                                value.is_not(None)).order_by(changes.c.change_index.desc()).limit(1).scalar_subquery()
                            headers = headers.add_columns(boundary.label(label))
                    if previous is not None and not replay:
                        headers = headers.where(events.c.store_order > previous)
                    stream = await conn.stream(headers)
                    async for rows in stream.mappings().partitions(PAGE_SIZE):
                        batch = [dict(row) for row in rows]
                        if not replay:
                            batch = [{key: value for key, value in row.items() if value is not None} for row in batch]
                        if replay:
                            batch = list((await replay_events(conn, iid, batch)).values())
                        await write('event', batch)
                    manifest = dict(schema_version=2, source_id=store_id, invocation_id=iid, snapshot_id=uuid4().hex,
                        version=f"2:{(trace['last_event'] or {}).get('event_id', iid)}:{trace['frontier']}:{0 if trace['invocation']['status'] in ('completed','failed','cancelled') else trace['observed_at_us']}",
                        frontier=trace['frontier'], through_sequence=upper, trace=trace, overview=overview,
                        cursor=encode_trace_cursor(store_id, iid, frontier), base_cursor=cursor or (old['manifest']['cursor'] if old else None), delta=previous is not None and not replay,
                        initial_trace=initial, history_complete=history_complete, replay=replay,
                        base_version=old['manifest']['version'] if old else None,
                        sections={kind:dict(count=counts.get(kind,0), pages=len(offsets.get(kind,[]))) for kind in (('event',) if replay else ('node','call','wait','event', *(['edge'] if previous is not None else [])))})
                while self.entries and (self.bytes + size > self.config.trace_snapshot_bytes or len(self.entries) >= 32):
                    self._drop(next(iter(self.entries)))
                self.entries[manifest['snapshot_id']] = dict(file=file, offsets=offsets, bytes=size, manifest=manifest, used=time.monotonic())
                self.bytes += size
                return manifest
            except BaseException:
                file.close()
                raise

    async def _trace_header(self, conn, frontier, iid):
        inv = await self.queries.storage.row(conn, 'invocations', invocation_id=iid)
        if inv is None:
            return None
        definitions, events, invs = (meta.tables[k] for k in ('workflow_revisions', 'runtime_events', 'invocations'))
        revision = (await conn.execute(select(definitions.c.workflow_id, definitions.c.workflow_version).where(definitions.c.revision_id == inv['workflow_revision_id']))).mappings().one()
        last = (await conn.execute(select(events.c.event_id, events.c.event_name, events.c.sequence, events.c.occurred_at_us).where(events.c.invocation_id == iid).order_by(events.c.sequence.desc()).limit(1))).mappings().first()
        children = await conn.scalar(select(func.count()).select_from(invs).where(invs.c.parent_invocation_id == iid))
        return dict(invocation=summary(inv) | dict(revision), nodes=[], edges=[], active_routes=[], child_count=children, frontier=frontier, last_event=dict(last) if last else None, observed_at_us=time.time_ns() // 1000, view='durable')

    def _overview_header(self, trace):
        inv = trace['invocation']
        return dict(invocation_id=inv['invocation_id'], start_at_us=inv['started_at_us'], end_at_us=inv.get('completed_at_us') or trace['observed_at_us'], observed_at_us=trace['observed_at_us'], status=inv['status'], control_state=inv['control_state'], counts=dict(node=0, call=0, wait=0), node_start_bins=[], untimed_count=0, frontier=trace['frontier'], view='durable')

    async def _edges(self, conn, iid, previous):
        events, evaluations, changes = (meta.tables[k] for k in ('runtime_events', 'edge_evaluations', 'graph_changes'))
        counts = select(evaluations.c.edge_id, func.count().label('evaluations_delta')).join(events, evaluations.c.event_id == events.c.event_id).where(evaluations.c.invocation_id == iid, events.c.store_order > previous).group_by(evaluations.c.edge_id)
        rows = {r['edge_id']: dict(r) for r in (await conn.execute(counts)).mappings()}
        # Decisions include unconditional and skipped Edges; evaluation counts alone do not.
        edge_id = changes.c.change['edge_id'].as_string()
        ranked = select(edge_id.label('edge_id'), changes.c.change['selected'].as_boolean().label('condition_result'), func.row_number().over(partition_by=edge_id, order_by=(changes.c.store_order.desc(), changes.c.change_index.desc())).label('_rank')).where(changes.c.invocation_id == iid, changes.c.store_order > previous, changes.c.entity_type == 'edge', edge_id.is_not(None), changes.c.change['selected'].as_boolean().is_not(None)).subquery()
        for row in (await conn.execute(select(ranked).where(ranked.c._rank == 1))).mappings():
            rows.setdefault(row['edge_id'], dict(edge_id=row['edge_id'], evaluations_delta=0))['condition_result'] = row['condition_result']
        # Evaluations can precede the routing decision in a later commit.
        missing = [key for key, row in rows.items() if 'condition_result' not in row]
        for key in missing[:]:
            previous_decision = (await conn.execute(select(changes.c.change['selected'].as_boolean()).where(
                changes.c.invocation_id == iid, edge_id == key,
                changes.c.change['selected'].as_boolean().is_not(None)).order_by(changes.c.store_order.desc(), changes.c.change_index.desc()).limit(1))).first()
            if previous_decision is not None:
                rows[key]['condition_result'] = previous_decision[0]
                missing.remove(key)
        if missing:
            ranked = select(evaluations.c.edge_id, evaluations.c.condition_result, func.row_number().over(partition_by=evaluations.c.edge_id, order_by=(events.c.store_order.desc(), evaluations.c.result_index.desc())).label('_rank')).join(events, evaluations.c.event_id == events.c.event_id).where(evaluations.c.invocation_id == iid, events.c.store_order > previous, evaluations.c.edge_id.in_(missing)).subquery()
            for row in (await conn.execute(select(ranked).where(ranked.c._rank == 1))).mappings():
                rows[row['edge_id']]['condition_result'] = row['condition_result']
        values = list(rows.values())
        return [values[start:start + PAGE_SIZE] for start in range(0, len(values), PAGE_SIZE)]

    async def page(self, snapshot_id, kind, page):
        async with self.lock:
            self._expire()
            entry = self.entries.get(snapshot_id)
            if entry is None:
                raise ValueError('Trace snapshot expired. Refresh this execution.')
            positions = entry['offsets'].get(kind, [])
            if type(page) is not int or not 0 <= page < len(positions):
                raise ValueError('Invalid trace dataset page.')
            self.entries.move_to_end(snapshot_id)
            entry['used'] = time.monotonic()
            offset, length = positions[page]
            entry['file'].seek(offset)
            payload = await asyncio.to_thread(entry['file'].read, length)
            return dict(items=json.loads(payload), snapshot_id=snapshot_id, section=kind, page=page,
                        next_page=page+1 if page+1 < len(positions) else None)

"""Windowed Timeline metadata; Runtime Event bodies and values stay unread."""

import time

from sqlalchemy import Integer, and_, case, cast, func, or_, select, tuple_

from .models import Page
from .queries import limit_value
from .schema import meta
from .tracing import TraceQueries, summary, trace_cursor, node_call_counts

MAX_TIME = 9007199254740991
ACTIVE = ('ready', 'running', 'waiting')
OVERVIEW_BINS = 64
CALL_PREVIEW_LIMIT = 6


def timing_item(row, kind):
    value = {k: v for k, v in dict(row, kind=kind, values=[]).items() if not k.startswith('_')}
    if value.get('status') not in ACTIVE:
        value.pop('activations', None)
    return value


def overlaps(spans, kind, start_us, end_us):
    live = spans.c.status.in_(ACTIVE if kind == 'node' else ('running',) if kind == 'call' else ('waiting',))
    return and_(spans.c.start_at_us < end_us, or_(spans.c.end_at_us > start_us,
        and_(spans.c.end_at_us == spans.c.start_at_us, spans.c.start_at_us >= start_us),
        and_(spans.c.end_at_us.is_(None), or_(live, spans.c.start_at_us >= start_us))))


def time_window(start_us, end_us):
    if any(type(t) is not int or not 0 <= t <= MAX_TIME for t in (start_us, end_us)) or start_us >= end_us:
        raise ValueError('Expected an increasing window of integer microseconds.')


class TimelineQueries:
    def __init__(self, queries):
        self.queries, self.storage = queries, queries.storage

    async def replay(self, iid, **position):
        from .replay import replay_window
        return await replay_window(self, iid, **position)

    def spans(self, kind, iid, *, after_order=None):
        """Select timing columns explicitly, keeping references off Timeline pages."""
        if kind not in ('node', 'call', 'wait'):
            raise ValueError('Unknown Timeline item kind.')
        t = meta.tables[{'node': 'node_executions', 'call': 'operator_calls', 'wait': 'waits'}[kind]]
        common = [t.c.invocation_id, t.c.occurrence_id, t.c.status]
        if kind == 'node':
            calls = meta.tables['operator_calls']
            waits = meta.tables['waits']
            call_count = select(func.count()).where(calls.c.invocation_id == iid,
                calls.c.occurrence_id == t.c.occurrence_id).correlate(t).scalar_subquery()
            wait_count = select(func.count()).where(waits.c.invocation_id == iid,
                waits.c.occurrence_id == t.c.occurrence_id).correlate(t).scalar_subquery()
            begin = func.coalesce(t.c.started_at_us, t.c.ready_at_us, t.c.completed_at_us)
            end, identity = t.c.completed_at_us, t.c.occurrence_id
            order = func.coalesce(t.c.ready_at_us, t.c.started_at_us, t.c.completed_at_us)
            columns = [t.c.node_id, t.c.activations, t.c.ready_at_us, t.c.started_at_us, t.c.completed_at_us,
                       t.c.error_type, t.c.error_code, call_count.label('call_count'), wait_count.label('wait_count'),
                       t.c.ordinal]
        elif kind == 'call':
            begin, end, identity = t.c.started_at_us, t.c.completed_at_us, t.c.call_id
            order = begin
            columns = [identity, t.c.operator_id, t.c.unit_index, t.c.started_at_us, t.c.completed_at_us,
                       t.c.queue_duration_ns, t.c.execution_duration_ns, t.c.error_type, t.c.error_code]
        else:
            begin, identity, order = t.c.created_at_us, t.c.wait_id, t.c.created_at_us
            g = meta.tables['graph_changes']
            # A cancelled Wait has no resumed_at_us. Its cancellation boundary
            # is recorded in the lightweight history, not the Invocation end.
            cancelled = select(func.max(g.c.occurred_at_us)).where(
                g.c.invocation_id == iid, g.c.entity_type == 'wait', g.c.entity_id == identity,
                g.c.change['status'].as_string() == 'cancelled',
            ).correlate(t).scalar_subquery()
            end = case((t.c.status == 'cancelled', cancelled), else_=t.c.resumed_at_us)
            columns = [identity, t.c.kind.label('wait_kind'), t.c.created_at_us, t.c.resumed_at_us, t.c.deadline_at_us]
        statement = select(*common, *columns, identity.label('id'), begin.label('start_at_us'),
                           end.label('end_at_us'), order.label('_order')).where(t.c.invocation_id == iid)
        if after_order is not None:
            statement = statement.where(t.c.updated_store_order > after_order)
        return statement.subquery()

    async def overview(self, iid):
        async with self.queries.view() as (conn, _, _, frontier):
            return await self._overview(conn, frontier, iid)

    async def _overview(self, conn, frontier, iid):
        t = meta.tables['invocations']
        inv = (await conn.execute(select(t.c.invocation_id, t.c.started_at_us, t.c.completed_at_us,
            t.c.status, t.c.control_state).where(t.c.invocation_id == iid))).mappings().first()
        if inv is None:
            return None
        starts, ends, counts, untimed = [], [], {}, 0
        for kind in ('node', 'call', 'wait'):
            spans = self.spans(kind, iid)
            statement = select(func.min(spans.c.start_at_us),
                func.max(func.coalesce(spans.c.end_at_us, spans.c.start_at_us)),
                func.count(), func.count(spans.c.start_at_us))
            if kind == 'node':
                statement = statement.where(spans.c.status != 'skipped')
            row = (await conn.execute(statement)).one()
            counts[kind] = row[2]
            untimed += row[2] - row[3]
            if row[0] is not None:
                starts.append(row[0])
            if row[1] is not None:
                ends.append(row[1])
        start = inv['started_at_us'] if inv['started_at_us'] is not None else min(starts, default=None)
        observed = time.time_ns() // 1000
        terminal = inv['status'] in ('completed', 'failed', 'cancelled')
        end = max([*ends, *([inv['completed_at_us']] if inv['completed_at_us'] is not None else []),
                   *([observed] if not terminal and start is not None else [])], default=start)
        bins = [0] * OVERVIEW_BINS
        if start is not None:
            nodes = meta.tables['node_executions']
            begin = func.coalesce(nodes.c.started_at_us, nodes.c.ready_at_us, nodes.c.completed_at_us)
            # Fixed-size SQL aggregation covers the whole history, including
            # Nodes outside the current page. No Event or value is loaded.
            bucket = cast(func.floor((begin - start) * 1.0 * OVERVIEW_BINS / max(1, end - start)), Integer)
            histogram = select(bucket.label('bucket'), func.count().label('count')).where(
                nodes.c.invocation_id == iid, nodes.c.status != 'skipped', begin.is_not(None),
            ).group_by(bucket)
            for row in (await conn.execute(histogram)).mappings():
                bins[max(0, min(OVERVIEW_BINS - 1, row['bucket']))] += row['count']
        return dict(inv, start_at_us=start, end_at_us=end, observed_at_us=observed,
                    counts=counts, node_start_bins=bins, untimed_count=untimed, frontier=frontier, view='durable')

    async def attach_waits(self, conn, iid, calls):
        """Batch this page's suspended Call identities; business values stay unread."""
        # Core uses the suspended command's Call ID as its Wait ID.
        ids = [row['id'] for row in calls if row['operator_id'].startswith('system_command:')]
        waits = {}
        if ids:
            spans = self.spans('wait', iid)
            for row in (await conn.execute(select(spans).where(spans.c.id.in_(ids)))).mappings():
                waits[row['id']] = timing_item(row, 'wait')
        for call in calls:
            wait = waits.get(call['id'])
            call['wait'] = wait if wait and wait['occurrence_id'] == call['occurrence_id'] else None

    async def page(self, iid, *, kind='node', start_us, end_us, occurrence_id=None, node_id=None, limit=50, cursor=None):
        time_window(start_us, end_us)
        limit_value(limit)
        trace_cursor(cursor)
        if node_id is not None and (kind != 'node' or not isinstance(node_id, str) or not 0 < len(node_id) <= 4096):
            raise ValueError('A Node filter is only valid for Node pages.')
        if kind != 'node' and not occurrence_id:
            raise ValueError('A Node Occurrence is required for Call and Wait pages.')
        spans = self.spans(kind, iid)
        async with self.queries.view() as (conn, _, _, frontier):
            invs = meta.tables['invocations']
            if await conn.scalar(select(invs.c.invocation_id).where(invs.c.invocation_id == iid)) is None:
                return None
            if occurrence_id is not None:
                nodes = meta.tables['node_executions']
                if await conn.scalar(select(nodes.c.occurrence_id).where(nodes.c.invocation_id == iid, nodes.c.occurrence_id == occurrence_id)) is None:
                    return None
            # Half-open windows include crossing spans and instantaneous records,
            # but exclude a completed span ending exactly at the left boundary.
            predicates = [overlaps(spans, kind, start_us, end_us)]
            if kind == 'node':
                predicates.append(spans.c.status != 'skipped')
            if occurrence_id is not None:
                predicates.append(spans.c.occurrence_id == occurrence_id)
            if node_id is not None:
                predicates.append(spans.c.node_id == node_id)
            if cursor is not None:
                predicates.append(tuple_(spans.c._order, spans.c.id) > tuple(cursor))
            rows = (await conn.execute(select(spans).where(*predicates)
                .order_by(spans.c._order, spans.c.id).limit(limit + 1))).mappings().all()
            position = (rows[limit - 1]['_order'], rows[limit - 1]['id']) if len(rows) > limit else None
            items = tuple(timing_item(row, kind) for row in rows[:limit])
            if kind == 'call':
                await self.attach_waits(conn, iid, items)
            elif kind == 'node' and items:
                calls = self.spans('call', iid)
                ranked = select(calls, func.row_number().over(partition_by=calls.c.occurrence_id,
                    order_by=(calls.c._order, calls.c.id)).label('_rank')).where(
                        calls.c.occurrence_id.in_([row['id'] for row in items]),
                        overlaps(calls, 'call', start_us, end_us),
                    ).subquery()
                previews = [timing_item(row, 'call') for row in (await conn.execute(select(ranked)
                    .where(ranked.c._rank <= CALL_PREVIEW_LIMIT).order_by(ranked.c._order, ranked.c.id))).mappings()]
                await self.attach_waits(conn, iid, previews)
                by_node = {row['id']: [] for row in items}
                for call in previews:
                    by_node[call['occurrence_id']].append(call)
                for row in items:
                    row['preview_calls'] = by_node[row['id']]
            return Page(items, position, frontier)

    async def position(self, iid, kind, identity, *, start_us, end_us, occurrence_id=None,
                       node_id=None, limit=50, page_index=None):
        """Seek an exact interval's page with SQL; never read preceding pages into memory."""
        time_window(start_us, end_us)
        limit_value(limit)
        if kind not in ('node', 'call') or kind == 'call' and not occurrence_id:
            raise ValueError('Expected a Node interval or a Call with its Occurrence.')
        if node_id is not None and kind != 'node':
            raise ValueError('A Node filter is only valid for Node pages.')
        if page_index is not None and (type(page_index) is not int or not 0 <= page_index <= MAX_TIME):
            raise ValueError('Invalid page index.')
        spans = self.spans(kind, iid)
        predicates = [overlaps(spans, kind, start_us, end_us)]
        if kind == 'node':
            predicates.append(spans.c.status != 'skipped')
        if occurrence_id is not None:
            predicates.append(spans.c.occurrence_id == occurrence_id)
        if node_id is not None:
            predicates.append(spans.c.node_id == node_id)
        async with self.queries.view() as (conn, _, _, frontier):
            target = (await conn.execute(select(spans.c._order, spans.c.id).where(
                *predicates, spans.c.id == identity))).first()
            if target is None:
                return None
            if page_index is None:
                preceding = await conn.scalar(select(func.count()).select_from(spans).where(
                    *predicates, tuple_(spans.c._order, spans.c.id) < tuple(target)))
                index = preceding // limit
            else:
                count = await conn.scalar(select(func.count()).select_from(spans).where(*predicates))
                if page_index * limit >= count:
                    raise ValueError('Page is outside the available intervals.')
                index = page_index
            cursor = None
            if index:
                before = (await conn.execute(select(spans.c._order, spans.c.id).where(*predicates)
                    .order_by(spans.c._order, spans.c.id).offset(index * limit - 1).limit(1))).first()
                if before is None:
                    raise ValueError('Page is outside the available intervals.')
                cursor = tuple(before)
            return dict(cursor=cursor, page_index=index, frontier=frontier)

    async def event_groups(self, iid, *, start_us, end_us, buckets=100, through_sequence=None):
        """Aggregate every Event header into a bounded time rail, with exact counts."""
        time_window(start_us, end_us)
        if type(buckets) is not int or not 1 <= buckets <= 512:
            raise ValueError('Expected 1 to 512 Event buckets.')
        if through_sequence is not None and (type(through_sequence) is not int or not 0 <= through_sequence <= MAX_TIME):
            raise ValueError('Invalid Event frontier.')
        e = meta.tables['runtime_events']
        bucket = cast(func.floor((e.c.occurred_at_us - start_us) * 1.0 * buckets / (end_us - start_us)), Integer)
        statement = select(bucket.label('bucket'), e.c.event_name, func.count().label('count'),
            func.avg(e.c.occurred_at_us - start_us).label('mean_offset'),
            func.min(e.c.event_id).label('event_id'), func.min(e.c.sequence).label('first_sequence'),
            func.max(e.c.sequence).label('last_sequence'), func.min(e.c.occurred_at_us).label('first_time'),
            func.max(e.c.occurred_at_us).label('last_time')).where(e.c.invocation_id == iid,
            e.c.occurred_at_us >= start_us, e.c.occurred_at_us < end_us)
        if through_sequence is not None:
            statement = statement.where(e.c.sequence <= through_sequence)
        async with self.queries.view() as (conn, _, _, frontier):
            if await self.storage.row(conn, 'invocations', invocation_id=iid) is None:
                return None
            rows = (await conn.execute(statement.group_by(bucket, e.c.event_name).order_by(bucket))).mappings()
            groups = {}
            for row in rows:
                group = groups.setdefault(row['bucket'], dict(count=0, names={}, offset_sum=0,
                    start_us=row['first_time'], end_us=row['last_time'] + 1,
                    first_sequence=row['first_sequence'], last_sequence=row['last_sequence'], marker=None))
                group['count'] += row['count']
                group['names'][row['event_name']] = row['count']
                group['offset_sum'] += row['mean_offset'] * row['count']
                group['start_us'] = min(group['start_us'], row['first_time'])
                group['end_us'] = max(group['end_us'], row['last_time'] + 1)
                group['first_sequence'] = min(group['first_sequence'], row['first_sequence'])
                group['last_sequence'] = max(group['last_sequence'], row['last_sequence'])
                group['marker'] = dict(event_id=row['event_id'], event_name=row['event_name'],
                    sequence=row['first_sequence'], occurred_at_us=row['first_time']) if group['count'] == 1 else None
            items = []
            for group in groups.values():
                group['occurred_at_us'] = start_us + group.pop('offset_sum') / group['count']
                items.append(group)
            return dict(items=items, total_count=sum(group['count'] for group in items), frontier=frontier)

    async def markers(self, iid, *, start_us, end_us, through_sequence=None, limit=100, cursor=None):
        """Page Event headers only; no Event payload, delta or Artifact is read."""
        time_window(start_us, end_us)
        limit_value(limit)
        trace_cursor(cursor)
        events = meta.tables['runtime_events']
        statement = select(events.c.event_id, events.c.event_name, events.c.sequence,
            events.c.occurred_at_us).where(events.c.invocation_id == iid,
            events.c.occurred_at_us >= start_us, events.c.occurred_at_us < end_us)
        if through_sequence is not None:
            if type(through_sequence) is not int or not 0 <= through_sequence <= MAX_TIME:
                raise ValueError('Invalid Event frontier.')
            statement = statement.where(events.c.sequence <= through_sequence)
        if cursor is not None:
            statement = statement.where(tuple_(events.c.sequence, events.c.event_id) > tuple(cursor))
        async with self.queries.view() as (conn, _, _, frontier):
            if await conn.scalar(select(meta.tables['invocations'].c.invocation_id).where(
                meta.tables['invocations'].c.invocation_id == iid)) is None:
                return None
            rows = (await conn.execute(statement.order_by(events.c.sequence, events.c.event_id).limit(limit + 1))).mappings().all()
            position = (rows[limit - 1]['sequence'], rows[limit - 1]['event_id']) if len(rows) > limit else None
            return Page(tuple(dict(row) for row in rows[:limit]), position, frontier)

    async def frame(self, iid, *, sequence=None, at_us=None, direction=0):
        """Resolve a read-only Graph frame with SQL over lightweight changes."""
        for value in (sequence, at_us):
            if value is not None and (type(value) is not int or not 0 <= value <= MAX_TIME):
                raise ValueError('Invalid Timeline position.')
        if sequence is not None and at_us is not None or direction not in (-1, 0, 1) or direction and sequence is None:
            raise ValueError('Expected one Timeline position and a valid direction.')
        async with self.queries.view() as (conn, _, _, frontier):
            return await self._frame(conn, frontier, iid, sequence=sequence, at_us=at_us, direction=direction)

    async def _frame(self, conn, frontier, iid, *, sequence=None, at_us=None, direction=0):
        e, g, n, inv = (meta.tables[name] for name in ('runtime_events', 'graph_changes', 'node_executions', 'invocations'))
        record = (await conn.execute(select(*(inv.c[name] for name in (
            'invocation_id', 'session_id', 'workflow_revision_id', 'root_invocation_id',
            'parent_invocation_id', 'entry_node_id', 'started_at_us'))).where(inv.c.invocation_id == iid))).mappings().first()
        if record is None:
            return None
        headers = select(e.c.event_id, e.c.event_name, e.c.sequence, e.c.occurred_at_us).where(e.c.invocation_id == iid)
        first = (await conn.execute(headers.order_by(e.c.sequence).limit(1))).mappings().first()
        last = (await conn.execute(headers.order_by(e.c.sequence.desc()).limit(1))).mappings().first()
        if first is None or first['event_name'] != 'invocation.started':
            return {'history_complete': False, 'trace': None, 'boundary': None, 'previous': None, 'next': None, 'first': None, 'last': dict(last) if last else None, 'frontier': frontier}
        target = headers
        if sequence is not None:
            target = target.where(e.c.sequence > sequence if direction == 1 else e.c.sequence < sequence if direction == -1 else e.c.sequence <= sequence)
        if at_us is not None:
            target = target.where(e.c.occurred_at_us <= at_us)
        target = target.order_by(e.c.sequence if direction == 1 else e.c.sequence.desc()).limit(1)
        boundary = (await conn.execute(target)).mappings().first() or (first if not direction else last if direction == 1 else first)
        seq = boundary['sequence']
        previous = (await conn.execute(headers.where(e.c.sequence < seq).order_by(e.c.sequence.desc()).limit(1))).mappings().first()
        following = (await conn.execute(headers.where(e.c.sequence > seq).order_by(e.c.sequence).limit(1))).mappings().first()

        # Pick the latest non-null status for each Occurrence at this boundary.
        status = g.c.change['status'].as_string()
        ranked = select(g.c.entity_id.label('occurrence_id'), status.label('status'),
            func.row_number().over(partition_by=g.c.entity_id, order_by=(g.c.sequence.desc(), g.c.change_index.desc())).label('_rank'),
            func.min(g.c.sequence).over(partition_by=g.c.entity_id).label('_created'),
        ).where(g.c.invocation_id == iid, g.c.sequence <= seq,
            g.c.entity_type == 'node', status.is_not(None)).subquery()
        occurrences = select(n.c.node_id, ranked.c.occurrence_id, ranked.c.status, ranked.c._created).select_from(
            ranked.join(n, and_(n.c.invocation_id == iid, n.c.occurrence_id == ranked.c.occurrence_id))
        ).where(ranked.c._rank == 1).subquery()
        grouped = select(occurrences.c.node_id, occurrences.c.status, occurrences.c.occurrence_id,
            func.count().over(partition_by=occurrences.c.node_id).label('occurrences'),
            func.row_number().over(partition_by=occurrences.c.node_id, order_by=(
                case((occurrences.c.status == 'running', 0), (occurrences.c.status == 'waiting', 1),
                    (occurrences.c.status == 'ready', 2), else_=3), occurrences.c._created.desc(), occurrences.c.occurrence_id.desc())).label('_rank')
        ).subquery()
        node_states = [dict(row) for row in (await conn.execute(select(*(c for c in grouped.c if c.name != '_rank')).where(grouped.c._rank == 1))).mappings()]
        call_counts = await node_call_counts(conn, iid, seq)
        for row in node_states:
            row['call_count'] = call_counts.get(row['node_id'], 0)
        invocation = dict(record, status='running', control_state='active', completed_at_us=None, last_sequence=seq, values=[])
        for field in ('status', 'control_state'):
            value = g.c.change[field].as_string()
            found = (await conn.execute(select(value.label('value'), g.c.occurred_at_us).where(g.c.invocation_id == iid, g.c.entity_type == 'invocation',
                g.c.entity_id == iid, g.c.sequence <= seq, value.is_not(None)).order_by(g.c.sequence.desc(), g.c.change_index.desc()).limit(1))
            ).mappings().first()
            if found is not None:
                invocation[field] = found['value']
                if field == 'status' and found['value'] in ('completed', 'failed', 'cancelled'):
                    invocation['completed_at_us'] = found['occurred_at_us']
        edge_id, selected = g.c.change['edge_id'].as_string(), g.c.change['selected'].as_boolean()
        decisions = select(edge_id.label('edge_id'), selected.label('condition_result'),
            func.row_number().over(partition_by=edge_id, order_by=(g.c.sequence.desc(), g.c.change_index.desc())).label('_rank')
        ).where(g.c.invocation_id == iid, g.c.sequence <= seq, g.c.entity_type == 'edge', edge_id.is_not(None), selected.is_not(None)).subquery()
        evaluations, events = meta.tables['edge_evaluations'], meta.tables['runtime_events']
        ranked_evaluations = select(*(evaluations.c[name] for name in ('edge_id', 'condition_result',
            'event_id', 'result_index', 'source_occurrence_id', 'source_status', 'duration_ns')),
            events.c.sequence, events.c.occurred_at_us,
            func.count().over(partition_by=evaluations.c.edge_id).label('evaluations'),
            func.row_number().over(partition_by=evaluations.c.edge_id,
                order_by=(events.c.sequence.desc(), evaluations.c.result_index.desc())).label('_rank')
        ).join(events, evaluations.c.event_id == events.c.event_id).where(
            evaluations.c.invocation_id == iid, events.c.sequence <= seq).subquery()
        by_edge = {}
        for row in (await conn.execute(select(*(c for c in ranked_evaluations.c if c.name != '_rank')).where(
            ranked_evaluations.c._rank == 1))).mappings():
            by_edge[row['edge_id']] = {'edge_id': row['edge_id'], 'condition_result': row['condition_result'],
                'evaluations': row['evaluations'], 'evaluation': dict(row, invocation_id=iid, values=[])}
        for row in (await conn.execute(select(decisions.c.edge_id, decisions.c.condition_result).where(decisions.c._rank == 1))).mappings():
            by_edge.setdefault(row['edge_id'], {'edge_id': row['edge_id'], 'evaluations': 0}).update(condition_result=row['condition_result'])
        edge_states = list(by_edge.values())
        created, activated = g.alias('created'), g.alias('activated')
        routes = select(activated.c.change['edge_id'].as_string().label('edge_id'),
            activated.c.change['source_occurrence_id'].as_string().label('source_occurrence_id'),
            occurrences.c.node_id.label('target_node_id'), occurrences.c.occurrence_id.label('target_occurrence_id'), occurrences.c.status,
        ).select_from(created.join(activated, and_(created.c.event_id == activated.c.event_id,
            activated.c.entity_type == 'edge', activated.c.change['selected'].as_boolean().is_(True),
            activated.c.change['target_node_id'].as_string() == created.c.change['node_id'].as_string(),
            activated.c.change['target_scope'] == created.c.change['scope'])).join(occurrences,
                occurrences.c.occurrence_id == created.c.entity_id)).where(created.c.invocation_id == iid,
            created.c.entity_type == 'node', created.c.sequence <= seq, occurrences.c.status.in_(ACTIVE),
            activated.c.change['source_occurrence_id'].as_string().is_not(None)).distinct()
        defs = meta.tables['workflow_revisions']
        revision = (await conn.execute(select(defs.c.workflow_id, defs.c.workflow_version).where(
            defs.c.revision_id == record['workflow_revision_id']))).mappings().one()
        invocation.update(revision)
        children = await conn.scalar(select(func.count(func.distinct(g.c.entity_id))).where(
            g.c.invocation_id == iid, g.c.sequence <= seq, g.c.entity_type == 'invocation',
            g.c.change['parent_invocation_id'].as_string() == iid))
        trace = dict(invocation=invocation, nodes=node_states, edges=edge_states,
            active_routes=[dict(row) for row in (await conn.execute(routes)).mappings()],
            child_count=children, frontier=frontier, view='durable')
        return dict(history_complete=True, trace=trace, boundary=dict(boundary),
            previous=dict(previous) if previous else None, next=dict(following) if following else None,
            first=dict(first), last=dict(last), frontier=frontier)

    async def record(self, iid, kind, identity):
        """Read one selected object's references; never resolve business values."""
        if kind == 'edge':
            try:
                event_id, index = identity.rsplit(':', 1)
                if not event_id or not index.isdecimal() or len(index) > 10:
                    raise ValueError
                index = int(index)
                if index > 2147483647:
                    raise ValueError
            except (ValueError, AttributeError):
                raise ValueError('Invalid Edge evaluation identity.') from None
            edges, events, nodes = (meta.tables[n] for n in ('edge_evaluations', 'runtime_events', 'node_executions'))
            async with self.queries.view() as (conn, _, _, _):
                row = (await conn.execute(select(*edges.c, events.c.sequence, events.c.occurred_at_us,
                    nodes.c.node_id).join(events, edges.c.event_id == events.c.event_id)
                    .join(nodes, and_(nodes.c.invocation_id == iid, nodes.c.occurrence_id == edges.c.source_occurrence_id))
                    .where(edges.c.invocation_id == iid, edges.c.event_id == event_id, edges.c.result_index == index))).mappings().first()
                if row is None:
                    return None
                result = summary(row)
                result['ordinal'] = 1 + await conn.scalar(select(func.count()).select_from(edges.join(events, edges.c.event_id == events.c.event_id)).where(
                    edges.c.invocation_id == iid, edges.c.edge_id == row['edge_id'],
                    tuple_(events.c.sequence, edges.c.result_index) < (row['sequence'], row['result_index'])))
                return result
        if kind not in ('node', 'call', 'wait'):
            raise ValueError('Unknown execution record kind.')
        name, key = {'node': ('node_executions', 'occurrence_id'),
                     'call': ('operator_calls', 'call_id'), 'wait': ('waits', 'wait_id')}[kind]
        async with self.queries.view() as (conn, _, _, _):
            row = await self.storage.row(conn, name, invocation_id=iid, **{key: identity})
            if row is None:
                return None
            if kind == 'node':
                result = (await TraceQueries(self.queries)._node_summaries(conn, [row]))[0]
                spans = self.spans('node', iid)
                result['ordinal'] = await conn.scalar(select(spans.c.ordinal).where(spans.c.id == identity))
                return result
            result = summary(row)
            nodes = meta.tables['node_executions']
            result['node_id'] = await conn.scalar(select(nodes.c.node_id).where(
                nodes.c.invocation_id == iid, nodes.c.occurrence_id == row['occurrence_id']))
            if kind == 'wait':
                spans = self.spans('wait', iid)
                result['end_at_us'] = await conn.scalar(select(spans.c.end_at_us).where(spans.c.id == identity))
            return result

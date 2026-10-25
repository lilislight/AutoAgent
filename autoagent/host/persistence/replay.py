"""Atomic Graph replay windows without Event bodies or business values."""
import json
from sqlalchemy import and_, func, or_, select
from .queries import limit_value
from .schema import meta


async def replay_window(timeline, iid, *, sequence=None, at_us=None, through_sequence=None, limit=200):
    limit_value(limit)
    if any(v is not None and (type(v) is not int or not 0 <= v <= 9007199254740991)
           for v in (sequence, at_us, through_sequence)) or sequence is not None and at_us is not None:
        raise ValueError('Expected one valid Replay position.')
    e, g, n, evaluations = (meta.tables[k] for k in ('runtime_events', 'graph_changes', 'node_executions', 'edge_evaluations'))
    async with timeline.queries.view() as (conn, _, _, frontier):
        if await timeline.storage.row(conn, 'invocations', invocation_id=iid) is None:
            return None
        headers = select(e.c.event_id, e.c.event_name, e.c.sequence, e.c.occurred_at_us).where(e.c.invocation_id == iid)
        if through_sequence is not None:
            headers = headers.where(e.c.sequence <= through_sequence)
        first = (await conn.execute(headers.order_by(e.c.sequence).limit(1))).mappings().first()
        last = (await conn.execute(headers.order_by(e.c.sequence.desc()).limit(1))).mappings().first()
        if first is None or first['event_name'] not in ('invocation.started','invocation.forked'):
            return dict(history_complete=False, baseline=None, events=[], boundary=None, previous=None, next=None, time_from_us=0, time_until_us=None,
                        first=None, last=dict(last) if last else None, frontier=frontier)
        target = headers
        if sequence is not None:
            target = target.where(e.c.sequence <= sequence)
        if at_us is not None:
            target = target.where(e.c.occurred_at_us <= at_us)
        boundary = (await conn.execute(target.order_by(e.c.sequence.desc()).limit(1))).mappings().first() or first
        before = (await conn.execute(headers.where(e.c.sequence < boundary['sequence']).order_by(e.c.sequence.desc()).limit(limit // 2))).mappings().all()
        start = before[-1]['sequence'] if before else boundary['sequence']
        page = (await conn.execute(headers.where(e.c.sequence >= start).order_by(e.c.sequence).limit(limit))).mappings().all()
        previous = (await conn.execute(headers.where(e.c.sequence < start).order_by(e.c.sequence.desc()).limit(1))).mappings().first()
        following = (await conn.execute(headers.where(e.c.sequence > page[-1]['sequence']).order_by(e.c.sequence).limit(1))).mappings().first()
        base_seq = previous['sequence'] if previous else 0
        frame = await timeline._frame(conn, frontier, iid, sequence=base_seq)
        trace = frame['trace']
        if previous is None:
            trace = dict(trace, invocation=dict(trace['invocation'], status='planned', control_state='active', last_sequence=0, completed_at_us=None),
                         nodes=[], edges=[], active_routes=[], child_count=0)
        trace.update(observed_at_us=previous['occurred_at_us'] if previous else first['occurred_at_us'], last_event=dict(previous) if previous else None)
        ids = [r['event_id'] for r in page]
        touched = set((await conn.execute(select(g.c.entity_id).where(g.c.invocation_id == iid, g.c.event_id.in_(ids), g.c.entity_type == 'node'))).scalars())
        status = g.c.change['status'].as_string()
        ranked = select(g.c.entity_id.label('occurrence_id'), status.label('status'),
            func.min(g.c.sequence).over(partition_by=g.c.entity_id).label('created_sequence'),
            func.row_number().over(partition_by=g.c.entity_id, order_by=(g.c.sequence.desc(), g.c.change_index.desc())).label('_rank')
        ).where(g.c.invocation_id == iid, g.c.entity_type == 'node', g.c.sequence <= base_seq, status.is_not(None)).subquery()
        retained = touched | {r['occurrence_id'] for r in trace['nodes']}
        terminal = select(ranked.c.occurrence_id, func.row_number().over(partition_by=n.c.node_id,
            order_by=(ranked.c.created_sequence.desc(), ranked.c.occurrence_id.desc())).label('_terminal_rank')).join(n,
            and_(n.c.invocation_id == iid, n.c.occurrence_id == ranked.c.occurrence_id)).where(ranked.c._rank == 1,
            ranked.c.status.not_in(('ready', 'running', 'waiting'))).subquery()
        latest_terminal = select(terminal.c.occurrence_id).where(terminal.c._terminal_rank == 1)
        initial = select(ranked.c.occurrence_id, n.c.node_id, ranked.c.status, ranked.c.created_sequence).join(n,
            and_(n.c.invocation_id == iid, n.c.occurrence_id == ranked.c.occurrence_id)).where(ranked.c._rank == 1,
            or_(ranked.c.status.in_(('ready', 'running', 'waiting')), ranked.c.occurrence_id.in_(retained), ranked.c.occurrence_id.in_(latest_terminal)))
        occurrences = [dict(r) for r in (await conn.execute(initial)).mappings()]
        from .tracing import node_call_counts
        counts = await node_call_counts(conn, iid, base_seq, initial.with_only_columns(ranked.c.occurrence_id))
        for row in occurrences:
            row['call_count'] = counts.get(row['occurrence_id'], 0)
        batches = await replay_events(conn, iid, page, timeline.storage)
        children = {child for batch in batches.values() for child in batch['child_ids']}
        known = list((await conn.execute(select(g.c.entity_id).where(g.c.invocation_id == iid, g.c.sequence <= base_seq,
            g.c.entity_type == 'invocation', g.c.entity_id.in_(children), g.c.change['parent_invocation_id'].as_string() == iid).distinct())).scalars()) if children else []
        future_min = await conn.scalar(select(func.min(e.c.occurred_at_us)).where(e.c.invocation_id == iid,
            e.c.sequence > page[-1]['sequence'], e.c.sequence <= last['sequence']))
        return dict(history_complete=True, baseline=dict(trace=trace, occurrences=occurrences, child_ids=known), events=list(batches.values()),
            time_from_us=min(row['occurred_at_us'] for row in page), time_until_us=future_min,
            boundary=dict(boundary), previous=dict(previous) if previous else None, next=dict(following) if following else None,
            first=dict(first), last=dict(last), frontier=frontier)


async def replay_events(conn, iid, page, storage):
    """Export only the changes needed to render Graph history."""
    e, g, n, evaluations = (meta.tables[k] for k in ('runtime_events', 'graph_changes', 'node_executions', 'edge_evaluations'))
    ids = [r['event_id'] for r in page]
    changes = (await conn.execute(select(g.c.event_id, g.c.entity_type, g.c.entity_id, g.c.change).where(g.c.invocation_id == iid,
        g.c.event_id.in_(ids)).order_by(g.c.sequence, g.c.change_index))).mappings().all()
    touched = {r['entity_id'] for r in changes if r['entity_type'] == 'node'}
    names = dict((await conn.execute(select(n.c.occurrence_id, n.c.node_id).where(n.c.invocation_id == iid, n.c.occurrence_id.in_(touched)))).all()) if touched else {}
    batches = {r['event_id']: dict(r, changes=[], evaluations=[], routes=[], child_ids=[], call_starts=[]) for r in page}
    from .tracing import edge_states
    for event in page:
        if event['event_name'] in ('invocation.forked','fork.state_imported'):
            batches[event['event_id']]['initial_edges'] = await edge_states(storage,conn,iid,sequence=event['sequence'])
    calls = meta.tables['operator_calls']
    started_event = calls.c.input_ref['id'].as_string()
    starts = select(started_event.label('event_id'), n.c.node_id, n.c.occurrence_id, func.count().label('count')).select_from(
        calls.join(n, and_(n.c.invocation_id == iid, n.c.occurrence_id == calls.c.occurrence_id))).where(
        calls.c.invocation_id == iid, calls.c.input_ref['kind'].as_string() == 'runtime_event',
        started_event.in_(ids)).group_by(started_event, n.c.node_id, n.c.occurrence_id)
    for row in (await conn.execute(starts)).mappings():
        batches[row['event_id']]['call_starts'].append(dict(node_id=row['node_id'], occurrence_id=row['occurrence_id'], count=row['count']))
    raw_by_event = {key: [] for key in batches}
    for row in changes:
        value, kind, batch = row['change'], row['entity_type'], batches[row['event_id']]
        raw_by_event[row['event_id']].append(row)
        if kind == 'invocation' and row['entity_id'] != iid:
            if value.get('parent_invocation_id') == iid:
                batch['child_ids'].append(row['entity_id'])
            continue
        allowed = {'invocation': ('status', 'control_state'), 'node': ('status', 'node_id'), 'edge': ('edge_id', 'selected')}.get(kind, ())
        light = {key: value[key] for key in allowed if key in value}
        if light:
            batch['changes'].append(dict(entity_type=kind, entity_id=row['entity_id'], change=light, node_id=names.get(row['entity_id']) if kind == 'node' else None))
    for row in (await conn.execute(select(evaluations.c.event_id, evaluations.c.edge_id, evaluations.c.condition_result)
        .where(evaluations.c.invocation_id == iid, evaluations.c.event_id.in_(ids)).order_by(evaluations.c.result_index))).mappings():
        batches[row['event_id']]['evaluations'].append(dict(edge_id=row['edge_id'], condition_result=row['condition_result']))
    for event_id, batch in batches.items():
        raw = raw_by_event[event_id]
        created = {(r['change']['node_id'], json.dumps(r['change'].get('scope', []), sort_keys=True)): r['entity_id']
                   for r in raw if r['entity_type'] == 'node' and 'node_id' in r['change']}
        for row in raw:
            value = row['change']
            if row['entity_type'] != 'edge' or not value.get('selected') or 'target_scope' not in value:
                continue
            target_id = created.get((value.get('target_node_id'), json.dumps(value['target_scope'], sort_keys=True)))
            if target_id and value.get('source_occurrence_id'):
                batch['routes'].append(dict(edge_id=value['edge_id'], source_occurrence_id=value['source_occurrence_id'], target_node_id=value['target_node_id'], target_occurrence_id=target_id))

    return batches

"""Bounded durable tracing queries; business values are read separately."""

import json
import time
from collections import Counter, defaultdict

from sqlalchemy import JSON, Text, and_, case, cast, func, literal, or_, select, tuple_, type_coerce

from .models import Page
from .queries import limit_value
from .reading import SourceReader, pointer_tokens
from .schema import meta


# Public summaries intentionally whitelist metadata, independent of table growth.
SUMMARY_FIELDS = frozenset({
    "invocation_id", "session_id", "workflow_id", "workflow_version", "workflow_revision_id",
    "root_invocation_id", "parent_invocation_id", "parent_occurrence_id", "entry_node_id",
    "status", "control_state", "started_at_us", "completed_at_us", "last_sequence",
    "occurrence_id", "node_id", "scope", "phase", "ready_at_us", "recovery_attempts",
    "call_id", "operator_id", "operator_definition_id", "unit_index", "queue_duration_ns",
    "execution_duration_ns", "error_type", "error_code", "error_phase", "wait_id", "kind",
    "created_sequence", "created_at_us", "resumed_at_us", "deadline_at_us", "edge_id", "event_id", "result_index",
    "source_occurrence_id", "source_status", "condition_result", "duration_ns", "sequence", "occurred_at_us",
})

STAGE_VALUES = {
    'input.mapped': ('mapped_input',),
    'capability.resolved': (),
    'node.aggregated': ('output',),
    'output.bound': ('patch',),
    'routing.resolved': ('conditions',),
    'node.faulted': ('error',),
}


def summary(row):
    result = {k: v for k, v in row.items() if k in SUMMARY_FIELDS}
    result["values"] = [k[:-4] for k, v in row.items() if k.endswith("_ref") and k != 'metrics_ref' and v is not None]
    result['value_keys'] = {k[:-4]: value_key(v) for k, v in row.items() if k.endswith('_ref') and k != 'metrics_ref' and v is not None}
    return result


def value_key(ref):
    """Share UI values by canonical source reference rather than projection name."""
    return json.dumps([ref['kind'], ref['id'], ref['path']], ensure_ascii=False, separators=(',', ':'))


async def fork_metadata(storage, conn, iid, *, occurrences=False):
    """Read only lineage metadata, never the initial Runtime State or Call bodies."""
    events = meta.tables['runtime_events']
    event_id = await conn.scalar(select(events.c.event_id).where(events.c.invocation_id == iid,
        events.c.sequence == 1,events.c.event_name == 'invocation.forked'))
    if event_id is None:
        return None
    reader = await SourceReader.open(storage,conn,'runtime_event',event_id)
    fields = ('source_session_id','source_invocation_id','source_sequence','source_event_id',
        'source_root_invocation_id','root_invocation_id','initialization_events')
    if occurrences:
        fields += ('inherited_occurrences',)
    return {field:await reader.read(('payload',field)) for field in fields}


async def fork_scopes(storage, conn, iid, *, sequence=None):
    """Return ordered source prefixes followed by this branch's own prefix."""
    scopes, seen, current = [(iid,sequence)], {iid}, iid
    while (origin := await fork_metadata(storage,conn,current)) is not None:
        if current == iid and sequence is not None and sequence < origin['initialization_events']:
            break
        current = origin['source_invocation_id']
        if current in seen:
            raise ValueError('Cyclic Fork lineage.')
        seen.add(current)
        scopes.append((current,origin['source_sequence']))
    return list(reversed(scopes))


async def edge_scope(storage, conn, iid, *, sequence=None):
    """Scope referenced Edge evaluations without copying source projection rows."""
    edges, events = meta.tables['edge_evaluations'], meta.tables['runtime_events']
    scopes = await fork_scopes(storage,conn,iid,sequence=sequence)
    conditions, ordering, offset = [], [], 0
    for target,upper in scopes:
        conditions.append(and_(edges.c.invocation_id == target,
            events.c.sequence <= upper if upper is not None else literal(True)))
        ordering.append((edges.c.invocation_id == target,events.c.sequence+offset))
        offset += upper or 0
    position = events.c.sequence if len(scopes) == 1 else case(*ordering,else_=events.c.sequence)
    return or_(*conditions), position


async def edge_states(storage, conn, iid, *, sequence=None, details=False):
    edges, events = meta.tables['edge_evaluations'], meta.tables['runtime_events']
    scope, position = await edge_scope(storage,conn,iid,sequence=sequence)
    ranked = select(*edges.c,events.c.sequence,events.c.occurred_at_us,
        func.count().over(partition_by=edges.c.edge_id).label('evaluations'),
        func.row_number().over(partition_by=edges.c.edge_id,
            order_by=(position.desc(),edges.c.result_index.desc())).label('_rank')).join(
                events,edges.c.event_id == events.c.event_id).where(scope).subquery()
    result = []
    for row in (await conn.execute(select(*(c for c in ranked.c if c.name != '_rank')).where(ranked.c._rank == 1))).mappings():
        item=dict(edge_id=row['edge_id'],condition_result=row['condition_result'],evaluations=row['evaluations'])
        if details:
            item['evaluation']=dict(summary(row),invocation_id=iid,evaluations=row['evaluations'],inherited=row['invocation_id'] != iid)
        result.append(item)
    return result


async def inherited_metadata(storage, conn, iid, kind, row, origin):
    if not origin:
        return None
    identity = row.get('occurrence_id') if kind == 'node' else row.get('call_id',row.get('wait_id'))
    events = meta.tables['runtime_events']
    event_id = await conn.scalar(select(events.c.event_id).where(events.c.invocation_id == iid,events.c.sequence == 1))
    reader = await SourceReader.open(storage,conn,'runtime_event',event_id)
    try:
        if kind == 'node':
            status = await reader.read(('payload','inherited_occurrences',identity))
            if status not in {'completed','failed','cancelled','skipped'}:
                return None
            source_id = identity
        else:
            source_id = await reader.read(('payload','source_call_ids',identity))
    except KeyError:
        return None
    return dict(invocation_id=origin['source_invocation_id'],identity=source_id,
        through_sequence=origin['source_sequence'])


def filter_identities(values, max_length):
    """Bound metadata-only selections before constructing SQL predicates."""
    if not isinstance(values, (tuple, list)) or len(values) > 64 or any(
        not isinstance(value, str) or not value or len(value) > max_length for value in values
    ):
        raise ValueError("Identity filters must contain at most 64 nonempty IDs of the allowed length.")
    return tuple(sorted(set(values)))


def invocation_match(invocations, definitions, search, workflow_ids=(), revision_ids=(), started_after_us=None, started_before_us=None, statuses=(), excluded_workflow_ids=()):
    """Match identities without reading definitions or business values."""
    predicates = []
    workflow_ids = filter_identities(workflow_ids, 512)
    revision_ids = filter_identities(revision_ids, 4096)
    excluded_workflow_ids = filter_identities(excluded_workflow_ids, 512)
    statuses = filter_identities(statuses, 16)
    if any(status not in ('created', 'running', 'waiting', 'paused', 'settling', 'completed', 'failed', 'cancelled') for status in statuses):
        raise ValueError('Unknown Invocation status filter.')
    if statuses:
        choices = [invocations.c.control_state == 'paused'] if 'paused' in statuses else []
        regular = tuple(status for status in statuses if status != 'paused')
        if regular:
            choices.append(and_(invocations.c.status.in_(regular), or_(invocations.c.control_state.is_(None), invocations.c.control_state != 'paused')))
        predicates.append(or_(*choices))
    for value in (started_after_us, started_before_us):
        if value is not None and (type(value) is not int or not 0 <= value <= 9007199254740991):
            raise ValueError('Start times must be nonnegative integer microseconds.')
    if started_after_us is not None and started_before_us is not None and started_after_us > started_before_us:
        raise ValueError('Start time range is reversed.')
    if started_after_us is not None:
        predicates.append(invocations.c.started_at_us >= started_after_us)
    if started_before_us is not None:
        predicates.append(invocations.c.started_at_us <= started_before_us)
    if workflow_ids:
        predicates.append(definitions.c.workflow_id.in_(workflow_ids))
    if excluded_workflow_ids:
        predicates.append(definitions.c.workflow_id.not_in(excluded_workflow_ids))
    if revision_ids:
        predicates.append(definitions.c.revision_id.in_(revision_ids))
    if search:
        escaped = search.lower().replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
        fuzzy = '%' + '%'.join(
            c.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
            for c in search.lower()
        ) + '%'
        predicates.append(or_(
            func.lower(definitions.c.workflow_id).like(fuzzy, escape='\\'),
            func.lower(definitions.c.definition_hash).like(escaped + '%', escape='\\'),
            func.lower(definitions.c.revision_id).like(escaped + '%', escape='\\'),
            *(func.lower(c).like('%' + escaped + '%', escape='\\')
              for c in (invocations.c.session_id, invocations.c.invocation_id)),
        ))
    return and_(*predicates) if predicates else literal(True)


def trace_cursor(cursor):
    if cursor is not None and (
        not isinstance(cursor, (tuple, list)) or len(cursor) != 2
        or type(cursor[0]) is not int or cursor[0] < 0
        or not isinstance(cursor[1], str) or not cursor[1]
    ):
        raise ValueError('Invalid trace cursor.')


async def node_call_counts(conn, iid, sequence=None, occurrence_ids=None):
    """Count Call starts using projection references, without reading Event values."""
    calls, nodes, events = (meta.tables[k] for k in ('operator_calls', 'node_executions', 'runtime_events'))
    source = calls.join(nodes, and_(nodes.c.invocation_id == calls.c.invocation_id, nodes.c.occurrence_id == calls.c.occurrence_id))
    statement = select(nodes.c.occurrence_id, func.count()).select_from(source).where(calls.c.invocation_id == iid)
    if occurrence_ids is not None:
        statement = statement.where(nodes.c.occurrence_id.in_(occurrence_ids))
    if sequence is not None:
        statement = statement.join(events, events.c.event_id == calls.c.input_ref['id'].as_string()).where(
            calls.c.input_ref['kind'].as_string() == 'runtime_event', events.c.invocation_id == iid, events.c.sequence <= sequence)
    return dict((await conn.execute(statement.group_by(nodes.c.occurrence_id))).all())


class TraceQueries:
    def __init__(self, queries):
        self.queries, self.storage = queries, queries.storage

    async def _node_summaries(self, conn, rows):
        """Batch-read fixed counters from Event JSON without loading business values."""
        refs = {row['occurrence_id']: row['metrics_ref'] for row in rows if row.get('metrics_ref')}
        identities = {ref['id'] for ref in refs.values() if ref['kind'] == 'runtime_event' and ref['path'] == ['payload', 'metrics']}
        counters = {}
        if identities:
            events = meta.tables['runtime_events']
            # SQLite JSON extraction operates on Text; PostgreSQL needs a JSON cast.
            body = type_coerce(events.c.payload_text, JSON) if conn.dialect.name == 'sqlite' else cast(events.c.payload_text, JSON)
            fields = ('call_count', 'peak_parallelism')
            statement = select(events.c.event_id, *(body['metrics'][field].label(field) for field in fields)).where(events.c.event_id.in_(identities))
            for row in (await conn.execute(statement)).mappings():
                counters[row['event_id']] = {field: row[field] for field in fields if type(row[field]) is int and row[field] >= 0}
        return tuple(summary(row) | counters.get(refs.get(row['occurrence_id'], {}).get('id'), {}) for row in rows)

    async def _graph(self, conn, revision_id):
        reader = await SourceReader.open(self.storage, conn, 'workflow_revision', revision_id)
        if reader is None:
            return None
        record = await reader.read(deep=False)
        definition = {}
        for name in ('nodes', 'edges'):
            path = ('definition', name)
            items = await reader.read(path, deep=False)
            light = []
            for index in range(len(items)):
                base = (*path, index)
                item = await reader.read(base, deep=False)
                if name == 'nodes':
                    executable = await reader.read((*base, 'executable'), deep=False)
                    # Mapping and conditions are presence flags, never their definitions.
                    mapped = item['map'] is not None
                    # A large mapped definition can itself be an Artifact placeholder.
                    if item['map'] is None:
                        mapped = await reader.present((*base, 'map'))
                    light.append({'id': item['id'], 'executable': {k: executable[k] for k in ('kind', 'id')}, 'mapped': mapped})
                else:
                    light.append({k: item[k] for k in ('id', 'source', 'target', 'on')} | {
                        'conditional': item['condition'] is not None or await reader.present((*base, 'condition'))})
            definition[name] = light
        for name in ('entry_node_ids', 'exit_node_ids'):
            definition[name] = await reader.read(('definition', name))
        try:
            definition['groups'] = await reader.read(('definition', 'groups'))
        except KeyError:
            definition['groups'] = []
        revisions = meta.tables['workflow_revisions']
        registered = await conn.scalar(select(revisions.c.registered_at_us).where(revisions.c.revision_id == revision_id))
        return {k: record[k] for k in ('schema_version', 'workflow_id', 'workflow_version', 'workflow_revision_id', 'definition_hash')} | {'definition': definition, 'registered_at_us': registered}

    async def graph(self, revision_id):
        async with self.queries.view() as (conn, _, _, _):
            return await self._graph(conn, revision_id)

    async def definition(self, revision_id, kind, identity):
        if kind not in ('node', 'edge', 'group'):
            raise ValueError('kind must be node, edge or group.')
        async with self.queries.view() as (conn, _, _, _):
            reader = await SourceReader.open(self.storage, conn, 'workflow_revision', revision_id)
            if reader is None:
                return None
            path = ('definition', {'node': 'nodes', 'edge': 'edges', 'group': 'groups'}[kind])
            try:
                items = await reader.read(path, deep=False)
            except KeyError:
                if kind == 'group':
                    return None
                raise
            for i in range(len(items)):
                if await reader.read((*path, i, 'id')) == identity:
                    return await reader.read((*path, i))
            return None

    async def stages(self, iid, oid, *, limit=20, cursor=None):
        """Page recorded stages through the existing lightweight Occurrence index."""
        limit_value(limit)
        trace_cursor(cursor)
        events, changes = (meta.tables[n] for n in ('runtime_events', 'graph_changes'))
        async with self.queries.view(invocation_id=iid) as (conn, _, _, frontier):
            if await self.storage.row(conn, 'node_executions', invocation_id=iid, occurrence_id=oid) is None:
                return None
            # Referenced ancestor stages remain exact original records. The
            # cursor orders ancestor prefixes before this branch's own Events.
            scopes = await fork_scopes(self.storage,conn,iid)
            seen = {target for target,_ in scopes}
            conditions, ordering, offset = [], [], 0
            for target,upper in scopes:
                conditions.append(and_(events.c.invocation_id == target,
                    events.c.sequence <= upper if upper is not None else literal(True)))
                ordering.append((events.c.invocation_id == target, events.c.sequence+offset))
                offset += upper or 0
            position_key = events.c.sequence if len(scopes) == 1 else case(*ordering,else_=events.c.sequence)
            linked = select(changes.c.event_id).where(changes.c.invocation_id.in_(seen),
                changes.c.entity_type == 'node',changes.c.entity_id == oid).distinct()
            statement = select(events.c.event_id,events.c.event_name,events.c.sequence,events.c.occurred_at_us,
                events.c.invocation_id,position_key.label('_order')).where(or_(*conditions),
                events.c.event_id.in_(linked),events.c.event_name.in_(STAGE_VALUES))
            if cursor is not None:
                statement = statement.where(tuple_(position_key,events.c.event_id)>tuple(cursor))
            rows = (await conn.execute(statement.order_by(position_key,events.c.event_id).limit(limit+1))).mappings().all()
            details_by_event = {}
            if rows:
                details = (await conn.execute(select(changes.c.event_id, changes.c.change,
                    changes.c.duration_ns, changes.c.error_type, changes.c.error_code).where(
                    changes.c.event_id.in_([row['event_id'] for row in rows[:limit]]),
                    changes.c.entity_type == 'node', changes.c.entity_id == oid,
                ))).mappings().all()
                for detail in details:
                    details_by_event.setdefault(detail['event_id'], []).append(detail)
            items = []
            for row in rows[:limit]:
                item = dict(row, occurrence_id=oid, inherited=row['invocation_id'] != iid,
                            status='failed' if row['event_name'] == 'node.faulted' else 'completed',
                            values=list(STAGE_VALUES[row['event_name']]),
                            value_keys={field: value_key({'kind': 'runtime_event', 'id': row['event_id'], 'path': ['payload', field]}) for field in STAGE_VALUES[row['event_name']]})
                for detail in details_by_event.get(row['event_id'], ()):
                    for field in ('duration_ns', 'error_type', 'error_code'):
                        if detail[field] is not None:
                            item[field] = detail[field]
                    if row['event_name'] == 'node.faulted' and detail['change'].get('phase'):
                        item['error_phase'] = detail['change']['phase']
                # Resolution identifiers are small, while business values stay packed.
                if row['event_name'] == 'capability.resolved':
                    reader = await SourceReader.open(self.storage, conn, 'runtime_event', row['event_id'])
                    for field in ('capability_id', 'operator_id'):
                        item[field] = await reader.read(('payload', field))
                items.append(item)
                item.pop('_order',None)
        position = (rows[limit - 1]['_order'], rows[limit - 1]['event_id']) if len(rows) > limit else None
        return Page(tuple(items), position, frontier)

    async def stage_value(self, iid, oid, event_id, field, *, pointer=None):
        """Read one stage value only after verifying its exact Occurrence scope."""
        pointer_tokens(pointer)
        events, changes = (meta.tables[n] for n in ('runtime_events', 'graph_changes'))
        async with self.queries.view(invocation_id=iid) as (conn, _, _, _):
            name = await conn.scalar(select(events.c.event_name).where(
                events.c.event_id == event_id, events.c.invocation_id == iid,
                events.c.event_id.in_(select(changes.c.event_id).where(
                    changes.c.invocation_id == iid, changes.c.entity_type == 'node', changes.c.entity_id == oid)),
            ))
            if name is None:
                return None
            if field not in STAGE_VALUES.get(name, ()):
                raise ValueError('Unknown value for this execution stage.')
            reader = await SourceReader.open(self.storage, conn, 'runtime_event', event_id)
            try:
                return {'available': True, 'value': await reader.read_pointer(pointer, base=('payload', field))}
            except (KeyError, IndexError, TypeError):
                return {'available': False, 'value': None}

    async def sessions(self, *, search='', workflow_ids=(), revision_ids=(), statuses=(), started_after_us=None, started_before_us=None, session_id=None, limit=50, cursor=None, session_scope=None, excluded_workflow_ids=()):
        """Page Sessions containing matching Invocations, retaining whole-Session counts."""
        limit_value(limit)
        trace_cursor(cursor)
        inv, defs, events = (meta.tables[n] for n in ('invocations', 'workflow_revisions', 'runtime_events'))
        excluded_workflow_ids = filter_identities(excluded_workflow_ids, 512)
        match = invocation_match(inv, defs, search, workflow_ids, revision_ids, started_after_us, started_before_us, statuses, excluded_workflow_ids)
        source = inv.join(defs, inv.c.workflow_revision_id == defs.c.revision_id).outerjoin(
            events, and_(events.c.session_id == inv.c.session_id, events.c.sequence == inv.c.last_sequence)
        )
        activity = func.max(func.coalesce(events.c.occurred_at_us, inv.c.completed_at_us, inv.c.started_at_us, 0))
        statement = select(
            inv.c.session_id,
            func.count().label('invocation_count'),
            func.count(func.distinct(defs.c.workflow_id)).label('workflow_count'),
            func.sum(case((match, 1), else_=0)).label('matching_count'),
            activity.label('updated_at_us'),
        ).select_from(source).where(inv.c.status.is_not(None)).group_by(inv.c.session_id)
        if excluded_workflow_ids:
            statement = statement.where(defs.c.workflow_id.not_in(excluded_workflow_ids))
        if session_scope is not None:
            statement = statement.where(inv.c.session_id.in_(session_scope))
        if session_id is not None:
            statement = statement.where(inv.c.session_id == session_id)
        elif search or workflow_ids or revision_ids or statuses or started_after_us is not None or started_before_us is not None:
            statement = statement.having(func.sum(case((match, 1), else_=0)) > 0)
        if cursor is not None:
            position = tuple_(activity, inv.c.session_id)
            statement = statement.having(position < tuple(cursor))
        async with self.queries.view() as (conn, _, _, frontier):
            rows = (await conn.execute(statement.order_by(activity.desc(), inv.c.session_id.desc()).limit(limit + 1))).mappings().all()
            count = select(func.count(func.distinct(inv.c.session_id))).select_from(inv.join(defs, inv.c.workflow_revision_id == defs.c.revision_id)).where(inv.c.status.is_not(None))
            if excluded_workflow_ids:
                count = count.where(defs.c.workflow_id.not_in(excluded_workflow_ids))
            count = count.where(inv.c.session_id == session_id) if session_id is not None else count.where(match)
            if session_scope is not None:
                count = count.where(inv.c.session_id.in_(session_scope))
            total = await conn.scalar(count)
        chosen = rows[:limit]
        position = (chosen[-1]['updated_at_us'], chosen[-1]['session_id']) if len(rows) > limit else None
        return Page(tuple(dict(r) for r in chosen), position, frontier, total_count=total)

    async def _detail_page(self, conn, frontier, kind, iid, *, node_id, occurrence_id, status, order, limit, cursor):
        """Page cached display metadata without round-tripping JSON through SQL."""
        trace_cursor(cursor)
        name={'node':'node_executions','call':'operator_calls','wait':'waits'}[kind]
        filters={'invocation_id':iid}
        if status is not None:filters['status']=status
        if kind=='node' and node_id is not None:filters['node_id']=node_id
        if kind!='node' and occurrence_id is not None:filters['occurrence_id']=occurrence_id
        rows=await conn.detail_rows(name,filters)
        time_field={'node':'ready_at_us','call':'started_at_us','wait':'created_at_us'}[kind]
        identity={'node':'occurrence_id','call':'call_id','wait':'wait_id'}[kind]
        position=lambda row:(row[time_field] or 0,row[identity])
        if cursor is not None:
            rows=[row for row in rows if (position(row)>tuple(cursor) if order=='asc' else position(row)<tuple(cursor))]
        rows.sort(key=position,reverse=order=='desc')
        chosen=rows[:limit]
        next_cursor=position(chosen[-1]) if len(rows)>limit else None
        items=await self._node_summaries(conn,chosen) if kind=='node' else tuple(summary(row) for row in chosen)
        if kind=='node':
            counts=Counter(row['occurrence_id'] for row in await conn.detail_rows('operator_calls',{'invocation_id':iid}))
            for item in items:item['call_count']=counts[item['occurrence_id']]
        origin=await fork_metadata(self.storage,conn,iid)
        if origin:
            for item,row in zip(items,chosen):
                inherited=await inherited_metadata(self.storage,conn,iid,kind,row,origin)
                if inherited:item['inherited_from']=inherited
        return Page(items,next_cursor,frontier)

    async def page(self, kind, iid=None, *, node_id=None, occurrence_id=None, call_id=None, edge_id=None, session_id=None, workflow_ids=(), revision_ids=(), statuses=(), started_after_us=None, started_before_us=None, search='', status=None, order='desc', limit=50, cursor=None, excluded_workflow_ids=()):
        limit_value(limit)
        if order not in ('asc', 'desc'):
            raise ValueError('Expected ascending or descending execution order.')
        if kind not in ('root', 'child', 'session', 'node', 'call', 'edge', 'wait'):
            raise ValueError('Unknown trace collection.')
        allowed = {'node': ('ready', 'running', 'waiting', 'completed', 'failed', 'skipped', 'cancelled'),
                   'call': ('running', 'completed', 'failed', 'lost', 'cancelled'),
                   'wait': ('waiting', 'resumed', 'cancelled')}
        if status is not None and kind in allowed and status not in allowed[kind]:
            raise ValueError('Unknown collection status filter.')
        async with self.queries.view(invocation_id=iid) as (conn, _, _, frontier):
            if iid is not None and await self.storage.row(conn, 'invocations', invocation_id=iid) is None:
                return None
            if kind == 'session' and await self.storage.row(conn, 'sessions', session_id=session_id) is None:
                return None
            if kind in ('node','call','wait'):
                return await self._detail_page(conn,frontier,kind,iid,node_id=node_id,occurrence_id=occurrence_id,
                    status=status,order=order,limit=limit,cursor=cursor)
            name = {'root':'invocations', 'child':'invocations', 'session':'invocations', 'edge':'edge_evaluations'}[kind]
            table = meta.tables[name]
            where = []
            if kind in ('root', 'child', 'session'):
                defs = meta.tables['workflow_revisions']
                source = table.join(defs, table.c.workflow_revision_id == defs.c.revision_id)
                columns = [*table.c, defs.c.workflow_id, defs.c.workflow_version]
                if excluded_workflow_ids:
                    where.append(defs.c.workflow_id.not_in(filter_identities(excluded_workflow_ids, 512)))
                if kind == 'session':
                    where.extend((table.c.session_id == session_id, table.c.status.is_not(None), invocation_match(table, defs, search, workflow_ids, revision_ids, started_after_us, started_before_us, statuses)))
                else:
                    where.append(table.c.parent_invocation_id.is_(None) if kind == 'root' else table.c.parent_invocation_id == iid)
                if kind == 'root':
                    where.append(invocation_match(table, defs, search, workflow_ids, revision_ids, started_after_us, started_before_us, statuses))
                    if session_id is not None:
                        where.append(table.c.session_id == session_id)
                if kind == 'child':
                    changes = meta.tables['graph_changes']
                    created = select(func.min(changes.c.sequence)).where(
                        changes.c.invocation_id == iid, changes.c.entity_type == 'invocation',
                        changes.c.entity_id == table.c.invocation_id,
                        changes.c.change['parent_invocation_id'].as_string() == iid,
                    ).correlate(table).scalar_subquery()
                    columns.append(created.label('created_sequence'))
                    if occurrence_id is not None:
                        where.append(table.c.parent_occurrence_id == occurrence_id)
                    if call_id is not None:
                        calls = meta.tables['operator_calls']
                        call = (await conn.execute(select(calls.c.occurrence_id, calls.c.unit_index).where(
                            calls.c.invocation_id == iid, calls.c.call_id == call_id,
                        ))).mappings().first()
                        if call is None:
                            return None
                        # Child plans are keyed by parent Occurrence and Unit,
                        # independently of replay attempts and Call UUIDs.
                        where.extend((table.c.parent_occurrence_id == call['occurrence_id'],
                                      table.c.unit_index == call['unit_index']))
                if kind == 'root':
                    where.append(table.c.status.is_not(None))
                time, identity = func.coalesce(table.c.started_at_us, 0), table.c.invocation_id
                if kind == 'child' and order == 'asc':
                    time = func.coalesce(created, 0)
                if search and kind == 'child':
                    pattern = '%' + search.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_').lower() + '%'
                    where.append(or_(*(func.lower(c).like(pattern, escape='\\') for c in (defs.c.workflow_id, table.c.session_id, table.c.invocation_id))))
                if status:
                    if status == 'paused':
                        where.append(table.c.control_state == 'paused')
                    else:
                        where.append(table.c.status == status)
            elif kind == 'edge':
                events = meta.tables['runtime_events']
                source = table.join(events, table.c.event_id == events.c.event_id)
                columns = [*table.c, events.c.occurred_at_us, events.c.sequence]
                scope, time = await edge_scope(self.storage,conn,iid)
                where = [scope]
                if edge_id is not None:
                    where.append(table.c.edge_id == edge_id)
                if occurrence_id is not None:
                    where.append(table.c.source_occurrence_id == occurrence_id)
                identity = table.c.event_id + literal(':') + cast(table.c.result_index, Text)
            if cursor is not None:
                trace_cursor(cursor)
                position = tuple_(time, identity)
                where.append(position > tuple(cursor) if order == 'asc' else position < tuple(cursor))
            rows = (await conn.execute(select(*columns, time.label('_time'), identity.label('_id')).select_from(source).where(*where).order_by(*(c.asc() if order == 'asc' else c.desc() for c in (time, identity))).limit(limit + 1))).mappings().all()
            page = rows[:limit]
            position = (page[-1]['_time'], page[-1]['_id']) if len(rows) > limit else None
            items = tuple(summary(r) for r in page)
            if kind == 'edge':
                for item,row in zip(items,page):
                    item['inherited'] = row['invocation_id'] != iid
                    item['invocation_id'] = iid
            return Page(items, position, frontier)

    async def family(self, iid):
        """Read bounded ancestry metadata without loading any execution values."""
        table, defs = meta.tables['invocations'], meta.tables['workflow_revisions']
        fields = ('invocation_id', 'session_id', 'workflow_revision_id', 'root_invocation_id',
                  'parent_invocation_id', 'parent_occurrence_id', 'unit_index', 'entry_node_id',
                  'status', 'control_state', 'started_at_us', 'completed_at_us')
        statement = select(*(table.c[k] for k in fields), defs.c.workflow_id, defs.c.workflow_version).select_from(
            table.join(defs, table.c.workflow_revision_id == defs.c.revision_id))
        async with self.queries.view(invocation_id=iid) as (conn, _, _, frontier):
            current = (await conn.execute(statement.where(table.c.invocation_id == iid))).mappings().first()
            if current is None:
                return None
            ancestors = []
            if current['parent_invocation_id']:
                chain = select(table.c.invocation_id, table.c.parent_invocation_id, literal(1).label('depth')).where(
                    table.c.invocation_id == current['parent_invocation_id']).cte('ancestry', recursive=True)
                chain = chain.union_all(select(table.c.invocation_id, table.c.parent_invocation_id, chain.c.depth + 1)
                    .join(chain, table.c.invocation_id == chain.c.parent_invocation_id).where(chain.c.depth < 32))
                ancestors = (await conn.execute(statement.join(chain, table.c.invocation_id == chain.c.invocation_id)
                    .order_by(chain.c.depth.desc()))).mappings().all()
            root = current if current['invocation_id'] == current['root_invocation_id'] else next(
                (row for row in ancestors if row['invocation_id'] == current['root_invocation_id']), None)
            if root is None:
                root = (await conn.execute(statement.where(table.c.invocation_id == current['root_invocation_id']))).mappings().first()
            metadata = lambda row: dict(row, values=[]) if row is not None else None
            return dict(current=metadata(current), root=metadata(root), ancestors=[metadata(row) for row in ancestors],
                        ancestry_complete=not current['parent_invocation_id'] or bool(ancestors and not ancestors[0]['parent_invocation_id']),
                        frontier=frontier, view='durable')

    async def _active_routes(self, conn, iid):
        """Resolve consumed Activations for live Occurrences using metadata only."""
        changes = meta.tables['graph_changes']
        created, activated = changes.alias('created_node'), changes.alias('consumed_edge')
        nodes = meta.tables['node_executions']
        statement = select(
            activated.c.change['edge_id'].as_string().label('edge_id'),
            activated.c.change['source_occurrence_id'].as_string().label('source_occurrence_id'),
            nodes.c.node_id.label('target_node_id'),
            nodes.c.occurrence_id.label('target_occurrence_id'),
            nodes.c.status,
        ).select_from(created.join(activated, and_(
            created.c.event_id == activated.c.event_id,
            activated.c.invocation_id == iid,
            activated.c.entity_type == 'edge',
            activated.c.change['selected'].as_boolean().is_(True),
            activated.c.change['target_node_id'].as_string() == created.c.change['node_id'].as_string(),
            activated.c.change['target_scope'] == created.c.change['scope'],
        )).join(nodes, and_(nodes.c.invocation_id == iid, nodes.c.occurrence_id == created.c.entity_id))).where(
            created.c.invocation_id == iid, created.c.entity_type == 'node',
            nodes.c.status.in_(('ready', 'running', 'waiting')),
            activated.c.change['source_occurrence_id'].as_string().is_not(None),
        ).distinct()
        rows = (await conn.execute(statement)).mappings().all()
        return [dict(row) for row in rows]

    async def execution(self, iid):
        async with self.queries.view(invocation_id=iid) as (conn, _, _, frontier):
            return await self._execution(conn, frontier, iid)

    async def _execution(self, conn, frontier, iid):
        inv = await self.storage.row(conn, 'invocations', invocation_id=iid)
        if inv is None:
            return None
        events=meta.tables['runtime_events']
        by_node=defaultdict(list)
        for row in await conn.detail_rows('node_executions',{'invocation_id':iid}):
            by_node[row['node_id']].append(row)
        call_counts=Counter(row['occurrence_id'] for row in await conn.detail_rows('operator_calls',{'invocation_id':iid}))
        node_states=[]
        priority={'running':0,'waiting':1,'ready':2}
        for node_id,rows in sorted(by_node.items()):
            rows.sort(key=lambda row:(row['ordinal'],row['occurrence_id']),reverse=True)
            row=min(rows,key=lambda row:priority.get(row['status'],3))
            node_states.append(dict(node_id=node_id,status=row['status'],occurrence_id=row['occurrence_id'],
                occurrences=len(rows),call_count=call_counts[row['occurrence_id']]))
        current_edges = await edge_states(self.storage,conn,iid)
        # Routing decisions include unconditional and source-status mismatches;
        # condition evaluations alone do not describe whether every Edge ran.
        decisions={}
        for row in await conn.detail_rows('graph_changes',{'invocation_id':iid,'entity_type':'edge'}):
            change=row['change'];edge_id=change.get('edge_id')
            if edge_id is None or change.get('selected') is None:continue
            previous=decisions.get(edge_id)
            if previous is None or (row['sequence'],row['change_index'])>(previous['sequence'],previous['change_index']):
                decisions[edge_id]=row
        by_edge = {row['edge_id']: row for row in current_edges}
        for edge_id,row in sorted(decisions.items()):
            by_edge.setdefault(edge_id,{'edge_id':edge_id,'evaluations':0}).update(condition_result=row['change']['selected'])
        current_edges = list(by_edge.values())
        children = await conn.scalar(select(func.count()).select_from(meta.tables['invocations']).where(meta.tables['invocations'].c.parent_invocation_id == iid))
        definitions = meta.tables['workflow_revisions']
        revision = (await conn.execute(select(definitions.c.workflow_id, definitions.c.workflow_version).where(definitions.c.revision_id == inv['workflow_revision_id']))).mappings().one()
        active_routes = await self._active_routes(conn, iid) if any(node['status'] in ('ready', 'running', 'waiting') for node in node_states) else []
        last_event = (await conn.execute(select(events.c.event_id, events.c.event_name,
            events.c.sequence, events.c.occurred_at_us).where(events.c.invocation_id == iid)
            .order_by(events.c.sequence.desc()).limit(1))).mappings().first()
        store_id = conn.store_id
        fork = await fork_metadata(self.storage,conn,iid,occurrences=True)
        return {'source_id': store_id, 'invocation': summary(inv) | {k:revision[k] for k in ('workflow_id', 'workflow_version')}, 'nodes': node_states, 'edges': current_edges, 'active_routes': active_routes, 'child_count': children, 'frontier': frontier, 'view': 'durable',
                'last_event': dict(last_event) if last_event else None, 'observed_at_us': time.time_ns() // 1000, 'fork':fork}

    async def value(self, iid, kind, identity, field, *, pointer=None):
        pointer_tokens(pointer)
        names = {'invocation': ('invocations', 'invocation_id'), 'node': ('node_executions', 'occurrence_id'), 'call': ('operator_calls', 'call_id'), 'wait': ('waits', 'wait_id')}
        if kind not in names or field not in ('input', 'output', 'error', 'metrics', 'definition', 'request', 'response', 'cancel_reason'):
            raise ValueError('Unknown trace value.')
        if field == 'cancel_reason' and kind != 'invocation':
            raise ValueError('Cancellation reasons belong to Invocations.')
        async with self.queries.view(invocation_id=iid) as (conn, _, _, _):
            inv = await self.storage.row(conn, 'invocations', invocation_id=iid)
            if inv is None:
                return None
            name, key = names[kind]
            row = inv if kind == 'invocation' and identity == iid else await self.storage.row(conn, name, invocation_id=iid, **{key:identity}) if kind != 'invocation' else None
            if row is None:
                return None
            if field == 'cancel_reason':
                if row['status'] != 'cancelled' and row.get('pending_outcome') != 'cancelled':
                    return {'available': False, 'value': None}
                events = meta.tables['runtime_events']
                event_id = await conn.scalar(select(events.c.event_id).where(
                    events.c.invocation_id == iid,
                    events.c.event_name == 'invocation.cancelled',
                ).order_by(events.c.sequence.desc()).limit(1))
                if event_id is None:
                    return {'available': False, 'value': None}
                reader = await SourceReader.open(self.storage, conn, 'runtime_event', event_id)
                try:
                    reason = await reader.read_pointer(pointer, base=('payload', 'reason')) if reader else None
                except (KeyError, IndexError, TypeError):
                    return {'available': False, 'value': None}
                return {'available': reason is not None, 'value': reason}
            if field == 'definition':
                if kind != 'call' or not row.get('operator_definition_id'):
                    return {'available': False, 'value': None}
                reader = await SourceReader.open(self.storage, conn, 'operator_definition', row['operator_definition_id'])
                try:
                    return {'available': reader is not None, 'value': await reader.read_pointer(pointer) if reader else None}
                except (KeyError, IndexError, TypeError):
                    return {'available': False, 'value': None}
            ref = row.get(field + '_ref')
            if not ref:
                return {'available': False, 'value': None}
            reader = await SourceReader.open(self.storage, conn, ref['kind'], ref['id'])
            try:
                return {'available': reader is not None, 'value': await reader.read_pointer(pointer, base=ref['path']) if reader else None}
            except (KeyError, IndexError, TypeError):
                return {'available': False, 'value': None}

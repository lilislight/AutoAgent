"""Bounded durable tracing queries; business values are read separately."""

import json
import time

from sqlalchemy import JSON, Text, and_, case, cast, func, literal, or_, select, tuple_, type_coerce

from .models import Page
from .queries import limit_value
from .reading import SourceReader
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


def filter_identities(values, max_length):
    """Bound metadata-only selections before constructing SQL predicates."""
    if not isinstance(values, (tuple, list)) or len(values) > 64 or any(
        not isinstance(value, str) or not value or len(value) > max_length for value in values
    ):
        raise ValueError("Identity filters must contain at most 64 nonempty IDs of the allowed length.")
    return tuple(sorted(set(values)))


def invocation_match(invocations, definitions, search, workflow_ids=(), revision_ids=(), started_after_us=None, started_before_us=None, statuses=()):
    """Match identities without reading definitions or business values."""
    predicates = []
    workflow_ids = filter_identities(workflow_ids, 512)
    revision_ids = filter_identities(revision_ids, 4096)
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


async def node_call_counts(conn, iid, sequence=None):
    """Count Call starts using projection references, without reading Event values."""
    calls, nodes, events = (meta.tables[k] for k in ('operator_calls', 'node_executions', 'runtime_events'))
    source = calls.join(nodes, and_(nodes.c.invocation_id == calls.c.invocation_id, nodes.c.occurrence_id == calls.c.occurrence_id))
    statement = select(nodes.c.node_id, func.count()).select_from(source).where(calls.c.invocation_id == iid)
    if sequence is not None:
        statement = statement.join(events, events.c.event_id == calls.c.input_ref['id'].as_string()).where(
            calls.c.input_ref['kind'].as_string() == 'runtime_event', events.c.invocation_id == iid, events.c.sequence <= sequence)
    return dict((await conn.execute(statement.group_by(nodes.c.node_id))).all())


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
        revisions = meta.tables['workflow_revisions']
        registered = await conn.scalar(select(revisions.c.registered_at_us).where(revisions.c.revision_id == revision_id))
        return {k: record[k] for k in ('schema_version', 'workflow_id', 'workflow_version', 'workflow_revision_id', 'definition_hash')} | {'definition': definition, 'registered_at_us': registered}

    async def graph(self, revision_id):
        async with self.queries.view() as (conn, _, _, _):
            return await self._graph(conn, revision_id)

    async def definition(self, revision_id, kind, identity):
        if kind not in ('node', 'edge'):
            raise ValueError('kind must be node or edge.')
        async with self.queries.view() as (conn, _, _, _):
            reader = await SourceReader.open(self.storage, conn, 'workflow_revision', revision_id)
            if reader is None:
                return None
            path = ('definition', 'nodes' if kind == 'node' else 'edges')
            items = await reader.read(path, deep=False)
            for i in range(len(items)):
                if await reader.read((*path, i, 'id')) == identity:
                    return await reader.read((*path, i))
            return None

    async def stages(self, iid, oid, *, limit=20, cursor=None):
        """Page recorded stages through the existing lightweight Occurrence index."""
        limit_value(limit)
        trace_cursor(cursor)
        events, changes = (meta.tables[n] for n in ('runtime_events', 'graph_changes'))
        linked = select(changes.c.event_id).where(
            changes.c.invocation_id == iid, changes.c.entity_type == 'node',
            changes.c.entity_id == oid,
        ).distinct()
        statement = select(events.c.event_id, events.c.event_name, events.c.sequence,
                           events.c.occurred_at_us).where(
            events.c.invocation_id == iid, events.c.event_id.in_(linked),
            events.c.event_name.in_(STAGE_VALUES),
        )
        if cursor is not None:
            statement = statement.where(tuple_(events.c.sequence, events.c.event_id) > tuple(cursor))
        async with self.queries.view() as (conn, _, _, frontier):
            if await self.storage.row(conn, 'node_executions', invocation_id=iid, occurrence_id=oid) is None:
                return None
            rows = (await conn.execute(statement.order_by(events.c.sequence, events.c.event_id).limit(limit + 1))).mappings().all()
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
                item = dict(row, invocation_id=iid, occurrence_id=oid,
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
        position = (rows[limit - 1]['sequence'], rows[limit - 1]['event_id']) if len(rows) > limit else None
        return Page(tuple(items), position, frontier)

    async def stage_value(self, iid, oid, event_id, field):
        """Read one stage value only after verifying its exact Occurrence scope."""
        events, changes = (meta.tables[n] for n in ('runtime_events', 'graph_changes'))
        async with self.queries.view() as (conn, _, _, _):
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
            return {'available': True, 'value': await reader.read(('payload', field))}

    async def sessions(self, *, search='', workflow_ids=(), revision_ids=(), statuses=(), started_after_us=None, started_before_us=None, session_id=None, limit=50, cursor=None):
        """Page Sessions containing matching Invocations, retaining whole-Session counts."""
        limit_value(limit)
        trace_cursor(cursor)
        inv, defs, events = (meta.tables[n] for n in ('invocations', 'workflow_revisions', 'runtime_events'))
        match = invocation_match(inv, defs, search, workflow_ids, revision_ids, started_after_us, started_before_us, statuses)
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
            count = count.where(inv.c.session_id == session_id) if session_id is not None else count.where(match)
            total = await conn.scalar(count)
        chosen = rows[:limit]
        position = (chosen[-1]['updated_at_us'], chosen[-1]['session_id']) if len(rows) > limit else None
        return Page(tuple(dict(r) for r in chosen), position, frontier, total_count=total)

    async def page(self, kind, iid=None, *, node_id=None, occurrence_id=None, call_id=None, edge_id=None, session_id=None, workflow_ids=(), revision_ids=(), statuses=(), started_after_us=None, started_before_us=None, search='', status=None, order='desc', limit=50, cursor=None):
        limit_value(limit)
        if order not in ('asc', 'desc'):
            raise ValueError('Expected ascending or descending execution order.')
        if kind not in ('root', 'child', 'session', 'node', 'call', 'edge', 'wait'):
            raise ValueError('Unknown trace collection.')
        async with self.queries.view() as (conn, _, _, frontier):
            if iid is not None and await self.storage.row(conn, 'invocations', invocation_id=iid) is None:
                return None
            if kind == 'session' and await self.storage.row(conn, 'sessions', session_id=session_id) is None:
                return None
            name = {'root':'invocations', 'child':'invocations', 'session':'invocations', 'node':'node_executions', 'call':'operator_calls', 'edge':'edge_evaluations', 'wait':'waits'}[kind]
            table = meta.tables[name]
            where = []
            if kind in ('root', 'child', 'session'):
                defs = meta.tables['workflow_revisions']
                source = table.join(defs, table.c.workflow_revision_id == defs.c.revision_id)
                columns = [*table.c, defs.c.workflow_id, defs.c.workflow_version]
                if kind == 'session':
                    where.extend((table.c.session_id == session_id, table.c.status.is_not(None), invocation_match(table, defs, search, workflow_ids, revision_ids, started_after_us, started_before_us, statuses)))
                else:
                    where.append(table.c.parent_invocation_id.is_(None) if kind == 'root' else table.c.parent_invocation_id == iid)
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
                if search and kind != 'session':
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
                where = [table.c.invocation_id == iid, table.c.edge_id == edge_id]
                if occurrence_id is not None:
                    where.append(table.c.source_occurrence_id == occurrence_id)
                time = events.c.sequence
                identity = table.c.event_id + literal(':') + cast(table.c.result_index, Text)
            else:
                source, columns = table, list(table.c)
                where = [table.c.invocation_id == iid]
                if kind == 'node':
                    where.append(table.c.node_id == node_id)
                    time, identity = func.coalesce(table.c.ready_at_us, 0), table.c.occurrence_id
                else:
                    if occurrence_id is not None:
                        where.append(table.c.occurrence_id == occurrence_id)
                    time, identity = (table.c.created_at_us, table.c.wait_id) if kind == 'wait' else (table.c.started_at_us, table.c.call_id)
            if cursor is not None:
                trace_cursor(cursor)
                position = tuple_(time, identity)
                where.append(position > tuple(cursor) if order == 'asc' else position < tuple(cursor))
            rows = (await conn.execute(select(*columns, time.label('_time'), identity.label('_id')).select_from(source).where(*where).order_by(*(c.asc() if order == 'asc' else c.desc() for c in (time, identity))).limit(limit + 1))).mappings().all()
            page = rows[:limit]
            position = (page[-1]['_time'], page[-1]['_id']) if len(rows) > limit else None
            items = await self._node_summaries(conn, page) if kind == 'node' else tuple(summary(r) for r in page)
            return Page(items, position, frontier)

    async def family(self, iid):
        """Read bounded ancestry metadata without loading any execution values."""
        table, defs = meta.tables['invocations'], meta.tables['workflow_revisions']
        fields = ('invocation_id', 'session_id', 'workflow_revision_id', 'root_invocation_id',
                  'parent_invocation_id', 'parent_occurrence_id', 'unit_index', 'entry_node_id',
                  'status', 'control_state', 'started_at_us', 'completed_at_us')
        statement = select(*(table.c[k] for k in fields), defs.c.workflow_id, defs.c.workflow_version).select_from(
            table.join(defs, table.c.workflow_revision_id == defs.c.revision_id))
        async with self.queries.view() as (conn, _, _, frontier):
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
        async with self.queries.view() as (conn, _, _, frontier):
            return await self._execution(conn, frontier, iid)

    async def _execution(self, conn, frontier, iid):
        inv = await self.storage.row(conn, 'invocations', invocation_id=iid)
        if inv is None:
            return None
        nodes, edges, events = (meta.tables[n] for n in ('node_executions', 'edge_evaluations', 'runtime_events'))
        ranked = select(nodes.c.node_id, nodes.c.status, nodes.c.occurrence_id,
            func.count().over(partition_by=nodes.c.node_id).label('occurrences'),
            func.row_number().over(partition_by=nodes.c.node_id, order_by=(case((nodes.c.status == 'running', 0), (nodes.c.status == 'waiting', 1), (nodes.c.status == 'ready', 2), else_=3), nodes.c.ordinal.desc(), nodes.c.occurrence_id.desc())).label('_rank')).where(nodes.c.invocation_id == iid).subquery()
        node_states = [dict(r) for r in (await conn.execute(select(*(c for c in ranked.c if c.name != '_rank')).where(ranked.c._rank == 1))).mappings()]
        call_counts = await node_call_counts(conn, iid)
        for row in node_states:
            row['call_count'] = call_counts.get(row['node_id'], 0)
        ranked_edges = select(edges.c.edge_id, edges.c.condition_result,
            func.count().over(partition_by=edges.c.edge_id).label('evaluations'),
            func.row_number().over(partition_by=edges.c.edge_id, order_by=(events.c.sequence.desc(), edges.c.result_index.desc())).label('_rank')).select_from(edges.join(events, edges.c.event_id == events.c.event_id)).where(edges.c.invocation_id == iid).subquery()
        edge_states = [dict(r) for r in (await conn.execute(select(*(c for c in ranked_edges.c if c.name != '_rank')).where(ranked_edges.c._rank == 1))).mappings()]
        # Routing decisions include unconditional and source-status mismatches;
        # condition evaluations alone do not describe whether every Edge ran.
        changes = meta.tables['graph_changes']
        edge_id = changes.c.change['edge_id'].as_string()
        decisions = select(edge_id.label('edge_id'),
            changes.c.change['selected'].as_boolean().label('condition_result'),
            func.row_number().over(partition_by=edge_id, order_by=(changes.c.sequence.desc(), changes.c.change_index.desc())).label('_rank'),
        ).where(changes.c.invocation_id == iid, changes.c.entity_type == 'edge',
            edge_id.is_not(None), changes.c.change['selected'].as_boolean().is_not(None)).subquery()
        by_edge = {row['edge_id']: row for row in edge_states}
        for row in (await conn.execute(select(decisions.c.edge_id, decisions.c.condition_result).where(decisions.c._rank == 1))).mappings():
            by_edge.setdefault(row['edge_id'], {'edge_id': row['edge_id'], 'evaluations': 0}).update(condition_result=row['condition_result'])
        edge_states = list(by_edge.values())
        children = await conn.scalar(select(func.count()).select_from(meta.tables['invocations']).where(meta.tables['invocations'].c.parent_invocation_id == iid))
        definitions = meta.tables['workflow_revisions']
        revision = (await conn.execute(select(definitions.c.workflow_id, definitions.c.workflow_version).where(definitions.c.revision_id == inv['workflow_revision_id']))).mappings().one()
        active_routes = await self._active_routes(conn, iid) if any(node['status'] in ('ready', 'running', 'waiting') for node in node_states) else []
        last_event = (await conn.execute(select(events.c.event_id, events.c.event_name,
            events.c.sequence, events.c.occurred_at_us).where(events.c.invocation_id == iid)
            .order_by(events.c.sequence.desc()).limit(1))).mappings().first()
        store_id = await conn.scalar(select(meta.tables['persistence_meta'].c.store_id))
        return {'source_id': store_id, 'invocation': summary(inv) | {k:revision[k] for k in ('workflow_id', 'workflow_version')}, 'nodes': node_states, 'edges': edge_states, 'active_routes': active_routes, 'child_count': children, 'frontier': frontier, 'view': 'durable',
                'last_event': dict(last_event) if last_event else None, 'observed_at_us': time.time_ns() // 1000}

    async def value(self, iid, kind, identity, field):
        names = {'invocation': ('invocations', 'invocation_id'), 'node': ('node_executions', 'occurrence_id'), 'call': ('operator_calls', 'call_id'), 'wait': ('waits', 'wait_id')}
        if kind not in names or field not in ('input', 'output', 'error', 'metrics', 'definition', 'request', 'response', 'cancel_reason'):
            raise ValueError('Unknown trace value.')
        if field == 'cancel_reason' and kind != 'invocation':
            raise ValueError('Cancellation reasons belong to Invocations.')
        async with self.queries.view() as (conn, _, _, _):
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
                reason = await reader.read(('payload', 'reason')) if reader else None
                return {'available': reason is not None, 'value': reason}
            if field == 'definition':
                if kind != 'call' or not row.get('operator_definition_id'):
                    return {'available': False, 'value': None}
                reader = await SourceReader.open(self.storage, conn, 'operator_definition', row['operator_definition_id'])
                return {'available': reader is not None, 'value': await reader.read() if reader else None}
            ref = row.get(field + '_ref')
            if not ref:
                return {'available': False, 'value': None}
            reader = await SourceReader.open(self.storage, conn, ref['kind'], ref['id'])
            try:
                return {'available': reader is not None, 'value': await reader.read(ref['path']) if reader else None}
            except (KeyError, IndexError, TypeError):
                return {'available': False, 'value': None}

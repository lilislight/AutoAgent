"""Read-only Revision comparison and Session history without Runtime restoration."""

from sqlalchemy import and_, func, select, tuple_

from .codec import DOMAIN, canonical, pointer
from .models import Page
from .queries import limit_value
from .reading import SourceReader
from .schema import meta
from .tracing import trace_cursor, invocation_match
import hashlib


async def fingerprint(reader, path):
    """Compare logical JSON structurally; opaque string Artifacts need no body read."""
    async def visit(value, refs, remaining):
        if '' in refs:
            digest = refs['']
            artifacts = meta.tables['artifacts']
            kind = await reader.conn.scalar(select(artifacts.c.value_type).where(artifacts.c.hash == digest))
            if not remaining and kind == 'string':
                return digest
            await reader._read(None, {'': digest}, (), False, frozenset())
            value, refs, _ = reader.cache[digest]
        if remaining:
            token = remaining[0]
            prefix = pointer((token,))
            children = {k[len(prefix):]: v for k, v in refs.items() if k == prefix or k.startswith(prefix + '/')}
            return await visit(value[token], children, remaining[1:])
        if isinstance(value, (dict, list)):
            keys = sorted(value) if isinstance(value, dict) else range(len(value))
            children = []
            for key in keys:
                prefix = pointer((key,))
                subset = {k[len(prefix):]: v for k, v in refs.items() if k == prefix or k.startswith(prefix + '/')}
                children.append((key, await visit(value[key], subset, ())))
            return hashlib.sha256(canonical(['object' if isinstance(value, dict) else 'array', children])).hexdigest()
        return hashlib.sha256(DOMAIN + canonical(value)).hexdigest()
    return await visit(reader.record, reader.refs, tuple(path))


class RevisionComparison:
    def __init__(self, queries):
        self.queries = queries

    async def _readers(self, conn, base, target):
        left = await SourceReader.open(self.queries.storage, conn, 'workflow_revision', base)
        right = await SourceReader.open(self.queries.storage, conn, 'workflow_revision', target)
        if left is None or right is None:
            return None
        if left.record['workflow_id'] != right.record['workflow_id']:
            raise ValueError('Compare Revisions of the same Workflow.')
        return left, right

    async def _index(self, reader):
        definition = await reader.read(('definition',), deep=False)
        result = {('workflow', reader.record['workflow_id']): (('definition',), definition)}
        for kind, name in (('node', 'nodes'), ('edge', 'edges')):
            items = await reader.read(('definition', name), deep=False)
            for index in range(len(items)):
                path = ('definition', name, index)
                value = await reader.read(path, deep=False)
                result[kind, value['id']] = path, value
        return result

    async def page(self, base, target, limit=50, cursor=None):
        limit_value(limit)
        trace_cursor(cursor)
        async with self.queries.view() as (conn, _, _, frontier):
            readers = await self._readers(conn, base, target)
            if readers is None:
                return None
            left, right = readers
            before, after = await self._index(left), await self._index(right)
            changes = []
            counts = dict(added=0, removed=0, modified=0, workflow=0, node=0, edge=0)
            for kind, identity in sorted(before.keys() | after.keys(), key=lambda key: (('workflow', 'node', 'edge').index(key[0]), key[1])):
                old, new = before.get((kind, identity)), after.get((kind, identity))
                fields = []
                if old and new:
                    for field in sorted(old[1].keys() | new[1].keys()):
                        if kind == 'workflow' and field in ('nodes', 'edges'):
                            continue
                        if field not in old[1] or field not in new[1] or await fingerprint(left, (*old[0], field)) != await fingerprint(right, (*new[0], field)):
                            fields.append(field)
                    if not fields:
                        continue
                status = 'added' if old is None else 'removed' if new is None else 'modified'
                counts[status] += 1
                counts[kind] += 1
                changes.append(dict(kind=kind, id=identity, status=status, fields=fields, ordinal=len(changes) + 1))
            chosen = [row for row in changes if cursor is None or (row['ordinal'], row['id']) > tuple(cursor)][:limit + 1]
            visible = chosen[:limit]
            next_cursor = (visible[-1]['ordinal'], visible[-1]['id']) if len(chosen) > limit else None
            return dict(items=visible, next_cursor=next_cursor, frontier=frontier, total_count=len(changes), counts=counts)

    async def object(self, base, target, kind, identity):
        if kind not in ('workflow', 'node', 'edge'):
            raise ValueError('Unknown comparison object kind.')
        async with self.queries.view() as (conn, _, _, _):
            readers = await self._readers(conn, base, target)
            if readers is None:
                return None
            indexes = [await self._index(reader) for reader in readers]
            pair = [index.get((kind, identity)) for index in indexes]
            if not any(pair):
                return None
            changed = None
            if all(pair):
                changed = []
                for field in sorted(pair[0][1].keys() | pair[1][1].keys()):
                    if kind == 'workflow' and field in ('nodes', 'edges'):
                        continue
                    if field not in pair[0][1] or field not in pair[1][1] or await fingerprint(readers[0], (*pair[0][0], field)) != await fingerprint(readers[1], (*pair[1][0], field)):
                        changed.append(field)
            result = {'fields_only': True}
            for side, reader, item in zip(('base', 'target'), readers, pair):
                if item is None:
                    result[side] = dict(present=False, value=None)
                else:
                    fields = changed if changed is not None else item[1].keys()
                    value = {key: await reader.read((*item[0], key)) for key in fields
                             if key in item[1] and not (kind == 'workflow' and key in ('nodes', 'edges'))}
                    result[side] = dict(present=True, value=value)
            return result



class SessionInformation:
    def __init__(self, queries):
        self.queries = queries

    def _history(self, sid):
        inv, defs, events = (meta.tables[n] for n in ('invocations', 'workflow_revisions', 'runtime_events'))
        source = inv.join(defs, inv.c.workflow_revision_id == defs.c.revision_id).outerjoin(events,
            and_(inv.c.input_ref['kind'].as_string() == 'runtime_event', events.c.event_id == inv.c.input_ref['id'].as_string()))
        opening = func.coalesce(events.c.sequence, 0)
        fields = ('invocation_id', 'session_id', 'workflow_revision_id', 'parent_invocation_id', 'root_invocation_id',
                  'entry_node_id', 'status', 'control_state', 'started_at_us', 'completed_at_us', 'recovery_count', 'last_sequence')
        return select(*(inv.c[k] for k in fields), defs.c.workflow_id, defs.c.workflow_version,
            events.c.sequence.label('opening_sequence'), opening.label('_order'),
            func.row_number().over(order_by=(opening, inv.c.invocation_id)).label('ordinal')
        ).select_from(source).where(inv.c.session_id == sid, inv.c.status.is_not(None)).subquery()

    async def info(self, sid):
        sessions, inv, defs, events = (meta.tables[n] for n in ('sessions', 'invocations', 'workflow_revisions', 'runtime_events'))
        async with self.queries.view() as (conn, _, _, frontier):
            session = (await conn.execute(select(sessions).where(sessions.c.session_id == sid))).mappings().first()
            if session is None:
                return None
            counts = (await conn.execute(select(func.count().label('invocation_count'),
                func.count(func.distinct(defs.c.workflow_id)).label('workflow_count'),
                func.count(func.distinct(inv.c.workflow_revision_id)).label('revision_count'),
                func.max(func.coalesce(inv.c.completed_at_us, inv.c.started_at_us)).label('fallback_activity')
            ).select_from(inv.join(defs, inv.c.workflow_revision_id == defs.c.revision_id))
                .where(inv.c.session_id == sid, inv.c.status.is_not(None)))).mappings().one()
            latest = await conn.scalar(select(events.c.occurred_at_us).where(events.c.session_id == sid, events.c.sequence == session['durable_sequence']))
            history = self._history(sid)
            first = (await conn.execute(select(history.c.parent_invocation_id).order_by(history.c.ordinal).limit(1))).first()
            return dict(session_id=sid, created_at_us=session['created_at_us'], updated_at_us=latest or counts['fallback_activity'],
                latest_invocation_id=session['latest_invocation_id'], parent_invocation_id=first[0] if first else None,
                invocation_count=counts['invocation_count'], workflow_count=counts['workflow_count'], revision_count=counts['revision_count'],
                durable_sequence=session['durable_sequence'], frontier=frontier)

    async def history(self, sid, *, limit=50, cursor=None,
        search='', workflow_ids=(), revision_ids=(), statuses=(), started_after_us=None, started_before_us=None):
        limit_value(limit)
        trace_cursor(cursor)
        async with self.queries.view() as (conn, _, _, frontier):
            if await conn.scalar(select(meta.tables['sessions'].c.session_id).where(meta.tables['sessions'].c.session_id == sid)) is None:
                return None
            history = self._history(sid)
            where = []
            definitions = meta.tables['workflow_revisions']
            source = history.join(definitions, history.c.workflow_revision_id == definitions.c.revision_id)
            where.append(invocation_match(history, definitions, search, workflow_ids, revision_ids,
                started_after_us, started_before_us, statuses))
            total = await conn.scalar(select(func.count()).select_from(source).where(*where))
            if cursor is not None:
                where.append(tuple_(history.c._order, history.c.invocation_id) > tuple(cursor))
            rows = [dict(row) for row in (await conn.execute(select(history).select_from(source).where(*where)
                .order_by(history.c._order, history.c.invocation_id).limit(limit + 1))).mappings()]
            chosen = rows[:limit]
            next_cursor = (chosen[-1]['_order'], chosen[-1]['invocation_id']) if len(rows) > limit else None
            for row in chosen:
                row.pop('_order')
                row['values'] = []
            return Page(tuple(chosen), next_cursor, frontier, total)

    async def workflows(self, sid, *, limit=50, cursor=None):
        limit_value(limit)
        trace_cursor(cursor)
        async with self.queries.view() as (conn, _, _, frontier):
            if await conn.scalar(select(meta.tables['sessions'].c.session_id).where(meta.tables['sessions'].c.session_id == sid)) is None:
                return None
            history = self._history(sid)
            first = func.min(history.c._order)
            statement = select(history.c.workflow_id, history.c.workflow_version, history.c.workflow_revision_id,
                func.count().label('invocation_count'), first.label('first_sequence'),
                func.min(history.c.started_at_us).label('first_started_at_us'), func.max(history.c.started_at_us).label('last_started_at_us')
            ).group_by(history.c.workflow_id, history.c.workflow_version, history.c.workflow_revision_id)
            total = await conn.scalar(select(func.count()).select_from(statement.subquery()))
            if cursor is not None:
                statement = statement.having(tuple_(first, history.c.workflow_revision_id) > tuple(cursor))
            rows = [dict(row) for row in (await conn.execute(statement.order_by(first, history.c.workflow_revision_id).limit(limit + 1))).mappings()]
            chosen = rows[:limit]
            position = (chosen[-1]['first_sequence'], chosen[-1]['workflow_revision_id']) if len(rows) > limit else None
            return Page(tuple(chosen), position, frontier, total)

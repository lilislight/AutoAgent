"""Checkpoint identity and metadata queries without loading snapshot payloads."""

import hashlib

from sqlalchemy import func, select, tuple_, update

from .codec import canonical
from .models import ConflictError, Page
from .queries import limit_value
from .schema import meta


def progress_order():
    """Execution position, not capture time: a newly saved historical cut is older."""
    cp, members = (meta.tables[k] for k in ('checkpoints', 'checkpoint_sessions'))
    root = select(members.c.sequence).where(members.c.checkpoint_id == cp.c.checkpoint_id,
        members.c.session_id == cp.c.root_session_id).correlate(cp).scalar_subquery()
    total = select(func.sum(members.c.sequence)).where(members.c.checkpoint_id == cp.c.checkpoint_id).correlate(cp).scalar_subquery()
    return root.desc(), total.desc(), cp.c.captured_at_us.desc(), cp.c.checkpoint_id.desc()


def checkpoint_key(checkpoint):
    """Capture timestamps and UUIDs do not change an acknowledged graph version."""
    version = {
        "root_session_id": checkpoint.root_session_id,
        "graph_schema": 1,
        "members": sorted(
            (s.session_id, s.schema_version, s.state.invocation.id if s.state.invocation else None, s.sequence)
            for s in checkpoint.sessions
        ),
    }
    return hashlib.sha256(b'autoagent-checkpoint-version-v1\0' + canonical(version)).hexdigest()


class Checkpoints:
    def __init__(self, queries, maintenance):
        self.queries, self.maintenance = queries, maintenance

    async def exists(self, identity):
        table = meta.tables['checkpoints']
        async with self.queries.view(control=True) as (conn, _, _, _):
            return await conn.scalar(select(table.c.checkpoint_id).where(table.c.checkpoint_id == identity)) is not None

    async def _members(self, conn, ids):
        table = meta.tables['checkpoint_sessions']
        result = {key: [] for key in ids}
        if ids:
            for row in (await conn.execute(select(table).where(table.c.checkpoint_id.in_(ids)).order_by(table.c.session_id))).mappings():
                value = dict(row)
                key = value.pop('checkpoint_id')
                result[key].append(value)
        return result

    async def page(self, sid, limit, cursor):
        limit_value(limit)
        cp, members = (meta.tables[k] for k in ('checkpoints', 'checkpoint_sessions'))
        async with self.queries.view(control=True) as (conn, _, _, frontier):
            matching = select(members.c.checkpoint_id).where(members.c.session_id == sid)
            where = [cp.c.checkpoint_id.in_(matching)]
            total = await conn.scalar(select(func.count()).select_from(cp).where(*where))
            if cursor is not None:
                if (not isinstance(cursor, (tuple, list)) or len(cursor) != 2
                        or type(cursor[0]) is not int or cursor[0] < 0 or not isinstance(cursor[1], str)):
                    raise ValueError('Invalid Checkpoint cursor.')
                where.append(tuple_(cp.c.captured_at_us, cp.c.checkpoint_id) > tuple(cursor))
            columns = [col for col in cp.columns if col.name != 'checkpoint_text']
            rows = [dict(row) for row in (await conn.execute(select(*columns).where(*where)
                .order_by(cp.c.captured_at_us, cp.c.checkpoint_id).limit(limit + 1))).mappings()]
            visible = rows[:limit]
            positions = await self._members(conn, [r['checkpoint_id'] for r in visible])
            for row in visible:
                row['members'] = positions[row['checkpoint_id']]
                row['sequence'] = next(p['sequence'] for p in row['members'] if p['session_id'] == sid)
            next_cursor = (visible[-1]['captured_at_us'], visible[-1]['checkpoint_id']) if len(rows) > limit else None
            return Page(tuple(visible), next_cursor, frontier, total)

    async def info(self, identity):
        cp = meta.tables['checkpoints']
        async with self.queries.view(control=True) as (conn, _, _, _):
            columns = [col for col in cp.columns if col.name != 'checkpoint_text']
            row = (await conn.execute(select(*columns).where(cp.c.checkpoint_id == identity))).mappings().first()
            if row is None:
                return None
            result = dict(row)
            result['members'] = (await self._members(conn, [identity]))[identity]
            result['delete_blockers'] = await self.maintenance.checkpoint_blockers(conn, identity)
            result['deletable'] = not result['delete_blockers']
            return result

    async def historical_bundle(self, sid, sequence, *, full_history=False):
        """Read one causal Event prefix, without installing or executing its state."""
        events, invs, sessions, cp = (meta.tables[k] for k in
            ('runtime_events', 'invocations', 'sessions', 'checkpoints'))
        async with self.queries.view(control=True) as (conn, _, _, _):
            target = (await conn.execute(select(events.c.invocation_id, events.c.store_order).where(
                events.c.session_id == sid, events.c.sequence == sequence))).first()
            if target is None:
                raise ValueError('The requested Event Sequence is unavailable in this Session.')
            if target.invocation_id is None:
                raise ValueError('A Checkpoint requires an Event belonging to an Invocation.')
            root_id = await conn.scalar(select(invs.c.root_invocation_id).where(invs.c.invocation_id == target.invocation_id))
            root_sid = await conn.scalar(select(invs.c.session_id).where(invs.c.invocation_id == root_id))
            graph_sids = set((await conn.scalars(select(invs.c.session_id).where(invs.c.root_invocation_id == root_id))).all())
            bounds = dict((await conn.execute(select(events.c.session_id, func.max(events.c.sequence)).where(
                events.c.session_id.in_(graph_sids), events.c.store_order <= target.store_order)
                .group_by(events.c.session_id))).all())
            starts = dict((await conn.execute(select(sessions.c.session_id, sessions.c.history_start_sequence)
                .where(sessions.c.session_id.in_(graph_sids)))).all())
            # Imported history can begin at an anchor rather than Event #1.
            for member_sid, start in starts.items():
                bounds[member_sid] = max(bounds.get(member_sid, 0), start - 1)
            anchor = None
            candidates = (await conn.scalars(select(cp.c.checkpoint_id).where(cp.c.root_session_id == root_sid)
                .order_by(*progress_order()))).all()
            for identity in candidates:
                positions = (await self._members(conn, [identity]))[identity]
                root_member = next(m for m in positions if m['session_id'] == root_sid)
                previous_invocation = root_member['invocation_id'] != root_id
                if all(m['sequence'] <= bounds.get(m['session_id'], 0)
                    or previous_invocation and m['session_id'] not in graph_sids for m in positions):
                    anchor = await self.queries.storage.source(conn, 'checkpoint', identity)
                    if not full_history:
                        break
            if full_history and all(start == 1 for start in starts.values()):
                anchor = None
            anchored = {item['session_id']: item['sequence'] for item in anchor['sessions']} if anchor else {}
            histories = {}
            for member_sid, end in bounds.items():
                after = anchored.get(member_sid, 0)
                if end == 0:
                    continue  # This Child was not opened at the selected position.
                if after + 1 < starts[member_sid]:
                    raise ValueError('The requested position has no usable Checkpoint anchor or complete Event history.')
                ids = (await conn.scalars(select(events.c.event_id).where(events.c.session_id == member_sid,
                    events.c.sequence > after, events.c.sequence <= end).order_by(events.c.sequence))).all()
                histories[member_sid] = [await self.queries.storage.source(conn, 'runtime_event', identity) for identity in ids]
            return dict(root_session_id=root_sid, checkpoint=anchor, events=histories)

    async def configure(self, conn, identity, pinned, reason, keep_latest):
        cp = meta.tables['checkpoints']
        values = {}
        if pinned:
            values['pinned'] = True
        if reason is not None:
            values['created_reason'] = reason
        if values:
            await conn.execute(update(cp).where(cp.c.checkpoint_id == identity).values(**values))
        root = await conn.scalar(select(cp.c.root_session_id).where(cp.c.checkpoint_id == identity))
        if root is None:
            raise ConflictError('Checkpoint no longer exists.')
        if keep_latest is not None:
            await self.prune(conn, root, keep_latest)

    async def pin(self, conn, identity, pinned):
        cp = meta.tables['checkpoints']
        changed = await conn.execute(update(cp).where(cp.c.checkpoint_id == identity).values(pinned=pinned))
        return bool(changed.rowcount)

    async def prune(self, conn, root, keep_latest):
        cp = meta.tables['checkpoints']
        candidates = (await conn.execute(select(cp.c.checkpoint_id, cp.c.pinned)
            .where(cp.c.root_session_id == root)
            .order_by(*progress_order()).offset(keep_latest))).all()
        removed = False
        for identity, pinned in candidates:
            if pinned or await self.maintenance.checkpoint_blockers(conn, identity):
                continue
            # Delete through the existing safety checks. Artifact GC is deferred
            # until all metadata removals in this transaction have finished.
            await self.maintenance.delete_checkpoint(conn, identity, collect=False)
            removed = True
        if removed:
            await self.maintenance.gc(conn)

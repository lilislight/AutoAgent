"""One durable snapshot of a Root family, without loading business values."""

from sqlalchemy import func, select

from .schema import meta
from .tracing import summary


async def capture(queries, invocation_id, limit):
    invs, events, definitions, waits, nodes = (meta.tables[name] for name in
        ("invocations", "runtime_events", "workflow_revisions", "waits", "node_executions"))
    async with queries.view(invocation_id=invocation_id) as (conn, _, _, frontier):
        root_id = await conn.scalar(select(invs.c.root_invocation_id).where(invs.c.invocation_id == invocation_id))
        if root_id is None:
            return None
        rows = (await conn.execute(select(invs, definitions.c.workflow_id, definitions.c.workflow_version)
            .join(definitions, invs.c.workflow_revision_id == definitions.c.revision_id)
            .where(invs.c.root_invocation_id == root_id).order_by(invs.c.invocation_id).limit(limit + 1))).mappings().all()
        if len(rows) > limit:
            raise ValueError("Execution family exceeds the evidence member limit.")
        members = []
        for row in rows:
            last = (await conn.execute(select(events.c.event_id, events.c.sequence, events.c.event_name)
                .where(events.c.invocation_id == row['invocation_id']).order_by(events.c.sequence.desc()).limit(1))).first()
            item = summary(row)
            item['creation_id'] = row['creation_id']
            item['pending_outcome'] = row['pending_outcome']
            users = meta.tables['user_events']
            item['user_upper_sequence'] = await conn.scalar(select(func.coalesce(func.max(users.c.sequence), 0))
                .where(users.c.invocation_id == row['invocation_id']))
            item['position'] = None if last is None else dict(event_id=last.event_id, sequence=last.sequence,
                event_name=last.event_name, session_id=row['session_id'], invocation_id=row['invocation_id'],
                sequence_scope='session', boundary='after')
            pending = (await conn.execute(select(waits, nodes.c.node_id)
                .outerjoin(nodes, (nodes.c.invocation_id == waits.c.invocation_id) &
                    (nodes.c.occurrence_id == waits.c.occurrence_id))
                .where(waits.c.invocation_id == row['invocation_id'], waits.c.status == 'waiting')
                .order_by(waits.c.wait_id).limit(limit + 1))).mappings().all()
            if len(pending) > limit:
                raise ValueError("Pending Waits exceed the evidence limit.")
            item['waits'] = [summary(wait) for wait in pending]
            item['active_nodes'] = await conn.scalar(select(func.count()).select_from(nodes).where(
                nodes.c.invocation_id == row['invocation_id'], nodes.c.status.in_(('ready', 'running'))))
            members.append(item)
        root = next(item for item in members if item['invocation_id'] == root_id)
        settled = all(row['status'] in ('completed', 'failed', 'cancelled') or
            row['ownership_phase'] == 'abandoned' for row in rows) and not any(item['active_nodes'] for item in members)
        boundary = settled or root.get('control_state') == 'paused' and not any(item['active_nodes'] for item in members)
        pending = [wait for member in members for wait in member['waits']]
        if not root['active_nodes'] and root.get('pending_outcome') not in ('failed', 'cancelled') and pending:
            boundary |= not any(item['active_nodes'] for item in members) or any(w['kind'] not in ('child', 'any') for w in pending)
        store_id = await conn.scalar(select(meta.tables['persistence_meta'].c.store_id))
        return dict(store_id=store_id, view='durable', frontier=frontier, root_invocation_id=root_id,
            invocation_id=invocation_id, settled=bool(settled), at_boundary=bool(boundary), members=members)

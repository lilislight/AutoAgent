"""Accepted control metadata, without loading Event values or Core Sessions."""
from sqlalchemy import JSON, and_, cast, func, or_, select, type_coerce, true
from .schema import meta


class ControlQueries:
    def __init__(self, queries):
        self.queries = queries

    async def _closed_wait(self, conn, overlay, iid, wid):
        """Reconcile a lost response receipt from facts, never a UI read model."""
        ended = overlay.ended_waits.get((iid, wid))
        if ended is not None:
            return ended
        events = meta.tables['runtime_events']
        if conn.dialect.name == 'sqlite':
            operations = func.json_each(events.c.delta_text, '$.operations').table_valued('value')
            value = type_coerce(operations.c.value, JSON)
        else:
            from sqlalchemy.dialects.postgresql import JSONB
            operations = func.jsonb_array_elements(cast(events.c.delta_text, JSONB)['operations']).table_valued('value')
            value = type_coerce(operations.c.value, JSONB)
        path, state = value['path'], value['value']
        statement = select(state['status'].as_string().label('status'),
            state['kind'].as_string().label('kind'), state['occurrence_id'].as_string().label('occurrence_id')).select_from(
                events.join(operations, true())).where(
            events.c.invocation_id == iid,
            path[0].as_string() == 'invocation', path[1].as_string() == 'scheduler',
            path[2].as_string() == 'waits', path[3].as_string() == wid,
            path[4].as_string().is_(None), state['status'].as_string().in_(('resumed', 'cancelled')),
        ).order_by(events.c.sequence.desc()).limit(1)
        row = (await conn.execute(statement)).mappings().first()
        return dict(row, invocation_id=iid, wait_id=wid) if row else None

    async def read(self, iid, registered, *, cursor=None, target_iid=None, wait_id=None, source_id=None):
        q = self.queries
        async with q.view('accepted', control=True, invocation_id=iid) as (conn, overlay, pending, frontier):
            async def row(name, **keys):
                table = meta.tables[name]
                key = tuple(keys[c.name] for c in table.primary_key.columns)
                changed = overlay.rows.get(name, {})
                return changed[key] if key in changed else await q.storage.row(conn, name, **keys)
            selected = await row('invocations', invocation_id=iid)
            if selected is None:
                return None
            root_id = selected['root_invocation_id']
            root = await row('invocations', invocation_id=root_id)
            if root is None:
                return None
            session = await row('sessions', session_id=root['session_id'])
            invs, waits = meta.tables['invocations'], meta.tables['active_waits']
            unfinished = lambda r: r.get('status') not in ('completed','failed','cancelled') and r.get('ownership_phase') not in ('abandoned','terminal')
            missing, _ = await q._rows(conn, overlay, 'invocations', {'root_invocation_id':root_id}, ('invocation_id',), 1,
                extra_where=(invs.c.workflow_revision_id.not_in(registered), or_(invs.c.status.is_(None), invs.c.status!='completed'),
                    or_(invs.c.ownership_phase.is_(None), invs.c.ownership_phase.not_in(('abandoned','terminal')), and_(invs.c.status.in_(('failed','cancelled')), root['status'] in ('failed','cancelled')))),
                extra_filter=lambda r: (unfinished(r) or root['status'] in ('failed','cancelled') and r.get('status') in ('failed','cancelled')) and r['workflow_revision_id'] not in registered)
            changed_ids = {r['invocation_id'] for r in overlay.rows.get('invocations',{}).values() if r and r['root_invocation_id']==root_id}
            wait_ids = {r['invocation_id'] for r in overlay.rows.get('active_waits',{}).values() if r}
            if wait_ids:
                changed_ids.update((await conn.scalars(select(invs.c.invocation_id).where(invs.c.root_invocation_id==root_id,invs.c.invocation_id.in_(wait_ids)))).all())
            family = select(invs.c.invocation_id).where(invs.c.root_invocation_id==root_id)
            waiting, next_cursor = await q._rows(conn, overlay, 'active_waits', {'kind':'external','status':'waiting'}, ('invocation_id','wait_id'), 20, cursor,
                extra_where=(or_(waits.c.invocation_id.in_(family),waits.c.invocation_id.in_(changed_ids)),),
                extra_filter=lambda r:r['invocation_id'] in changed_ids)
            target, wait, node = None, None, None
            if wait_id:
                target = await row('invocations', invocation_id=target_iid or iid)
                if target and target['root_invocation_id']==root_id:
                    wait = await row('active_waits',invocation_id=target['invocation_id'],wait_id=wait_id)
                    if wait:
                        node = dict(node_id=wait['node_id'])
                    else:
                        wait = await self._closed_wait(conn, overlay, target['invocation_id'], wait_id)
            source_sequence = 0
            if source_id:
                source_invocation = selected['invocation_id']
                events=meta.tables['runtime_events']
                body=type_coerce(events.c.payload_text,JSON) if conn.dialect.name=='sqlite' else cast(events.c.payload_text,JSON)
                source_sequence=await conn.scalar(select(func.max(body['external']['sequence'].as_integer())).where(
                    events.c.invocation_id==source_invocation,events.c.event_name=='signal.accepted',body['external']['source_id'].as_string()==source_id)) or 0
                for entry in pending:
                    if entry.kind=='runtime_event' and entry.value.invocation_id==source_invocation and entry.value.event_name=='signal.accepted':
                        external=entry.value.payload.external
                        if external and external['source_id']==source_id:
                            source_sequence=max(source_sequence,external['sequence'])
            return dict(selected=selected,root=root,latest=bool(session and session['latest_invocation_id']==root_id),
                missing=bool(missing),waits=waiting,next_cursor=next_cursor,target=target,wait=wait,node=node,
                source_sequence=source_sequence,frontier=frontier)

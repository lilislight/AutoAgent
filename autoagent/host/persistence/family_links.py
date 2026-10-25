"""Small, occurrence-scoped navigation links derived from existing trace records."""
from sqlalchemy import and_, func, select

from .reading import SourceReader
from .schema import meta


async def family_links(conn, storage, iid, after_order=None):
    """Stream child identities and internal Await handles, never business values."""
    inv, nodes, changes, events, definitions, waits = (meta.tables[k] for k in
        ('invocations', 'node_executions', 'graph_changes', 'runtime_events', 'workflow_revisions', 'waits'))
    if await conn.scalar(select(inv.c.invocation_id).where(inv.c.parent_invocation_id == iid).limit(1)) is None:
        return
    first = select(changes.c.entity_id.label('child_id'),
        func.min(changes.c.sequence).label('created_sequence')).where(
        changes.c.invocation_id == iid, changes.c.entity_type == 'invocation',
        changes.c.change['parent_invocation_id'].as_string() == iid).group_by(changes.c.entity_id).subquery()
    child_fields = (inv.c.invocation_id, inv.c.session_id, inv.c.workflow_revision_id,
        inv.c.root_invocation_id, inv.c.parent_invocation_id, inv.c.parent_occurrence_id,
        inv.c.unit_index, definitions.c.workflow_id, definitions.c.workflow_version, first.c.created_sequence)
    children = select(*child_fields).select_from(inv.join(first, inv.c.invocation_id == first.c.child_id)
        .join(definitions, definitions.c.revision_id == inv.c.workflow_revision_id)).where(inv.c.parent_invocation_id == iid)
    spawn = children.add_columns(nodes.c.node_id, events.c.event_id, events.c.event_name, events.c.sequence, events.c.occurred_at_us).join(
        nodes, and_(nodes.c.invocation_id == iid, nodes.c.occurrence_id == inv.c.parent_occurrence_id)).join(
        events, and_(events.c.invocation_id == iid, events.c.sequence == first.c.created_sequence))
    if after_order is not None:
        spawn = spawn.where(events.c.store_order > after_order)
    stream = await conn.stream(spawn.order_by(events.c.sequence, inv.c.unit_index))
    async for row in stream.mappings():
        target = {c.key: row[c.key] for c in child_fields}
        yield dict(id=f"spawn:{row['event_id']}:{row['invocation_id']}", kind='spawn',
            event_id=row['event_id'], sequence=row['sequence'], occurred_at_us=row['occurred_at_us'],
            occurrence_id=row['parent_occurrence_id'], node_id=row['node_id'], wait_id=None,
            target=dict(target, values=[]), animate=row['event_name'] == 'child_invocation.planned')
    # Internal wait requests contain Runtime Handles only. Restrict reading to
    # these conditions; external requests, Node inputs and outputs stay lazy.
    request_id = waits.c.request_ref['id'].as_string()
    statement = select(waits.c.wait_id, waits.c.occurrence_id, waits.c.kind, waits.c.request_ref,
        nodes.c.node_id, events.c.event_id, events.c.sequence, events.c.occurred_at_us).join(
        nodes, and_(nodes.c.invocation_id == iid, nodes.c.occurrence_id == waits.c.occurrence_id)).join(
        events, and_(events.c.event_id == request_id, events.c.invocation_id == iid)).where(
        waits.c.invocation_id == iid, waits.c.kind.in_(('child', 'any')),
        waits.c.request_ref['kind'].as_string() == 'runtime_event', events.c.event_name == 'node_occurrence.waiting')
    if after_order is not None:
        statement = statement.where(events.c.store_order > after_order)
    stream = await conn.stream(statement.order_by(events.c.sequence))
    async for row in stream.mappings():
        reader = await SourceReader.open(storage, conn, 'runtime_event', row['event_id'])
        path = tuple(row['request_ref']['path'])
        if row['kind'] == 'child':
            handles = [await reader.read((*path, 'handle', 'invocation_id'))]
        else:
            cases = await reader.read((*path, 'cases'), deep=False)
            handles = []
            for index in range(len(cases)):
                base = (*path, 'cases', index)
                if await reader.read((*base, 'kind')) == 'child':
                    handles.append(await reader.read((*base, 'condition', 'handle', 'invocation_id')))
        if not handles:
            continue
        # Scope to directly owned children and their actual creation boundary.
        targets = (await conn.execute(children.where(inv.c.invocation_id.in_(handles),
            first.c.created_sequence <= row['sequence']))).mappings()
        for target in targets:
            yield dict(id=f"await:{row['event_id']}:{target['invocation_id']}", kind='await',
                event_id=row['event_id'], sequence=row['sequence'], occurred_at_us=row['occurred_at_us'],
                occurrence_id=row['occurrence_id'], node_id=row['node_id'], wait_id=row['wait_id'],
                target=dict(target, values=[]))

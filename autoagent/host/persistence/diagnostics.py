"""Bounded persisted facts for execution diagnosis; never hydrate business values."""

from sqlalchemy import and_, func, select

from .schema import meta
from .tracing import SUMMARY_FIELDS, summary


def metadata(row):
    result = summary(row)
    result.pop('value_keys', None)
    return result


def columns(table):
    return [column for column in table.c if column.name in SUMMARY_FIELDS
        or column.name.endswith('_ref') and column.name != 'metrics_ref']


class Diagnostics:
    def __init__(self, queries):
        self.queries = queries

    async def read(self, iid, *, limit=20):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError('Diagnostic evidence limit must be between 1 and 100.')
        invs, nodes, calls, waits, definitions = (meta.tables[name] for name in
            ('invocations', 'node_executions', 'operator_calls', 'waits', 'workflow_revisions'))
        family = select(invs.c.invocation_id).where(invs.c.invocation_id == iid).cte('diagnostic_family', recursive=True)
        family = family.union(select(invs.c.invocation_id).join(family,
            invs.c.parent_invocation_id == family.c.invocation_id))
        ids = select(family.c.invocation_id)
        async with self.queries.view() as (conn, _, _, frontier):
            selected = (await conn.execute(select(*columns(invs), definitions.c.workflow_id, definitions.c.workflow_version).select_from(invs)
                .join(definitions, invs.c.workflow_revision_id == definitions.c.revision_id)
                .where(invs.c.invocation_id == iid))).mappings().first()
            if selected is None:
                return None

            async def evidence(statement):
                total = await conn.scalar(select(func.count()).select_from(statement.order_by(None).subquery()))
                rows = (await conn.execute(statement.limit(limit))).mappings()
                return dict(items=[metadata(row) for row in rows], total_count=total, truncated=total > limit)

            counts = {}
            for label, table in (('invocations', invs), ('nodes', nodes), ('calls', calls), ('waits', waits)):
                counts[label] = {status or 'planned': count for status, count in (await conn.execute(
                    select(table.c.status, func.count()).where(table.c.invocation_id.in_(ids))
                    .group_by(table.c.status))).all()}
            node_scope = nodes.join(invs, nodes.c.invocation_id == invs.c.invocation_id)
            unresolved = and_(invs.c.status != 'completed', nodes.c.status == 'failed', selected['status'] != 'completed')
            failures = await evidence(select(*columns(nodes)).select_from(node_scope).where(
                nodes.c.invocation_id.in_(ids), unresolved).order_by(nodes.c.updated_store_order.desc(), nodes.c.occurrence_id))
            active = await evidence(select(*columns(nodes)).select_from(node_scope).where(nodes.c.invocation_id.in_(ids),
                nodes.c.status.in_(('ready', 'running', 'waiting')),
                invs.c.status.not_in(('completed', 'failed', 'cancelled')))
                .order_by(nodes.c.updated_store_order.desc(), nodes.c.occurrence_id))
            # Projection updates can touch an older Call again. Its immutable
            # start Event, rather than the last row update or wall clock,
            # identifies attempt order. Imported records fall back to timing.
            starts = meta.tables['runtime_events'].alias('diagnostic_call_starts')
            start_scope = calls.outerjoin(starts, and_(calls.c.input_ref['kind'].as_string() == 'runtime_event',
                calls.c.input_ref['id'].as_string() == starts.c.event_id,
                starts.c.event_name == 'operator_call.started'))
            ranked = select(calls.c.invocation_id, calls.c.call_id,
                func.row_number().over(partition_by=(calls.c.invocation_id, calls.c.occurrence_id, calls.c.unit_index),
                    order_by=(func.coalesce(starts.c.sequence, 0).desc(), calls.c.started_at_us.desc(),
                        calls.c.updated_store_order.desc(), calls.c.call_id.desc())).label('rank')
            ).select_from(start_scope).where(calls.c.invocation_id.in_(ids)).subquery()
            call_scope = calls.join(ranked, and_(calls.c.invocation_id == ranked.c.invocation_id,
                calls.c.call_id == ranked.c.call_id)).join(nodes, and_(calls.c.invocation_id == nodes.c.invocation_id,
                calls.c.occurrence_id == nodes.c.occurrence_id)).join(invs, calls.c.invocation_id == invs.c.invocation_id)
            call_failures = await evidence(select(*columns(calls), nodes.c.node_id).select_from(call_scope).where(
                ranked.c.rank == 1, calls.c.status.in_(('failed', 'lost')), unresolved)
                .order_by(calls.c.updated_store_order.desc(), calls.c.call_id))
            waiting = await evidence(select(*columns(waits), nodes.c.node_id).select_from(waits).join(nodes, and_(
                waits.c.invocation_id == nodes.c.invocation_id, waits.c.occurrence_id == nodes.c.occurrence_id))
                .join(invs, waits.c.invocation_id == invs.c.invocation_id).where(waits.c.invocation_id.in_(ids),
                    waits.c.status == 'waiting', invs.c.status.not_in(('completed', 'failed', 'cancelled')))
                .order_by(waits.c.created_at_us, waits.c.wait_id))
            descendants = await evidence(select(*columns(invs), definitions.c.workflow_id, definitions.c.workflow_version).select_from(invs)
                .join(definitions, invs.c.workflow_revision_id == definitions.c.revision_id)
                .where(invs.c.invocation_id.in_(ids), invs.c.invocation_id != iid)
                .order_by(invs.c.updated_store_order.desc(), invs.c.invocation_id))
            assessment = 'paused' if selected['control_state'] == 'paused' else selected['status'] or 'planned'
            return dict(invocation=metadata(selected), assessment=assessment, view='durable', frontier=frontier,
                runtime_observed=False, scope='selected_invocation_and_descendants', counts=counts,
                failed_nodes=failures, failed_calls=call_failures, active_nodes=active, pending_waits=waiting, descendants=descendants,
                historical_failures_may_be_handled=True)

"""Bounded accepted execution summaries without resolving business values."""

from sqlalchemy import or_, select, tuple_

from .schema import meta

TERMINAL = ("completed", "failed", "cancelled")


class ExecutionState:
    def __init__(self, queries):
        self.queries = queries

    async def read(self, iid, *, settled_version=None):
        q = self.queries
        async with q.view("accepted", control=True, invocation_id=iid) as (conn, overlay, _, frontier):
            selected = await overlay.get("invocations", invocation_id=iid)
            if selected is None:
                return None
            root_id = selected["root_invocation_id"]
            root = await overlay.get("invocations", invocation_id=root_id)
            if root is None:
                return None
            invs, nodes, waits = (meta.tables[name] for name in ("invocations", "active_nodes", "active_waits"))
            async def positions():
                rows = {r['invocation_id']: dict(r) for r in (await conn.execute(select(
                    invs.c.invocation_id, invs.c.session_id, invs.c.last_sequence
                ).where(invs.c.root_invocation_id == root_id))).mappings()}
                for row in overlay.rows.get('invocations', {}).values():
                    if row and row['root_invocation_id'] == root_id:
                        rows[row['invocation_id']] = row
                return tuple(sorted((r['session_id'], r['invocation_id'], r['last_sequence'])
                                    for r in rows.values() if r['last_sequence'] > 0))

            # Core confirms quiescence. SQL and the accepted tail must describe
            # exactly that graph before its lifecycle flags can be reused.
            graph_version = ()
            confirmed = False
            if settled_version is not None and root['status'] in TERMINAL:
                graph_version = await positions()
                confirmed = graph_version == settled_version
            if confirmed:
                settled = boundary = True
                waiting_boundary = False
                pending, cursor = [], None
            else:
                family = select(invs.c.invocation_id).where(invs.c.root_invocation_id == root_id)
                changed = {r["invocation_id"] for r in overlay.rows.get("invocations", {}).values()
                           if r and r["root_invocation_id"] == root_id}
                changed.add(root_id)
                member = lambda column: or_(column.in_(family), column.in_(changed))
                belongs = lambda row: row["invocation_id"] in changed
                def exists(name, *conditions):
                    table=meta.tables[name]
                    statement=select(1).select_from(table).where(*conditions)
                    changed_keys=tuple(overlay.rows[name])
                    if changed_keys:
                        columns=tuple(table.primary_key.columns)
                        excluded=(columns[0].in_([key[0] for key in changed_keys]) if len(columns)==1
                                  else tuple_(*columns).in_(changed_keys))
                        statement=statement.where(~excluded)
                    return statement.correlate(None).exists()

                owned=or_(invs.c.ownership_phase.is_(None),invs.c.ownership_phase.not_in(("abandoned","terminal")))
                flags=(await conn.execute(select(
                    exists('active_nodes',member(nodes.c.invocation_id),nodes.c.status.in_(("ready","running"))).label('active'),
                    exists('active_nodes',nodes.c.invocation_id==root_id,nodes.c.status.in_(("ready","running"))).label('root_active'),
                    exists('invocations',invs.c.root_invocation_id==root_id,owned,
                           or_(invs.c.status.is_(None),invs.c.status.not_in(TERMINAL))).label('unfinished'),
                    exists('invocations',invs.c.root_invocation_id==root_id,owned,
                           or_(invs.c.status.is_(None),invs.c.status=='created')).label('planned'),
                    exists('active_waits',member(waits.c.invocation_id),waits.c.status=='waiting',
                           waits.c.has_independent_case.is_(True)).label('independent')))).one()
                live=[row for row in overlay.rows['active_nodes'].values()
                      if row and belongs(row) and row['status'] in ('ready','running')]
                owned_rows=[row for row in overlay.rows['invocations'].values()
                            if row and row['root_invocation_id']==root_id
                            and row.get('ownership_phase') not in ('abandoned','terminal')]
                active=flags.active or bool(live)
                root_active=flags.root_active or any(row['invocation_id']==root_id for row in live)
                unfinished=flags.unfinished or any(row.get('status') not in TERMINAL for row in owned_rows)
                planned=flags.planned or any(row.get('status') in (None,'created') for row in owned_rows)
                independent=flags.independent or any(row and belongs(row) and row['status']=='waiting'
                    and row['has_independent_case'] for row in overlay.rows['active_waits'].values())
                pending,cursor=await q._rows(conn,overlay,'active_waits',{'status':'waiting'},
                    ('invocation_id','wait_id'),100,extra_where=(member(waits.c.invocation_id),),extra_filter=belongs)
                settled = root["status"] in TERMINAL and not unfinished and not active
                stopping = root.get("pending_outcome") in ("failed", "cancelled")
                boundary = settled or root.get("control_state") == "paused" and not active
                waiting_boundary = not stopping and root.get("control_state") != "pausing" and not root_active and bool(pending) and root["status"] not in TERMINAL
                if waiting_boundary:
                    boundary = boundary or not (active or planned) or bool(independent)
                if (boundary or waiting_boundary) and not graph_version:
                    graph_version = await positions()
            workflow = meta.tables["workflow_revisions"]
            workflow_id = await conn.scalar(select(workflow.c.workflow_id).where(workflow.c.revision_id == root["workflow_revision_id"]))
            return dict(invocation_id=root_id, session_id=root["session_id"], workflow_id=workflow_id,
                revision_id=root["workflow_revision_id"], entry_node_id=root["entry_node_id"],
                status=root["status"], control_state=root.get("control_state") or "active",
                pending_outcome=root.get("pending_outcome"), expected_version=root["last_sequence"],
                error_type=root.get("error_type"), error_code=root.get("error_code"),
                at_boundary=bool(boundary), settled=bool(settled), frontier=frontier,
                _waiting_boundary=waiting_boundary, _graph_version=graph_version,
                waits=[{key: w[key] for key in ("invocation_id", "wait_id", "occurrence_id", "kind", "deadline_at_us")} for w in pending],
                waits_truncated=cursor is not None)

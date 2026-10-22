"""Bounded accepted execution summaries without resolving business values."""

from sqlalchemy import or_, select

from .schema import meta

TERMINAL = ("completed", "failed", "cancelled")


class ExecutionState:
    def __init__(self, queries):
        self.queries = queries

    async def read(self, iid):
        q = self.queries
        async with q.view("accepted") as (conn, overlay, _, frontier):
            selected = await overlay.get("invocations", invocation_id=iid)
            if selected is None:
                return None
            root_id = selected["root_invocation_id"]
            root = await overlay.get("invocations", invocation_id=root_id)
            if root is None:
                return None
            invs, nodes, waits = (meta.tables[name] for name in ("invocations", "node_executions", "waits"))
            family = select(invs.c.invocation_id).where(invs.c.root_invocation_id == root_id)
            changed = {r["invocation_id"] for r in overlay.rows.get("invocations", {}).values()
                       if r and r["root_invocation_id"] == root_id}
            changed.add(root_id)
            member = lambda column: or_(column.in_(family), column.in_(changed))
            belongs = lambda row: row["invocation_id"] in changed
            active, _ = await q._rows(conn, overlay, "node_executions", {}, ("invocation_id", "occurrence_id"), 1,
                extra_where=(member(nodes.c.invocation_id), nodes.c.status.in_(("ready", "running"))),
                extra_filter=lambda r: belongs(r) and r["status"] in ("ready", "running"))
            root_active, _ = await q._rows(conn, overlay, "node_executions", {"invocation_id": root_id},
                ("invocation_id", "occurrence_id"), 1,
                extra_where=(nodes.c.status.in_(("ready", "running")),),
                extra_filter=lambda r: r["status"] in ("ready", "running"))
            unfinished, _ = await q._rows(conn, overlay, "invocations", {"root_invocation_id": root_id},
                ("invocation_id",), 1,
                extra_where=(or_(invs.c.status.is_(None), invs.c.status.not_in(TERMINAL)),
                             or_(invs.c.ownership_phase.is_(None), invs.c.ownership_phase.not_in(("abandoned", "terminal")))),
                extra_filter=lambda r: r.get("status") not in TERMINAL and r.get("ownership_phase") not in ("abandoned", "terminal"))
            pending, cursor = await q._rows(conn, overlay, "waits", {"status": "waiting"},
                ("invocation_id", "wait_id"), 100,
                extra_where=(member(waits.c.invocation_id),), extra_filter=belongs)
            independent, _ = await q._rows(conn, overlay, "waits", {"status": "waiting"},
                ("invocation_id", "wait_id"), 1,
                extra_where=(member(waits.c.invocation_id), waits.c.kind.not_in(("child", "any"))),
                extra_filter=lambda r: belongs(r) and r["kind"] not in ("child", "any"))
            # Planned children also count as work before their first Node starts.
            planned, _ = await q._rows(conn, overlay, "invocations", {"root_invocation_id": root_id},
                ("invocation_id",), 1,
                extra_where=(or_(invs.c.status.is_(None), invs.c.status == "created"),
                             or_(invs.c.ownership_phase.is_(None), invs.c.ownership_phase.not_in(("abandoned", "terminal")))),
                extra_filter=lambda r: r.get("status") in (None, "created") and r.get("ownership_phase") not in ("abandoned", "terminal"))
            settled = root["status"] in TERMINAL and not unfinished and not active
            stopping = root.get("pending_outcome") in ("failed", "cancelled")
            boundary = settled or root.get("control_state") == "paused" and not active
            waiting_boundary = not stopping and root.get("control_state") != "pausing" and not root_active and bool(pending) and root["status"] not in TERMINAL
            if waiting_boundary:
                boundary = boundary or not (active or planned) or bool(independent)
            workflow = meta.tables["workflow_revisions"]
            workflow_id = await conn.scalar(select(workflow.c.workflow_id).where(workflow.c.revision_id == root["workflow_revision_id"]))
            return dict(invocation_id=root_id, session_id=root["session_id"], workflow_id=workflow_id,
                revision_id=root["workflow_revision_id"], entry_node_id=root["entry_node_id"],
                status=root["status"], control_state=root.get("control_state") or "active",
                pending_outcome=root.get("pending_outcome"), expected_version=root["last_sequence"],
                error_type=root.get("error_type"), error_code=root.get("error_code"),
                at_boundary=bool(boundary), settled=bool(settled), frontier=frontier,
                _waiting_boundary=waiting_boundary,
                waits=[{key: w[key] for key in ("invocation_id", "wait_id", "occurrence_id", "kind", "deadline_at_us")} for w in pending],
                waits_truncated=cursor is not None)

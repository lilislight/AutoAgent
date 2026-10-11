"""Explicit whole-history maintenance; canonical events are immutable."""

from __future__ import annotations

from sqlalchemy import delete, func, select, update

from .models import ConflictError
from .projections import ProjectionBatch, light_record
from .schema import meta
from .storage import PROJECTION_VERSION


class Maintenance:
    def __init__(self, writer):
        self.writer = writer
        self.storage = writer.storage

    async def run(self, operation, *args):
        async with self.writer.admission:
            self.writer.check_open()
            await self.writer.flush()
            async with self.writer.mutation, self.storage.engine.begin() as conn:
                return await operation(conn, *args)

    async def gc(self, conn):
        refs = meta.tables["artifact_refs"]
        deps = meta.tables["artifact_dependencies"]
        arts = meta.tables["artifacts"]
        # A recursive CTE performs reachability without a global Python body cache.
        reachable = (
            select(refs.c.artifact_hash.label("hash"))
            .distinct()
            .cte("reachable", recursive=True)
        )
        reachable = reachable.union(
            select(deps.c.child_hash.label("hash")).join(
                reachable, deps.c.parent_hash == reachable.c.hash
            )
        )
        unreachable = select(arts.c.hash).where(
            ~arts.c.hash.in_(select(reachable.c.hash))
        )
        count = await conn.scalar(
            select(func.count()).select_from(arts).where(arts.c.hash.in_(unreachable))
        )
        await conn.execute(delete(deps).where(deps.c.parent_hash.in_(unreachable)))
        await conn.execute(delete(arts).where(arts.c.hash.in_(unreachable)))
        return count

    async def _refs_delete(self, conn, kind, identities):
        refs = meta.tables["artifact_refs"]
        await conn.execute(
            delete(refs).where(
                refs.c.owner_kind == kind, refs.c.owner_id.in_(identities)
            )
        )

    async def delete_definition(self, conn, kind, identity):
        if kind == "workflow_revision":
            inv = meta.tables["invocations"]
            members = meta.tables["checkpoint_sessions"]
            if await conn.scalar(
                select(inv.c.invocation_id)
                .where(inv.c.workflow_revision_id == identity)
                .limit(1)
            ) or await conn.scalar(
                select(members.c.checkpoint_id)
                .where(members.c.workflow_revision_id == identity)
                .limit(1)
            ):
                raise ConflictError("Workflow revision is referenced by history.")
            name, key = "workflow_revisions", "revision_id"
        else:
            events = meta.tables["runtime_events"]
            calls = meta.tables["operator_calls"]
            if await conn.scalar(
                select(events.c.event_id)
                .where(events.c.operator_definition_id == identity)
                .limit(1)
            ) or await conn.scalar(
                select(calls.c.call_id)
                .where(calls.c.operator_definition_id == identity)
                .limit(1)
            ):
                raise ConflictError("Operator definition is referenced by history.")
            name, key = "operator_definitions", "definition_id"
        await self._refs_delete(conn, kind, [identity])
        await conn.execute(
            delete(meta.tables[name]).where(meta.tables[name].c[key] == identity)
        )
        await self.gc(conn)

    async def delete_checkpoint(self, conn, identity):
        members = meta.tables["checkpoint_sessions"]
        covered = (
            await conn.execute(
                select(members.c.session_id, members.c.sequence).where(
                    members.c.checkpoint_id == identity
                )
            )
        ).all()
        for sid, sequence in covered:
            session = await self.storage.row(conn, "sessions", session_id=sid)
            if session and session["history_start_sequence"] > 1:
                other = await conn.scalar(
                    select(members.c.checkpoint_id)
                    .where(
                        members.c.session_id == sid,
                        members.c.checkpoint_id != identity,
                        members.c.sequence >= session["history_start_sequence"] - 1,
                    )
                    .limit(1)
                )
                if not other:
                    raise ConflictError("Cannot delete the only recovery anchor.")
        # Import-seeded detail references must remain valid even with another anchor.
        for name in ("invocations", "node_executions", "operator_calls", "waits"):
            table = meta.tables[name]
            for col in table.columns:
                if col.name.endswith("_ref") and await conn.scalar(
                    select(func.count())
                    .select_from(table)
                    .where(
                        col["kind"].as_string() == "checkpoint",
                        col["id"].as_string() == identity,
                    )
                ):
                    raise ConflictError(
                        "Checkpoint is referenced by execution details."
                    )
        await conn.execute(delete(members).where(members.c.checkpoint_id == identity))
        await self._refs_delete(conn, "checkpoint", [identity])
        await conn.execute(
            delete(meta.tables["checkpoints"]).where(
                meta.tables["checkpoints"].c.checkpoint_id == identity
            )
        )
        await self.gc(conn)

    async def purge(self, conn, root):
        inv = meta.tables["invocations"]
        sessions = meta.tables["sessions"]
        row = await self.storage.row(conn, "invocations", invocation_id=root)
        if row is None:
            return
        if row["root_invocation_id"] != root:
            raise ConflictError("Purge requires a Root Invocation identity.")
        rows = (
            (await conn.execute(select(inv).where(inv.c.root_invocation_id == root)))
            .mappings()
            .all()
        )
        if any(
            r["status"] not in ("completed", "failed", "cancelled")
            and r.get("ownership_phase") != "abandoned"
            for r in rows
        ):
            raise ConflictError(
                "Root Graph must be stopped before purge; Host must also unload it."
            )
        ids, sids = {r["invocation_id"] for r in rows}, {r["session_id"] for r in rows}
        if await conn.scalar(
            select(inv.c.invocation_id)
            .where(inv.c.session_id.in_(sids), ~inv.c.invocation_id.in_(ids))
            .limit(1)
        ):
            raise ConflictError(
                "A Session contains other Root history; partial Session purge would break the Event prefix."
            )
        events = meta.tables["runtime_events"]
        users = meta.tables["user_events"]
        cp = meta.tables["checkpoints"]
        members = meta.tables["checkpoint_sessions"]
        event_ids = select(events.c.event_id).where(events.c.session_id.in_(sids))
        user_ids = select(users.c.event_id).where(users.c.session_id.in_(sids))
        checkpoint_ids = select(members.c.checkpoint_id).where(
            members.c.session_id.in_(sids)
        )
        cpids = (await conn.execute(checkpoint_ids.distinct())).scalars().all()
        for name in ("graph_changes", "edge_evaluations", "operator_calls", "waits"):
            table = meta.tables[name]
            await conn.execute(delete(table).where(table.c.invocation_id.in_(ids)))
        # Break only projection ownership links, then remove the dependent rows.
        await conn.execute(
            update(inv)
            .where(inv.c.invocation_id.in_(ids))
            .values(
                parent_invocation_id=None,
                parent_occurrence_id=None,
                creation_id=None,
                unit_index=None,
                ownership_phase=None,
                root_invocation_id=inv.c.invocation_id,
            )
        )
        await conn.execute(
            delete(meta.tables["node_executions"]).where(
                meta.tables["node_executions"].c.invocation_id.in_(ids)
            )
        )
        await conn.execute(delete(inv).where(inv.c.invocation_id.in_(ids)))
        await self._refs_delete(conn, "runtime_event", event_ids)
        await self._refs_delete(conn, "user_event", user_ids)
        await self._refs_delete(conn, "checkpoint", cpids)
        await conn.execute(delete(events).where(events.c.session_id.in_(sids)))
        await conn.execute(delete(users).where(users.c.session_id.in_(sids)))
        await conn.execute(delete(members).where(members.c.checkpoint_id.in_(cpids)))
        await conn.execute(delete(cp).where(cp.c.checkpoint_id.in_(cpids)))
        await conn.execute(delete(sessions).where(sessions.c.session_id.in_(sids)))
        await self.gc(conn)

    async def rebuild(self, conn):
        inv = meta.tables["invocations"]
        await conn.execute(
            update(inv).values(
                parent_invocation_id=None,
                parent_occurrence_id=None,
                creation_id=None,
                unit_index=None,
                ownership_phase=None,
                root_invocation_id=inv.c.invocation_id,
            )
        )
        for name in (
            "graph_changes",
            "edge_evaluations",
            "operator_calls",
            "waits",
            "node_executions",
            "invocations",
            "sessions",
        ):
            await conn.execute(delete(meta.tables[name]))
        # Seed imported histories only; complete Event histories start at sequence 1.
        members = meta.tables["checkpoint_sessions"]
        events = meta.tables["runtime_events"]
        anchors = (
            (
                await conn.execute(
                    select(members.c.checkpoint_id)
                    .where(
                        ~members.c.session_id.in_(
                            select(events.c.session_id).where(events.c.sequence == 1)
                        )
                    )
                    .distinct()
                    .order_by(members.c.checkpoint_id)
                )
            )
            .scalars()
            .all()
        )
        for checkpoint_id in anchors:
            record = await self.storage.source(conn, "checkpoint", checkpoint_id)
            light = await self.writer.cpu(
                lambda record=record: light_record(record, availability=True)
            )
            batch = ProjectionBatch(conn, self.storage.source)
            await batch.checkpoint(light, checkpoint_id, anchor=True)
            await self.storage.projections(conn, batch)
        after = 0
        while True:
            rows = (
                (
                    await conn.execute(
                        select(events)
                        .where(events.c.store_order > after)
                        .order_by(events.c.store_order)
                        .limit(256)
                    )
                )
                .mappings()
                .all()
            )
            if not rows:
                break
            batch = ProjectionBatch(conn, self.storage.source)
            for row in rows:
                record = await self.storage.source(
                    conn, "runtime_event", row["event_id"]
                )
                light = await self.writer.cpu(light_record, record)
                session = await batch.get("sessions", session_id=row["session_id"])
                if session and row["sequence"] <= session["durable_sequence"]:
                    continue
                expected = (session or {}).get("durable_sequence", 0) + 1
                if row["sequence"] != expected:
                    raise ConflictError("Rebuild encounters a missing history prefix.")
                await batch.runtime(
                    light, row["store_order"], row["operator_definition_id"]
                )
            await self.storage.projections(conn, batch)
            after = rows[-1]["store_order"]
        await conn.execute(
            update(meta.tables["persistence_meta"]).values(
                projection_version=PROJECTION_VERSION
            )
        )

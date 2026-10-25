"""Explicit whole-history maintenance; canonical events are immutable."""

from __future__ import annotations

from uuid import uuid4

from sqlalchemy import delete, func, literal, or_, select, update

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
            # Maintenance owns mutation; no cached metadata may outlive a
            # definition deletion or be reused by a rebuild SQL snapshot.
            self.storage.graph_cache.clear()
            self.storage.metadata_cache.clear()
            self.storage._store_order = None
            async with self.writer.mutation, self.storage.engine.begin() as conn:
                result = await operation(conn, *args)
                if operation.__name__ in ("rebuild", "purge"):
                    await conn.execute(update(meta.tables['persistence_meta']).values(store_id=uuid4().hex))
            async with self.writer.condition:
                self.writer.commit_revision += 1
                self.writer.condition.notify_all()
            return result

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

    async def checkpoint_blockers(self, conn, identity):
        """Check only graph metadata and indexed member Invocations, never bodies."""
        members, sessions = (meta.tables[name] for name in ('checkpoint_sessions', 'sessions'))
        covered = (await conn.execute(
            select(members.c.session_id, sessions.c.history_start_sequence)
            .join(sessions, sessions.c.session_id == members.c.session_id)
            .where(members.c.checkpoint_id == identity)
        )).all()
        for sid, start in covered:
            if start > 1 and not await conn.scalar(
                select(members.c.checkpoint_id).where(
                    members.c.session_id == sid, members.c.checkpoint_id != identity,
                    members.c.sequence >= start - 1,
                ).limit(1)
            ):
                return ['required_anchor']
        # Only imported snapshots seed detail references. Their referenced
        # objects belong to these member Invocations, all indexed by ID.
        invocations = select(members.c.invocation_id).where(members.c.checkpoint_id == identity)
        for name in ('invocations', 'node_executions', 'operator_calls', 'waits'):
            table = meta.tables[name]
            refs = [col for col in table.columns if col.name.endswith('_ref')]
            if refs and await conn.scalar(
                select(literal(1)).select_from(table).where(
                    table.c.invocation_id.in_(invocations),
                    or_(*((col['kind'].as_string() == 'checkpoint') &
                           (col['id'].as_string() == identity) for col in refs)),
                ).limit(1)
            ):
                return ['referenced_details']
        return []

    async def delete_checkpoint(self, conn, identity, *, collect=True):
        table = meta.tables['checkpoints']
        if not await conn.scalar(select(table.c.checkpoint_id).where(table.c.checkpoint_id == identity)):
            return False
        blockers = await self.checkpoint_blockers(conn, identity)
        if blockers:
            message = ('Cannot delete the only recovery anchor.' if blockers[0] == 'required_anchor'
                       else 'Checkpoint is referenced by execution details.')
            raise ConflictError(message)
        members = meta.tables['checkpoint_sessions']
        await conn.execute(delete(members).where(members.c.checkpoint_id == identity))
        await self._refs_delete(conn, "checkpoint", [identity])
        await conn.execute(delete(table).where(table.c.checkpoint_id == identity))
        if collect:
            await self.gc(conn)
        return True

    async def purge(self, conn, root):
        from .codec import canonical
        events = meta.tables['runtime_events']
        reference = canonical({'source_root_invocation_id':root}).decode()[1:-1]
        if await conn.scalar(select(events.c.event_id).where(events.c.event_name == 'invocation.forked',
                events.c.payload_text.contains(reference, autoescape=True)).limit(1)):
            raise ConflictError('Source history is referenced by a Fork; delete dependent Fork histories first.')
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
        from sqlalchemy import inspect

        if await conn.run_sync(lambda sync: inspect(sync).has_table("llm_calls")):
            from .llm_calls import llm_calls

            await conn.execute(delete(llm_calls).where(llm_calls.c.invocation_id.in_(ids)))
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

"""Agent Session preferences, metadata, and immutable invocation model bindings."""

import time
from uuid import uuid4

from sqlalchemy import CheckConstraint, Index, LargeBinary, MetaData, Table, delete, insert, select, text, update
from sqlalchemy import case, func, tuple_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from autoagent.agent.context import restore
from autoagent.agent_app.models import ApplicationSettingsRecord, ApplicationSessionSettings
from autoagent.agent_app.images import UploadedImage
from autoagent.agent.workspace import WorkspaceBinding
from autoagent.host.persistence.models import ConflictError
from autoagent.host.persistence.schema import B, J, c, meta

context_meta = MetaData()
sessions = Table("agent_app_sessions", context_meta,
    c("session_id", nullable=False, pk=True), c("version", B, False), c("settings", J, False),
    c("workspace", J), c("title"), c("metadata_version", B, False), c("updated_at_us", B, False),
    c("fork_source_session_id"), c("fork_source_invocation_id"), c("fork_source_sequence", B),
    CheckConstraint("version >= 1"), CheckConstraint("metadata_version >= 0"))
fork_index = Index("agent_app_sessions_fork_source", sessions.c.fork_source_session_id)
invocation_bindings = Table("agent_app_invocation_bindings", context_meta,
    c("binding_id", nullable=False, pk=True), c("session_id", nullable=False),
    c("invocation_id", nullable=False, unique=True), c("model", J, False), c("workspace", J),
    c("bindings", J, False), c("activation_id", nullable=False), c("created_at_us", B, False))
Index("agent_app_invocation_bindings_session", invocation_bindings.c.session_id, invocation_bindings.c.created_at_us)
workspaces = Table("agent_app_workspaces", context_meta,
    c("workspace_id", nullable=False, pk=True), c("configuration", J, False), c("updated_at_us", B, False))
resource_configs = Table("agent_app_resource_configs", context_meta,
    c("id", nullable=False, pk=True), c("version", B, False),
    c("configuration", J, False), c("updated_at_us", B, False))
mcp_catalogs = Table("agent_app_mcp_catalogs", context_meta,
    c("connection_id", nullable=False, pk=True), c("catalog", J, False))
images = Table("agent_app_images", context_meta, c("id", nullable=False, pk=True),
    c("session_id", nullable=False), c("mime_type", nullable=False), c("data", LargeBinary, False))
Index("agent_app_images_session", images.c.session_id)


class ApplicationContextStorage:
    def __init__(self, writer, queries):
        self.writer, self.queries, self._ready = writer, queries, False

    async def _prepare(self, conn):
        if not self._ready:
            from sqlalchemy import inspect
            from autoagent.host.persistence.models import FormatError
            await conn.run_sync(context_meta.create_all)
            for table in context_meta.tables.values():
                columns = await conn.run_sync(lambda sync, name=table.name:
                    {item["name"] for item in inspect(sync).get_columns(name)})
                if table is sessions:
                    for name in ("fork_source_session_id", "fork_source_invocation_id", "fork_source_sequence"):
                        if name not in columns:
                            sql_type = sessions.c[name].type.compile(dialect=conn.dialect)
                            await conn.execute(text(f"ALTER TABLE agent_app_sessions ADD COLUMN {name} {sql_type}"))
                            columns.add(name)
                    await conn.run_sync(lambda sync: fork_index.create(sync, checkfirst=True))
                if columns != set(table.columns.keys()):
                    raise FormatError("Agent storage does not match the current schema.")


    async def purge_history(self, conn, invocation_ids, session_ids):
        """Participate in Host history deletion using the same SQL transaction."""
        from .queue import messages
        if await self._exists(conn, sessions):
            dependent = await conn.scalar(select(sessions.c.session_id).where(
                sessions.c.session_id.not_in(session_ids),
                (sessions.c.fork_source_session_id.in_(session_ids)
                    | sessions.c.fork_source_invocation_id.in_(invocation_ids))).limit(1))
            if dependent:
                raise ConflictError("Conversation history is referenced by a Fork; delete dependent Forks first.")
        for table, field, values in ((invocation_bindings, "invocation_id", invocation_ids),
                (sessions, "session_id", session_ids), (messages, "session_id", session_ids),
                (images, "session_id", session_ids)):
            if await self._exists(conn, table):
                await conn.execute(delete(table).where(table.c[field].in_(values)))


    async def prepare(self):
        """Initialize and validate the current product schema without rewriting history."""
        self.writer.check_open()
        if self.writer.read_only:
            return
        async with self.writer.mutation:
            async with self.writer.storage.transaction() as conn:
                await self._prepare(conn)
            self._ready = True


    @staticmethod
    async def _exists(conn, table):
        from sqlalchemy import inspect
        return await conn.run_sync(lambda sync: inspect(sync).has_table(table.name))

    @staticmethod
    async def _store_images(conn, session_id, uploads):
        if uploads:
            statement = (sqlite_insert if conn.dialect.name == "sqlite" else pg_insert)(images)
            await conn.execute(statement.on_conflict_do_nothing(index_elements=[images.c.id]), [
                dict(id=image.id, session_id=session_id, mime_type=image.mime_type, data=image.data) for image in uploads])

    async def image(self, image_id, *, session_id=None):
        engine = self.writer.storage.query_engine or self.writer.storage.engine
        async with engine.connect() as conn:
            if not self._ready and not await self._exists(conn, images):
                return None
            query = select(images).where(images.c.id == image_id)
            if session_id is not None:
                query = query.where(images.c.session_id == session_id)
            row = (await conn.execute(query)).mappings().first()
            return UploadedImage(row["id"], row["mime_type"], bytes(row["data"])) if row else None

    async def resource_records(self):
        table = resource_configs
        engine = self.writer.storage.query_engine or self.writer.storage.engine
        async with engine.connect() as conn:
            if not await self._exists(conn, table):
                return []
            return [dict(row) for row in (await conn.execute(select(table)
                .order_by(table.c.id))).mappings()]

    async def save_resource(self, resource_id, configuration, expected_version):
        table = resource_configs
        if type(expected_version) is not int or expected_version < 0:
            raise ValueError("expected_version must be a nonnegative integer.")
        self.writer.check_open()
        if self.writer.read_only:
            raise PermissionError("A read-only Host cannot configure Agent resources.")
        async with self.writer.mutation:
            async with self.writer.storage.transaction() as conn:
                await self._prepare(conn)
                row = (await conn.execute(select(table).where(table.c.id == resource_id))).mappings().first()
                if (row["version"] if row else 0) != expected_version:
                    raise ConflictError("Agent resource configuration version conflict.")
                if configuration is None:
                    if row is None:
                        raise ConflictError("Agent resource configuration is unavailable.")
                    await conn.execute(delete(mcp_catalogs).where(mcp_catalogs.c.connection_id == resource_id))
                    await conn.execute(delete(table).where(table.c.id == resource_id))
                    result = None
                else:
                    result = dict(id=resource_id, version=expected_version + 1,
                        configuration=configuration, updated_at_us=time.time_ns() // 1000)
                    if row:
                        await conn.execute(update(table).where(table.c.id == resource_id).values(**result))
                    else:
                        await conn.execute(insert(table).values(**result))
            self._ready = True
        return result

    async def mcp_catalogs(self):
        engine = self.writer.storage.query_engine or self.writer.storage.engine
        async with engine.connect() as conn:
            if not await self._exists(conn, mcp_catalogs):
                return {}
            return dict((await conn.execute(select(mcp_catalogs.c.connection_id, mcp_catalogs.c.catalog))).tuples().all())

    async def save_mcp_catalog(self, connection_id, catalog):
        self.writer.check_open()
        if self.writer.read_only:
            return
        async with self.writer.mutation:
            async with self.writer.storage.transaction() as conn:
                await self._prepare(conn)
                if (await conn.execute(select(resource_configs.c.id).where(
                        resource_configs.c.id == connection_id))).scalar_one_or_none() is None:
                    raise ConflictError("MCP server no longer exists.")
                match = mcp_catalogs.c.connection_id == connection_id
                exists = (await conn.execute(select(mcp_catalogs.c.connection_id).where(match))).scalar_one_or_none()
                if exists is None:
                    await conn.execute(insert(mcp_catalogs).values(connection_id=connection_id, catalog=catalog))
                else:
                    await conn.execute(update(mcp_catalogs).where(match).values(catalog=catalog))
            self._ready = True

    async def workspace_records(self):
        engine = self.writer.storage.query_engine or self.writer.storage.engine
        async with engine.connect() as conn:
            if not await self._exists(conn, workspaces):
                return []
            return list((await conn.execute(select(workspaces.c.configuration))).scalars())

    async def register_workspace(self, configuration):
        self.writer.check_open()
        if self.writer.read_only:
            raise PermissionError("A read-only Host cannot register Workspaces.")
        async with self.writer.mutation:
            async with self.writer.storage.transaction() as conn:
                await self._prepare(conn)
                wid = configuration["id"]
                exists = await conn.scalar(select(workspaces.c.workspace_id).where(workspaces.c.workspace_id == wid))
                values = dict(configuration=configuration, updated_at_us=time.time_ns() // 1000)
                if exists is not None:
                    await conn.execute(update(workspaces).where(workspaces.c.workspace_id == wid).values(**values))
                else:
                    await conn.execute(insert(workspaces).values(workspace_id=wid, **values))
            self._ready = True

    @classmethod
    async def _session_workspace(cls, conn, sid):
        if not await cls._exists(conn, invocation_bindings):
            return False, None
        row = (await conn.execute(select(invocation_bindings.c.workspace).join(meta.tables["runtime_events"],
            meta.tables["runtime_events"].c.event_id == invocation_bindings.c.activation_id).where(
                invocation_bindings.c.session_id == sid).order_by(invocation_bindings.c.created_at_us).limit(1))).first()
        return (True, restore(WorkspaceBinding, row[0]) if row[0] is not None else None) if row else (False, None)

    async def _first_title(self, conn, sid):
        inv = meta.tables["invocations"]
        row = (await conn.execute(select(inv.c.input_ref).where(inv.c.session_id == sid,
            inv.c.parent_invocation_id.is_(None), inv.c.started_at_us.is_not(None))
            .order_by(inv.c.started_at_us, inv.c.invocation_id).limit(1))).first()
        if not row:
            return None
        value = (await self.queries._resolve(conn, (), row[0]))["value"]
        return self.title(value.get("message", "")) if isinstance(value, dict) else None

    @staticmethod
    def title(message):
        return " ".join(message.split())[:100] or None

    async def completed_tool_results(self, sid, current_iid):
        invocations, nodes, calls = (meta.tables[name] for name in ("invocations", "node_executions", "operator_calls"))
        engine = self.writer.storage.query_engine or self.writer.storage.engine
        async with engine.connect() as conn:
            previous = await conn.scalar(select(invocations.c.invocation_id).where(
                invocations.c.session_id == sid, invocations.c.invocation_id != current_iid,
                invocations.c.started_at_us.is_not(None)).order_by(invocations.c.started_at_us.desc()).limit(1))
            if previous is None:
                return []
            llm_end = await conn.scalar(select(nodes.c.completed_at_us).where(nodes.c.invocation_id == previous,
                nodes.c.node_id == "llm_call", nodes.c.status == "completed").order_by(nodes.c.completed_at_us.desc()).limit(1))
            if llm_end is None:
                return []
            rows = await conn.execute(select(calls.c.output_ref).join(nodes,
                (calls.c.invocation_id == nodes.c.invocation_id) & (calls.c.occurrence_id == nodes.c.occurrence_id)).where(
                calls.c.invocation_id == previous, calls.c.status == "completed", calls.c.started_at_us >= llm_end,
                nodes.c.node_id.in_(("tool_execute", "tool_execute_approved"))).order_by(calls.c.completed_at_us))
            result = []
            for row in rows:
                if row[0] is not None:
                    value = (await self.queries._resolve(conn, (), row[0]))["value"]
                    if isinstance(value, dict):
                        result.append(value)
            return result

    async def remember_model(self, sid, model):
        self.writer.check_open()
        async with self.writer.mutation:
            async with self.writer.storage.transaction() as conn:
                await self._prepare(conn)
                await self._remember_model(conn, sid, model)
                row = (await conn.execute(select(sessions).where(sessions.c.session_id == sid))).mappings().one()
                return ApplicationSettingsRecord(session_id=sid, version=row["version"],
                    settings=restore(ApplicationSessionSettings, row["settings"]),
                    workspace=restore(WorkspaceBinding, row["workspace"]) if row["workspace"] else None,
                    title=row["title"], metadata_version=row["metadata_version"],
                    **{key: row[key] for key in ("fork_source_session_id", "fork_source_invocation_id", "fork_source_sequence")})

    @staticmethod
    async def _remember_model(conn, sid, model):
        row = (await conn.execute(select(sessions).where(sessions.c.session_id == sid))).mappings().one()
        if row["settings"]["model"] != model:
            await conn.execute(update(sessions).where(sessions.c.session_id == sid).values(
                settings={**row["settings"], "model": model}, version=row["version"] + 1, updated_at_us=time.time_ns() // 1000))

    async def settings(self, sid, defaults):
        engine = self.writer.storage.query_engine or self.writer.storage.engine
        async with engine.connect() as conn:
            row = None
            if await self._exists(conn, sessions):
                row = (await conn.execute(select(sessions).where(sessions.c.session_id == sid))).mappings().first()
            fixed, workspace = await self._session_workspace(conn, sid)
            return ApplicationSettingsRecord(session_id=sid, version=row["version"] if row else 0,
                settings=restore(ApplicationSessionSettings, row["settings"]) if row else ApplicationSessionSettings.from_runtime(defaults),
                workspace=(restore(WorkspaceBinding, row["workspace"]) if row["workspace"] is not None else None)
                    if row else workspace if fixed else defaults.workspace,
                title=row["title"] if row and row["title"] else await self._first_title(conn, sid),
                metadata_version=row["metadata_version"] if row else 0,
                **({key: row[key] for key in ("fork_source_session_id", "fork_source_invocation_id", "fork_source_sequence")} if row else {}))

    async def configure(self, sid, settings, expected_version, workspace):
        self.writer.check_open()
        if self.writer.read_only:
            raise PermissionError("A read-only Host cannot configure Agent settings.")
        async with self.writer.mutation:
            async with self.writer.storage.transaction() as conn:
                await self._prepare(conn)
                row = (await conn.execute(select(sessions).where(sessions.c.session_id == sid))).mappings().first()
                version = row["version"] if row else 0
                if expected_version is not None and expected_version != version:
                    raise ConflictError("Agent Session settings version conflict.")
                fixed, saved_workspace = (True, row["workspace"]) if row else await self._session_workspace(conn, sid)
                saved_workspace = restore(WorkspaceBinding, saved_workspace) if isinstance(saved_workspace, dict) else saved_workspace
                if fixed and workspace != saved_workspace:
                    raise ConflictError("A Session's Workspace binding cannot be changed; create a new Session.")
                values = dict(version=version + 1, settings=settings.model_dump(mode="json"),
                    updated_at_us=time.time_ns() // 1000)
                if row:
                    await conn.execute(update(sessions).where(sessions.c.session_id == sid).values(**values))
                else:
                    await conn.execute(insert(sessions).values(session_id=sid, workspace=workspace.model_dump(mode="json") if workspace else None,
                        title=None, metadata_version=0, **values))
            self._ready = True
        return await self.settings(sid, None)

    async def initialize_settings(self, sid, settings, workspace, message=None, *, uploads=()):
        self.writer.check_open()
        if self.writer.read_only:
            raise PermissionError("A read-only Host cannot create conversations.")
        async with self.writer.mutation:
            async with self.writer.storage.transaction() as conn:
                await self._prepare(conn)
                await self._initialize_settings(conn, sid, settings, workspace, message)
                await self._store_images(conn, sid, uploads)
            self._ready = True
        return await self.settings(sid, None)

    @classmethod
    async def _initialize_settings(cls, conn, sid, settings, workspace, message):
        row = (await conn.execute(select(sessions).where(sessions.c.session_id == sid))).mappings().first()
        serialized = workspace.model_dump(mode="json") if workspace else None
        if row:
            if row["workspace"] != serialized:
                raise ConflictError("A Session's Workspace binding cannot be changed; create a new Session.")
            if row["title"] is None and message:
                await conn.execute(update(sessions).where(sessions.c.session_id == sid).values(
                    title=cls.title(message), metadata_version=row["metadata_version"] + 1))
        else:
            fixed, previous = await cls._session_workspace(conn, sid)
            if fixed and previous != workspace:
                raise ConflictError("A Session's Workspace binding cannot be changed.")
            await conn.execute(insert(sessions).values(session_id=sid, version=1,
                settings=settings.model_dump(mode="json"), workspace=serialized,
                title=cls.title(message) if message else None, metadata_version=1,
                updated_at_us=time.time_ns() // 1000))

    async def rename(self, sid, title, expected_version, settings, workspace):
        title = " ".join(title.split())
        if not title or len(title) > 120:
            raise ValueError("Conversation title must contain 1–120 characters.")
        self.writer.check_open()
        if self.writer.read_only:
            raise PermissionError("A read-only Host cannot rename conversations.")
        async with self.writer.mutation:
            async with self.writer.storage.transaction() as conn:
                await self._prepare(conn)
                row = (await conn.execute(select(sessions).where(sessions.c.session_id == sid))).mappings().first()
                if (row["metadata_version"] if row else 0) != expected_version:
                    raise ConflictError("Conversation metadata version conflict.")
                if row is None:
                    await conn.execute(insert(sessions).values(session_id=sid, version=1,
                        settings=settings.model_dump(mode="json"), workspace=workspace.model_dump(mode="json") if workspace else None,
                        title=title, metadata_version=1, updated_at_us=time.time_ns() // 1000))
                else:
                    await conn.execute(update(sessions).where(sessions.c.session_id == sid).values(
                        title=title, metadata_version=expected_version + 1))
        return await self.settings(sid, None)

    async def conversation_sessions(self, workspace_id, *, all_workspaces=False, limit=20, cursor=None):
        from autoagent.agent_app import BUILTIN_AGENT_WORKFLOW_ID
        from autoagent.host.persistence.models import Page
        from autoagent.host.persistence.queries import limit_value
        from autoagent.host.persistence.tracing import trace_cursor

        limit_value(limit)
        trace_cursor(cursor)

        inv, revisions, events = (meta.tables[name] for name in
            ("invocations", "workflow_revisions", "runtime_events"))
        scope = select(inv.c.session_id).join(revisions).where(inv.c.status.is_not(None)).group_by(
            inv.c.session_id).having(
                func.min(revisions.c.workflow_id) == BUILTIN_AGENT_WORKFLOW_ID,
                func.max(revisions.c.workflow_id) == BUILTIN_AGENT_WORKFLOW_ID,
                func.count(inv.c.parent_invocation_id) == 0)
        configured_workspace = sessions.c.workspace["workspace_id"].as_string()
        if not all_workspaces:
            accepted_workspace = invocation_bindings.c.workspace["workspace_id"].as_string()
            configured = select(sessions.c.session_id).where(
                configured_workspace.is_not(None) if workspace_id is None else configured_workspace == workspace_id)
            accepted = select(invocation_bindings.c.session_id).join(events,
                events.c.event_id == invocation_bindings.c.activation_id).where(
                    invocation_bindings.c.session_id.not_in(select(sessions.c.session_id)),
                    accepted_workspace.is_not(None) if workspace_id is None else accepted_workspace == workspace_id)
            bound = configured.union(accepted)
            scope = scope.where(inv.c.session_id.not_in(bound) if workspace_id is None else inv.c.session_id.in_(bound))
        activity = func.max(func.coalesce(events.c.occurred_at_us, inv.c.completed_at_us, inv.c.started_at_us, 0))
        physical = select(inv.c.session_id, func.count().label("invocation_count"),
            activity.label("updated_at_us")).select_from(inv.outerjoin(events,
                (events.c.session_id == inv.c.session_id) & (events.c.sequence == inv.c.last_sequence))).where(
                    inv.c.session_id.in_(scope), inv.c.status.is_not(None)).group_by(inv.c.session_id)
        drafts = select(sessions.c.session_id, func.cast(0, B).label("invocation_count"), sessions.c.updated_at_us).where(
            sessions.c.fork_source_session_id.is_not(None),
            sessions.c.session_id.not_in(select(inv.c.session_id).where(inv.c.status.is_not(None))))
        if not all_workspaces:
            drafts = drafts.where(configured_workspace.is_(None) if workspace_id is None else configured_workspace == workspace_id)
        combined = physical.union_all(drafts).subquery()
        statement = select(combined)
        if cursor is not None:
            statement = statement.where(tuple_(combined.c.updated_at_us, combined.c.session_id) < cursor)
        async with self.queries.view() as (conn, _, _, frontier):
            rows = (await conn.execute(statement.order_by(combined.c.updated_at_us.desc(),
                combined.c.session_id.desc()).limit(limit + 1))).mappings().all()
            total = await conn.scalar(select(func.count()).select_from(combined))
        page = rows[:limit]
        position = (page[-1]["updated_at_us"], page[-1]["session_id"]) if len(rows) > limit else None
        return Page(tuple(dict(row) for row in page), position, frontier, total)

    async def conversation_summaries(self, workspace_id, *, all_workspaces=False, limit=20, cursor=None):
        from dataclasses import replace

        page = await self.conversation_sessions(workspace_id, all_workspaces=all_workspaces, limit=limit, cursor=cursor)
        if not page.items:
            return page
        inv, events = (meta.tables[name] for name in ("invocations", "runtime_events"))
        ids = [row["session_id"] for row in page.items]
        latest = select(inv.c.session_id, inv.c.invocation_id, inv.c.status, inv.c.control_state, inv.c.last_sequence,
            func.row_number().over(partition_by=inv.c.session_id,
                order_by=(func.coalesce(inv.c.started_at_us, 0).desc(), inv.c.invocation_id.desc())).label("rank")
            ).where(inv.c.session_id.in_(ids), inv.c.status.is_not(None)).subquery()
        initial_workspace = select(invocation_bindings.c.workspace["workspace_id"].as_string()).join(events,
            events.c.event_id == invocation_bindings.c.activation_id).where(
                invocation_bindings.c.session_id == latest.c.session_id).order_by(
                    invocation_bindings.c.created_at_us).limit(1).scalar_subquery()
        initial_input = select(inv.c.input_ref).where(inv.c.session_id == latest.c.session_id,
            inv.c.parent_invocation_id.is_(None), inv.c.started_at_us.is_not(None)).order_by(
                inv.c.started_at_us, inv.c.invocation_id).limit(1).scalar_subquery()
        statement = select(latest.c.session_id, latest.c.invocation_id, latest.c.status, latest.c.control_state,
            latest.c.last_sequence, sessions.c.title, func.coalesce(sessions.c.metadata_version, 0).label("metadata_version"),
            case((sessions.c.session_id.is_not(None), sessions.c.workspace["workspace_id"].as_string()),
                else_=initial_workspace).label("workspace_id"),
            case((sessions.c.title.is_(None), initial_input), else_=None).label("initial_input")
            ).select_from(latest.outerjoin(sessions, latest.c.session_id == sessions.c.session_id)).where(latest.c.rank == 1)
        async with self.queries.view() as (conn, _, _, _):
            details = {}
            for row in (await conn.execute(statement)).mappings():
                item = dict(row)
                reference = item.pop("initial_input")
                if reference is not None:
                    value = (await self.queries._resolve(conn, (), reference))["value"]
                    if isinstance(value, dict):
                        item["title"] = self.title(value.get("message", ""))
                item["title"] = item["title"] or "Chat"
                details[item["session_id"]] = item
            missing = set(ids) - details.keys()
            if missing:
                for row in (await conn.execute(select(sessions).where(sessions.c.session_id.in_(missing)))).mappings():
                    workspace = row["workspace"]
                    details[row["session_id"]] = dict(session_id=row["session_id"], title=row["title"] or "Conversation",
                        invocation_id=None, status="ready", control_state=None, last_sequence=0,
                        metadata_version=row["metadata_version"],
                        workspace_id=workspace["workspace_id"] if workspace else None)
        return replace(page, items=tuple({
            **details[row["session_id"]], "updated_at_us": row["updated_at_us"],
            "invocation_count": row["invocation_count"],
        } for row in page.items if row["session_id"] in details))

    async def conversation_subagents(self, sid, *, limit, cursor):
        from autoagent.agent_app import BUILTIN_AGENT_WORKFLOW_ID
        from autoagent.host.persistence.models import Page

        inv, revisions = (meta.tables[name] for name in ("invocations", "workflow_revisions"))
        roots = select(inv.c.invocation_id).join(revisions).where(
            inv.c.session_id == sid, revisions.c.workflow_id == BUILTIN_AGENT_WORKFLOW_ID)
        family = roots.cte(recursive=True)
        family = family.union(select(inv.c.invocation_id).join(family, inv.c.parent_invocation_id == family.c.invocation_id))
        started = func.coalesce(inv.c.started_at_us, 0)
        statement = select(inv.c.invocation_id, inv.c.session_id, inv.c.status, inv.c.input_ref,
            started.label("started")).join(revisions).where(
                inv.c.session_id != sid, inv.c.invocation_id.in_(select(family.c.invocation_id)),
                inv.c.status.is_not(None), revisions.c.workflow_id == BUILTIN_AGENT_WORKFLOW_ID)
        if cursor is not None:
            statement = statement.where(tuple_(started, inv.c.invocation_id) < tuple(cursor))
        async with self.queries.view() as (conn, _, _, frontier):
            rows = (await conn.execute(statement.order_by(started.desc(), inv.c.invocation_id.desc())
                .limit(limit + 1))).mappings().all()
            items = []
            for row in rows[:limit]:
                record = (await self.queries._resolve(conn, (), row["input_ref"]))["value"]
                task = self.title(record["message"]) if isinstance(record, dict) and isinstance(record.get("message"), str) else None
                items.append(dict(invocation_id=row["invocation_id"], session_id=row["session_id"],
                    task=task, status=row["status"]))
        following = (rows[limit - 1]["started"], rows[limit - 1]["invocation_id"]) if len(rows) > limit else None
        return Page(tuple(items), following, frontier)

    async def activate(self, sid, iid, model, workspace, bindings, activation_id):
        self.writer.check_open()
        async with self.writer.mutation:
            async with self.writer.storage.transaction() as conn:
                await self._prepare(conn)
                row = (await conn.execute(select(invocation_bindings).where(invocation_bindings.c.invocation_id == iid))).mappings().first()
                if row:
                    return dict(row)
                row = dict(binding_id=uuid4().hex, session_id=sid, invocation_id=iid,
                    model=model.model_dump(mode="json"), workspace=workspace.model_dump(mode="json") if workspace else None,
                    bindings=bindings, activation_id=activation_id, created_at_us=time.time_ns() // 1000)
                await conn.execute(insert(invocation_bindings).values(**row))
            self._ready = True
            return row

    async def snapshot(self, iid):
        engine = self.writer.storage.query_engine or self.writer.storage.engine
        async with engine.connect() as conn:
            if not await self._exists(conn, invocation_bindings):
                return None
            row = (await conn.execute(select(invocation_bindings).join(meta.tables["runtime_events"],
                meta.tables["runtime_events"].c.event_id == invocation_bindings.c.activation_id).where(
                    invocation_bindings.c.invocation_id == iid))).mappings().first()
            return dict(row) if row else None

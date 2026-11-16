"""Agent Session preferences, metadata, and immutable invocation model bindings."""

import time
from uuid import uuid4

from sqlalchemy import CheckConstraint, Index, MetaData, Table, delete, insert, select, update

from autoagent.agent.context import restore
from autoagent.agent_app.models import ApplicationSettingsRecord, ApplicationSessionSettings
from autoagent.agent.workspace import WorkspaceBinding
from autoagent.host.persistence.models import ConflictError
from autoagent.host.persistence.schema import B, J, c, meta

context_meta = MetaData()
sessions = Table("agent_app_sessions", context_meta,
    c("session_id", nullable=False, pk=True), c("version", B, False), c("settings", J, False),
    c("workspace", J), c("title"), c("metadata_version", B, False), c("updated_at_us", B, False),
    CheckConstraint("version >= 1"), CheckConstraint("metadata_version >= 0"))
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
                if columns != set(table.columns.keys()):
                    raise FormatError("Agent storage does not match the current schema.")


    async def purge_history(self, conn, invocation_ids, session_ids):
        """Participate in Host history deletion using the same SQL transaction."""
        from .queue import messages
        for table, field, values in ((invocation_bindings, "invocation_id", invocation_ids),
                (sessions, "session_id", session_ids), (messages, "session_id", session_ids)):
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


    async def _exists(self, conn, table):
        from sqlalchemy import inspect
        return await conn.run_sync(lambda sync: inspect(sync).has_table(table.name))

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

    async def _session_workspace(self, conn, sid):
        if not await self._exists(conn, invocation_bindings):
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
                row = (await conn.execute(select(sessions).where(sessions.c.session_id == sid))).mappings().one()
                if row["settings"]["model"] != model:
                    await conn.execute(update(sessions).where(sessions.c.session_id == sid).values(
                        settings={**row["settings"], "model": model}, version=row["version"] + 1, updated_at_us=time.time_ns() // 1000))
                row = (await conn.execute(select(sessions).where(sessions.c.session_id == sid))).mappings().one()
                return ApplicationSettingsRecord(session_id=sid, version=row["version"],
                    settings=restore(ApplicationSessionSettings, row["settings"]),
                    workspace=restore(WorkspaceBinding, row["workspace"]) if row["workspace"] else None,
                    title=row["title"], metadata_version=row["metadata_version"])

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
                metadata_version=row["metadata_version"] if row else 0)

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

    async def initialize_settings(self, sid, settings, workspace, message=None):
        self.writer.check_open()
        if self.writer.read_only:
            raise PermissionError("A read-only Host cannot create conversations.")
        async with self.writer.mutation:
            async with self.writer.storage.transaction() as conn:
                await self._prepare(conn)
                row = (await conn.execute(select(sessions).where(sessions.c.session_id == sid))).mappings().first()
                serialized = workspace.model_dump(mode="json") if workspace else None
                if row:
                    if row["workspace"] != serialized:
                        raise ConflictError("A Session's Workspace binding cannot be changed; create a new Session.")
                    if row["title"] is None and message:
                        await conn.execute(update(sessions).where(sessions.c.session_id == sid).values(
                            title=self.title(message), metadata_version=row["metadata_version"] + 1))
                else:
                    fixed, previous = await self._session_workspace(conn, sid)
                    if fixed and previous != workspace:
                        raise ConflictError("A Session's Workspace binding cannot be changed.")
                    await conn.execute(insert(sessions).values(session_id=sid, version=1,
                        settings=settings.model_dump(mode="json"), workspace=serialized,
                        title=self.title(message) if message else None, metadata_version=1,
                        updated_at_us=time.time_ns() // 1000))
            self._ready = True
        return await self.settings(sid, None)

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

    async def conversation_sessions(self, workspace_id, *, limit=20, cursor=None):
        from autoagent.agent_app import BUILTIN_AGENT_WORKFLOW_ID
        from autoagent.host.persistence.tracing import TraceQueries
        configured_workspace = sessions.c.workspace["workspace_id"].as_string()
        accepted_workspace = invocation_bindings.c.workspace["workspace_id"].as_string()
        configured = select(sessions.c.session_id).where(
            configured_workspace.is_not(None) if workspace_id is None else configured_workspace == workspace_id)
        accepted = select(invocation_bindings.c.session_id).join(meta.tables["runtime_events"],
            meta.tables["runtime_events"].c.event_id == invocation_bindings.c.activation_id).where(
                invocation_bindings.c.session_id.not_in(select(sessions.c.session_id)),
                accepted_workspace.is_not(None) if workspace_id is None else accepted_workspace == workspace_id)
        scope = configured.union(accepted)
        if workspace_id is None:
            scope = select(meta.tables["sessions"].c.session_id).where(meta.tables["sessions"].c.session_id.not_in(scope))
        return await TraceQueries(self.queries).sessions(workflow_ids=(BUILTIN_AGENT_WORKFLOW_ID,),
            session_scope=scope, limit=limit, cursor=cursor)

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

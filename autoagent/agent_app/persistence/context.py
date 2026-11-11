"""Agent Session preferences, metadata, and immutable invocation model bindings."""

import time
from uuid import uuid4

from sqlalchemy import CheckConstraint, Index, MetaData, Table, delete, insert, select, update

from autoagent.agent.context import AgentSessionSettings, restore
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
provider_configs = Table("agent_app_providers", context_meta,
    c("id", nullable=False, pk=True), c("version", B, False),
    c("configuration", J, False), c("updated_at_us", B, False))


class ApplicationContextStorage:
    def __init__(self, writer, queries):
        self.writer, self.queries, self._ready = writer, queries, False

    async def _prepare(self, conn):
        if not self._ready:
            from sqlalchemy import inspect
            from autoagent.host.persistence.models import FormatError
            await self._migrate_tables(conn)
            await conn.run_sync(context_meta.create_all)
            await self._upgrade(conn)
            await self._separate_sdk_records(conn)
            for row in (await conn.execute(select(sessions))).mappings():
                data = row["settings"]
                if data.get("approval_mode") in {"bypass", "never_ask"}:
                    data = {**data, "approval_mode": "auto_approve" if data["approval_mode"] == "bypass" else "policy"}
                    await conn.execute(update(sessions).where(sessions.c.session_id == row["session_id"]).values(settings=data))
            for row in (await conn.execute(select(resource_configs))).mappings():
                configuration = row["configuration"]
                old = configuration.get("permissions")
                if old is not None and "decision" in old:
                    rules = []
                    for rule in old.get("operation_rules", ()):
                        conditions = []
                        for key in ("action", "resource"):
                            if rule.get(key) is not None:
                                conditions.append(dict(source="operation", path="/" + key, operator="glob", value=rule[key]))
                        for key, value in rule.get("attributes", {}).items():
                            conditions.append(dict(source="operation", path="/attributes/" + key.replace("~", "~0").replace("/", "~1"), operator="glob", value=value))
                        rank = {"allow": 0, "ask": 1, "deny": 2}
                        decision = max((old["decision"], rule["decision"]), key=rank.__getitem__)
                        rules.append(dict(conditions=conditions, decision=decision, reason=rule.get("reason")))
                    configuration = {**configuration, "permissions": dict(default=old["decision"], rules=rules)}
                    await conn.execute(update(resource_configs).where(resource_configs.c.id == row["id"]).values(configuration=configuration))
            for table in context_meta.tables.values():
                columns = await conn.run_sync(lambda sync, name=table.name:
                    {item["name"] for item in inspect(sync).get_columns(name)})
                if columns != set(table.columns.keys()):
                    raise FormatError("Agent storage does not match the current schema.")

    async def _migrate_tables(self, conn):
        """Rename the previous product tables atomically, leaving Core facts untouched."""
        from sqlalchemy import inspect, text
        from autoagent.host.persistence.models import FormatError
        names = await conn.run_sync(lambda sync: set(inspect(sync).get_table_names()))
        for suffix in ("sessions", "invocation_bindings", "workspaces", "resource_configs", "message_queue"):
            old, new = "agent_" + suffix, "agent_app_" + suffix
            if old in names:
                if new in names:
                    raise FormatError(f"Both {old} and {new} exist; resolve the application schema conflict.")
                await conn.execute(text(f'ALTER TABLE "{old}" RENAME TO "{new}"'))

    async def purge_history(self, conn, invocation_ids, session_ids):
        """Participate in Host history deletion using the same SQL transaction."""
        from .queue import messages
        for table, field, values in ((invocation_bindings, "invocation_id", invocation_ids),
                (sessions, "session_id", session_ids), (messages, "session_id", session_ids)):
            if await self._exists(conn, table):
                await conn.execute(delete(table).where(table.c[field].in_(values)))

    async def _separate_sdk_records(self, conn):
        """Old shared settings never become product authority for user Workflows."""
        from autoagent.agent_app.workflow import BUILTIN_AGENT_WORKFLOW_ID
        invocations, revisions = meta.tables["invocations"], meta.tables["workflow_revisions"]
        owned = select(invocations.c.invocation_id).join(revisions,
            revisions.c.revision_id == invocations.c.workflow_revision_id).where(
                revisions.c.workflow_id == BUILTIN_AGENT_WORKFLOW_ID)
        foreign = select(invocations.c.invocation_id).where(invocations.c.invocation_id.not_in(owned))
        await conn.execute(delete(invocation_bindings).where(invocation_bindings.c.invocation_id.in_(foreign)))
        owned_sessions = select(invocations.c.session_id).where(invocations.c.invocation_id.in_(owned))
        foreign_sessions = select(invocations.c.session_id).where(invocations.c.session_id.not_in(owned_sessions))
        await conn.execute(delete(sessions).where(sessions.c.session_id.in_(foreign_sessions)))

    async def prepare(self):
        """Upgrade only the obsolete Agent tables, preserving Core execution history."""
        self.writer.check_open()
        if self.writer.read_only:
            return
        async with self.writer.mutation:
            async with self.writer.storage.transaction() as conn:
                await self._prepare(conn)
            self._ready = True

    async def _upgrade(self, conn):
        """One-time conversion of full settings rows to preferences and model bindings."""
        from sqlalchemy import inspect
        names = await conn.run_sync(lambda sync: set(inspect(sync).get_table_names()))
        old_meta = MetaData()
        for name in ("agent_session_settings", "agent_execution_settings"):
            if name in names:
                await conn.run_sync(lambda sync, name=name: Table(name, old_meta, autoload_with=sync))
        def preferences(settings):
            model = settings.get("model", {})
            return dict(model={key: model.get(key) for key in ("provider", "model") if key in model},
                approval_mode=settings.get("approval_mode", "policy"))
        old = old_meta.tables.get("agent_session_settings")
        if old is not None:
            for row in (await conn.execute(select(old))).mappings():
                if not await conn.scalar(select(sessions.c.session_id).where(sessions.c.session_id == row["session_id"])):
                    await conn.execute(insert(sessions).values(session_id=row["session_id"], version=row["version"],
                        settings=preferences(row["settings"]), workspace=row["settings"].get("workspace"),
                        title=None, metadata_version=0, updated_at_us=row["updated_at_us"]))
        old = old_meta.tables.get("agent_execution_settings")
        if old is not None:
            # Preserve the last accepted model of existing executions. No full
            # settings history remains authoritative after this conversion.
            accepted = select(old).join(meta.tables["runtime_events"],
                old.c.activation_id == meta.tables["runtime_events"].c.event_id).order_by(old.c.epoch.desc())
            for row in (await conn.execute(accepted)).mappings():
                exists = await conn.scalar(select(invocation_bindings.c.binding_id).where(
                    invocation_bindings.c.invocation_id == row["invocation_id"]))
                if not exists:
                    await conn.execute(insert(invocation_bindings).values(binding_id=row["snapshot_id"],
                        session_id=row["session_id"], invocation_id=row["invocation_id"],
                        model=preferences(row["settings"])["model"], workspace=row["settings"].get("workspace"),
                        bindings=row["bindings"], activation_id=row["activation_id"], created_at_us=row["created_at_us"]))
        if "agent_app_message_queue" in names:
            columns = await conn.run_sync(lambda sync: {item["name"] for item in inspect(sync).get_columns("agent_app_message_queue")})
            if "model" not in columns:
                from .queue import messages
                from sqlalchemy import text
                await conn.execute(text("ALTER TABLE agent_app_message_queue ADD COLUMN model JSON"))
                # Old queued submissions did not capture a model. Capture their
                # existing Session selection once during the schema upgrade.
                for row in (await conn.execute(select(messages).where(messages.c.state.in_(("queued", "starting", "blocked"))))).mappings():
                    saved = await conn.scalar(select(sessions.c.settings).where(sessions.c.session_id == row["session_id"]))
                    if saved:
                        await conn.execute(update(messages).where(messages.c.id == row["id"]).values(model=saved["model"]))
        await conn.run_sync(old_meta.drop_all)

    async def _exists(self, conn, table):
        from sqlalchemy import inspect
        return await conn.run_sync(lambda sync: inspect(sync).has_table(table.name))

    async def resource_records(self):
        return await self._configuration_records(resource_configs)

    async def provider_records(self):
        return await self._configuration_records(provider_configs)

    async def _configuration_records(self, table):
        engine = self.writer.storage.query_engine or self.writer.storage.engine
        async with engine.connect() as conn:
            if not await self._exists(conn, table):
                return []
            return [dict(row) for row in (await conn.execute(select(table)
                .order_by(table.c.id))).mappings()]

    async def save_resource(self, resource_id, configuration, expected_version):
        return await self._save_configuration(resource_configs, resource_id, configuration, expected_version)

    async def save_provider(self, resource_id, configuration, expected_version):
        return await self._save_configuration(provider_configs, resource_id, configuration, expected_version)

    async def _save_configuration(self, table, resource_id, configuration, expected_version):
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

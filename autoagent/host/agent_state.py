"""Process-local SDK preferences with recovery bindings in normal Core Context."""

import time
from autoagent.agent.context import AgentSettingsRecord, AgentSessionSettings, restore
from autoagent.agent.workspace import WorkspaceBinding
from .persistence.models import ConflictError


class AgentRuntimeState:
    def __init__(self, service):
        self.service = service
        self.records = {}
        self.bindings = {}

    async def call(self, method, *args, **kwargs):
        return await getattr(self, method)(*args, **kwargs)

    async def settings(self, sid, defaults):
        with self.service._lock:
            return self.records.get(sid) or AgentSettingsRecord(session_id=sid, version=0,
                settings=defaults.session_defaults(), workspace=defaults.workspace)

    async def configure(self, sid, settings, expected_version, workspace):
        with self.service._lock:
            previous = self.records.get(sid)
            version = previous.version if previous else 0
            if expected_version is not None and expected_version != version:
                raise ConflictError("Agent Session settings version conflict.")
            if previous and previous.workspace != workspace:
                raise ConflictError("A Session's Workspace binding cannot be changed; create a new Session.")
            record = AgentSettingsRecord(session_id=sid, version=version + 1, settings=settings, workspace=workspace)
            self.records[sid] = record
            return record

    async def initialize_settings(self, sid, settings, workspace, message=None):
        with self.service._lock:
            record = self.records.get(sid)
            if record is not None:
                if record.workspace != workspace:
                    raise ConflictError("A Session's Workspace binding cannot be changed; create a new Session.")
                return record
            record = AgentSettingsRecord(session_id=sid, version=1, settings=settings, workspace=workspace)
            self.records[sid] = record
            return record

    async def activate(self, sid, iid, model, workspace, bindings, activation_id):
        with self.service._lock:
            if iid in self.bindings:
                return self.bindings[iid]
            row = dict(binding_id="sdk:" + iid, session_id=sid, invocation_id=iid,
                model=model.model_dump(mode="json"), workspace=workspace.model_dump(mode="json") if workspace else None,
                bindings=bindings, activation_id=activation_id, created_at_us=time.time_ns() // 1000,
                preferences=self.records[sid].settings.model_dump(mode="json"))
            self.bindings[iid] = row
            return row

    async def snapshot(self, iid):
        with self.service._lock:
            if iid in self.bindings:
                return self.bindings[iid]
        persistence = self.service.host._persistence
        row = await persistence.get_invocation(iid, view="accepted")
        if row is None:
            return None
        await persistence.flush()
        row = await persistence.get_invocation(iid)
        value = await persistence.inspect_execution_state(iid, sequence=row["last_sequence"],
            scope="invocation", pointer="/agent_runtime_binding")
        if value and value.get("available"):
            raw = value["value"]
            with self.service._lock:
                self.bindings[iid] = raw
                self.records.setdefault(raw["session_id"], AgentSettingsRecord(session_id=raw["session_id"], version=1,
                    settings=restore(AgentSessionSettings, raw["preferences"]),
                    workspace=restore(WorkspaceBinding, raw["workspace"]) if raw["workspace"] else None))
            return raw
        original = await persistence.trace_value(iid, "invocation", iid, "input")
        value = (original.get("value") or {}) if original else {}
        return await self.service.activate(row["session_id"], iid, reason="recover", activation_id="sdk-recover:" + iid,
            workflow_revision=row["workflow_revision_id"], model=value.get("model") if isinstance(value, dict) else None)

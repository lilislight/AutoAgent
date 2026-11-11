"""Product preferences and runtime services injected into the Agent SDK."""

from contextlib import asynccontextmanager
import json
from types import SimpleNamespace

from autoagent.agent.context import (AgentExecutionSnapshot, AgentModelSelection, AgentHostSettings, AgentSessionSettings, restore, plain)
from autoagent.host.agent_context import HostAgentContext, _UNSET
from .queue import AgentMessageQueue
from .resources import ManagedAgentResources
from .workspaces import ApplicationWorkspaces
from .models import ApplicationSessionSettings
from autoagent.host.models import HostNotFoundError


class ApplicationContext(HostAgentContext):
    """Persistent preferences are owned exclusively by the built-in product."""

    def __init__(self, host):
        super().__init__(host)
        self._defaults_explicit = False
        self._agent_parents = host.agent_context._agent_parents
        self.approvals = host.agent_context.approvals
        self.messages = AgentMessageQueue(self)
        self.shared_resources = ManagedAgentResources(self)
        self._workspaces = ApplicationWorkspaces(self)
        self.workspaces = self._workspaces.registry

    def set_defaults(self, settings):
        self._defaults_explicit = True
        super().set_defaults(settings)

    def initialize_defaults(self, sdk):
        if not self._defaults_explicit:
            self.defaults = sdk.defaults.model_copy(deep=True)
        for workspace in sdk.workspaces.entries():
            if not any(entry.info.id == workspace.info.id for entry in self.workspaces.entries()):
                self.workspaces.register(workspace)
        for entry in sdk.resources.entries("provider"):
            if not self.resources.has("provider", entry.name):
                self.resources.register_provider(entry.name, entry.value, version=entry.version)
        for entry in sdk.resources.entries("policy"):
            if not self.resources.has("policy", entry.name):
                self.resources.register_policy(entry.name, entry.value, version=entry.version)

    def default_preferences(self):
        return ApplicationSessionSettings.from_runtime(self.defaults)

    def _remember(self, record):
        with self._lock:
            previous = self._session_records.get(record.session_id)
            if previous is None or (record.version, record.metadata_version) >= (previous.version, previous.metadata_version):
                self._session_records[record.session_id] = record
            return self._session_records[record.session_id]

    async def workspace_records(self):
        return await self.storage("workspace_records")

    async def export_binding(self, iid):
        return None

    @asynccontextmanager
    async def borrow_tool(self, resource_id):
        async with self.shared_resources.borrow(resource_id):
            yield

    async def register_workspace_record(self, configuration):
        return await self.storage("register_workspace", configuration)

    async def storage(self, method, *args, **kwargs):
        return await self.host.agent_app.storage.context(method, *args, **kwargs)

    async def restore_state(self):
        await self.storage("prepare")
        await self._workspaces.restore()

    def resource_revision(self, snapshot):
        return (*super().resource_revision(snapshot), self.host.agent_context.resources.revision,
            self.shared_resources.identity)

    def additional_resources(self, snapshot, tools, skills):
        workflow_id = self.host._runtime.workflow_irs[snapshot.bindings["workflow_revision"]].workflow_id
        project_tools, project_skills = self.host.agent_context.resources.catalog(workflow_id)
        tools, skills = (*tools, *project_tools), (*skills, *project_skills)
        managed_tools, managed_skills = self.shared_resources.catalog((tools, skills))
        return ((*tools, *managed_tools),
                self.shared_resources.filter((*skills, *managed_skills)))

    def finalize_catalog(self, snapshot, catalog, skills):
        return {entry.name: entry for entry in self.shared_resources.filter(catalog.values())}, skills

    def toolset(self, snapshot, *, binding=None, catalog=None, observations=None):
        from autoagent.agent import ToolContext, ToolDecision
        from autoagent.agent.tools.operations import combine_decisions
        tools = super().toolset(snapshot, binding=binding, catalog=catalog, observations=observations)
        catalog = catalog if catalog is not None else self.catalog(snapshot)[0]
        registered_policy = tools.policy

        async def policy(call, context):
            entry = catalog.get(call.name)
            if entry is None:
                return ToolDecision(action="deny", reason="Tool is outside the Agent application.")
            selected = self.shared_resources.effective_permissions(entry)
            if selected is not None:
                return selected(call, context)
            decisions = [await registered_policy(call, context)]
            value = entry.value
            while getattr(value, "_owner", None) is not None:
                owner = value._owner
                if owner.policy is not None:
                    decisions.append(await self._resource_policy(owner.policy, call, context))
                value = owner._tools[call.name]
            return combine_decisions(decisions)

        def operation_policy(operation, context):
            from autoagent.agent import LLMToolCall
            call = LLMToolCall(name=context.tool_name, call_id=context.call_id,
                arguments=json.dumps(plain(context.arguments))) if context.tool_name and context.call_id else None
            target = context.tool_name
            if operation not in context.operations:
                target = {"file.read":"read_file", "file.write":"write_file", "file.list":"list_files",
                    "file.delete":"manage_files", "file.stat":"manage_files"}.get(operation.action, target)
            entry = catalog.get(target)
            selected = self.shared_resources.effective_permissions(entry) if entry is not None else None
            if selected is None or call is None:
                return None
            return selected.evaluate_operation(call, context, operation)

        tools.policy = policy
        tools.replace_policies = True
        from dataclasses import replace
        tools.context = replace(tools.context, services={**tools.context.services, "operation_policy": operation_policy})
        return tools

    async def create_workspace(self, *, name, path):
        async def create():
            return await self._workspaces.create(name=name, path=path)
        return await self.host._perform(create, control=True)


    async def list_conversation_sessions(self, workspace_id, *, limit=20, cursor=None):
        async def read():
            return await self.storage("conversation_sessions", workspace_id,
                limit=limit, cursor=cursor)
        return await self.host._perform(read)


    async def conversation_settings(self, session_id, workspace_id=None, *, unbound=False):
        """Preview a new chat's binding without mutating or rebinding existing chats."""
        from autoagent.agent import WorkspaceBinding
        from autoagent.host.persistence import ConflictError
        if unbound and workspace_id is not None:
            raise ValueError("Choose a Workspace or an unbound conversation.")
        info = await self.workspace_session(session_id, allow_new=True)
        record = await self.get_settings(session_id)
        if info is not None or record.version:
            if unbound and record.workspace is not None:
                raise ConflictError("A Session's Workspace binding cannot be changed; create a new Session.")
            if workspace_id is not None and (record.workspace is None or record.workspace.workspace_id != workspace_id):
                raise ConflictError("A Session's Workspace binding cannot be changed; create a new Session.")
            return record
        if unbound:
            return record.model_copy(update={"workspace": None})
        workspace_id = workspace_id or self.project_workspace_id
        await self.workspaces.aresolve(workspace_id)
        return record.model_copy(update={"workspace": WorkspaceBinding(workspace_id=workspace_id)})


    async def workspace_catalog(self):
        """Describe only the SDK Agent; user Agent builds are execution Workflows."""
        async def read():
            from autoagent.agent_app.workflow import BUILTIN_AGENT_WORKFLOW_ID, is_builtin_agent
            runtime = self.host._runtime
            revision = runtime.workflow_ids.get(BUILTIN_AGENT_WORKFLOW_ID) if runtime else None
            ir = runtime.workflow_irs[revision] if revision else None
            agent = None
            if ir is not None and is_builtin_agent(ir):
                options = self.options_for_workflow(ir)
                start = next(node for node in ir.nodes if node.id == "start")
                policy = getattr(start.executable.handler, "__autoagent_context_policy__", None)
                agent = dict(revision_id=revision, user_signal_endpoint=options.user_signal_endpoint,
                    include_ask_user=options.include_ask_user, subagents=options.subagents.enabled,
                    context_policy={key: getattr(policy, key, None) for key in
                        ("context_window", "compact_at_ratio", "compact_at_tokens", "target_ratio", "target_tokens")})
            providers = sorted({entry.name for entry in self.resources.entries("provider")} | {self.defaults.model.provider})
            return dict(agent=agent, standalone=bool(agent is not None and len(runtime.workflow_ids) == 1),
                providers=providers, defaults=self.default_preferences().model_dump(mode="json"),
                generation_defaults=self.defaults.model.model_dump(mode="json", exclude={"provider", "model"}),
                project_workspace_id=self.project_workspace_id)
        return await self.host._perform(read)


    async def workspace_session(self, session_id, *, allow_new=False):
        """Reject heterogeneous or user-workflow Sessions at the application boundary."""
        async def read():
            from autoagent.agent_app.workflow import BUILTIN_AGENT_WORKFLOW_ID
            from autoagent.host.models import HostNotFoundError
            info = await self.host._persistence.session_information(session_id)
            if info is None:
                if allow_new:
                    return None
                raise HostNotFoundError("Conversation is unavailable.")
            if info["workflow_count"]:
                workflows = await self.host._persistence.session_workflows(session_id, limit=1)
                if info["workflow_count"] != 1 or not workflows.items or workflows.items[0]["workflow_id"] != BUILTIN_AGENT_WORKFLOW_ID:
                    raise HostNotFoundError("This Session is not an SDK Agent conversation.")
            return info
        return await self.host._perform(read)


    async def start_conversation(self, session_id, message, workspace_id=None, *, request_id=None, unbound=False, model=None):
        from autoagent.agent_app.workflow import BUILTIN_AGENT_WORKFLOW_ID
        from autoagent.agent.workflow.models import AgentInput
        from autoagent.host.models import HostNotFoundError
        input_value = AgentInput(message=message, request_id=request_id,
            model=restore(AgentModelSelection, model) if model is not None else None)
        catalog = await self.workspace_catalog()
        if catalog["agent"] is None:
            raise HostNotFoundError("The SDK Agent is disabled for this project.")
        existing = await self.workspace_session(session_id, allow_new=True)
        record = await self.conversation_settings(session_id, workspace_id, unbound=unbound)
        await self.approvals._run(lambda: self._workspaces.validate(record.workspace))
        async def initialize():
            return await self.storage("initialize_settings", session_id, record.settings, record.workspace, message)
        await self.host._perform(initialize, control=True)
        input_value = input_value.model_copy(update={"model": input_value.model or record.settings.model})
        return await self.host.execute_json(BUILTIN_AGENT_WORKFLOW_ID, input_value.model_dump(mode="json"),
            revision_id=catalog["agent"]["revision_id"], entry_node_id="start", session_id=session_id,
            require_existing_session=existing is not None, require_new_session=existing is None)


    async def configure(self, session_id, settings, *, expected_version=None, workspace=_UNSET):
        settings = restore(ApplicationSessionSettings, settings)
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError("Session ID must be nonempty.")
        if expected_version is not None and (type(expected_version) is not int or expected_version < 0):
            raise ValueError("expected_version must be a nonnegative integer.")
        async def update():
            previous = await self.get_settings(session_id)
            from autoagent.agent import WorkspaceBinding
            binding = previous.workspace if workspace is _UNSET else restore(WorkspaceBinding, workspace) if workspace is not None else None
            await self.approvals._run(lambda: self._workspaces.validate(binding))
            record = await self.storage("configure", session_id, settings, expected_version, binding)
            self._remember(record)
            await self.approvals.changed(session_id)
            return record
        return await self.host._perform(update, payload=settings.model_dump(mode="json"), control=True)


    async def rename_conversation(self, session_id, title, *, expected_version):
        await self.workspace_session(session_id)
        async def update():
            previous = await self.get_settings(session_id)
            record = await self.storage("rename", session_id, title,
                expected_version, previous.settings, previous.workspace)
            self._remember(record)
            return record
        return await self.host._perform(update, control=True)


    async def _read_settings(self, session_id):
        return self._remember(await self.storage("settings", session_id, self.defaults))


    async def _get_snapshot(self, iid, *, cache=True):
        with self._lock:
            found = self._snapshots.get(iid) if cache else None
        if found is None:
            found = await self.storage("snapshot", iid)
            if found is not None and cache:
                with self._lock:
                    self._snapshots[iid] = found
        if found is None:
            return None
        record = self._session_records.get(found["session_id"]) if cache else None
        record = record or await self._read_settings(found["session_id"])
        from autoagent.agent import WorkspaceBinding
        selected = restore(AgentModelSelection, found["model"])
        effective = self.effective_settings(record, selected)
        effective = effective.model_copy(update={"workspace": restore(WorkspaceBinding, found["workspace"]) if found["workspace"] else None})
        bindings = {**found["bindings"], **self._bindings(effective)}
        return AgentExecutionSnapshot(snapshot_id=found["binding_id"], session_id=found["session_id"], invocation_id=iid,
            epoch=1, reason="start", created_at_us=found["created_at_us"], settings_version=record.version,
            settings=effective, bindings=bindings)


    async def activate(self, sid, iid, *, reason, activation_id, workflow_revision=None, model=None, message=None):
        record = await self._read_settings(sid)
        selected = restore(AgentModelSelection, model) if model is not None else record.settings.model
        workspace = record.workspace
        parent_id = self._agent_parents.get(iid)
        if parent_id is not None and not record.version:
            parent = await self._get_snapshot(parent_id) if await self.for_invocation(parent_id) is self else None
            if parent is not None:
                if model is None:
                    selected = AgentModelSelection(provider=parent.settings.model.provider, model=parent.settings.model.model)
                workspace = parent.settings.workspace
                record = record.model_copy(update={"settings": record.settings.model_copy(
                    update={"approval_mode": parent.settings.approval_mode})})
        if not record.version:
            preferences = record.settings.model_copy(update={"model": selected})
            record = self._remember(await self.storage(
                "initialize_settings", sid, preferences, workspace, message))
        settings = self.effective_settings(record, selected).model_copy(update={"workspace": workspace})
        bindings = {"workflow_revision": workflow_revision}
        self._bindings(settings)  # Validate the selected Provider before admission.
        ir = self.host._runtime.workflow_irs.get(workflow_revision)
        options = self.options_for_workflow(ir) if ir is not None else None
        if options is not None and options.subagents.enabled:
            for alias, target in options.subagents.targets.items():
                revision = target.workflow_revision_id or self.host._runtime.workflow_ids.get(target.workflow_id)
                if revision not in self.host._runtime.workflow_irs:
                    raise ValueError(f"Agent target {alias!r} is not registered.")
                current = self.host._runtime.workflow_irs[revision]
                if current.workflow_id != target.workflow_id or target.entry_node_id not in current.entry_node_ids:
                    raise ValueError(f"Agent target {alias!r} has an invalid Workflow or entry Node.")
                bindings["target:" + alias] = revision
        return await self.storage("activate", sid, iid, selected, workspace, bindings, activation_id)




    async def workspace_invocation(self, invocation_id):
        async def read():
            from autoagent.agent_app.workflow import BUILTIN_AGENT_WORKFLOW_ID
            from autoagent.host.models import HostNotFoundError
            family = await self.host._persistence.execution_family(invocation_id)
            if family is None or any(row["workflow_id"] != BUILTIN_AGENT_WORKFLOW_ID
                    for row in [family["current"], *(family["ancestors"] or []), *([family["root"]] if family["root"] else [])]):
                raise HostNotFoundError("This execution is not an SDK Agent conversation.")
            return family
        family = await self.host._perform(read)
        await self.workspace_session(family["current"]["session_id"])
        return family


    async def workspace_resources(self, revision, *, builtin=False, session_id=None, workspace_id=None, unbound=False):
        """List only the selected Workflow's Build bindings and explicit Host grants."""
        async def read():
            from types import SimpleNamespace
            from autoagent.agent.workflow.builtins import workflow_catalog
            ir = self.host._runtime.workflow_irs.get(revision) if self.host._runtime else None
            if builtin:
                from autoagent.agent_app.workflow import is_builtin_agent
                from autoagent.host.models import HostNotFoundError
                if ir is None or not is_builtin_agent(ir):
                    raise HostNotFoundError("SDK Agent resources are unavailable.")
            if ir is None or self.options_for_workflow(ir) is None:
                raise ValueError("Choose a registered Agent Workflow Revision.")
            if session_id is not None:
                if builtin:
                    await self.workspace_session(session_id, allow_new=True)
                record = await self.conversation_settings(session_id, workspace_id, unbound=unbound) if builtin else await self.get_settings(session_id)
                selected_settings = self.effective_settings(record)
            else:
                selected_settings = self.defaults
            snapshot = SimpleNamespace(bindings={"workflow_revision": revision}, settings=selected_settings)
            if selected_settings.workspace is not None:
                await self.approvals._run(lambda: self.prepare_workspace(snapshot))
            tools, skills = workflow_catalog(self, snapshot, self.options_for_workflow(ir))
            registry = self._build_resources.get(revision)
            local_skills = {entry.name for entry in registry.catalog(ir.workflow_id)[1]} if registry else set()
            return dict(user_signal_endpoint=self.options_for_workflow(ir).user_signal_endpoint,
                tools=[dict(name=name, description=entry.value.definition.description,
                origin="Built-in" if entry.resource_id.startswith("builtin:") else
                    "Skill" if entry.resource_id.startswith("skills:") else "Build" if entry.resource_id.startswith("build:") else "Host",
                version=entry.version, resource_id=entry.resource_id) for name, entry in tools.items()],
                skills=[dict(name=skill.metadata.name, description=skill.metadata.description,
                    origin="Workspace" if skill.metadata.name in self._workspaces_skill_names(snapshot, ir.workflow_id)
                        else "Build" if skill.metadata.name in local_skills else "Host") for skill in skills])
        return await self.host._perform(read)

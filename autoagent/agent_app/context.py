"""Product preferences and runtime services injected into the Agent SDK."""

import asyncio
from contextlib import asynccontextmanager
import json
from autoagent.agent.context import AgentModelSelection, restore, plain
from autoagent.agent.runtime.context import AgentRuntime
from .queue import AgentMessageQueue
from .resources import ManagedAgentResources
from .workspaces import ApplicationWorkspaces
from .models import ApplicationSessionSettings
from autoagent.host.models import HostNotFoundError
from .images import IMAGE_URI_PREFIX


class ApplicationContext(AgentRuntime):
    """Persistent preferences are owned exclusively by the built-in product."""

    session_settings_type = ApplicationSessionSettings

    def __init__(self, host):
        super().__init__(host.runtime_services)
        self.host = host
        self._defaults_explicit = False
        self._live_invocations = set()
        from .approvals import AgentApprovals
        self.approvals = AgentApprovals(self)
        self.messages = AgentMessageQueue(self)
        self.shared_resources = ManagedAgentResources(self)
        self._workspaces = ApplicationWorkspaces(self)
        self.workspaces = self._workspaces.registry

    def set_defaults(self, settings):
        self._defaults_explicit = True
        super().set_defaults(settings)

    def initialize_defaults(self, sdk):
        self._agent_parents = sdk._agent_parents
        if not self._defaults_explicit:
            self.defaults = self.defaults.model_copy(update={"model": self.defaults.model.model_copy(update={"streaming": True})})

    async def _watch_resources(self):
        """Reevaluate product approval waits after application resources change."""
        previous = self.resources.revision
        while True:
            revision = self.resources.revision
            if revision != previous:
                previous = revision
                await self.approvals.changed()
            await asyncio.sleep(.1)

    async def reconcile_history(self, conversation, invocation_id):
        """Preserve committed Map results even when their Node never reached Output Binding."""
        from autoagent.core import OperatorCallContext
        from autoagent.agent import LLMToolCall, LLMToolResult
        pending = conversation.pending
        expected = {item.call_id for item in conversation.items[pending.start:] if isinstance(item, LLMToolCall)}
        rows = await self.storage("completed_tool_results", OperatorCallContext.current().session_id, invocation_id)
        results = dict(pending.results)
        for row in rows:
            if row.get("type") == "tool_result" and row.get("call_id") in expected:
                result = restore(LLMToolResult, row)
                results.setdefault(result.call_id, result)
        return conversation.model_copy(update={"pending": pending.model_copy(update={"results": results})})

    def allows_recovery(self, status, *, interrupted=False, automatic=False):
        if status.status in {"completed", "failed", "cancelled"}:
            return False
        if automatic:
            # Rehydrating a clean wait created in this Host is residency management,
            # not reopening a stopped task. Startup cancels all older product tasks.
            return status.invocation_id in self._live_invocations and bool(status.waits)
        return status.control_state == "paused"

    async def remember_model(self, session_id, model):
        """Save the newest submission without replay activation overwriting user choices."""
        record = await self.storage("remember_model", session_id, model)
        return self._remember(record)

    def bind_submission_model(self, selected):
        selected = self.bind_model(selected)
        if selected.provider != "default" and not self.host.providers.has(selected.provider):
            raise ValueError(f"Unknown Agent Provider: {selected.provider!r}.")
        return selected

    def default_preferences(self):
        result = ApplicationSessionSettings.from_runtime(self.defaults)
        selected = self.host.providers.default_model
        if selected is None:
            selected = next((AgentModelSelection(provider=name, model=model.id)
                for name, provider in self.host.providers.records.items() if provider.config.enabled
                for model, info in provider.models.values() if model.enabled), None)
        return result.model_copy(update={"model": selected}) if selected is not None else result

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

    async def register_workspace_record(self, configuration):
        return await self.storage("register_workspace", configuration)

    async def storage(self, method, *args, **kwargs):
        return await self.host.applications["agent"].storage.context(method, *args, **kwargs)

    @asynccontextmanager
    async def resource_scope(self):
        async with super().resource_scope():
            try:
                await self.storage("prepare")
                await self._workspaces.restore()
                yield
            finally:
                await self._workspaces.close_execution()
                self._workspaces.clear()

    @property
    def project_workspace_id(self):
        return self._workspaces.project_id

    async def register_workspace(self, workspace, *, replace=False):
        return await self.host._perform(lambda: self._workspaces.register(workspace, replace=replace), control=True)

    async def list_workspaces(self):
        return await self.host._perform(self._workspaces.describe)

    async def prepare_workspace(self, snapshot):
        return await self._workspaces.prepare(snapshot)

    async def prepare_tool_workspace(self, snapshot, resource_id):
        # Skill Tools depend on current document bytes. Other Tools only need
        # the current Workspace and the catalog already refreshed by preflight.
        return await self._workspaces.prepare(snapshot, refresh=resource_id.startswith("skills:"))

    def process_managers(self):
        managers = super().process_managers()
        if self._workspaces.process_manager is not None:
            managers.add(self._workspaces.process_manager)
        return managers

    def resource_revision(self, snapshot):
        return (*super().resource_revision(snapshot), self._workspaces.revision(snapshot),
            self.shared_resources.identity)

    def toolset(self, snapshot, *, binding=None, catalog=None, observations=None):
        from autoagent.agent import ToolDecision
        from autoagent.agent.tools.operations import combine_decisions
        tools = super().toolset(snapshot, binding=binding, catalog=catalog, observations=observations)
        catalog = catalog if catalog is not None else self.catalog(snapshot)[0]
        registered_policy = tools.policy

        async def policy(call, context):
            entry = catalog.get(call.name)
            if entry is None:
                return ToolDecision(action="deny", reason="Tool is outside the Agent application.")
            identity = self.shared_resources.resource_identity(entry)
            configured = self.shared_resources.configured_policies(identity, entry.resource_id)
            decisions = [await registered_policy(call, context), *(policy(call, context) for _, policy in configured)]
            value = entry.value
            while getattr(value, "_owner", None) is not None:
                owner = value._owner
                if owner.policy is not None:
                    decisions.append(await self._resource_policy(owner.policy, call, context))
                value = owner._tools[call.name]
            return combine_decisions(decisions)

        def operation_policy(operation, context):
            if operation in context.operations:
                return None
            from autoagent.agent import LLMToolCall
            call = LLMToolCall(name=context.tool_name, call_id=context.call_id,
                arguments=json.dumps(plain(context.arguments))) if context.tool_name and context.call_id else None
            entry = catalog.get(context.tool_name)
            if entry is None or call is None:
                return None
            identity = self.shared_resources.resource_identity(entry)
            configured = self.shared_resources.configured_policies(identity, entry.resource_id)
            return combine_decisions(policy.evaluate_operation(call, context, operation) for _, policy in configured)

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

    async def list_conversation_summaries(self, workspace_id, *, all_workspaces, limit, cursor):
        return await self.host._perform(lambda: self.storage("conversation_summaries", workspace_id,
            all_workspaces=all_workspaces, limit=limit, cursor=cursor))


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
        """Describe the product Workflow and its application defaults."""
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
            return dict(agent=agent, standalone=bool(agent is not None and len(runtime.workflow_ids) == 1),
                defaults=self.default_preferences().model_dump(mode="json"),
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
                fork = await self.host.applications["agent"].storage.forks("info", session_id)
                if fork and fork["fork_source_session_id"]:
                    return dict(session_id=session_id, invocation_count=0, runtime_started=False)
                if allow_new:
                    return None
                raise HostNotFoundError("Conversation is unavailable.")
            if info["workflow_count"]:
                workflows = await self.host._persistence.session_workflows(session_id, limit=1)
                if info["workflow_count"] != 1 or not workflows.items or workflows.items[0]["workflow_id"] != BUILTIN_AGENT_WORKFLOW_ID:
                    raise HostNotFoundError("This Session is not an Agent application conversation.")
            return info
        return await self.host._perform(read)


    async def start_conversation(self, session_id, message, workspace_id=None, *, unbound=False, model=None, images=(), uploads=(),
                                 remember_model=True, before_start=None):
        from autoagent.agent_app.workflow import BUILTIN_AGENT_WORKFLOW_ID
        from autoagent.agent.workflow.models import AgentInput
        from autoagent.host.models import HostNotFoundError
        from autoagent.agent.models import ImageContent
        input_value = AgentInput(message=message,
            images=tuple(image.content() for image in uploads) if uploads else tuple(restore(ImageContent, image) for image in images),
            model=restore(AgentModelSelection, model) if model is not None else None)
        catalog = await self.workspace_catalog()
        if catalog["agent"] is None:
            raise HostNotFoundError("The Agent application is disabled for this project.")
        existing = await self.workspace_session(session_id, allow_new=True)
        record = await self.conversation_settings(session_id, workspace_id, unbound=unbound)
        selected = self.bind_submission_model(input_value.model or record.settings.model)
        await self.approvals._run(lambda: self._workspaces.validate(record.workspace))
        async def initialize():
            return await self.storage("initialize_settings", session_id, record.settings, record.workspace,
                message or ("Image" if input_value.images else ""), uploads=uploads)
        await self.host._perform(initialize, control=True)
        if remember_model:
            await self.remember_model(session_id, selected.model_dump(mode="json"))
        input_value = input_value.model_copy(update={"model": selected})
        started = existing is not None and existing.get("runtime_started", True)
        initial_context = None if started else await self.host.applications["agent"].forks.initial_context(session_id)
        return await self.host.execute_json(BUILTIN_AGENT_WORKFLOW_ID, input_value.model_dump(mode="json"),
            revision_id=catalog["agent"]["revision_id"], entry_node_id="start", session_id=session_id,
            session_context=initial_context, require_existing_session=started, require_new_session=not started, before_start=before_start)

    async def tool_context(self, snapshot, call, *, context, phase):
        context = await super().tool_context(snapshot, call, context=context, phase=phase)
        if call.name == "register_workflow":
            from dataclasses import replace
            import asyncio
            import json
            key = self._workspaces.key(snapshot)
            group = self._workspaces._workflow_tools.get(key)
            registry = self._workspaces._catalogs.get(key)
            binding = context.authorization.get("tool_binding", {})
            if group is not None and registry is not None and any(entry.name == call.name and
                    entry.resource_id == binding.get("resource_id") for entry in registry.entries("tool")):
                try:
                    arguments = json.loads(call.arguments)
                    digest = await asyncio.to_thread(group.source_fingerprint, arguments)
                except (ValueError, TypeError, AttributeError, OSError):
                    digest = None
                context = replace(context, attributes={**context.attributes, "workflow_source_digest": digest})
        return context

    async def resolve_request_media(self, request):
        from autoagent.agent.models import AssetSource, ImageContent, LLMMessage
        from autoagent.core import OperatorCallContext

        sid = OperatorCallContext.current().session_id
        resolved, inputs = {}, []
        for item in request.input:
            if not isinstance(item, LLMMessage) or item.role != "user":
                inputs.append(item)
                continue
            parts = []
            for part in item.content:
                if isinstance(part, ImageContent) and isinstance(part.source, AssetSource) and part.source.uri.startswith(IMAGE_URI_PREFIX):
                    key = part.source.uri.removeprefix(IMAGE_URI_PREFIX)
                    if key not in resolved:
                        asset = await self.host.applications["agent"].forks.image(sid, key)
                        if asset is None or asset.mime_type != part.source.mime_type:
                            raise ValueError("An attached image is no longer available.")
                        resolved[key] = asset.inline()
                    part = part.model_copy(update={"source": resolved[key]})
                parts.append(part)
            inputs.append(item.model_copy(update={"content": tuple(parts)}) if any(
                before is not after for before, after in zip(item.content, parts)) else item)
        return request.model_copy(update={"input": tuple(inputs)}) if resolved else request


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
        record = await self.storage("settings", session_id, self.defaults)
        if not record.version:
            record = record.model_copy(update={"settings": self.default_preferences()})
        return self._remember(record)

    def _runtime_defaults(self, revision):
        defaults = super()._runtime_defaults(revision)
        selected = self.default_preferences().model
        return defaults.model_copy(update={"model": defaults.model.model_copy(update={
            "provider": selected.provider, "model": selected.model, "reasoning_effort": selected.reasoning_effort})})


    async def activate(self, sid, iid, **options):
        binding = await super().activate(sid, iid, **options)
        self._live_invocations.add(iid)
        return binding


    async def workspace_invocation(self, invocation_id):
        async def read():
            from autoagent.agent_app.workflow import BUILTIN_AGENT_WORKFLOW_ID
            from autoagent.host.models import HostNotFoundError
            family = await self.host._persistence.execution_family(invocation_id)
            if family is None or any(row["workflow_id"] != BUILTIN_AGENT_WORKFLOW_ID
                    for row in [family["current"], *(family["ancestors"] or []), *([family["root"]] if family["root"] else [])]):
                raise HostNotFoundError("This execution is not an Agent application conversation.")
            return family
        family = await self.host._perform(read)
        await self.workspace_session(family["current"]["session_id"])
        return family


    async def workspace_resources(self, revision, *, session_id=None, workspace_id=None, unbound=False):
        """List application resources for one selected product Workspace."""
        async def read():
            from types import SimpleNamespace
            from autoagent.agent.workflow.builtins import workflow_catalog
            ir = self.host._runtime.workflow_irs.get(revision) if self.host._runtime else None
            from .workflow import is_builtin_agent
            if ir is None or not is_builtin_agent(ir):
                raise HostNotFoundError("Agent application resources are unavailable.")
            if session_id is not None:
                await self.workspace_session(session_id, allow_new=True)
                record = await self.conversation_settings(session_id, workspace_id, unbound=unbound)
                selected_settings = self.effective_settings(record)
            else:
                selected_settings = self.defaults
            snapshot = SimpleNamespace(bindings={"workflow_revision": revision}, settings=selected_settings)
            await self.approvals._run(lambda: self.prepare_workspace(snapshot))
            tools, skills = workflow_catalog(self, snapshot, self.options_for_workflow(ir))
            workspace_skills = {entry.name for entry in self._workspaces.catalog(snapshot, ir.workflow_id)[1]}
            from .builtin_skills import is_bundled
            return dict(user_signal_endpoint=self.options_for_workflow(ir).user_signal_endpoint,
                tools=[dict(name=name, description=entry.value.definition.description,
                origin="Built-in" if entry.resource_id.startswith("builtin:") else
                    "Skill" if entry.resource_id.startswith("skills:") else "Project",
                version=entry.version, resource_id=entry.resource_id) for name, entry in tools.items()],
                skills=[dict(name=skill.metadata.name, description=skill.metadata.description,
                    origin="Workspace" if skill.metadata.name in workspace_skills
                        else "Built-in" if is_bundled(skill) else "Project") for skill in skills])
        return await self.host._perform(read)

"""SDK runtime state, resource access and invocation-bound model selection."""

from __future__ import annotations

import hashlib
import inspect
import json
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from collections.abc import AsyncIterator
from threading import RLock

from autoagent.agent.context import (
    AgentExecutionSnapshot, AgentModelSelection, AgentHostSettings, AgentSessionSettings, restore,
)
from autoagent.agent.workspace import WorkspaceRegistry
from autoagent.agent.models import LLMRequest, LLMStreamChunk

_UNSET = object()

class AgentRuntime:
    session_settings_type = AgentSessionSettings

    def __init__(self, services):
        self.services = services
        self._compaction_model = None
        self.defaults = AgentHostSettings()
        self._snapshots = {}
        self._session_records = {}
        self._agent_parents = {}
        self._lock = RLock()
        self._build_resources = {}
        self._resource_stack = None
        self._delegates = []
        self.workspaces = WorkspaceRegistry()
        from .approvals import AgentApprovals
        self.approvals = AgentApprovals(self)
        from .collaboration import AgentCollaboration
        self.collaboration = AgentCollaboration(self)
        from .state import AgentRuntimeState
        self._state = AgentRuntimeState(self)

    @contextmanager
    def context_scope(self, owns, context):
        """Borrow an application Context for explicitly selected Workflow definitions."""
        entry = (owns, context)
        self._delegates.append(entry)
        try:
            yield context
        finally:
            self._delegates.remove(entry)

    def contexts(self):
        root = self.services.extension("agent").context
        return (root, *(context for _, context in root._delegates))

    def for_definition(self, definition):
        root = self.services.extension("agent").context
        matches = [context for owns, context in root._delegates if owns(definition)]
        if len(matches) > 1:
            raise RuntimeError("Multiple application Contexts claim this Workflow.")
        return matches[0] if matches else root

    def for_workflow(self, revision):
        ir = self.services.runtime.workflow_irs.get(revision) if self.services.runtime else None
        return self.for_definition(ir) if ir is not None else self.services.extension("agent").context

    def for_call(self):
        from autoagent.core import OperatorCallContext
        call = OperatorCallContext.current()
        return self.for_workflow(call.workflow_revision_id) if call is not None else self

    async def for_invocation(self, iid):
        for context in self.contexts():
            if iid in context._snapshots:
                return context
        row = await self.services.persistence.get_invocation(iid, view="accepted")
        return self.for_workflow(row["workflow_revision_id"]) if row else self

    async def storage(self, method, *args, **kwargs):
        return await self._state.call(method, *args, **kwargs)

    def finalize_catalog(self, snapshot, catalog, skills):
        registry = self._build_resources.get(snapshot.bindings.get("workflow_revision"))
        entries = registry.filter_tools(catalog.values(), context=snapshot) if registry is not None else catalog.values()
        return {entry.name: entry for entry in entries}, skills

    @property
    def compaction_model(self):
        selected = self.services.providers.compaction_model
        return self._compaction_model or (selected.model if selected else None)

    @compaction_model.setter
    def compaction_model(self, value):
        self._compaction_model = value

    @property
    def compaction_provider(self):
        selected = self.services.providers.compaction_model
        return selected.provider if self._compaction_model is None and selected else None

    @property
    def compaction_reasoning_effort(self):
        selected = self.services.providers.compaction_model
        return self.bind_model(selected).reasoning_effort if self._compaction_model is None and selected else None

    def allows_recovery(self, status, *, interrupted=False, automatic=False):
        """Let execution owners restrict resumption without changing Core Recovery."""
        return True

    def _runtime_defaults(self, revision):
        ir = self.services.runtime.workflow_irs.get(revision)
        options = self.options_for_workflow(ir) if ir is not None else None
        defaults = options.runtime if options is not None and options.runtime is not None else self.defaults
        selected = self.services.providers.default_model
        if selected is not None and defaults.model.provider == "default" and defaults.model.model is None:
            defaults = defaults.model_copy(update={"model": defaults.model.model_copy(update=selected.model_dump())})
        return defaults

    async def export_binding(self, iid):
        return self._snapshots.get(iid)

    @asynccontextmanager
    async def borrow_tool(self, resource_id):
        from autoagent.core import OperatorCallContext
        call = OperatorCallContext.current()
        registry = self._build_resources.get(call.workflow_revision_id) if call is not None else None
        if registry is None:
            yield
        else:
            async with registry.borrow(resource_id):
                yield

    @asynccontextmanager
    async def resource_scope(self):
        async with AsyncExitStack() as stack:
            self._resource_stack = stack
            try:
                yield
            finally:
                self._resource_stack = None
                self._build_resources.clear()

    def process_managers(self):
        """Find invocation-scoped processes owned by explicitly bound Tool groups."""
        managers = set()
        for registry in self._build_resources.values():
            for entry in registry.entries("tool"):
                tool = entry.value
                while getattr(tool, "_owner", None) is not None:
                    tool = tool._owner._tools[entry.name]
                group = getattr(tool, "group", None)
                manager = getattr(getattr(group, "service", None), "manager", None)
                if manager is not None:
                    managers.add(manager)
        return managers

    async def cancel_processes(self, invocation_ids):
        """Cancel live OS processes on the loop that owns Agent resources."""
        async def stop():
            for context in self.contexts():
                for manager in context.process_managers():
                    await manager.terminate_invocations(set(invocation_ids))
        await self.approvals._run(stop)

    async def register_workspace(self, workspace, *, replace=False):
        """Register an explicitly supplied Workspace without binding it to Sessions."""
        async def register():
            self.workspaces.register(workspace, replace=replace)
            return workspace.info
        return await self.services.perform(register, control=True)

    async def list_workspaces(self):
        async def read():
            return [workspace.info.model_dump(mode="json") for workspace in self.workspaces.entries()]
        return await self.services.perform(read)
    async def resolve_workspace(self, binding):
        """Borrow an available Workspace using a stable Session binding."""
        from autoagent.agent.workspace import WorkspaceBinding
        binding = restore(WorkspaceBinding, binding)
        workspace = await self.workspaces.aresolve(binding.workspace_id)
        await workspace.avalidate(binding.working_directory)
        return workspace

    async def prepare_workspace(self, snapshot):
        binding = snapshot.settings.workspace
        if binding is not None:
            workspace = await self.resolve_workspace(binding)
            return await workspace.adiscover(binding.working_directory)

    def resources_for(self, revision):
        """Return the registry bound to one compiled Agent Workflow Revision."""
        return self._build_resources[revision]

    async def prepare_resources(self, workflows):
        async def prepare():
            for revision, ir in workflows.items():
                declaration = next((getattr(getattr(node.executable, "handler", None), "__autoagent_resources__", None)
                    for node in ir.nodes if getattr(getattr(node.executable, "handler", None), "__autoagent_resources__", None) is not None), None)
                if declaration is not None:
                    await declaration.install(self._resource_stack)
                    self._build_resources[revision] = declaration
        return await self.approvals._run(prepare)

    def resource_revision(self, snapshot):
        registry = self._build_resources.get(snapshot.bindings.get("workflow_revision"))
        return (registry.revision if registry is not None else 0,
            snapshot.settings.approval_mode, snapshot.settings.access.model_dump_json())

    def set_defaults(self, settings):
        self.defaults = restore(AgentHostSettings, settings)
        self._session_records.clear()


    async def resource_catalog(self, revision, *, session_id=None):
        """List only the selected Workflow's registry and local built-in Tools."""
        async def read():
            from types import SimpleNamespace
            from autoagent.agent.workflow.builtins import workflow_catalog
            ir = self.services.runtime.workflow_irs.get(revision) if self.services.runtime else None
            if ir is None or self.options_for_workflow(ir) is None:
                raise ValueError("Choose a registered Agent Workflow Revision.")
            if session_id is not None:
                record = await self.get_settings(session_id)
                selected_settings = self.effective_settings(record)
            else:
                selected_settings = self.defaults
            snapshot = SimpleNamespace(bindings={"workflow_revision": revision}, settings=selected_settings)
            if selected_settings.workspace is not None:
                await self.approvals._run(lambda: self.prepare_workspace(snapshot))
            tools, skills = workflow_catalog(self, snapshot, self.options_for_workflow(ir))
            return dict(user_signal_endpoint=self.options_for_workflow(ir).user_signal_endpoint,
                tools=[dict(name=name, description=entry.value.definition.description,
                origin="Built-in" if entry.resource_id.startswith("builtin:") else
                    "Skill" if entry.resource_id.startswith("skills:") else "Build",
                version=entry.version, resource_id=entry.resource_id) for name, entry in tools.items()],
                skills=[dict(name=skill.metadata.name, description=skill.metadata.description,
                    origin="Build") for skill in skills])
        return await self.services.perform(read)

    def effective_settings(self, record, model=None, workspace=None, *, defaults=None):
        defaults = defaults or self.defaults
        selected = model or record.settings.model
        return defaults.model_copy(update={
            "model": defaults.model.model_copy(update={"provider": selected.provider, "model": selected.model,
                "reasoning_effort": selected.reasoning_effort}),
            "approval_mode": record.settings.approval_mode,
            "workspace": record.workspace if workspace is None else workspace,
        })

    def bind_model(self, selected):
        if selected.provider in getattr(self.services.providers, "managed_ids", ()):
            model, info = self.services.providers.model(selected.provider, selected.model)
            if selected.reasoning_effort is None and model.reasoning_effort is not None:
                return selected.model_copy(update={"reasoning_effort": model.reasoning_effort.default})
        return selected

    def _remember(self, record):
        with self._lock:
            previous = self._session_records.get(record.session_id)
            if previous is None or record.version >= previous.version:
                self._session_records[record.session_id] = record
            return self._session_records[record.session_id]


    async def get_settings(self, session_id):
        return await self.services.perform(lambda: self._read_settings(session_id))

    async def get_snapshot(self, invocation_id):
        async def read():
            context = await self.for_invocation(invocation_id)
            return await context._get_snapshot(invocation_id, cache=False)
        return await self.services.perform(read)

    async def get_approval(self, execution, wait_id):
        """Read the current approval view, including automatic policy decisions."""
        async def read():
            async with self.services.execution(execution, load=True) as ref:
                result = await self.approvals.project(await self.services.runtime.app.astatus(ref))
                wait = next((wait for wait in result.waits if wait.id == wait_id), None)
                if wait is None:
                    raise ValueError("The approval Wait is no longer pending.")
                from autoagent.agent.workflow import AgentApprovalRequest
                return restore(AgentApprovalRequest, wait.request)
        return await self.services.perform(read)


    def _bindings(self, settings, revision):
        bindings = {}
        if settings.workspace is not None:
            bindings["workspace"] = settings.workspace.workspace_id
        if settings.model.provider in getattr(self.services.providers, "managed_ids", ()):
            bindings["provider"] = self.services.providers.get(settings.model.provider).version
        elif self.services.providers.has(settings.model.provider):
            bindings["provider"] = self.services.providers.registration(settings.model.provider).version
        elif settings.model.provider == "default":
            # Preserve application-provided LLM_CALL implementations. Explicit
            # Provider aliases always require a registered resource.
            bindings["provider"] = "@capability"
        else:
            raise ValueError(f"Unknown Agent Provider: {settings.model.provider!r}.")
        if settings.access.policy is not None:
            bindings["policy"] = self.resources_for(revision).get("policy", settings.access.policy).version
        return bindings

    def workflow_options(self, snapshot):
        ir = self.services.runtime.workflow_irs.get(snapshot.bindings.get("workflow_revision"))
        if ir is None:
            raise RuntimeError("Agent Workflow Revision is not registered.")
        return self.options_for_workflow(ir)

    async def tool_context(self, snapshot, call, *, context, phase):
        """Ask this Workflow's caller for fresh permission facts, never product data."""
        from ..tools import ToolContextRequest, provide_tool_context
        revision = snapshot.bindings.get("workflow_revision")
        ir = self.services.runtime.workflow_irs.get(revision)
        if ir is None:
            raise RuntimeError("Agent Workflow Revision is not registered.")
        source = next((getattr(getattr(node.executable, "handler", None), "__autoagent_tool_context_source__", None)
            for node in ir.nodes if getattr(getattr(node.executable, "handler", None), "__autoagent_tool_context_source__", None)
            is not None), None)
        if source is None:
            return context
        return await provide_tool_context(source, ToolContextRequest(
            call=call, context=context.for_call(call), phase=phase,
            workflow_id=ir.workflow_id, workflow_revision_id=revision, invocation_id=snapshot.invocation_id))

    @staticmethod
    def options_for_workflow(ir):
        if ir is None:
            return None
        for node in ir.nodes:
            options = getattr(getattr(node.executable, "handler", None), "__autoagent_options__", None)
            if options is not None:
                return options
        return None

    def adopt(self, binding):
        with self._lock:
            self._snapshots[binding["invocation_id"]] = binding

    def forget(self, invocation_id):
        with self._lock:
            discarded, pending = set(), [invocation_id]
            while pending:
                current = pending.pop()
                if current in discarded:
                    continue
                discarded.add(current)
                pending.extend(child for child, parent in self._agent_parents.items() if parent == current)
            for current in discarded:
                for context in self.contexts():
                    context._snapshots.pop(current, None)
                    context._state.bindings.pop(current, None)
                self._agent_parents.pop(current, None)

    async def snapshot(self):
        from autoagent.core import OperatorCallContext
        call = OperatorCallContext.current()
        if call is None or call.invocation_id is None:
            raise RuntimeError("An Agent execution Context requires an Operator call identity.")
        return await self.execution_snapshot(call.invocation_id)

    async def execution_snapshot(self, invocation_id):
        context = await self.for_invocation(invocation_id)
        snapshot = await context._get_snapshot(invocation_id)
        if snapshot is None:
            raise RuntimeError("This Invocation has no Agent execution snapshot.")
        return snapshot

    def catalog(self, snapshot):
        from autoagent.agent import SkillSet
        ir = self.services.runtime.workflow_irs.get(snapshot.bindings.get("workflow_revision"))
        if ir is None:
            raise RuntimeError("Agent Workflow Revision is not registered.")
        registry = self._build_resources.get(ir.workflow_revision_id)
        tools, skills = registry.snapshot(snapshot) if registry is not None else ((), ())
        for kind, entries in (("Tool", tools), ("Skill", skills)):
            if len({entry.name for entry in entries}) != len(entries):
                raise ValueError(f"{kind} names collide within Workflow {ir.workflow_id!r}.")
        selected = {entry.name: entry for entry in tools}
        if skills:
            skillset = SkillSet(entry.value for entry in skills)
            version = hashlib.sha256(json.dumps([(entry.resource_id, entry.version) for entry in skills],
                sort_keys=True).encode()).hexdigest()
            from autoagent.agent.resources import AgentResource
            async def skill_policy(call, context):
                entry = next((entry for entry in skills if entry.name == context.arguments.get("name")), None)
                if entry is None:
                    return {"action": "deny", "reason": "Skill is outside this Workflow's resource scope."}
                return await self._resource_policy(entry.policy, call, context) if entry.policy else {"action": "allow"}
            for tool in skillset.as_tools():
                if tool.definition.name in selected:
                    raise ValueError("A registered Tool collides with a Skill Tool.")
                selected[tool.definition.name] = AgentResource(tool.definition.name, version, tool,
                    resource_id="skills:" + hashlib.sha256(json.dumps([entry.resource_id for entry in skills]).encode()).hexdigest()
                        + "/" + tool.definition.name,
                    policy=skill_policy, dependencies=skills)
        return selected, tuple(entry.value for entry in skills)

    @staticmethod
    async def _resource_policy(handler, call, context):
        from autoagent.core.operators import run_sync
        if inspect.iscoroutinefunction(handler) or inspect.iscoroutinefunction(getattr(handler, "__call__", None)):
            result = handler(call, context)
        else:
            result = await run_sync(handler, call, context)
        return await result if inspect.isawaitable(result) else result

    @staticmethod
    def _permissions(entry):
        from autoagent.agent.workflow.resources import resource_permissions
        return resource_permissions(entry)

    def toolset(self, snapshot, *, binding=None, catalog=None, observations=None):
        from autoagent.agent import ToolContext, ToolDecision, ToolSet
        from autoagent.core.operators import run_sync
        if catalog is None:
            catalog, _ = self.catalog(snapshot)
        access = snapshot.settings.access

        async def policy(call, context):
            action = access.rules.get(call.name, access.default)
            decisions = [ToolDecision(action=action, reason="Agent Tool access rule" if action != "allow" else None)]
            entry = catalog.get(call.name)
            if entry is None:
                return ToolDecision(action="deny", reason="Tool is outside this Workflow's resource scope.")
            if entry.policy is not None:
                decisions.append(ToolDecision.model_validate(await self._resource_policy(entry.policy, call, context)))
            if access.policy is not None:
                handler = self.resources_for(snapshot.bindings["workflow_revision"]).get(
                    "policy", access.policy, version=snapshot.bindings["policy"]).value
                if inspect.iscoroutinefunction(handler) or inspect.iscoroutinefunction(getattr(handler, "__call__", None)):
                    result = handler(call, context)
                else:
                    result = await run_sync(handler, call, context)
                if inspect.isawaitable(result):
                    result = await result
                decisions.append(ToolDecision.model_validate(result))
            denied = next((value for value in decisions if value.action == "deny"), None)
            if denied is not None:
                return denied
            asks = [value for value in decisions if value.action == "ask"]
            return ToolDecision(action="ask" if asks else "allow",
                reason="; ".join(value.reason for value in asks if value.reason) or None)

        authorization = {"access": access.model_dump(mode="json"), "policy_version": snapshot.bindings.get("policy"),
                         "snapshot_id": snapshot.snapshot_id}
        if binding is not None:
            authorization["tool_binding"] = binding
            entry = catalog.get(binding["name"])
            authorization["resource_permissions"] = self._permissions(entry) if entry is not None else None
        context = ToolContext(session_id=snapshot.session_id,
            allowed_tools=frozenset(access.allowed_tools) if access.allowed_tools is not None else None,
            services={"agent_snapshot": snapshot, "agent_observations": observations or {}}, authorization=authorization,
            approval_scope={"session_id": snapshot.session_id, "invocation_id": snapshot.invocation_id})
        return ToolSet((entry.value for entry in catalog.values()), context=context, policy=policy,
            approval_mode=snapshot.settings.approval_mode)

    def history_route(self, snapshot, model, *, provider=None, provider_name=None):
        """Capture replay affinity without binding ordinary history to a Provider."""
        from autoagent.agent.workflow.history import HistoryRoute
        version = snapshot.bindings.get("provider")
        if version in (None, "@capability") or model is None:
            return None
        try:
            if provider is not None:
                version = getattr(provider, "version", version)
            elif snapshot.settings.model.provider in getattr(self.services.providers, "managed_ids", ()):
                provider = self.services.providers.get(snapshot.settings.model.provider)
                version = provider.version
            else:
                provider = self.services.providers.registration(snapshot.settings.model.provider, version=version).value
        except KeyError:
            # Unregistration may race a completed call's output binding. Keep
            # its original response; future dispatch still checks availability.
            return None
        adapter = getattr(provider, "adapter", None)
        supports_tool_media = getattr(provider, "supports_tool_media", None)
        tool_media = supports_tool_media(model) if callable(supports_tool_media) else getattr(adapter, "tool_media", False)
        config = getattr(provider, "config", None)
        def identity(value):
            return type(value).__module__ + "." + type(value).__qualname__ if value is not None else None
        connection = hashlib.sha256(json.dumps({
            "implementation": identity(provider), "adapter": identity(adapter),
            "base_url": getattr(config, "base_url", None), "model_prefix": getattr(config, "model_prefix", None),
        }, sort_keys=True).encode()).hexdigest()
        return HistoryRoute(provider=provider_name or snapshot.settings.model.provider, version=version, model=model,
            provider_id=getattr(provider, "provider_id", None), protocol=getattr(provider, "protocol", None),
            connection=connection, tool_media=tool_media,
            context_role=provider.context_message_role(model) if callable(getattr(provider, "context_message_role", None)) else "user")

    async def resolve_selection(self, request):
        context = self.for_call()
        if context is not self:
            return await context.resolve_selection(request)
        from autoagent.agent import LLMSelection
        snapshot = await self.snapshot()
        if snapshot.settings.workspace is not None:
            await self.resolve_workspace(snapshot.settings.workspace)
        if request.routing.get("agent_snapshot_id") != snapshot.snapshot_id:
            raise RuntimeError("Prepared LLM Request belongs to an obsolete execution stage.")
        if request.model is None:
            raise ValueError("Agent calls require an explicit model.")
        if request.routing.get("agent_role") == "compaction" and request.routing.get("compaction_provider"):
            return LLMSelection(self.services.providers.get(request.routing["compaction_provider"]), request.model)
        if snapshot.settings.model.provider in getattr(self.services.providers, "managed_ids", ()):
            return LLMSelection(self.services.providers.get(request.routing.get("compaction_provider") or snapshot.settings.model.provider), request.model)
        resource = self.services.providers.registration(snapshot.settings.model.provider,
            version=snapshot.bindings["provider"])
        return LLMSelection(resource.value, request.model)

    async def resolve_request_media(self, request):
        return request

    def install_operators(self, setup, workflows):
        """Route managed requests without changing custom/non-Agent selection."""
        if not any(getattr(getattr(node.executable, "handler", None), "__autoagent_options__", None) is not None
                   for workflow in workflows.values() for node in workflow.nodes):
            return
        from autoagent.agent import LLM_CALL
        from autoagent.agent.integration.capabilities import _invoke_provider
        from autoagent.agent.workflow.provenance import stamp_response
        from autoagent.core import Operator

        async def invoke(request: LLMRequest) -> AsyncIterator[LLMStreamChunk]:
            snapshot = await self.snapshot()
            selection = await self.resolve_selection(request)
            context = self.for_call()
            origin = context.history_route(snapshot, request.model, provider=selection.provider,
                provider_name=request.routing.get("compaction_provider"))
            request = await context.resolve_request_media(request)
            source = _invoke_provider(selection.provider, request)
            try:
                async for chunk in source:
                    if chunk.type == "completed" and chunk.response is not None:
                        chunk = chunk.model_copy(update={"response": stamp_response(
                            chunk.response, origin, snapshot.snapshot_id)})
                    yield chunk
            finally:
                await source.aclose()

        managed = Operator(invoke, id="agent.llm.call")
        previous = setup.capability_resolvers.get(LLM_CALL.id)
        ordinary = tuple(item for item in setup.operators if item.capability_id == LLM_CALL.id and item.enabled)
        setup.register_operator(managed, capability_id=LLM_CALL.id)

        async def resolve(capability, request):
            if request.routing.get("agent_managed_provider"):
                return managed
            if previous is not None:
                value = previous(capability, request)
                return await value if inspect.isawaitable(value) else value
            defaults = [item for item in ordinary if item.default]
            if defaults:
                return defaults[0].operator
            if ordinary:
                highest = max(item.priority for item in ordinary)
                best = [item for item in ordinary if item.priority == highest]
                if len(best) == 1:
                    return best[0].operator
            raise RuntimeError("No unambiguous LLM_CALL implementation is configured.")
        setup.capability_resolvers[LLM_CALL.id] = resolve

    async def configure(self, session_id, settings, *, expected_version=None, workspace=_UNSET):
        settings = restore(self.session_settings_type, settings)
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError("Session ID must be nonempty.")
        if expected_version is not None and (type(expected_version) is not int or expected_version < 0):
            raise ValueError("expected_version must be a nonnegative integer.")
        async def update():
            previous = await self.get_settings(session_id)
            from autoagent.agent import WorkspaceBinding
            binding = previous.workspace if workspace is _UNSET else restore(WorkspaceBinding, workspace) if workspace is not None else None
            if binding is not None:
                await self.approvals._run(lambda: self.resolve_workspace(binding))
            record = await self.storage("configure", session_id, settings, expected_version, binding)
            self._remember(record)
            await self.approvals.changed(session_id)
            return record
        return await self.services.perform(update, payload=settings.model_dump(mode="json"), control=True)

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
        effective = self.effective_settings(record, selected, defaults=self._runtime_defaults(found["bindings"]["workflow_revision"]))
        effective = effective.model_copy(update={"workspace": restore(WorkspaceBinding, found["workspace"]) if found["workspace"] else None})
        bindings = {**found["bindings"], **self._bindings(effective, found["bindings"]["workflow_revision"])}
        return AgentExecutionSnapshot(snapshot_id=found["binding_id"], session_id=found["session_id"], invocation_id=iid,
            epoch=1, reason="start", created_at_us=found["created_at_us"], settings_version=record.version,
            settings=effective, bindings=bindings)

    async def activate(self, sid, iid, *, reason, activation_id, workflow_revision=None, model=None, message=None):
        record = await self._read_settings(sid)
        defaults = self._runtime_defaults(workflow_revision)
        if not record.version:
            record = record.model_copy(update={"settings": defaults.session_defaults(), "workspace": defaults.workspace})
        selected = restore(AgentModelSelection, model) if model is not None else record.settings.model
        workspace = record.workspace
        parent_id = self._agent_parents.get(iid)
        if parent_id is not None and not record.version:
            parent = await self._get_snapshot(parent_id) if await self.for_invocation(parent_id) is self else None
            if parent is not None:
                if model is None:
                    selected = AgentModelSelection(provider=parent.settings.model.provider, model=parent.settings.model.model,
                        reasoning_effort=parent.settings.model.reasoning_effort)
                workspace = parent.settings.workspace
                record = record.model_copy(update={"settings": record.settings.model_copy(
                    update={"approval_mode": parent.settings.approval_mode})})
        selected = self.bind_model(selected)
        if not record.version:
            preferences = record.settings.model_copy(update={"model": selected})
            record = self._remember(await self.storage(
                "initialize_settings", sid, preferences, workspace, message))
        settings = self.effective_settings(record, selected, defaults=defaults).model_copy(update={"workspace": workspace})
        bindings = {"workflow_revision": workflow_revision}
        self._bindings(settings, workflow_revision)  # Validate before admission.
        ir = self.services.runtime.workflow_irs.get(workflow_revision)
        options = self.options_for_workflow(ir) if ir is not None else None
        if options is not None and options.subagents.enabled:
            for alias, target in options.subagents.targets.items():
                revision = target.workflow_revision_id or self.services.runtime.workflow_ids.get(target.workflow_id)
                if revision not in self.services.runtime.workflow_irs:
                    raise ValueError(f"Agent target {alias!r} is not registered.")
                current = self.services.runtime.workflow_irs[revision]
                if current.workflow_id != target.workflow_id or target.entry_node_id not in current.entry_node_ids:
                    raise ValueError(f"Agent target {alias!r} has an invalid Workflow or entry Node.")
                bindings["target:" + alias] = revision
        return await self.storage("activate", sid, iid, selected, workspace, bindings, activation_id)

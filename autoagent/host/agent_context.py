"""Host-owned Session controls, immutable execution settings and live resources."""

from __future__ import annotations

import hashlib
import inspect
import json
from contextlib import AsyncExitStack, asynccontextmanager
from collections.abc import AsyncIterator
from threading import RLock

from autoagent.agent.context import (
    AgentExecutionSnapshot, AgentModelSettings, AgentSessionSettings, restore,
)
from autoagent.agent.resources import AgentResourceRegistry
from autoagent.agent.models import LLMRequest, LLMStreamChunk


class HostAgentContext:
    def __init__(self, host):
        from .agent import AgentSettings
        self.host = host
        self.resources = AgentResourceRegistry()
        defaults = AgentSettings.from_environment(host.environment)
        self.compaction_model = host.environment.get("AUTOAGENT_COMPACTION_LLM_MODEL", "").strip() or None
        self.defaults = AgentSessionSettings(model=AgentModelSettings(
            model=defaults.model if defaults else None,
            streaming=defaults.streaming if defaults else False,
            max_output_tokens=defaults.max_output_tokens if defaults else None,
        ))
        self._snapshots = {}
        self._agent_parents = {}
        self._lock = RLock()
        self._build_resources = {}
        self._resource_stack = None
        from .agent_approvals import AgentApprovals
        self.approvals = AgentApprovals(self)
        from .agent_collaboration import AgentCollaboration
        self.collaboration = AgentCollaboration(self)

    def mcp_tools(self, client, *, name=None, reconnect_policy=None, workflows=(), policy=None, **discovery):
        """Enter this source in Host lifespan after entering the MCP client."""
        from autoagent.agent import MCPToolRegistry, MCPResourceSource
        return MCPResourceSource(self.resources, MCPToolRegistry(client, **discovery),
            name=name, reconnect_policy=reconnect_policy, workflows=workflows, policy=policy)

    @asynccontextmanager
    async def resource_scope(self):
        async with AsyncExitStack() as stack:
            self._resource_stack = stack
            try:
                yield
            finally:
                self._resource_stack = None
                self._build_resources.clear()

    async def prepare_resources(self, workflows):
        async def prepare():
            for revision, ir in workflows.items():
                declaration = next((getattr(getattr(node.executable, "handler", None), "__autoagent_resources__", None)
                    for node in ir.nodes if getattr(getattr(node.executable, "handler", None), "__autoagent_resources__", None) is not None), None)
                if declaration is not None:
                    registry = AgentResourceRegistry(namespace="build:" + revision)
                    await declaration.install(self._resource_stack, registry, ir.workflow_id)
                    self._build_resources[revision] = registry
        return await self.approvals._run(prepare)

    def resource_revision(self, snapshot):
        registry = self._build_resources.get(snapshot.bindings.get("workflow_revision"))
        return self.resources.revision, registry.revision if registry is not None else 0

    def skill_directories(self, roots, **options):
        """Enter this source in Host lifespan to synchronize Skill snapshots."""
        from autoagent.agent import SkillDirectorySource
        return SkillDirectorySource(self.resources, roots, **options)

    def set_defaults(self, settings):
        self.defaults = restore(AgentSessionSettings, settings)

    async def workspace_catalog(self):
        """Describe only the SDK Agent; user Agent builds are execution Workflows."""
        async def read():
            from autoagent.agent.workflow.application import BUILTIN_AGENT_WORKFLOW_ID, is_builtin_agent
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
                providers=providers, defaults=self.defaults.model_dump(mode="json"))
        return await self.host._perform(read)

    async def workspace_session(self, session_id, *, allow_new=False):
        """Reject heterogeneous or user-workflow Sessions at the application boundary."""
        async def read():
            from autoagent.agent.workflow.application import BUILTIN_AGENT_WORKFLOW_ID
            from .models import HostNotFoundError
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

    async def workspace_invocation(self, invocation_id):
        async def read():
            from autoagent.agent.workflow.application import BUILTIN_AGENT_WORKFLOW_ID
            from .models import HostNotFoundError
            family = await self.host._persistence.execution_family(invocation_id)
            if family is None or any(row["workflow_id"] != BUILTIN_AGENT_WORKFLOW_ID
                    for row in [family["current"], *(family["ancestors"] or []), *([family["root"]] if family["root"] else [])]):
                raise HostNotFoundError("This execution is not an SDK Agent conversation.")
            return family
        family = await self.host._perform(read)
        await self.workspace_session(family["current"]["session_id"])
        return family

    async def start_conversation(self, session_id, message):
        from autoagent.agent.workflow.application import BUILTIN_AGENT_WORKFLOW_ID
        from .models import HostNotFoundError
        catalog = await self.workspace_catalog()
        if catalog["agent"] is None:
            raise HostNotFoundError("The SDK Agent is disabled for this project.")
        existing = await self.workspace_session(session_id, allow_new=True)
        return await self.host.execute_json(BUILTIN_AGENT_WORKFLOW_ID, {"message": message},
            revision_id=catalog["agent"]["revision_id"], entry_node_id="start", session_id=session_id,
            require_existing_session=existing is not None, require_new_session=existing is None)

    async def workspace_resources(self, revision, *, builtin=False):
        """List only the selected Workflow's Build bindings and explicit Host grants."""
        async def read():
            from types import SimpleNamespace
            from autoagent.agent.workflow.builtins import workflow_catalog
            ir = self.host._runtime.workflow_irs.get(revision) if self.host._runtime else None
            if builtin:
                from autoagent.agent.workflow.application import is_builtin_agent
                from .models import HostNotFoundError
                if ir is None or not is_builtin_agent(ir):
                    raise HostNotFoundError("SDK Agent resources are unavailable.")
            if ir is None or self.options_for_workflow(ir) is None:
                raise ValueError("Choose a registered Agent Workflow Revision.")
            snapshot = SimpleNamespace(bindings={"workflow_revision": revision})
            tools, skills = workflow_catalog(self, snapshot, self.options_for_workflow(ir))
            registry = self._build_resources.get(revision)
            local_skills = {entry.name for entry in registry.catalog(ir.workflow_id)[1]} if registry else set()
            return dict(user_signal_endpoint=self.options_for_workflow(ir).user_signal_endpoint,
                tools=[dict(name=name, description=entry.value.definition.description,
                origin="Built-in" if entry.resource_id.startswith("builtin:") else
                    "Skill" if entry.resource_id.startswith("skills:") else "Build" if entry.resource_id.startswith("build:") else "Host",
                version=entry.version) for name, entry in tools.items()],
                skills=[dict(name=skill.metadata.name, description=skill.metadata.description,
                    origin="Build" if skill.metadata.name in local_skills else "Host") for skill in skills])
        return await self.host._perform(read)

    async def configure(self, session_id, settings, *, expected_version=None):
        settings = restore(AgentSessionSettings, settings)
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError("Session ID must be nonempty.")
        if expected_version is not None and (type(expected_version) is not int or expected_version < 0):
            raise ValueError("expected_version must be a nonnegative integer.")
        async def update():
            return await self.host._persistence.agent_context_operation(
                "configure", session_id, settings, expected_version)
        return await self.host._perform(update, payload=settings.model_dump(mode="json"), control=True)

    async def get_settings(self, session_id):
        async def read():
            return await self.host._persistence.agent_context_operation("settings", session_id, self.defaults)
        return await self.host._perform(read)

    async def get_snapshot(self, invocation_id):
        async def read():
            return await self._get_snapshot(invocation_id, cache=False)
        return await self.host._perform(read)

    async def get_approval(self, execution, wait_id):
        """Read the current approval view, including automatic policy decisions."""
        async def read():
            async with self.host._execution(execution, load=True) as ref:
                result = await self.approvals.project(await self.host._runtime.app.astatus(ref))
                wait = next((wait for wait in result.waits if wait.id == wait_id), None)
                if wait is None:
                    raise ValueError("The approval Wait is no longer pending.")
                from autoagent.agent.workflow import AgentApprovalRequest
                return restore(AgentApprovalRequest, wait.request)
        return await self.host._perform(read)

    async def _get_snapshot(self, iid, *, cache=True):
        with self._lock:
            found = self._snapshots.get(iid)
        if found is None:
            found = await self.host._persistence.agent_context_operation("snapshot", iid)
            if found is not None and cache:
                self.adopt(found)
        return restore(AgentExecutionSnapshot, found) if found is not None else None

    def _bindings(self, settings):
        bindings = {}
        if self.resources.has("provider", settings.model.provider):
            bindings["provider"] = self.resources.get("provider", settings.model.provider).version
        elif settings.model.provider == "default":
            # Preserve application-provided LLM_CALL implementations. Explicit
            # Provider aliases always require a registered resource.
            bindings["provider"] = "@capability"
        else:
            raise ValueError(f"Unknown Agent Provider: {settings.model.provider!r}.")
        if settings.access.policy is not None:
            bindings["policy"] = self.resources.get("policy", settings.access.policy).version
        return bindings

    async def activate(self, sid, iid, *, reason, activation_id, workflow_revision=None):
        previous = await self._get_snapshot(iid)
        workflow_revision = workflow_revision or (previous.bindings.get("workflow_revision") if previous else None)
        defaults = self.defaults
        parent_id = self._agent_parents.get(iid)
        if parent_id is not None:
            parent = await self._get_snapshot(parent_id)
            if parent is not None:
                defaults = parent.settings
        def bind(settings):
            bindings = {**self._bindings(settings), **({"workflow_revision": workflow_revision} if workflow_revision else {})}
            ir = self.host._runtime.workflow_irs.get(workflow_revision)
            options = self.options_for_workflow(ir) if ir is not None else None
            if options is not None and options.subagents.enabled:
                for alias, target in options.subagents.targets.items():
                    revision = target.workflow_revision_id or self.host._runtime.workflow_ids.get(target.workflow_id)
                    if revision not in self.host._runtime.workflow_irs:
                        raise ValueError(f"Agent target {alias!r} is not registered.")
                    selected = self.host._runtime.workflow_irs[revision]
                    if selected.workflow_id != target.workflow_id or target.entry_node_id not in selected.entry_node_ids:
                        raise ValueError(f"Agent target {alias!r} has an invalid Workflow or entry Node.")
                    bindings["target:" + alias] = revision
            return bindings
        return await self.host._persistence.agent_context_operation(
            "activate", sid, iid, defaults, reason, activation_id, bind)

    def workflow_options(self, snapshot):
        ir = self.host._runtime.workflow_irs.get(snapshot.bindings.get("workflow_revision"))
        if ir is None:
            raise RuntimeError("Agent Workflow Revision is not registered.")
        return self.options_for_workflow(ir)

    @staticmethod
    def options_for_workflow(ir):
        for node in ir.nodes:
            options = getattr(getattr(node.executable, "handler", None), "__autoagent_options__", None)
            if options is not None:
                return options
        return None

    def adopt(self, snapshot):
        with self._lock:
            for iid, previous in tuple(self._snapshots.items()):
                if previous.session_id == snapshot.session_id:
                    del self._snapshots[iid]
            self._snapshots[snapshot.invocation_id] = snapshot

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
                self._snapshots.pop(current, None)
                self._agent_parents.pop(current, None)

    async def snapshot(self):
        from autoagent.core import OperatorCallContext
        call = OperatorCallContext.current()
        if call is None or call.invocation_id is None:
            raise RuntimeError("An Agent execution Context requires an Operator call identity.")
        return await self.execution_snapshot(call.invocation_id)

    async def execution_snapshot(self, invocation_id):
        snapshot = await self._get_snapshot(invocation_id)
        if snapshot is None:
            raise RuntimeError("This Invocation has no Agent execution snapshot.")
        return snapshot

    def catalog(self, snapshot):
        from autoagent.agent import SkillSet
        ir = self.host._runtime.workflow_irs.get(snapshot.bindings.get("workflow_revision"))
        if ir is None:
            raise RuntimeError("Agent Workflow Revision is not registered.")
        registry = self._build_resources.get(ir.workflow_revision_id)
        # Read a complete union while publishers for both origins are excluded.
        with self.resources._lock:
            if registry is None:
                tools, skills = self.resources.catalog(ir.workflow_id)
            else:
                with registry._lock:
                    shared_tools, shared_skills = self.resources.catalog(ir.workflow_id)
                    local_tools, local_skills = registry.catalog(ir.workflow_id)
                    tools, skills = (*local_tools, *shared_tools), (*local_skills, *shared_skills)
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
            decisions = [ToolDecision(action=action, reason="Session Tool access rule" if action != "allow" else None)]
            entry = catalog.get(call.name)
            if entry is None:
                return ToolDecision(action="deny", reason="Tool is outside this Workflow's resource scope.")
            if entry.policy is not None:
                decisions.append(ToolDecision.model_validate(await self._resource_policy(entry.policy, call, context)))
            if access.policy is not None:
                handler = self.resources.get("policy", access.policy, version=snapshot.bindings["policy"]).value
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

    def history_route(self, snapshot, model):
        """Capture replay affinity without binding ordinary history to a Provider."""
        from autoagent.agent.workflow.history import HistoryRoute
        version = snapshot.bindings.get("provider")
        if version in (None, "@capability") or model is None:
            return None
        try:
            provider = self.resources.get("provider", snapshot.settings.model.provider, version=version).value
        except KeyError:
            # Unregistration may race a completed call's output binding. Keep
            # its original response; future dispatch still checks availability.
            return None
        adapter = getattr(provider, "adapter", None)
        config = getattr(provider, "config", None)
        def identity(value):
            return type(value).__module__ + "." + type(value).__qualname__ if value is not None else None
        connection = hashlib.sha256(json.dumps({
            "implementation": identity(provider), "adapter": identity(adapter),
            "base_url": getattr(config, "base_url", None), "model_prefix": getattr(config, "model_prefix", None),
        }, sort_keys=True).encode()).hexdigest()
        return HistoryRoute(provider=snapshot.settings.model.provider, version=version, model=model,
            provider_id=getattr(provider, "provider_id", None), protocol=getattr(provider, "protocol", None),
            connection=connection, tool_media=getattr(adapter, "tool_media", False))

    async def resolve_selection(self, request):
        from autoagent.agent import LLMSelection
        snapshot = await self.snapshot()
        if request.routing.get("agent_snapshot_id") != snapshot.snapshot_id:
            raise RuntimeError("Prepared LLM Request belongs to an obsolete execution stage.")
        resource = self.resources.get("provider", snapshot.settings.model.provider,
            version=snapshot.bindings["provider"])
        if request.model is None:
            raise ValueError("Agent calls require an explicit model.")
        return LLMSelection(resource.value, request.model)

    def install_operators(self, setup):
        """Route managed requests without changing custom/non-Agent selection."""
        if not any(getattr(getattr(node.executable, "handler", None), "__autoagent_options__", None) is not None
                   for workflow in setup.workflows for node in workflow.nodes):
            return
        from autoagent.agent import LLM_CALL, llm_call_operator
        from autoagent.agent.workflow.provenance import stamp_response
        from autoagent.core import Operator
        adapter = llm_call_operator(resolver=self.resolve_selection)

        async def invoke(request: LLMRequest) -> AsyncIterator[LLMStreamChunk]:
            snapshot = await self.snapshot()
            origin = self.history_route(snapshot, request.model)
            source = adapter.handler(request)
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

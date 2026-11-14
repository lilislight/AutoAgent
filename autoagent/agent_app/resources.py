"""Persisted product resources, owned and synchronized on the Runtime loop."""

import asyncio
from collections.abc import Mapping
import json
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from uuid import uuid4

from pydantic import TypeAdapter

from autoagent.agent.context import plain
from .resource_config import AgentResourceConfig
from autoagent.agent.resources import AgentResourceRegistry


CONFIG = TypeAdapter(AgentResourceConfig)


@dataclass
class _Source:
    record: dict
    registry: AgentResourceRegistry
    stop: asyncio.Event = field(default_factory=asyncio.Event)
    drained: asyncio.Event = field(default_factory=asyncio.Event)
    active: int = 0
    task: object = None
    source: object = None
    state: str = "connecting"
    error: str | None = None

    def __post_init__(self):
        self.drained.set()


class ManagedAgentResources:
    """Product resource configuration; SDK Workflows retain their own registries."""

    def __init__(self, service):
        self.service = service
        self.records = {}
        self.sources = {}
        self.retired = []
        self.revision = 0
        self.lock = asyncio.Lock()
        self.monitor = None
        self.enabled = False
        self._known_resources = {}

    def skill_roots(self):
        roots = [workspace.root for workspace in self.service.workspaces.entries() if hasattr(workspace, "root")]
        roots.extend(Path(config.path) for _, config in self.records.values() if config.kind == "skills")
        configured = self.service.host.environment.get("AUTOAGENT_APP_SKILLS_DIR", "").strip()
        if configured:
            root = Path(configured).expanduser()
            roots.append(root if root.is_absolute() else self.service.host.root / root)
        return tuple(roots)

    def _permissions_editable(self, origin):
        return origin != "builtin" and (origin != "project" or self.service.host.applications["agent"].edit_code_permissions)

    @asynccontextmanager
    async def lifespan(self):
        self.enabled = self.service.host.applications["agent"].enabled
        rows = await self.service.storage("resource_records")
        for row in rows:
            self._publish(row["id"], row)
        configured = self.service.host.environment.get("AUTOAGENT_APP_SKILLS_DIR", "").strip()
        if configured:
            from autoagent.agent import SkillDirectorySource
            root = Path(configured).expanduser()
            root = root if root.is_absolute() else self.service.host.root / root
            self.environment_skills = SkillDirectorySource(self.service.resources, [root], name="environment-skills",
                workflows={"autoagent.agent"})
            await self.environment_skills.__aenter__()
        else:
            self.environment_skills = None
        self.monitor = asyncio.create_task(self._watch())
        try:
            yield
        finally:
            self.monitor.cancel()
            await asyncio.gather(self.monitor, return_exceptions=True)
            sources = [*self.sources.values(), *self.retired]
            for source in sources:
                source.stop.set()
                source.task.cancel()
            await asyncio.gather(*(source.task for source in sources), return_exceptions=True)
            self.sources.clear()
            self.retired.clear()
            self.records.clear()
            if self.environment_skills is not None:
                await self.environment_skills.__aexit__(None, None, None)

    async def _watch(self):
        previous = None
        while True:
            revision = tuple((key, value.registry.revision, value.state) for key, value in self.sources.items())
            if revision != previous:
                previous = revision
                await self.service.approvals.changed()
            self.retired[:] = [source for source in self.retired if not source.task.done()]
            await asyncio.sleep(.1)

    @property
    def identity(self):
        return (self.revision, tuple((key, value.registry.revision)
            for key, value in tuple(self.sources.items())))

    def _publish(self, resource_id, row):
        config = CONFIG.validate_json(json.dumps(row["configuration"])) if row is not None else None
        previous = self.records.get(resource_id)
        old = self.sources.get(resource_id)
        if (old is not None and previous is not None and config is not None and config.enabled
                and old.state != "error" and config.model_dump(exclude={"name", "permissions"}) ==
                    previous[1].model_dump(exclude={"name", "permissions"})):
            # A permission-only edit must not replace an executable or drop an
            # MCP connection that is already serving approved calls.
            self.records[resource_id] = (row, config)
            old.record = row
            self.revision += 1
            return
        old = self.sources.pop(resource_id, None)
        if old is not None:
            old.stop.set()
            if old.state == "connecting":
                old.task.cancel()
            self.retired.append(old)
        if row is None:
            self.records.pop(resource_id, None)
        else:
            self.records[resource_id] = (row, config)
            if self.enabled and config.enabled and config.kind in {"mcp", "skills"}:
                source = _Source(row, AgentResourceRegistry(namespace="managed:" + resource_id))
                self.sources[resource_id] = source
                source.task = asyncio.create_task(self._serve(source, config))
        self.revision += 1

    async def _serve(self, state, config):
        """Open and close clients in the same Task; drain already borrowed calls."""
        from autoagent.agent_app import BUILTIN_AGENT_WORKFLOW_ID
        from autoagent.agent import SkillDirectorySource, MCPClient, MCPToolRegistry, MCPResourceSource
        try:
            async with AsyncExitStack() as stack:
                if config.kind == "skills":
                    source = SkillDirectorySource(state.registry, [config.path], name="skills",
                        include=config.include, interval_seconds=config.interval_seconds,
                        workflows={BUILTIN_AGENT_WORKFLOW_ID})
                else:
                    transport = config.resolve_transport(self.service.host.applications["agent"].credentials)
                    client = await stack.enter_async_context(MCPClient(transport, reconnect_policy=config.reconnect))
                    tools = MCPToolRegistry(client, prefix=config.prefix)
                    source = MCPResourceSource(state.registry, tools, name="mcp", workflows={BUILTIN_AGENT_WORKFLOW_ID},
                        reconnect_policy=config.reconnect)
                state.source = await stack.enter_async_context(source)
                state.state = "ready"
                await state.stop.wait()
                state.state = "draining"
                await state.drained.wait()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            state.state = "error"
            # Transport exceptions can contain credentials or whole responses.
            state.error = type(error).__name__ + ": resource unavailable; check its configuration and retry."
        finally:
            state.registry = AgentResourceRegistry(namespace=state.registry.namespace)

    async def save(self, configuration, *, resource_id=None, expected_version=0):
        if not self.enabled:
            from autoagent.host.models import HostNotFoundError
            raise HostNotFoundError("The Agent application is disabled for this project.")
        if isinstance(configuration, Mapping):
            configuration = CONFIG.validate_json(json.dumps(plain(configuration)))
        if type(expected_version) is not int or expected_version < 0:
            raise ValueError("expected_version must be a nonnegative integer.")
        if resource_id is not None and (not isinstance(resource_id, str) or not resource_id.strip()):
            raise ValueError("Resource ID must be nonempty.")
        resource_id = resource_id or uuid4().hex
        async def update():
            async with self.lock:
                config = configuration
                if config is not None:
                    data = config.model_dump(mode="json")
                    if config.kind == "mcp" and config.transport.type == "stdio":
                        directory = Path(config.transport.cwd).expanduser() if config.transport.cwd else self.service.host.root
                        if not directory.is_absolute():
                            directory = self.service.host.root / directory
                        data["transport"]["cwd"] = str(directory.resolve())
                    previous = self.records.get(resource_id)
                    config = CONFIG.validate_json(json.dumps(data))
                    if config.kind == "resource":
                        target = self._known_resources.get(config.resource_id)
                        if target is None:
                            registries = [self.service.resources,
                                *self.service._build_resources.values(), *self.service._workspaces._catalogs.values(),
                                *(source.registry for source in self.sources.values())]
                            target = next((entry for registry in registries for kind in ("tool", "skill")
                                for entry in registry.entries(kind, include_disabled=True) if entry.resource_id == config.resource_id), None)
                        if target is not None:
                            config = config.model_copy(update={"resource_id": self.resource_identity(target)["management_id"]})
                        if target is not None and hasattr(target.value, "metadata") and not hasattr(target.value, "definition"):
                            if config.permissions is not None:
                                raise ValueError("Skills support only Enable/Disable.")
                            from .skills import set_enabled
                            try:
                                await asyncio.to_thread(set_enabled, target.value, config.enabled, roots=self.skill_roots())
                            except OSError as error:
                                raise ValueError("Cannot update this Skill document; check its file permissions and refresh.") from error
                            await self._refresh_registered_skill(target)
                            for source in self.sources.values():
                                if source.source is not None and hasattr(source.source, "roots"):
                                    await source.source.refresh()
                            if getattr(self, "environment_skills", None) is not None:
                                await self.environment_skills.refresh()
                            self.revision += 1
                            await self.service.approvals.changed()
                            return dict(id=config.resource_id, version=0, configuration=config.model_dump(mode="json"), state="ready", error=None)
                        for key, (_, other) in self.records.items():
                            if key != resource_id and other.kind == "resource" and other.resource_id == config.resource_id:
                                raise ValueError("A resource already has a shared configuration; update that entry.")
                    if config.kind == "group" and any(key != resource_id and other.kind == "group"
                            and other.group_id == config.group_id for key, (_, other) in self.records.items()):
                        raise ValueError("A group already has a configuration; update that entry.")
                    target_id = config.resource_id if config.kind == "resource" else config.group_id if config.kind == "group" else ""
                    locked = target_id.startswith("builtin/") or (
                        target_id.startswith("project/") and not self.service.host.applications["agent"].edit_code_permissions)
                    if locked:
                        if config.permissions is not None and (previous is None or config.permissions != previous[1].permissions):
                            raise ValueError("Permission editing for this resource is disabled.")
                        # Locked permissions stay inactive; saving Enable also
                        # clears permission overrides hidden by the edit policy.
                        config = config.model_copy(update={"permissions": None})
                    data = config.model_dump(mode="json")
                else:
                    data = None
                row = await self.service.storage("save_resource", resource_id, data, expected_version)
                self._publish(resource_id, row)
            await self.service.approvals.changed()
            return self._describe(resource_id) if row is not None else None
        return await self.service.host._perform(lambda: self.service.approvals._run(update), control=True)

    async def _refresh_registered_skill(self, target):
        """Republish explicitly registered local snapshots after a document edit."""
        from autoagent.agent import Skill
        from .skills import location
        path = location(target.value)
        registries = {self.service.resources, *self.service._build_resources.values()}
        for registry in registries:
            for entry in registry.entries("skill", include_disabled=True):
                if (entry.resource_id == f"{registry.namespace}/skill/{entry.name}"
                        and location(entry.value) == path):
                    skill = await asyncio.to_thread(lambda: Skill(path.parent, limits=entry.value.limits).freeze())
                    registry.register_skill(skill, version=uuid4().hex, replace=True,
                        workflows=entry.workflows, policy=entry.policy)

    def _describe(self, resource_id):
        row, config = self.records[resource_id]
        data = config.model_dump(mode="json")
        credentials = []
        if config.kind == "mcp":
            credentials = [name + "." + key for name in ("environment", "headers") for key in data.get(name, {})]
        source = self.sources.get(resource_id)
        source_error = source.error if source is not None else None
        if source is not None and source.source is not None and source.source.last_error is not None:
            source_error = "Resource synchronization failed; check its configuration and retry."
        return dict(id=resource_id, version=row["version"], configuration=data, credential_fields=credentials,
            state="unavailable" if not self.enabled else "disabled" if not config.enabled else "error" if source_error else source.state if source else "ready",
            error=source_error)

    async def describe(self, workspace_id=None):
        async def read():
            from autoagent.agent_app import BUILTIN_AGENT_WORKFLOW_ID
            from autoagent.agent import WorkspaceBinding
            from autoagent.agent.workflow.questions import ASK_USER_RESOURCE
            from autoagent.agent.workflow.builtins import _resources
            from types import SimpleNamespace
            runtime = self.service.host._runtime
            revision = runtime.workflow_ids.get(BUILTIN_AGENT_WORKFLOW_ID)
            registries = [self.service.resources]
            if revision in self.service._build_resources:
                registries.append(self.service._build_resources[revision])
            registries.extend(source.registry for source in self.sources.values())
            builtins = []
            if revision is not None:
                snapshot = SimpleNamespace(bindings={"workflow_revision": revision}, settings=self.service.defaults.model_copy(
                    update={"workspace": WorkspaceBinding(workspace_id=workspace_id or self.service.project_workspace_id)}))
                await self.service.prepare_workspace(snapshot)
                workspace = self.service._workspaces._catalogs.get(self.service._workspaces.key(snapshot))
                if workspace is not None:
                    registries.append(workspace)
                catalog, _ = self.service.catalog(snapshot)
                builtins.extend(catalog.values())
                options = self.service.workflow_options(snapshot)
                if options.include_ask_user:
                    builtins.append(ASK_USER_RESOURCE)
                if options.subagents.enabled:
                    builtins.extend(_resources(options.subagents.model_dump_json()).values())
            inventory = []
            seen = set()
            for registry in registries:
                for kind in ("tool", "skill"):
                    for entry in registry.entries(kind, include_disabled=True):
                        if not entry.workflows or BUILTIN_AGENT_WORKFLOW_ID in entry.workflows:
                            identity = self.resource_identity(entry, kind)
                            if identity["management_id"] not in seen:
                                seen.add(identity["management_id"])
                                inventory.append(self._resource_description(entry, kind,
                                    available=registry.has(kind, entry.name)))
            for entry in builtins:
                identity = self.resource_identity(entry, "tool")
                if identity["management_id"] not in seen:
                    seen.add(identity["management_id"])
                    inventory.append(self._resource_description(entry, "tool"))
            groups = {}
            for entry in inventory:
                group = groups.setdefault(entry["group_id"], dict(id=entry["group_id"], name=entry["group_name"],
                    origin=entry["origin"], resources=[]))
                group["resources"].append(entry)
            for group in groups.values():
                control = next((config for _, config in self.records.values() if config.kind == 'group' and config.group_id == group['id']), None)
                editable = self._permissions_editable(group['origin'])
                group.update(enabled=control.enabled if control else True,
                    permissions=control.permissions.model_dump(mode='json') if editable and control and control.permissions else None,
                    permissions_editable=editable)
            return dict(items=[self._describe(key) for key in sorted(self.records)],
                resources=inventory, groups=list(groups.values()), workspace_id=workspace_id or self.service.project_workspace_id,
                revision=self.revision)
        return await self.service.host._perform(lambda: self.service.approvals._run(read))

    def _resource_description(self, entry, kind, *, available=True):
        identity = self.resource_identity(entry, kind)
        self._known_resources[identity["management_id"]] = entry
        self._known_resources[entry.resource_id] = entry
        selected = next(((row, cfg) for row, cfg in self.records.values() if cfg.kind == "resource"
            and cfg.resource_id in {entry.resource_id, identity["management_id"]}), None)
        group = next(((row, cfg) for row, cfg in self.records.values() if cfg.kind == "group"
            and cfg.group_id == identity["group_id"]), None)
        permissions = self.effective_permissions(entry)
        editable = self._permissions_editable(identity["origin"])
        access = self.service.defaults.access
        registered_default = access.rules.get(entry.name, access.default)
        registered_policies = [entry.policy] if entry.policy is not None else []
        value = entry.value
        while getattr(value, '_owner', None) is not None:
            owner = value._owner
            if owner.policy is not None:
                registered_policies.append(owner.policy)
            value = owner._tools[entry.name]
        from autoagent.agent.tools import ToolPolicy
        rank = {'allow':0, 'ask':1, 'deny':2}
        registered_default = max((registered_default, *(policy.default for policy in registered_policies
            if isinstance(policy, ToolPolicy))), key=rank.__getitem__)
        source_enabled = True
        if identity["origin"] == "mcp":
            config = self.records[identity["source_id"]][1]
            remote = getattr(self._executable(entry.value), "remote_name", entry.name)
            source_enabled = (config.include is None or remote in config.include) and remote not in config.exclude
        result = dict(**identity, resource_id=entry.resource_id, name=entry.name, kind=kind, version=entry.version,
            available=available, enabled=available and source_enabled and bool(self.filter([entry])),
            permissions=permissions.model_dump(mode="json") if permissions is not None else None,
            default_decision=permissions.default if permissions is not None else registered_default,
            parameter_policy=bool(permissions.rules) if permissions is not None else bool(registered_policies or access.policy),
            permissions_editable=editable,
            permission_source=("tool" if selected and selected[1].permissions is not None else "group" if group and group[1].permissions is not None else
                "source" if identity.get("source_id") in self.records else "registration"))
        if kind == "skill":
            from .skills import enabled, editable
            result["enabled"] = available and enabled(entry.value)
            result["enabled_editable"] = editable(entry.value, self.skill_roots())
            result["permissions_editable"] = False
        if kind == "tool":
            result.update(description=entry.value.definition.description, input_schema=entry.value.definition.parameters)
            result["operation_schema"] = getattr(self._executable(entry.value), "operation_schema", None)
            result['approval_exempt'] = entry.resource_id.startswith('builtin:')
            if result['approval_exempt']:
                result.update(default_decision='allow', permissions_editable=False, permission_source='system')
            if identity['origin'] == 'mcp':
                result['remote_name'] = getattr(self._executable(entry.value), 'remote_name', entry.name)
        else:
            source = entry.value.source
            result.update(description=entry.value.metadata.description,
                location=getattr(getattr(source, "snapshot", None), "source_location", None)
                    or getattr(source, "location", None))
        return result

    async def skill_document(self, management_id, workspace_id=None):
        await self.describe(workspace_id)
        async def read():
            registries = [self.service.resources,
                *self.service._build_resources.values(), *self.service._workspaces._catalogs.values(),
                *(source.registry for source in self.sources.values())]
            for registry in registries:
                for entry in registry.entries("skill", include_disabled=True):
                    from .workflow import BUILTIN_AGENT_WORKFLOW_ID
                    if entry.workflows and BUILTIN_AGENT_WORKFLOW_ID not in entry.workflows:
                        continue
                    if self.resource_identity(entry, "skill")["management_id"] == management_id:
                        document = await asyncio.to_thread(entry.value.load)
                        return dict(management_id=management_id, name=entry.name, content=document.instructions)
            from autoagent.host.models import HostNotFoundError
            raise HostNotFoundError("Skill is unavailable in the Agent application.")
        return await self.service.approvals._run(read)

    def catalog(self, reserved=((), ())):
        from autoagent.agent_app import BUILTIN_AGENT_WORKFLOW_ID
        result = [[], []]
        names = [set(entry.name for entry in entries) for entries in reserved]
        for resource_id, source in tuple(self.sources.items()):
            if source.stop.is_set() or source.state != "ready":
                continue
            published = source.registry.catalog(BUILTIN_AGENT_WORKFLOW_ID)
            config = self.records[resource_id][1]
            if config.kind == "mcp":
                def selected(entry):
                    name = self._executable(entry.value)
                    name = getattr(name, "remote_name", entry.name)
                    return (config.include is None or name in config.include) and name not in config.exclude
                published = (tuple(entry for entry in published[0] if selected(entry)), published[1])
            if any(names[index].intersection(entry.name for entry in entries) for index, entries in enumerate(published)):
                source.error = "Resource names conflict with another source; configure a Tool prefix or distinct Skill names."
                continue
            source.error = None
            for index, entries in enumerate(published):
                result[index].extend(self._wrap(entry, self.records[resource_id][1].permissions,
                    self.records[resource_id][0]["version"]) for entry in entries)
                names[index].update(entry.name for entry in entries)
        return tuple(tuple(items) for items in result)

    def filter(self, entries):
        controls = {config.resource_id: (row, config) for row, config in tuple(self.records.values()) if config.kind == "resource"}
        groups = {config.group_id: (row, config) for row, config in tuple(self.records.values()) if config.kind == "group"}
        result = []
        for entry in entries:
            if hasattr(entry.value, "metadata") and not hasattr(entry.value, "definition"):
                from .skills import enabled
                if enabled(entry.value):
                    result.append(entry)
                continue
            identity = self.resource_identity(entry)
            selected = controls.get(identity["management_id"]) or controls.get(entry.resource_id)
            group = groups.get(identity["group_id"])
            if group is not None and not group[1].enabled or selected is not None and not selected[1].enabled:
                continue
            permission = selected if selected is not None and selected[1].permissions is not None else group
            if not self._permissions_editable(identity["origin"]):
                permission = None
            result.append(self._wrap(entry, permission[1].permissions, permission[0]["version"])
                if permission is not None and permission[1].permissions is not None else entry)
        return tuple(result)

    @staticmethod
    def _executable(tool):
        while getattr(tool, "_owner", None) is not None:
            tool = tool._owner._tools[tool.definition.name]
        return tool

    def resource_identity(self, entry, kind=None):
        """Management keys exclude implementation versions and Workflow Revisions."""
        kind = kind or ("skill" if hasattr(entry.value, "metadata") and not hasattr(entry.value, "definition") else "tool")
        name = entry.name
        if kind == "skill" and "/source/environment-skills/" in entry.resource_id:
            return dict(management_id=f"shared/skill/{name}", group_id="shared/skills",
                group_name="Shared directory", origin="shared")
        for key, source in tuple(self.sources.items()):
            if entry.resource_id.startswith(source.registry.namespace + "/"):
                config = self.records[key][1]
                if config.kind == "mcp":
                    remote = getattr(self._executable(entry.value), "remote_name", name)
                    return dict(management_id=f"mcp/{key}/tool/{remote}", group_id=f"mcp/{key}",
                        group_name=config.name, origin="mcp", source_id=key)
                location = getattr(getattr(entry.value, "source", None), "location", None)
                try:
                    relative = Path(location).relative_to(config.path).as_posix() if location else name
                except ValueError:
                    relative = name
                return dict(management_id=f"skill-directory/{key}/{relative}", group_id=f"skill-directory/{key}",
                    group_name=config.name, origin="manual", source_id=key)
        for key, registry in self.service._workspaces._catalogs.items():
            if kind == "skill" and entry.resource_id.startswith(registry.namespace + "/"):
                workspace_id = key[0]
                workspace = next((item for item in self.service.workspaces.entries() if item.info.id == workspace_id), None)
                location = getattr(getattr(entry.value, "source", None), "location", None)
                try:
                    relative = Path(location).relative_to(workspace.root).as_posix() if location and hasattr(workspace, "root") else name
                except ValueError:
                    relative = name
                return dict(management_id=f"workspace/{workspace_id}/skill/{relative}", group_id=f"workspace/{workspace_id}",
                    group_name=workspace.info.name if workspace else workspace_id, origin="workspace", workspace_id=workspace_id)
        group_owner = getattr(self._executable(entry.value), "group", None) if kind == "tool" else None
        if group_owner is not None:
            group = group_owner.group_id
            return dict(management_id=f"builtin/tool/{name}", group_id=f"builtin/{group}",
                group_name={"files":"Files", "commands":"Commands"}.get(group, group), origin="builtin")
        if entry.resource_id.startswith(("builtin:", "skills:")):
            if kind == "skill":
                group, label = "builtin/skills", "Built-in Skills"
            elif name.startswith("skill_"):
                group, label = "builtin/skill-tools", "Skill Tools"
            elif name in {"execute_command", "manage_process"}:
                group, label = "builtin/commands", "Commands"
            elif name == "ask_user":
                group, label = "builtin/interaction", "User Interaction"
            elif entry.resource_id.startswith("builtin:"):
                group, label = "builtin/collaboration", "Agents"
            else:
                group, label = "builtin/files", "Files"
            return dict(management_id=f"builtin/{kind}/{name}", group_id=group, group_name=label, origin="builtin")
        project = self.service.project_workspace_id
        return dict(management_id=f"project/{project}/{kind}/{name}", group_id=f"project/{project}",
            group_name="Project Tools" if kind == "tool" else "Project Skills", origin="project")

    def _wrap(self, entry, permissions, version):
        return replace(entry, policy=permissions, permission_revision=entry.permission_revision + version)

    def effective_permissions(self, entry):
        identity = self.resource_identity(entry)
        if not self._permissions_editable(identity["origin"]):
            return None
        tool = next((config.permissions for _, config in self.records.values() if config.kind == "resource"
            and config.resource_id in {identity["management_id"], entry.resource_id} and config.permissions is not None), None)
        if tool is not None:
            return tool
        group = next((config.permissions for _, config in self.records.values() if config.kind == "group"
            and config.group_id == identity["group_id"] and config.permissions is not None), None)
        if group is not None:
            return group
        source_id = identity.get("source_id")
        if source_id in self.records:
            return self.records[source_id][1].permissions
        from autoagent.agent.tools import ToolPolicy
        if entry.policy is not None:
            return entry.policy if isinstance(entry.policy, ToolPolicy) else None
        value = entry.value
        while getattr(value, "_owner", None) is not None:
            owner = value._owner
            if owner.policy is not None:
                return owner.policy if isinstance(owner.policy, ToolPolicy) else None
            value = owner._tools[entry.name]
        return None

    @asynccontextmanager
    async def borrow(self, resource_id):
        state = next((source for source in self.sources.values()
            if resource_id.startswith(source.registry.namespace + "/")), None)
        if state is not None:
            state.active += 1
            state.drained.clear()
        try:
            yield
        finally:
            if state is not None:
                state.active -= 1
                if not state.active:
                    state.drained.set()

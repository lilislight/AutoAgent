"""Persisted product resources, owned and synchronized on the Runtime loop."""

import asyncio
from collections.abc import Mapping
import json
import logging
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from uuid import uuid4

from pydantic import TypeAdapter

from autoagent.agent.context import plain
from .resource_config import AgentResourceConfig
from .mcp_catalog import observed_catalog, schema_version
from autoagent.agent.resources import AgentResourceRegistry


CONFIG = TypeAdapter(AgentResourceConfig)
logger = logging.getLogger(__name__)


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
    catalog_revision: int = -1

    def __post_init__(self):
        self.drained.set()


class ManagedAgentResources:
    """Product resource configuration; SDK Workflows retain their own registries."""

    def __init__(self, service):
        self.service = service
        self.records = {}
        self.sources = {}
        self.mcp_catalogs = {}
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
        return origin not in {"builtin", "project"} or self.service.host.applications["agent"].edit_code_permissions

    @asynccontextmanager
    async def lifespan(self):
        self.enabled = self.service.host.applications["agent"].enabled
        rows = await self.service.storage("resource_records")
        self.mcp_catalogs = await self.service.storage("mcp_catalogs")
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
            self.mcp_catalogs.clear()
            if self.environment_skills is not None:
                await self.environment_skills.__aexit__(None, None, None)

    async def _watch(self):
        previous = None
        while True:
            async with self.lock:
                for key, source in tuple(self.sources.items()):
                    if self.records[key][1].kind == "mcp" and self._source_ready(source):
                        try:
                            await self._sync_mcp_catalog(key, source)
                        except Exception:
                            logger.exception("Could not store the MCP tool catalog for %s", key)
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
                and old.state != "error" and getattr(old.source, "last_error", None) is None
                and config.model_dump(exclude={"name", "permissions"}) ==
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
            self.mcp_catalogs.pop(resource_id, None)
        else:
            self.records[resource_id] = (row, config)
            if self.enabled and config.enabled and config.kind in {"mcp", "skills"}:
                source = _Source(row, AgentResourceRegistry(namespace="managed:" + resource_id))
                self.sources[resource_id] = source
                source.task = asyncio.create_task(self._serve(source, config))
        self.revision += 1

    @staticmethod
    def _source_ready(source):
        return source is not None and source.state == "ready" and not source.stop.is_set() and (
            getattr(source.source, "last_error", None) is None)

    async def _sync_mcp_catalog(self, connection_id, source):
        revision = source.registry.revision
        if revision == source.catalog_revision:
            return
        entries = source.registry.entries("tool")
        tools = {self._executable(entry.value).remote_name: entry.value.definition for entry in entries}
        old = self.mcp_catalogs.get(connection_id, {})
        catalog = observed_catalog(old, tools)
        if catalog != old:
            await self.service.storage("save_mcp_catalog", connection_id, catalog)
            self.mcp_catalogs[connection_id] = catalog
            self.revision += 1
        source.catalog_revision = revision

    def _mcp_tool_state(self, entry, identity):
        cached = self.mcp_catalogs.get(identity["source_id"], {}).get(identity["remote_name"])
        source = self.sources.get(identity["source_id"])
        if not self._source_ready(source):
            return "offline"
        if cached is None:
            return "connecting"
        if schema_version(entry.value.definition.parameters) != cached["approved_version"]:
            return "review"
        return "ready"

    async def accept_mcp_schema(self, connection_id, remote_name, version):
        async def accept():
            async with self.lock:
                source = self.sources.get(connection_id)
                if not self._source_ready(source):
                    raise self.service.services.conflict("mcp_unavailable", "The MCP server is offline.")
                await self._sync_mcp_catalog(connection_id, source)
                catalog = self.mcp_catalogs.get(connection_id, {})
                tool = catalog.get(remote_name)
                if tool is None or not tool["listed"] or tool["schema_version"] != version:
                    raise self.service.services.conflict("mcp_schema_changed", "The tool schema changed. Refresh before reviewing again.")
                updated = {**catalog, remote_name: {**tool, "approved_schema": tool["input_schema"],
                    "approved_version": version}}
                await self.service.storage("save_mcp_catalog", connection_id, updated)
                self.mcp_catalogs[connection_id] = updated
                self.revision += 1
            await self.service.approvals.changed()
        await self.service.host._perform(lambda: self.service.approvals._run(accept), control=True)

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
                    locked = (target_id.startswith("builtin/") and config.kind == "group"
                        or target_id.startswith(("builtin/", "project/"))
                        and not self.service.host.applications["agent"].edit_code_permissions)
                    if locked:
                        if config.permissions is not None and (previous is None or config.permissions != previous[1].permissions):
                            raise ValueError("Permission editing for this resource is disabled.")
                        config = config.model_copy(update={"permissions": previous[1].permissions if previous else None})
                    elif config.kind == "resource" and config.permissions is not None:
                        policy = config.permissions
                        policy = policy.model_copy(update={"rules": tuple(rule if rule.id else
                            rule.model_copy(update={"id": uuid4().hex}) for rule in policy.rules)})
                        config = config.model_copy(update={"permissions": policy})
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

    async def describe(self, workspace_id=None, *, tool_id=None, scope=None):
        async def read():
            from autoagent.agent_app import BUILTIN_AGENT_WORKFLOW_ID
            from autoagent.agent import WorkspaceBinding
            from autoagent.agent.workflow.questions import ASK_USER_RESOURCE
            from autoagent.agent.workflow.builtins import _resources
            from types import SimpleNamespace
            runtime = self.service.host._runtime
            revision = runtime.workflow_ids.get(BUILTIN_AGENT_WORKFLOW_ID)
            registries = [] if scope == "mcp" else [self.service.resources]
            if scope != "mcp" and revision in self.service._build_resources:
                registries.append(self.service._build_resources[revision])
            registries.extend(source.registry for key, source in self.sources.items()
                if scope is None or (self.records[key][1].kind == "mcp") == (scope == "mcp"))
            builtins = []
            if revision is not None and scope != "mcp":
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
                            if scope == "tools" and (kind != "tool" or identity["origin"] == "mcp"):
                                continue
                            if scope == "mcp" and identity["origin"] != "mcp":
                                continue
                            if tool_id is not None:
                                if kind == "tool" and identity["management_id"] == tool_id:
                                    return entry
                                continue
                            if identity["management_id"] not in seen:
                                seen.add(identity["management_id"])
                                inventory.append(self._resource_description(entry, kind,
                                    available=registry.has(kind, entry.name)))
            for entry in builtins:
                identity = self.resource_identity(entry, "tool")
                if scope == "mcp" or scope == "tools" and identity["origin"] == "mcp":
                    continue
                if tool_id is not None:
                    if identity["management_id"] == tool_id:
                        return entry
                    continue
                if identity["management_id"] not in seen:
                    seen.add(identity["management_id"])
                    inventory.append(self._resource_description(entry, "tool"))
            if tool_id is not None:
                from autoagent.host.models import HostNotFoundError
                raise HostNotFoundError("Tool is unavailable in the Agent application.")
            if scope != "tools":
                for connection_id, (_, config) in self.records.items():
                    if config.kind != "mcp":
                        continue
                    for remote_name, cached in self.mcp_catalogs.get(connection_id, {}).items():
                        identity = self._mcp_identity(connection_id, remote_name)
                        if identity["management_id"] not in seen:
                            inventory.append(self._offline_mcp_resource(identity, cached, config))
            groups = {}
            for entry in inventory:
                group = groups.setdefault(entry["group_id"], dict(id=entry["group_id"], name=entry["group_name"],
                    origin=entry["origin"], resources=[]))
                group["resources"].append(entry)
            for group in groups.values():
                control = next((config for _, config in self.records.values() if config.kind == 'group' and config.group_id == group['id']), None)
                editable = group['origin'] != "builtin" and self._permissions_editable(group['origin'])
                group.update(enabled=control.enabled if control else True,
                    permissions=control.permissions.model_dump(mode='json') if editable and control and control.permissions else None,
                    permissions_editable=editable)
            return dict(items=[self._describe(key) for key in sorted(self.records)
                    if scope != "tools" or self.records[key][1].kind != "mcp"],
                resources=inventory, groups=list(groups.values()), workspace_id=workspace_id or self.service.project_workspace_id,
                revision=self.revision)
        return await self.service.host._perform(lambda: self.service.approvals._run(read))

    def _resource_description(self, entry, kind, *, available=True):
        identity = self.resource_identity(entry, kind)
        self._known_resources[identity["management_id"]] = entry
        self._known_resources[entry.resource_id] = entry
        registered, callback = self.registered_policies(entry)
        source_enabled = True
        mcp = {}
        if identity["origin"] == "mcp":
            config = self.records[identity["source_id"]][1]
            remote = identity["remote_name"]
            source_enabled = (config.include is None or remote in config.include) and remote not in config.exclude
            status = self._mcp_tool_state(entry, identity)
            available = available and status == "ready"
            cached = self.mcp_catalogs.get(identity["source_id"], {}).get(remote)
            mcp = dict(mcp_status=status, schema_version=schema_version(entry.value.definition.parameters),
                approved_schema=cached["approved_schema"] if status == "review" and cached else None)
        result = dict(**identity, resource_id=entry.resource_id, name=entry.name, kind=kind, version=entry.version,
            available=available, enabled=available and source_enabled and bool(self.filter([entry])),
            **self._policy_details(identity, entry.resource_id, registered=registered, callback=callback), **mcp)
        if kind == "skill":
            from .skills import enabled, editable
            result["enabled"] = available and enabled(entry.value)
            result["enabled_editable"] = editable(entry.value, self.skill_roots())
            result["permissions_editable"] = False
        if kind == "tool":
            result.update(description=entry.value.definition.description, input_schema=entry.value.definition.parameters)
            result["operation_schema"] = getattr(self._executable(entry.value), "operation_schema", None)
            if identity['origin'] == 'mcp':
                result['remote_name'] = getattr(self._executable(entry.value), 'remote_name', entry.name)
        else:
            source = entry.value.source
            result.update(description=entry.value.metadata.description,
                location=getattr(getattr(source, "snapshot", None), "source_location", None)
                    or getattr(source, "location", None))
        return result

    def _offline_mcp_resource(self, identity, cached, config):
        ready = self._source_ready(self.sources.get(identity["source_id"]))
        status = "removed" if ready and not cached["listed"] else "offline"
        return dict(**identity, resource_id=identity["management_id"],
            name=config.prefix + identity["remote_name"], kind="tool", available=False, enabled=False,
            version=cached["schema_version"], description=cached["description"], input_schema=cached["input_schema"],
            mcp_status=status, schema_version=cached["schema_version"],
            **self._policy_details(identity, identity["management_id"]))

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
            if not self._source_ready(source):
                continue
            published = source.registry.catalog(BUILTIN_AGENT_WORKFLOW_ID)
            config = self.records[resource_id][1]
            if config.kind == "mcp":
                def selected(entry):
                    identity = self.resource_identity(entry)
                    name = identity["remote_name"]
                    return ((config.include is None or name in config.include) and name not in config.exclude
                        and self._mcp_tool_state(entry, identity) == "ready")
                published = (tuple(entry for entry in published[0] if selected(entry)), published[1])
            if any(names[index].intersection(entry.name for entry in entries) for index, entries in enumerate(published)):
                source.error = "Resource names conflict with another source; configure a Tool prefix or distinct Skill names."
                continue
            source.error = None
            for index, entries in enumerate(published):
                result[index].extend(entries)
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
            source = self.records.get(identity.get("source_id"))
            versions = sum(row["version"] for row, config in
                (value for value in (selected, group, source) if value is not None))
            result.append(replace(entry, permission_revision=entry.permission_revision + versions) if versions else entry)
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
                    return self._mcp_identity(key, remote)
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

    def _mcp_identity(self, connection_id, remote_name):
        config = self.records[connection_id][1]
        return dict(management_id=f"mcp/{connection_id}/tool/{remote_name}", group_id=f"mcp/{connection_id}",
            group_name=config.name, origin="mcp", source_id=connection_id, remote_name=remote_name)

    def configured_policies(self, identity, resource_id):
        """Return persisted policies without replacing the Tool's registration."""
        if not self._permissions_editable(identity["origin"]):
            return ()
        configured = []
        for _, config in self.records.values():
            if config.permissions is None:
                continue
            if config.kind == "resource" and config.resource_id in {identity["management_id"], resource_id}:
                configured.append(("tool", config.permissions))
            if config.kind == "group" and identity["origin"] != "builtin" and config.group_id == identity["group_id"]:
                configured.append(("group", config.permissions))
        source = self.records.get(identity.get("source_id"))
        if source is not None and source[1].kind == "mcp" and source[1].permissions is not None:
            configured.append(("source", source[1].permissions))
        return tuple(configured)

    def registered_policies(self, entry):
        from autoagent.agent.files import FileToolsConfig
        from autoagent.agent.tools import ToolPolicy
        from autoagent.agent.tools.operations import ToolOperationPolicy

        layers = []
        callbacks = False
        access = self.service.defaults.access
        action = access.rules.get(entry.name, access.default)
        if action != "allow":
            layers.append(ToolPolicy(default=action))
        if access.policy is not None:
            registry = self.service._build_resources.get(self.service.host._runtime.workflow_ids.get("autoagent.agent"))
            binding = registry.get("policy", access.policy) if registry is not None and registry.has("policy", access.policy) else None
            if binding is not None and isinstance(binding.value, ToolPolicy):
                layers.append(binding.value)
            else:
                callbacks = True
        handlers = [entry.policy]
        value = entry.value
        while getattr(value, "_owner", None) is not None:
            owner = value._owner
            value = owner._tools[entry.name]
            handler = owner.policy
            if isinstance(handler, ToolOperationPolicy):
                config = getattr(getattr(value, "service", None), "config", None)
                if isinstance(config, FileToolsConfig):
                    handler = config.policy
            handlers.append(handler)
        for handler in handlers:
            if isinstance(handler, ToolPolicy):
                layers.append(handler)
            elif handler is not None:
                callbacks = True
        return tuple(layers), callbacks

    def _policy_details(self, identity, resource_id, *, registered=(), callback=False):
        configured = self.configured_policies(identity, resource_id)
        custom = next((policy for source, policy in configured if source == "tool"), None)
        inherited = tuple((source, policy) for source, policy in configured if source != "tool")
        readonly = (*(("code", policy) for policy in registered), *inherited)
        return dict(custom_policy=custom.to_dict() if custom is not None else None,
            registered_policies=[{"source": source, "policy": policy.to_dict()} for source, policy in readonly],
            policy_in_code=callback,
            permissions_editable=self._permissions_editable(identity["origin"]))

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

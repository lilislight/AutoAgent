"""Workflow-owned declarations; live resource instances never enter Workflow IR."""

from dataclasses import dataclass
import hashlib
import inspect
import json
import marshal
from pathlib import Path
from types import MappingProxyType

from ..tools import ExecutableTool, Tool, ToolSet
from ..skills import Skill, SkillSet


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def callable_identity(value):
    """Version code without serializing borrowed runtime dependencies."""
    if value is None:
        return None
    target = value if inspect.isfunction(value) or inspect.ismethod(value) else type(value).__call__
    target = inspect.unwrap(target)
    code = getattr(target, "__code__", None)
    return dict(name=getattr(target, "__module__", "") + "." + getattr(target, "__qualname__", type(value).__qualname__),
        code=hashlib.sha256(marshal.dumps(code)).hexdigest() if code else None,
        version=getattr(value, "version", None))


def tool_identity(tool):
    owner = getattr(tool, "_owner", None)
    if owner is not None:
        return dict(bound=tool_identity(owner._tools[tool.definition.name]), policy=callable_identity(owner.policy),
            hooks=[callable_identity(getattr(owner.hooks, name)) for name in ("before", "after", "on_error")],
            allowed=sorted(owner.context.allowed_tools) if owner.context and owner.context.allowed_tools is not None else None,
            approval_mode=owner.approval_mode)
    return dict(definition=tool.definition.model_dump(mode="json"),
        implementation=callable_identity(getattr(tool, "_handler", tool.ainvoke)))


def resource_permissions(entry):
    return dict(resource_id=entry.resource_id, revision=entry.permission_revision,
        policy=callable_identity(entry.policy), workflows=sorted(entry.workflows),
        dependencies=[resource_permissions(value) for value in entry.dependencies])


@dataclass(frozen=True, slots=True)
class MCPSource:
    """Create and own one MCP client on the Runtime loop."""

    config: object = None
    client_factory: object = None
    name: str | None = None
    prefix: str = ""
    names: object = None
    include: tuple[str, ...] | None = None
    exclude: tuple[str, ...] = ()
    reconnect_policy: object = None
    policy: object = None

    def __post_init__(self):
        from ..mcp import MCPStdioConfig, MCPSSEConfig, MCPStreamableHTTPConfig
        if (self.config is None) == (self.client_factory is None):
            raise ValueError("MCPSource requires exactly one of config or client_factory.")
        if self.config is not None and not isinstance(self.config, (MCPStdioConfig, MCPSSEConfig, MCPStreamableHTTPConfig)):
            raise TypeError("MCPSource config must be an MCP transport configuration.")
        if self.config is not None:
            object.__setattr__(self, "config", type(self.config).model_validate(self.config.model_dump(mode="python")))
        if self.client_factory is not None and (not callable(self.client_factory) or not self.name):
            raise ValueError("MCP client factories require a callable and stable source name.")
        for field in ("include", "exclude"):
            value = getattr(self, field)
            if isinstance(value, str):
                raise TypeError("MCP filters require collections of names.")
            if value is not None:
                object.__setattr__(self, field, tuple(value))
        object.__setattr__(self, "names", MappingProxyType(dict(self.names or {})))
        if self.policy is not None and not callable(self.policy):
            raise TypeError("MCP source policy must be callable.")
        if self.reconnect_policy is not None:
            from ..mcp import MCPReconnectPolicy
            object.__setattr__(self, "reconnect_policy", MCPReconnectPolicy.model_validate(self.reconnect_policy))

    def identity(self):
        return dict(name=self.name or self.config.server_id,
            connection=digest(self.config.model_dump(mode="json", exclude={"headers", "env"})) if self.config is not None else callable_identity(self.client_factory),
            prefix=self.prefix, names=dict(self.names), include=self.include, exclude=self.exclude,
            reconnect=self.reconnect_policy.model_dump(mode="json") if self.reconnect_policy is not None else None,
            policy=callable_identity(self.policy))

    async def install(self, stack, registry, workflow_id):
        from ..mcp import MCPClient, MCPToolRegistry
        from ..resource_sources import MCPResourceSource
        client = MCPClient(self.config, reconnect_policy=self.reconnect_policy) if self.config is not None else self.client_factory()
        if inspect.isawaitable(client):
            client = await client
        client = await stack.enter_async_context(client)
        tools = MCPToolRegistry(client, prefix=self.prefix, names=self.names, include=self.include, exclude=self.exclude)
        await stack.enter_async_context(MCPResourceSource(registry, tools, name=self.name,
            reconnect_policy=self.reconnect_policy, workflows={workflow_id}, policy=self.policy))


@dataclass(frozen=True, slots=True)
class SkillDirectory:
    root: Path | str
    include: tuple[str, ...] | None = None
    interval_seconds: float = 1
    limits: object = None
    policy: object = None

    def __post_init__(self):
        import math
        object.__setattr__(self, "root", Path(self.root).resolve())
        if isinstance(self.include, str):
            raise TypeError("Skill filters require a collection of names.")
        if self.include is not None:
            object.__setattr__(self, "include", tuple(self.include))
        if self.policy is not None and not callable(self.policy):
            raise TypeError("Skill policy must be callable.")
        if self.limits is not None:
            from ..skills import SkillLimits
            object.__setattr__(self, "limits", SkillLimits.model_validate(self.limits))
        if isinstance(self.interval_seconds, bool) or not math.isfinite(self.interval_seconds) or self.interval_seconds <= 0:
            raise ValueError("Skill scan interval must be finite and positive.")

    def identity(self):
        return dict(root=str(self.root), include=self.include, interval_seconds=self.interval_seconds,
            limits=self.limits.model_dump(mode="json") if self.limits is not None else None,
            policy=callable_identity(self.policy))


class AgentResources:
    """Resource declarations retained on the Workflow's start Operator."""

    def __init__(self, tools=(), mcp=(), skills=()):
        if isinstance(tools, ToolSet):
            tools = tuple(tools[name] for name in tools._tools)
        self.tools = tuple(value if isinstance(value, ExecutableTool) else Tool(value) for value in tools)
        if len({value.definition.name for value in self.tools}) != len(self.tools):
            raise ValueError("Build Tools require unique names.")
        self.mcp = tuple(mcp)
        self.skills = tuple(skills._skills.values()) if isinstance(skills, SkillSet) else tuple(skills)
        if any(not isinstance(value, MCPSource) for value in self.mcp):
            raise TypeError("mcp accepts MCPSource declarations.")
        source_names = [value.name or value.config.server_id for value in self.mcp]
        if len(source_names) != len(set(source_names)):
            raise ValueError("Build MCP sources require unique names.")
        if any(not isinstance(value, (Skill, SkillDirectory)) for value in self.skills):
            raise TypeError("skills accepts Skills, a SkillSet, or SkillDirectory declarations.")
        self.skills = tuple(value.freeze() if isinstance(value, Skill) else value for value in self.skills)
        self.manifest = dict(tools=[tool_identity(value) for value in self.tools],
            mcp=[value.identity() for value in self.mcp], skills=[value.identity() if isinstance(value, SkillDirectory)
                else dict(name=value.metadata.name, revision=value.freeze().snapshot_revision) for value in self.skills])

    async def install(self, stack, registry, workflow_id):
        from ..resource_sources import SkillDirectorySource
        for tool, identity in zip(self.tools, self.manifest["tools"]):
            registry.register_tool(tool, version=digest(identity), workflows={workflow_id})
        for index, skill in enumerate(self.skills):
            if isinstance(skill, SkillDirectory):
                await stack.enter_async_context(SkillDirectorySource(registry, [skill.root], name=f"skills:{index}",
                    workflows={workflow_id}, include=skill.include, interval_seconds=skill.interval_seconds,
                    limits=skill.limits, policy=skill.policy))
            else:
                registry.register_skill(skill, workflows={workflow_id})
        for source in self.mcp:
            await source.install(stack, registry, workflow_id)

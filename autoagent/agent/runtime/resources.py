"""Unified Tool, MCP and Skill registry used by Agent execution."""

from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass

from ..resources import AgentResourceRegistry
from ..tools import ExecutableTool, Tool, ToolSet
from ..skills import Skill, SkillSet
from ..workflow.resources import MCPSource, SkillDirectory, digest, tool_identity


@dataclass
class _MCPConnection:
    stack: object
    source: object
    active: int = 0
    drained: object = None


class AgentResources(AgentResourceRegistry):
    """One SDK resource registry; callers decide whether and when to update it."""

    def __init__(self, *, tools=(), mcp=(), skills=(), namespace="agent"):
        super().__init__(namespace=namespace)
        if isinstance(tools, ToolSet):
            tools = tuple(tools[name] for name in tools._tools)
        self.tools = tuple(value if isinstance(value, ExecutableTool) else Tool(value) for value in tools)
        self.mcp = tuple(mcp)
        self.skills = tuple(skills._skills.values()) if isinstance(skills, SkillSet) else tuple(skills)
        if any(not isinstance(value, MCPSource) for value in self.mcp):
            raise TypeError("mcp accepts MCPSource declarations.")
        if any(not isinstance(value, (Skill, SkillDirectory)) for value in self.skills):
            raise TypeError("skills accepts Skills, a SkillSet, or SkillDirectory declarations.")
        names = [value.name or value.config.server_id for value in self.mcp]
        if len(names) != len(set(names)):
            raise ValueError("MCP sources require unique names.")
        self._groups = set()
        for item in self.tools:
            self.register_tool(item, version=digest(tool_identity(item)))
        for item in self.skills:
            if isinstance(item, Skill):
                self.register_skill(item)
        self._installed = False
        self._loop = None
        self._mcp_sources = {}
        self._connections = {}
        self._retiring = {}
        self._stack = None

    def snapshot(self, context=None):
        """Read a complete current catalog; custom registries may scope it to a call."""
        return self.catalog()

    def filter_tools(self, tools, *, context=None):
        return tuple(tools)

    @property
    def manifest(self):
        return dict(tools=[tool_identity(entry.value) for entry in self.entries("tool", include_disabled=True)],
            mcp=[value.identity() for value in self.mcp],
            skills=[value.identity() if isinstance(value, SkillDirectory) else
                dict(name=value.metadata.name, revision=value.freeze().snapshot_revision) for value in self.skills])

    @asynccontextmanager
    async def lifespan(self):
        async with AsyncExitStack() as stack:
            await self.install(stack)
            yield self

    async def install(self, stack):
        import asyncio
        if self._installed:
            if self._loop is not asyncio.get_running_loop():
                raise RuntimeError("AgentResources is already active on another runtime loop.")
            return
        self._installed = True
        self._loop = asyncio.get_running_loop()
        self._stack = stack
        async def release():
            self._installed = False
            self._loop = None
            self._mcp_sources.clear()
            self._stack = None
        stack.push_async_callback(release)
        # Initial Skill discovery is fixed unless the owner explicitly refreshes.
        for index, directory in enumerate(self.skills):
            if isinstance(directory, SkillDirectory) and "skills:" + str(index) not in self._source_claims:
                self.claim_source("skills:" + str(index), policy=directory.policy)
        await self.refresh_skills()
        stack.push_async_callback(self._close_connections)
        for source in self.mcp:
            await self.connect_mcp(source)
        stack.push_async_callback(self._close_groups)

    def register_tool(self, tool, **options):
        entry = super().register_tool(tool, **options)
        selected = entry.value
        while getattr(selected, "_owner", None) is not None:
            selected = selected._owner._tools[entry.name]
        group = getattr(selected, "group", None)
        if group is not None:
            self._groups.add(group)
        return entry

    async def _close_groups(self):
        for group in self._groups:
            close = getattr(group, "aclose", None)
            if close is not None:
                await close()

    async def refresh_skills(self):
        import asyncio
        for index, directory in enumerate(self.skills):
            if isinstance(directory, SkillDirectory):
                found = await asyncio.to_thread(SkillSet.discover, [directory.root], limits=directory.limits)
                selected = [found[item.name].freeze() for item in found.metadata
                    if directory.include is None or item.name in directory.include]
                name = "skills:" + str(index)
                self.replace_source(name, {("skill", item.metadata.name): (item.snapshot_revision, item) for item in selected})

    async def connect_mcp(self, source):
        """Connect an MCP source on this registry's owning runtime loop."""
        import asyncio
        if self._stack is None or self._loop is not asyncio.get_running_loop():
            raise RuntimeError("Connect MCP on the active AgentResources runtime loop.")
        if not isinstance(source, MCPSource):
            raise TypeError("Expected MCPSource.")
        name = source.name or source.config.server_id
        if name in self._connections or name in self._retiring:
            raise ValueError("MCP source is already connected.")
        stack = AsyncExitStack()
        try:
            await source.install(stack, self)
            drained = asyncio.Event()
            drained.set()
            self._connections[name] = _MCPConnection(stack, self._mcp_sources[name], drained=drained)
        except BaseException:
            self._mcp_sources.pop(name, None)
            await stack.aclose()
            raise

    async def disconnect_mcp(self, name):
        """Withdraw Tools immediately and close the transport after borrowers finish."""
        import asyncio
        state = self._connections.pop(name)
        self._mcp_sources.pop(name, None)
        async def retire():
            await state.source.__aexit__(None, None, None)
            await state.drained.wait()
            await state.stack.aclose()
        task = asyncio.create_task(retire())
        self._retiring[name] = task
        def finished(task):
            if not task.cancelled() and task.exception() is None:
                self._retiring.pop(name, None)
        task.add_done_callback(finished)
        await asyncio.shield(task)

    async def _close_connections(self):
        import asyncio
        for name in tuple(self._connections):
            await self.disconnect_mcp(name)
        if self._retiring:
            await asyncio.gather(*tuple(self._retiring.values()))

    async def refresh_mcp(self, name):
        await self._mcp_sources[name].refresh()

    @asynccontextmanager
    async def borrow(self, resource_id):
        state = next((state for state in self._connections.values()
            if resource_id.startswith(self.namespace + "/source/" + state.source.name + "/")), None)
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

    def mcp_tools(self, client, *, name=None, reconnect_policy=None, workflows=(), policy=None, **discovery):
        """Enter this source in resource lifespan after entering the MCP client."""
        from autoagent.agent import MCPToolRegistry, MCPResourceSource
        return MCPResourceSource(self, MCPToolRegistry(client, **discovery),
            name=name, reconnect_policy=reconnect_policy, workflows=workflows, policy=policy)

    def skill_directories(self, roots, **options):
        """Enter this source in resource lifespan to synchronize Skill snapshots."""
        from autoagent.agent import SkillDirectorySource
        return SkillDirectorySource(self, roots, **options)

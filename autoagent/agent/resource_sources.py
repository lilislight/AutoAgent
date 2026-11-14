"""Lifespan-owned bridges from changing external catalogs to Agent resources."""

import asyncio
from contextlib import aclosing
import hashlib
import json
import math

from .resources import AgentResourceRegistry


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


class MCPResourceSource:
    """Synchronize discovery, notifications and reconnects without replaying Tools.

    Enter on the MCP client's owning loop, normally the Host lifespan. The
    source owns its listener, not the borrowed client. Terminal listener errors
    withdraw its Tools and are available as last_error.
    """

    def __init__(self, resources: AgentResourceRegistry, registry, *, name=None, reconnect_policy=None,
                 workflows=(), policy=None, watch=True):
        self.resources, self.registry = resources, registry
        self.name = name or "mcp:" + registry.server_id
        self.reconnect_policy = reconnect_policy
        if type(watch) is not bool:
            raise TypeError("watch must be a boolean.")
        self.watch = watch
        from .resources import workflow_scope
        self.workflows, self.policy = workflow_scope(workflows), policy
        self.last_error = None
        self._task = self._unsubscribe = None
        self._entered = False
        self._active = False

    def _publish(self, registry):
        try:
            snapshot = registry.snapshot
            metadata = registry.metadata
            self.resources.replace_source(self.name, {
                ("tool", tool.definition.name): (
                    _digest([registry.generation, metadata[tool.definition.name]]), tool)
                for tool in (snapshot[definition.name] for definition in snapshot.definitions)
            })
            self.last_error = None
        except Exception as error:
            self.last_error = error
            self.resources.remove_source(self.name)
            raise

    async def refresh(self):
        """Explicitly rediscover and atomically publish a complete catalog."""
        if not self._active:
            raise RuntimeError("Enter the MCP resource source before refreshing it.")
        await self.registry.refresh()
        if not self._active:
            return
        if self._task is not None and self._task.done():
            ready = asyncio.get_running_loop().create_future()
            self._task = asyncio.create_task(self._watch(ready))
            await ready

    async def __aenter__(self):
        if self._entered:
            raise RuntimeError("A resource source can be entered only once.")
        self._entered = True
        self.resources.claim_source(self.name, workflows=self.workflows, policy=self.policy)
        self._active = True
        self._unsubscribe = self.registry.subscribe(self._publish)
        ready = asyncio.get_running_loop().create_future()
        try:
            await self.registry.aprepare()
            self._publish(self.registry)
            if self.watch:
                self._task = asyncio.create_task(self._watch(ready))
                await ready
            return self
        except BaseException:
            await self.__aexit__(None, None, None)
            raise

    async def _watch(self, ready):
        try:
            async with aclosing(self.registry.watch(policy=self.reconnect_policy)) as updates:
                async for _ in updates:
                    if not ready.done():
                        ready.set_result(None)
            raise RuntimeError("The MCP catalog listener stopped.")
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.last_error = error
            self.resources.remove_source(self.name)
            if not ready.done():
                ready.set_exception(error)

    async def __aexit__(self, *_):
        if not self._active:
            return
        self._active = False
        if self._unsubscribe is not None:
            self._unsubscribe()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        self.resources.release_source(self.name)


class SkillDirectorySource:
    """Rescan complete Skill snapshots, including nested resources, off-loop.

    Polling needs no optional watcher dependency and covers lost filesystem
    events. Two identical scans debounce automatic publications; refresh() is an
    explicit immediate boundary. A broken scan withdraws this source until it
    becomes valid again, rather than exposing mutable or partially read Skills.
    """

    def __init__(self, resources: AgentResourceRegistry, roots, *, name="skills", interval_seconds=1, limits=None,
                 include=None, workflows=(), policy=None):
        if isinstance(roots, (str, bytes)):
            raise TypeError("Skill directories require a collection of roots.")
        if (isinstance(interval_seconds, bool) or not isinstance(interval_seconds, (int, float))
                or not math.isfinite(interval_seconds) or interval_seconds <= 0):
            raise ValueError("Skill scan interval must be finite and positive.")
        self.resources, self.roots, self.name = resources, tuple(roots), name
        self.interval_seconds, self.limits = interval_seconds, limits
        from .resources import workflow_scope
        self.workflows, self.policy = workflow_scope(workflows), policy
        if isinstance(include, str):
            raise TypeError("Skill includes must be a collection of names.")
        self.include = None if include is None else frozenset(include)
        self.last_error = None
        self._task = None
        self._lock = asyncio.Lock()
        self._candidate = None
        self._entered = False
        self._active = False

    def _scan(self):
        from .skills import SkillSet
        discovered = SkillSet.discover(self.roots, limits=self.limits)
        snapshots = [discovered[item.name].freeze() for item in discovered.metadata
            if self.include is None or item.name in self.include]
        return {("skill", skill.metadata.name): (skill.snapshot_revision, skill) for skill in snapshots}

    async def _refresh(self, *, stable):
        async with self._lock:
            try:
                replacement = await asyncio.to_thread(self._scan)
                if not self._active:
                    return
                identity = tuple(sorted((key, value[0]) for key, value in replacement.items()))
                publish = not stable or identity == self._candidate
                self._candidate = identity
                if publish:
                    self.resources.replace_source(self.name, replacement)
                    self.last_error = None
            except Exception as error:
                self.last_error = error
                self._candidate = None
                self.resources.remove_source(self.name)
                raise

    async def refresh(self):
        """Read complete immutable snapshots and publish without debounce."""
        if not self._active:
            raise RuntimeError("Enter the Skill directory source before refreshing it.")
        await self._refresh(stable=False)

    async def __aenter__(self):
        if self._entered:
            raise RuntimeError("A resource source can be entered only once.")
        self._entered = True
        self.resources.claim_source(self.name, workflows=self.workflows, policy=self.policy)
        self._active = True
        try:
            await self.refresh()
            self._task = asyncio.create_task(self._watch())
            return self
        except BaseException:
            self._active = False
            self.resources.release_source(self.name)
            raise

    async def _watch(self):
        while True:
            await asyncio.sleep(self.interval_seconds)
            try:
                await self._refresh(stable=True)
            except Exception:
                # Retain the diagnostic, retry discovery on the next interval.
                pass

    async def __aexit__(self, *_):
        if not self._active:
            return
        self._active = False
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        self.resources.release_source(self.name)


__all__ = ["MCPResourceSource", "SkillDirectorySource"]

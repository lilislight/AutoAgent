"""Host lifecycle and scoped resource catalogs for optional Agent Workspaces."""

import asyncio
import hashlib
import json
from pathlib import Path
from uuid import uuid4

from autoagent.agent.resources import AgentResourceRegistry
from autoagent.agent.workspace import LocalWorkspace, WorkspaceError, WorkspaceRegistry, PROJECT_WORKSPACE_ID


class AgentWorkspaces:
    def __init__(self, service):
        self.service = service
        self.registry = WorkspaceRegistry()
        self.registry.register(LocalWorkspace(PROJECT_WORKSPACE_ID, service.host.root, service.host.root.name or "Current project"))
        self._catalogs = {}
        self._discoveries = {}
        self._locks = {}
        self._registration_lock = asyncio.Lock()

    @staticmethod
    def configuration(workspace):
        return dict(id=workspace.id, name=workspace.name, root=str(workspace.root))

    async def restore(self):
        storage = self.service.host._persistence
        root = self.service.host.root
        self.registry.register(LocalWorkspace(PROJECT_WORKSPACE_ID, root, root.name or "Current project"), replace=True)
        declared = {value.info.id: value for value in self.registry.entries()}
        for record in await storage.agent_context_operation("workspace_records"):
            if record["id"] not in declared:
                self.registry.register(LocalWorkspace(**record), replace=True)
        for value in declared.values():
            if isinstance(value, LocalWorkspace):
                await storage.agent_context_operation("register_workspace", self.configuration(value))

    def clear(self):
        self._catalogs.clear()
        self._discoveries.clear()
        self._locks.clear()

    async def register(self, workspace, *, replace=False):
        if not isinstance(workspace, LocalWorkspace):
            raise TypeError("Persisted Workspace registration currently accepts LocalWorkspace.")
        async with self._registration_lock:
            await self._publish(workspace, replace=replace)
        return workspace.info

    async def _publish(self, workspace, *, replace=False):
        if workspace.id == PROJECT_WORKSPACE_ID and workspace.root != self.service.host.root:
            raise WorkspaceError("The project Workspace always uses the current project root.")
        if not replace and any(item.info.id == workspace.id for item in self.registry.entries()):
            raise WorkspaceError("Workspace is already registered.")
        await self.service.host._persistence.agent_context_operation("register_workspace", self.configuration(workspace))
        self.registry.register(workspace, replace=replace)

    async def describe(self):
        entries = self.registry.entries()
        def read():
            result = []
            for workspace in entries:
                local = isinstance(workspace, LocalWorkspace)
                result.append({**workspace.info.model_dump(mode="json"),
                    "path": str(workspace.root) if local else None,
                    "is_project": workspace.info.id == PROJECT_WORKSPACE_ID,
                    "available": workspace.root.is_dir() if local else None})
            return sorted(result, key=lambda item: (not item["is_project"], item["name"].casefold(), item["id"]))
        return await asyncio.to_thread(read)

    async def create(self, *, name, path, create_directory=False):
        if not isinstance(name, str) or not name.strip() or len(name.strip()) > 128:
            raise WorkspaceError("Workspace name must contain 1–128 characters.")
        if not isinstance(path, str) or not path.strip() or "\x00" in path:
            raise WorkspaceError("A directory path is required.")
        if type(create_directory) is not bool:
            raise TypeError("create_directory must be a boolean.")
        async with self._registration_lock:
            requested = self.service.host.root / Path(path.strip()).expanduser()
            root = await asyncio.to_thread(requested.resolve)
            for entry in self.registry.entries():
                if isinstance(entry, LocalWorkspace) and await asyncio.to_thread(entry.root.resolve) == root:
                    raise WorkspaceError("This directory is already registered as a Workspace.")
            made = False
            try:
                if create_directory:
                    await asyncio.to_thread(root.mkdir, exist_ok=False)
                    made = True
                workspace = LocalWorkspace(uuid4().hex, root, name.strip())
                await workspace.avalidate()
                await self._publish(workspace)
            except BaseException:
                if made:
                    try:
                        await asyncio.to_thread(root.rmdir)
                    except OSError:
                        pass
                raise
            return next(item for item in await self.describe() if item["id"] == workspace.id)

    @staticmethod
    def key(snapshot):
        binding = getattr(getattr(snapshot, "settings", None), "workspace", None)
        if binding is None:
            return None
        return binding.workspace_id, binding.working_directory, snapshot.bindings.get("workflow_revision")

    async def validate(self, binding):
        if binding is not None:
            workspace = await self.registry.aresolve(binding.workspace_id)
            await workspace.avalidate(binding.working_directory)

    async def prepare(self, snapshot):
        key = self.key(snapshot)
        if key is None:
            return None
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            registry = self._catalogs.get(key)
            if registry is None:
                namespace = "workspace:" + hashlib.sha256(json.dumps(key).encode()).hexdigest()
                registry = self._catalogs[key] = AgentResourceRegistry(namespace=namespace)
                ir = self.service.host._runtime.workflow_irs[key[2]]
                registry.claim_source("skills", workflows={ir.workflow_id})
            try:
                workspace = await self.registry.aresolve(key[0])
                discovery = await workspace.adiscover(key[1])
                if discovery.workspace.id != key[0] or discovery.working_directory != key[1]:
                    raise WorkspaceError("Workspace discovery does not match its binding.")
                registry.replace_source("skills", {("skill", skill.metadata.name): (skill.snapshot_revision, skill)
                    for skill in discovery.skills})
                self._discoveries[key] = discovery
                return discovery
            except BaseException:
                registry.remove_source("skills")
                self._discoveries.pop(key, None)
                raise

    def catalog(self, snapshot, workflow_id):
        key = self.key(snapshot)
        if key is None:
            return (), ()
        registry = self._catalogs.get(key)
        if registry is None or key not in self._discoveries:
            raise WorkspaceError("Prepare the bound Workspace before reading its catalog.")
        return registry.catalog(workflow_id)

    def revision(self, snapshot):
        registry = self._catalogs.get(self.key(snapshot))
        return registry.revision if registry is not None else 0

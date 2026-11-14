"""App-owned Workspace discovery and built-in Tool group composition."""

import asyncio
import hashlib
import json
import os

from autoagent.agent.resources import AgentResourceRegistry
from autoagent.agent.workspace import LocalWorkspace, WorkspaceError, WorkspaceRegistry


class AgentWorkspaces:
    def __init__(self, service):
        self.service = service
        self.registry = WorkspaceRegistry()
        root = service.host.root.resolve()
        self.project_id = "project:" + hashlib.sha256(os.path.normcase(str(root)).encode()).hexdigest()
        self.registry.register(LocalWorkspace(self.project_id, root, root.name or "Current project"))
        self._catalogs = {}
        self._discoveries = {}
        self._locks = {}
        self._registration_lock = asyncio.Lock()
        self._file_services = {}
        self._file_sources = {}
        self._execution_services = {}
        self._execution_sources = {}
        self.process_manager = None


    def clear(self):
        self._catalogs.clear()
        self._discoveries.clear()
        self._locks.clear()
        self._file_services.clear()
        self._file_sources.clear()
        self._execution_services.clear()
        self._execution_sources.clear()

    async def close_execution(self):
        if self.process_manager is not None:
            await self.process_manager.aclose()
            self.process_manager = None


    async def describe(self):
        entries = self.registry.entries()
        def read():
            result = []
            for workspace in entries:
                local = isinstance(workspace, LocalWorkspace)
                result.append({**workspace.info.model_dump(mode="json"),
                    "path": str(workspace.root) if local else None,
                    "is_project": workspace.info.id == self.project_id,
                    "available": workspace.root.is_dir() if local else None})
            return sorted(result, key=lambda item: (not item["is_project"], item["name"].casefold(), item["id"]))
        return await asyncio.to_thread(read)


    @staticmethod
    def key(snapshot):
        binding = getattr(getattr(snapshot, "settings", None), "workspace", None)
        return (binding.workspace_id, binding.working_directory, snapshot.bindings.get("workflow_revision")) if binding else ("@unbound", ".", snapshot.bindings.get("workflow_revision"))

    async def validate(self, binding):
        if binding is not None:
            workspace = await self.registry.aresolve(binding.workspace_id)
            await workspace.avalidate(binding.working_directory)

    async def prepare(self, snapshot):
        key = self.key(snapshot)
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            registry = self._catalogs.get(key)
            if registry is None:
                namespace = "workspace:" + hashlib.sha256(json.dumps(key).encode()).hexdigest()
                registry = self._catalogs[key] = AgentResourceRegistry(namespace=namespace)
                ir = self.service.host._runtime.workflow_irs[key[2]]
                registry.claim_source("skills", workflows={ir.workflow_id})
                registry.claim_source("files", workflows={ir.workflow_id})
                registry.claim_source("execution", workflows={ir.workflow_id})
            try:
                bound = key[0] != "@unbound"
                workspace = await self.registry.aresolve(key[0] if bound else self.project_id)
                from autoagent.agent.workspace import WorkspaceDiscovery
                discovery = await workspace.adiscover(key[1]) if bound else WorkspaceDiscovery(workspace.info, key[1])
                if bound and (discovery.workspace.id != key[0] or discovery.working_directory != key[1]):
                    raise WorkspaceError("Workspace discovery does not match its binding.")
                registry.replace_source("skills", {("skill", skill.metadata.name): (skill.snapshot_revision, skill)
                    for skill in discovery.skills} if bound else {})
                ir = self.service.host._runtime.workflow_irs[key[2]]
                handler = next(getattr(node.executable, "handler", None) for node in ir.nodes
                    if getattr(getattr(node.executable, "handler", None), "__autoagent_builtin_chat__", False))
                from .tool_defaults import file_defaults
                config = file_defaults() if bound else None
                factory = getattr(workspace, "file_service", None)
                if config is not None and callable(factory):
                    identity = (id(workspace), key[1], json.dumps(config.model_dump(mode="json"), sort_keys=True))
                    if self._file_sources.get(key) != identity:
                        from autoagent.agent.files import FileTools
                        backend = self._file_services.get(identity)
                        if backend is None:
                            backend = await asyncio.to_thread(factory, key[1], config=config)
                            self._file_services[identity] = backend
                        group = FileTools(service=backend)
                        tools = group._tools
                        registry.replace_source("files", {("tool", item.name): (backend.version, tools[item.name])
                            for item in tools.definitions})
                        self._file_sources[key] = identity
                else:
                    registry.replace_source("files", {})
                    self._file_sources.pop(key, None)
                execution = getattr(handler, "__agent_app_execution__", None)
                execution_factory = getattr(workspace, "execution_service", None)
                if execution is not None and callable(execution_factory):
                    source = (id(workspace), key[1], json.dumps(execution.model_dump(mode="json"), sort_keys=True))
                    if self._execution_sources.get(key) != source:
                        from autoagent.agent.execution import ProcessManager, CommandTools
                        if self.process_manager is None:
                            self.process_manager = ProcessManager()
                        backend = self._execution_services.get(source)
                        if backend is None:
                            backend = await asyncio.to_thread(execution_factory, key[1], config=execution, manager=self.process_manager)
                            self._execution_services[source] = backend
                        group = CommandTools(service=backend)
                        tools = group._tools
                        registry.replace_source("execution", {("tool", item.name): (backend.version, tools[item.name])
                            for item in tools.definitions})
                        self._execution_sources[key] = source
                else:
                    registry.replace_source("execution", {})
                    self._execution_sources.pop(key, None)
                self._discoveries[key] = discovery
                return discovery if bound else None
            except BaseException:
                registry.remove_source("skills")
                registry.remove_source("files")
                registry.remove_source("execution")
                self._file_sources.pop(key, None)
                self._execution_sources.pop(key, None)
                self._discoveries.pop(key, None)
                raise

    def catalog(self, snapshot, workflow_id):
        key = self.key(snapshot)
        registry = self._catalogs.get(key)
        if registry is None or key not in self._discoveries:
            raise WorkspaceError("Prepare the bound Workspace before reading its catalog.")
        return registry.catalog(workflow_id)

    def revision(self, snapshot):
        registry = self._catalogs.get(self.key(snapshot))
        return registry.revision if registry is not None else 0

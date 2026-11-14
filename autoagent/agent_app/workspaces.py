"""Persistent Workspace management belonging to the Agent application."""

import asyncio
from pathlib import Path
from uuid import uuid4

from autoagent.agent.workspace import LocalWorkspace, WorkspaceError
from .workspace_resources import AgentWorkspaces


class ApplicationWorkspaces(AgentWorkspaces):
    def __init__(self, service):
        super().__init__(service)

    @staticmethod
    def configuration(workspace):
        return dict(id=workspace.id, name=workspace.name, root=str(workspace.root))


    async def restore(self):
        storage = self.service
        root = self.service.host.root.resolve()
        records = await storage.workspace_records()
        # Reuse the registered identity for this directory. A saved Workspace
        # keeps its own path when another project opens the same database.
        matches = []
        for record in records:
            try:
                if Path(record["root"]).resolve() == root:
                    matches.append(record)
            except (OSError, RuntimeError):
                # An unavailable unrelated directory must not prevent startup.
                continue
        if matches:
            current = min(matches, key=lambda record: (record["id"] != self.project_id, record["id"]))
            self.registry.unregister(self.project_id)
            self.project_id = current["id"]
            self.registry.register(LocalWorkspace(**current), replace=True)
        declared = {value.info.id: value for value in self.registry.entries()}
        for record in records:
            if record["id"] not in declared:
                self.registry.register(LocalWorkspace(**record), replace=True)
        for value in declared.values():
            if isinstance(value, LocalWorkspace):
                await storage.register_workspace_record(self.configuration(value))


    async def register(self, workspace, *, replace=False):
        if not isinstance(workspace, LocalWorkspace):
            raise TypeError("Persisted Workspace registration currently accepts LocalWorkspace.")
        async with self._registration_lock:
            await self._publish(workspace, replace=replace)
        return workspace.info


    async def _publish(self, workspace, *, replace=False):
        if workspace.id == self.project_id and workspace.root != self.service.host.root:
            raise WorkspaceError("The project Workspace always uses the current project root.")
        if not replace and any(item.info.id == workspace.id for item in self.registry.entries()):
            raise WorkspaceError("Workspace is already registered.")
        await self.service.register_workspace_record(self.configuration(workspace))
        self.registry.register(workspace, replace=replace)


    async def create(self, *, name, path):
        if not isinstance(name, str) or not name.strip() or len(name.strip()) > 128:
            raise WorkspaceError("Workspace name must contain 1–128 characters.")
        if not isinstance(path, str) or not path.strip() or "\x00" in path:
            raise WorkspaceError("A directory path is required.")
        async with self._registration_lock:
            requested = self.service.host.root / Path(path.strip()).expanduser()
            root = await asyncio.to_thread(requested.resolve)
            for entry in self.registry.entries():
                if isinstance(entry, LocalWorkspace) and await asyncio.to_thread(entry.root.resolve) == root:
                    raise WorkspaceError("This directory is already registered as a Workspace.")
            made = []
            def prepare_directory():
                missing, current = [], root
                while not current.exists():
                    missing.append(current)
                    current = current.parent
                if not current.is_dir():
                    raise WorkspaceError("Workspace path must be a directory.")
                for directory in reversed(missing):
                    try:
                        directory.mkdir()
                    except FileExistsError:
                        if not directory.is_dir():
                            raise WorkspaceError("Workspace path must be a directory.")
                    else:
                        made.append(directory)
            try:
                creation = asyncio.create_task(asyncio.to_thread(prepare_directory))
                try:
                    await asyncio.shield(creation)
                except BaseException:
                    # Finish filesystem work before rolling back its new directories.
                    await creation
                    raise
                workspace = LocalWorkspace(uuid4().hex, root, name.strip())
                await workspace.avalidate()
                await self._publish(workspace)
            except BaseException as error:
                for directory in reversed(made):
                    try:
                        await asyncio.to_thread(directory.rmdir)
                    except OSError:
                        pass
                if isinstance(error, OSError):
                    raise WorkspaceError(f"Cannot prepare Workspace directory: {error.strerror or error}") from error
                raise
            return next(item for item in await self.describe() if item["id"] == workspace.id)

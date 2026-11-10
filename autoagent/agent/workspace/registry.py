"""Application-owned Workspace implementations and an optional remote resolver."""

from threading import RLock

from .models import Workspace, WorkspaceError, WorkspaceResolver


class WorkspaceRegistry:
    def __init__(self, *, resolver: WorkspaceResolver | None = None):
        self.resolver = resolver
        self._items = {}
        self._lock = RLock()

    def register(self, workspace: Workspace, *, replace=False):
        if not isinstance(workspace, Workspace):
            raise TypeError("Workspace must provide info, avalidate and adiscover.")
        with self._lock:
            if workspace.info.id in self._items and not replace:
                raise ValueError("Workspace is already registered.")
            self._items[workspace.info.id] = workspace
        return workspace

    def entries(self):
        with self._lock:
            return tuple(self._items.values())

    def unregister(self, workspace_id: str):
        with self._lock:
            return self._items.pop(workspace_id, None)

    async def aresolve(self, workspace_id: str) -> Workspace:
        with self._lock:
            workspace = self._items.get(workspace_id)
        if workspace is None and self.resolver is not None:
            workspace = await self.resolver.aresolve(workspace_id)
        if not isinstance(workspace, Workspace) or workspace.info.id != workspace_id:
            raise WorkspaceError(f"Workspace {workspace_id!r} is not registered.")
        return workspace

"""Optional Agent workspaces; runtime resources are managed by Host."""

from .models import Workspace, WorkspaceBinding, WorkspaceDiscovery, WorkspaceError, WorkspaceInfo, WorkspaceInstruction, WorkspaceResolver
from .local import LocalWorkspace
from .registry import WorkspaceRegistry

__all__ = ["Workspace", "WorkspaceBinding", "WorkspaceError", "WorkspaceInfo", "WorkspaceInstruction",
           "WorkspaceResolver", "LocalWorkspace", "WorkspaceDiscovery", "WorkspaceRegistry"]

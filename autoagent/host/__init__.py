"""Project configuration and Workflow loading for the V2 Host layer."""

from .environment import load_project_environment, project_environment_scope
from .errors import (
    HostConfigurationError,
    HostDiagnostic,
    HostOperationError,
    HostSettingsError,
    ProjectLoadError,
)
from .host import AutoAgentHost
from .loader import LoadedProject, LoadedWorkflow, ProjectLoader
from .manifest import (
    MANIFEST_FILENAME,
    ProjectManifest,
    ProjectMetadata,
    WorkflowLocator,
    load_project_manifest,
    resolve_manifest_path,
)
from .settings import AUTOAGENT_ENV_KEYS, HostSettings, load_host_settings
from .sinks import (
    HostRuntimeEventSink,
    RecoverySource,
    WorkflowDefinitionSink,
    create_runtime_event_sink,
)

__all__ = [
    "AUTOAGENT_ENV_KEYS",
    "HostConfigurationError",
    "HostDiagnostic",
    "HostOperationError",
    "HostRuntimeEventSink",
    "HostSettings",
    "HostSettingsError",
    "LoadedProject",
    "LoadedWorkflow",
    "MANIFEST_FILENAME",
    "AutoAgentHost",
    "ProjectLoadError",
    "ProjectLoader",
    "ProjectManifest",
    "ProjectMetadata",
    "RecoverySource",
    "WorkflowLocator",
    "WorkflowDefinitionSink",
    "create_runtime_event_sink",
    "load_host_settings",
    "load_project_environment",
    "load_project_manifest",
    "project_environment_scope",
    "resolve_manifest_path",
]

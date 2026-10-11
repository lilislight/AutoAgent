"""Host-owned integration capabilities, separate from Workflow Core."""

from importlib import import_module

__all__ = [
    "AutoAgentHost",
    "HostError",
    "HostCapacityError",
    "HostSettings",
    "HostSetup",
    "ProjectCheck",
    "ProjectError",
    "load_project_environment",
    "workflow",
]


def __getattr__(name):
    """Keep the existing Persistence extra independent of project dotenv imports."""
    modules = {
        "AutoAgentHost": "host",
        "HostError": "models",
        "HostCapacityError": "models",
        "HostSettings": "models",
        "ProjectCheck": "models",
        "ProjectError": "models",
        "HostSetup": "runtime",
        "load_project_environment": "project",
        "workflow": "project",
    }
    if name not in modules:
        raise AttributeError(name)
    value = getattr(import_module(f"{__name__}.{modules[name]}"), name)
    globals()[name] = value
    return value

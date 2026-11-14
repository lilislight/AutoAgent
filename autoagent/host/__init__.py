"""Host-owned integration capabilities, separate from Workflow Core."""

from importlib import import_module

__all__ = [
    "AutoAgentHost",
    "ExecutionView",
    "ViewUnavailable",
    "HostError",
    "HostCapacityError",
    "HostNotFoundError",
    "HostRunConflict",
    "HostSettings",
    "HostSetup",
    "UserEventSubscription",
    "UserEventStreamOverflow",
    "ProjectCheck",
    "ProjectError",
    "load_project_environment",
    "workflow",
]


def __getattr__(name):
    """Keep the existing Persistence extra independent of project dotenv imports."""
    modules = {
        "AutoAgentHost": "host",
        "ExecutionView": "execution_view",
        "ViewUnavailable": "execution_view",
        "HostError": "models",
        "HostCapacityError": "models",
        "HostNotFoundError": "models",
        "HostRunConflict": "models",
        "HostSettings": "models",
        "ProjectCheck": "models",
        "ProjectError": "models",
        "HostSetup": "runtime",
        "UserEventSubscription": "user_events",
        "UserEventStreamOverflow": "user_events",
        "load_project_environment": "project",
        "workflow": "project",
    }
    if name not in modules:
        raise AttributeError(name)
    value = getattr(import_module(f"{__name__}.{modules[name]}"), name)
    globals()[name] = value
    return value

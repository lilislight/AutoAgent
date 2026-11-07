"""Host-owned integration capabilities, separate from Workflow Core."""

from importlib import import_module

__all__ = [
    "AutoAgentHost",
    "HostError",
    "HostCapacityError",
    "HostNotFoundError",
    "HostRunConflict",
    "HostSettings",
    "HostSetup",
    "UserEventSubscription",
    "UserEventStreamOverflow",
    "HostAgentContext",
    "ProjectCheck",
    "ProjectError",
    "load_project_environment",
    "workflow",
    "AgentSettings",
    "llm_request",
    "embedding_request",
]


def __getattr__(name):
    """Keep the existing Persistence extra independent of project dotenv imports."""
    modules = {
        "AutoAgentHost": "host",
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
        "HostAgentContext": "agent_context",
        "load_project_environment": "project",
        "workflow": "project",
        "AgentSettings": "agent",
        "llm_request": "agent",
        "embedding_request": "agent",
    }
    if name not in modules:
        raise AttributeError(name)
    value = getattr(import_module(f"{__name__}.{modules[name]}"), name)
    globals()[name] = value
    return value

"""The built-in Agent application, separate from the Agent Workflow SDK."""

from .application import AgentApplication
from .workflow import BUILTIN_AGENT_WORKFLOW_ID, builtin_agent_workflow
from .credentials import CredentialReference
from .models import ApplicationSessionSettings, ApplicationSettingsRecord

__all__ = ["AgentApplication", "BUILTIN_AGENT_WORKFLOW_ID", "builtin_agent_workflow",
    "CredentialReference", "ApplicationSessionSettings", "ApplicationSettingsRecord"]

from .resource_config import AgentResourceConfig, MCPResourceConfig, SkillResourceConfig, ResourceControl, GroupControl
__all__ += ["AgentResourceConfig", "MCPResourceConfig", "SkillResourceConfig", "ResourceControl", "GroupControl"]

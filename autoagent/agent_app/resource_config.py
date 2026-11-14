"""Declarative resources owned by the Agent application."""

from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator

from autoagent.agent.mcp.config import MCPTransportConfig
from autoagent.agent.mcp.recovery import MCPReconnectPolicy
from autoagent.agent.models._base import Model, NonEmpty
from autoagent.agent.tools import ToolPolicy
from .credentials import CredentialReference
from autoagent.host.providers import connection_url


class _ResourceConfig(Model):
    name: NonEmpty
    enabled: bool = True
    permissions: ToolPolicy = Field(default_factory=ToolPolicy)


class MCPResourceConfig(_ResourceConfig):
    kind: Literal["mcp"] = "mcp"
    transport: Annotated[MCPTransportConfig, Field(discriminator="type")]
    prefix: str = ""
    include: tuple[NonEmpty, ...] | None = None
    exclude: tuple[NonEmpty, ...] = ()
    reconnect: MCPReconnectPolicy = Field(default_factory=MCPReconnectPolicy)
    environment: dict[str, CredentialReference] = Field(default_factory=dict)
    headers: dict[str, CredentialReference] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _references(self):
        if getattr(self.transport, "env", None) or getattr(self.transport, "headers", None):
            raise ValueError("MCP environment and Headers must use credential references.")
        if self.transport.type == "stdio" and self.headers or self.transport.type != "stdio" and self.environment:
            raise ValueError("Environment references require stdio; Header references require HTTP.")
        if self.transport.type != "stdio":
            connection_url(self.transport.url)
        return self

    def resolve_transport(self, credentials):
        values = self.environment if self.transport.type == "stdio" else self.headers
        name = "env" if self.transport.type == "stdio" else "headers"
        return self.transport.model_copy(update={name:{key:credentials.resolve(ref) for key, ref in values.items()}})

    @field_validator("include", "exclude", mode="before")
    @classmethod
    def _names(cls, value):
        return tuple(value) if isinstance(value, list) else value


class SkillResourceConfig(_ResourceConfig):
    kind: Literal["skills"] = "skills"
    path: NonEmpty
    include: tuple[NonEmpty, ...] | None = None
    interval_seconds: float = Field(default=1, gt=0, allow_inf_nan=False)

    @field_validator("include", mode="before")
    @classmethod
    def _names(cls, value):
        return tuple(value) if isinstance(value, list) else value

    @field_validator("path")
    @classmethod
    def _absolute(cls, value):
        path = Path(value).expanduser()
        if not path.is_absolute():
            raise ValueError("Skill directories require an absolute path on the Host.")
        return str(path.resolve())


class ResourceControl(_ResourceConfig):
    """A live override for a registered Tool or Skill, identified by resource ID."""

    kind: Literal["resource"] = "resource"
    resource_id: NonEmpty
    permissions: ToolPolicy | None = None


class GroupControl(_ResourceConfig):
    """A product-wide switch and permission override for one resource group."""

    kind: Literal["group"] = "group"
    group_id: NonEmpty
    permissions: ToolPolicy | None = None


AgentResourceConfig = Annotated[MCPResourceConfig | SkillResourceConfig | ResourceControl | GroupControl,
    Field(discriminator="kind")]


__all__ = ["AgentResourceConfig", "MCPResourceConfig", "SkillResourceConfig", "ResourceControl", "GroupControl"]

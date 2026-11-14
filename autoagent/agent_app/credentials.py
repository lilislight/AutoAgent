"""Environment references for MCP authentication; values never enter product rows."""

from typing import Literal
from pydantic import Field
from autoagent.agent.models._base import Model


class CredentialReference(Model):
    source: Literal["env"] = "env"
    name: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")


class CredentialUnavailable(ValueError):
    pass


class ApplicationCredentials:
    def __init__(self, host):
        self.host = host

    def resolve(self, reference):
        value = self.host.environment.get(reference.name)
        if not isinstance(value, str) or not value:
            raise CredentialUnavailable(f"Environment variable {reference.name!r} is unavailable.")
        return value


__all__ = ["CredentialReference", "CredentialUnavailable"]

"""Small application-owned credential references; secret values never enter configuration rows."""

import os
from pathlib import Path
import re

from pydantic import Field
from typing import Literal
from autoagent.agent.models._base import Model


class CredentialReference(Model):
    source: Literal["env", "file"] = "env"
    name: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")


class CredentialUnavailable(ValueError):
    pass


class ApplicationCredentials:
    def __init__(self, host, path=None):
        self.host = host
        self.path = Path(path).expanduser() if path else host.root / ".autoagent" / ".env.credentials"
        if not self.path.is_absolute():
            raise ValueError("Agent credential files require an absolute path.")

    def resolve(self, reference):
        from dotenv import dotenv_values
        try:
            values = self.host.environment if reference.source == "env" else dotenv_values(self.path, interpolate=False)
            value = values.get(reference.name)
        except (OSError, ValueError):
            raise CredentialUnavailable("Credential source is unavailable.") from None
        if not isinstance(value, str) or not value:
            raise CredentialUnavailable(f"Credential reference {reference.name!r} is unavailable.")
        return value

    def import_legacy(self, name, value):
        """Move an old persisted credential into the dedicated file before dropping its SQL value."""
        from dotenv import set_key
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError("Invalid credential name.")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.is_symlink():
            raise CredentialUnavailable("Credential file must not be a symbolic link.")
        descriptor = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        os.close(descriptor)
        os.chmod(self.path, 0o600)
        set_key(str(self.path), name, value, quote_mode="always")
        os.chmod(self.path, 0o600)
        return CredentialReference(source="file", name=name)


__all__ = ["CredentialReference", "CredentialUnavailable"]

"""Persisted metadata owned by the Agent application."""

import json
from typing import Literal
from pydantic import Field
from autoagent.agent.context import AgentSettingsRecord, AgentSessionSettings


class ApplicationSessionSettings(AgentSessionSettings):
    approval_mode: Literal["policy", "always_ask", "auto_approve"] = "policy"

    @classmethod
    def from_runtime(cls, settings):
        values = settings.session_defaults().model_dump(mode="json")
        return cls.model_validate_json(json.dumps(values))


class ApplicationSettingsRecord(AgentSettingsRecord):
    settings: ApplicationSessionSettings
    title: str | None = None
    metadata_version: int = Field(default=0, ge=0)

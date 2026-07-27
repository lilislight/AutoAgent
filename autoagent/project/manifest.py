from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


_MODULE_SEGMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_OBJECT_PATH = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$"
)


def _non_empty(value: str, *, field_name: str) -> str:
    resolved = value.strip()
    if not resolved:
        raise ValueError(f"{field_name} cannot be empty.")
    return resolved


class ProjectMetadata(BaseModel):
    """Versioned identity and human-readable metadata for one AutoAgent project."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(description="Stable project name.")
    version: str = Field(description="Project version.")
    description: str | None = Field(
        default=None,
        description="Optional human-readable project description.",
    )

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return _non_empty(value, field_name="Project name")

    @field_validator("version")
    @classmethod
    def validate_version(cls, value: str) -> str:
        return _non_empty(value, field_name="Project version")

    @field_validator("description")
    @classmethod
    def normalize_description(cls, value: str | None) -> str | None:
        if value is None:
            return None
        resolved = value.strip()
        return resolved or None


class WorkflowLocator(BaseModel):
    """Import locator for one Workflow object."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    entrypoint: str = Field(
        description="Python import locator in '<module>:<object>' format.",
    )

    @field_validator("entrypoint")
    @classmethod
    def validate_entrypoint(cls, value: str) -> str:
        resolved = value.strip()
        if resolved.count(":") != 1:
            raise ValueError(
                "Workflow entrypoint must use '<module>:<object>' format."
            )

        module_name, object_path = resolved.split(":", 1)
        if not module_name or any(
            not _MODULE_SEGMENT.fullmatch(segment)
            for segment in module_name.split(".")
        ):
            raise ValueError(
                "Workflow entrypoint module must be a dotted Python module name."
            )
        if not _OBJECT_PATH.fullmatch(object_path):
            raise ValueError(
                "Workflow entrypoint object must be a Python attribute path."
            )
        return f"{module_name}:{object_path}"

    @property
    def module_name(self) -> str:
        return self.entrypoint.split(":", 1)[0]

    @property
    def object_path(self) -> str:
        return self.entrypoint.split(":", 1)[1]


class ProjectManifest(BaseModel):
    """Strict V1 representation of ``auto-agent.toml``."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = Field(
        description="Manifest schema version understood by this AutoAgent release.",
    )
    project: ProjectMetadata
    workflows: tuple[WorkflowLocator, ...] = Field(
        min_length=1,
        description="Explicit Workflow objects exported by the project.",
    )

    @model_validator(mode="after")
    def validate_unique_entrypoints(self) -> ProjectManifest:
        seen: set[str] = set()
        duplicates: set[str] = set()
        for locator in self.workflows:
            if locator.entrypoint in seen:
                duplicates.add(locator.entrypoint)
            seen.add(locator.entrypoint)
        if duplicates:
            rendered = ", ".join(sorted(duplicates))
            raise ValueError(f"Duplicate Workflow entrypoint: {rendered}")
        return self

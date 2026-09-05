"""Strict schema and parser for the versioned ``autoagent.toml`` file."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from .errors import HostDiagnostic, ProjectLoadError


MANIFEST_FILENAME = "autoagent.toml"
_MODULE_SEGMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_OBJECT_PATH = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$"
)


class ProjectMetadata(BaseModel):
    """Versioned identity and human-readable metadata for one project."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    name: str
    version: str
    description: str | None = None

    @field_validator("name", "version")
    @classmethod
    def require_non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("value cannot be empty")
        return value.strip()

    @field_validator("description")
    @classmethod
    def normalize_description(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return value.strip() or None


class WorkflowLocator(BaseModel):
    """An explicit Python object in ``<module>:<object>`` form."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    entrypoint: str

    @field_validator("entrypoint")
    @classmethod
    def validate_entrypoint(cls, value: str) -> str:
        resolved = value.strip()
        if resolved.count(":") != 1:
            raise ValueError(
                "Workflow entrypoint must use '<module>:<object>' format"
            )
        module_name, object_path = resolved.split(":", 1)
        if not module_name or any(
            not _MODULE_SEGMENT.fullmatch(segment)
            for segment in module_name.split(".")
        ):
            raise ValueError("Workflow module must be a dotted Python name")
        if not _OBJECT_PATH.fullmatch(object_path):
            raise ValueError("Workflow object must be a Python attribute path")
        return f"{module_name}:{object_path}"

    @property
    def module_name(self) -> str:
        return self.entrypoint.split(":", 1)[0]

    @property
    def object_path(self) -> str:
        return self.entrypoint.split(":", 1)[1]


class ProjectManifest(BaseModel):
    """Complete schema-version-1 project manifest."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal[1]
    project: ProjectMetadata
    workflows: tuple[WorkflowLocator, ...] = Field(min_length=1)

    @field_validator("workflows", mode="before")
    @classmethod
    def accept_toml_array_of_tables(cls, value: object) -> object:
        # tomllib represents ``[[workflows]]`` as a list.  The explicit
        # conversion keeps the public model immutable without enabling general
        # Pydantic coercion for field values.
        return tuple(value) if isinstance(value, list) else value

    @field_validator("workflows")
    @classmethod
    def require_unique_entrypoints(
        cls, value: tuple[WorkflowLocator, ...]
    ) -> tuple[WorkflowLocator, ...]:
        seen: set[str] = set()
        duplicates: set[str] = set()
        for locator in value:
            if locator.entrypoint in seen:
                duplicates.add(locator.entrypoint)
            seen.add(locator.entrypoint)
        if duplicates:
            rendered = ", ".join(sorted(duplicates))
            raise ValueError(f"Duplicate Workflow entrypoint: {rendered}")
        return value


def resolve_manifest_path(path: str | Path | None = None) -> Path:
    """Find the nearest manifest or validate one explicit manifest file."""

    candidate = Path.cwd() if path is None else Path(path)
    candidate = candidate.expanduser().resolve()
    if candidate.is_dir():
        for directory in (candidate, *candidate.parents):
            manifest = directory / MANIFEST_FILENAME
            if manifest.is_file():
                return manifest
        raise ProjectLoadError(
            [
                HostDiagnostic(
                    code="PROJECT_MANIFEST_NOT_FOUND",
                    message=(
                        f"Could not find {MANIFEST_FILENAME} from {candidate} "
                        "or any parent directory."
                    ),
                    path=str(candidate),
                    hint=f"Create {MANIFEST_FILENAME} at the project root.",
                )
            ]
        )
    if not candidate.exists() and candidate.name != MANIFEST_FILENAME:
        raise ProjectLoadError(
            [
                HostDiagnostic(
                    code="PROJECT_MANIFEST_NOT_FOUND",
                    message=f"Project path does not exist: {candidate}",
                    path=str(candidate),
                )
            ]
        )
    if candidate.name != MANIFEST_FILENAME:
        raise ProjectLoadError(
            [
                HostDiagnostic(
                    code="PROJECT_MANIFEST_FILENAME_INVALID",
                    message=f"Project manifest must be named {MANIFEST_FILENAME}.",
                    path=str(candidate),
                    hint=f"Rename the file to {MANIFEST_FILENAME}.",
                )
            ]
        )
    return candidate


def load_project_manifest(path: str | Path) -> ProjectManifest:
    """Read and strictly validate one project manifest."""

    manifest_path = resolve_manifest_path(path)
    try:
        with manifest_path.open("rb") as stream:
            raw = tomllib.load(stream)
    except FileNotFoundError as error:
        raise ProjectLoadError(
            [
                HostDiagnostic(
                    code="PROJECT_MANIFEST_NOT_FOUND",
                    message=f"Project manifest does not exist: {manifest_path}",
                    path=str(manifest_path),
                )
            ]
        ) from error
    except tomllib.TOMLDecodeError as error:
        raise ProjectLoadError(
            [
                HostDiagnostic(
                    code="PROJECT_MANIFEST_INVALID_TOML",
                    message=f"Cannot parse project manifest: {error}",
                    path=str(manifest_path),
                )
            ]
        ) from error
    except OSError as error:
        raise ProjectLoadError(
            [
                HostDiagnostic(
                    code="PROJECT_MANIFEST_READ_FAILED",
                    message=f"Cannot read project manifest: {error}",
                    path=str(manifest_path),
                )
            ]
        ) from error

    try:
        return ProjectManifest.model_validate(raw)
    except ValidationError as error:
        diagnostics = [
            HostDiagnostic(
                code="PROJECT_MANIFEST_INVALID",
                message=item["msg"],
                path=str(manifest_path),
                field=".".join(str(part) for part in item["loc"]),
                metadata={"type": item["type"]},
            )
            for item in error.errors(include_url=False, include_context=False)
        ]
        raise ProjectLoadError(diagnostics) from error

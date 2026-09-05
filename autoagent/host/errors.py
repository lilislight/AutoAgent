"""Stable diagnostics for V2 project configuration and loading failures."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class HostDiagnostic(BaseModel):
    """One machine-readable problem found before a Host can be assembled."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    code: str
    message: str
    severity: Literal["error"] = "error"
    path: str | None = None
    field: str | None = None
    entrypoint: str | None = None
    hint: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)


class HostConfigurationError(RuntimeError):
    """Base class for expected, structured Host configuration failures."""

    def __init__(self, diagnostics: list[HostDiagnostic]) -> None:
        if not diagnostics:
            raise ValueError("HostConfigurationError requires a diagnostic.")
        self.diagnostics = tuple(diagnostics)
        super().__init__("\n".join(item.message for item in diagnostics))


class ProjectLoadError(HostConfigurationError):
    """The project manifest or one of its Workflow entrypoints is invalid."""


class HostSettingsError(HostConfigurationError):
    """The environment cannot be converted into immutable Host settings."""


class HostOperationError(RuntimeError):
    """A Host lifecycle operation is unavailable in its current state."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")

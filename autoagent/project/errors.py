from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class ProjectDiagnostic(BaseModel):
    """Machine-readable problem discovered while loading an AutoAgent project."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    code: str
    severity: Literal["error", "warning", "info"] = "error"
    message: str
    path: str | None = None
    field: str | None = None
    entrypoint: str | None = None
    hint: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ProjectLoadError(RuntimeError):
    """Expected project-loading failure with structured diagnostics."""

    def __init__(self, diagnostics: list[ProjectDiagnostic]) -> None:
        if not diagnostics:
            raise ValueError("ProjectLoadError requires at least one diagnostic.")
        self.diagnostics = tuple(diagnostics)
        super().__init__("\n".join(item.message for item in diagnostics))

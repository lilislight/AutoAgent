from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from autoagent.compiler.workflow_ir import WorkflowIR


class Diagnostic(BaseModel):
    """Compiler diagnostic."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    code: str = Field(description="Stable diagnostic code.")
    severity: Literal["error", "warning", "info"] = Field(
        description="Diagnostic severity.",
    )
    message: str = Field(description="Human-readable diagnostic message.")
    subject: str | None = Field(
        default=None,
        description="Optional related workflow object id.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional machine-readable diagnostic data.",
    )


class CompileResult(BaseModel):
    """Result returned by WorkflowCompiler."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    workflow_ir: WorkflowIR | None = Field(
        default=None,
        description="Compiled Workflow IR when compilation succeeds.",
    )
    diagnostics: list[Diagnostic] = Field(
        default_factory=list,
        description="Compiler diagnostics.",
    )

    @property
    def ok(self) -> bool:
        return self.workflow_ir is not None and not any(
            diagnostic.severity == "error" for diagnostic in self.diagnostics
        )

from __future__ import annotations

import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from autoagent.core.compiler.analysis import WorkflowAnalysis
from autoagent.core.compiler.workflow_ir import WorkflowIR
from autoagent.core.compiler.snapshot import WorkflowVersionSnapshot


class Diagnostic(BaseModel):
    """Compiler diagnostic."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    code: str = Field(description="Stable diagnostic code.")
    severity: Literal["error", "warning", "info"] = Field(
        description="Diagnostic severity.",
    )
    message: str = Field(description="Human-readable diagnostic message.")
    workflow_id: str | None = Field(
        default=None,
        description="Workflow that produced this diagnostic.",
    )
    object_type: Literal["workflow", "node", "edge"] | None = Field(
        default=None,
        description="Kind of Workflow object associated with the problem.",
    )
    object_id: str | None = Field(
        default=None,
        description="Stable Workflow, Node, or Edge id associated with the problem.",
    )
    field: str | None = Field(
        default=None,
        description="Authoring field that should be inspected or changed.",
    )
    hint: str | None = Field(
        default=None,
        description="Optional concrete next step for resolving the problem.",
    )
    source_index: int | None = Field(
        default=None,
        ge=0,
        description="Optional zero-based source-list index for deterministic tooling.",
    )
    subject: str | None = Field(
        default=None,
        exclude=True,
        description=(
            "Internal Compiler subject used while diagnostics are assembled. "
            "Agent-facing documents use object_id."
        ),
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional machine-readable diagnostic data.",
    )


class CompileResult(BaseModel):
    """Result returned by WorkflowCompiler."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    workflow_id: str | None = Field(
        default=None,
        description="Workflow requested for compilation, including failed results.",
    )
    workflow_version: str | int | None = Field(
        default=None,
        description="Resolved source Workflow version.",
    )
    analysis: WorkflowAnalysis = Field(
        description=(
            "Expanded static graph analysis available for valid and invalid "
            "Workflow definitions."
        ),
    )
    workflow_ir: WorkflowIR | None = Field(
        default=None,
        description="Compiled Workflow IR when compilation succeeds.",
    )
    workflow_snapshot: WorkflowVersionSnapshot | None = Field(
        default=None,
        description="Portable structural snapshot when compilation succeeds.",
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

    def to_diagnostic_document(self) -> dict[str, Any]:
        """Return deterministic lightweight output for a CLI or Coding Agent."""

        counts = {"error": 0, "warning": 0, "info": 0}
        rendered: list[dict[str, Any]] = []
        for diagnostic in self.diagnostics:
            counts[diagnostic.severity] += 1
            item = diagnostic.model_dump(exclude_none=True)
            metadata = dict(item.get("metadata", {}))
            metadata.pop("object_type", None)
            metadata.pop("source_index", None)
            if metadata:
                item["metadata"] = metadata
            else:
                item.pop("metadata", None)
            rendered.append(item)

        return {
            "ok": self.ok,
            "workflow_id": self.workflow_id,
            "workflow_version": self.workflow_version,
            "summary": counts,
            "diagnostics": rendered,
        }

    def to_diagnostic_json(self, *, indent: int | None = 2) -> str:
        """Serialize the lightweight diagnostic document as stable JSON."""

        return json.dumps(
            self.to_diagnostic_document(),
            ensure_ascii=False,
            indent=indent,
            sort_keys=True,
        )

"""Stable compiler output for applications and authoring tools."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Literal

from ..errors import WorkflowCompileError
from ..workflow import WorkflowIR
from .snapshot import WorkflowDefinitionSnapshot


@dataclass(frozen=True, slots=True)
class Diagnostic:
    code: str
    severity: Literal["error", "warning", "info"]
    message: str
    workflow_id: str | None = None
    object_type: Literal["workflow", "node", "edge"] | None = None
    object_id: str | None = None
    field: str | None = None
    hint: str | None = None

    def to_record(self) -> dict[str, object]:
        return {
            key: value
            for key, value in asdict(self).items()
            if value is not None
        }


@dataclass(frozen=True, slots=True)
class CompileResult:
    workflow_id: str | None
    workflow_ir: WorkflowIR | None = None
    workflow_definition_snapshot: WorkflowDefinitionSnapshot | None = None
    diagnostics: tuple[Diagnostic, ...] = ()

    @property
    def ok(self) -> bool:
        return self.workflow_ir is not None and not any(
            item.severity == "error" for item in self.diagnostics
        )

    def require_workflow_ir(self) -> WorkflowIR:
        if self.ok:
            assert self.workflow_ir is not None
            return self.workflow_ir
        diagnostic = next(
            (item for item in self.diagnostics if item.severity == "error"),
            Diagnostic("COMPILE_FAILED", "error", "Workflow compilation failed."),
        )
        raise WorkflowCompileError(
            diagnostic.message,
            code=diagnostic.code,
            diagnostics=self.diagnostics,
        )

    def to_diagnostic_document(self) -> dict[str, object]:
        counts = {
            severity: sum(item.severity == severity for item in self.diagnostics)
            for severity in ("error", "warning", "info")
        }
        return {
            "ok": self.ok,
            "workflow_id": self.workflow_id,
            "summary": counts,
            "diagnostics": [item.to_record() for item in self.diagnostics],
        }

    def to_diagnostic_json(self, *, indent: int | None = 2) -> str:
        return json.dumps(
            self.to_diagnostic_document(),
            ensure_ascii=False,
            indent=indent,
            sort_keys=True,
        )

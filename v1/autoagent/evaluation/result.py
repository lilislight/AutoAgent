from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


EvalStatus = Literal["completed", "passed", "failed", "error"]
EvalStepAction = Literal["invoke", "resume"]


class EvalError(BaseModel):
    """Stable infrastructure or evaluator failure information."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    code: str = Field(min_length=1)
    message: str = Field(min_length=1)
    detail: dict[str, Any] = Field(default_factory=dict)


class EvalEvidenceRef(BaseModel):
    """Reference from an evaluator conclusion to authoritative Runtime data."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    invocation_id: str = Field(min_length=1)
    through_sequence: int = Field(ge=0)
    kind: str = Field(default="invocation", min_length=1)
    reference_id: str | None = None


class EvaluatorResult(BaseModel):
    """One evaluator conclusion without copying Runtime evidence."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    key: str = Field(min_length=1)
    passed: bool | None = None
    score: float | None = None
    value: Any | None = None
    comment: str | None = None
    evidence: tuple[EvalEvidenceRef, ...] = ()
    error: EvalError | None = None

    @model_validator(mode="after")
    def validate_error_is_not_a_business_result(self) -> EvaluatorResult:
        if self.error is not None and self.passed is not None:
            raise ValueError("An evaluator error cannot also pass or fail.")
        return self


def _results_status(results: tuple[EvaluatorResult, ...]) -> EvalStatus:
    if any(result.error is not None for result in results):
        return "error"
    if any(result.passed is False for result in results):
        return "failed"
    if any(result.passed is True for result in results):
        return "passed"
    return "completed"


class EvalStepResult(BaseModel):
    """Evaluator conclusions at one Invoke or Resume Runtime boundary."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    index: int = Field(ge=1)
    action: EvalStepAction
    invocation_id: str | None = None
    through_sequence: int | None = Field(default=None, ge=0)
    evaluator_results: tuple[EvaluatorResult, ...] = ()
    error: EvalError | None = None

    @property
    def status(self) -> EvalStatus:
        if self.error is not None:
            return "error"
        return _results_status(self.evaluator_results)


class EvalCaseResult(BaseModel):
    """Ordered execution results for one ``eval_*`` method and Session."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    case_id: str = Field(min_length=1)
    session_id: str | None = None
    step_results: tuple[EvalStepResult, ...] = ()
    error: EvalError | None = None

    @property
    def status(self) -> EvalStatus:
        if self.error is not None or any(
            step.status == "error" for step in self.step_results
        ):
            return "error"
        if any(step.status == "failed" for step in self.step_results):
            return "failed"
        if any(step.status == "passed" for step in self.step_results):
            return "passed"
        return "completed"


class EvalResult(BaseModel):
    """Result of one Manifest-registered Evaluation against one Revision."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    suite_id: str = Field(min_length=1)
    workflow_id: str = Field(min_length=1)
    workflow_revision_id: str = Field(min_length=1)
    case_results: tuple[EvalCaseResult, ...] = ()
    error: EvalError | None = None

    @property
    def status(self) -> EvalStatus:
        if self.error is not None or any(
            case.status == "error" for case in self.case_results
        ):
            return "error"
        if any(case.status == "failed" for case in self.case_results):
            return "failed"
        if any(case.status == "passed" for case in self.case_results):
            return "passed"
        return "completed"

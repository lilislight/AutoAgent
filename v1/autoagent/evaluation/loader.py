from __future__ import annotations

from dataclasses import dataclass
import importlib
import inspect
from typing import Any

from autoagent.evaluation.definition import Evaluation, evaluation_case_methods
from autoagent.project.errors import ProjectDiagnostic, ProjectLoadError
from autoagent.project.loader import (
    ProjectDefinition,
    _project_import_path,
    _resolve_object,
)
from autoagent.project.manifest import EvalSuiteLocator


@dataclass(frozen=True, slots=True)
class LoadedEvaluation:
    """Validated Evaluation class selected from one Manifest Suite."""

    locator: EvalSuiteLocator
    evaluation_type: type[Evaluation]
    case_ids: tuple[str, ...]


class EvaluationLoader:
    """Load Evaluation code only when an Eval command explicitly asks for it."""

    def list(self, project: ProjectDefinition) -> tuple[EvalSuiteLocator, ...]:
        return project.eval_suites

    def check_all(
        self,
        project: ProjectDefinition,
    ) -> tuple[LoadedEvaluation, ...]:
        loaded: list[LoadedEvaluation] = []
        diagnostics: list[ProjectDiagnostic] = []
        for locator in project.eval_suites:
            try:
                loaded.append(self._load_locator(project, locator))
            except ProjectLoadError as exc:
                diagnostics.extend(exc.diagnostics)
        if diagnostics:
            raise ProjectLoadError(
                sorted(
                    diagnostics,
                    key=lambda item: (
                        item.entrypoint or "",
                        item.field or "",
                        item.code,
                    ),
                )
            )
        return tuple(loaded)

    def load(
        self,
        project: ProjectDefinition,
        suite_id: str,
    ) -> LoadedEvaluation:
        try:
            locator = project.eval_suite_by_id(suite_id)
        except KeyError as exc:
            raise ProjectLoadError(
                [
                    ProjectDiagnostic(
                        code="EVAL_SUITE_NOT_FOUND",
                        message=f"Unknown Eval Suite: {suite_id}",
                        path=_manifest_path(project),
                        field="eval_suites.id",
                    )
                ]
            ) from exc
        return self._load_locator(project, locator)

    def _load_locator(
        self,
        project: ProjectDefinition,
        locator: EvalSuiteLocator,
    ) -> LoadedEvaluation:
        try:
            project.workflow_by_id(locator.workflow_id)
        except KeyError as exc:
            raise _evaluation_error(
                project,
                locator,
                code="EVAL_WORKFLOW_NOT_FOUND",
                message=(
                    f"Eval Suite '{locator.id}' references unknown Workflow "
                    f"'{locator.workflow_id}'."
                ),
                field="eval_suites.workflow_id",
            ) from exc

        with _project_import_path(project.root):
            importlib.invalidate_caches()
            try:
                module = importlib.import_module(locator.module_name)
            except Exception as exc:
                raise _evaluation_error(
                    project,
                    locator,
                    code="EVAL_MODULE_IMPORT_FAILED",
                    message=(
                        f"Cannot import Evaluation module "
                        f"'{locator.module_name}': {exc}"
                    ),
                    metadata={"exception_type": type(exc).__name__},
                ) from exc

        try:
            value: Any = _resolve_object(module, locator.object_path)
        except AttributeError as exc:
            raise _evaluation_error(
                project,
                locator,
                code="EVAL_OBJECT_NOT_FOUND",
                message=(
                    f"Module '{locator.module_name}' does not export "
                    f"'{locator.object_path}'."
                ),
            ) from exc

        if not inspect.isclass(value) or not issubclass(value, Evaluation):
            raise _evaluation_error(
                project,
                locator,
                code="EVAL_OBJECT_INVALID",
                message=(
                    f"Entrypoint '{locator.entrypoint}' must export an "
                    "Evaluation subclass."
                ),
            )

        try:
            inspect.signature(value).bind()
        except TypeError as exc:
            raise _evaluation_error(
                project,
                locator,
                code="EVAL_CONSTRUCTOR_INVALID",
                message=(
                    f"Evaluation '{locator.entrypoint}' must be constructible "
                    "without arguments."
                ),
            ) from exc

        methods = evaluation_case_methods(value)
        if not methods:
            raise _evaluation_error(
                project,
                locator,
                code="EVAL_CASES_MISSING",
                message=(
                    f"Evaluation '{locator.entrypoint}' has no eval_* methods."
                ),
            )
        for case_id, method in methods:
            if not inspect.iscoroutinefunction(method):
                raise _evaluation_error(
                    project,
                    locator,
                    code="EVAL_CASE_NOT_ASYNC",
                    message=f"Eval Case '{case_id}' must be async.",
                )
            parameters = tuple(inspect.signature(method).parameters.values())
            if len(parameters) != 2 or any(
                parameter.kind
                not in {
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                }
                for parameter in parameters
            ):
                raise _evaluation_error(
                    project,
                    locator,
                    code="EVAL_CASE_SIGNATURE_INVALID",
                    message=(
                        f"Eval Case '{case_id}' must accept exactly "
                        "'(self, case)'."
                    ),
                )

        return LoadedEvaluation(
            locator=locator,
            evaluation_type=value,
            case_ids=tuple(case_id for case_id, _ in methods),
        )


def _manifest_path(project: ProjectDefinition) -> str:
    return str(project.manifest_path or project.root)


def _evaluation_error(
    project: ProjectDefinition,
    locator: EvalSuiteLocator,
    *,
    code: str,
    message: str,
    field: str = "eval_suites.entrypoint",
    metadata: dict[str, Any] | None = None,
) -> ProjectLoadError:
    return ProjectLoadError(
        [
            ProjectDiagnostic(
                code=code,
                message=message,
                path=_manifest_path(project),
                field=field,
                entrypoint=locator.entrypoint,
                metadata=metadata or {},
            )
        ]
    )

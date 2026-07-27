from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import importlib
from pathlib import Path
import sys
import tomllib
from types import ModuleType
from typing import Any, Iterator

from pydantic import ValidationError

from autoagent.core.workflow import Workflow
from autoagent.project.errors import ProjectDiagnostic, ProjectLoadError
from autoagent.project.manifest import (
    ProjectManifest,
    ProjectMetadata,
    WorkflowLocator,
)


MANIFEST_FILENAME = "auto-agent.toml"


@dataclass(frozen=True, slots=True)
class LoadedWorkflow:
    """One Workflow resolved from a project manifest."""

    locator: WorkflowLocator
    workflow: Workflow


@dataclass(frozen=True, slots=True)
class ProjectDefinition:
    """Loaded project identity and its explicit Workflow definitions."""

    manifest_path: Path
    root: Path
    metadata: ProjectMetadata
    workflows: tuple[LoadedWorkflow, ...]

    def workflow_by_id(self, workflow_id: str) -> Workflow:
        for loaded in self.workflows:
            if loaded.workflow.id == workflow_id:
                return loaded.workflow
        raise KeyError(workflow_id)


def find_project_manifest(start: str | Path | None = None) -> Path:
    """Find the nearest ``auto-agent.toml`` from a path and its parents."""

    current = Path.cwd() if start is None else Path(start)
    current = current.expanduser().resolve()
    if current.is_file():
        if current.name == MANIFEST_FILENAME:
            return current
        current = current.parent

    for candidate_root in (current, *current.parents):
        candidate = candidate_root / MANIFEST_FILENAME
        if candidate.is_file():
            return candidate

    raise ProjectLoadError(
        [
            ProjectDiagnostic(
                code="PROJECT_MANIFEST_NOT_FOUND",
                message=(
                    f"Could not find {MANIFEST_FILENAME} from '{current}' "
                    "or any parent directory."
                ),
                path=str(current),
                hint=(
                    f"Create {MANIFEST_FILENAME} at the project root or pass its "
                    "path explicitly."
                ),
            )
        ]
    )


def load_project_manifest(path: str | Path) -> ProjectManifest:
    """Read and validate one explicit project manifest."""

    manifest_path = Path(path).expanduser().resolve()
    try:
        with manifest_path.open("rb") as stream:
            raw = tomllib.load(stream)
    except FileNotFoundError as exc:
        raise ProjectLoadError(
            [
                ProjectDiagnostic(
                    code="PROJECT_MANIFEST_NOT_FOUND",
                    message=f"Project manifest does not exist: {manifest_path}",
                    path=str(manifest_path),
                )
            ]
        ) from exc
    except (OSError, tomllib.TOMLDecodeError) as exc:
        metadata: dict[str, Any] = {}
        if isinstance(exc, tomllib.TOMLDecodeError):
            metadata = {
                "line": getattr(exc, "lineno", None),
                "column": getattr(exc, "colno", None),
            }
        raise ProjectLoadError(
            [
                ProjectDiagnostic(
                    code="PROJECT_MANIFEST_INVALID_TOML",
                    message=f"Cannot parse project manifest: {exc}",
                    path=str(manifest_path),
                    hint="Fix the TOML syntax and run the project check again.",
                    metadata=metadata,
                )
            ]
        ) from exc

    try:
        return ProjectManifest.model_validate(raw)
    except ValidationError as exc:
        diagnostics = [
            ProjectDiagnostic(
                code="PROJECT_MANIFEST_INVALID",
                message=error["msg"],
                path=str(manifest_path),
                field=".".join(str(item) for item in error["loc"]),
                hint="Update the manifest field to match schema_version 1.",
                metadata={"type": error["type"]},
            )
            for error in exc.errors(include_url=False, include_context=False)
        ]
        raise ProjectLoadError(diagnostics) from exc


class ProjectLoader:
    """Load only the Workflow objects explicitly listed by a project manifest."""

    def load(self, path: str | Path | None = None) -> ProjectDefinition:
        manifest_path = (
            find_project_manifest()
            if path is None
            else self._resolve_manifest_path(path)
        )
        manifest = load_project_manifest(manifest_path)
        root = manifest_path.parent

        loaded: list[LoadedWorkflow] = []
        diagnostics: list[ProjectDiagnostic] = []
        with _project_import_path(root):
            importlib.invalidate_caches()
            for locator in manifest.workflows:
                try:
                    workflow = self._load_workflow(locator)
                except ProjectLoadError as exc:
                    diagnostics.extend(
                        diagnostic.model_copy(update={"path": str(manifest_path)})
                        for diagnostic in exc.diagnostics
                    )
                    continue
                loaded.append(LoadedWorkflow(locator=locator, workflow=workflow))

        duplicate_ids = _duplicate_workflow_ids(loaded)
        for workflow_id in duplicate_ids:
            entrypoints = [
                item.locator.entrypoint
                for item in loaded
                if item.workflow.id == workflow_id
            ]
            diagnostics.append(
                ProjectDiagnostic(
                    code="WORKFLOW_ID_DUPLICATE",
                    message=f"Multiple project Workflows use id '{workflow_id}'.",
                    path=str(manifest_path),
                    field="workflows",
                    hint="Give every exported Workflow a unique stable id.",
                    metadata={"entrypoints": entrypoints},
                )
            )

        if diagnostics:
            raise ProjectLoadError(
                sorted(
                    diagnostics,
                    key=lambda item: (
                        item.entrypoint or "",
                        item.field or "",
                        item.code,
                        item.message,
                    ),
                )
            )

        return ProjectDefinition(
            manifest_path=manifest_path,
            root=root,
            metadata=manifest.project,
            workflows=tuple(loaded),
        )

    def _resolve_manifest_path(self, path: str | Path) -> Path:
        candidate = Path(path).expanduser().resolve()
        if candidate.is_dir():
            return find_project_manifest(candidate)
        return candidate

    def _load_workflow(self, locator: WorkflowLocator) -> Workflow:
        try:
            module = importlib.import_module(locator.module_name)
        except Exception as exc:
            raise ProjectLoadError(
                [
                    ProjectDiagnostic(
                        code="WORKFLOW_MODULE_IMPORT_FAILED",
                        message=(
                            f"Cannot import Workflow module "
                            f"'{locator.module_name}': {exc}"
                        ),
                        field="workflows.entrypoint",
                        entrypoint=locator.entrypoint,
                        hint=(
                            "Make the module importable from the project root and "
                            "fix any exception raised while importing it."
                        ),
                        metadata={"exception_type": type(exc).__name__},
                    )
                ]
            ) from exc

        try:
            value = _resolve_object(module, locator.object_path)
        except AttributeError as exc:
            raise ProjectLoadError(
                [
                    ProjectDiagnostic(
                        code="WORKFLOW_OBJECT_NOT_FOUND",
                        message=(
                            f"Module '{locator.module_name}' does not export "
                            f"'{locator.object_path}'."
                        ),
                        field="workflows.entrypoint",
                        entrypoint=locator.entrypoint,
                        hint="Export the Workflow object or correct the entrypoint.",
                    )
                ]
            ) from exc

        if not isinstance(value, Workflow):
            raise ProjectLoadError(
                [
                    ProjectDiagnostic(
                        code="WORKFLOW_OBJECT_INVALID",
                        message=(
                            f"Entrypoint '{locator.entrypoint}' resolved to "
                            f"{type(value).__name__}, not Workflow."
                        ),
                        field="workflows.entrypoint",
                        entrypoint=locator.entrypoint,
                        hint="Point the entrypoint at an exported Workflow object.",
                    )
                ]
            )
        return value


def _resolve_object(module: ModuleType, object_path: str) -> Any:
    value: Any = module
    for part in object_path.split("."):
        value = getattr(value, part)
    return value


def _duplicate_workflow_ids(loaded: list[LoadedWorkflow]) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for item in loaded:
        workflow_id = item.workflow.id
        if workflow_id in seen:
            duplicates.add(workflow_id)
        seen.add(workflow_id)
    return sorted(duplicates)


@contextmanager
def _project_import_path(root: Path) -> Iterator[None]:
    root_text = str(root)
    added = root_text not in sys.path
    if added:
        sys.path.insert(0, root_text)
    try:
        yield
    finally:
        if added:
            try:
                sys.path.remove(root_text)
            except ValueError:
                pass

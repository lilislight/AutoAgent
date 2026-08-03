from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
import importlib
import importlib.util
from pathlib import Path
import re
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
class _WorkflowFileLocator:
    """Explicit Python file and exported object used by standalone CLI mode."""

    path: Path
    object_path: str

    @property
    def entrypoint(self) -> str:
        return f"{self.path}:{self.object_path}"


@dataclass(frozen=True, slots=True)
class LoadedWorkflow:
    """One Workflow resolved from a manifest or standalone Python file."""

    locator: WorkflowLocator | _WorkflowFileLocator
    workflow: Workflow


@dataclass(frozen=True, slots=True)
class ProjectDefinition:
    """Loaded project identity and its explicit Workflow definitions."""

    manifest_path: Path | None
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
    """Load manifest projects or one explicitly selected Workflow file."""

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

    def load_workflow_file(
        self,
        path: str | Path,
        *,
        object_path: str = "workflow",
        project_root: str | Path | None = None,
    ) -> ProjectDefinition:
        """Load one exported Workflow without requiring a project manifest."""

        workflow_path = Path(path).expanduser().resolve()
        try:
            resolved_object_path = _validate_object_path(object_path)
        except ValueError as exc:
            raise ProjectLoadError(
                [
                    ProjectDiagnostic(
                        code="WORKFLOW_OBJECT_PATH_INVALID",
                        message=f"Invalid Workflow object path: {object_path}",
                        path=str(workflow_path),
                        entrypoint=f"{workflow_path}:{object_path}",
                        hint="Use a Python attribute path such as 'workflow'.",
                    )
                ]
            ) from exc
        locator = _WorkflowFileLocator(
            path=workflow_path,
            object_path=resolved_object_path,
        )
        if not workflow_path.is_file():
            raise ProjectLoadError(
                [
                    ProjectDiagnostic(
                        code="WORKFLOW_FILE_NOT_FOUND",
                        message=f"Workflow file does not exist: {workflow_path}",
                        path=str(workflow_path),
                        entrypoint=locator.entrypoint,
                        hint="Pass an existing Python file to --file.",
                    )
                ]
            )
        if workflow_path.suffix.lower() != ".py":
            raise ProjectLoadError(
                [
                    ProjectDiagnostic(
                        code="WORKFLOW_FILE_INVALID",
                        message="Standalone Workflow files must use the .py suffix.",
                        path=str(workflow_path),
                        entrypoint=locator.entrypoint,
                        hint="Export the Workflow from a Python source file.",
                    )
                ]
            )

        root = _standalone_project_root(project_root, workflow_path.parent)
        workflow = self._load_workflow_file(locator)
        return ProjectDefinition(
            manifest_path=None,
            root=root,
            metadata=ProjectMetadata(name=workflow_path.stem, version="1"),
            workflows=(LoadedWorkflow(locator=locator, workflow=workflow),),
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

    def _load_workflow_file(self, locator: _WorkflowFileLocator) -> Workflow:
        module_name = _file_module_name(locator.path)
        spec = importlib.util.spec_from_file_location(module_name, locator.path)
        if spec is None or spec.loader is None:
            raise ProjectLoadError(
                [
                    ProjectDiagnostic(
                        code="WORKFLOW_FILE_IMPORT_FAILED",
                        message=f"Cannot create an import spec for '{locator.path}'.",
                        path=str(locator.path),
                        entrypoint=locator.entrypoint,
                    )
                ]
            )

        module = importlib.util.module_from_spec(spec)
        previous_module = sys.modules.get(module_name)
        sys.modules[module_name] = module
        try:
            with _project_import_path(locator.path.parent):
                importlib.invalidate_caches()
                spec.loader.exec_module(module)
        except Exception as exc:
            if previous_module is None:
                sys.modules.pop(module_name, None)
            else:
                sys.modules[module_name] = previous_module
            raise ProjectLoadError(
                [
                    ProjectDiagnostic(
                        code="WORKFLOW_FILE_IMPORT_FAILED",
                        message=f"Cannot import Workflow file '{locator.path}': {exc}",
                        path=str(locator.path),
                        entrypoint=locator.entrypoint,
                        hint="Fix any exception raised while importing the file.",
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
                            f"Workflow file '{locator.path}' does not export "
                            f"'{locator.object_path}'."
                        ),
                        path=str(locator.path),
                        entrypoint=locator.entrypoint,
                        hint="Export the Workflow object or correct --object.",
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
                        path=str(locator.path),
                        entrypoint=locator.entrypoint,
                        hint="Point --object at an exported Workflow object.",
                    )
                ]
            )
        return value


def _resolve_object(module: ModuleType, object_path: str) -> Any:
    value: Any = module
    for part in object_path.split("."):
        value = getattr(value, part)
    return value


def _validate_object_path(value: str) -> str:
    """Reuse the manifest's object-path contract for standalone files."""

    return WorkflowLocator(entrypoint=f"workflow_file:{value}").object_path


def _standalone_project_root(
    value: str | Path | None,
    default: Path,
) -> Path:
    if value is None:
        return default.resolve()
    candidate = Path(value).expanduser().resolve()
    if candidate.is_dir():
        return candidate
    if candidate.is_file() and candidate.name == MANIFEST_FILENAME:
        return candidate.parent
    raise ProjectLoadError(
        [
            ProjectDiagnostic(
                code="PROJECT_ROOT_INVALID",
                message=(
                    "In --file mode, --project must be a directory or an "
                    f"explicit {MANIFEST_FILENAME} path."
                ),
                path=str(candidate),
                hint="Pass the directory whose .env and relative outputs should be used.",
            )
        ]
    )


def _file_module_name(path: Path) -> str:
    safe_stem = re.sub(r"[^A-Za-z0-9_]", "_", path.stem)
    digest = sha256(str(path).encode("utf-8")).hexdigest()[:16]
    return f"_autoagent_workflow_{safe_stem}_{digest}"


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

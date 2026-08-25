"""Load the Workflow objects explicitly named by a V2 project manifest."""

from __future__ import annotations

import importlib
import sys
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Iterator

from autoagent.core.workflow import Workflow

from .errors import HostDiagnostic, ProjectLoadError
from .manifest import (
    ProjectManifest,
    WorkflowLocator,
    load_project_manifest,
    resolve_manifest_path,
)


_IMPORT_LOCK = threading.RLock()


@dataclass(frozen=True, slots=True)
class LoadedWorkflow:
    """One manifest locator paired with its resolved Workflow object."""

    locator: WorkflowLocator
    workflow: Workflow


@dataclass(frozen=True, slots=True)
class LoadedProject:
    """A validated manifest and all explicitly exported Workflow definitions."""

    manifest_path: Path
    root: Path
    manifest: ProjectManifest
    workflows: tuple[LoadedWorkflow, ...]

    def workflow_by_id(self, workflow_id: str) -> Workflow:
        for loaded in self.workflows:
            if loaded.workflow.id == workflow_id:
                return loaded.workflow
        raise KeyError(workflow_id)


class ProjectLoader:
    """Resolve one ``autoagent.toml`` without compiling or running Workflows."""

    def load(self, path: str | Path | None = None) -> LoadedProject:
        manifest_path = resolve_manifest_path(path)
        manifest = load_project_manifest(manifest_path)
        root = manifest_path.parent
        loaded: list[LoadedWorkflow] = []
        diagnostics: list[HostDiagnostic] = []

        module_names = tuple(item.module_name for item in manifest.workflows)
        with _project_import_path(root):
            _guard_project_module_namespaces(
                root,
                module_names,
                manifest_path=manifest_path,
            )
            importlib.invalidate_caches()
            for locator in manifest.workflows:
                try:
                    workflow = _load_workflow(locator)
                except ProjectLoadError as error:
                    diagnostics.extend(
                        item.model_copy(update={"path": str(manifest_path)})
                        for item in error.diagnostics
                    )
                else:
                    loaded.append(LoadedWorkflow(locator, workflow))

        for workflow_id in _duplicate_workflow_ids(loaded):
            entrypoints = [
                item.locator.entrypoint
                for item in loaded
                if item.workflow.id == workflow_id
            ]
            diagnostics.append(
                HostDiagnostic(
                    code="WORKFLOW_ID_DUPLICATE",
                    message=f"Multiple project Workflows use id {workflow_id!r}.",
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
        return LoadedProject(
            manifest_path=manifest_path,
            root=root,
            manifest=manifest,
            workflows=tuple(loaded),
        )


def _load_workflow(locator: WorkflowLocator) -> Workflow:
    try:
        module = importlib.import_module(locator.module_name)
    except (Exception, SystemExit) as error:
        raise ProjectLoadError(
            [
                HostDiagnostic(
                    code="WORKFLOW_MODULE_IMPORT_FAILED",
                    message=(
                        f"Cannot import Workflow module {locator.module_name!r}: "
                        f"{error}"
                    ),
                    field="workflows.entrypoint",
                    entrypoint=locator.entrypoint,
                    metadata={"exception_type": type(error).__name__},
                )
            ]
        ) from error

    try:
        value = _resolve_object(module, locator.object_path)
    except AttributeError as error:
        raise ProjectLoadError(
            [
                HostDiagnostic(
                    code="WORKFLOW_OBJECT_NOT_FOUND",
                    message=(
                        f"Module {locator.module_name!r} does not export "
                        f"{locator.object_path!r}."
                    ),
                    field="workflows.entrypoint",
                    entrypoint=locator.entrypoint,
                )
            ]
        ) from error

    if not isinstance(value, Workflow):
        raise ProjectLoadError(
            [
                HostDiagnostic(
                    code="WORKFLOW_OBJECT_INVALID",
                    message=(
                        f"Entrypoint {locator.entrypoint!r} resolved to "
                        f"{type(value).__name__}, not Workflow."
                    ),
                    field="workflows.entrypoint",
                    entrypoint=locator.entrypoint,
                )
            ]
        )
    return value


def _resolve_object(module: ModuleType, object_path: str) -> object:
    value: object = module
    for segment in object_path.split("."):
        value = getattr(value, segment)
    return value


def _duplicate_workflow_ids(loaded: list[LoadedWorkflow]) -> tuple[str, ...]:
    counts: dict[str, int] = {}
    for item in loaded:
        counts[item.workflow.id] = counts.get(item.workflow.id, 0) + 1
    return tuple(sorted(key for key, count in counts.items() if count > 1))


@contextmanager
def _project_import_path(root: Path) -> Iterator[None]:
    """Expose the project root only while its definitions are imported."""

    root_text = str(root)
    with _IMPORT_LOCK:
        original = list(sys.path)
        sys.path.insert(0, root_text)
        try:
            yield
        finally:
            sys.path[:] = original


def _guard_project_module_namespaces(
    root: Path,
    module_names: tuple[str, ...],
    *,
    manifest_path: Path,
) -> None:
    """Reuse one project's modules and reject ambiguous cross-project aliases."""

    roots = frozenset(name.partition(".")[0] for name in module_names)
    conflicts: dict[str, list[str]] = {}
    for namespace in roots:
        origins: list[str] = []
        for name, module in tuple(sys.modules.items()):
            if name != namespace and not name.startswith(f"{namespace}."):
                continue
            source = getattr(module, "__file__", None)
            if source is None:
                paths = getattr(module, "__path__", ())
                candidates = tuple(str(item) for item in paths)
            else:
                candidates = (str(source),)
            for candidate in candidates:
                try:
                    belongs = Path(candidate).expanduser().resolve().is_relative_to(root)
                except (OSError, RuntimeError):
                    belongs = False
                if not belongs:
                    origins.append(candidate)
        if origins:
            conflicts[namespace] = sorted(set(origins))
    if conflicts:
        raise ProjectLoadError(
            [
                HostDiagnostic(
                    code="WORKFLOW_MODULE_CONFLICT",
                    message=(
                        "Workflow module namespace is already loaded from "
                        "another project."
                    ),
                    path=str(manifest_path),
                    field="workflows.entrypoint",
                    hint=(
                        "Use a project-unique Python package name or run each "
                        "Host in a separate process."
                    ),
                    metadata={"namespaces": conflicts},
                )
            ]
        )

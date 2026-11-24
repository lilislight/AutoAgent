"""Explicit TOML entries or decorated Workflow discovery, plus project .env."""

from __future__ import annotations

import ast
import importlib
import inspect
import os
import sys
import threading
import tomllib
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import dotenv_values

from autoagent import Workflow
from autoagent.core.compiler import WorkflowCompiler

from .models import ProjectCheck, ProjectError

MANIFEST_FILENAME = "auto-agent.toml"
_IMPORT_LOCK = threading.RLock()
_ENV_USERS = {}
_EXCLUDED = {
    "v1",
    "tests",
    "test",
    "build",
    "dist",
    "node_modules",
    "venv",
    "__pycache__",
}


def workflow(factory=None):
    """Mark a zero-argument Workflow factory; importing never invokes it."""

    def decorate(value):
        if not inspect.isfunction(value) or inspect.iscoroutinefunction(value):
            raise TypeError("@workflow requires a synchronous Workflow factory.")
        if inspect.signature(value).parameters:
            raise TypeError("A discovered Workflow factory must take no arguments.")
        value.__autoagent_workflow__ = True
        return value

    return decorate if factory is None else decorate(factory)


def load_project_environment(
    project_root, *, env_file=None, use_env_file=True, environ=None
):
    """Merge project .env then process environment, as in V1."""
    root = Path(project_root).expanduser().resolve()
    process = dict(os.environ if environ is None else environ)
    if not use_env_file:
        return process
    selected = Path(env_file).expanduser() if env_file is not None else root / ".env"
    if not selected.is_absolute():
        selected = root / selected
    if env_file is not None and not selected.is_file():
        raise ProjectError(f"Environment file does not exist: {selected}")
    # Interpolation observes the supplied process snapshot without overwriting
    # existing process variables. Values with no assignment are omitted.
    with environment_scope(process):
        values = {
            key: value
            for key, value in dotenv_values(selected).items()
            if value is not None
        }
    values.update(process)
    return values


@contextmanager
def environment_scope(environment):
    """Expose missing .env values for imports and the Host lifetime, then release."""
    acquired = []
    try:
        with _IMPORT_LOCK:
            for key, value in environment.items():
                if key in _ENV_USERS:
                    saved, count = _ENV_USERS[key]
                    if saved != value:
                        raise ProjectError(
                            f"Active projects disagree on environment variable {key}."
                        )
                    _ENV_USERS[key] = (saved, count + 1)
                    acquired.append(key)
                elif key not in os.environ:
                    os.environ[key] = value
                    _ENV_USERS[key] = (value, 1)
                    acquired.append(key)
        yield
    finally:
        with _IMPORT_LOCK:
            for key in reversed(acquired):
                value, count = _ENV_USERS[key]
                if count > 1:
                    _ENV_USERS[key] = (value, count - 1)
                else:
                    del _ENV_USERS[key]
                    if os.environ.get(key) == value:
                        del os.environ[key]


@dataclass(frozen=True)
class Project:
    root: Path
    name: str
    entrypoints: tuple[str, ...] | None = None
    lifespan_entrypoint: str | None = None
    evaluation_entrypoints: tuple[str, ...] = ()
    source_dirs: tuple[Path | None, ...] = ()
    manifest_path: Path | None = None
    origins: dict = field(default_factory=dict, init=False, compare=False, repr=False)

    @classmethod
    def find(cls, path="."):
        selected = Path(path).expanduser().resolve()
        if selected.is_file():
            manifest, root = selected, selected.parent
        else:
            if not selected.is_dir():
                raise ProjectError(f"Project directory does not exist: {selected}")
            manifest = next(
                (
                    parent / MANIFEST_FILENAME
                    for parent in (selected, *selected.parents)
                    if (parent / MANIFEST_FILENAME).is_file()
                ),
                None,
            )
            root = manifest.parent if manifest else selected
        if manifest is None:
            return cls(root, root.name)
        try:
            with manifest.open("rb") as stream:
                raw = tomllib.load(stream)
        except (OSError, tomllib.TOMLDecodeError) as exc:
            raise ProjectError(
                f"Cannot read project manifest {manifest}: {exc}"
            ) from exc
        if raw.get("schema_version") != 1 or type(raw.get("schema_version")) is not int:
            raise ProjectError("Project schema_version must be 1.")
        if set(raw) - {"schema_version", "project", "workflows", "evaluations"}:
            raise ProjectError("Unknown project manifest fields.")
        metadata = raw.get("project", {})
        if not isinstance(metadata, dict) or set(metadata) - {
            "name",
            "version",
            "description",
            "lifespan",
        }:
            raise ProjectError("Invalid project metadata.")
        entries = raw.get("workflows")
        source_dirs = ()
        if entries is not None:
            if not isinstance(entries, list):
                raise ProjectError("workflows must be an array.")
            if any(
                not isinstance(item, dict) or "entrypoint" not in item or set(item) - {"entrypoint", "source_dir"}
                for item in entries
            ):
                raise ProjectError("Each Workflow entry requires entrypoint and optional source_dir.")
            roots = []
            for item in entries:
                source = item.get("source_dir")
                if source is not None and (not isinstance(source, str) or not source.strip()):
                    raise ProjectError("source_dir must be a nonempty path string.")
                path = Path(source).expanduser() if source is not None else None
                roots.append((path if path.is_absolute() else root / path).resolve() if path is not None else None)
            source_dirs = tuple(roots)
            entries = tuple(validate_entrypoint(item["entrypoint"]) for item in entries)
            if len(entries) != len(set(zip((str(path or root) for path in source_dirs), entries))):
                raise ProjectError("Duplicate Workflow entrypoints.")
        evaluations = raw.get('evaluations', [])
        if not isinstance(evaluations, list) or any(not isinstance(item, dict) or set(item) != {'entrypoint'} for item in evaluations):
            raise ProjectError('evaluations must be an array of entrypoint tables.')
        evaluations = tuple(validate_entrypoint(item['entrypoint']) for item in evaluations)
        if len(evaluations) != len(set(evaluations)):
            raise ProjectError('Duplicate Evaluation entrypoints.')
        lifespan = metadata.get("lifespan")
        if lifespan is not None:
            lifespan = validate_entrypoint(lifespan)
        name = metadata.get("name", root.name)
        if not isinstance(name, str) or not name.strip():
            raise ProjectError("Project name cannot be empty.")
        return cls(root, name, entries, lifespan, evaluations, source_dirs, manifest)

    def load(self, environment=None):
        with _IMPORT_LOCK, import_scope(self.root):
            if self.entrypoints is not None:
                from .source_loading import resolve_source_entrypoint
                roots = self.source_dirs or (None,) * len(self.entrypoints)
                definitions = [(resolve_source_entrypoint(entry, source) if source is not None else
                    resolve_entrypoint(entry, self.root), source or self.root, entry)
                    for entry, source in zip(self.entrypoints, roots)]
            else:
                definitions = []
                for path in source_files(self.root):
                    if not has_workflow_decorator(path):
                        continue
                    module_name = module_for(path, self.root)
                    module = import_project_module(module_name, self.root)
                    definitions.extend((value, self.root, module_name + ":" + value.__name__)
                        for value in vars(module).values()
                        if getattr(value, "__autoagent_workflow__", False)
                        and getattr(value, "__module__", None) == module.__name__)
            seen, workflows = set(), []
            self.origins.clear()
            for value, source, entry in definitions:
                if id(value) in seen:
                    continue
                seen.add(id(value))
                if isinstance(value, Workflow):
                    built = value
                elif inspect.isfunction(value) and not inspect.iscoroutinefunction(
                    value
                ):
                    built = value()
                else:
                    raise ProjectError(
                        "Workflow entry must be a Workflow or synchronous factory."
                    )
                if not isinstance(built, Workflow):
                    raise ProjectError(
                        f"Workflow factory returned {type(built).__name__}."
                    )
                if any(existing.id == built.id for existing in workflows):
                    raise ProjectError(f"Duplicate Workflow id: {built.id}")
                workflows.append(built)
                self.origins[built.id] = (source, entry)
                pending, nested_seen = [built], set()
                while pending:
                    current = pending.pop()
                    if id(current) in nested_seen:
                        continue
                    nested_seen.add(id(current))
                    self.origins.setdefault(current.id, (source, entry))
                    pending.extend(child for node in current.nodes
                        if isinstance((child := getattr(node.executable, "workflow", None)), Workflow))
                    pending.extend(child.workflow for child in current.sub_workflows)
            lifespan = (
                resolve_entrypoint(self.lifespan_entrypoint, self.root)
                if self.lifespan_entrypoint
                else None
            )
            return tuple(workflows), lifespan

    def check(self, environment):
        with environment_scope(environment):
            workflows, _ = self.load(environment)
            return ProjectCheck(
                self.name,
                tuple(WorkflowCompiler().compile(value) for value in workflows),
            )


def validate_entrypoint(value):
    if not isinstance(value, str) or value.count(":") != 1:
        raise ProjectError("Entrypoint must use module:object syntax.")
    module, attribute = value.split(":")
    if any(
        not part.isidentifier() for part in (*module.split("."), *attribute.split("."))
    ):
        raise ProjectError(f"Invalid entrypoint: {value}")
    return value


@contextmanager
def import_scope(root):
    paths = [str(root / "src"), str(root)] if (root / "src").is_dir() else [str(root)]
    old = list(sys.path)
    sys.path[:0] = paths
    try:
        importlib.invalidate_caches()
        yield
    finally:
        sys.path[:] = old


def import_project_module(name, root):
    try:
        # Never silently reuse another project's module with the same name.
        for prefix in (
            ".".join(name.split(".")[:i]) for i in range(1, len(name.split(".")) + 1)
        ):
            cached = sys.modules.get(prefix)
            origins = (
                [cached.__file__]
                if getattr(cached, "__file__", None)
                else list(getattr(cached, "__path__", ()))
            )
            if (
                cached is not None
                and origins
                and not all(Path(p).resolve().is_relative_to(root) for p in origins)
            ):
                raise ProjectError(
                    f"Module {prefix} is already loaded from another project."
                )
        module = importlib.import_module(name)
        origins = (
            [module.__file__]
            if getattr(module, "__file__", None)
            else list(getattr(module, "__path__", ()))
        )
        if not origins or not all(
            Path(p).resolve().is_relative_to(root) for p in origins
        ):
            raise ProjectError(f"Project module {name} resolves outside {root}.")
        return module
    except ProjectError:
        raise
    except Exception as exc:
        raise ProjectError(f"Cannot import project module {name}: {exc}") from exc


def resolve_entrypoint(value, root):
    module, attribute = validate_entrypoint(value).split(":")
    resolved = import_project_module(module, root)
    try:
        for part in attribute.split("."):
            resolved = getattr(resolved, part)
    except AttributeError as exc:
        raise ProjectError(f"Entrypoint does not exist: {value}") from exc
    return resolved


def source_files(root):
    for directory, names, files in os.walk(root, followlinks=False):
        names[:] = sorted(
            name for name in names if not name.startswith(".") and name not in _EXCLUDED
        )
        for name in sorted(files):
            path = Path(directory) / name
            if name.endswith(".py") and not path.is_symlink():
                yield path


def module_for(path, root):
    base = root / "src" if path.is_relative_to(root / "src") else root
    parts = list(path.relative_to(base).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    if not parts or any(not value.isidentifier() for value in parts):
        raise ProjectError(f"Decorated module has no importable name: {path}")
    return ".".join(parts)


def has_workflow_decorator(path):
    try:
        tree = ast.parse(path.read_bytes(), filename=str(path))
    except (SyntaxError, UnicodeError) as exc:
        raise ProjectError(f"Cannot scan Python source {path}: {exc}") from exc
    aliases = {"workflow"}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "autoagent.host":
            aliases.update(
                alias.asname or alias.name
                for alias in node.names
                if alias.name == "workflow"
            )
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for decorator in node.decorator_list:
                value = decorator.func if isinstance(decorator, ast.Call) else decorator
                if (
                    isinstance(value, ast.Name)
                    and value.id in aliases
                    or isinstance(value, ast.Attribute)
                    and value.attr == "workflow"
                ):
                    return True
    return False

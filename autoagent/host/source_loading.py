"""In-memory Python source generations; no copied files or persistent registry."""

import builtins
import hashlib
import importlib.abc
import importlib.util
import sys
import inspect
from dataclasses import replace
from pathlib import Path
from threading import RLock

from .models import ProjectError

_lock = RLock()


class _Generation(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def __init__(self, root, sources, digest):
        self.root, self.sources = root, sources
        self.prefix = "_autoagent_source_" + digest

    def find_spec(self, fullname, path=None, target=None):
        if fullname == self.prefix:
            return importlib.util.spec_from_loader(fullname, self, is_package=True)
        if not fullname.startswith(self.prefix + "."):
            return None
        relative = fullname[len(self.prefix) + 1:]
        if relative in self.sources:
            return importlib.util.spec_from_loader(fullname, self, is_package=self.sources[relative][2])
        if any(name.startswith(relative + ".") for name in self.sources):
            return importlib.util.spec_from_loader(fullname, self, is_package=True)
        return None

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        name = module.__name__[len(self.prefix) + 1:]
        record = self.sources.get(name)
        if record is None:
            module.__path__ = []
            return
        path, source, package = record
        module.__file__ = str(path)
        if package:
            # Only this generation's finder may load local code. Falling back
            # to the live directory would expose files added after registration.
            module.__path__ = []
        module.__builtins__ = {**vars(builtins), "__import__": self.import_module}
        exec(compile(source, str(path), "exec"), module.__dict__)

    def import_module(self, name, globals=None, locals=None, fromlist=(), level=0):
        # Local absolute and relative imports stay on the same captured source
        # generation, including imports deferred until an Operator executes.
        top = name.split(".", 1)[0]
        local = top != "autoagent" and (top in self.sources or any(
            value.startswith(top + ".") for value in self.sources))
        if level == 0 and local:
            qualified = self.prefix + "." + name
            result = builtins.__import__(qualified, globals, locals, fromlist, 0)
            return result if fromlist else sys.modules[self.prefix + "." + top]
        return builtins.__import__(name, globals, locals, fromlist, level)


def capture_sources(root):
    from .project import source_files, module_for
    root = Path(root).resolve()
    sources = {}
    digest = hashlib.sha256(str(root).encode())
    for path in source_files(root):
        try:
            name = module_for(path, root)
        except ProjectError:
            continue
        source = path.read_bytes()
        sources[name] = (path, source, path.name == "__init__.py")
        digest.update(str(path.relative_to(root)).encode() + b"\0" + source + b"\0")
    return sources, digest.hexdigest()


def resolve_source_entrypoint(entrypoint, root, *, expected_digest=None):
    """Capture current Python bytes and load a fresh, deterministic module tree."""
    from .project import validate_entrypoint
    root = Path(root).expanduser().resolve()
    if not root.is_dir():
        raise ProjectError(f"Workflow source directory does not exist: {root}")
    module, attribute = validate_entrypoint(entrypoint).split(":")
    with _lock:
        sources, digest = capture_sources(root)
        if module not in sources:
            raise ProjectError(f"Project module {module} does not exist in source directory {root}.")
        if expected_digest is not None and digest != expected_digest:
            raise ProjectError("Workflow source changed after authorization; register its current contents again.")
        generation = _Generation(root, sources, digest)
        existing = next((finder for finder in sys.meta_path if isinstance(finder, _Generation)
            and finder.prefix == generation.prefix), None)
        if existing is None:
            sys.meta_path.insert(0, generation)
        else:
            generation = existing
        try:
            value = importlib.import_module(generation.prefix + "." + module)
            for name in attribute.split("."):
                value = getattr(value, name)
            def build():
                from autoagent import Workflow
                if isinstance(value, Workflow):
                    workflow = value
                elif inspect.isfunction(value) and not inspect.iscoroutinefunction(value) and not inspect.signature(value).parameters:
                    workflow = value()
                else:
                    raise ProjectError("Workflow entry must be a Workflow or a zero-argument synchronous factory.")
                if not isinstance(workflow, Workflow):
                    raise ProjectError("Workflow factory must return a Workflow.")
                return _versioned(workflow, generation.prefix.removeprefix("_autoagent_source_"), {})
            return build
        except Exception as error:
            # A failed import must not keep partially initialized modules.
            if existing is None:
                for name in tuple(sys.modules):
                    if name == generation.prefix or name.startswith(generation.prefix + "."):
                        sys.modules.pop(name, None)
                sys.meta_path.remove(generation)
            raise ProjectError(f"Cannot load Workflow entrypoint {entrypoint}: {error}") from error


def _versioned(workflow, digest, seen):
    """Code changes produce new executable identities without changing Node/Edge IDs."""
    from autoagent import Operator, Workflow
    if id(workflow) in seen:
        return seen[id(workflow)]
    result = replace(workflow, version=str(workflow.version) + "+source." + digest[:16], nodes=[], sub_workflows=[])
    seen[id(workflow)] = result
    for node in workflow.nodes:
        executable = node.executable
        if isinstance(executable, Operator):
            executable = Operator(executable.handler, id=executable.id, version=executable.version + "+source." + digest)
        elif isinstance(getattr(executable, "workflow", None), Workflow):
            executable = replace(executable, workflow=_versioned(executable.workflow, digest, seen))
        result.nodes.append(replace(node, executable=executable))
    result.sub_workflows.extend(replace(child, workflow=_versioned(child.workflow, digest, seen))
        for child in workflow.sub_workflows)
    return result

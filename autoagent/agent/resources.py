"""Versioned, application-owned resources; registration never closes clients."""

from dataclasses import dataclass, replace
from threading import RLock
from uuid import uuid4


@dataclass(frozen=True)
class AgentResource:
    name: str
    version: str
    value: object
    resource_id: str
    workflows: frozenset[str] = frozenset()
    policy: object = None
    permission_revision: int = 0
    dependencies: tuple = ()


def workflow_scope(values):
    if isinstance(values, str):
        raise TypeError("workflows must be a collection of explicit Workflow IDs.")
    result = frozenset(values)
    if any(not isinstance(value, str) or not value.strip() or value == "*" for value in result):
        raise ValueError("workflows must contain explicit nonempty Workflow IDs.")
    return result


class AgentResourceRegistry:
    """Publish complete bindings atomically and retain explicitly versioned resources.

    Application lifespan owns resource cleanup. A replacement requires a new
    version, including when an implementation changes without a schema change.
    Unregistering prevents new resolutions; already borrowed calls may finish.
    """

    def __init__(self, *, namespace="host"):
        self.namespace = namespace
        self._lock = RLock()
        self._current = {}
        self._versions = {}
        self._published = set()
        self._disabled = set()
        self._owners = {}
        self._sources = {}
        self._revision = 0
        self._source_claims = set()
        self._source_access = {}
        self._source_overrides = {}

    def claim_source(self, source, *, workflows=(), policy=None):
        """Reserve a source name for one active synchronization lifespan."""
        if not isinstance(source, str) or not source.strip():
            raise ValueError("A resource source needs a nonempty name.")
        scope = workflow_scope(workflows)
        if policy is not None and not callable(policy):
            raise TypeError("Resource policy must be callable.")
        with self._lock:
            if source in self._source_claims:
                raise ValueError(f"Resource source is already active: {source!r}.")
            self._source_claims.add(source)
            self._source_access[source] = (scope, policy)

    def release_source(self, source):
        with self._lock:
            self.remove_source(source)
            self._source_claims.discard(source)
            self._source_access.pop(source, None)
            for key in tuple(self._source_overrides):
                if key[0] == source:
                    del self._source_overrides[key]

    @property
    def revision(self):
        with self._lock:
            return self._revision

    def _register(self, kind, name, value, version, replace, *, workflows=(), policy=None):
        if not isinstance(name, str) or not name.strip() or not isinstance(version, str) or not version.strip():
            raise ValueError("Resource names and versions must be nonempty strings.")
        scope = workflow_scope(workflows)
        if policy is not None and not callable(policy):
            raise TypeError("Resource policy must be callable.")
        with self._lock:
            key = (kind, name)
            if key in self._owners:
                raise ValueError("A synchronized resource must be updated through its source.")
            if key in self._current and not replace:
                raise ValueError(f"Resource already registered: {kind}/{name}.")
            identity = (kind, name, version)
            if identity in self._published:
                raise ValueError("A resource version cannot be rebound; use a new version.")
            entry = AgentResource(name, version, value, f"{self.namespace}/{kind}/{name}", scope, policy)
            self._versions[identity] = entry
            self._published.add(identity)
            self._current[key] = entry
            self._disabled.discard(key)
            self._revision += 1
            return entry

    def replace_source(self, source, resources):
        """Atomically reconcile one source's complete Tool/Skill catalog.

        Values map (kind, name) to (content identity, executable). Unchanged
        bindings retain their version; changed or reintroduced bindings receive
        a fresh version, even when content returns to an earlier value.
        """
        if not isinstance(source, str) or not source.strip():
            raise ValueError("A resource source needs a nonempty name.")
        replacement = dict(resources)
        from .skills import Skill
        from .tools import ExecutableTool
        for (kind, name), (fingerprint, value) in replacement.items():
            if kind not in ("tool", "skill") or not isinstance(name, str) or not name.strip():
                raise ValueError("Sources publish named Tools or Skills.")
            if not isinstance(fingerprint, str) or not fingerprint:
                raise ValueError("Source content identities must be nonempty strings.")
            if kind == "tool" and (not isinstance(value, ExecutableTool) or value.definition.name != name):
                raise TypeError("Source Tool names must match executable definitions.")
            if kind == "skill" and (not isinstance(value, Skill) or value.metadata.name != name
                                    or value.snapshot_revision is None):
                raise TypeError("Source Skills must be named, frozen snapshots.")
        with self._lock:
            for key in replacement:
                if key in self._current and self._owners.get(key) != source:
                    raise ValueError(f"Resource source collides with an existing binding: {key!r}.")
            previous = self._sources.get(source, {})
            changed = set(previous) != set(replacement)
            for key in previous.keys() - replacement.keys():
                self._current.pop(key, None)
                self._owners.pop(key, None)
                self._disabled.discard(key)
                self._drop_versions(key)
            for key, (fingerprint, value) in replacement.items():
                if previous.get(key) == fingerprint and key in self._current:
                    continue
                changed = True
                self._drop_versions(key)
                version = uuid4().hex
                old = self._current.get(key)
                scope, policy = self._source_access.get(source, (frozenset(), None))
                override = self._source_overrides.get((source, key), {})
                entry = AgentResource(key[1], version, value, f"{self.namespace}/source/{source}/{key[0]}/{key[1]}",
                    old.workflows if old else override.get("workflows", scope),
                    old.policy if old else override.get("policy", policy),
                    old.permission_revision if old else 0)
                if override.get("disabled"):
                    self._disabled.add(key)
                self._current[key] = entry
                self._owners[key] = source
                self._versions[(*key, version)] = entry
                self._published.add((*key, version))
            self._sources[source] = {key: value[0] for key, value in replacement.items()}
            if changed:
                self._revision += 1
            return tuple(self._current[key] for key in replacement)

    def remove_source(self, source):
        self.replace_source(source, {})
        with self._lock:
            self._sources.pop(source, None)

    def _drop_versions(self, key):
        for identity in tuple(self._versions):
            if identity[:2] == key:
                del self._versions[identity]

    def register_provider(self, name, provider, *, version="1", replace=False):
        if any(not callable(getattr(provider, method, None)) for method in ("ainvoke", "astream")):
            raise TypeError("Provider must implement ainvoke and astream.")
        return self._register("provider", name, provider, version, replace)

    def register_tool(self, tool, *, version="1", replace=False, workflows=(), policy=None):
        from .tools import ExecutableTool, Tool
        if not isinstance(tool, ExecutableTool):
            tool = Tool(tool)
        return self._register("tool", tool.definition.name, tool, version, replace, workflows=workflows, policy=policy)

    def register_skill(self, skill, *, version=None, replace=False, workflows=(), policy=None):
        from .skills import Skill
        if not isinstance(skill, Skill):
            raise TypeError("Expected Skill.")
        frozen = skill.freeze()
        document = frozen.load()
        return self._register("skill", frozen.metadata.name, frozen,
                              version or document.snapshot_revision or document.revision, replace,
                              workflows=workflows, policy=policy)

    def _update_access(self, kind, name, **changes):
        with self._lock:
            key = (kind, name)
            if kind not in {"tool", "skill"} or key not in self._current:
                raise KeyError(key)
            current = self._current[key]
            source = self._owners.get(key)
            if source is not None:
                self._source_overrides.setdefault((source, key), {}).update(changes)
            entry = replace(current, permission_revision=current.permission_revision + 1, **changes)
            self._current[key] = entry
            self._versions[(*key, entry.version)] = entry
            self._revision += 1

    def set_workflows(self, kind, name, workflows):
        """Change one resource's live grants without changing its implementation."""
        self._update_access(kind, name, workflows=workflow_scope(workflows))

    def set_policy(self, kind, name, policy):
        """Change the resource's live policy without replacing its executable."""
        if policy is not None and not callable(policy):
            raise TypeError("Resource policy must be callable.")
        self._update_access(kind, name, policy=policy)

    def set_source_workflows(self, source, workflows):
        """Update existing and future grants for one synchronized source atomically."""
        scope = workflow_scope(workflows)
        with self._lock:
            if source not in self._source_access:
                raise KeyError(source)
            self._source_access[source] = (scope, self._source_access[source][1])
            for (owner, _), changes in self._source_overrides.items():
                if owner == source:
                    changes.pop("workflows", None)
            for key, owner in self._owners.items():
                if owner == source:
                    self._update_access(*key, workflows=scope)
            self._revision += 1

    def register_policy(self, name, policy, *, version="1", replace=False):
        if not callable(policy):
            raise TypeError("Permission policy must be callable.")
        return self._register("policy", name, policy, version, replace)

    def unregister(self, kind, name):
        with self._lock:
            if (kind, name) in self._owners:
                raise ValueError("A synchronized resource must be removed through its source.")
            entry = self._current.pop((kind, name), None)
            if entry is None:
                raise KeyError((kind, name))
            # Active, already borrowed objects remain valid. Future resolution,
            # including a restored execution snapshot, must fail explicitly.
            for identity in tuple(self._versions):
                if identity[:2] == (kind, name):
                    del self._versions[identity]
            self._disabled.discard((kind, name))
            self._revision += 1

    def set_enabled(self, kind, name, enabled):
        """Hide or republish a resource without changing its implementation version."""
        if type(enabled) is not bool:
            raise TypeError("enabled must be a boolean.")
        with self._lock:
            if (kind, name) not in self._current:
                raise KeyError((kind, name))
            if enabled:
                self._disabled.discard((kind, name))
            else:
                self._disabled.add((kind, name))
            source = self._owners.get((kind, name))
            if source is not None:
                self._source_overrides.setdefault((source, (kind, name)), {})["disabled"] = not enabled
            self._revision += 1

    def get(self, kind, name, *, version=None):
        with self._lock:
            entry = (self._current.get((kind, name)) if version is None else
                     self._versions.get((kind, name, version)))
            if entry is None or (kind, name) in self._disabled:
                raise KeyError(f"Agent resource is unavailable: {kind}/{name}@{version or 'current'}.")
            return entry

    def entries(self, kind, *, include_disabled=False):
        with self._lock:
            return tuple(entry for (category, name), entry in self._current.items()
                if category == kind and (include_disabled or (category, name) not in self._disabled))

    def has(self, kind, name):
        with self._lock:
            return (kind, name) in self._current and (kind, name) not in self._disabled

    def catalog(self, workflow_id=None):
        """Capture Tool and Skill bindings together under one publication lock."""
        with self._lock:
            return tuple(tuple(entry for entry in self.entries(kind)
                if workflow_id is None or workflow_id in entry.workflows) for kind in ("tool", "skill"))


__all__ = ["AgentResource", "AgentResourceRegistry"]

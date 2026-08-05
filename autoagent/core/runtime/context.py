from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from autoagent.core.runtime.output import OutputView


class RuntimeContext(BaseModel):
    """User-controlled context persisted by RuntimeStore.

    RuntimeContext is intentionally small. Framework execution state such as
    node transitions, waiting executions, and node outputs must not be stored
    here. `data` is the mutable space exposed to mapping/binding hooks.
    `metadata` is for caller/tooling annotations that should be stored with the
    same lifetime but should not drive scheduler decisions.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    data: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Mutable user data. OutputBindingContext may write it; "
            "InputMappingContext and ConditionContext receive read-only copies."
        ),
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Auxiliary user/tooling metadata with the same lifetime.",
    )
    revision: int = Field(default=0, ge=0)
    path_revisions: dict[str, int] = Field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        return {
            "data": deepcopy(self.data),
            "metadata": deepcopy(self.metadata),
            "revision": self.revision,
            "path_revisions": dict(self.path_revisions),
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> RuntimeContext:
        return cls(
            data=dict(record.get("data", {})),
            metadata=dict(record.get("metadata", {})),
            revision=int(record.get("revision", 0)),
            path_revisions={
                str(path): int(revision)
                for path, revision in record.get("path_revisions", {}).items()
            },
        )

    def commit_isolated(
        self,
        working: RuntimeContext,
        *,
        base_revision: int,
        node_id: str,
    ) -> tuple[str, ...]:
        """Atomically publish one Output Binding or reject a concurrent overlap."""

        changed = _changed_paths(
            {"data": self.data, "metadata": self.metadata},
            {"data": working.data, "metadata": working.metadata},
        )
        if not changed:
            return ()
        conflicts = tuple(
            path
            for path in changed
            if any(
                revision > base_revision and _paths_overlap(path, previous)
                for previous, revision in self.path_revisions.items()
            )
        )
        if conflicts:
            raise ConcurrentContextWriteError(
                node_id=node_id,
                paths=conflicts,
                base_revision=base_revision,
                current_revision=self.revision,
            )
        authoritative = {
            "data": deepcopy(self.data),
            "metadata": deepcopy(self.metadata),
        }
        working_value = {
            "data": working.data,
            "metadata": working.metadata,
        }
        for path in changed:
            _merge_changed_path(authoritative, working_value, path)
        self.revision += 1
        self.data = authoritative["data"]
        self.metadata = authoritative["metadata"]
        for path in changed:
            self.path_revisions[path] = self.revision
        return changed


class ConcurrentContextWriteError(RuntimeError):
    def __init__(
        self,
        *,
        node_id: str,
        paths: tuple[str, ...],
        base_revision: int,
        current_revision: int,
    ) -> None:
        super().__init__(
            "Concurrent Context write overlaps a path changed after this Node "
            f"started: node_id={node_id}, paths={list(paths)}, "
            f"base_revision={base_revision}, current_revision={current_revision}"
        )
        self.node_id = node_id
        self.paths = paths
        self.base_revision = base_revision
        self.current_revision = current_revision


def _changed_paths(previous: Any, current: Any, prefix: str = "") -> tuple[str, ...]:
    if isinstance(previous, dict) and isinstance(current, dict):
        values: list[str] = []
        for key in sorted(set(previous) | set(current)):
            path = f"{prefix}/{_escape_path(str(key))}"
            if key not in previous or key not in current:
                values.append(path)
                continue
            values.extend(_changed_paths(previous[key], current[key], path))
        return tuple(values)
    if isinstance(previous, list) and isinstance(current, list):
        if previous == current:
            return ()
        # Lists are one logical write location. Index-level merging would make
        # append ordering depend on task completion timing.
        return (prefix or "/",)
    return () if previous == current else (prefix or "/",)


def _escape_path(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def _merge_changed_path(
    authoritative: dict[str, Any],
    working: dict[str, Any],
    path: str,
) -> None:
    segments = [
        value.replace("~1", "/").replace("~0", "~")
        for value in path.split("/")[1:]
    ]
    current_parent: Any = authoritative
    working_parent: Any = working
    for segment in segments[:-1]:
        current_parent = current_parent[segment]
        working_parent = working_parent[segment]
    leaf = segments[-1]
    if isinstance(working_parent, dict) and leaf not in working_parent:
        del current_parent[leaf]
        return
    current_parent[leaf] = deepcopy(working_parent[leaf])


def _paths_overlap(left: str, right: str) -> bool:
    return (
        left == right
        or left.startswith(f"{right}/")
        or right.startswith(f"{left}/")
    )


class SessionContext(RuntimeContext):
    """User data shared by all invocations in one Session.

    Typical use: chat history, long-lived task memory, or tenant-specific
    preferences. Output binding is the normal way to update this object during
    execution. Operators should not receive the mutable object directly.
    """


class InvocationContext(RuntimeContext):
    """User data scoped to one Invocation.

    Typical use: scratch data for one request, temporary routing notes, or
    values that should not survive into the next invocation in the same session.
    """


@dataclass(frozen=True, slots=True)
class ContextSnapshot:
    """Isolated context copy for non-writing Workflow hooks.

    The top-level mappings are read-only. Nested values preserve their original
    Python container types and belong to this snapshot, so even accidental
    nested mutation cannot reach the authoritative Runtime Context.
    """

    data: Mapping[str, Any]
    metadata: Mapping[str, Any]

    @classmethod
    def capture(cls, context: RuntimeContext) -> ContextSnapshot:
        return cls(
            data=MappingProxyType(deepcopy(context.data)),
            metadata=MappingProxyType(deepcopy(context.metadata)),
        )


@dataclass(frozen=True, slots=True)
class IncomingOutput:
    """Read-only value carried by one selected incoming edge.

    NodeExecutor builds these records from NodeExecutionRequest activations.
    `edge_id` lets loop input mappings distinguish initial entry from a back
    edge, while `source_execution_id` identifies the exact historical output.
    """

    edge_id: str
    source_node_id: str
    source_execution_id: UUID
    value: Any


@dataclass(frozen=True, slots=True)
class HookContextSnapshot:
    """Common fields shared by every non-writing Workflow hook."""

    invocation_input: Mapping[str, Any]
    invocation_context: ContextSnapshot
    session_context: ContextSnapshot
    outputs: OutputView

    def isolate(self) -> HookContextSnapshot:
        """Create a phase-local copy without copying indexed node outputs."""

        return HookContextSnapshot(
            invocation_input=MappingProxyType(
                deepcopy(dict(self.invocation_input))
            ),
            invocation_context=ContextSnapshot(
                data=MappingProxyType(deepcopy(dict(self.invocation_context.data))),
                metadata=MappingProxyType(
                    deepcopy(dict(self.invocation_context.metadata))
                ),
            ),
            session_context=ContextSnapshot(
                data=MappingProxyType(deepcopy(dict(self.session_context.data))),
                metadata=MappingProxyType(
                    deepcopy(dict(self.session_context.metadata))
                ),
            ),
            outputs=self.outputs,
        )


def capture_hook_context(
    *,
    invocation_input: Mapping[str, Any],
    invocation_context: InvocationContext,
    session_context: SessionContext,
    outputs: OutputView,
) -> HookContextSnapshot:
    return HookContextSnapshot(
        invocation_input=MappingProxyType(deepcopy(dict(invocation_input))),
        invocation_context=ContextSnapshot.capture(invocation_context),
        session_context=ContextSnapshot.capture(session_context),
        outputs=outputs,
    )


@dataclass(frozen=True, slots=True)
class InputMappingContext:
    """Context passed to a Node input mapping."""

    invocation_input: Mapping[str, Any]
    invocation_context: ContextSnapshot
    session_context: ContextSnapshot
    outputs: OutputView
    node_id: str
    workflow_path: tuple[str, ...] = ()
    incoming: tuple[IncomingOutput, ...] = ()

    @classmethod
    def create(
        cls,
        *,
        invocation_input: Mapping[str, Any],
        invocation_context: InvocationContext,
        session_context: SessionContext,
        outputs: OutputView,
        node_id: str,
        workflow_path: tuple[str, ...] = (),
        incoming: tuple[IncomingOutput, ...] = (),
    ) -> InputMappingContext:
        common = capture_hook_context(
            invocation_input=invocation_input,
            invocation_context=invocation_context,
            session_context=session_context,
            outputs=outputs,
        )
        return cls(
            invocation_input=common.invocation_input,
            invocation_context=common.invocation_context,
            session_context=common.session_context,
            outputs=common.outputs,
            node_id=node_id,
            workflow_path=tuple(workflow_path),
            incoming=tuple(
                IncomingOutput(
                    edge_id=item.edge_id,
                    source_node_id=item.source_node_id,
                    source_execution_id=item.source_execution_id,
                    value=deepcopy(item.value),
                )
                for item in incoming
            ),
        )


@dataclass(frozen=True, slots=True)
class ConditionContext:
    """Context passed to an Edge condition."""

    invocation_input: Mapping[str, Any]
    invocation_context: ContextSnapshot
    session_context: ContextSnapshot
    outputs: OutputView
    edge_id: str
    source_node_id: str
    target_node_id: str
    source_output: Any

    @classmethod
    def create(
        cls,
        *,
        invocation_input: Mapping[str, Any],
        invocation_context: InvocationContext,
        session_context: SessionContext,
        outputs: OutputView,
        edge_id: str,
        source_node_id: str,
        target_node_id: str,
        source_output: Any,
    ) -> ConditionContext:
        common = capture_hook_context(
            invocation_input=invocation_input,
            invocation_context=invocation_context,
            session_context=session_context,
            outputs=outputs,
        )
        return cls(
            invocation_input=common.invocation_input,
            invocation_context=common.invocation_context,
            session_context=common.session_context,
            outputs=common.outputs,
            edge_id=edge_id,
            source_node_id=source_node_id,
            target_node_id=target_node_id,
            source_output=deepcopy(source_output),
        )


@dataclass(frozen=True, slots=True)
class MapItemSelectionContext:
    """Context passed to ``MapPolicy.item_selector``."""

    invocation_input: Mapping[str, Any]
    invocation_context: ContextSnapshot
    session_context: ContextSnapshot
    outputs: OutputView
    node_id: str
    input: Any
    workflow_path: tuple[str, ...] = ()
    incoming: tuple[IncomingOutput, ...] = ()


@dataclass(frozen=True, slots=True)
class MapAggregationContext:
    """Context passed to a map output aggregator."""

    invocation_input: Mapping[str, Any]
    invocation_context: ContextSnapshot
    session_context: ContextSnapshot
    outputs: OutputView
    node_id: str
    item_outputs: list[Any]


@dataclass(frozen=True, slots=True)
class ReplicationAggregationContext:
    """Context passed to a replication output aggregator."""

    invocation_input: Mapping[str, Any]
    invocation_context: ContextSnapshot
    session_context: ContextSnapshot
    outputs: OutputView
    node_id: str
    replica_outputs: list[Any]


@dataclass(frozen=True, slots=True)
class OutputBindingContext:
    """Transactional context passed to a Node output binding."""

    invocation_input: Mapping[str, Any]
    invocation_context: InvocationContext
    session_context: SessionContext
    outputs: OutputView
    node_id: str
    output: Any
    workflow_path: tuple[str, ...] = ()

    @classmethod
    def create(
        cls,
        *,
        invocation_input: Mapping[str, Any],
        invocation_context: InvocationContext,
        session_context: SessionContext,
        outputs: OutputView,
        node_id: str,
        output: Any,
        workflow_path: tuple[str, ...] = (),
    ) -> OutputBindingContext:
        return cls(
            invocation_input=MappingProxyType(deepcopy(dict(invocation_input))),
            invocation_context=invocation_context,
            session_context=session_context,
            outputs=outputs,
            node_id=node_id,
            output=deepcopy(output),
            workflow_path=tuple(workflow_path),
        )

"""Invocation-local Context and atomic patch helpers."""

from __future__ import annotations

import copy
from collections.abc import Iterator, Mapping
from types import MappingProxyType
from typing import Any

from ..workflow import ContextPatch, ExecutionContext


class IsolatedMappingView(Mapping[str, Any]):
    """A cheap snapshot of keys that isolates values only when a hook reads them."""

    def __init__(self, source: Mapping[str, Any]) -> None:
        self._source = dict(source)

    def __getitem__(self, key: str) -> Any:
        return copy.deepcopy(self._source[key])

    def __iter__(self) -> Iterator[str]:
        return iter(self._source)

    def __len__(self) -> int:
        return len(self._source)


def readonly_context(
    *,
    session: Mapping[str, Any],
    invocation: Mapping[str, Any],
    outputs: Mapping[str, Any],
    incoming: Mapping[str, Any] | None = None,
    invocation_input: Any,
    node_id: str | None = None,
    edge_id: str | None = None,
    workflow_path: tuple[str, ...] = (),
) -> ExecutionContext:
    """Create an isolated hook view; hooks cannot mutate live Runtime state."""

    return ExecutionContext(
        session=MappingProxyType(copy.deepcopy(dict(session))),
        invocation=MappingProxyType(copy.deepcopy(dict(invocation))),
        outputs=IsolatedMappingView(outputs),
        incoming=IsolatedMappingView(incoming or {}),
        invocation_input=copy.deepcopy(invocation_input),
        node_id=node_id,
        edge_id=edge_id,
        workflow_path=workflow_path,
    )


def patch_paths(patch: ContextPatch) -> set[tuple[str, ...]]:
    paths: set[tuple[str, ...]] = set()
    for root, values in (("session", patch.session), ("invocation", patch.invocation)):
        for path in _leaf_paths(values):
            paths.add((root, *path))
    return paths


def patches_conflict(left: ContextPatch, right: ContextPatch) -> bool:
    for left_path in patch_paths(left):
        for right_path in patch_paths(right):
            size = min(len(left_path), len(right_path))
            if left_path[:size] == right_path[:size]:
                return True
    return False


def apply_patch(
    session: dict[str, Any],
    invocation: dict[str, Any],
    patch: ContextPatch,
) -> None:
    """Apply a validated patch atomically using recursive mapping merge."""

    _merge(session, patch.session)
    _merge(invocation, patch.invocation)


def _merge(target: dict[str, Any], values: Mapping[str, Any]) -> None:
    for key, value in values.items():
        if isinstance(value, Mapping) and isinstance(target.get(key), Mapping):
            nested = copy.deepcopy(dict(target[key]))
            _merge(nested, value)
            target[key] = nested
        else:
            target[key] = copy.deepcopy(value)


def _leaf_paths(values: Mapping[str, Any]) -> set[tuple[str, ...]]:
    result: set[tuple[str, ...]] = set()
    for key, value in values.items():
        if isinstance(value, Mapping) and value:
            result.update((str(key), *path) for path in _leaf_paths(value))
        else:
            result.add((str(key),))
    return result

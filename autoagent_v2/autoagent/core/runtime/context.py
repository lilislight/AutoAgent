"""Isolated Hook inputs and atomic Context patch helpers."""

from __future__ import annotations

import copy
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from ..workflow import (
    AggregationContext,
    ContextPatch,
    EdgeConditionContext,
    HookContext,
    IncomingActivation,
    InputMappingContext,
    ItemSelectorContext,
    OutputBindingContext,
)
from .serialization import RuntimeValueCodec


class IsolatedMappingView(Mapping[str, Any]):
    """Snapshot keys now and deepcopy a value only if user code reads it."""

    def __init__(self, source: Mapping[str, Any]) -> None:
        self._source = dict(source)

    def __getitem__(self, key: str) -> Any:
        return RuntimeValueCodec.isolate(self._source[key])

    def __iter__(self) -> Iterator[str]:
        return iter(self._source)

    def __len__(self) -> int:
        return len(self._source)


class IsolatedIncomingView(Sequence[IncomingActivation]):
    """Preserve activation order while lazily isolating each carried value."""

    def __init__(self, values: Sequence[IncomingActivation]) -> None:
        self._values = tuple(values)

    def __getitem__(self, index: int | slice) -> Any:
        if isinstance(index, slice):
            return tuple(self._clone(item) for item in self._values[index])
        return self._clone(self._values[index])

    def __len__(self) -> int:
        return len(self._values)

    @staticmethod
    def _clone(item: IncomingActivation) -> IncomingActivation:
        return IncomingActivation(
            edge_id=item.edge_id,
            source_node_id=item.source_node_id,
            source_execution_id=item.source_execution_id,
            source_scope=item.source_scope,
            value=RuntimeValueCodec.isolate(item.value),
        )


def hook_context(
    context_type: type[HookContext],
    *,
    workflow_id: str,
    workflow_revision_id: str,
    workflow_path: tuple[str, ...],
    session_id: str,
    invocation_id: str,
    session_context: Mapping[str, Any],
    invocation_context: Mapping[str, Any],
    invocation_input: Any,
    node_id: str | None = None,
    node_execution_id: str | None = None,
    execution_scope: tuple[Any, ...] = (),
    incoming: Sequence[IncomingActivation] = (),
    input: Any = None,
    output: Any = None,
    operator_outputs: list[Any] | None = None,
    edge_id: str | None = None,
    source_node_id: str | None = None,
    source_execution_id: str | None = None,
    source_scope: tuple[Any, ...] = (),
) -> HookContext:
    """Construct one Hook-specific Context without JSON round-tripping values."""

    common = dict(
        workflow_id=workflow_id,
        workflow_revision_id=workflow_revision_id,
        workflow_path=workflow_path,
        session_id=session_id,
        invocation_id=invocation_id,
        session_context=IsolatedMappingView(session_context),
        invocation_context=IsolatedMappingView(invocation_context),
        invocation_input=RuntimeValueCodec.isolate(invocation_input),
    )
    if context_type is EdgeConditionContext:
        assert edge_id is not None and source_node_id is not None
        assert source_execution_id is not None
        return EdgeConditionContext(
            **common,
            edge_id=edge_id,
            source_node_id=source_node_id,
            source_execution_id=source_execution_id,
            source_scope=source_scope,
            output=RuntimeValueCodec.isolate(output),
        )
    assert node_id is not None and node_execution_id is not None
    node = dict(
        **common,
        node_id=node_id,
        node_execution_id=node_execution_id,
        execution_scope=execution_scope,
        incoming=IsolatedIncomingView(incoming),
    )
    if context_type is InputMappingContext:
        return InputMappingContext(**node)
    if context_type is ItemSelectorContext:
        return ItemSelectorContext(**node, input=RuntimeValueCodec.isolate(input))
    if context_type is AggregationContext:
        # This list is the Aggregator's private working value. It is ordered by
        # unit_index and intentionally writable; Runtime Events already own
        # their separately captured Operator-call values.
        return AggregationContext(
            **node,
            input=RuntimeValueCodec.isolate(input),
            operator_outputs=operator_outputs if operator_outputs is not None else [],
        )
    if context_type is OutputBindingContext:
        return OutputBindingContext(
            **node,
            input=RuntimeValueCodec.isolate(input),
            output=RuntimeValueCodec.isolate(output),
        )
    raise TypeError(f"Unsupported Hook Context type: {context_type!r}")


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

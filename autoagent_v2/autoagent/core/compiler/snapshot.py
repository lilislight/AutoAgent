"""Portable, JSON-compatible snapshots of compiled Workflow definitions."""

from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import Callable
from dataclasses import asdict, dataclass, is_dataclass
from enum import Enum
from functools import partial
from typing import get_args, get_origin, get_type_hints

from pydantic import TypeAdapter

from ..operators import Operator, Wait
from ..workflow import Capability, WorkflowIR, workflow_hook_version


WORKFLOW_DEFINITION_SNAPSHOT_SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class WorkflowDefinitionSnapshot:
    """Portable graph semantics for display and compatibility checks.

    The snapshot intentionally contains no live callable. Executing or recovering
    still requires the application to register compatible Python code.
    """

    schema_version: int
    workflow_id: str
    workflow_version: str
    workflow_revision_id: str
    definition_hash: str
    definition: dict[str, object]

    @classmethod
    def from_workflow_ir(cls, workflow: WorkflowIR) -> "WorkflowDefinitionSnapshot":
        definition = workflow_semantic_definition(
            workflow_id=workflow.workflow_id,
            workflow_version=workflow.workflow_version,
            failure_mode=workflow.failure_mode,
            nodes=workflow.nodes,
            edges=workflow.edges,
            loops=workflow.loop_regions,
            entry_node_ids=workflow.entry_node_ids,
            exit_node_ids=workflow.exit_node_ids,
        )
        definition_hash = definition_digest(definition)
        if definition_hash != workflow.definition_hash:
            raise ValueError("Workflow IR definition hash does not match its snapshot.")
        return cls(
            schema_version=WORKFLOW_DEFINITION_SNAPSHOT_SCHEMA_VERSION,
            workflow_id=workflow.workflow_id,
            workflow_version=workflow.workflow_version,
            workflow_revision_id=workflow.workflow_revision_id,
            definition_hash=definition_hash,
            definition=definition,
        )

    def to_record(self) -> dict[str, object]:
        """Return a detached JSON-compatible record."""

        return json.loads(canonical_json(asdict(self)))


def workflow_semantic_definition(
    *,
    workflow_id: str,
    workflow_version: str,
    failure_mode: str,
    nodes: tuple[object, ...],
    edges: tuple[object, ...],
    loops: tuple[object, ...],
    entry_node_ids: tuple[str, ...],
    exit_node_ids: tuple[str, ...],
) -> dict[str, object]:
    """Build the single canonical definition used by revision and snapshot."""

    return {
        "workflow_id": workflow_id,
        "workflow_version": workflow_version,
        "failure_mode": failure_mode,
        "nodes": [
            {
                "id": node.id,
                "executable": _executable_definition(node.executable),
                "input_contract": _contract_definition(node.input_contract),
                "output_contract": _contract_definition(node.output_contract),
                "input_mapping": _hook_definition(node.input_mapping),
                "output_binding": _hook_definition(node.output_binding),
                "execution_mode": node.execution_mode,
                "map": (
                    {
                        "aggregate": _hook_definition(node.map.aggregate),
                        "max_parallelism": node.map.max_parallelism,
                    }
                    if node.map is not None
                    else None
                ),
                "stream": (
                    {
                        "reducer": _callable_name(type(node.stream.reducer)),
                        "initial": _hook_definition(node.stream.reducer.initial),
                        "add": _hook_definition(node.stream.reducer.add),
                        "finish": _hook_definition(node.stream.reducer.finish),
                    }
                    if node.stream is not None
                    else None
                ),
                "user_events": [
                    {
                        "kind": mapping.kind,
                        "mapper": _hook_definition(mapping.mapper),
                        "output_contract": _contract_definition(
                            mapping.output_contract
                        ),
                    }
                    for mapping in node.user_events
                ],
                "operator_policy": _canonicalize_operator_policy(
                    node.operator_policy
                ),
                "recovery": {
                    "mode": node.recovery_mode.mode,
                    "max_attempts": node.recovery_mode.max_attempts,
                },
                "max_occurrences_per_invocation": (
                    node.max_occurrences_per_invocation
                ),
            }
            for node in nodes
        ],
        "edges": [
            {
                "id": edge.id,
                "source": edge.source,
                "target": edge.target,
                "on": edge.on,
                "condition": _hook_definition(edge.condition),
            }
            for edge in edges
        ],
        "entry_node_ids": list(entry_node_ids),
        "exit_node_ids": list(exit_node_ids),
        "loops": [
            {
                "id": loop.id,
                "header": loop.header_node_id,
                "nodes": list(loop.node_ids),
                "entries": list(loop.entry_edge_ids),
                "backs": list(loop.back_edge_ids),
                "exits": list(loop.exit_edge_ids),
                "parent": loop.parent_loop_region_id,
            }
            for loop in loops
        ],
    }


def definition_digest(definition: dict[str, object]) -> str:
    return hashlib.sha256(canonical_json(definition).encode("utf-8")).hexdigest()


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _contract_definition(contract: object | None) -> object | None:
    if contract is None:
        return None
    # JSON Schema carries the complete durable shape. ``ValueContract.name``
    # contains a Python module path and is deliberately excluded from revision.
    return {"schema": json.loads(contract.schema)}


def _executable_definition(executable: object) -> dict[str, object]:
    if isinstance(executable, WorkflowIR):
        return {
            "kind": "workflow",
            "id": executable.workflow_id,
            "definition_hash": executable.definition_hash,
        }
    if isinstance(executable, Operator):
        return {
            "kind": "operator",
            "id": executable.id,
            "handler": _hook_definition(executable.handler),
            "input_contract": _contract_definition(executable.contract.input),
            "output_contract": _contract_definition(executable.contract.output),
            "stream_chunk_contract": _contract_definition(
                executable.contract.stream_chunk
            ),
        }
    if isinstance(executable, Capability):
        return {
            "kind": "capability",
            "id": executable.id,
            "input_contract": _contract_definition(executable.contract.input),
            "output_contract": _contract_definition(executable.contract.output),
            "stream_chunk_contract": _contract_definition(
                executable.contract.stream_chunk
            ),
        }
    if isinstance(executable, Wait):
        return {
            "kind": "wait",
            "id": executable.id,
            "request_contract": _contract_definition(executable.input_contract),
            "response_contract": _contract_definition(executable.output_contract),
        }
    raise TypeError(f"Unsupported executable: {type(executable).__name__}")


def _hook_definition(handler: Callable[..., object] | None) -> object | None:
    if handler is None:
        return None
    target = handler.func if isinstance(handler, partial) else handler
    annotation_source = (
        target
        if inspect.isfunction(target) or inspect.ismethod(target)
        else target.__call__
    )
    hints = get_type_hints(annotation_source, include_extras=True)
    signature = inspect.signature(target)
    return {
        "name": _callable_name(target),
        "contract": {
            "parameters": [
                _annotation_definition(
                    hints.get(parameter.name, parameter.annotation)
                )
                for parameter in signature.parameters.values()
            ],
            "return": _annotation_definition(
                hints.get("return", signature.return_annotation)
            ),
        },
        "version": workflow_hook_version(target),
    }


def _callable_name(value: object) -> str:
    return str(
        getattr(value, "__name__", None)
        or getattr(value, "__class__", type(value)).__name__
    )


def _annotation_definition(annotation: object) -> object:
    if annotation is inspect.Signature.empty:
        return None
    try:
        return TypeAdapter(annotation).json_schema()
    except Exception:
        origin = get_origin(annotation)
        if origin is not None:
            return {
                "type": _callable_name(origin),
                "arguments": [
                    _annotation_definition(argument)
                    for argument in get_args(annotation)
                ],
            }
        return {"type": _callable_name(annotation)}


def _canonicalize_operator_policy(policy: object | None) -> object | None:
    if policy is None:
        return None
    return {
        "retry": {
            "max_attempts": policy.retry.max_attempts,
            "backoff": _canonicalize(policy.retry.backoff),
        },
        "timeout_ms": policy.timeout_ms,
        "fallback": [
            _executable_definition(operator) for operator in policy.fallback
        ],
        "max_concurrency": policy.max_concurrency,
        "max_operator_calls_per_invocation": (
            policy.max_operator_calls_per_invocation
        ),
        "max_runtime_ms_per_invocation": policy.max_runtime_ms_per_invocation,
    }


def _canonicalize(value: object) -> object:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Enum):
        return _canonicalize(value.value)
    if is_dataclass(value):
        return _canonicalize(asdict(value))
    if isinstance(value, dict):
        return {
            str(key): _canonicalize(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_canonicalize(item) for item in value]
    raise TypeError(f"Value is not canonicalizable: {type(value).__name__}")


__all__ = [
    "WORKFLOW_DEFINITION_SNAPSHOT_SCHEMA_VERSION",
    "WorkflowDefinitionSnapshot",
    "definition_digest",
    "workflow_semantic_definition",
]

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from enum import Enum
from functools import partial
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from autoagent.core.compiler.workflow_ir import WorkflowIR
from autoagent.core.operators import Operator, OperatorManifest, OperatorRegistry
from autoagent.core.operators.contract import SchemaContract
from autoagent.core.operators.manifest import callable_operator_id
from autoagent.core.workflow import CapabilityRef, OperatorRef, SystemCommand
from autoagent.core.workflow.hooks import get_workflow_hook_version


class WorkflowVersionSnapshot(BaseModel):
    """Portable description of one successfully compiled Workflow definition.

    Durable stores use ``definition_hash`` to bind every Invocation to the exact
    graph semantics it started with. ``definition`` is JSON-compatible and is
    sufficient for historical graph display, but it intentionally cannot execute
    Python hooks by itself. Recovery still requires the application to register
    the current Workflow and Operators and pass compatibility checks.

    Operator manifests are recorded beside the graph but are excluded from the
    definition hash. This preserves late-bound Capability selection while still
    giving recovery enough information to validate the concrete OperatorCall.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    workflow_id: str
    workflow_version: str | int | None
    ir_version: str
    compiler_version: str
    definition_hash: str
    operator_manifest_hash: str = Field(
        description=(
            "Hash of the exact Operator manifest set available when this "
            "Workflow was compiled. It is separate from structural identity."
        )
    )
    definition: dict[str, Any] = Field(
        description="JSON-compatible graph and execution-semantic snapshot."
    )
    operator_manifests: tuple[OperatorManifest, ...] = Field(
        default_factory=tuple,
        description="Known concrete Operators usable by this compiled Workflow.",
    )

    @classmethod
    def from_workflow_ir(
        cls,
        workflow_ir: WorkflowIR,
        *,
        operator_registry: OperatorRegistry | None = None,
    ) -> WorkflowVersionSnapshot:
        semantic = _semantic_definition(workflow_ir)
        definition = {
            **semantic,
            "name": workflow_ir.name,
            "description": workflow_ir.description,
            "nodes": [
                {
                    **node,
                    "name": workflow_ir.nodes[node["id"]].name,
                    "description": workflow_ir.nodes[node["id"]].description,
                }
                for node in semantic["nodes"]
            ],
        }
        manifests = _operator_manifests(
            workflow_ir,
            operator_registry=operator_registry,
        )
        return cls(
            workflow_id=workflow_ir.workflow_id,
            workflow_version=workflow_ir.workflow_version,
            ir_version=workflow_ir.ir_version,
            compiler_version=workflow_ir.compiler_version,
            definition_hash=_hash_json(semantic),
            operator_manifest_hash=_hash_json(
                [manifest.model_dump(mode="python") for manifest in manifests]
            ),
            definition=definition,
            operator_manifests=manifests,
        )


def _semantic_definition(workflow_ir: WorkflowIR) -> dict[str, Any]:
    """Build only fields that change execution or recovery compatibility."""

    nodes = []
    for node in workflow_ir.nodes.values():
        nodes.append(
            {
                "id": node.id,
                "local_id": node.local_id,
                "scope_node_ids": dict(node.scope_node_ids),
                "workflow_path": list(node.workflow_path),
                "capability": _binding_definition(node.capability),
                "input_contract": node.input_contract.describe(),
                "operator_output_contract": node.operator_output_contract.describe(),
                "output_contract": node.output_contract.describe(),
                "input_plan": _hook_definition(node.input_plan),
                "output_binding": _hook_definition(node.output_binding),
                "policy": _canonicalize(node.policy),
                "entry": node.entry,
                "exit": node.exit,
            }
        )

    edges = []
    for edge in sorted(workflow_ir.edges.values(), key=lambda item: item.order):
        edges.append(
            {
                "id": edge.id,
                "local_id": edge.local_id,
                "local_from_node": edge.local_from_node,
                "local_to_node": edge.local_to_node,
                "scope_node_ids": dict(edge.scope_node_ids),
                "workflow_path": list(edge.workflow_path),
                "from_node": edge.from_node,
                "to_node": edge.to_node,
                "condition": _hook_definition(edge.condition),
                "policy": _canonicalize(edge.policy),
                "order": edge.order,
            }
        )

    loop_regions = [
        _canonicalize(region)
        for region in sorted(
            workflow_ir.graph.loop_regions.values(),
            key=lambda item: item.id,
        )
    ]
    return {
        "ir_version": workflow_ir.ir_version,
        "workflow_version": workflow_ir.workflow_version,
        "nodes": nodes,
        "edges": edges,
        "entry_node_ids": list(workflow_ir.entry_node_ids),
        "exit_node_ids": list(workflow_ir.exit_node_ids),
        "loop_regions": loop_regions,
    }


def _operator_manifests(
    workflow_ir: WorkflowIR,
    *,
    operator_registry: OperatorRegistry | None,
) -> tuple[OperatorManifest, ...]:
    manifests: dict[str, OperatorManifest] = {}
    for node in workflow_ir.nodes.values():
        binding = node.capability
        if isinstance(binding, Operator):
            manifests[binding.id] = binding.manifest
            continue
        if callable(binding):
            operator = Operator.from_callable(binding)
            manifests[operator.id] = operator.manifest
            continue
        if operator_registry is None:
            continue
        if isinstance(binding, OperatorRef):
            operator = operator_registry.get(binding.id)
            if operator is not None:
                manifests[operator.id] = operator.manifest
            continue
        if isinstance(binding, CapabilityRef):
            for operator in operator_registry.for_capability(
                binding.id,
                include_disabled=True,
            ):
                manifests[operator.id] = operator.manifest
    return tuple(manifests[key] for key in sorted(manifests))


def _binding_definition(binding: Any) -> dict[str, Any]:
    if isinstance(binding, Operator):
        return {"kind": "operator", "id": binding.id}
    if callable(binding):
        return {"kind": "operator", "id": callable_operator_id(binding)}
    if isinstance(binding, CapabilityRef):
        return {"kind": "capability", "id": binding.id}
    if isinstance(binding, OperatorRef):
        return {"kind": "operator", "id": binding.id}
    if isinstance(binding, SystemCommand):
        return {"kind": "system_command", "id": binding.id}
    raise TypeError(f"Unsupported Workflow IR capability: {type(binding).__name__}")


def _hook_definition(hook: Any) -> Any:
    if hook is None:
        return None
    if callable(hook):
        definition = {"callable": _callable_identity(hook)}
        version = get_workflow_hook_version(hook)
        if version is not None:
            definition["version"] = version
        return definition
    return _canonicalize(hook)


def _callable_identity(handler: Any) -> Any:
    """Identify hook location without treating source text as compatibility."""

    if isinstance(handler, partial):
        return {
            "partial": _callable_identity(handler.func),
            "args": _canonicalize(handler.args),
            "keywords": _canonicalize(handler.keywords or {}),
        }
    module = getattr(handler, "__module__", handler.__class__.__module__)
    qualname = getattr(handler, "__qualname__", handler.__class__.__qualname__)
    return f"{module}:{qualname}"


def _canonicalize(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Enum):
        return _canonicalize(value.value)
    if isinstance(value, SchemaContract):
        return value.describe()
    if isinstance(value, BaseModel):
        return {
            name: _canonicalize(getattr(value, name))
            for name in type(value).model_fields
        }
    if isinstance(value, Mapping):
        return {
            str(key): _canonicalize(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_canonicalize(item) for item in value]
    if isinstance(value, (set, frozenset)):
        encoded = [_canonicalize(item) for item in value]
        return sorted(encoded, key=_canonical_json)
    if callable(value):
        return _hook_definition(value)
    raise TypeError(f"Value is not canonicalizable: {type(value).__name__}")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _hash_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()

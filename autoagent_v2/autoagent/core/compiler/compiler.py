"""Deterministic V2 compiler with child expansion and natural-loop analysis."""

from __future__ import annotations

import hashlib
import inspect
import json
import re
from collections import Counter, defaultdict
from dataclasses import asdict, replace
from typing import Any, get_args, get_origin, get_type_hints

from ..errors import WorkflowCompileError
from ..operators import (
    Operator,
    ValueContract,
    WaitOperator,
    annotation_name,
    callable_id,
)
from ..workflow import (
    ContextPatch,
    Edge,
    EdgeIR,
    ExecutionContext,
    Node,
    NodeIR,
    SubworkflowIR,
    Workflow,
    WorkflowIR,
    WorkflowPolicy,
)
from .graph import analyze_loops


def _type_name(value: Any) -> str:
    if value is inspect.Signature.empty:
        return "any"
    if isinstance(value, type):
        return value.__qualname__
    origin = get_origin(value)
    if origin is not None:
        origin_name = getattr(origin, "__qualname__", str(origin))
        arguments = ",".join(_type_name(item) for item in get_args(value))
        return f"{origin_name}[{arguments}]"
    return str(value)


def _callable_contract(function: Any) -> tuple[str, str, str]:
    signature = inspect.signature(function)
    try:
        hints = get_type_hints(function)
    except Exception:
        hints = {}
    parameters = [
        {
            "name": parameter.name,
            "kind": parameter.kind.name,
            "type": _type_name(hints.get(parameter.name, parameter.annotation)),
        }
        for parameter in signature.parameters.values()
    ]
    return (
        callable_id(function),
        json.dumps(parameters, sort_keys=True, separators=(",", ":")),
        _type_name(hints.get("return", signature.return_annotation)),
    )


def _hook_identity(function: Any | None, version: str | int) -> Any:
    if function is None:
        return None
    name, inputs, output = _callable_contract(function)
    return {
        "name": name,
        "input_schema": inputs,
        "output_schema": output,
        "version": str(version),
    }


def _policy_identity(policy: Any) -> Any:
    if policy is None:
        return None
    value = asdict(policy)
    for key in ("item_selector", "output_aggregator"):
        if key in value and callable(value[key]):
            value[key] = _hook_identity(value[key], 1)
    for section in ("map", "replication"):
        nested = value.get(section)
        if isinstance(nested, dict):
            for key in ("item_selector", "output_aggregator"):
                if callable(nested.get(key)):
                    nested[key] = _hook_identity(nested[key], 1)
    stream = value.get("stream")
    if isinstance(stream, dict) and isinstance(stream.get("reducer"), type):
        reducer = stream["reducer"]
        stream["reducer"] = f"{reducer.__module__}:{reducer.__qualname__}"
    return value


def _resolved_hints(function: Any, description: str) -> dict[str, object]:
    try:
        return get_type_hints(function, include_extras=True)
    except Exception as error:
        raise WorkflowCompileError(
            f"{description} type annotations cannot be resolved: {error}"
        ) from error


def _strict_signature(
    function: Any, arity: int, description: str
) -> tuple[inspect.Signature, dict[str, object]]:
    try:
        signature = inspect.signature(function)
    except (TypeError, ValueError) as error:
        raise WorkflowCompileError(f"{description} signature is invalid: {error}") from error
    parameters = tuple(signature.parameters.values())
    if len(parameters) != arity or any(
        item.kind
        not in {inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD}
        for item in parameters
    ):
        raise WorkflowCompileError(
            f"{description} must declare exactly {arity} positional parameter(s)."
        )
    return signature, _resolved_hints(function, description)


def _annotation(
    signature: inspect.Signature,
    hints: dict[str, object],
    parameter: inspect.Parameter | None,
    description: str,
) -> object:
    value = (
        hints.get("return", signature.return_annotation)
        if parameter is None
        else hints.get(parameter.name, parameter.annotation)
    )
    try:
        return ValueContract.create(value, location=description).annotation
    except TypeError as error:
        raise WorkflowCompileError(str(error)) from error


def _same_contract(left: ValueContract, right: ValueContract) -> bool:
    return left.schema == right.schema


def _require_exact_annotation(
    value: object, expected: object, description: str
) -> None:
    if value != expected:
        raise WorkflowCompileError(
            f"{description} must be annotated as {annotation_name(expected)!r}."
        )


class WorkflowCompiler:
    """Compile one mutable source Workflow into immutable executable IR."""

    def compile(self, workflow: Workflow) -> WorkflowIR:
        if not workflow.id.strip():
            raise WorkflowCompileError("Workflow id must be non-empty.")
        expanded, subworkflows = self._expand(workflow, path=(), stack=())
        if not expanded.nodes:
            raise WorkflowCompileError(
                "Workflow must contain at least one structural entry Node.",
                code="WORKFLOW_NO_ENTRY",
            )

        node_ids = [node.id for node in expanded.nodes]
        invalid_node_ids = [
            node.id
            for node in expanded.nodes
            if not node.id.strip()
            or not (node._local_id or node.id).strip()
            or "/" in (node._local_id or node.id)
        ]
        if invalid_node_ids:
            raise WorkflowCompileError(
                "Node ids must be non-empty path segments: "
                + ", ".join(repr(value) for value in invalid_node_ids)
            )
        duplicates = sorted(
            value for value, count in Counter(node_ids).items() if count > 1
        )
        if duplicates:
            raise WorkflowCompileError(f"Duplicate Node ids: {', '.join(duplicates)}")
        known = set(node_ids)

        edge_irs: list[EdgeIR] = []
        used_edge_ids: set[str] = set()
        incoming: dict[str, list[str]] = defaultdict(list)
        outgoing: dict[str, list[str]] = defaultdict(list)
        for index, edge in enumerate(expanded.edges, 1):
            if edge.source not in known or edge.target not in known:
                raise WorkflowCompileError(
                    f"Edge {edge.source!r} -> {edge.target!r} references an unknown Node."
                )
            edge_id = edge.id or f"{edge.source}__{edge.target}__{index}"
            if edge_id in used_edge_ids:
                raise WorkflowCompileError(f"Duplicate Edge id: {edge_id}")
            if edge.condition is not None and not callable(edge.condition):
                raise WorkflowCompileError(f"Edge {edge_id!r} condition is not callable.")
            if edge.condition is not None:
                self._validate_condition(edge.condition, f"Edge {edge_id!r} condition")
            used_edge_ids.add(edge_id)
            incoming[edge.target].append(edge.source)
            outgoing[edge.source].append(edge.target)
            edge_irs.append(
                EdgeIR(
                    id=edge_id,
                    source=edge.source,
                    target=edge.target,
                    condition=edge.condition,
                    hook_version=str(edge.hook_version),
                    workflow_path=edge._workflow_path,
                )
            )

        explicit_entries = tuple(node.id for node in expanded.nodes if node.entry)
        inferred_entries = tuple(node_id for node_id in node_ids if not incoming[node_id])
        entries = inferred_entries
        if not entries:
            raise WorkflowCompileError(
                "Workflow has no structural entry Node.",
                code="WORKFLOW_NO_ENTRY",
            )
        for entry in explicit_entries:
            if incoming[entry]:
                raise WorkflowCompileError(
                    f"Entry Node {entry!r} cannot have incoming Edges."
                )
        reachable = set(entries)
        pending = list(entries)
        while pending:
            current = pending.pop()
            for target in outgoing[current]:
                if target not in reachable:
                    reachable.add(target)
                    pending.append(target)
        unreachable = [node_id for node_id in node_ids if node_id not in reachable]
        if unreachable:
            raise WorkflowCompileError(
                "Workflow contains Nodes unreachable from its entry: "
                + ", ".join(unreachable),
                code="WORKFLOW_UNREACHABLE_NODE",
            )
        exits = tuple(node_id for node_id in node_ids if not outgoing[node_id])
        if not exits:
            raise WorkflowCompileError(
                "Workflow has no structural exit Node.",
                code="WORKFLOW_NO_EXIT",
            )

        node_irs: list[NodeIR] = []
        canonical_nodes: list[dict[str, Any]] = []
        for node in expanded.nodes:
            if isinstance(node.operator, Workflow):
                raise WorkflowCompileError("Child Workflow expansion left a placeholder Node.")
            node_ir = self._compile_node(node)
            node_irs.append(node_ir)
            canonical_nodes.append(
                {
                    "id": node.id,
                    "operator": self._operator_identity(node_ir.operator),
                    "fallback_operators": [
                        self._operator_identity(item) for item in node_ir.fallback_operators
                    ],
                    "input_mapping": _hook_identity(node.input_mapping, node.hook_version),
                    "output_binding": _hook_identity(node.output_binding, node.hook_version),
                    "stream_user_events": [
                        {"type": item.type, "transform": _hook_identity(item.transform, node.hook_version)}
                        for item in node.stream_user_event_mappings
                    ],
                    "user_events": [
                        {"type": item.type, "transform": _hook_identity(item.transform, node.hook_version)}
                        for item in node.user_event_mappings
                    ],
                    "hook_version": str(node.hook_version),
                    "policy": _policy_identity(node.policy),
                    "workflow_path": node._workflow_path,
                }
            )

        loops = analyze_loops(tuple(node_ids), tuple(edge_irs), entries)
        canonical_edges = [
            {
                "id": edge.id,
                "source": edge.source,
                "target": edge.target,
                "condition": _hook_identity(edge.condition, edge.hook_version),
                "hook_version": edge.hook_version,
                "workflow_path": edge.workflow_path,
            }
            for edge in edge_irs
        ]
        definition = {
            "workflow_id": workflow.id,
            "workflow_version": str(workflow.version),
            "nodes": canonical_nodes,
            "edges": canonical_edges,
            "policy": _policy_identity(workflow.policy),
            "subworkflows": [
                {
                    "path": value.path,
                    "workflow_id": value.workflow_id,
                    "workflow_version": value.workflow_version,
                    "name": value.name,
                }
                for value in subworkflows
            ],
        }
        definition_hash = hashlib.sha256(
            json.dumps(
                definition, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ).encode("utf-8")
        ).hexdigest()
        return WorkflowIR(
            workflow_id=workflow.id,
            workflow_revision_id=f"{workflow.id}:{definition_hash}",
            definition_hash=definition_hash,
            workflow_version=str(workflow.version),
            name=workflow.name,
            nodes=tuple(node_irs),
            edges=tuple(edge_irs),
            entry_node_ids=entries,
            exit_node_ids=exits,
            loop_regions=loops,
            subworkflows=subworkflows,
            policy=workflow.policy,
        )

    def _compile_node(self, node: Node) -> NodeIR:
        if isinstance(node.operator, WaitOperator):
            if node.fallback_operators:
                raise WorkflowCompileError(
                    f"Wait Node {node.id!r} cannot define fallback Operators."
                )
            if node.policy is not None:
                raise WorkflowCompileError(
                    f"Wait Node {node.id!r} cannot define NodePolicy."
                )
            try:
                request_contract = node.operator.request_contract
                output_contract = node.operator.response_contract
            except TypeError as error:
                raise WorkflowCompileError(str(error)) from error
            operator: Operator | WaitOperator = node.operator
            fallbacks: tuple[Operator, ...] = ()
            input_schema = request_contract.schema
            stream_chunk = None
        else:
            operator = self._operator(node.operator, node.id)
            fallbacks = tuple(
                self._operator(value, f"{node.id}:fallback:{index}")
                for index, value in enumerate(node.fallback_operators, 1)
            )
            input_schema = operator.contract.input_schema
            stream_policy = node.policy.stream if node.policy else None
            if operator.contract.stream_chunk is None:
                if stream_policy is not None:
                    raise WorkflowCompileError(
                        f"Node {node.id!r} configures StreamPolicy but its Operator "
                        "does not return Iterator/AsyncIterator."
                    )
                assert operator.contract.output is not None
                unit_output = operator.contract.output
                stream_chunk = None
            else:
                if stream_policy is None:
                    raise WorkflowCompileError(
                        f"Node {node.id!r} streaming Operator requires StreamPolicy."
                    )
                stream_chunk = operator.contract.stream_chunk
                unit_output = self._stream_result_contract(
                    stream_policy.reducer, stream_chunk, f"Node {node.id!r} Stream reducer"
                )
            for fallback in fallbacks:
                if (
                    fallback.contract.input_schema != operator.contract.input_schema
                    or fallback.contract.declared_output_schema
                    != operator.contract.declared_output_schema
                    or fallback.contract.stream_kind != operator.contract.stream_kind
                ):
                    raise WorkflowCompileError(
                        f"Node {node.id!r} fallback Operator {fallback.id!r} "
                        "does not match the primary Operator contract."
                    )
            recovery = node.policy.recovery if node.policy else None
            if recovery is not None and recovery.mode == "idempotent":
                for candidate in (operator, *fallbacks):
                    self._validate_idempotent_operator(candidate, node.id)
            output_contract = self._logical_output_contract(node, unit_output)

        mapped_contract = None
        if node.input_mapping is not None:
            mapped_contract = self._validate_input_mapping(
                node.input_mapping, f"Node {node.id!r} Input Mapping"
            )
            if isinstance(operator, WaitOperator):
                if not _same_contract(mapped_contract, request_contract):
                    raise WorkflowCompileError(
                        f"Node {node.id!r} Input Mapping return does not match "
                        "WaitOperator request_type."
                    )
            elif len(operator.contract.parameters) == 1 and not _same_contract(
                mapped_contract, operator.contract.parameters[0].value
            ):
                raise WorkflowCompileError(
                    f"Node {node.id!r} Input Mapping return does not match its "
                    "Operator input contract."
                )

        if node.output_binding is not None:
            self._validate_output_binding(
                node.output_binding,
                output_contract,
                f"Node {node.id!r} Output Binding",
            )

        if node.policy and node.policy.map and node.policy.map.item_selector is not None:
            self._validate_map_selector(
                node.policy.map.item_selector,
                mapped_contract,
                operator.contract.parameters if isinstance(operator, Operator) else (),
                f"Node {node.id!r} Map item selector",
            )

        stream_user_event_contracts: list[ValueContract] = []
        for mapping in node.stream_user_event_mappings:
            if stream_chunk is None:
                raise WorkflowCompileError(
                    f"Node {node.id!r} stream UserEvent mapping requires StreamPolicy."
                )
            stream_user_event_contracts.append(
                self._validate_user_event_mapping(node, mapping, stream_chunk, "stream")
            )
        user_event_contracts = [
            self._validate_user_event_mapping(node, mapping, output_contract, "output")
            for mapping in node.user_event_mappings
        ]

        return NodeIR(
            id=node.id,
            operator=operator,
            fallback_operators=fallbacks,
            input_schema=input_schema,
            output_schema=output_contract.schema,
            output_contract=output_contract,
            input_mapping=node.input_mapping,
            output_binding=node.output_binding,
            stream_user_event_mappings=tuple(node.stream_user_event_mappings),
            stream_user_event_contracts=tuple(stream_user_event_contracts),
            user_event_mappings=tuple(node.user_event_mappings),
            user_event_contracts=tuple(user_event_contracts),
            hook_version=str(node.hook_version),
            name=node.name,
            policy=node.policy,
            workflow_path=node._workflow_path,
        )

    @staticmethod
    def _operator(value: Any, node_id: str) -> Operator:
        if isinstance(value, Operator):
            return value
        if not callable(value):
            raise WorkflowCompileError(f"Node {node_id!r} Operator is not callable.")
        try:
            return Operator.from_callable(value)
        except (TypeError, ValueError) as error:
            raise WorkflowCompileError(
                f"Node {node_id!r} Operator contract is invalid: {error}"
            ) from error

    @staticmethod
    def _validate_condition(function: Any, description: str) -> None:
        signature, hints = _strict_signature(function, 1, description)
        parameter = tuple(signature.parameters.values())[0]
        _require_exact_annotation(
            hints.get(parameter.name, parameter.annotation), ExecutionContext,
            f"{description} parameter",
        )
        _require_exact_annotation(
            hints.get("return", signature.return_annotation), bool,
            f"{description} return",
        )

    @staticmethod
    def _validate_input_mapping(function: Any, description: str) -> ValueContract:
        signature, hints = _strict_signature(function, 1, description)
        parameter = tuple(signature.parameters.values())[0]
        _require_exact_annotation(
            hints.get(parameter.name, parameter.annotation), ExecutionContext,
            f"{description} parameter",
        )
        try:
            return ValueContract.create(
                hints.get("return", signature.return_annotation),
                location=f"{description} return",
            )
        except TypeError as error:
            raise WorkflowCompileError(str(error)) from error

    @staticmethod
    def _validate_output_binding(
        function: Any, output: ValueContract, description: str
    ) -> None:
        signature, hints = _strict_signature(function, 2, description)
        context_parameter, output_parameter = tuple(signature.parameters.values())
        _require_exact_annotation(
            hints.get(context_parameter.name, context_parameter.annotation),
            ExecutionContext,
            f"{description} context parameter",
        )
        try:
            declared_output = ValueContract.create(
                hints.get(output_parameter.name, output_parameter.annotation),
                location=f"{description} output parameter",
            )
        except TypeError as error:
            raise WorkflowCompileError(str(error)) from error
        if not _same_contract(declared_output, output):
            raise WorkflowCompileError(
                f"{description} output parameter does not match the Node output contract."
            )
        return_annotation = hints.get("return", signature.return_annotation)
        arguments = set(get_args(return_annotation))
        if return_annotation is ContextPatch:
            return
        if arguments != {ContextPatch, type(None)}:
            raise WorkflowCompileError(
                f"{description} must return ContextPatch or ContextPatch | None."
            )

    @staticmethod
    def _validate_map_selector(
        function: Any,
        mapped_input: ValueContract | None,
        parameters: tuple[Any, ...],
        description: str,
    ) -> None:
        signature, hints = _strict_signature(function, 2, description)
        context_parameter, parameter = tuple(signature.parameters.values())
        _require_exact_annotation(
            hints.get(context_parameter.name, context_parameter.annotation),
            ExecutionContext,
            f"{description} context parameter",
        )
        try:
            input_contract = ValueContract.create(
                hints.get(parameter.name, parameter.annotation),
                location=f"{description} parameter",
            )
            return_annotation = hints.get("return", signature.return_annotation)
            if get_origin(return_annotation) is not list or len(get_args(return_annotation)) != 1:
                raise TypeError(f"{description} must return list[ItemInput].")
            item_contract = ValueContract.create(
                get_args(return_annotation)[0], location=f"{description} item"
            )
        except TypeError as error:
            raise WorkflowCompileError(str(error)) from error
        if mapped_input is not None and not _same_contract(input_contract, mapped_input):
            raise WorkflowCompileError(
                f"{description} parameter does not match Input Mapping return."
            )
        if len(parameters) == 1 and not _same_contract(item_contract, parameters[0].value):
            raise WorkflowCompileError(
                f"{description} item does not match Operator input contract."
            )

    def _logical_output_contract(
        self, node: Node, unit_output: ValueContract
    ) -> ValueContract:
        parallel = bool(node.policy and (node.policy.map or node.policy.replication))
        if not parallel:
            return unit_output
        aggregator = (
            node.policy.map.output_aggregator
            if node.policy and node.policy.map
            else node.policy.replication.output_aggregator
            if node.policy and node.policy.replication
            else None
        )
        if aggregator is None:
            return ValueContract.create(
                list[unit_output.annotation], location=f"Node {node.id!r} output"
            )
        description = f"Node {node.id!r} output aggregator"
        signature, hints = _strict_signature(aggregator, 2, description)
        context_parameter, parameter = tuple(signature.parameters.values())
        _require_exact_annotation(
            hints.get(context_parameter.name, context_parameter.annotation),
            ExecutionContext,
            f"{description} context parameter",
        )
        expected_input = ValueContract.create(
            list[unit_output.annotation], location=f"{description} input"
        )
        try:
            actual_input = ValueContract.create(
                hints.get(parameter.name, parameter.annotation),
                location=f"{description} parameter",
            )
            result = ValueContract.create(
                hints.get("return", signature.return_annotation),
                location=f"{description} return",
            )
        except TypeError as error:
            raise WorkflowCompileError(str(error)) from error
        if not _same_contract(actual_input, expected_input):
            raise WorkflowCompileError(
                f"{description} parameter must be list of Operator outputs."
            )
        return result

    @staticmethod
    def _validate_idempotent_operator(operator: Operator, node_id: str) -> None:
        parameters = {
            parameter.name: parameter for parameter in operator.contract.parameters
        }
        parameter = parameters.get("idempotency_key")
        if parameter is None or parameter.value.annotation is not str:
            raise WorkflowCompileError(
                f"Node {node_id!r} uses idempotent recovery, so Operator "
                f"{operator.id!r} must declare an explicit idempotency_key: str parameter."
            )

    @staticmethod
    def _stream_result_contract(
        reducer: type[Any], chunk: ValueContract, description: str
    ) -> ValueContract:
        try:
            inspect.signature(reducer).bind()
        except (TypeError, ValueError) as error:
            raise WorkflowCompileError(
                f"{description} must be constructible without arguments: {error}"
            ) from error
        if "<locals>" in reducer.__qualname__ or reducer.__module__ == "__main__":
            raise WorkflowCompileError(
                f"{description} class must be defined at module scope and importable."
            )
        add = getattr(reducer, "add", None)
        finish = getattr(reducer, "finish", None)
        if not callable(add) or not callable(finish):
            raise WorkflowCompileError(
                f"{description} must define add(chunk) and finish()."
            )
        if inspect.iscoroutinefunction(add) or inspect.iscoroutinefunction(finish):
            raise WorkflowCompileError(f"{description} methods must be synchronous.")
        add_signature, add_hints = _strict_signature(add, 2, f"{description}.add")
        _, chunk_parameter = tuple(add_signature.parameters.values())
        try:
            actual_chunk = ValueContract.create(
                add_hints.get(chunk_parameter.name, chunk_parameter.annotation),
                location=f"{description}.add chunk",
            )
        except TypeError as error:
            raise WorkflowCompileError(str(error)) from error
        _require_exact_annotation(
            add_hints.get("return", add_signature.return_annotation), type(None),
            f"{description}.add return",
        )
        if not _same_contract(actual_chunk, chunk):
            raise WorkflowCompileError(
                f"{description}.add chunk does not match Operator stream Chunk."
            )
        finish_signature, finish_hints = _strict_signature(
            finish, 1, f"{description}.finish"
        )
        try:
            return ValueContract.create(
                finish_hints.get("return", finish_signature.return_annotation),
                location=f"{description}.finish return",
            )
        except TypeError as error:
            raise WorkflowCompileError(str(error)) from error

    @staticmethod
    def _validate_user_event_mapping(
        node: Node, mapping: Any, source: ValueContract, stage: str
    ) -> ValueContract:
        if re.fullmatch(
            r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*", mapping.type.strip()
        ) is None:
            raise WorkflowCompileError(
                f"Node {node.id!r} UserEvent type must be lowercase snake_case."
            )
        if not callable(mapping.transform):
            raise WorkflowCompileError(
                f"Node {node.id!r} UserEvent transform is not callable."
            )
        description = f"Node {node.id!r} {stage} UserEvent transform"
        signature, hints = _strict_signature(mapping.transform, 1, description)
        parameter = tuple(signature.parameters.values())[0]
        try:
            input_contract = ValueContract.create(
                hints.get(parameter.name, parameter.annotation),
                location=f"{description} parameter",
            )
            output_contract = ValueContract.create(
                hints.get("return", signature.return_annotation),
                location=f"{description} return",
            )
        except TypeError as error:
            raise WorkflowCompileError(str(error)) from error
        if not _same_contract(input_contract, source):
            raise WorkflowCompileError(
                f"{description} parameter does not match its source contract."
            )
        return output_contract

    @staticmethod
    def _operator_identity(operator: Operator | WaitOperator) -> dict[str, Any]:
        if isinstance(operator, WaitOperator):
            return {
                "id": operator.id,
                "request_schema": operator.request_contract.schema,
                "response_schema": operator.response_contract.schema,
            }
        return {
            "id": operator.id,
            "version": str(operator.version),
            "input_schema": operator.contract.input_schema,
            "output_schema": operator.contract.declared_output_schema,
        }

    def _expand(
        self,
        workflow: Workflow,
        *,
        path: tuple[str, ...],
        stack: tuple[int, ...],
    ) -> tuple[Workflow, tuple[SubworkflowIR, ...]]:
        if id(workflow) in stack:
            raise WorkflowCompileError("A child Workflow cannot contain itself recursively.")
        nodes: list[Node] = []
        edges: list[Edge] = []
        input_endpoints: dict[str, str] = {}
        output_endpoints: dict[str, str] = {}
        subworkflows: list[SubworkflowIR] = []
        next_stack = (*stack, id(workflow))

        for source in workflow.nodes:
            if isinstance(source.operator, Workflow):
                if any(
                    (
                        source.fallback_operators,
                        source.input_mapping,
                        source.output_binding,
                        source.stream_user_event_mappings,
                        source.user_event_mappings,
                        source.policy,
                    )
                ):
                    raise WorkflowCompileError(
                        f"Child Workflow placeholder {source.id!r} cannot define Node behavior."
                    )
                if source.operator.policy != WorkflowPolicy():
                    raise WorkflowCompileError(
                        f"Child Workflow {source.id!r} cannot override Workflow failure policy in V2 Core."
                    )
                child_path = (*path, source.id)
                child, nested_subworkflows = self._expand(
                    source.operator, path=child_path, stack=next_stack
                )
                subworkflows.append(
                    SubworkflowIR(
                        path=child_path,
                        workflow_id=source.operator.id,
                        workflow_version=str(source.operator.version),
                        name=source.operator.name,
                    )
                )
                subworkflows.extend(nested_subworkflows)
                child_incoming = {node.id: 0 for node in child.nodes}
                child_outgoing = {node.id: 0 for node in child.nodes}
                for edge in child.edges:
                    child_incoming[edge.target] += 1
                    child_outgoing[edge.source] += 1
                entries = [node.id for node in child.nodes if not child_incoming[node.id]]
                exits = [node.id for node in child.nodes if not child_outgoing[node.id]]
                entry = self._select_boundary(
                    entries, source.child_entry_node_id, "entry", source.id
                )
                exit_node = self._select_boundary(
                    exits, source.child_exit_node_id, "exit", source.id
                )
                nodes.extend(child.nodes)
                edges.extend(child.edges)
                input_endpoints[source.id] = entry
                output_endpoints[source.id] = exit_node
            else:
                qualified = "/".join((*path, source.id)) if path else source.id
                cloned = replace(
                    source,
                    id=qualified,
                    entry=source.entry if not path else None,
                    _workflow_path=path,
                    _local_id=source.id,
                )
                nodes.append(cloned)
                input_endpoints[source.id] = qualified
                output_endpoints[source.id] = qualified

        for index, source in enumerate(workflow.edges, 1):
            source_id = input_endpoints.get(source.source)
            target_id = input_endpoints.get(source.target)
            from_id = output_endpoints.get(source.source)
            if from_id is None or target_id is None or source_id is None:
                raise WorkflowCompileError(
                    f"Edge {source.source!r} -> {source.target!r} references an unknown Node."
                )
            local_id = source.id or f"{source.source}__{source.target}__{index}"
            qualified_id = "/".join((*path, local_id)) if path else local_id
            edges.append(
                replace(
                    source,
                    id=qualified_id,
                    source=from_id,
                    target=target_id,
                    _workflow_path=path,
                    _local_id=local_id,
                )
            )
        return (
            Workflow(
                id=workflow.id,
                version=workflow.version,
                name=workflow.name,
                nodes=nodes,
                edges=edges,
                policy=workflow.policy,
            ),
            tuple(subworkflows),
        )

    @staticmethod
    def _select_boundary(
        candidates: list[str], selector: str | None, kind: str, subject: str
    ) -> str:
        if selector is not None:
            matches = [item for item in candidates if item.rsplit("/", 1)[-1] == selector]
            if len(matches) == 1:
                return matches[0]
            raise WorkflowCompileError(
                f"Child Workflow {subject!r} {kind} selector {selector!r} is invalid."
            )
        if len(candidates) != 1:
            raise WorkflowCompileError(
                f"Child Workflow {subject!r} has {len(candidates)} {kind} Nodes; select one."
            )
        return candidates[0]

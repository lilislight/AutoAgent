"""Compile authoring definitions into deterministic immutable Workflow IR."""

from __future__ import annotations

import inspect
import json
from collections.abc import Callable
from dataclasses import replace
from typing import get_args, get_origin, get_type_hints

from ..errors import WorkflowCompileError
from ..operators import Operator, ValueContract, Wait
from ..workflow import (
    AggregationContext,
    Capability,
    ChildInvocationHandle,
    ConditionContext,
    ContextPatch,
    Edge,
    EdgeIR,
    InputMappingContext,
    Map,
    Node,
    NodeIR,
    OperatorPolicy,
    OutputBindingContext,
    OperatorPolicyIR,
    Recovery,
    Stream,
    StreamContext,
    SubWorkflow,
    UserEventMapping,
    Workflow,
    WorkflowIR,
    UserEventMappingIR,
)
from .diagnostic import CompileResult, Diagnostic
from .graph import analyze_loops
from .snapshot import (
    WorkflowDefinitionSnapshot,
    definition_digest,
    workflow_semantic_definition,
)


def _error(
    code: str,
    message: str,
    *,
    object_type: str | None = None,
    object_id: str | None = None,
    field: str | None = None,
    hint: str | None = None,
) -> WorkflowCompileError:
    return WorkflowCompileError(
        message,
        code=code,
        object_type=object_type,
        object_id=object_id,
        field=field,
        hint=hint,
    )


def _optional_contract_same(
    left: ValueContract | None, right: ValueContract | None
) -> bool:
    if left is None or right is None:
        return left is right
    return left.same_as(right)


class WorkflowCompiler:
    """Strict V2 compiler with stable diagnostics and no legacy adapters."""

    def compile(self, workflow: Workflow) -> CompileResult:
        """Compile without throwing for an invalid Workflow definition."""

        workflow_id = workflow.id if isinstance(workflow, Workflow) else None
        try:
            # The successful path compiles exactly once. A diagnostic recovery
            # pass is only needed after compilation has already failed.
            workflow_ir = self._compile(workflow, ())
        except WorkflowCompileError as error:
            diagnostics = self._collect_definition_diagnostics(workflow)
            if not diagnostics:
                diagnostics = (
                    Diagnostic(
                        code=error.code or "COMPILE_FAILED",
                        severity="error",
                        message=error.message,
                        workflow_id=workflow_id,
                        object_type=error.object_type,
                        object_id=error.object_id,
                        field=error.field,
                        hint=error.hint,
                    ),
                )
            return CompileResult(workflow_id, diagnostics=diagnostics)
        except (TypeError, ValueError) as error:
            diagnostics = self._collect_definition_diagnostics(workflow)
            return CompileResult(
                workflow_id,
                diagnostics=diagnostics
                or (
                    Diagnostic(
                        code="DEFINITION_INVALID",
                        severity="error",
                        message=str(error),
                        workflow_id=workflow_id,
                    ),
                ),
            )
        return CompileResult(
            workflow_id,
            workflow_ir=workflow_ir,
            workflow_definition_snapshot=(
                WorkflowDefinitionSnapshot.from_workflow_ir(workflow_ir)
            ),
        )

    def _collect_definition_diagnostics(
        self, workflow: Workflow
    ) -> tuple[Diagnostic, ...]:
        """Collect independent local definition errors before graph analysis."""

        if not isinstance(workflow, Workflow):
            return (
                Diagnostic(
                    code="DEFINITION_INVALID",
                    severity="error",
                    message="compile() requires a Workflow.",
                ),
            )
        collected: list[Diagnostic] = []

        def add(error: Exception, *, object_type=None, object_id=None) -> None:
            if isinstance(error, WorkflowCompileError):
                collected.append(
                    Diagnostic(
                        code=error.code or "COMPILE_FAILED",
                        severity="error",
                        message=error.message,
                        workflow_id=workflow.id,
                        object_type=error.object_type or object_type,
                        object_id=error.object_id or object_id,
                        field=error.field,
                        hint=error.hint,
                    )
                )
            else:
                collected.append(
                    Diagnostic(
                        code="DEFINITION_INVALID",
                        severity="error",
                        message=str(error),
                        workflow_id=workflow.id,
                        object_type=object_type,
                        object_id=object_id,
                    )
                )

        if not isinstance(workflow.id, str) or not workflow.id.strip():
            add(_error("WORKFLOW_ID_EMPTY", "Workflow id cannot be empty."))
        try:
            nodes, edges = self._flatten(workflow)
        except (WorkflowCompileError, TypeError, ValueError) as error:
            add(error)
            return tuple(collected)
        if not nodes:
            add(_error("WORKFLOW_EMPTY", "Workflow must contain at least one Node."))
            return tuple(collected)

        node_ids: set[str] = set()
        for node in nodes:
            if node.id in node_ids:
                add(
                    _error("NODE_ID_DUPLICATE", f"Duplicate Node id {node.id!r}."),
                    object_type="node",
                    object_id=node.id,
                )
            node_ids.add(node.id)
            try:
                self._compile_node(node, (id(workflow),))
            except (WorkflowCompileError, TypeError, ValueError) as error:
                add(error, object_type="node", object_id=node.id)

        pairs: set[tuple[str, str]] = set()
        edge_ids: set[str] = set()
        for edge in edges:
            edge_id = edge.id or f"{edge.source}->{edge.target}"
            if edge.source not in node_ids or edge.target not in node_ids:
                missing_field = "source" if edge.source not in node_ids else "target"
                add(
                    _error(
                        "EDGE_ENDPOINT_UNKNOWN",
                        f"Edge {edge.source!r}->{edge.target!r} references an unknown Node.",
                        object_type="edge",
                        object_id=edge_id,
                        field=missing_field,
                        hint="Reference an existing Node id.",
                    ),
                    object_type="edge",
                    object_id=edge_id,
                )
            pair = (edge.source, edge.target)
            if pair in pairs:
                add(
                    _error(
                        "EDGE_DUPLICATE_ENDPOINTS",
                        f"Only one Edge is allowed from {edge.source!r} to {edge.target!r}.",
                    ),
                    object_type="edge",
                    object_id=edge_id,
                )
            pairs.add(pair)
            if edge_id in edge_ids:
                add(
                    _error("EDGE_ID_DUPLICATE", f"Duplicate Edge id {edge_id!r}."),
                    object_type="edge",
                    object_id=edge_id,
                )
            edge_ids.add(edge_id)
            if edge.on not in {"complete", "error"}:
                add(
                    _error("EDGE_ON_INVALID", f"Edge {edge_id!r} has invalid on value."),
                    object_type="edge",
                    object_id=edge_id,
                )
            try:
                self._validate_condition(edge, edge_id)
            except (WorkflowCompileError, TypeError, ValueError) as error:
                add(error, object_type="edge", object_id=edge_id)
        return tuple(collected)

    def compile_or_raise(self, workflow: Workflow) -> WorkflowIR:
        """Compile for Runtime/App code that requires executable IR."""

        return self._compile(workflow, ())

    def _compile(
        self,
        workflow: Workflow,
        parent_workflows: tuple[int, ...],
    ) -> WorkflowIR:

        if not isinstance(workflow, Workflow):
            raise TypeError("compile() requires a Workflow.")
        if id(workflow) in parent_workflows:
            raise _error(
                "CHILD_WORKFLOW_RECURSION",
                f"Child Workflow {workflow.id!r} recursively invokes itself.",
            )
        workflow_stack = (*parent_workflows, id(workflow))
        if not isinstance(workflow.id, str) or not workflow.id.strip():
            raise _error("WORKFLOW_ID_EMPTY", "Workflow id cannot be empty.")
        if not isinstance(workflow.failure_mode, str) or workflow.failure_mode not in {
            "fail_fast",
            "continue_active_branches",
        }:
            raise _error(
                "WORKFLOW_FAILURE_MODE_INVALID",
                "Workflow failure_mode must be 'fail_fast' or "
                "'continue_active_branches'.",
            )
        nodes, edges = self._flatten(workflow)
        if not nodes:
            raise _error("WORKFLOW_EMPTY", "Workflow must contain at least one Node.")
        node_ids = tuple(node.id for node in nodes)
        if len(set(node_ids)) != len(node_ids):
            raise _error("NODE_ID_DUPLICATE", "Node ids must be unique after expansion.")
        node_set = set(node_ids)
        edge_pairs: set[tuple[str, str]] = set()
        edge_ids: set[str] = set()
        edge_ir: list[EdgeIR] = []
        for index, edge in enumerate(edges):
            if edge.source not in node_set or edge.target not in node_set:
                missing_field = "source" if edge.source not in node_set else "target"
                raise _error(
                    "EDGE_ENDPOINT_UNKNOWN",
                    f"Edge {edge.source!r}->{edge.target!r} references an unknown Node.",
                    object_type="edge",
                    object_id=edge.id or f"{edge.source}->{edge.target}",
                    field=missing_field,
                    hint="Reference an existing Node id.",
                )
            pair = (edge.source, edge.target)
            if pair in edge_pairs:
                raise _error(
                    "EDGE_DUPLICATE_ENDPOINTS",
                    f"Only one Edge is allowed from {edge.source!r} to {edge.target!r}.",
                    object_type="edge",
                    object_id=edge.id or f"{edge.source}->{edge.target}",
                    hint="Merge status and Condition semantics into one Edge.",
                )
            edge_pairs.add(pair)
            edge_id = edge.id or f"{edge.source}->{edge.target}"
            if edge_id in edge_ids:
                raise _error(
                    "EDGE_ID_DUPLICATE",
                    f"Duplicate Edge id {edge_id!r}.",
                    object_type="edge",
                    object_id=edge_id,
                    field="id",
                    hint="Assign a unique Edge id.",
                )
            edge_ids.add(edge_id)
            if edge.on not in {"complete", "error"}:
                raise _error(
                    "EDGE_ON_INVALID",
                    f"Edge {edge_id!r} has invalid on value.",
                    object_type="edge",
                    object_id=edge_id,
                    field="on",
                    hint="Use 'complete' or 'error'.",
                )
            self._validate_condition(edge, edge_id)
            edge_ir.append(
                EdgeIR(edge_id, edge.source, edge.target, edge.condition, edge.on)
            )

        incoming: dict[str, list[EdgeIR]] = {node_id: [] for node_id in node_ids}
        outgoing: dict[str, list[EdgeIR]] = {node_id: [] for node_id in node_ids}
        for edge in edge_ir:
            incoming[edge.target].append(edge)
            outgoing[edge.source].append(edge)
        for source, values in outgoing.items():
            complete_targets = {edge.target for edge in values if edge.on == "complete"}
            error_targets = {edge.target for edge in values if edge.on == "error"}
            overlap = complete_targets & error_targets
            if overlap:
                raise _error(
                    "EDGE_STATUS_TARGET_CONFLICT",
                    f"Node {source!r} routes complete and error to the same Target.",
                )

        entries = tuple(node_id for node_id in node_ids if not incoming[node_id])
        exits = tuple(node_id for node_id in node_ids if not outgoing[node_id])
        if not entries:
            raise _error("WORKFLOW_WITHOUT_ENTRY", "Workflow has no structural Entry Node.")
        if not exits:
            raise _error("WORKFLOW_WITHOUT_EXIT", "Workflow has no structural Exit Node.")
        reachable = set(entries)
        pending = list(entries)
        while pending:
            source = pending.pop()
            for edge in outgoing[source]:
                if edge.target not in reachable:
                    reachable.add(edge.target)
                    pending.append(edge.target)
        unreachable = tuple(node_id for node_id in node_ids if node_id not in reachable)
        if unreachable:
            raise _error(
                "WORKFLOW_UNREACHABLE_NODE",
                "Nodes are unreachable from every structural Entry: "
                + ", ".join(unreachable),
                object_type="workflow",
                object_id=workflow.id,
                hint="Connect or remove the unreachable graph component.",
            )

        node_ir = tuple(self._compile_node(node, workflow_stack) for node in nodes)
        node_index = {node.id: node for node in node_ir}
        loops = analyze_loops(node_ids, tuple(edge_ir), entries)
        back_edge_ids = {
            edge_id
            for loop in loops
            for edge_id in loop.back_edge_ids
        }
        for target, values in incoming.items():
            target_node = node_index[target]
            # A Loop Header is reached once from outside its region and later
            # once per Back Edge selection. Those activations belong to
            # different occurrences; they are not one ordinary Fan-in. Only
            # the non-Back inputs participate in the Header's initial Join.
            join_inputs = tuple(
                edge for edge in values if edge.id not in back_edge_ids
            )
            if len(join_inputs) > 1 and target_node.input_mapping is None:
                raise _error(
                    "INPUT_MAPPING_REQUIRED",
                    f"Multi-input Node {target!r} requires Input Mapping.",
                )
            if any(edge.on == "error" for edge in values) and target_node.input_mapping is None:
                raise _error(
                    "ERROR_INPUT_MAPPING_REQUIRED",
                    f"Error Target Node {target!r} requires Input Mapping.",
                )
            if target_node.input_mapping is None:
                for edge in values:
                    if edge.on != "complete":
                        continue
                    source_contract = node_index[edge.source].output_contract
                    target_contract = target_node.input_contract
                    if (
                        source_contract is None
                        or target_contract is None
                        or not source_contract.same_as(target_contract)
                    ):
                        raise _error(
                            "CONTRACT_MISMATCH",
                            f"{edge.source!r} output and {target!r} input must use the same declared contract.",
                        )

        definition = workflow_semantic_definition(
            workflow_id=workflow.id,
            workflow_version=str(workflow.version),
            failure_mode=workflow.failure_mode,
            nodes=node_ir,
            edges=tuple(edge_ir),
            loops=loops,
            entry_node_ids=entries,
            exit_node_ids=exits,
        )
        digest = definition_digest(definition)
        return WorkflowIR(
            workflow_id=workflow.id,
            workflow_revision_id=f"{workflow.id}:{digest[:16]}",
            definition_hash=digest,
            workflow_version=str(workflow.version),
            nodes=node_ir,
            edges=tuple(edge_ir),
            entry_node_ids=entries,
            exit_node_ids=exits,
            failure_mode=workflow.failure_mode,
            loop_regions=loops,
        )

    def _flatten(
        self,
        workflow: Workflow,
        prefix: str = "",
        parent_workflows: tuple[int, ...] = (),
    ) -> tuple[list[Node], list[Edge]]:
        if not isinstance(workflow.id, str) or not workflow.id.strip():
            raise _error("WORKFLOW_ID_EMPTY", "Workflow id cannot be empty.")
        if id(workflow) in parent_workflows:
            raise _error(
                "SUBWORKFLOW_RECURSION",
                f"SubWorkflow {workflow.id!r} recursively contains itself.",
            )
        workflow_stack = (*parent_workflows, id(workflow))
        for node in workflow.nodes:
            if not isinstance(node, Node):
                raise _error("NODE_DEFINITION_INVALID", "Workflow nodes must be Node objects.")
            if not isinstance(node.id, str):
                raise _error("NODE_ID_INVALID", "Node id must be a string.")
            if not node.id.strip():
                raise _error("NODE_ID_EMPTY", "Node id cannot be empty.")
        for edge in workflow.edges:
            if not isinstance(edge, Edge):
                raise _error("EDGE_DEFINITION_INVALID", "Workflow edges must be Edge objects.")
            if not isinstance(edge.source, str) or not isinstance(edge.target, str):
                raise _error("EDGE_ENDPOINT_INVALID", "Edge source and target must be Node ids.")
            if edge.id is not None and not isinstance(edge.id, str):
                raise _error("EDGE_ID_INVALID", "Edge id must be a string or None.")
            if edge.id is not None and not edge.id.strip():
                raise _error("EDGE_ID_EMPTY", "Edge id cannot be empty.")
            if not isinstance(edge.on, str) or edge.on not in {"complete", "error"}:
                raise _error("EDGE_ON_INVALID", "Edge on must be 'complete' or 'error'.")
        for child in workflow.sub_workflows:
            if not isinstance(child, SubWorkflow):
                raise _error(
                    "SUBWORKFLOW_DEFINITION_INVALID",
                    "Workflow sub_workflows must be SubWorkflow objects.",
                )
            if not isinstance(child.workflow, Workflow):
                raise _error(
                    "SUBWORKFLOW_INVALID", "SubWorkflow must contain a Workflow."
                )
        nodes = [replace(node, id=f"{prefix}{node.id}") for node in workflow.nodes]
        edges = [
            replace(
                edge,
                id=f"{prefix}{edge.id}" if edge.id else None,
                source=f"{prefix}{edge.source}",
                target=f"{prefix}{edge.target}",
            )
            for edge in workflow.edges
        ]
        for child in workflow.sub_workflows:
            if not isinstance(child.id, str) or not child.id.strip():
                raise _error("SUBWORKFLOW_ID_EMPTY", "SubWorkflow id cannot be empty.")
            child_prefix = f"{prefix}{child.id}."
            child_nodes, child_edges = self._flatten(
                child.workflow,
                child_prefix,
                workflow_stack,
            )
            nodes.extend(child_nodes)
            edges.extend(child_edges)
        return nodes, edges

    def _compile_node(
        self,
        node: Node,
        parent_workflows: tuple[int, ...],
    ) -> NodeIR:
        if not isinstance(node.id, str) or not node.id.strip():
            raise _error("NODE_ID_EMPTY", "Node id cannot be empty.")
        if (
            node.max_occurrences_per_invocation is not None
            and (
                not isinstance(node.max_occurrences_per_invocation, int)
                or isinstance(node.max_occurrences_per_invocation, bool)
                or node.max_occurrences_per_invocation < 1
            )
        ):
            raise _error(
                "NODE_OCCURRENCE_LIMIT_INVALID",
                f"Node {node.id!r} occurrence limit must be positive.",
            )
        for name, value, expected in (
            ("map", node.map, Map),
            ("stream", node.stream, Stream),
            ("operator_policy", node.operator_policy, OperatorPolicy),
        ):
            if value is not None and not isinstance(value, expected):
                raise _error(
                    "NODE_CONFIGURATION_INVALID",
                    f"Node {node.id!r} {name} has an invalid definition.",
                )
        if not isinstance(node.recovery_mode, Recovery):
            raise _error(
                "NODE_CONFIGURATION_INVALID",
                f"Node {node.id!r} recovery_mode must be Recovery.",
            )
        if not isinstance(node.user_events, tuple) or not all(
            isinstance(item, UserEventMapping) for item in node.user_events
        ):
            raise _error(
                "NODE_CONFIGURATION_INVALID",
                f"Node {node.id!r} user_events must contain UserEventMapping values.",
            )
        executable = node.executable
        if isinstance(executable, Workflow):
            if node.stream is not None:
                raise _error(
                    "STREAM_OPERATOR_REQUIRED",
                    f"Child Workflow Node {node.id!r} cannot define Stream.",
                )
            compiled_child = self._compile(executable, parent_workflows)
            if len(compiled_child.entry_node_ids) != 1 or len(compiled_child.exit_node_ids) != 1:
                raise _error(
                    "CHILD_WORKFLOW_BOUNDARY_AMBIGUOUS",
                    f"Child Workflow Node {node.id!r} requires one Entry and one Exit.",
                )
            input_contract = compiled_child.node(compiled_child.entry_node_ids[0]).input_contract
            output_contract = compiled_child.node(compiled_child.exit_node_ids[0]).output_contract
            compiled_executable: object = compiled_child
        elif isinstance(executable, Capability):
            contract = executable.contract
            input_contract = contract.input
            output_contract = self._compile_stream(
                node, contract.output, contract.stream_chunk
            )
            compiled_executable = executable
        elif isinstance(executable, Wait):
            if node.stream is not None:
                raise _error(
                    "STREAM_OPERATOR_REQUIRED",
                    f"Wait Node {node.id!r} cannot define Stream.",
                )
            input_contract = executable.input_contract
            output_contract = executable.output_contract
            compiled_executable = executable
        else:
            operator = executable if isinstance(executable, Operator) else Operator(executable)
            input_contract = operator.contract.input
            output_contract = self._compile_stream(
                node,
                operator.contract.output,
                operator.contract.stream_chunk,
            )
            compiled_executable = operator

        if not isinstance(node.execution_mode, str) or node.execution_mode not in {
            "await",
            "spawn",
        }:
            raise _error("EXECUTION_MODE_INVALID", f"Node {node.id!r} has invalid execution_mode.")
        if node.execution_mode != "await" and not isinstance(compiled_executable, WorkflowIR):
            raise _error(
                "EXECUTION_MODE_NOT_WORKFLOW",
                f"Node {node.id!r} execution_mode only applies to Workflow executable.",
            )
        if node.execution_mode == "spawn":
            output_contract = ValueContract.create(
                ChildInvocationHandle,
                location=f"Node {node.id} spawn output",
            )
        self._validate_input_mapping(node, input_contract)
        self._validate_output_binding(node)
        if node.map is not None:
            if node.input_mapping is None:
                raise _error("MAP_INPUT_MAPPING_REQUIRED", f"Map Node {node.id!r} requires Input Mapping.")
            if node.map.aggregate is None:
                assert output_contract is not None
                output_contract = ValueContract(
                    annotation=list[output_contract.annotation],  # type: ignore[valid-type]
                    name=f"list[{output_contract.name}]",
                    schema=json.dumps({"type": "array", "items": json.loads(output_contract.schema)}, sort_keys=True),
                )
            else:
                returned = self._validate_hook(
                    node.map.aggregate,
                    (AggregationContext,),
                    code="MAP_AGGREGATION_SIGNATURE",
                    label=f"Map Node {node.id!r} Aggregation",
                )
                output_contract = ValueContract.create(
                    returned,
                    location=f"Map Node {node.id} Aggregation return",
                )
        user_event_kinds: set[str] = set()
        user_events: list[UserEventMappingIR] = []
        for mapping in node.user_events:
            if mapping.kind in user_event_kinds:
                raise _error(
                    "USER_EVENT_KIND_DUPLICATE",
                    f"Node {node.id!r} repeats User Event kind {mapping.kind!r}.",
                )
            user_event_kinds.add(mapping.kind)
            returned = self._validate_hook(
                mapping.mapper,
                (OutputBindingContext,),
                code="USER_EVENT_MAPPING_SIGNATURE",
                label=f"Node {node.id!r} User Event Mapping {mapping.kind!r}",
            )
            user_events.append(
                UserEventMappingIR(
                    mapping.kind,
                    mapping.mapper,
                    ValueContract.create(
                        returned,
                        location=f"Node {node.id} User Event {mapping.kind}",
                    ),
                )
            )
        operator_policy = None
        if node.operator_policy is not None:
            if not isinstance(compiled_executable, (Operator, Capability)):
                raise _error(
                    "OPERATOR_POLICY_EXECUTABLE_INVALID",
                    f"Node {node.id!r} OperatorPolicy requires Operator or Capability.",
                )
            fallback = tuple(
                value if isinstance(value, Operator) else Operator(value)
                for value in node.operator_policy.fallback
            )
            primary_contract = (
                compiled_executable.contract
                if isinstance(compiled_executable, Operator)
                else compiled_executable.contract
            )
            for operator in fallback:
                if (
                    not operator.contract.input.same_as(primary_contract.input)
                    or not _optional_contract_same(
                        operator.contract.output,
                        primary_contract.output,
                    )
                    or not _optional_contract_same(
                        operator.contract.stream_chunk,
                        primary_contract.stream_chunk,
                    )
                ):
                    raise _error(
                        "FALLBACK_CONTRACT_MISMATCH",
                        f"Node {node.id!r} fallback Operator {operator.id!r} has another contract.",
                    )
            operator_policy = OperatorPolicyIR(
                retry=node.operator_policy.retry,
                timeout_ms=node.operator_policy.timeout_ms,
                fallback=fallback,
                max_concurrency=node.operator_policy.max_concurrency,
                max_operator_calls_per_invocation=(
                    node.operator_policy.max_operator_calls_per_invocation
                ),
                max_runtime_ms_per_invocation=(
                    node.operator_policy.max_runtime_ms_per_invocation
                ),
            )
        return NodeIR(
            id=node.id,
            executable=compiled_executable,  # type: ignore[arg-type]
            input_contract=input_contract,
            output_contract=output_contract,
            input_mapping=node.input_mapping,
            output_binding=node.output_binding,
            execution_mode=node.execution_mode,
            map=node.map,
            stream=node.stream,
            user_events=tuple(user_events),
            operator_policy=operator_policy,
            recovery_mode=node.recovery_mode,
            max_occurrences_per_invocation=node.max_occurrences_per_invocation,
        )

    def _validate_input_mapping(self, node: Node, target: ValueContract | None) -> None:
        if node.input_mapping is None:
            return
        returned = self._validate_hook(
            node.input_mapping,
            (InputMappingContext,),
            code="INPUT_MAPPING_SIGNATURE",
            label=f"Node {node.id!r} Input Mapping",
        )
        if node.map is not None:
            if get_origin(returned) is not list or len(get_args(returned)) != 1:
                raise _error(
                    "MAP_INPUT_MAPPING_RETURN",
                    f"Map Node {node.id!r} Input Mapping must return list[ExecutableInput].",
                )
            returned = get_args(returned)[0]
        contract = ValueContract.create(returned, location=f"Node {node.id} Input Mapping return")
        if target is None or not contract.same_as(target):
            raise _error(
                "INPUT_MAPPING_CONTRACT_MISMATCH",
                f"Node {node.id!r} Input Mapping must return its executable input contract.",
            )

    def _validate_condition(self, edge: Edge, edge_id: str) -> None:
        if edge.condition is None:
            return
        returned = self._validate_hook(
            edge.condition,
            (ConditionContext,),
            code="CONDITION_SIGNATURE",
            label=f"Edge {edge_id!r} Condition",
        )
        if returned is not bool:
            raise _error("CONDITION_RETURN", f"Edge {edge_id!r} Condition must return bool.")

    def _validate_output_binding(self, node: Node) -> None:
        if node.output_binding is None:
            return
        returned = self._validate_hook(
            node.output_binding,
            (OutputBindingContext,),
            code="OUTPUT_BINDING_SIGNATURE",
            label=f"Node {node.id!r} Output Binding",
        )
        allowed = returned in {ContextPatch, type(None)} or (
            set(get_args(returned)) == {ContextPatch, type(None)}
        )
        if not allowed:
            raise _error(
                "OUTPUT_BINDING_RETURN",
                f"Node {node.id!r} Output Binding must return ContextPatch or None.",
            )

    def _compile_stream(
        self,
        node: Node,
        output: ValueContract | None,
        chunk: ValueContract | None,
    ) -> ValueContract:
        if node.stream is None:
            if output is None:
                raise _error(
                    "STREAM_CONFIGURATION_REQUIRED",
                    f"Streaming executable on Node {node.id!r} requires Stream.",
                )
            return output
        if chunk is None:
            raise _error(
                "STREAM_OPERATOR_REQUIRED",
                f"Node {node.id!r} Stream requires a streaming executable.",
            )
        reducer = node.stream.reducer
        if not all(
            callable(getattr(reducer, name, None))
            for name in ("initial", "add", "finish")
        ):
            raise _error(
                "STREAM_REDUCER_INVALID",
                f"Node {node.id!r} Stream Reducer requires initial/add/finish.",
            )
        state_type = self._validate_hook(
            reducer.initial,
            (StreamContext,),
            code="STREAM_REDUCER_INITIAL",
            label=f"Node {node.id!r} Stream Reducer initial",
        )
        state = ValueContract.create(
            state_type, location=f"Node {node.id} Stream state"
        )
        add_return = self._validate_hook(
            reducer.add,
            (StreamContext, state.annotation, chunk.annotation),
            code="STREAM_REDUCER_ADD",
            label=f"Node {node.id!r} Stream Reducer add",
        )
        add_contract = ValueContract.create(
            add_return, location=f"Node {node.id} Stream Reducer add return"
        )
        if not add_contract.same_as(state):
            raise _error(
                "STREAM_REDUCER_STATE_MISMATCH",
                f"Node {node.id!r} Stream Reducer add must return its state contract.",
            )
        finish_return = self._validate_hook(
            reducer.finish,
            (StreamContext, state.annotation),
            code="STREAM_REDUCER_FINISH",
            label=f"Node {node.id!r} Stream Reducer finish",
        )
        return ValueContract.create(
            finish_return, location=f"Node {node.id} Stream output"
        )

    def _validate_hook(
        self,
        hook: Callable[..., object],
        expected: tuple[object, ...],
        *,
        code: str,
        label: str,
    ) -> object:
        if not callable(hook):
            raise _error(code, f"{label} must be callable.")
        signature = inspect.signature(hook)
        parameters = tuple(signature.parameters.values())
        if len(parameters) != len(expected) or any(
            item.kind
            not in {inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD}
            for item in parameters
        ):
            raise _error(code, f"{label} has an invalid parameter list.")
        try:
            hints = get_type_hints(hook, include_extras=True)
        except Exception as error:
            raise _error(code, f"{label} annotations cannot be resolved: {error}") from error
        for parameter, expected_type in zip(parameters, expected, strict=True):
            actual = hints.get(parameter.name, parameter.annotation)
            if actual is not expected_type:
                raise _error(
                    code,
                    f"{label} parameter {parameter.name!r} must use "
                    f"{getattr(expected_type, '__name__', expected_type)!s}.",
                )
        returned = hints.get("return", signature.return_annotation)
        if returned is inspect.Signature.empty:
            raise _error(code, f"{label} must declare a return contract.")
        return returned

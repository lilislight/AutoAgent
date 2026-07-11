from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from autoagent.compiler.diagnostic import CompileResult, Diagnostic
from autoagent.compiler.id_generation import (
    generate_edge_id,
    generate_node_id,
    generate_workflow_id,
)
from autoagent.compiler.workflow_ir import EdgeIR, GraphIR, NodeIR, WorkflowIR
from autoagent.workflow import (
    CapabilityRef,
    Edge,
    Node,
    OperatorRef,
    SystemCommand,
    Workflow,
)


class CompilerConfig(BaseModel):
    """Workflow compiler configuration."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    ir_version: str = Field(
        default="0.1",
        description="Workflow IR schema version emitted by this compiler.",
    )
    compiler_version: str = Field(
        default="0.1",
        description="Compiler implementation version.",
    )


class WorkflowCompiler:
    """Compile Workflow source models into WorkflowIR."""

    def __init__(self, config: CompilerConfig | None = None) -> None:
        self.config = config or CompilerConfig()

    def compile(self, workflow: Workflow) -> CompileResult:
        diagnostics: list[Diagnostic] = []
        workflow_id = workflow.id or generate_workflow_id()
        workflow_version = workflow.version if workflow.version is not None else 1

        node_ids, node_object_ids = self._compile_node_ids(workflow, diagnostics)
        edge_refs = self._compile_edge_refs(
            workflow, node_ids, node_object_ids, diagnostics
        )

        if self._has_errors(diagnostics):
            return CompileResult(diagnostics=diagnostics)

        nodes = self._compile_nodes(workflow, node_ids, diagnostics)
        edges = self._compile_edges(workflow, edge_refs, diagnostics)

        if self._has_errors(diagnostics):
            return CompileResult(diagnostics=diagnostics)

        graph = self._build_graph(nodes, edges)
        self._validate_policies(nodes, graph, diagnostics)
        entry_node_ids = self._infer_entry_node_ids(nodes, graph, diagnostics)
        exit_node_ids = self._infer_exit_node_ids(nodes, graph)
        self._mark_entry_exit_flags(nodes, entry_node_ids, exit_node_ids)

        if self._has_errors(diagnostics):
            return CompileResult(diagnostics=diagnostics)

        workflow_ir = WorkflowIR(
            ir_version=self.config.ir_version,
            compiler_version=self.config.compiler_version,
            workflow_id=workflow_id,
            workflow_version=workflow_version,
            nodes=nodes,
            edges=edges,
            graph=graph,
            entry_node_ids=entry_node_ids,
            exit_node_ids=exit_node_ids,
            policy=workflow.policy,
            metadata=workflow.metadata,
        )
        return CompileResult(workflow_ir=workflow_ir, diagnostics=diagnostics)

    def _compile_node_ids(
        self,
        workflow: Workflow,
        diagnostics: list[Diagnostic],
    ) -> tuple[dict[int, str], dict[int, str]]:
        used_manual_ids: set[str] = set()
        duplicate_ids: set[str] = set()

        for node in workflow.nodes:
            if node.id is None:
                continue
            if node.id in used_manual_ids:
                duplicate_ids.add(node.id)
            used_manual_ids.add(node.id)

        for node_id in sorted(duplicate_ids):
            diagnostics.append(
                Diagnostic(
                    code="NODE_DUPLICATE_ID",
                    severity="error",
                    message=f"Duplicate node id: {node_id}",
                    subject=node_id,
                )
            )

        node_ids: dict[int, str] = {}
        object_id_to_node_id: dict[int, str] = {}
        next_index = 1
        assigned_ids = set(used_manual_ids)

        for node in workflow.nodes:
            if node.id is None:
                node_id, next_index = generate_node_id(assigned_ids, next_index)
            else:
                node_id = node.id
            assigned_ids.add(node_id)
            node_ids[id(node)] = node_id
            object_id_to_node_id[id(node)] = node_id

        return node_ids, object_id_to_node_id

    def _compile_edge_refs(
        self,
        workflow: Workflow,
        node_ids: dict[int, str],
        node_object_ids: dict[int, str],
        diagnostics: list[Diagnostic],
    ) -> list[tuple[Edge, str, str]]:
        known_node_ids = set(node_ids.values())
        edge_refs: list[tuple[Edge, str, str]] = []

        for edge in workflow.edges:
            from_node = self._resolve_node_ref(
                edge.from_node, known_node_ids, node_object_ids
            )
            to_node = self._resolve_node_ref(
                edge.to_node, known_node_ids, node_object_ids
            )

            if from_node is None:
                diagnostics.append(
                    Diagnostic(
                        code="EDGE_UNKNOWN_NODE",
                        severity="error",
                        message="Edge references an unknown source node.",
                        subject=edge.id,
                    )
                )
            if to_node is None:
                diagnostics.append(
                    Diagnostic(
                        code="EDGE_UNKNOWN_NODE",
                        severity="error",
                        message="Edge references an unknown target node.",
                        subject=edge.id,
                    )
                )
            if from_node is not None and to_node is not None:
                edge_refs.append((edge, from_node, to_node))

        return edge_refs

    def _resolve_node_ref(
        self,
        ref: str | Node,
        known_node_ids: set[str],
        node_object_ids: dict[int, str],
    ) -> str | None:
        if isinstance(ref, Node):
            return node_object_ids.get(id(ref))
        if ref in known_node_ids:
            return ref
        return None

    def _compile_nodes(
        self,
        workflow: Workflow,
        node_ids: dict[int, str],
        diagnostics: list[Diagnostic],
    ) -> dict[str, NodeIR]:
        nodes: dict[str, NodeIR] = {}

        for node in workflow.nodes:
            node_id = node_ids[id(node)]
            capability = self._compile_capability(node.capability, node_id, diagnostics)
            if capability is None:
                continue

            if node.input_mapping is not None and not callable(node.input_mapping):
                diagnostics.append(
                    Diagnostic(
                        code="MAPPING_UNSUPPORTED",
                        severity="error",
                        message="Only callable input_mapping is supported.",
                        subject=node_id,
                    )
                )
            if node.output_binding is not None and not callable(node.output_binding):
                diagnostics.append(
                    Diagnostic(
                        code="MAPPING_UNSUPPORTED",
                        severity="error",
                        message="Only callable output_binding is supported.",
                        subject=node_id,
                    )
                )

            nodes[node_id] = NodeIR(
                id=node_id,
                capability=capability,
                input_schema=node.input_schema,
                output_schema=None,
                input_plan=node.input_mapping,
                output_binding=node.output_binding,
                policy=node.policy,
                entry=bool(node.entry),
                metadata=node.metadata,
            )

        return nodes

    def _compile_capability(
        self,
        capability: Callable[..., Any] | str | CapabilityRef | OperatorRef | SystemCommand,
        node_id: str,
        diagnostics: list[Diagnostic],
    ) -> Any | None:
        if callable(capability):
            return capability

        if isinstance(capability, str):
            # TODO: Resolve CapabilityRef through capability registry.
            diagnostics.append(
                Diagnostic(
                    code="CAPABILITY_REF_UNSUPPORTED",
                    severity="error",
                    message="String capability refs are not supported until registry exists.",
                    subject=node_id,
                )
            )
            return None

        if isinstance(capability, CapabilityRef):
            # TODO: Resolve CapabilityRef through capability registry.
            diagnostics.append(
                Diagnostic(
                    code="CAPABILITY_REF_UNSUPPORTED",
                    severity="error",
                    message="CapabilityRef is not supported until registry exists.",
                    subject=node_id,
                )
            )
            return None

        if isinstance(capability, OperatorRef):
            # TODO: Validate OperatorRef through operator registry.
            diagnostics.append(
                Diagnostic(
                    code="OPERATOR_REF_UNSUPPORTED",
                    severity="error",
                    message="OperatorRef is not supported until operator registry exists.",
                    subject=node_id,
                )
            )
            return None

        if isinstance(capability, SystemCommand):
            # TODO: Validate SystemCommand through system command registry.
            diagnostics.append(
                Diagnostic(
                    code="SYSTEM_COMMAND_UNSUPPORTED",
                    severity="error",
                    message="SystemCommand is not supported until system registry exists.",
                    subject=node_id,
                )
            )
            return None

        diagnostics.append(
            Diagnostic(
                code="CAPABILITY_UNSUPPORTED",
                severity="error",
                message="Unsupported node capability.",
                subject=node_id,
            )
        )
        return None

    def _compile_edges(
        self,
        workflow: Workflow,
        edge_refs: list[tuple[Edge, str, str]],
        diagnostics: list[Diagnostic],
    ) -> dict[str, EdgeIR]:
        manual_ids: set[str] = set()
        duplicate_manual_ids: set[str] = set()

        for edge in workflow.edges:
            if edge.id is None:
                continue
            if edge.id in manual_ids:
                duplicate_manual_ids.add(edge.id)
            manual_ids.add(edge.id)

        for edge_id in sorted(duplicate_manual_ids):
            diagnostics.append(
                Diagnostic(
                    code="EDGE_DUPLICATE_ID",
                    severity="error",
                    message=f"Duplicate edge id: {edge_id}",
                    subject=edge_id,
                )
            )

        used_edge_ids = set(manual_ids)
        edges: dict[str, EdgeIR] = {}
        outgoing_order: dict[str, int] = {}

        for edge, from_node, to_node in edge_refs:
            if isinstance(edge.condition, str):
                # TODO: Add safe string condition expression compiler.
                diagnostics.append(
                    Diagnostic(
                        code="STRING_CONDITION_UNSUPPORTED",
                        severity="error",
                        message="String edge conditions are not supported yet.",
                        subject=edge.id,
                    )
                )
                continue

            if edge.condition is not None and not callable(edge.condition):
                diagnostics.append(
                    Diagnostic(
                        code="CONDITION_UNSUPPORTED",
                        severity="error",
                        message="Only callable edge conditions are supported.",
                        subject=edge.id,
                    )
                )
                continue

            if edge.id is None:
                edge_base = f"edge_{from_node}_{to_node}"
                edge_id = generate_edge_id(edge_base, used_edge_ids)
            else:
                edge_id = edge.id

            used_edge_ids.add(edge_id)
            order = outgoing_order.get(from_node, 0)
            outgoing_order[from_node] = order + 1

            edges[edge_id] = EdgeIR(
                id=edge_id,
                from_node=from_node,
                to_node=to_node,
                condition=edge.condition,
                order=order,
                metadata=edge.metadata,
            )

        return edges

    def _build_graph(self, nodes: dict[str, NodeIR], edges: dict[str, EdgeIR]) -> GraphIR:
        outgoing_edges = {node_id: [] for node_id in nodes}
        incoming_edges = {node_id: [] for node_id in nodes}
        predecessors = {node_id: [] for node_id in nodes}
        successors = {node_id: [] for node_id in nodes}

        for edge_id, edge in edges.items():
            outgoing_edges[edge.from_node].append(edge_id)
            incoming_edges[edge.to_node].append(edge_id)
            successors[edge.from_node].append(edge.to_node)
            predecessors[edge.to_node].append(edge.from_node)

        return GraphIR(
            outgoing_edges={
                node_id: tuple(edge_ids) for node_id, edge_ids in outgoing_edges.items()
            },
            incoming_edges={
                node_id: tuple(edge_ids) for node_id, edge_ids in incoming_edges.items()
            },
            predecessors={
                node_id: tuple(dict.fromkeys(node_ids))
                for node_id, node_ids in predecessors.items()
            },
            successors={
                node_id: tuple(dict.fromkeys(node_ids))
                for node_id, node_ids in successors.items()
            },
        )

    def _infer_entry_node_ids(
        self,
        nodes: dict[str, NodeIR],
        graph: GraphIR,
        diagnostics: list[Diagnostic],
    ) -> tuple[str, ...]:
        explicit_entries = tuple(
            node_id for node_id, node in nodes.items() if node.entry
        )
        if explicit_entries:
            return explicit_entries

        inferred_entries = tuple(
            node_id for node_id in nodes if not graph.incoming_edges.get(node_id)
        )
        if not inferred_entries:
            diagnostics.append(
                Diagnostic(
                    code="WF_NO_ENTRY",
                    severity="error",
                    message="Workflow has no entry node.",
                )
            )
        return inferred_entries

    def _infer_exit_node_ids(
        self,
        nodes: dict[str, NodeIR],
        graph: GraphIR,
    ) -> tuple[str, ...]:
        return tuple(node_id for node_id in nodes if not graph.outgoing_edges.get(node_id))

    def _mark_entry_exit_flags(
        self,
        nodes: dict[str, NodeIR],
        entry_node_ids: tuple[str, ...],
        exit_node_ids: tuple[str, ...],
    ) -> None:
        entry_ids = set(entry_node_ids)
        exit_ids = set(exit_node_ids)
        for node_id, node in nodes.items():
            node.entry = node_id in entry_ids
            node.exit = node_id in exit_ids

    def _validate_policies(
        self,
        nodes: dict[str, NodeIR],
        graph: GraphIR,
        diagnostics: list[Diagnostic],
    ) -> None:
        for node_id, node in nodes.items():
            policy = node.policy
            if policy is None:
                continue

            if policy.join is not None:
                incoming_count = len(graph.incoming_edges.get(node_id, ()))
                if policy.join.mode == "n":
                    if policy.join.count is None:
                        diagnostics.append(
                            Diagnostic(
                                code="POLICY_JOIN_INVALID",
                                severity="error",
                                message="JoinPolicy mode n requires count.",
                                subject=node_id,
                            )
                        )
                    elif policy.join.count <= 0:
                        diagnostics.append(
                            Diagnostic(
                                code="POLICY_JOIN_INVALID",
                                severity="error",
                                message="JoinPolicy count must be positive.",
                                subject=node_id,
                            )
                        )
                    elif policy.join.count > incoming_count:
                        diagnostics.append(
                            Diagnostic(
                                code="POLICY_JOIN_INVALID",
                                severity="error",
                                message="JoinPolicy count exceeds incoming edge count.",
                                subject=node_id,
                            )
                        )
                elif policy.join.count is not None:
                    diagnostics.append(
                        Diagnostic(
                            code="POLICY_JOIN_INVALID",
                            severity="error",
                            message="JoinPolicy count is only valid when mode is n.",
                            subject=node_id,
                        )
                    )

            if policy.selection is not None:
                preferred = set(policy.selection.preferred_operator_ids)
                excluded = set(policy.selection.excluded_operator_ids)
                if preferred & excluded:
                    diagnostics.append(
                        Diagnostic(
                            code="POLICY_SELECTION_INVALID",
                            severity="error",
                            message="Selection policy cannot prefer and exclude the same operator.",
                            subject=node_id,
                        )
                    )

            if policy.retry is not None:
                if policy.retry.max_attempts < 1:
                    diagnostics.append(
                        Diagnostic(
                            code="POLICY_RETRY_INVALID",
                            severity="error",
                            message="RetryPolicy max_attempts must be at least 1.",
                            subject=node_id,
                        )
                    )
                if policy.retry.backoff is not None:
                    backoff = policy.retry.backoff
                    if backoff.initial_delay_ms < 0:
                        diagnostics.append(
                            Diagnostic(
                                code="POLICY_BACKOFF_INVALID",
                                severity="error",
                                message="BackoffPolicy initial_delay_ms cannot be negative.",
                                subject=node_id,
                            )
                        )
                    if backoff.max_delay_ms is not None and backoff.max_delay_ms < 0:
                        diagnostics.append(
                            Diagnostic(
                                code="POLICY_BACKOFF_INVALID",
                                severity="error",
                                message="BackoffPolicy max_delay_ms cannot be negative.",
                                subject=node_id,
                            )
                        )
                    if backoff.multiplier <= 0:
                        diagnostics.append(
                            Diagnostic(
                                code="POLICY_BACKOFF_INVALID",
                                severity="error",
                                message="BackoffPolicy multiplier must be positive.",
                                subject=node_id,
                            )
                        )

            if policy.timeout is not None and policy.timeout.timeout_ms <= 0:
                diagnostics.append(
                    Diagnostic(
                        code="POLICY_TIMEOUT_INVALID",
                        severity="error",
                        message="TimeoutPolicy timeout_ms must be positive.",
                        subject=node_id,
                    )
                )

            if policy.resource is not None:
                resource_values = {
                    "max_invocations": policy.resource.max_invocations,
                    "max_tokens": policy.resource.max_tokens,
                    "max_cost": policy.resource.max_cost,
                    "max_tool_calls": policy.resource.max_tool_calls,
                    "max_runtime_ms": policy.resource.max_runtime_ms,
                }
                for field_name, value in resource_values.items():
                    if value is not None and value < 0:
                        diagnostics.append(
                            Diagnostic(
                                code="POLICY_RESOURCE_INVALID",
                                severity="error",
                                message=f"ResourcePolicy {field_name} cannot be negative.",
                                subject=node_id,
                            )
                        )

    def _has_errors(self, diagnostics: list[Diagnostic]) -> bool:
        return any(diagnostic.severity == "error" for diagnostic in diagnostics)

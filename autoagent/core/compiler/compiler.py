from __future__ import annotations

from collections.abc import Callable
from typing import Any

from autoagent.core.compiler.constants import COMPILER_VERSION, WORKFLOW_IR_VERSION
from autoagent.core.compiler.analysis import build_workflow_analysis
from autoagent.core.compiler.diagnostic import CompileResult, Diagnostic
from autoagent.core.compiler.expansion import expand_child_workflows
from autoagent.core.compiler.id_generation import generate_edge_id
from autoagent.core.compiler.snapshot import WorkflowVersionSnapshot
from autoagent.core.compiler.workflow_ir import (
    EdgeIR,
    GraphIR,
    LoopRegionIR,
    NodeIR,
    WorkflowIR,
)
from autoagent.core.operators import CapabilityRegistry, Operator, OperatorRegistry
from autoagent.core.operators.contract import (
    OperatorContract,
    callable_contract,
    value_contract,
)
from autoagent.core.operators import callable_operator_id
from autoagent.core.workflow import (
    CapabilityRef,
    Edge,
    Node,
    OperatorRef,
    SystemCommand,
    Workflow,
)
from autoagent.core.workflow.capability import WAIT_SYSTEM_COMMAND_ID


def _wait_contract(
    *,
    wait_key: str | None = None,
    wait_type: str | None = None,
    payload: dict[str, Any] | None = None,
) -> Any:
    """Signature-only definition of the framework wait command contract."""

    raise RuntimeError("System wait contracts are compiled, not invoked.")


_WAIT_OPERATOR_CONTRACT, _ = callable_contract(_wait_contract)


class WorkflowCompiler:
    """Compile Workflow source models into WorkflowIR."""

    def __init__(
        self,
        *,
        capability_registry: CapabilityRegistry | None = None,
        operator_registry: OperatorRegistry | None = None,
        require_operator_bindings: bool = True,
    ) -> None:
        """Create a compiler with optional application registry visibility.

        Direct callables require no registry. CapabilityRef and OperatorRef are
        validated against these registries, but remain references in WorkflowIR
        so NodeExecutor can resolve the current implementation at execution time.
        """

        self.capability_registry = capability_registry
        self.operator_registry = operator_registry
        self.require_operator_bindings = require_operator_bindings

    def compile(self, workflow: Workflow) -> CompileResult:
        diagnostics: list[Diagnostic] = []
        workflow_id = workflow.id
        workflow_version = workflow.version if workflow.version is not None else 1
        workflow = expand_child_workflows(workflow, diagnostics)

        node_ids, node_object_ids = self._compile_node_ids(workflow, diagnostics)
        edge_refs = self._compile_edge_refs(
            workflow, node_ids, node_object_ids, diagnostics
        )

        nodes = self._compile_nodes(workflow, node_ids, diagnostics)
        edges = self._compile_edges(workflow, edge_refs, diagnostics)

        # Continue graph-level validation on the compilable subgraph so one
        # preview can report independent node, edge, policy, and loop errors.
        # Edges touching a node that failed compilation are excluded because
        # that node's own diagnostic already explains why it is unavailable.
        graph_edges = {
            edge_id: edge
            for edge_id, edge in edges.items()
            if edge.from_node in nodes and edge.to_node in nodes
        }
        graph = self._build_graph(nodes, graph_edges)
        entry_node_ids = (
            self._infer_entry_node_ids(nodes, graph, diagnostics) if nodes else ()
        )
        if nodes:
            self._compile_loop_regions(
                nodes=nodes,
                edges=graph_edges,
                graph=graph,
                entry_node_ids=entry_node_ids,
                diagnostics=diagnostics,
            )
            self._validate_policies(nodes, graph_edges, diagnostics)
        if not self._has_errors(diagnostics):
            self._compile_final_output_contracts(nodes, graph_edges, diagnostics)
        exit_node_ids = self._infer_exit_node_ids(nodes, graph)
        self._mark_entry_exit_flags(nodes, entry_node_ids, exit_node_ids)

        finalized_diagnostics = self._finalize_diagnostics(
            workflow_id=workflow_id,
            diagnostics=diagnostics,
            nodes=nodes,
            edges=edges,
        )
        analysis = build_workflow_analysis(
            workflow=workflow,
            workflow_id=workflow_id,
            workflow_version=workflow_version,
            nodes=nodes,
            edges=edges,
            graph=graph,
            entry_node_ids=entry_node_ids,
            exit_node_ids=exit_node_ids,
            complete=not self._has_errors(finalized_diagnostics),
        )

        if self._has_errors(finalized_diagnostics):
            return CompileResult(
                workflow_id=workflow_id,
                workflow_version=workflow_version,
                analysis=analysis,
                diagnostics=finalized_diagnostics,
            )

        workflow_ir = WorkflowIR(
            ir_version=WORKFLOW_IR_VERSION,
            compiler_version=COMPILER_VERSION,
            workflow_id=workflow_id,
            workflow_version=workflow_version,
            # Snapshot computation needs the complete compiled graph. This
            # placeholder exists only inside this method and is replaced below.
            definition_hash="pending",
            name=workflow.name,
            description=workflow.description,
            policy=workflow.policy,
            nodes=nodes,
            edges=edges,
            graph=graph,
            entry_node_ids=entry_node_ids,
            exit_node_ids=exit_node_ids,
            metadata=workflow.metadata,
        )
        snapshot = WorkflowVersionSnapshot.from_workflow_ir(workflow_ir)
        workflow_ir.definition_hash = snapshot.definition_hash
        return CompileResult(
            workflow_id=workflow_id,
            workflow_version=workflow_version,
            analysis=analysis,
            workflow_ir=workflow_ir,
            workflow_snapshot=snapshot,
            diagnostics=finalized_diagnostics,
        )

    def _compile_node_ids(
        self,
        workflow: Workflow,
        diagnostics: list[Diagnostic],
    ) -> tuple[dict[int, str], dict[int, str]]:
        used_manual_ids: set[str] = set()
        duplicate_ids: set[str] = set()

        for node in workflow.nodes:
            if not node.id:
                diagnostics.append(
                    Diagnostic(
                        code="NODE_ID_REQUIRED",
                        severity="error",
                        message="Every node must define a non-empty node id.",
                    )
                )
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
        for node in workflow.nodes:
            if not node.id:
                continue
            node_id = node.id
            node_ids[id(node)] = node_id
            object_id_to_node_id[id(node)] = node_id

        return node_ids, object_id_to_node_id

    def _compile_edge_refs(
        self,
        workflow: Workflow,
        node_ids: dict[int, str],
        node_object_ids: dict[int, str],
        diagnostics: list[Diagnostic],
    ) -> list[tuple[int, Edge, str, str]]:
        known_node_ids = set(node_ids.values())
        edge_refs: list[tuple[int, Edge, str, str]] = []

        for edge_index, edge in enumerate(workflow.edges):
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
                        metadata={"object_type": "edge", "source_index": edge_index},
                    )
                )
            if to_node is None:
                diagnostics.append(
                    Diagnostic(
                        code="EDGE_UNKNOWN_NODE",
                        severity="error",
                        message="Edge references an unknown target node.",
                        subject=edge.id,
                        metadata={"object_type": "edge", "source_index": edge_index},
                    )
                )
            if from_node is not None and to_node is not None:
                edge_refs.append((edge_index, edge, from_node, to_node))

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
        direct_operators: dict[int, Operator] = {}

        for node in workflow.nodes:
            if id(node) not in node_ids:
                continue
            node_id = node_ids[id(node)]
            source_capability = node.capability
            capability = self._compile_capability(
                source_capability,
                node_id,
                diagnostics,
            )
            if capability is None:
                continue
            if callable(capability):
                callable_identity = id(capability)
                capability = direct_operators.get(callable_identity)
                if capability is None:
                    capability = Operator.from_callable(
                        source_capability,
                        operator_id=(
                            f"{callable_operator_id(source_capability)}"
                            f"@{node_id}"
                        ),
                    )
                    direct_operators[callable_identity] = capability

            contract = self._binding_contract(capability)
            if contract is None:
                diagnostics.append(
                    Diagnostic(
                        code="OPERATOR_CONTRACT_UNAVAILABLE",
                        severity="error",
                        message="Compiled node capability has no Operator contract.",
                        subject=node_id,
                    )
                )
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
                local_id=node._local_id or node_id,
                scope_node_ids=node._scope_node_ids,
                workflow_path=node._workflow_path,
                name=node.name,
                description=node.description,
                capability=capability,
                input_contract=contract.input,
                operator_output_contract=contract.output,
                output_contract=contract.output,
                input_plan=node.input_mapping,
                output_binding=node.output_binding,
                stream_user_event_mapping=node.stream_user_event_mapping,
                user_event_mapping=node.user_event_mapping,
                policy=node.policy,
                entry=bool(node.entry),
                metadata=node.metadata,
            )

        return nodes

    def _compile_capability(
        self,
        capability: (
            Callable[..., Any]
            | Operator
            | str
            | CapabilityRef
            | OperatorRef
            | SystemCommand
        ),
        node_id: str,
        diagnostics: list[Diagnostic],
    ) -> Any | None:
        if isinstance(capability, Operator):
            return capability

        if callable(capability):
            _, issues = callable_contract(capability)
            for issue in issues:
                if issue.severity == "error":
                    diagnostics.append(
                        Diagnostic(
                            code="OPERATOR_CONTRACT_INVALID",
                            severity="error",
                            message=issue.message,
                            subject=node_id,
                        )
                    )
            if any(issue.severity == "error" for issue in issues):
                return None
            return capability

        if isinstance(capability, str):
            capability = CapabilityRef(id=capability)

        if isinstance(capability, CapabilityRef):
            if (
                self.capability_registry is None
                or not self.capability_registry.contains(capability.id)
            ):
                diagnostics.append(
                    Diagnostic(
                        code="CAPABILITY_NOT_REGISTERED",
                        severity="error",
                        message=f"Capability is not registered: {capability.id}",
                        subject=node_id,
                    )
                )
                return None
            operators = (
                self.operator_registry.for_capability(
                    capability.id,
                    include_disabled=True,
                )
                if self.operator_registry is not None
                else ()
            )
            if not operators and self.require_operator_bindings:
                diagnostics.append(
                    Diagnostic(
                        code="CAPABILITY_HAS_NO_OPERATOR",
                        severity="error",
                        message=f"Capability has no registered Operator: {capability.id}",
                        subject=node_id,
                    )
                )
                return None
            return capability

        if isinstance(capability, OperatorRef):
            if (
                self.operator_registry is None
                or not self.operator_registry.contains(capability.id)
            ):
                diagnostics.append(
                    Diagnostic(
                        code="OPERATOR_NOT_REGISTERED",
                        severity="error",
                        message=f"Operator is not registered: {capability.id}",
                        subject=node_id,
                    )
                )
                return None
            return capability

        if isinstance(capability, SystemCommand):
            if capability.id != WAIT_SYSTEM_COMMAND_ID:
                diagnostics.append(
                    Diagnostic(
                        code="SYSTEM_COMMAND_UNSUPPORTED",
                        severity="error",
                        message=(
                            "Unsupported SystemCommand. V1 accepts only "
                            f"'{WAIT_SYSTEM_COMMAND_ID}'."
                        ),
                        subject=node_id,
                    )
                )
                return None
            if capability.command is not None:
                diagnostics.append(
                    Diagnostic(
                        code="SYSTEM_COMMAND_CONFIG_UNSUPPORTED",
                        severity="error",
                        message="SystemCommand.command is reserved and unsupported in V1.",
                        subject=node_id,
                    )
                )
                return None
            return capability

        diagnostics.append(
            Diagnostic(
                code="CAPABILITY_UNSUPPORTED",
                severity="error",
                message="Unsupported node capability.",
                subject=node_id,
            )
        )
        return None

    def _binding_contract(self, binding: Any) -> OperatorContract | None:
        if isinstance(binding, Operator):
            return binding.contract
        if callable(binding):
            contract, _ = callable_contract(binding)
            return contract
        if isinstance(binding, CapabilityRef):
            capability = (
                self.capability_registry.get(binding.id)
                if self.capability_registry is not None
                else None
            )
            if capability is None:
                return None
            return capability.contract
        if isinstance(binding, OperatorRef):
            operator = (
                self.operator_registry.get(binding.id)
                if self.operator_registry is not None
                else None
            )
            if operator is None:
                return None
            return operator.contract
        if isinstance(binding, SystemCommand) and binding.id == WAIT_SYSTEM_COMMAND_ID:
            return _WAIT_OPERATOR_CONTRACT
        return None

    def _compile_edges(
        self,
        workflow: Workflow,
        edge_refs: list[tuple[int, Edge, str, str]],
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

        for edge_index, edge, from_node, to_node in edge_refs:
            if edge.id is None:
                edge_base = f"edge_{from_node}_{to_node}"
                edge_id = generate_edge_id(edge_base, used_edge_ids)
            else:
                edge_id = edge.id
            used_edge_ids.add(edge_id)

            if isinstance(edge.condition, str):
                # TODO: Add safe string condition expression compiler.
                diagnostics.append(
                    Diagnostic(
                        code="STRING_CONDITION_UNSUPPORTED",
                        severity="error",
                        message="String edge conditions are not supported yet.",
                        subject=edge_id,
                        metadata={"object_type": "edge", "source_index": edge_index},
                    )
                )
                continue

            if edge.condition is not None and not callable(edge.condition):
                diagnostics.append(
                    Diagnostic(
                        code="CONDITION_UNSUPPORTED",
                        severity="error",
                        message="Only callable edge conditions are supported.",
                        subject=edge_id,
                        metadata={"object_type": "edge", "source_index": edge_index},
                    )
                )
                continue

            order = outgoing_order.get(from_node, 0)
            outgoing_order[from_node] = order + 1

            edges[edge_id] = EdgeIR(
                id=edge_id,
                local_id=edge._local_id or edge_id,
                local_from_node=edge._local_from_node or from_node,
                local_to_node=edge._local_to_node or to_node,
                scope_node_ids=edge._scope_node_ids,
                workflow_path=edge._workflow_path,
                from_node=from_node,
                to_node=to_node,
                condition=edge.condition,
                policy=edge.policy,
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
        inferred_entries = [
            node_id for node_id in nodes if not graph.incoming_edges.get(node_id)
        ]
        entry_ids = set(inferred_entries)

        # Explicit entry markers assert the same property inferred from the
        # graph: an entry has no incoming edge. They never replace inference,
        # so every other zero-incoming node remains an entry as well.
        for node_id, node in nodes.items():
            if not node.entry or node_id in entry_ids:
                continue
            diagnostics.append(
                Diagnostic(
                    code="WF_ENTRY_HAS_INCOMING_EDGE",
                    severity="error",
                    message="An explicit entry node cannot have incoming edges.",
                    subject=node_id,
                    metadata={
                        "incoming_edge_ids": list(
                            graph.incoming_edges.get(node_id, ())
                        ),
                    },
                )
            )

        if not inferred_entries:
            diagnostics.append(
                Diagnostic(
                    code="WF_NO_ENTRY",
                    severity="error",
                    message="Workflow has no entry node.",
                )
            )
        return tuple(inferred_entries)

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

    def _compile_loop_regions(
        self,
        *,
        nodes: dict[str, NodeIR],
        edges: dict[str, EdgeIR],
        graph: GraphIR,
        entry_node_ids: tuple[str, ...],
        diagnostics: list[Diagnostic],
    ) -> None:
        """Compile reducible natural loops and their nesting relationship.

        A back edge is an edge whose target dominates its source. Removing all
        such edges must leave a DAG. This gives runtime an acyclic body for each
        iteration while preserving ordinary fan-out and complete fan-in inside
        the loop. Overlapping loops must be strictly nested; irreducible cycles
        are rejected because they have no unambiguous execution scope.
        """

        node_order = {node_id: index for index, node_id in enumerate(nodes)}
        reachable: set[str] = set()
        pending = list(entry_node_ids)
        while pending:
            node_id = pending.pop()
            if node_id in reachable:
                continue
            reachable.add(node_id)
            pending.extend(graph.successors.get(node_id, ()))

        dominators: dict[str, set[str]] = {}
        for node_id in nodes:
            dominators[node_id] = (
                {node_id} if node_id in entry_node_ids else set(reachable)
            )
        changed = True
        while changed:
            changed = False
            for node_id in nodes:
                if node_id not in reachable or node_id in entry_node_ids:
                    continue
                predecessors = [
                    predecessor
                    for predecessor in graph.predecessors.get(node_id, ())
                    if predecessor in reachable
                ]
                inherited = (
                    set.intersection(*(dominators[item] for item in predecessors))
                    if predecessors
                    else set()
                )
                updated = {node_id} | inherited
                if updated != dominators[node_id]:
                    dominators[node_id] = updated
                    changed = True

        back_edges_by_header: dict[str, list[str]] = {}
        all_back_edge_ids: set[str] = set()
        for edge_id, edge in edges.items():
            if edge.from_node not in reachable or edge.to_node not in reachable:
                continue
            if edge.to_node in dominators[edge.from_node]:
                back_edges_by_header.setdefault(edge.to_node, []).append(edge_id)
                all_back_edge_ids.add(edge_id)

        # Report a concrete illegal entry before the more general irreducible
        # cycle diagnostic. This is both more actionable and lets diagrams mark
        # the exact edge that prevents a unique loop header.
        for component in self._strongly_connected_components(graph, tuple(nodes)):
            component_set = set(component)
            is_cycle = len(component) > 1 or any(
                node_id in graph.successors.get(node_id, ())
                for node_id in component
            )
            if not is_cycle:
                continue
            external_entries = [
                edge_id
                for edge_id, edge in edges.items()
                if edge.from_node not in component_set
                and edge.to_node in component_set
            ]
            entry_targets = {
                edges[edge_id].to_node for edge_id in external_entries
            } | (set(entry_node_ids) & component_set)
            if len(entry_targets) <= 1:
                continue
            canonical = min(entry_targets, key=node_order.__getitem__)
            invalid_edges = [
                edge_id
                for edge_id in external_entries
                if edges[edge_id].to_node != canonical
            ]
            diagnostics.append(
                Diagnostic(
                    code="LOOP_ENTRY_INVALID",
                    severity="error",
                    message=(
                        "Every edge entering a loop must target its unique "
                        f"header node {canonical}."
                    ),
                    subject=invalid_edges[0] if invalid_edges else canonical,
                    metadata={
                        "object_type": "edge" if invalid_edges else "node",
                        "loop_node_ids": sorted(
                            component_set, key=node_order.__getitem__
                        ),
                        "expected_entry_node_id": canonical,
                    },
                )
            )
            return

        # Any cycle left after removing dominance back edges is irreducible.
        remaining_indegree = {node_id: 0 for node_id in reachable}
        for edge_id, edge in edges.items():
            if (
                edge_id not in all_back_edge_ids
                and edge.from_node in reachable
                and edge.to_node in reachable
            ):
                remaining_indegree[edge.to_node] += 1
        acyclic_pending = [
            node_id
            for node_id in nodes
            if node_id in reachable and remaining_indegree[node_id] == 0
        ]
        visited = 0
        while acyclic_pending:
            node_id = acyclic_pending.pop(0)
            visited += 1
            for edge_id in graph.outgoing_edges.get(node_id, ()):
                if edge_id in all_back_edge_ids:
                    continue
                target = edges[edge_id].to_node
                if target not in remaining_indegree:
                    continue
                remaining_indegree[target] -= 1
                if remaining_indegree[target] == 0:
                    acyclic_pending.append(target)
        if visited != len(reachable):
            diagnostics.append(
                Diagnostic(
                    code="LOOP_IRREDUCIBLE",
                    severity="error",
                    message=(
                        "Workflow contains a cycle that cannot be represented as "
                        "a natural loop with a dominating header."
                    ),
                )
            )
            return

        candidates: list[dict[str, Any]] = []
        for header_node_id, back_edge_ids in back_edges_by_header.items():
            loop_nodes = {header_node_id}
            reverse_pending = [edges[edge_id].from_node for edge_id in back_edge_ids]
            while reverse_pending:
                node_id = reverse_pending.pop()
                if node_id in loop_nodes:
                    continue
                loop_nodes.add(node_id)
                reverse_pending.extend(graph.predecessors.get(node_id, ()))
            candidates.append(
                {
                    "header": header_node_id,
                    "nodes": loop_nodes,
                    "back_edges": tuple(back_edge_ids),
                }
            )

        for left_index, left in enumerate(candidates):
            for right in candidates[left_index + 1 :]:
                overlap = left["nodes"] & right["nodes"]
                if overlap and not (
                    left["nodes"] < right["nodes"]
                    or right["nodes"] < left["nodes"]
                ):
                    diagnostics.append(
                        Diagnostic(
                            code="LOOP_OVERLAP_INVALID",
                            severity="error",
                            message=(
                                "Natural loops may be disjoint or strictly nested; "
                                "overlapping loop bodies are ambiguous."
                            ),
                            subject=",".join(sorted(overlap, key=node_order.__getitem__)),
                        )
                    )
                    return

        candidates.sort(key=lambda item: node_order[item["header"]])
        for index, candidate in enumerate(candidates, start=1):
            candidate["id"] = f"loop_{index}"

        for candidate in candidates:
            supersets = [
                other
                for other in candidates
                if candidate["nodes"] < other["nodes"]
            ]
            candidate["parent"] = (
                min(supersets, key=lambda item: len(item["nodes"]))
                if supersets
                else None
            )

        loop_regions: dict[str, LoopRegionIR] = {}
        for candidate in candidates:
            node_set = candidate["nodes"]
            header_node_id = candidate["header"]
            node_ids = tuple(sorted(node_set, key=node_order.__getitem__))
            internal_edge_ids = tuple(
                edge_id
                for edge_id, edge in edges.items()
                if edge.from_node in node_set and edge.to_node in node_set
            )
            external_entry_edge_ids = tuple(
                edge_id
                for edge_id, edge in edges.items()
                if edge.from_node not in node_set and edge.to_node in node_set
            )
            invalid_entries = tuple(
                edge_id
                for edge_id in external_entry_edge_ids
                if edges[edge_id].to_node != header_node_id
            )
            workflow_entries = set(entry_node_ids) & node_set
            if invalid_entries or any(
                node_id != header_node_id for node_id in workflow_entries
            ):
                subject = invalid_entries[0] if invalid_entries else next(
                    node_id
                    for node_id in workflow_entries
                    if node_id != header_node_id
                )
                diagnostics.append(
                    Diagnostic(
                        code="LOOP_ENTRY_INVALID",
                        severity="error",
                        message=(
                            "Every edge entering a natural loop must target its "
                            f"header node {header_node_id}."
                        ),
                        subject=subject,
                        metadata={
                            "object_type": "edge" if invalid_entries else "node",
                            "loop_node_ids": list(node_ids),
                            "expected_entry_node_id": header_node_id,
                        },
                    )
                )
                continue
            exit_edge_ids = tuple(
                edge_id
                for edge_id, edge in edges.items()
                if edge.from_node in node_set and edge.to_node not in node_set
            )
            parent = candidate["parent"]
            children = tuple(
                child["id"]
                for child in candidates
                if child["parent"] is candidate
            )
            depth = 0
            ancestor = parent
            while ancestor is not None:
                depth += 1
                ancestor = ancestor["parent"]
            loop_regions[candidate["id"]] = LoopRegionIR(
                id=candidate["id"],
                node_ids=node_ids,
                header_node_id=header_node_id,
                internal_edge_ids=internal_edge_ids,
                external_entry_edge_ids=external_entry_edge_ids,
                back_edge_ids=candidate["back_edges"],
                exit_edge_ids=exit_edge_ids,
                parent_loop_region_id=parent["id"] if parent is not None else None,
                child_loop_region_ids=children,
                depth=depth,
            )

        node_loop_stacks: dict[str, tuple[str, ...]] = {}
        for node_id in nodes:
            containing = [
                region
                for region in loop_regions.values()
                if node_id in region.node_ids
            ]
            containing.sort(key=lambda region: region.depth)
            if containing:
                node_loop_stacks[node_id] = tuple(
                    region.id for region in containing
                )

        graph.loop_regions = loop_regions
        graph.node_loop_stacks = node_loop_stacks

    def _strongly_connected_components(
        self,
        graph: GraphIR,
        node_ids: tuple[str, ...],
    ) -> list[tuple[str, ...]]:
        index = 0
        indices: dict[str, int] = {}
        lowlinks: dict[str, int] = {}
        stack: list[str] = []
        on_stack: set[str] = set()
        components: list[tuple[str, ...]] = []

        def visit(node_id: str) -> None:
            nonlocal index
            indices[node_id] = index
            lowlinks[node_id] = index
            index += 1
            stack.append(node_id)
            on_stack.add(node_id)

            for successor_id in graph.successors.get(node_id, ()):
                if successor_id not in indices:
                    visit(successor_id)
                    lowlinks[node_id] = min(
                        lowlinks[node_id], lowlinks[successor_id]
                    )
                elif successor_id in on_stack:
                    lowlinks[node_id] = min(
                        lowlinks[node_id], indices[successor_id]
                    )

            if lowlinks[node_id] != indices[node_id]:
                return

            component: list[str] = []
            while True:
                member = stack.pop()
                on_stack.remove(member)
                component.append(member)
                if member == node_id:
                    break
            components.append(tuple(component))

        for node_id in node_ids:
            if node_id not in indices:
                visit(node_id)
        return components

    def _validate_policies(
        self,
        nodes: dict[str, NodeIR],
        edges: dict[str, EdgeIR],
        diagnostics: list[Diagnostic],
    ) -> None:
        for node_id, node in nodes.items():
            policy = node.policy
            if policy is None:
                continue

            if isinstance(node.capability, SystemCommand):
                diagnostics.append(
                    Diagnostic(
                        code="SYSTEM_COMMAND_POLICY_UNSUPPORTED",
                        severity="error",
                        message="SystemCommand wait nodes do not support NodePolicy in V1.",
                        subject=node_id,
                    )
                )
                continue

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
                if not isinstance(node.capability, CapabilityRef):
                    diagnostics.append(
                        Diagnostic(
                            code="POLICY_SELECTION_INVALID",
                            severity="error",
                            message="Selection policy requires a CapabilityRef node.",
                            subject=node_id,
                        )
                    )
                elif self.operator_registry is not None:
                    for operator_id in preferred | excluded:
                        registered = self.operator_registry.get(operator_id)
                        if registered is None:
                            diagnostics.append(
                                Diagnostic(
                                    code="POLICY_SELECTION_OPERATOR_UNKNOWN",
                                    severity="error",
                                    message=(
                                        "Selection policy references an unknown "
                                        f"Operator: {operator_id}"
                                    ),
                                    subject=node_id,
                                )
                            )
                        elif registered.capability_id != node.capability.id:
                            diagnostics.append(
                                Diagnostic(
                                    code="POLICY_SELECTION_OPERATOR_MISMATCH",
                                    severity="error",
                                    message=(
                                        f"Operator {operator_id} does not implement "
                                        f"Capability {node.capability.id}."
                                    ),
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

            if policy.recovery is not None and policy.recovery.mode == "idempotent":
                parameter_names = {
                    parameter.name for parameter in node.input_contract.parameters
                }
                if "idempotency_key" not in parameter_names:
                    diagnostics.append(
                        Diagnostic(
                            code="POLICY_RECOVERY_IDEMPOTENCY_KEY_REQUIRED",
                            severity="error",
                            message=(
                                "Idempotent RecoveryPolicy requires the Operator "
                                "input contract to accept idempotency_key."
                            ),
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

            if policy.replication is not None:
                if policy.replication.count <= 0:
                    diagnostics.append(
                        Diagnostic(
                            code="POLICY_REPLICATION_INVALID",
                            severity="error",
                            message="ReplicationPolicy count must be positive.",
                            subject=node_id,
                        )
                    )
                if (
                    policy.replication.output_aggregator is not None
                    and not callable(policy.replication.output_aggregator)
                ):
                    diagnostics.append(
                        Diagnostic(
                            code="POLICY_REPLICATION_INVALID",
                            severity="error",
                            message="ReplicationPolicy output_aggregator must be callable.",
                            subject=node_id,
                        )
                    )
                if (
                    policy.replication.max_parallelism is not None
                    and policy.replication.max_parallelism <= 0
                ):
                    diagnostics.append(
                        Diagnostic(
                            code="POLICY_REPLICATION_INVALID",
                            severity="error",
                            message="ReplicationPolicy max_parallelism must be positive.",
                            subject=node_id,
                        )
                    )

            if policy.max_concurrency is not None and policy.max_concurrency <= 0:
                diagnostics.append(
                    Diagnostic(
                        code="POLICY_CONCURRENCY_INVALID",
                        severity="error",
                        message="NodePolicy max_concurrency must be positive.",
                        subject=node_id,
                    )
                )

            if policy.resource is not None:
                resource_values = {
                    "max_node_executions_per_invocation": (
                        policy.resource.max_node_executions_per_invocation
                    ),
                    "max_operator_attempts_per_invocation": (
                        policy.resource.max_operator_attempts_per_invocation
                    ),
                    "max_runtime_ms_per_invocation": (
                        policy.resource.max_runtime_ms_per_invocation
                    ),
                }
                for field_name, value in resource_values.items():
                    if value is not None and value <= 0:
                        diagnostics.append(
                            Diagnostic(
                                code="POLICY_RESOURCE_INVALID",
                                severity="error",
                                message=f"ResourcePolicy {field_name} must be positive.",
                                subject=node_id,
                            )
                        )

        for edge_id, edge in edges.items():
            policy = edge.policy
            if policy is None or policy.map is None:
                continue

            map_policy = policy.map
            if map_policy.item_selector is not None and not callable(
                map_policy.item_selector
            ):
                diagnostics.append(
                    Diagnostic(
                        code="POLICY_MAP_INVALID",
                        severity="error",
                        message="MapPolicy item_selector must be callable.",
                        subject=edge_id,
                    )
                )

            if map_policy.output_aggregator is not None and not callable(
                map_policy.output_aggregator
            ):
                diagnostics.append(
                    Diagnostic(
                        code="POLICY_MAP_INVALID",
                        severity="error",
                        message="MapPolicy output_aggregator must be callable.",
                        subject=edge_id,
                    )
                )
            if map_policy.max_parallelism is not None and map_policy.max_parallelism <= 0:
                diagnostics.append(
                    Diagnostic(
                        code="POLICY_MAP_INVALID",
                        severity="error",
                        message="MapPolicy max_parallelism must be positive.",
                        subject=edge_id,
                    )
                )

            target = nodes[edge.to_node]
            if isinstance(target.capability, SystemCommand):
                diagnostics.append(
                    Diagnostic(
                        code="SYSTEM_COMMAND_MAP_UNSUPPORTED",
                        severity="error",
                        message="MapPolicy cannot target a SystemCommand wait node in V1.",
                        subject=edge_id,
                    )
                )
            incoming_count = sum(
                1 for candidate in edges.values() if candidate.to_node == edge.to_node
            )
            if incoming_count != 1:
                diagnostics.append(
                    Diagnostic(
                        code="POLICY_MAP_FAN_IN_UNSUPPORTED",
                        severity="error",
                        message=(
                            "A MapPolicy target must have exactly one incoming edge; "
                            "map and complete fan-in cannot share one target in V1."
                        ),
                        subject=edge_id,
                    )
                )
            if target.input_plan is not None:
                diagnostics.append(
                    Diagnostic(
                        code="POLICY_MAP_INPUT_MAPPING_CONFLICT",
                        severity="error",
                        message=(
                            "A MapPolicy item_selector owns per-item input construction; "
                            "the target node cannot also define input_mapping."
                        ),
                        subject=edge_id,
                    )
                )
            if target.policy is not None and target.policy.replication is not None:
                diagnostics.append(
                    Diagnostic(
                        code="POLICY_MAP_REPLICATION_CONFLICT",
                        severity="error",
                        message="MapPolicy and ReplicationPolicy cannot target the same node.",
                        subject=edge_id,
                    )
                )

    def _compile_final_output_contracts(
        self,
        nodes: dict[str, NodeIR],
        edges: dict[str, EdgeIR],
        diagnostics: list[Diagnostic],
    ) -> None:
        """Derive NodeExecution outputs after map/replication aggregation."""

        for node_id, node in nodes.items():
            replication = node.policy.replication if node.policy is not None else None
            if replication is None:
                continue
            if replication.output_aggregator is None:
                item_annotation = node.operator_output_contract.annotation
                node.output_contract = value_contract(list[item_annotation])
                continue
            contract, _ = callable_contract(replication.output_aggregator)
            node.output_contract = contract.output
            if not contract.output.known:
                diagnostics.append(
                    Diagnostic(
                        code="POLICY_AGGREGATOR_OUTPUT_UNVERIFIED",
                        severity="warning",
                        message=(
                            "Replication output_aggregator has no concrete return "
                            "annotation; final node output cannot be validated."
                        ),
                        subject=node_id,
                    )
                )

        for edge_id, edge in edges.items():
            map_policy = edge.policy.map if edge.policy is not None else None
            if map_policy is None:
                continue
            target = nodes[edge.to_node]
            if map_policy.output_aggregator is None:
                item_annotation = target.operator_output_contract.annotation
                target.output_contract = value_contract(list[item_annotation])
                continue
            contract, _ = callable_contract(map_policy.output_aggregator)
            target.output_contract = contract.output
            if not contract.output.known:
                diagnostics.append(
                    Diagnostic(
                        code="POLICY_AGGREGATOR_OUTPUT_UNVERIFIED",
                        severity="warning",
                        message=(
                            "Map output_aggregator has no concrete return annotation; "
                            "final node output cannot be validated."
                        ),
                        subject=edge_id,
                    )
                )

    def _has_errors(self, diagnostics: list[Diagnostic]) -> bool:
        return any(diagnostic.severity == "error" for diagnostic in diagnostics)

    def _finalize_diagnostics(
        self,
        *,
        workflow_id: str,
        diagnostics: list[Diagnostic],
        nodes: dict[str, NodeIR],
        edges: dict[str, EdgeIR],
    ) -> list[Diagnostic]:
        """Attach stable agent-facing context and deterministically order results."""

        finalized: list[Diagnostic] = []
        for diagnostic in diagnostics:
            object_id = diagnostic.object_id or diagnostic.subject
            object_type = diagnostic.object_type or _diagnostic_object_type(
                diagnostic.code,
                object_id=object_id,
                node_ids=nodes.keys(),
                edge_ids=edges.keys(),
            )
            source_index = diagnostic.source_index
            if source_index is None:
                raw_source_index = diagnostic.metadata.get("source_index")
                if isinstance(raw_source_index, int) and raw_source_index >= 0:
                    source_index = raw_source_index

            finalized.append(
                diagnostic.model_copy(
                    update={
                        "workflow_id": diagnostic.workflow_id or workflow_id,
                        "object_type": object_type,
                        "object_id": object_id,
                        "field": diagnostic.field
                        or _diagnostic_field(diagnostic.code, diagnostic.message),
                        "hint": diagnostic.hint
                        or _diagnostic_hint(diagnostic.code),
                        "source_index": source_index,
                    },
                )
            )

        severity_order = {"error": 0, "warning": 1, "info": 2}
        object_order = {"workflow": 0, "node": 1, "edge": 2, None: 3}
        return sorted(
            finalized,
            key=lambda item: (
                severity_order[item.severity],
                item.source_index if item.source_index is not None else 2**31,
                object_order[item.object_type],
                item.object_id or "",
                item.field or "",
                item.code,
                item.message,
            ),
        )


def _diagnostic_object_type(
    code: str,
    *,
    object_id: str | None,
    node_ids: Any,
    edge_ids: Any,
) -> str:
    if code in {"WF_NO_ENTRY", "LOOP_IRREDUCIBLE", "LOOP_OVERLAP_INVALID"}:
        return "workflow"
    if code == "LOOP_ENTRY_INVALID":
        return "edge"
    if code == "SUBWORKFLOW_RECURSION":
        return "workflow"
    if code.startswith("EDGE_") or code in {
        "CONDITION_UNSUPPORTED",
        "STRING_CONDITION_UNSUPPORTED",
        "SUBWORKFLOW_MAP_UNSUPPORTED",
        "SYSTEM_COMMAND_MAP_UNSUPPORTED",
    }:
        return "edge"
    if code.startswith("POLICY_MAP_"):
        return "edge"
    if object_id is not None and object_id in edge_ids:
        return "edge"
    if object_id is not None and object_id in node_ids:
        return "node"
    if code.startswith("WF_"):
        return "node"
    return "node" if object_id is not None else "workflow"


def _diagnostic_field(code: str, message: str) -> str | None:
    if code in {"NODE_ID_REQUIRED", "NODE_DUPLICATE_ID"}:
        return "id"
    if code == "EDGE_DUPLICATE_ID":
        return "id"
    if code == "EDGE_UNKNOWN_NODE":
        return "from_node" if "source" in message.lower() else "to_node"
    if code in {"CONDITION_UNSUPPORTED", "STRING_CONDITION_UNSUPPORTED"}:
        return "condition"
    if code == "MAPPING_UNSUPPORTED":
        return "output_binding" if "output_binding" in message else "input_mapping"
    if code.startswith("POLICY_") or code.endswith("_POLICY_UNSUPPORTED"):
        return "policy"
    if code.startswith("CAPABILITY_") or code.startswith("OPERATOR_"):
        return "capability"
    if code == "SYSTEM_COMMAND_CONFIG_UNSUPPORTED":
        return "capability.command"
    if code.startswith("SUBWORKFLOW_"):
        if "_ENTRY_" in code:
            return "child_entry_node_id"
        if "_EXIT_" in code:
            return "child_exit_node_id"
        return "capability"
    if code == "WF_ENTRY_HAS_INCOMING_EDGE":
        return "entry"
    return None


_DIAGNOSTIC_HINTS = {
    "NODE_ID_REQUIRED": "Pass a stable non-empty node_id to workflow.add_node().",
    "NODE_DUPLICATE_ID": "Give every Node in the Workflow a unique stable id.",
    "EDGE_DUPLICATE_ID": "Give manually identified Edges unique ids.",
    "EDGE_UNKNOWN_NODE": "Correct the Edge endpoint to reference an existing Node id.",
    "WF_NO_ENTRY": "Add a Node with no incoming Edge to create a Workflow entry.",
    "WF_ENTRY_HAS_INCOMING_EDGE": (
        "Remove the incoming Edge or do not mark this Node as an explicit entry."
    ),
    "STRING_CONDITION_UNSUPPORTED": (
        "Replace the string condition with a callable condition."
    ),
    "CONDITION_UNSUPPORTED": "Use a callable Edge condition.",
    "MAPPING_UNSUPPORTED": "Use a callable mapping or binding function.",
    "CAPABILITY_NOT_REGISTERED": (
        "Register a provider for the Capability before compiling for execution."
    ),
    "CAPABILITY_HAS_NO_OPERATOR": (
        "Install at least one Operator that implements this Capability."
    ),
    "OPERATOR_NOT_REGISTERED": (
        "Register the referenced Operator or bind a callable directly."
    ),
    "SUBWORKFLOW_RECURSION": "Remove the recursive child Workflow reference.",
}


def _diagnostic_hint(code: str) -> str | None:
    if code in _DIAGNOSTIC_HINTS:
        return _DIAGNOSTIC_HINTS[code]
    if code.startswith("POLICY_"):
        return "Update the referenced Policy field to satisfy the diagnostic."
    if code.startswith("CAPABILITY_"):
        return "Correct the Capability reference or install a matching provider."
    if code.startswith("OPERATOR_"):
        return "Correct the callable or Operator contract used by this Node."
    if code.startswith("SUBWORKFLOW_"):
        return "Update the child Workflow boundary or placeholder configuration."
    if code.startswith("SYSTEM_COMMAND_"):
        return "Remove the unsupported behavior from the SystemCommand Node."
    if code.startswith("LOOP_"):
        return "Restructure the cycle as a natural loop with one entry header."
    return None

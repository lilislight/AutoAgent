from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from re import sub
from typing import TYPE_CHECKING, Literal

from autoagent.operators.operator import Operator
from autoagent.workflow.capability import CapabilityRef, OperatorRef, SystemCommand
from autoagent.workflow.node import Node

if TYPE_CHECKING:
    from autoagent.compiler import Diagnostic, WorkflowCompiler
    from autoagent.workflow.edge import Edge
    from autoagent.workflow.workflow import Workflow


DiagramStatus = Literal["normal", "warning", "error"]


@dataclass(frozen=True)
class DiagramNode:
    """One source Workflow node prepared for static visualization."""

    key: str
    id: str
    label: str
    capability: str
    entry: bool
    exit: bool
    missing: bool = False


@dataclass(frozen=True)
class DiagramEdge:
    """One directed source Edge with Compiler diagnostics attached."""

    key: str
    index: int
    id: str
    source: str
    target: str
    label: str
    status: DiagramStatus
    diagnostics: tuple[Diagnostic, ...] = ()


class WorkflowDiagram:
    """Compiler-assisted static Workflow preview.

    This is an authoring utility. It never executes a Workflow and does not
    replace compilation before invocation. Compiler diagnostics are attached to
    their source edges so Mermaid output uses the same status: errors are red,
    warnings are amber, and valid edges are gray.
    """

    def __init__(
        self,
        *,
        workflow_id: str | None,
        workflow_name: str | None,
        nodes: tuple[DiagramNode, ...],
        edges: tuple[DiagramEdge, ...],
        diagnostics: tuple[Diagnostic, ...],
        compiled: bool,
    ) -> None:
        self.workflow_id = workflow_id
        self.workflow_name = workflow_name
        self.nodes = nodes
        self.edges = edges
        self.diagnostics = diagnostics
        self.compiled = compiled

    @classmethod
    def from_workflow(
        cls,
        workflow: Workflow,
        *,
        compiler: WorkflowCompiler | None = None,
    ) -> WorkflowDiagram:
        """Compile for diagnostics and build a diagram from the source graph."""

        from autoagent.compiler import WorkflowCompiler

        compile_result = (compiler or WorkflowCompiler()).compile(workflow)
        diagnostics = tuple(compile_result.diagnostics)
        source_nodes, object_keys, id_keys = _source_nodes(workflow)
        source_edges, missing_nodes = _source_edges(
            workflow,
            source_nodes=source_nodes,
            object_keys=object_keys,
            id_keys=id_keys,
            diagnostics=diagnostics,
        )
        all_nodes = list(source_nodes) + missing_nodes
        all_nodes = _mark_boundaries(
            all_nodes,
            source_edges,
            compile_result.workflow_ir.entry_node_ids
            if compile_result.workflow_ir is not None
            else (),
            compile_result.workflow_ir.exit_node_ids
            if compile_result.workflow_ir is not None
            else (),
        )
        return cls(
            workflow_id=workflow.id,
            workflow_name=workflow.name,
            nodes=tuple(all_nodes),
            edges=tuple(source_edges),
            diagnostics=diagnostics,
            compiled=compile_result.ok,
        )

    @property
    def error_count(self) -> int:
        return sum(item.severity == "error" for item in self.diagnostics)

    @property
    def warning_count(self) -> int:
        return sum(item.severity == "warning" for item in self.diagnostics)

    def to_mermaid(self) -> str:
        """Return Mermaid flowchart source with Compiler edge status styles."""

        lines = ["flowchart LR"]
        for node in self.nodes:
            label = _mermaid_text(node.label)
            capability = _mermaid_text(node.capability)
            lines.append(f'    {node.key}["{label}<br/><small>{capability}</small>"]')

        for edge in self.edges:
            label = _mermaid_text(edge.label)
            lines.append(f"    {edge.source} -->|{label}| {edge.target}")

        lines.extend(
            [
                "    classDef normal fill:#ffffff,stroke:#334155,color:#111827",
                "    classDef entry fill:#ecfdf5,stroke:#047857,color:#111827,stroke-width:2px",
                "    classDef exit fill:#eff6ff,stroke:#1d4ed8,color:#111827,stroke-width:2px",
                "    classDef missing fill:#fef2f2,stroke:#dc2626,color:#991b1b,stroke-dasharray:5 3",
            ]
        )
        for node in self.nodes:
            node_class = "missing" if node.missing else "entry" if node.entry else "exit" if node.exit else "normal"
            lines.append(f"    class {node.key} {node_class}")
        for index, edge in enumerate(self.edges):
            if edge.status == "error":
                lines.append(
                    f"    linkStyle {index} stroke:#dc2626,stroke-width:3px,color:#991b1b"
                )
            elif edge.status == "warning":
                lines.append(
                    f"    linkStyle {index} stroke:#b45309,stroke-width:3px,color:#92400e"
                )
        return "\n".join(lines)

    def save(self, path: str | Path) -> Path:
        """Write Mermaid flowchart source and return its absolute path."""

        target = Path(path).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(self.to_mermaid(), encoding="utf-8")
        return target.resolve()


def _source_nodes(
    workflow: Workflow,
) -> tuple[list[DiagramNode], dict[int, str], dict[str, str]]:
    nodes: list[DiagramNode] = []
    object_keys: dict[int, str] = {}
    id_keys: dict[str, str] = {}
    for index, node in enumerate(workflow.nodes):
        node_id = node.id
        key = f"n{index}"
        object_keys[id(node)] = key
        id_keys.setdefault(node_id, key)
        nodes.append(
            DiagramNode(
                key=key,
                id=node_id,
                label=node.name or node_id,
                capability=_capability_name(node),
                entry=bool(node.entry),
                exit=False,
            )
        )
    return nodes, object_keys, id_keys


def _source_edges(
    workflow: Workflow,
    *,
    source_nodes: list[DiagramNode],
    object_keys: dict[int, str],
    id_keys: dict[str, str],
    diagnostics: tuple[Diagnostic, ...],
) -> tuple[list[DiagramEdge], list[DiagramNode]]:
    from autoagent.compiler.id_generation import generate_edge_id

    node_ids = {node.key: node.id for node in source_nodes}
    manual_ids = {edge.id for edge in workflow.edges if edge.id is not None}
    used_ids = set(manual_ids)
    missing_nodes: list[DiagramNode] = []
    missing_keys: dict[str, str] = {}
    edges: list[DiagramEdge] = []

    for index, edge in enumerate(workflow.edges):
        source = _resolve_endpoint(
            edge.from_node,
            edge_index=index,
            role="source",
            object_keys=object_keys,
            id_keys=id_keys,
            missing_keys=missing_keys,
            missing_nodes=missing_nodes,
        )
        target = _resolve_endpoint(
            edge.to_node,
            edge_index=index,
            role="target",
            object_keys=object_keys,
            id_keys=id_keys,
            missing_keys=missing_keys,
            missing_nodes=missing_nodes,
        )
        source_id = node_ids.get(source) or next(
            node.id for node in missing_nodes if node.key == source
        )
        target_id = node_ids.get(target) or next(
            node.id for node in missing_nodes if node.key == target
        )
        if edge.id is None:
            edge_id = generate_edge_id(f"edge_{source_id}_{target_id}", used_ids)
        else:
            edge_id = edge.id
        used_ids.add(edge_id)

        edge_diagnostics = tuple(
            item
            for item in diagnostics
            if _diagnostic_matches_edge(item, edge_id=edge_id, source_index=index)
        )
        status = _diagnostic_status(edge_diagnostics)
        label_parts = [edge_id]
        if edge.condition is not None:
            label_parts.append("condition")
        if edge.policy is not None and edge.policy.map is not None:
            label_parts.append("map")
        edges.append(
            DiagramEdge(
                key=f"e{index}",
                index=index,
                id=edge_id,
                source=source,
                target=target,
                label=" | ".join(label_parts),
                status=status,
                diagnostics=edge_diagnostics,
            )
        )
    return edges, missing_nodes


def _resolve_endpoint(
    ref: str | Node,
    *,
    edge_index: int,
    role: str,
    object_keys: dict[int, str],
    id_keys: dict[str, str],
    missing_keys: dict[str, str],
    missing_nodes: list[DiagramNode],
) -> str:
    if isinstance(ref, Node):
        key = object_keys.get(id(ref))
        label = ref.id or f"unregistered_{role}_{edge_index}"
    else:
        key = id_keys.get(ref)
        label = ref
    if key is not None:
        return key

    missing_key = f"{role}:{label}"
    if missing_key not in missing_keys:
        key = f"missing{len(missing_keys)}"
        missing_keys[missing_key] = key
        missing_nodes.append(
            DiagramNode(
                key=key,
                id=label,
                label=f"Missing: {label}",
                capability="Unknown node reference",
                entry=False,
                exit=False,
                missing=True,
            )
        )
    return missing_keys[missing_key]


def _diagnostic_matches_edge(
    diagnostic: Diagnostic,
    *,
    edge_id: str,
    source_index: int,
) -> bool:
    if diagnostic.subject == edge_id:
        return True
    return (
        diagnostic.metadata.get("object_type") == "edge"
        and diagnostic.metadata.get("source_index") == source_index
    )


def _diagnostic_status(diagnostics: tuple[Diagnostic, ...]) -> DiagramStatus:
    if any(item.severity == "error" for item in diagnostics):
        return "error"
    if any(item.severity == "warning" for item in diagnostics):
        return "warning"
    return "normal"


def _mark_boundaries(
    nodes: list[DiagramNode],
    edges: list[DiagramEdge],
    compiled_entries: tuple[str, ...],
    compiled_exits: tuple[str, ...],
) -> list[DiagramNode]:
    incoming = {node.key: 0 for node in nodes}
    outgoing = {node.key: 0 for node in nodes}
    for edge in edges:
        incoming[edge.target] = incoming.get(edge.target, 0) + 1
        outgoing[edge.source] = outgoing.get(edge.source, 0) + 1
    explicit_entry = any(node.entry for node in nodes if not node.missing)
    matched_compiled_entry = any(
        node.id in compiled_entries for node in nodes if not node.missing
    )
    matched_compiled_exit = any(
        node.id in compiled_exits for node in nodes if not node.missing
    )
    return [
        DiagramNode(
            key=node.key,
            id=node.id,
            label=node.label,
            capability=node.capability,
            entry=(
                node.id in compiled_entries
                or node.entry
                or (
                    not explicit_entry
                    and not matched_compiled_entry
                    and incoming.get(node.key, 0) == 0
                    and not node.missing
                )
            ),
            exit=(
                node.id in compiled_exits
                or (
                    not matched_compiled_exit
                    and outgoing.get(node.key, 0) == 0
                    and not node.missing
                )
            ),
            missing=node.missing,
        )
        for node in nodes
    ]


def _capability_name(node: Node) -> str:
    from autoagent.workflow.workflow import Workflow

    capability = node.capability
    if isinstance(capability, Workflow):
        return f"Workflow: {capability.id}"
    if isinstance(capability, Operator):
        return capability.id
    if isinstance(capability, str):
        return capability
    if isinstance(capability, CapabilityRef):
        return f"Capability: {capability.id}"
    if isinstance(capability, OperatorRef):
        return f"Operator: {capability.id}"
    if isinstance(capability, SystemCommand):
        return f"System: {capability.id}"
    return getattr(capability, "__name__", capability.__class__.__name__)


def _mermaid_text(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', "&quot;").replace("|", "&#124;")


def default_preview_path(workflow: Workflow) -> Path:
    """Return a deterministic local filename for Workflow.preview()."""

    base = workflow.id or workflow.name or "workflow"
    safe = sub(r"[^A-Za-z0-9._-]+", "_", base).strip("._") or "workflow"
    return Path(f"{safe}_preview.mmd")

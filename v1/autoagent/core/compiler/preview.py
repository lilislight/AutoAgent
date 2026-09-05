from __future__ import annotations

from collections import defaultdict
import json
from pathlib import Path
from re import sub
from collections.abc import Iterable
from typing import Literal

from autoagent.core.compiler.analysis import (
    WorkflowAnalysisEdge,
    WorkflowAnalysisNode,
)
from autoagent.core.compiler.diagnostic import CompileResult, Diagnostic


WorkflowPreviewFormat = Literal["terminal", "mermaid", "json"]
PreviewStatus = Literal["normal", "warning", "error"]


class WorkflowPreview:
    """Render one Compiler result without deriving execution semantics."""

    def __init__(self, result: CompileResult) -> None:
        self.result = result
        self.analysis = result.analysis
        self.diagnostics = tuple(result.diagnostics)
        self._diagnostics_by_object: dict[
            tuple[str, str],
            list[Diagnostic],
        ] = defaultdict(list)
        self._edge_diagnostics_by_index: dict[int, list[Diagnostic]] = defaultdict(
            list
        )
        for diagnostic in self.diagnostics:
            if diagnostic.object_type is not None and diagnostic.object_id is not None:
                self._diagnostics_by_object[
                    (diagnostic.object_type, diagnostic.object_id)
                ].append(diagnostic)
            if diagnostic.object_type == "edge" and diagnostic.source_index is not None:
                self._edge_diagnostics_by_index[diagnostic.source_index].append(
                    diagnostic
                )

    @property
    def compiled(self) -> bool:
        return self.result.ok

    @property
    def error_count(self) -> int:
        return sum(item.severity == "error" for item in self.diagnostics)

    @property
    def warning_count(self) -> int:
        return sum(item.severity == "warning" for item in self.diagnostics)

    def render(self, format: WorkflowPreviewFormat = "terminal") -> str:
        if format == "terminal":
            return self.to_terminal()
        if format == "mermaid":
            return self.to_mermaid()
        if format == "json":
            return self.to_json()
        raise ValueError(f"Unsupported Workflow preview format: {format}")

    def to_terminal(self) -> str:
        analysis = self.analysis
        lines = [
            f"WORKFLOW {analysis.workflow_id}",
            f"VERSION {analysis.workflow_version}",
            f"STATUS {_result_status(self.result)}",
            (
                f"NODES {len(analysis.nodes)}  EDGES {len(analysis.edges)}  "
                f"LOOPS {_count_or_unknown(analysis.loop_regions)}"
            ),
            f"ENTRIES {_ids_or_unknown(analysis.entry_node_ids)}",
            f"EXITS {_ids_or_unknown(analysis.exit_node_ids)}",
            "",
            "NODES",
        ]
        for node in analysis.nodes:
            markers: list[str] = []
            if node.entry:
                markers.append("ENTRY")
            if node.exit:
                markers.append("EXIT")
            if not node.resolved:
                markers.append("UNRESOLVED")
            if node.map_policy is not None:
                markers.append("MAP")
            status = self.node_status(node)
            if status != "normal":
                markers.append(status.upper())
            marker_text = f" [{', '.join(markers)}]" if markers else ""
            lines.append(
                f"  {node.id}{marker_text} "
                f"({node.binding.kind}:{node.binding.id})"
            )

        lines.extend(["", "FLOW"])
        outgoing: dict[str, list[WorkflowAnalysisEdge]] = defaultdict(list)
        for edge in analysis.edges:
            outgoing[edge.from_node_id].append(edge)
        for node in analysis.nodes:
            node_edges = outgoing.get(node.id, ())
            if not node_edges:
                continue
            lines.extend(self._terminal_edges(node.id, node_edges))
        known_node_ids = {node.id for node in analysis.nodes}
        for source_id, source_edges in outgoing.items():
            if source_id in known_node_ids:
                continue
            lines.extend(self._terminal_edges(source_id, source_edges))

        lines.extend(["", "LOOPS"])
        if analysis.loop_regions is None:
            lines.append("  unknown")
        elif not analysis.loop_regions:
            lines.append("  none")
        else:
            for region in analysis.loop_regions:
                lines.extend(
                    [
                        f"  {region.id}",
                        f"    HEADER {region.header_node_id}",
                        f"    NODES {', '.join(region.node_ids)}",
                        f"    BACK_EDGES {', '.join(region.back_edge_ids)}",
                    ]
                )

        lines.extend(["", "DIAGNOSTICS"])
        if not self.diagnostics:
            lines.append("  none")
        else:
            for diagnostic in self.diagnostics:
                lines.append(f"  {diagnostic.severity.upper()} {diagnostic.code}")
                if diagnostic.object_type and diagnostic.object_id:
                    lines.append(
                        f"    OBJECT {diagnostic.object_type}:{diagnostic.object_id}"
                    )
                if diagnostic.field:
                    lines.append(f"    FIELD {diagnostic.field}")
                lines.append(f"    MESSAGE {diagnostic.message}")
                if diagnostic.hint:
                    lines.append(f"    HINT {diagnostic.hint}")

        lines.extend(["", "RESULT previewed"])
        return "\n".join(lines)

    def to_mermaid(self) -> str:
        analysis = self.analysis
        node_keys = {
            node.id: f"n{index}" for index, node in enumerate(analysis.nodes)
        }
        missing_ids: list[str] = []
        for edge in analysis.edges:
            for node_id, resolved in (
                (edge.from_node_id, edge.source_resolved),
                (edge.to_node_id, edge.target_resolved),
            ):
                if not resolved and node_id not in node_keys:
                    node_keys[node_id] = f"missing{len(missing_ids)}"
                    missing_ids.append(node_id)

        lines = ["flowchart LR"]
        root_nodes = [node for node in analysis.nodes if not node.workflow_path]
        for node in root_nodes:
            lines.append(_mermaid_node(node_keys[node.id], node))
        grouped: dict[tuple[str, ...], list[WorkflowAnalysisNode]] = defaultdict(list)
        for node in analysis.nodes:
            if node.workflow_path:
                grouped[node.workflow_path].append(node)
        for index, (path, nodes) in enumerate(grouped.items()):
            label = _mermaid_text("/".join(path))
            lines.append(f'    subgraph scope{index}["{label}"]')
            for node in nodes:
                lines.append("    " + _mermaid_node(node_keys[node.id], node))
            lines.append("    end")
        for node_id in missing_ids:
            key = node_keys[node_id]
            label = _mermaid_text(f"Missing: {node_id}")
            lines.append(f'    {key}["{label}"]')

        for edge in analysis.edges:
            label = _mermaid_text(_edge_label(edge))
            lines.append(
                f"    {node_keys[edge.from_node_id]} -->|{label}| "
                f"{node_keys[edge.to_node_id]}"
            )

        lines.extend(
            [
                "    classDef normal fill:#ffffff,stroke:#334155,color:#111827",
                "    classDef entry fill:#ecfdf5,stroke:#047857,color:#111827,stroke-width:2px",
                "    classDef exit fill:#eff6ff,stroke:#1d4ed8,color:#111827,stroke-width:2px",
                "    classDef warning fill:#fffbeb,stroke:#b45309,color:#92400e,stroke-width:2px",
                "    classDef error fill:#fef2f2,stroke:#dc2626,color:#991b1b,stroke-width:2px",
                "    classDef missing fill:#fef2f2,stroke:#dc2626,color:#991b1b,stroke-dasharray:5 3",
            ]
        )
        for node in analysis.nodes:
            status = self.node_status(node)
            node_class = (
                status
                if status != "normal"
                else "entry"
                if node.entry
                else "exit"
                if node.exit
                else "normal"
            )
            lines.append(f"    class {node_keys[node.id]} {node_class}")
        for node_id in missing_ids:
            lines.append(f"    class {node_keys[node_id]} missing")
        for index, edge in enumerate(analysis.edges):
            status = self.edge_status(edge)
            if status == "error" or not (
                edge.source_resolved and edge.target_resolved
            ):
                lines.append(
                    f"    linkStyle {index} stroke:#dc2626,stroke-width:3px,color:#991b1b"
                )
            elif status == "warning":
                lines.append(
                    f"    linkStyle {index} stroke:#b45309,stroke-width:3px,color:#92400e"
                )
        for diagnostic in self.diagnostics:
            subject = (
                f" {diagnostic.object_type}:{diagnostic.object_id}"
                if diagnostic.object_type and diagnostic.object_id
                else ""
            )
            lines.append(
                f"    %% {diagnostic.severity.upper()} {diagnostic.code}{subject}"
            )
        return "\n".join(lines)

    def to_json(self) -> str:
        diagnostic_document = self.result.to_diagnostic_document()
        document = {
            "valid": self.result.ok,
            "analysis": self.analysis.model_dump(mode="json"),
            "summary": diagnostic_document["summary"],
            "diagnostics": diagnostic_document["diagnostics"],
        }
        return json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True)

    def save(
        self,
        path: str | Path,
        *,
        format: WorkflowPreviewFormat = "mermaid",
    ) -> Path:
        target = Path(path).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"{self.render(format)}\n", encoding="utf-8")
        return target.resolve()

    def node_status(self, node: WorkflowAnalysisNode) -> PreviewStatus:
        return _diagnostic_status(
            self._diagnostics_by_object.get(("node", node.id), ())
        )

    def edge_status(self, edge: WorkflowAnalysisEdge) -> PreviewStatus:
        diagnostics = list(
            self._diagnostics_by_object.get(("edge", edge.id), ())
        )
        diagnostics.extend(self._edge_diagnostics_by_index.get(edge.source_index, ()))
        return _diagnostic_status(diagnostics)

    def _loop_back_edge(self, edge_id: str) -> str | None:
        if self.analysis.loop_regions is None:
            return None
        for region in self.analysis.loop_regions:
            if edge_id in region.back_edge_ids:
                return region.id
        return None

    def _terminal_edges(
        self,
        source_id: str,
        edges: Iterable[WorkflowAnalysisEdge],
    ) -> list[str]:
        values = tuple(edges)
        source_missing = not all(edge.source_resolved for edge in values)
        source_tag = " [MISSING SOURCE]" if source_missing else ""
        lines = [f"  {source_id}{source_tag}"]
        for index, edge in enumerate(values):
            branch = "└─" if index == len(values) - 1 else "├─"
            tags: list[str] = []
            if edge.conditional:
                tags.append("condition")
            status = self.edge_status(edge)
            if status != "normal":
                tags.append(status)
            if not edge.target_resolved:
                tags.append("missing-target")
            tag_text = f" [{', '.join(tags)}]" if tags else ""
            back_edge = self._loop_back_edge(edge.id)
            arrow = "↩" if back_edge else "→"
            loop_text = f" [LOOP {back_edge}]" if back_edge else ""
            lines.append(
                f"    {branch} {edge.id}{tag_text} {arrow} "
                f"{edge.to_node_id}{loop_text}"
            )
        return lines


def default_preview_path(workflow_id: str) -> Path:
    base = workflow_id or "workflow"
    safe = sub(r"[^A-Za-z0-9._-]+", "_", base).strip("._") or "workflow"
    return Path(f"{safe}_preview.mmd")


def _result_status(result: CompileResult) -> str:
    if any(item.severity == "error" for item in result.diagnostics):
        return "invalid"
    if any(item.severity == "warning" for item in result.diagnostics):
        return "warning"
    return "valid"


def _count_or_unknown(value: tuple[object, ...] | None) -> str:
    return "unknown" if value is None else str(len(value))


def _ids_or_unknown(value: tuple[str, ...] | None) -> str:
    if value is None:
        return "unknown"
    return ", ".join(value) if value else "none"


def _diagnostic_status(diagnostics: Iterable[Diagnostic]) -> PreviewStatus:
    values = tuple(diagnostics)
    if any(item.severity == "error" for item in values):
        return "error"
    if any(item.severity == "warning" for item in values):
        return "warning"
    return "normal"


def _edge_label(edge: WorkflowAnalysisEdge) -> str:
    parts = [edge.id]
    if edge.conditional:
        parts.append("condition")
    return " | ".join(parts)


def _mermaid_node(key: str, node: WorkflowAnalysisNode) -> str:
    label = _mermaid_text(node.name or node.id)
    binding_kind = {
        "capability": "Capability",
        "operator": "Operator",
        "system_command": "System",
    }[node.binding.kind]
    binding = _mermaid_text(f"{binding_kind}: {node.binding.id}")
    policy = (
        "<br/><small>Map</small>"
        if node.map_policy is not None
        else ""
    )
    return f'    {key}["{label}<br/><small>{binding}</small>{policy}"]'


def _mermaid_text(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', "&quot;").replace("|", "&#124;")

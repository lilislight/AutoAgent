import { Activity, FileJson, ListTree, X } from "lucide-react";

import type { TraceSelection, WorkflowGraphView } from "../types";

export type DetailTab = "definition" | "executions" | "data" | "contracts" | "policies" | "events";

interface SelectionMenuProps {
  selection: TraceSelection;
  graph: WorkflowGraphView;
  anchor: { x: number; y: number } | null;
  onOpen: (tab: DetailTab) => void;
  onClose: () => void;
}

export function SelectionMenu({
  selection,
  graph,
  anchor,
  onOpen,
  onClose,
}: SelectionMenuProps) {
  if (!selection || !anchor) return null;
  const title = selectionTitle(selection, graph);
  return (
    <div
      className="selection-menu"
      style={{
        left: Math.min(anchor.x + 12, window.innerWidth - 230),
        top: Math.min(anchor.y + 12, window.innerHeight - 180),
      }}
    >
      <div className="selection-menu-heading">
        <strong>{title}</strong>
        <button type="button" onClick={onClose} title="Close">
          <X size={14} />
        </button>
      </div>
      <button type="button" onClick={() => onOpen("definition")}>
        <FileJson size={14} />
        Definition
      </button>
      <button type="button" onClick={() => onOpen("executions")}>
        <Activity size={14} />
        History
      </button>
      <button type="button" onClick={() => onOpen("events")}>
        <ListTree size={14} />
        Events
      </button>
    </div>
  );
}

function selectionTitle(selection: NonNullable<TraceSelection>, graph: WorkflowGraphView): string {
  if (selection.type === "node") {
    const node = graph.nodes.find((value) => value.id === selection.id);
    return node?.name || selection.id;
  }
  if (selection.type === "edge") return selection.id;
  if (selection.type === "group") {
    return graph.groups.find((value) => value.id === selection.id)?.label || selection.id;
  }
  return selection.id.slice(0, 8);
}

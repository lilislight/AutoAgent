---
modules:
  - workflow-authoring
  - workflow-compilation
  - runtime-execution
tags:
  - workflow-authoring
  - workflow-compilation
  - execution-policy
---

# Move Map Execution Policy to Node

## Intent

Make Map describe how one Node executes instead of attaching parallel execution behavior to one selected Edge.

## Outcome

MapPolicy is owned by NodePolicy alongside ReplicationPolicy. Custom selectors can consume complete fan-in and Loop-header activations; selector-less Map keeps the unambiguous single-incoming default. Edges contain routing conditions only, while child Workflow Map remains unsupported until child execution has a real Runtime boundary.

## Reason

Attaching execution behavior to one incoming Edge makes multiple inputs and Loop headers ambiguous even though Map produces one logical NodeExecution. Node ownership keeps graph routing on Edges and execution strategy on Nodes.

## Impact

Workflow IR and Compiler versions are 0.5. Compiler diagnostics and Preview mark Map Nodes, and WorkflowAnalysis exposes their map_policy summary directly.

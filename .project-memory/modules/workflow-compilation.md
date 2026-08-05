---
code_paths:
  - autoagent/core/compiler/
  - autoagent/project/compiler.py
tags:
  - workflow-compilation
  - workflow-ir
  - diagnostics
  - workflow-analysis
---

# Workflow Compilation

## Responsibility

Convert Workflow source into validated, runtime-ready Workflow IR and a stable revision snapshot while producing deterministic diagnostics and static analysis for authoring tools.

## Current Design

WorkflowCompiler recursively expands child Workflows, resolves Node and Edge identities, compiles contracts and Node-owned Map/Replication policies, builds graph indexes, derives natural-loop regions, and infers entry and exit Nodes. Successful compilation yields WorkflowIR and a content-derived WorkflowVersionSnapshot. CompileResult also carries WorkflowAnalysis: an immutable, display-safe view that remains useful for incomplete graphs; mapped Nodes expose a direct map_policy summary rather than a partial generic NodePolicy model. WorkflowPreview renders the shared analysis and diagnostics as terminal text, Mermaid, or JSON.

## Boundaries and Rules

- Scheduler and Executors consume only successful Workflow IR, never WorkflowAnalysis.
- Compilation reports independent errors where possible instead of stopping at the first invalid object.
- Child Workflow expansion preserves local ids and Workflow paths for hook contexts and trace grouping.
- Map diagnostics and Preview markers belong to the mapped Node. The Compiler requires a selector when graph structure cannot supply one unambiguous default Map input.
- Revision identity is derived from semantic compiled definition; display metadata and import location do not redefine execution semantics.
- Compiler diagnostics are shared by project check and Workflow preview.

## Relationships

Workflow Authoring supplies source objects and Operator System supplies contract visibility. AutoAgentApp stores compiled entries and snapshots. Runtime Execution uses Workflow IR; Runtime Persistence stores revision snapshots; Tracing Server and UI use analysis-safe graph representations.

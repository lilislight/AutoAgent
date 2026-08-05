---
code_paths:
  - autoagent/__init__.py
  - autoagent/core/workflow/
tags:
  - workflow-authoring
  - graph-model
  - execution-policy
---

# Workflow Authoring

## Responsibility

Define the static Python source model that Coding Agents and developers use to express Workflow graphs, execution policies, data hooks, Wait commands, child Workflows, and User Event mappings.

## Current Design

Workflow owns ordered Node and Edge definitions plus Workflow-wide policy and non-semantic metadata. Nodes bind direct callables, Operators, Capability references, SystemCommands, or child Workflows; NodePolicy owns Map and Replication execution behavior. Edges express dependencies and optional conditions. Input Mapping, Conditions, selectors, and aggregators receive restricted read views; Output Binding receives a transactional writable context whose changes are committed only after success.

## Boundaries and Rules

- Workflow definitions are static source, not live Runtime state.
- Node ids are required and stable within a Workflow; omitted Edge ids are assigned by the Compiler.
- Child Workflows are source composition and are expanded before execution.
- A custom Map selector may consume complete fan-in or the current external/back activation of a Loop header. Selector-less Map requires exactly one incoming Edge, and Map remains unsupported on child Workflow placeholders.
- Workflow semantics belong in definitions and policies, not deployment environment settings.
- The root package exports only the authoring names listed in `autoagent.__all__` as the stable authoring contract.

## Relationships

Workflow Compilation turns this model into Workflow IR. Operator System supplies executable bindings and contracts. Runtime Execution invokes compiled hooks and policies without mutating the source graph.

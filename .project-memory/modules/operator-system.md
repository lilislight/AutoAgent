---
code_paths:
  - autoagent/core/operators/
tags:
  - operators
  - capabilities
  - typed-contracts
---

# Operator System

## Responsibility

Represent executable Python callables, infer their named-argument contracts, register application-local Capabilities and Operators, and resolve the concrete Operator used by a compiled Node.

## Current Design

Operator wraps a live callable and inferred contract plus stable identity, version, priority, availability, and optional Capability relationship. CapabilityRegistry and OperatorRegistry belong to one AutoAgentApp, reject duplicate identities, and validate structural contract compatibility. OperatorResolver applies compiled direct bindings or Capability selection rules at execution time. StreamingResult exposes an explicit stream plus reducer and typed final output contract.

## Boundaries and Rules

- Registries are application-local and thread-safe; no global public registration is required for Workflow execution.
- Capability contracts are abstract and stable after binding; concrete Operator availability may change for future calls.
- Direct callables compile to direct Operators and do not require registry registration.
- Operator input is always a mapping whose keys bind to handler parameters.
- Selection and retry/fallback behavior are separate: resolution chooses an Operator, while Node execution policy controls attempts.

## Relationships

Workflow Authoring references direct Operators or Capabilities. Workflow Compilation validates their contracts with registry visibility. Runtime Execution resolves and invokes them. AI Building Blocks installs `llm_call` as an ordinary Capability and Operator.

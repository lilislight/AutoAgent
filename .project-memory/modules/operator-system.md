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

Operator wraps a live callable and inferred executable contract plus stable identity, version, priority, availability, and optional Capability relationship. Every parameter and return annotation must describe a serializable Workflow value. Typed Pydantic contracts retain restoration adapters; explicit Any or dynamic containers define normalized JSON contracts. CapabilityRegistry and OperatorRegistry belong to one AutoAgentApp, reject duplicate identities, and validate structural contract compatibility. OperatorResolver applies compiled direct bindings or Capability selection rules at execution time. StreamingResult exposes an explicit stream plus reducer and typed final output contract.

## Boundaries and Rules

- Registries are application-local and thread-safe; no global public registration is required for Workflow execution.
- Capability contracts are abstract and stable after binding; concrete Operator availability may change for future calls.
- Direct callables compile to direct Operators and do not require registry registration.
- Operator input is always a mapping whose keys bind to handler parameters.
- Workflow contracts permit serializable scalar/container values, Pydantic models, and ArtifactRef; process-local resources and raw streams cannot cross an Operator boundary.
- Missing annotations are errors. Explicit Any is allowed but means dynamic JSON rather than arbitrary Python objects.
- Selection and retry/fallback behavior are separate: resolution chooses an Operator, while Node execution policy controls attempts.

## Relationships

Workflow Authoring references direct Operators or Capabilities. Workflow Compilation validates their contracts with registry visibility. Runtime Execution resolves and invokes them. AI Building Blocks installs `llm_call` as an ordinary Capability and Operator.

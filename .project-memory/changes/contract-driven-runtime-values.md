---
modules:
  - operator-system
  - workflow-compilation
  - runtime-execution
  - runtime-persistence
tags:
  - typed-contracts
  - serialization
  - recovery
supersedes:
  - remove-unreleased-compatibility-layers
---

# Make Workflow Contracts Own Runtime Types

## Intent

Prevent process-local Python objects and model import identities from becoming durable Workflow state while preserving typed live execution and recovery.

## Outcome

Workflow callable boundaries require serializable annotations. Dynamic contracts normalize to JSON, Pydantic values persist as type-neutral JSON, and executable Recovery or Resume restores typed Node outputs through the exact registered Workflow revision.

## Reason

Persistence must remain readable without importing historical project code, while continuing execution still needs the authoritative contracts of the selected Workflow revision.

## Impact

Connections, clients, locks, classes, generators, and other process-local resources stay inside Operator implementations. Context is JSON, historical observation is JSON, and revision contracts are the only user-type restoration authority.

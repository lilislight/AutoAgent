---
modules:
  - runtime-execution
  - runtime-persistence
  - tracing-server-and-ui
tags:
  - runtime-events
  - serialization
  - ui-contract
---

# Remove Unreleased Compatibility Layers

## Intent

Keep one V1 Runtime, persistence, and UI contract before release instead of carrying aliases for superseded internal shapes.

## Outcome

Runtime Events flow from Server to UI with their canonical names and subjects, Pydantic Runtime models use one stable type id, timestamps restore only from integer milliseconds, and Scheduler requests expose incoming activations directly.

## Reason

Aliases and permissive decoders make the active contract ambiguous and preserve states that no released version needs to read.

## Impact

Current code and newly persisted data must use the single V1 shapes; no migration or fallback path exists for the removed internal forms.

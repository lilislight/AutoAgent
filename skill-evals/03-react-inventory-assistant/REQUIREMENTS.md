# Inventory Replenishment Assistant

Build an AutoAgent project containing a ReActWorkflow that recommends an
inventory replenishment plan using deterministic Tools.

## Request and Result

The request contains a natural-language question plus a `warehouse_id`.
The structured result contains:

- `warehouse_id`
- `sku`
- `recommended_quantity`
- `supplier`
- `reasoning_summary`
- `tool_calls_used`

## Tools

Provide typed local Tools for:

- reading current inventory and recent demand for a SKU
- listing supplier lead times and minimum order quantities
- simulating whether a proposed order covers forecast demand

Tool behavior must be deterministic and must not call external services.
Descriptions and schemas should give the model enough information to select
and call them correctly.

## Behavior

- Use an LLM Call through AutoAgent's OpenAI-compatible capability and a
  ReActWorkflow.
- Give the model explicit instructions to inspect inventory and suppliers
  before making a recommendation and to return the requested structured result.
- Support multiple Tool calls in one model response.
- A nonexistent Tool name, invalid Tool arguments, Tool execution error, or
  structured-output validation error must be returned to the model so it can
  correct the request within bounded retries.
- The Workflow must terminate with either a valid structured result or a clear
  bounded failure; it must never retry forever.

## Project Contract

- Create one `auto-agent.toml` at the project root.
- Expose exactly one top-level Workflow from the manifest.
- Use public AutoAgent authoring APIs only.
- Include a deterministic fake OpenAI-compatible provider or mocked LLM
  Operator for automated tests. Tests must not require paid services or
  credentials.
- Include JSON fixtures and expected results for a normal recommendation,
  parallel Tool calls, invalid Tool arguments followed by repair, Tool failure
  followed by repair, and malformed structured output followed by repair.
- Include automated tests for Tool schemas, retry limits, structured output,
  compiler validation, and top-level Workflow output.
- Document optional environment variables for running manually against a real
  OpenAI-compatible provider, but keep the default test path local.
- Document the exact install, check, run, and test commands without assuming a
  particular package manager.

## Acceptance

The project passes `autoagent project check`, all local tests pass without
network access, repair cases visibly exercise the model feedback loop, and the
final output always matches the declared structured result type.

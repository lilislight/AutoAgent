# Stable Authoring API

This reference owns public imports and construction surfaces. It describes what
Workflow authors may use, not how to design a graph or select a policy.

## Contents

- Contract rule
- `autoagent` exports
- `autoagent.ai` exports
- Direct callable contract
- Durable values
- Versioned hooks

## Contract rule

Only names in `autoagent.__all__` and `autoagent.ai.__all__` are stable
authoring contracts. A root attribute that exists but is absent from `__all__`
is not public merely because Python can import it.

Never import `autoagent.core.*` in generated Workflow code.

## `autoagent` exports

### Workflow construction

- `Workflow`
- `CapabilityRef`
- `SystemCommand`
- `ArtifactRef`

The standard form is:

```python
from autoagent import Workflow

workflow = Workflow(id="order_review", version=1)
validate = workflow.add_node(validate_order, node_id="validate_order")
workflow.add_edge(validate, "finalize", edge_id="validated")
```

`Workflow.add_node(...)` accepts a typed callable, an abstract capability
reference, a SystemCommand, or a child Workflow through the recommended public
authoring surface. Prefer the typed callable form for project-owned business
logic.

`Workflow.add_edge(...)` accepts Node objects or Node ID strings. Always provide
an explicit `edge_id` for stable traces and diagnostics.

### Hook context

- `InputMappingContext`
- `ConditionContext`
- `OutputBindingContext`
- `MapItemSelectionContext`
- `MapAggregationContext`
- `ReplicationAggregationContext`
- `InputMapping`
- `OutputBinding`
- `workflow_hook`

Read [hook-contracts.md](hook-contracts.md) before implementing these functions.

### Policy

- `WorkflowPolicy`
- `FailurePolicy`
- `NodePolicy`
- `EdgePolicy`
- `CapabilitySelectionPolicy`
- `RetryPolicy`
- `BackoffPolicy`
- `TimeoutPolicy`
- `RecoveryPolicy`
- `ResourcePolicy`
- `MapPolicy`
- `ReplicationPolicy`

Read [policies.md](policies.md) for semantics and valid combinations.

## `autoagent.ai` exports

### LLM protocol

- `LLM_CALL_CAPABILITY`
- `LLM_CALL_CAPABILITY_ID`
- `LLM_CALL_CONTRACT`
- `LLMMessage`
- `LLMRequest`
- `LLMResponse`
- `LLMResponseFormat`
- `LLMToolCall`
- `LLMToolChoice`
- `LLMNamedToolChoice`
- `LLMToolDefinition`
- `LLMUsage`
- `response_format_from_type`

### Tools and ReAct

- `tool`
- `ToolDefinition`
- `get_tool_definition`
- `react_workflow`
- `ToolArgumentsRepairExhausted`
- `StructuredOutputRepairExhausted`

### Chat Completions Provider

- `ChatCompletionsConfig`
- `ChatCompletionsProvider`
- `LLMProvider`
- `LLMStreamChunk`
- `LLMProviderError`
- `StructuredOutputMode`
- `create_llm_call_operator`
- `register_llm_call_operator`

Workflow modules normally use protocol types, `tool`, and `react_workflow`.
CLI/host infrastructure configures the Provider. Do not create an App or
register the Provider inside a Workflow definition.

Read [ai-workflows.md](ai-workflows.md) for AI-specific authoring.

## Direct callable contract

Use concrete parameter and return annotations:

```python
def score_order(order: Order) -> RiskScore:
    ...
```

AutoAgent derives an Operator contract from the signature. Pydantic models are
recommended for structured boundaries. Dataclasses, typed containers, and
other types supported by the framework's Pydantic contract layer may also
work, but verify them with `workflow check`.

Avoid:

- untyped parameters or returns;
- positional meaning that is not visible in names;
- `Any` at durable or external boundaries without a concrete need;
- capturing live clients, locks, generators, or other non-serializable objects
  in Runtime values.

## Durable values

Values crossing Node, Context, Wait, or persistence boundaries must be
serializable by the installed Runtime contract. Prefer typed models, primitive
containers, UUIDs, timestamps, and other explicitly supported values.

Do not place large documents, media, model blobs, open streams, or live client
objects directly in Context or Node outputs. Preserve an `ArtifactRef` supplied
by the host and pass the reference through the Workflow instead of copying the
payload. Workflow code must not invent an ArtifactRef for data that was never
stored by an artifact service.

## Versioned hooks

Apply `@workflow_hook(version=...)` to a Condition, mapping, binding, selector,
or aggregator when a semantic code change must alter the Workflow definition
hash even though the graph structure and callable identity stay the same:

```python
from autoagent import workflow_hook

@workflow_hook(version=2)
def select_path(ctx: ConditionContext) -> bool:
    ...
```

The decorator preserves the original callable and async behavior.

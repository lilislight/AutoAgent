# Stable Authoring API

This reference owns public imports and construction surfaces. It describes what
Workflow authors may use, not how to design a graph or select a policy.

## Contents

- Contract rule
- `autoagent` exports
- `autoagent.ai` exports
- Direct callable contract
- Streaming callable contract
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

### Streaming Operator results

- `StreamingResult`
- `StreamReducer`
- `streaming_result`

These names are required only when a project-owned Operator returns a live
sync or async stream. Ordinary Operators continue to return their normal typed
business value.

### User-facing events

- `UserEventMapping`

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

- `llm_call_node`
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

Workflow modules normally use protocol types, `llm_call_node`, `tool`, and
`react_workflow`.
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

## Streaming callable contract

A raw Generator, AsyncGenerator, Iterator, or AsyncIterator is not a valid
Operator output because it does not define the final value passed to Output
Binding and downstream Nodes. Wrap an intentional stream explicitly:

```python
from autoagent import StreamReducer, StreamingResult, streaming_result

class TextReducer:
    def __init__(self) -> None:
        self.parts: list[str] = []

    def add(self, chunk: str) -> None:
        self.parts.append(chunk)

    def finish(self) -> str:
        return "".join(self.parts)

def stream_text() -> StreamingResult[str, str]:
    return streaming_result(generate_text(), reducer=TextReducer())
```

NodeExecutor consumes the source under normal Timeout, Retry, Fallback,
cancellation, Map, and Replication policies. Only `finish()` becomes the
validated and durable Operator output; transient chunks are not Runtime values.
Both reducer methods are synchronous and should remain small. A blocking sync
source is consumed in the shared thread pool.

## UserEvent mappings

Use `UserEventMapping` when a Workflow must expose application-facing data
independently from Runtime tracing:

```python
from autoagent import UserEventMapping

workflow.add_node(
    create_answer,
    node_id="create_answer",
    user_event_mapping=UserEventMapping(
        type="answer_completed",
        transform=lambda output: {"answer": output},
    ),
)
```

`type` must use lowercase `snake_case`. `transform` receives only the completed
Node output and returns a serializable payload or `None`. For an explicit
`StreamingResult`, `stream_user_event_mapping` receives each chunk. The
framework owns stage selection, Event identity, sequence, occurrence time, and
execution correlation; do not create these values in Workflow code. Mapping
failure never changes an otherwise successful Node result.

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

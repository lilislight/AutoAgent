# AI Workflows

This reference owns LLM, Tool, structured output, ReActWorkflow, and the
Provider environment expected by those Workflows. It does not define general
graph or Runtime policy.

## Contents

- One model call
- Structured output
- Tool definition
- ReActWorkflow
- Provider environment
- Evaluate without a paid service

## One model call

AutoAgent defines the abstract Capability ID `llm_call`.

Use provider-neutral values:

```python
from autoagent.ai import LLMMessage, LLMRequest, LLMResponse

def prepare_request(question: str) -> LLMRequest:
    return LLMRequest(
        messages=(LLMMessage(role="user", content=question),),
        temperature=0,
    )
```

An LLM Node should consume `LLMRequest` and produce `LLMResponse`. Build it
with `llm_call_node(...)`, then add the returned Node to the Workflow. This
installs AutoAgent's stable message, reasoning, and Tool-call UserEvent
mappings. Do not use a raw `CapabilityRef("llm_call")` when the call should be
visible in Agent Activity.

The runtime host registers an Operator implementing `llm_call`; Workflow source
does not construct the App or register Provider credentials.

`LLMRequest` supports:

- `messages`;
- optional `model`;
- `tools` and `tool_choice`;
- `response_format`;
- `temperature`;
- `max_output_tokens`;
- provider-specific `provider_options`.

Do not put normalized reserved request fields inside `provider_options`.

The `llm_call` Capability accepts an execution `mode` separately from the
model request. An Input Mapping may select `invoke` or `stream`:

```python
def map_llm_call(ctx):
    return {
        "request": LLMRequest(
            messages=(LLMMessage(role="user", content=ctx.input),),
            provider_options={"provider_extension": "value"},
        ),
        "mode": "invoke",
    }
```

Provider-specific model parameters belong only in
`LLMRequest.provider_options`. `mode` controls which Provider method the
Operator calls and is not sent to the model API. Both modes produce one final
`LLMResponse`. In stream mode, NodeExecutor consumes the Provider stream as a
framework `StreamingResult`; chunks remain transient and are not copied into
Runtime state or persistence.

Both `llm_call_node(...)` and ReActWorkflow emit mode-independent UserEvents
for Agent UIs. Standard types use lowercase `snake_case`:

- `message_delta`, `reasoning_delta`, `message_completed`, and
  `message_aborted`;
- `tool_call_delta` and `tool_call_requested`;
- `tool_result`, `agent_output`, and `agent_failed`.

The standard LLM Node installs message, reasoning, and Tool-call mappings.
ReActWorkflow reuses those mappings and additionally emits `tool_result`,
`agent_output`, and `agent_failed`. Workflow authors do not inspect execution
stages or manually translate LLM chunks. Tool-call parse or schema validation
failures and structured-output validation failures are internal repair steps
and do not emit failure-specific UserEvents. The raw model request is still
visible as `tool_call_requested`; no `tool_result` exists unless a Tool Node
actually runs.

## Structured output

`response_format` accepts a Pydantic model, dataclass, TypedDict, or another
type supported by Pydantic `TypeAdapter`.

```python
class Answer(BaseModel):
    summary: str
    confidence: float
```

AutoAgent serializes the type to `LLMResponseFormat`; the Python class object
does not cross the persistence boundary.

Always validate the final model content. Do not parse prose by stripping code
fences or guessing JSON boundaries when structured output is available.

## Tool definition

Decorate an ordinary typed function:

```python
from autoagent.ai import tool

@tool(
    id="weather.current",
    name="get_current_weather",
    description="Return current weather for one city.",
)
def get_current_weather(city: str) -> WeatherReading:
    ...
```

Rules:

- all parameters and the return value require concrete annotations;
- Tool names start with a letter or underscore and contain only letters,
  digits, underscores, or hyphens, up to 64 characters;
- explicit stable IDs are recommended;
- name defaults to the function name;
- description defaults to the first docstring paragraph;
- Tool arguments are untrusted until schema validation succeeds.

Keep Tool outputs serializable. Return structured error information through the
ReAct loop rather than mutating Context from the Tool.

## ReActWorkflow

Use `react_workflow(...)` for a bounded model/Tool loop:

```python
workflow = react_workflow(
    id="weather_assistant",
    instructions="Use the provided tools and return verified weather data.",
    tools=(get_current_weather,),
    response_format=WeatherAnswer,
    max_steps=8,
    max_tool_parse_retries=1,
    max_output_parse_retries=1,
)
```

The returned value is a normal single-entry/single-exit Workflow and may be
embedded as a child Workflow.

Its first Node accepts an Invocation Mapping with `input` or `messages`.
`provider_options` and `mode` are optional:

```python
{
    "input": "What is the weather in Tokyo?",
    "provider_options": {"provider_extension": "value"},
    "mode": "stream",
}
```

These settings are preserved across Tool and output-repair iterations. A
downstream LLM Node does not inherit them; its own Input Mapping constructs its
own `LLMRequest` and selects its own mode.

Parameters:

- `id` and non-empty `instructions`;
- typed Tool sequence;
- optional structured `response_format`;
- optional model;
- `max_steps` execution bound;
- invalid Tool-call repair count;
- invalid structured-output repair count;
- display name and description.

Repair counts are additional LLM repair turns, not transport Retry attempts.
Keep these three failure paths distinct:

- `max_tool_parse_retries` bounds repair after an unknown Tool, empty Tool
  identity, malformed JSON arguments, or arguments rejected by the Tool input
  schema;
- `max_output_parse_retries` bounds repair after invalid final structured
  output;
- a Tool handler exception becomes a `tool_execution_error` result returned to
  the model, but it has no separate repair counter in V1 and remains bounded
  only by `max_steps`.

Do not describe `max_tool_parse_retries` as a limit on Tool handler failures.
Operator transport Retry is also separate from all three paths.

Never leave `max_steps` unbounded.

## Provider environment

The built-in Chat Completions Provider uses:

```text
AUTOAGENT_LLM_PROVIDER
AUTOAGENT_LLM_MODEL
AUTOAGENT_LLM_TIMEOUT_MS
AUTOAGENT_LLM_STRUCTURED_OUTPUT_MODE
AUTOAGENT_LLM_BASE_URL
AUTOAGENT_LLM_API_KEY
```

Structured output modes are `auto`, `json_schema`, `json_object`, and `prompt`.
Use `AUTOAGENT_LLM_PROVIDER=chat_completions`. The Base URL may point to OpenAI
or another service implementing the OpenAI Chat Completions protocol. In V1,
`auto` resolves to `json_object` for broad endpoint compatibility. When the
same request may call Tools, AutoAgent adds the JSON Schema instruction to the
conversation but omits `response_format={"type":"json_object"}` so Tool calls
remain valid. Choose `json_schema` explicitly only for an endpoint known to
support that constraint.

Put placeholders and explanations in `.env.example`; never commit a real API
key. Compilation and `workflow check` do not require Provider secrets.

## Evaluate without a paid service

Use a local Chat Completions-compatible HTTP service when model quality itself
is not the business assertion. Point the CLI host at it
through environment variables so the Workflow continues to reference the
abstract `llm_call` Capability exactly as production does.

Write Eval Cases against the public answer and promised business failure
behavior, not the generated internal ReAct graph. Read
[testing.md](testing.md) for dependency realism and the boundary between Eval
and focused unit tests.

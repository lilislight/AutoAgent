# AI Workflows

This reference owns LLM, Tool, structured output, ReActWorkflow, and
OpenAI-compatible Provider authoring. It does not define general graph or
Runtime policy.

## Contents

- One model call
- Structured output
- Tool definition
- ReActWorkflow
- Provider environment
- Test without a paid service

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

An LLM Node should consume `LLMRequest` and produce `LLMResponse`. The runtime
host registers an Operator implementing `llm_call`; Workflow source does not
construct the App or register Provider credentials.

`LLMRequest` supports:

- `messages`;
- optional `model`;
- `tools` and `tool_choice`;
- `response_format`;
- `temperature`;
- `max_output_tokens`;
- provider-specific `provider_options`.

Do not put normalized reserved request fields inside `provider_options`.

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

Unknown Tool names and invalid Tool arguments are returned to the model for
repair until the configured limit is exhausted. Tool execution exceptions are
represented as Tool-result errors and returned to the model, allowing
self-correction. Invalid final structured output is likewise returned for a
bounded repair turn.

Never leave `max_steps` unbounded.

## Provider environment

The built-in OpenAI-compatible Operator uses:

```text
AUTOAGENT_OPENAI_BASE_URL
AUTOAGENT_OPENAI_API_KEY
AUTOAGENT_OPENAI_MODEL
AUTOAGENT_OPENAI_TIMEOUT_MS
AUTOAGENT_OPENAI_STRUCTURED_OUTPUT_MODE
```

Structured output modes are `auto`, `json_schema`, `json_object`, and `prompt`.
`auto` selects a compatible strategy for known providers.

Put placeholders and explanations in `.env.example`; never commit a real API
key. Compilation and `workflow check` do not require Provider secrets.

## Test without a paid service

Prefer a local OpenAI-compatible mock HTTP service. Point the CLI host at it
through environment variables so the Workflow continues to reference the
abstract `llm_call` Capability exactly as production does.

A fake `llm_call` Operator is acceptable only in test-only host code that
already owns App construction and Operator registration. Never register the
fake Operator or create an App inside the Workflow module. When no stable
test-host surface is available, use the mock HTTP Provider.

Test at least:

- valid Tool calls;
- unknown Tool;
- invalid argument schema and repair;
- Tool execution exception and model recovery;
- valid structured output;
- invalid structured output and repair exhaustion;
- `max_steps` termination.

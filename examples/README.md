# Examples

Install AutoAgent and run examples from the repository root.

## ReAct weather agent

`react_weather_agent.py` starts the embedded tracing server with a parent
Workflow containing a ReAct child Workflow. The child calls an OpenAI-compatible
Chat Completions endpoint, uses deterministic mocked city and weather Tools, and
validates a structured response. A final `llm_call` Node translates that answer
into Simplified Chinese.

Copy `.env.example` to `.env`, then fill at least:

```text
AUTOAGENT_OPENAI_API_KEY=
AUTOAGENT_OPENAI_MODEL=
```

For a different compatible provider, also change
`AUTOAGENT_OPENAI_BASE_URL`. Start the server:

```bash
python -m examples.react_weather_agent
```

Open `http://127.0.0.1:8765`, select `mock_weather_chinese`, and inspect its
graph. The `weather_agent` group is the expanded ReAct child Workflow. Click its
`start` entry Node, choose Invoke, and submit:

```json
{
  "input": "What is the weather in Tokyo, and what should I wear?"
}
```

The execution runs in `full` mode when selected in the Invoke form, so the
Timeline and Inspector can show the ReAct phases, model calls, and mocked Tool
execution.

## Incident response tracing server

`incident_response_tracing_server.py` defines a runnable incident-response
Workflow, persists traces, and serves the embedded tracing UI:

```bash
python examples/incident_response_tracing_server.py
```

## Workflow validation preview

`workflow_validation_preview.py` intentionally builds an invalid Workflow to
demonstrate compiler diagnostics and Mermaid preview generation:

```bash
python examples/workflow_validation_preview.py
```

The generated diagram is written to `workflow_validation_preview.mmd` beside
the example.

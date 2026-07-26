# Examples

Run examples from the repository root through the uv environment.

## Incident response tracing server

`incident_response_tracing_server.py` defines a runnable incident-response
Workflow, persists traces, and serves the embedded tracing UI:

```bash
UV_CACHE_DIR=/tmp/autoagent-uv-cache uv run python \
  examples/incident_response_tracing_server.py
```

## Workflow validation preview

`workflow_validation_preview.py` intentionally builds an invalid Workflow to
demonstrate compiler diagnostics and Mermaid preview generation:

```bash
UV_CACHE_DIR=/tmp/autoagent-uv-cache uv run python \
  examples/workflow_validation_preview.py
```

The generated diagram is written to `workflow_validation_preview.mmd` beside
the example.

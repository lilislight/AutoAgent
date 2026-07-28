# Normative Sample Index

This reference only routes an author to existing complete examples. It does
not duplicate their source or define general API rules.

All samples belong to one project rooted at:

```text
examples/authoring/
```

Start with `examples/authoring/README.md`, then read only the closest Workflow
and its fixtures.

## Conditional orchestration

Use when the requirement includes Condition, multiple branches, parallel work,
fan-in, or Loop.

Files:

```text
examples/authoring/workflows/orchestration.py
examples/authoring/inputs/orchestration.json
examples/authoring/expected/orchestration.json
tests/test_authoring_examples.py
```

Demonstrates:

- Pydantic input, intermediate, and output models;
- direct typed callables;
- explicit stable Node and Edge IDs;
- Loop-aware Input Mapping using `incoming.edge_id`;
- conditional routing;
- parallel specialist Nodes;
- ordinary fan-in;
- bounded natural Loop.

Do not copy its business-specific models into unrelated projects.

## Durable Wait and Resume

Use when the requirement pauses for human approval, webhook data, or another
external response.

Files:

```text
examples/authoring/workflows/wait_resume.py
examples/authoring/inputs/wait-request.json
examples/authoring/inputs/wait-response.json
examples/authoring/expected/wait-resume.json
tests/test_authoring_examples.py
```

Demonstrates:

- `SystemCommand(id="wait")`;
- typed Resume data;
- mapping Wait output to the next Node;
- database-backed Resume from a new host.

## LLM, Tools, and ReActWorkflow

Use when the requirement includes an LLM call, typed Tools, model repair, or
structured final output.

Files:

```text
examples/authoring/workflows/react_assistant.py
examples/authoring/mock_openai_provider.py
examples/authoring/inputs/react-weather.json
examples/authoring/expected/react-weather.json
tests/test_authoring_examples.py
```

Demonstrates:

- typed `@tool` functions;
- one `react_workflow(...)` definition;
- bounded Tool and output repair;
- structured Pydantic output;
- fake Operator and local OpenAI-compatible mock testing.

## Selection rule

- Read one sample for the dominant pattern.
- Read a second only when the requirement genuinely combines both patterns.
- Reuse structure and public API style, not sample business logic.
- Validate the resulting project independently; a copied pattern is not proof
  that a new Workflow compiles.

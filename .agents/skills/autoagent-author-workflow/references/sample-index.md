# Normative Sample Index

This reference only routes an author to existing complete examples. It does
not duplicate their source or define general API rules.

All normative samples belong to the selected installed `autoagent` package.
Locate their root with Python's package-resource API:

```bash
python -c "from importlib.resources import files; root = files('autoagent').joinpath('examples', 'authoring'); assert root.joinpath('auto-agent.toml').is_file(), 'installed AutoAgent package has no authoring examples'; print(root)"
```

The printed resource is the sample root used by every relative path below.
Start with `README.md`, then read only the closest Workflow and its fixtures.

If the installed package has no authoring examples, treat the package check as
failed and repeat the package availability procedure.

## Conditional orchestration

Use when the requirement includes Condition, multiple branches, parallel work,
fan-in, or Loop.

Files:

```text
workflows/orchestration.py
inputs/orchestration.json
expected/orchestration.json
tests/test_examples.py
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
workflows/wait_resume.py
inputs/wait-request.json
inputs/wait-response.json
expected/wait-resume.json
tests/test_examples.py
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
workflows/react_assistant.py
mock_openai_provider.py
inputs/react-weather.json
expected/react-weather.json
tests/test_examples.py
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

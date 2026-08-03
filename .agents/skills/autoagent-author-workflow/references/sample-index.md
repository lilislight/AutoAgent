# Authoring Example Index

This reference routes an author to complete installed examples. Examples are
behavior references, not project templates. Never reproduce an example's graph,
models, policies, or directory layout unless the requested business behavior
needs them.

Locate the examples in the selected installed `autoagent` package:

```bash
python -c "from importlib.resources import files; root = files('autoagent').joinpath('examples', 'authoring'); assert root.joinpath('auto-agent.toml').is_file(), 'installed AutoAgent package has no authoring examples'; print(root)"
```

The printed resource is the root for every path below. Read its `README.md`,
then read one Workflow and its matching Evaluation. If the package has no
authoring examples, repeat the package availability procedure in
[project-contract.md](project-contract.md).

## Conditional orchestration

Use when the requirement genuinely needs conditional routing, independent
parallel work, fan-in, or a bounded Loop.

```text
workflows/orchestration.py
evals/release_review.py
```

The Workflow demonstrates typed callables, stable IDs, named Hooks, conditional
Edges, parallel specialist Nodes, fan-in, and a natural Loop. Its Evaluation
protects both the specialist and automatic business outcomes without asserting
internal execution IDs.

## Wait and Resume

Use when a later person or external system must continue a waiting execution.

```text
workflows/wait_resume.py
evals/human_approval.py
```

The Workflow demonstrates `SystemCommand(id="wait")`, typed Resume data, and a
small finalization step. Its Evaluation uses Invoke and Resume as two Steps in
one Case Session. Database-backed cross-process Resume is a runtime deployment
concern, not Workflow or Eval source.

## LLM, Tools, and ReActWorkflow

Use when the requirement needs model-directed typed Tools, bounded repair, or
structured final output.

```text
workflows/react_assistant.py
evals/weather_assistant.py
mock_chat_completions_provider.py
```

The Workflow defines only business Tools, instructions, response schema, and
bounds, then delegates the internal graph to `react_workflow(...)`. Its
Evaluation checks the public answer. The local Chat Completions endpoint makes
the example reproducible without putting Provider construction in Workflow or
Evaluation code.

## Selection rule

- Start from the business contract, not from a sample graph.
- Read only the closest example; read a second when the request genuinely
  combines both behaviors.
- Reuse public API style and validation flow, not business code or file shape.
- Keep a simpler Workflow simpler than every example.
- Prove the new behavior with its own registered Evaluation; copying a pattern
  is never proof that a Workflow compiles or meets its requirement.

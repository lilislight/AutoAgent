---
name: autoagent-author-workflow
description: Create, modify, validate, and test AutoAgent Workflow projects. Use when implementing Workflow, Node, Edge, Condition, Input Mapping, Output Binding, aggregation, Loop, Wait/Resume, Map/Replication, LLM, Tool, or ReActWorkflow behavior; maintaining auto-agent.toml; or fixing AutoAgent project and compiler diagnostics. Do not use for modifying AutoAgent framework internals, RuntimeStore, Server, tracing UI, replay, or fork implementation.
---

# Author AutoAgent Workflows

Create business Workflow definitions without coupling them to an App, database,
Server, or framework internals. Treat the installed public API, CLI output, and
current project source as authoritative.

## Follow this workflow

1. Read the repository guidance that applies to the target directory.
2. Find `auto-agent.toml`, exported Workflow objects, dependency files,
   `.env.example`, input/output models, fixtures, and tests.
3. Translate the request into Workflow input, final output, business steps,
   branches, parallel work, aggregation, loops, waits, external side effects,
   and failure behavior.
4. Read only the references required by the routing table below.
5. Modify the existing project structure when one exists. Do not reorganize a
   project merely to match an example.
6. Implement typed callables and the Workflow graph through `autoagent` and
   `autoagent.ai` public imports only.
7. Update `auto-agent.toml`, dependencies, `.env.example`, fixtures, and tests
   when the change requires them.
8. Run Project Check, Workflow Check, and deterministic Invocation tests.
9. Repair diagnostics by stable code and re-run the failing command.
10. Report the resulting graph, input/output contract, policies, checks, tests,
    environment requirements, and any unverified external dependency.

Ask one focused question only when an unresolved choice would materially change
the Workflow graph or public contract. Otherwise make the smallest reasonable
assumption and continue.

## Route reference reading

Read every selected file completely before authoring:

- New project, Manifest, dependency, or environment boundary:
  [project-contract.md](references/project-contract.md)
- Public imports, types, and supported construction surface:
  [public-api.md](references/public-api.md)
- Node/Edge topology, branches, parallelism, fan-in, Loop, Map, Wait, or child
  Workflow: [workflow-design.md](references/workflow-design.md)
- Callable Operator, Condition, Input Mapping, Output Binding, item selector,
  or aggregator: [hook-contracts.md](references/hook-contracts.md)
- Retry, fallback, timeout, recovery, resource, or failure behavior:
  [policies.md](references/policies.md)
- LLM, Tool, structured output, or ReActWorkflow:
  [ai-workflows.md](references/ai-workflows.md)
- CLI checking, running, resuming, serving, or environment overrides:
  [cli.md](references/cli.md)
- Failed project load or compilation:
  [diagnostics.md](references/diagnostics.md)
- Test design, fixtures, mocks, or acceptance:
  [testing.md](references/testing.md)
- Choosing a normative example:
  [sample-index.md](references/sample-index.md)

Examples of minimal routing:

- A conditional parallel Workflow: public API, Workflow design, Hook contracts,
  Testing, and Sample index.
- A ReAct assistant: public API, AI Workflows, Policies, Testing, and Sample
  index.
- A Loop compilation error: Workflow design, Diagnostics, and CLI.
- A new project: Project contract, public API, Workflow design, CLI, and
  Testing.

## Preserve authoring boundaries

- Export Workflow objects; do not create `AutoAgentApp`, RuntimeStore,
  DatabaseBackend, or Server in Workflow modules.
- Never import `autoagent.core.*` or mutate Runtime aggregates.
- Prefer a typed callable directly as a Node capability. Use `CapabilityRef`
  only for an abstract runtime-provided capability.
- Use stable, descriptive Workflow, Node, and Edge IDs.
- Keep Condition and Input Mapping read-only. Write Invocation or Session
  Context only through Output Binding.
- Prevent overlapping parallel Context writes.
- Bound Loops and dynamic parallel work.
- Keep secrets out of source, Manifest, fixtures, Runtime values, and reports.
- Do not depend on a paid or nondeterministic external service in default tests.
- Do not invent an API. If a referenced public symbol is unavailable in the
  installed version, stop and report the version mismatch.

## Validate before completion

Run from the target project:

```bash
autoagent project check
autoagent workflow list
autoagent workflow check <workflow-id>
```

Then run a deterministic Invocation and the relevant project tests. Validate
every material branch, Loop termination, aggregation boundary, Wait/Resume
contract, and AI schema path introduced by the change.

Do not claim completion when compilation fails, an Invocation ends in
`failed`, `interrupted`, or `cancelled`, or required tests have not run. State
the exact blocker instead.

## Return a compact handoff

Include:

- created or modified Workflow IDs;
- input and output types;
- graph and policy decisions;
- Manifest, dependency, or environment changes;
- commands and tests run with their outcomes;
- assumptions and unverified external behavior.

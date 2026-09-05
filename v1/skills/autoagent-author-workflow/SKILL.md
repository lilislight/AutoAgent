---
name: autoagent-author-workflow
description: Create, modify, evaluate, and validate AutoAgent Workflow projects. Use when implementing Workflow, Node, Edge, Condition, Input Mapping, Output Binding, aggregation, Loop, Wait/Resume, Map/Replication, LLM, Tool, or ReActWorkflow behavior; maintaining auto-agent.toml or Eval Suites; or fixing AutoAgent project and compiler diagnostics. Do not use for modifying AutoAgent framework internals, RuntimeStore, Server, tracing UI, replay, or fork implementation.
---

# Author AutoAgent Workflows

Create business Workflow definitions without coupling them to an App, database,
Server, or framework internals. Treat the selected installed package's public
API, CLI output, and packaged authoring examples as authoritative.

## Follow this workflow

1. Read the repository guidance that applies to the target directory.
2. Ensure the selected Python environment contains the intended AutoAgent
   package. Follow
   [project-contract.md](references/project-contract.md).
3. Locate normative examples inside that installed package. Follow
   [sample-index.md](references/sample-index.md).
4. Find `auto-agent.toml`, exported Workflow objects, registered Evaluation
   classes, dependency files, `.env.example`, and business models.
5. Translate the request into Workflow input, final output, business steps,
   branches, parallel work, aggregation, loops, waits, external side effects,
   and failure behavior.
6. Read only the remaining references required by the routing table below.
7. Modify the existing project structure when one exists. Do not reorganize a
   project merely to match an example.
8. Implement typed callables and the Workflow graph through `autoagent` and
   `autoagent.ai` public imports only.
9. Define or update the smallest Evaluation Cases that protect the requested
   end-to-end business behavior. Keep each Suite attached to one Workflow.
10. Update `auto-agent.toml`, dependencies, and `.env.example` when required.
    Add unit tests only for nontrivial isolated project functions that are not
    already exercised as the same business scenario by Evaluation.
11. Run Project Check, Workflow Check, Eval Check, and the registered Eval
    Suite. Run the smallest relevant unit tests only when the project has them.
12. Repair diagnostics by stable code and re-run the failing command.
13. Report the result, validation performed, and any unverified external
    dependency.

Ask one focused question only when an unresolved choice would materially change
the Workflow graph or public contract. Otherwise make the smallest reasonable
assumption and continue.

## Translate business language

Assume the requester does not know AutoAgent terminology. Accept requirements
that describe business outcomes and infer the smallest suitable graph:

- independent work or a latency requirement implies parallel branches;
- a dynamic collection implies Map;
- repeated execution of the same input implies Replication;
- bounded business reassessment implies a Loop;
- a response arriving later from a person or system implies Wait;
- continuation after process restart implies durable persistence at run time;
- model-directed capability use and bounded self-correction imply
  ReActWorkflow.

Do not ask the requester to choose Nodes, Edges, Hooks, Policies, Event mode, or
Runtime infrastructure. Ask only when two implementations would expose
materially different business behavior. Do not add behavior merely to exercise
a framework feature.

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
- CLI checking, running, resuming, or required environment overrides:
  [cli.md](references/cli.md)
- Failed project load or compilation:
  [diagnostics.md](references/diagnostics.md)
- Business Evaluation, isolated unit tests, fixtures, mocks, or acceptance:
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
- Pass only declared serializable Workflow values across Node, Context, Wait,
  and persistence boundaries: supported scalar/container types, typed Pydantic
  models, dynamic JSON, or `ArtifactRef`. Keep clients, connections, locks,
  classes, generators, and other live resources inside Operator implementations.
- Prevent overlapping parallel Context writes.
- Bound Loops and dynamic parallel work.
- Keep secrets out of source, Manifest, fixtures, Runtime values, and reports.
- Do not depend on a paid or nondeterministic external service in default tests.
- Do not invent an API. If a referenced public symbol is unavailable in the
  installed version, stop and report the version mismatch.
- Do not make authored Workflow code depend on framework internals. When
  reproducible evidence points to an AutoAgent defect, inspecting the installed
  package implementation or explicitly supplied framework source is allowed
  for diagnosis; follow [diagnostics.md](references/diagnostics.md).

## Validate before completion

Run from the target project:

```bash
autoagent project check
autoagent workflow list
autoagent workflow check <workflow-id>
```

Then validate and run the Workflow's registered Evaluation:

```bash
autoagent eval check <suite-id>
autoagent eval run <suite-id>
```

Use Eval Cases for materially different end-to-end business paths introduced
by the change. Unit-test only nontrivial isolated Conditions, mappings, Tools,
Operators, or custom Evaluators when that adds evidence not already supplied by
the Suite. Do not test unrelated framework behavior or duplicate one business
scenario in both places.

Create the static checks and relevant Eval Cases even when the requester asks
only for business behavior. They are part of a complete AutoAgent project, not
requirements the requester must know to request.

Do not claim completion when compilation fails, an Invocation ends in
`failed`, `interrupted`, or `cancelled`, or the relevant Suite has not run.
State the exact blocker instead.

## Return a compact handoff

Name the modified Workflow, summarize its public behavior, list checks and Eval
outcomes, mention any focused unit tests, and identify assumptions or
unverified external behavior.

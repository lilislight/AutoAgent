# AutoAgent TODO

This file tracks the current unfinished work and changes frequently. Stable
stage goals and acceptance criteria live in [MVP.md](MVP.md). Completed
implementation history belongs in Git, tests, and benchmark results.

## 1. Validate the Agent authoring experience

- Forward-test the packaged `autoagent-author-workflow` Skill in clean,
  independent projects using business-only requirements.
- Verify that a Coding Agent can discover or install AutoAgent, locate the
  packaged examples, create `auto-agent.toml`, compile every Workflow, and run
  deterministic tests without reading framework internals.
- Exercise the three normative patterns independently and in combination:
  conditional/parallel/Loop orchestration, durable Wait/Resume, and
  LLM/Tool/ReActWorkflow.
- Use failures from these evaluations to tighten the public API, Compiler
  Diagnostics, examples, and Skill before expanding the toolchain.

## 2. Build the local Agent debugging kit

- Produce a concise, deterministic Invocation report intended for Coding
  Agents. Summarize result, actual graph path, loops, failures, Retry/Fallback,
  Wait/Resume, timing, and relevant identifiers without dumping the complete
  Event journal.
- Add progressive CLI queries for one Invocation, NodeExecution, Event, and
  reconstructed Full-mode state.
- Add Invocation rerun from the original input using a newly loaded Workflow
  definition.
- Add deterministic comparison of old and new Invocations, including result,
  path, execution counts, errors, timing, and relevant Context differences.
- Define legal Full-mode Fork points, Workflow compatibility checks, and a
  backend Fork operation that creates a new Session and Invocation without
  modifying the original execution.
- Add Tracing UI entry points for report, compare, and Fork only after the
  backend contracts are stable.

## 3. Clarify Server and persistence ownership

- Give embedded `AutoAgentServer` Router users an explicit lifecycle ownership
  contract. A host-owned `AutoAgentApp` must not be closed by the Router, while
  standalone Server mode must continue to own startup and shutdown.
- Add an optional local persistence spool for prolonged database outages. The
  current in-memory backlog and admission limit remain the safety boundary
  until this exists.
- Continue validating persistence queue size, serialization cost, retention,
  and recovery latency under high-concurrency and large-output workloads.

## 4. Decide durable UserEvent retention

- Keep the implemented UserEvent queue independent from Runtime Event modes.
  Decide later whether completed semantic UserEvents need durable history and
  which high-volume deltas must remain process-local.
- Define compaction and retention before persisting message or reasoning
  deltas. Never make token-level persistence the default.
- After the Agent UI contract is stable, replace UserEvent SSE polling with
  notification-driven delivery and batch UI updates per render frame. Do not
  optimize the current provisional UI protocol first.

## 5. Add optimization workflows

- Feed Agent-friendly reports and selected production traces into offline
  optimization tools.
- Generate reviewable Workflow code patches instead of mutating an active
  Workflow or Invocation.
- Evaluate candidate patches with recorded inputs, rerun/compare, and Fork
  before promotion.
- Promote accepted changes as a new Workflow definition and revision.

## 6. Prepare for hosted execution

- Separate project management, runner processes, remote persistence ingestion,
  and tracing/query services while preserving the local App/Runtime contracts.
- Add per-Invocation leases, fencing tokens, and idempotent takeover before
  multiple runners can own the same durable RuntimeStore.
- Add Workflow revision publishing, rollback, tenancy, authentication, Secret
  management, quotas, and remote Artifact storage.
- Reuse the local Skill, Compiler Diagnostics, reports, rerun/compare, and Fork
  contracts instead of creating platform-only execution semantics.

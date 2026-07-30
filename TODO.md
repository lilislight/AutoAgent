# AutoAgent TODO

This file tracks the current unfinished work and changes frequently. Stable
stage goals and acceptance criteria live in [MVP.md](MVP.md). Completed
implementation history belongs in Git, tests, and benchmark results.

## 0. Resolve remaining backend audit findings

These are the intentionally deferred findings from the 2026-07-30 backend
audit. Confirmed correctness, retention, SSE, recovery, lifecycle, and database
configuration fixes from that audit are covered by source history and tests.

- [ ] Finish Trace paging for large in-memory overlays. Workflow database
  directories now use keyset pages, a Graph loads one Revision directly by ID,
  and registered Workflow pages are loaded on demand. Session and Invocation
  database endpoints are also keyset-paged. Remaining work is to avoid copying
  and sorting complete in-memory Session and Invocation collections before
  merging each database page, and to add large mixed memory/database soak
  coverage.
- [ ] Bound ReAct conversation history together with the planned summary and
  Context-window work. Until then every Session message is retained and copied
  into each subsequent LLM request. Add a configurable token budget, preserve
  recent complete exchanges, and introduce the summary hook as one coherent
  conversation policy.
- [ ] Document the accepted synchronous Operator timeout limitation. Cancelling
  a `ThreadPoolExecutor` Future cannot stop a function that has already begun;
  users enabling Retry or Fallback must make side effects idempotent. Process
  isolation and forced termination are explicitly deferred.
- [ ] Add long-running soak coverage for the remaining findings. Extend the
  existing Producer ownership, structural-sharing, queue-size, and smoke
  benchmarks with large mutable Context/output workloads; also cover mixed
  memory/database Trace pages and long ReAct Sessions.

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

- Keep the implemented UserEvent journal independent from Runtime Event modes.
  Semantic UserEvents are already durable; `message_delta`,
  `reasoning_delta`, and `tool_call_delta` intentionally remain process-local.
- Define compaction and retention for durable semantic UserEvents and their
  in-memory delta prefix. Never make token-level persistence the default.
- Keep the existing notification-driven UserEvent SSE delivery and add
  transport/UI batching only after the Agent UI contract is stable.

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

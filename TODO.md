# AutoAgent TODO

This file tracks current implementation order. Stable product goals and stage
acceptance criteria live in [MVP.md](MVP.md). Completed implementation history
belongs in Git, tests, and benchmark results.

## 0. Close MVP 1 with one clean release-gate evaluation

The first independent runs of all three Authoring scenarios are complete. Their
findings have already been applied to the Skill, public Host API, ReAct failure
guidance, tests, and database startup responsiveness. Do not continue repairing
the preserved generated projects; they are evidence from the previous Skill
version.

- [ ] Build a fresh Wheel from the current commit and copy the current
  `autoagent-author-workflow` Skill into three empty evaluation workspaces.
- [ ] Rerun all three scenarios from only their business-only
  `REQUIREMENTS.md`: conditional orchestration, durable Wait/Resume, and the
  ReAct inventory assistant. Do not expose `EVALUATION.md`, repository source,
  or previous generated projects to the Coding Agent.
- [ ] Record one concise release-gate report outside the generated projects.
  Score every common hard gate and required business case, include the exact
  Wheel version/commit and commands used, and classify any failure by owning
  layer.
- [ ] Fix only release-blocking findings, rerun the affected scenario from a
  fresh workspace, and declare MVP 1 complete when all three pass.

## 1. Finish bounded Runtime and persistence hardening

These items are real implementation limits, but they do not block starting the
MVP 1 evaluations unless an evaluation reproduces one of them.

- [ ] Split the shared shutdown deadline into independently configurable
  Invocation grace and persistence flush deadlines. Keep both available through
  CLI/environment configuration.
- [ ] Make persistence shutdown a strict upper bound even when a database
  driver does not acknowledge Task cancellation. Log the undurable record count
  and byte size before isolating the stuck daemon Runtime.
- [ ] Finish Trace paging for large in-memory overlays. Database directories are
  keyset-paged and Graphs load by Revision ID, but Session and Invocation pages
  still copy and sort complete in-memory collections before merging a database
  page.
- [ ] Bound ReAct conversation history with one coherent Context-window policy:
  configurable token budget, recent complete exchanges, and an optional summary
  hook. Until then every Session message is retained and copied into later LLM
  requests.
- [ ] Decide the next ReAct failure contract before changing it: distinguish
  infrastructure/transport Tool failures from model-correctable business
  errors, and decide whether `max_steps` should request one bounded final answer
  instead of immediately failing the Invocation. Preserve current behavior
  until that contract and its deterministic tests are agreed.
- [ ] Document the accepted synchronous Operator timeout limitation. A running
  `ThreadPoolExecutor` function cannot be force-stopped, so Retry/Fallback side
  effects must be idempotent. Process isolation remains deferred.
- [ ] Extend soak and benchmark coverage with large mutable Context/output
  values, long ReAct Sessions, mixed memory/database Trace pages, database
  outage recovery, and cancellation-resistant shutdown simulations.

## 2. Build MVP 2 in dependency order

- [ ] After the MVP 1 release gate passes, define one bounded, deterministic
  Invocation report for Coding Agents.
  Include final state/result/error, actual graph path, Loop counts,
  Retry/Fallback/timeout, Wait/Resume, timing, persistence status, and stable
  execution identifiers without dumping the Event journal.
- [ ] Expose the report through the CLI, then add progressive queries for one
  Invocation, NodeExecution, Event, and reconstructed Full-mode state.
- [ ] Add rerun from the original input using a newly loaded immutable Workflow
  definition and a new Invocation.
- [ ] Add deterministic old/new Invocation comparison for result, graph path,
  execution counts, errors, timing, and relevant Context differences.
- [ ] Define legal Full-mode Fork points and Workflow compatibility checks.
- [ ] Implement backend Fork by reconstructing state at a legal boundary and
  creating a new Session and Invocation without mutating the original trace.
- [ ] Add report, compare, rerun, and Fork entry points to the Tracing UI only
  after the backend and CLI contracts are stable.

## 3. Complete local Server and journal ownership

- [ ] Formalize embedded Router lifecycle ownership. Standalone Server and CLI
  shutdown are bounded and close their ProjectHost on the serving Event Loop;
  an externally hosted Router still needs an explicit contract that prevents it
  from closing a host-owned `AutoAgentApp` unexpectedly.
- [ ] Add an optional local persistence spool for prolonged database outages.
  Until then, the in-memory backlog and admission limit are the safety boundary
  and shutdown timeout may abandon undurable records.
- [ ] Define durable semantic UserEvent compaction and retention together with
  the process-local delta prefix. Continue excluding `message_delta`,
  `reasoning_delta`, and `tool_call_delta` from database persistence by default.
- [ ] Add UserEvent transport/UI batching only after the Agent Activity contract
  stabilizes; keep notification-driven SSE delivery.

## 4. Add offline optimization workflows

- [ ] Feed Agent-friendly reports and selected production traces into offline
  optimization tools.
- [ ] Generate reviewable Workflow code patches rather than mutating an active
  Workflow or Invocation.
- [ ] Evaluate candidate patches with recorded inputs, rerun/compare, and Fork
  before promotion.
- [ ] Promote accepted code as a new immutable Workflow Revision.

## 5. Prepare for hosted execution

- [ ] Separate project management, runner processes, remote persistence
  ingestion, Artifact storage, and tracing/query services while preserving the
  local App/Runtime contracts.
- [ ] Add per-Invocation leases, fencing tokens, and idempotent takeover before
  multiple runners can own the same durable RuntimeStore.
- [ ] Add Workflow Revision publishing, rollback, tenancy, authentication,
  authorization, Secret management, quotas, and observability.
- [ ] Reuse the local Skill, Compiler Diagnostics, reports, rerun/compare, and
  Fork contracts instead of creating platform-only execution semantics.

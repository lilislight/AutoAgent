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

Detailed contracts and acceptance criteria live in [MVP2.md](MVP2.md). Eval
definitions use one code-first model, live under `evals/` by convention, and
are registered explicitly in `auto-agent.toml`. `autoagent eval` is the only
public Eval execution surface; do not duplicate Cases as pytest functions.

### Phase 0: Freeze local debugging and evaluation contracts

- [ ] Define versioned Invocation Report, value summary, evidence warning, and
  progressive detail-query models for Minimal, Standard, and Full modes.
- [ ] Define the first version of Eval Suite, Eval Case, unified Step, Eval
  Runner, Eval Run, and Case Result around final Invocation state and final
  business output only.
- [ ] Record graph path, call counts, Retry/Loop/Wait, performance budgets,
  scoring, aggregate Gates, and automatic Invocation capture as later
  extensions rather than Phase 0 requirements.
- [ ] Define the boundary between Invocation Report, Fork, ordinary rerun, and
  Eval; do not require every bad Invocation to become an Eval Case.
- [ ] Define Eval data realism rules: real Workflow execution, redacted or
  synthetic representative inputs, real models when model behavior is under
  evaluation, and sandbox/simulated dependencies when appropriate.
- [ ] Define project testing ownership: Eval for end-to-end Workflow business
  behavior, optional unit tests for isolated user code, with no duplicated
  business scenarios.
- [ ] Define the `[[eval_suites]]` Manifest schema, `module:object` loading, ID
  uniqueness, Workflow targeting, and stable diagnostics.
- [ ] Define the conventional `evals/` layout and `autoagent eval` CLI contract.
- [ ] Define ignored local artifact layout, atomic file format, schema
  versioning, value-size limits, redaction, and incomplete-evidence behavior.
- [ ] Audit current Runtime Events and trace APIs against the Report/Eval models;
  list missing facts before adding new Runtime recording.

### Phase 1: Eval framework and CLI

- [ ] Implement Suite, Case, unified Step, Case Result, and Eval Run models plus
  Manifest loading and stable diagnostics.
- [ ] Implement deterministic final-state and exact-output evaluation plus the
  narrow custom output Evaluator contract.
- [ ] Run isolated Cases through ProjectHost/AutoAgentApp with explicit
  multi-turn and Wait/Resume support.
- [ ] Write atomic local Run artifacts and add `autoagent eval list`, `check`,
  `run`, and `inspect`.
- [ ] Add bounded concurrency, interruption, and Suite-scale performance tests.

### Phase 2: Authoring integration

- [ ] Replace standalone authoring-example input/expected pairs with registered
  Eval Suites and reusable fixtures where appropriate.
- [ ] Update the Authoring Skill to generate and pass Eval Suites before handoff.
- [ ] Forward-test requirement -> Workflow + Suite -> compile -> Eval Run in a
  clean project.

### Phase 3: Invocation Report and progressive queries

- [ ] Add type-neutral read models and a read-only Debug Query service.
- [ ] Build bounded Reports for active, waiting, completed, failed, and partially
  durable Invocations.
- [ ] Add CLI queries for one Invocation, NodeExecution, Edge evaluation,
  Operator Call, Event, or Full-mode state boundary.
- [ ] Add large-journal and large-value performance coverage.

### Phase 4: Local debugging Skill

- [ ] Add a separate Invocation-ID debugging Skill after Report CLI stabilizes.
- [ ] Forward-test Report -> optional Case update -> code change -> compile ->
  Eval Run -> user-review handoff using CLI only.

### Phase 5: Baseline/Candidate comparison

- [ ] Compare compatible Eval Runs by stable Case ID across correctness, path,
  errors, counts, latency, Token, and cost.
- [ ] Add tolerances, aggregate Gates, and `accepted`, `rejected`, or
  `needs_review` decisions.
- [ ] Expose comparison through `autoagent eval compare`.

### Phase 6: Optional Invocation-to-Case capture

- [ ] After the manual loop is stable, capture a provisional redacted Case from
  an Invocation; require an explicit business oracle before Suite installation.

### Phase 7: Optional Full-mode Replay/Fork

- [ ] Add legal Full-mode Replay/Fork and Workflow compatibility validation only
  after the ordinary CLI repair loop is stable.

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

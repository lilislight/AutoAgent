# AutoAgent TODO

This file tracks current implementation order. Stable product goals and stage
acceptance criteria live in [MVP.md](MVP.md). Completed implementation history
belongs in Git, tests, and benchmark results.

## 1. Finish bounded Runtime and persistence hardening

These are cross-cutting post-foundation limits. They do not make the MVP 1
authoring contract incomplete. MVP 1 is closed; continue these items only when
their owning Runtime work is prioritized or a later evaluation reproduces one
of them.

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

- [x] Define versioned Invocation Report, value summary, evidence warning, and
  progressive detail-query models for Minimal, Standard, and Full modes.
- [x] Define the first public Evaluation and result contracts: Manifest Suite
  locator, ``Evaluation`` class, ``eval_*`` Case method, internal Invoke/Resume
  Step, and the Evaluator/Step/Case/Eval Result hierarchy.
- [x] Record graph path, call counts, Retry/Loop/Wait, performance budgets,
  scoring, aggregate Gates, and automatic Invocation capture as later
  extensions rather than Phase 0 requirements.
- [x] Define the boundary between Invocation Report, Fork, ordinary rerun, and
  Eval; do not require every bad Invocation to become an Eval Case.
- [x] Define Eval data realism rules: real Workflow execution, redacted or
  synthetic representative inputs, real models when model behavior is under
  evaluation, and sandbox/simulated dependencies when appropriate.
- [x] Define project testing ownership: Eval for end-to-end Workflow business
  behavior, optional unit tests for isolated user code, with no duplicated
  business scenarios.
- [x] Define the `[[eval_suites]]` Manifest schema, `module:object` loading, ID
  uniqueness, Workflow targeting, and stable diagnostics.
- [x] Define the conventional `evals/` layout and `autoagent eval` CLI contract.
- [x] Define Report value-size limits, redaction, and incomplete-evidence
  behavior. Eval Results remain stdout plus an optional ordinary report file.
- [x] Audit current Runtime Events and trace APIs against the Report/Eval models;
  list missing facts before adding new Runtime recording.

### Phase 1: Eval framework and CLI

- [x] Implement the public Evaluation, EvalCase, Evaluator, built-in
  Invocation-state/result Evaluators, and minimal result models.
- [x] Implement Manifest loading and stable Evaluation diagnostics without
  importing Evaluation modules during normal project or Server startup.
- [x] Run isolated Cases through ProjectHost/AutoAgentApp with explicit
  multi-turn and Wait/Resume support.
- [x] Add `autoagent eval list`, `check`, and `run` with report-file teeing and
  deterministic status/exit-code rendering.
- [x] Implement bounded Case concurrency and an optional Suite-level timeout.
- [x] Add strict concurrency-limit, timeout-cancellation, external task
  cancellation, Runtime cleanup, and Memory/SQLite Suite-scale smoke tests.

### Phase 2: Authoring integration

- [x] Replace standalone authoring-example input/expected pairs with registered
  Eval Suites and reusable fixtures where appropriate.
- [x] Update the Authoring Skill to generate and pass Eval Suites before handoff.
- [x] Forward-test a fresh conditional-orchestration project from only its
  business requirements using the packaged Wheel and copied Authoring Skill.
  Project/Workflow/Eval checks passed and all nine business Cases passed.
- [x] Review the generated project against the hidden hard gates and authoring
  boundaries. No blocking Skill or framework defect was found; one scenario is
  accepted as sufficient coverage for the current Authoring Skill milestone.
- [x] Close Phase 2. Durable Wait/Resume and ReAct forward-test workspaces remain
  optional future regression fixtures rather than MVP 2 gates.

### Phase 3: Invocation Report and progressive queries

Phase 3 is closed. Report and progressive-query contracts are stable inputs to
the local debugging loop.

- [x] Add same-project Server discovery and evidence-source resolution: prefer
  the matching live Server, otherwise use an explicitly configured database,
  and return an actionable missing-source diagnostic when neither exists.
- [x] Add the accepted `autoagent.debug` V1 read models: one
  `InvocationReport` plus bounded value, error, primary-boundary, and warning
  models; keep them outside the root Workflow authoring API.
- [x] Add a type-neutral read-only Debug Query service shared by CLI, Server,
  and future platform adapters.
- [x] Build bounded Reports for active, waiting, completed, failed, and partially
  durable Invocations.
- [x] For `created` or `running` Server Invocations, wait through notifications
  for at most 10 seconds for `waiting` or a terminal boundary, then return the
  current Report with an explicit still-running warning.
- [x] Keep the root Report compact and expose NodeExecution, Edge evaluation,
  Operator Call, Event, and Full-mode state collections through stable cursor
  pages fixed to the Report's observed sequence.
- [x] Record each actual Map/Replication attempt as an Operator Call Event,
  keep its Full-mode values in one place, support NodeExecution-filtered Call
  pages, and bound the Timeline projection to 50 Call rows per execution.
- [x] Include only categorized UserEvent counts in the root Report; page
  completed semantic/custom events separately and exclude stream deltas unless
  a stream-diagnostic query explicitly requests them.
- [x] Add direct CLI detail queries for one Invocation, NodeExecution, Edge
  evaluation, Operator Call, Event, or Full-mode state boundary.
- [x] Keep Report and comparison results out of Runtime persistence; render to
  stdout and optionally tee the same content to an ordinary `--report-file`.
- [x] Complete fixture coverage for Minimal, Standard, Full, partial durability,
  Loop, Retry/Fallback, Map, and ReAct evidence. Core active, waiting,
  completed, failed, memory, and historical-database paths are covered.
- [x] Add large-journal and large-value performance coverage. The SQLite smoke
  guard uses an 80-Node journal plus a 100 KB Invocation input and bounds both
  query latency and rendered Report/page size.

### Phase 4: Local debugging Skill

**Current focus:** forward-test the complete CLI-only debugging loop from one
real failed or business-incorrect Invocation. The test must exercise Report,
bounded Query, a focused code repair, same-mode Rerun, Comparison, and the
owning Eval Suite without using framework-internal APIs.

- [x] Add a separate Invocation-ID debugging Skill after Report CLI stabilizes.
- [x] Add isolated same-mode Rerun against the current project Revision. Reuse
  exact Standard/Full Genesis Session Context and reject Minimal evidence.
- [x] Add read-only Invocation Comparison for matching Standard or matching
  Full modes with semantic Node, Loop, Edge, and actual Operator Call alignment;
  reject Minimal and mixed modes and keep results out of Runtime storage.
- [x] Update the debugging Skill with side-effect, Wait, Rerun, Comparison, and
  Eval ownership boundaries.
- [x] Add a deterministic Full-mode Debug Skill Evaluation with an existing
  project, business-incorrect Invocation input, registered four-Case Eval
  Suite, and hidden Report/Query/Rerun/Comparison acceptance gates.
- [ ] Forward-test Report -> optional Case update -> code change -> compile ->
  Rerun -> Comparison -> Eval Result -> user-review handoff using CLI only.

### Phase 5: Optional Invocation-to-Case capture

- [ ] After the manual loop is stable, capture a provisional redacted Case from
  an Invocation; require an explicit business oracle before Suite installation.

### Phase 6: Optional Full-mode Replay/Fork

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
- [ ] Evaluate candidate patches with registered Suites, rerun, and Fork
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
- [ ] Reuse the local Skill, Compiler Diagnostics, reports, rerun, Eval, and
  Fork contracts instead of creating platform-only execution semantics.

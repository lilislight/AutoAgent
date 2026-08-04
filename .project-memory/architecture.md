# Architecture

## System Overview

AutoAgent separates static Workflow definition from deployment configuration and live execution. Python Workflow objects compile into validated Workflow IR and a content-derived revision snapshot. AutoAgentApp registers those revisions, owns application-local Operator registries and lifecycle, and delegates each Invocation to the Scheduler and Executors. RuntimeStore remains the authoritative live aggregate while an optional backend consumes persistence boundaries asynchronously. Project, CLI, Evaluation, Server, and UI layers all reuse this same App and Runtime path.

## Module Map

- [Workflow Authoring](modules/workflow-authoring.md) — static Workflow, Node, Edge, hook, event-mapping, and policy definitions exposed to authors.
- [Operator System](modules/operator-system.md) — callable contracts, Capability and Operator registration, selection, and streaming result contracts.
- [Workflow Compilation](modules/workflow-compilation.md) — child expansion, validation, Workflow IR, revision snapshots, diagnostics, analysis, and preview.
- [Runtime Execution](modules/runtime-execution.md) — App lifecycle, Session and Invocation state, scheduling, node execution, Events, Wait/Resume, recovery, and retention.
- [Runtime Persistence](modules/runtime-persistence.md) — bounded asynchronous durability, serialization, artifacts, and SQLite/PostgreSQL storage.
- [AI Building Blocks](modules/ai-building-blocks.md) — provider-neutral LLM models, Providers, Tools, llm_call, and ReAct Workflow construction.
- [Project and CLI Hosting](modules/project-and-cli-hosting.md) — manifest loading, environment resolution, ProjectHost ownership, local/Server CLI execution, and reporting.
- [Evaluation](modules/evaluation.md) — manifest-owned Eval Suites, isolated Cases, evidence, Evaluators, and result aggregation.
- [Tracing Server and UI](modules/tracing-server-and-ui.md) — execution HTTP API, trace projection/query services, SSE notifications, and the embedded inspection UI.

## Key Flows

1. **Author and compile:** Workflow source enters WorkflowCompiler, child Workflows are expanded, graph/contracts/policies are validated, and successful compilation yields Workflow IR plus a definition-hash revision snapshot. WorkflowAnalysis and Preview reuse compiler state but are tooling views, not runtime inputs.
2. **Register and start:** ProjectLoader imports only manifest-declared objects. ProjectHost resolves deployment environment, creates AutoAgentApp, installs required AI Providers, registers Workflows, then explicitly starts the App. Startup initializes RuntimeStore and recovers only registered durable revisions.
3. **Invoke:** AutoAgentApp admits a Session/Invocation into RuntimeStore. WorkflowExecutor runs the control loop: Scheduler produces scoped ready work, NodeExecutor resolves and executes Operators, results and edge decisions mutate Runtime state, and Runtime Events are recorded after changes are applied.
4. **Persist:** RuntimeStore hands immutable envelopes to PersistenceCoordinator. DatabaseBackend serializes, batches fairly across Session queues, externalizes large values, and writes on its own runtime loop. Recovery state is periodically compacted while Event sequence remains the latest durable journal position.
5. **Observe and control:** AutoAgentServer exposes registered Workflow execution and paged trace APIs. TraceService projects Runtime Events and merges live memory with durable history where required. SSE wakes the UI for status, directory, Invocation, and User Event changes.
6. **Evaluate:** EvaluationLoader resolves a manifest Suite only when requested. EvaluationRunner gives every Case an isolated Session, executes invoke/resume through ProjectHost in Full Event mode, and supplies bounded Runtime evidence to Evaluators.

## Global Boundaries and Rules

- Current source and tests are authoritative; `docs-deprecated/` is archived design material.
- Workflow behavior belongs in Workflow, Node, Edge, hook, and policy definitions. Database, concurrency, Provider credentials, Server, and process lifecycle belong to the host environment.
- Only successful Workflow IR is executable. WorkflowAnalysis exists for diagnostics and display even when compilation fails.
- RuntimeStore owns latest execution state whether or not a durable backend is attached. The database is a downstream durability and historical-query layer, not the live execution owner.
- Runtime Events and User Events are independent ordered journals. User Events do not participate in execution replay or recovery.
- App startup and Workflow registration are explicit. A durable Invocation is recovered only when its exact Workflow revision is registered.
- One Session cannot admit a second active Invocation while its current Invocation is created, running, or waiting.
- The root package `__all__` is the stable Workflow-authoring contract; hosting, compiler, Runtime, persistence, and Server types remain owned by their modules.

## Runtime and Deployment Shape

Local execution uses one AutoAgentApp with the authoritative in-memory RuntimeStore. Adding DatabaseBackend keeps the same Store model and introduces a separate persistence runtime loop for SQLite or PostgreSQL. The CLI runs Invocations locally by default; explicit Server mode uses the HTTP execution API, while `submit` returns after admission. AutoAgentServer can run standalone or be embedded as a FastAPI Router and may serve the bundled tracing UI.

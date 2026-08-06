---
code_paths:
  - autoagent/core/app/
  - autoagent/core/executor/
  - autoagent/core/scheduler/
  - autoagent/core/runtime/store.py
  - autoagent/core/runtime/invocation.py
  - autoagent/core/runtime/session.py
  - autoagent/core/runtime/execution.py
  - autoagent/core/runtime/scheduler.py
  - autoagent/core/runtime/context.py
  - autoagent/core/runtime/event.py
  - autoagent/core/runtime/user_event.py
  - autoagent/core/runtime/snapshot.py
tags:
  - runtime-execution
  - scheduling
  - recovery
  - runtime-events
  - wait-resume
---

# Runtime Execution

## Responsibility

Own App lifecycle and registered revisions, admit and execute Sessions and Invocations, schedule graph work, run Nodes and Operators, maintain authoritative in-memory state, and record replay/recovery evidence.

## Current Design

AutoAgentApp owns registries, Compiler, RuntimeStore, WorkflowExecutor, and one dedicated RuntimeEventLoop that supports mixed synchronous and asynchronous entry points. WorkflowExecutor is the control-plane bridge between Scheduler decisions, NodeExecutor tasks, Runtime mutation, and Event recording. Scheduler advances scoped Node instances using ordinary fan-out and complete fan-in, with compiler-derived natural-loop scopes. NodeExecutor applies Node-owned Map/Replication after WorkflowExecutor constructs the current logical input, then handles contract validation, retry/fallback, timeout, aggregation, streaming, and isolated Output Binding. A Map selector receives both the normalized input and the complete current activation set, so fan-in and Loop headers retain ordinary graph occurrence semantics.

RuntimeStore is one authoritative in-memory aggregate for Workflow revisions, Sessions, Invocations, outputs, Runtime Events, User Events, reduced state, and replay checkpoints. Event modes trade recording cost for capability: Minimal retains terminal/wait state, Standard records graph-level facts and recovery points, and Full also records internal phases and state operations.

Every actual Operator attempt, including each Map item, Replication unit, retry, and fallback, produces its own terminal Operator Call Event with exact timing, stable unit identity, and a name matching its completed, failed, cancelled, or interrupted outcome. NodeExecution retains only bounded aggregate counters and an optional Map/Replication summary; individual Call values live in Events rather than executable Runtime state. In Full mode the Call Event is the canonical recorded owner of that attempt's input and output. Selector and Output Binding phases do not duplicate those values, while Aggregation records only the final logical Node output.

Runtime Context and Invocation input are canonical JSON values. Typed Node outputs remain typed during live execution, while Dynamic JSON contracts normalize values immediately. Recovery and Resume materialize persisted Node JSON through the exact registered Workflow revision before graph execution continues.

## Boundaries and Rules

- App startup is explicit; Workflows that may recover durable work must be registered before startup.
- Invocation sequence starts at one and is local to each Invocation; sequence zero is the genesis execution snapshot.
- Runtime changes are applied before the corresponding Runtime Event is recorded.
- Standard recovery checkpoints are compact state images near durable Event positions; Full mode can rebuild through state operations.
- Recovery reuses normal graph behavior in recovery mode and stops or skips when Node recovery policy forbids execution.
- Output Binding commits Context transactionally and rejects any non-serializable value without publishing partial changes.
- Runtime Events support execution state, replay, and recovery. User Events are an independent semantic/live journal.
- Recovery checkpoints contain Operator summaries but not unbounded per-Call records; a Node interrupted mid-execution is recovered according to its Node recovery policy.
- Waiting, running, or newly created work makes its Session busy; later Invocation admission is rejected until the active Invocation settles.
- Resume claims process-local Invocation ownership atomically before consuming a Wait; concurrent Resume requests cannot execute the same continuation twice.

## Relationships

Workflow Compilation supplies Workflow IR and revision snapshots. Operator System supplies executable implementations. Runtime Persistence consumes immutable Store boundaries. ProjectHost and Server own App lifecycle. Evaluation and tracing read Runtime evidence without introducing a separate execution engine.

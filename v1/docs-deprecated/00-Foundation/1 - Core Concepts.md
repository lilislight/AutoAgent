# Core Concepts

This document defines the foundational concepts of AutoAgent OS.

AutoAgent OS reinterprets Agent systems through operating-system abstractions.
The goal is not to copy an operating system mechanically, but to use operating
system thinking to separate program definition, compilation, invocation, runtime
state, scheduling, execution control, computation, and optimization.

## Design Scope

AutoAgent OS uses operating-system thinking to organize autonomous software
execution, but the first version should remain a framework, not a full operating
system.

The goal is to provide OS-like architectural boundaries: Workflow defines the
program, Compiler produces Workflow IR, Input Adapters turn outside-world events
or calls into Workflow Invocations, Runtime Session stores durable execution
state, Runtime Run stores one invocation's execution state, Scheduler decides the
next graph action, WorkflowExecutor drives the run loop, NodeExecutor executes
nodes, StateManager persists state, and Operators provide computation.

The first version should focus on executing one Workflow or a small number of
Workflows reliably. It should not attempt to provide a full daemon runtime,
distributed scheduler, cross-session IPC, live migration, or general-purpose
process management.

AutoAgent OS should feel like an operating system at the architecture boundary,
but like a framework at the authoring and implementation boundary.

## Capability Levels

The system design should distinguish core framework features from advanced
OS-like extensions.

### Level 1: Framework Core

These are expected Version 1 capabilities:

- Workflow execution through nodes and edges
- Edge conditions
- Basic join behavior
- Basic routing behavior
- Multiple Workflow entry nodes, with one selected entry per invocation
- Workflow invocation with explicit input
- Runtime Session lookup or creation by session key
- One Runtime Run per invocation
- Operator invocation
- Runtime Context
- Retry, timeout, and resource policy
- Waiting nodes and resume through external events

### Level 2: Autonomous Runtime Extensions

These are important but can be designed after the core model is stable:

- Optimizer patches
- Nested Workflow through child Runtime Session or child Runtime Run
- Dynamic map/fan-out
- Checkpoint and recovery
- Multiple coordinated entries sharing one Runtime Session state model
- Same-session concurrent Runtime Runs with explicit context conflict handling

### Level 3: OS-like Advanced Features

These should be treated as future capabilities:

- Hot patching
- Streaming graph execution
- Cross-session communication
- Daemon-style Workflow lifecycle
- Distributed scheduling
- Runtime graph mutation
- Live migration

## Workflow

A Workflow is the static program written by the developer.

From the developer's perspective, using AutoAgent OS means writing a Workflow and
letting the operating system execute it. The developer should not need to manage
node dispatch, runtime scheduling, state persistence, or execution recovery
inside the Workflow itself.

A Workflow defines what the autonomous software can do. It may describe logic,
steps, dependencies, control flow, constraints, and expected behavior, but it is
not itself the runtime executor.

A Workflow is invoked with explicit input. Long-running listeners, schedulers, or
message consumers live outside the Workflow and call into it.

Operating system analogy:

```text
Program source code -> Workflow
```

## Compiler

The Compiler transforms a Workflow into Workflow IR.

It is responsible for converting developer-facing Workflow definitions into a
runtime-readable intermediate representation. The Compiler may validate the
Workflow, normalize its structure, resolve static references, and produce IR
nodes, edges, bindings, entry nodes, graph indexes, and constraints.

The Compiler does not execute the Workflow. It prepares the Workflow for runtime
execution.

Operating system analogy:

```text
Compiler / interpreter -> Workflow Compiler
```

## Workflow IR

Workflow IR is the static compiled representation of a Workflow.

It describes possible execution structure, not the runtime state of a particular
execution.

Workflow IR may contain:

- Nodes: static workflow steps that may be scheduled at runtime.
- Edges: possible transitions, dependencies, or control-flow relationships.
- Entry nodes: internal start locations that an invocation may select.
- Exit nodes: terminal nodes or terminal side-effect nodes.
- Graph indexes: incoming edges, outgoing edges, predecessors, successors.
- Static constraints: declared requirements, limits, or policies.
- Bindings: how inputs and outputs are connected.

A node may contain an execution specification, but it is not itself a runtime
command. Runtime work is produced during execution by WorkflowExecutor based on
Scheduler decisions, Workflow IR, Runtime Session data, and Runtime Run state.

Operating system analogy:

```text
Control-flow graph / intermediate representation -> Workflow IR
```

## Input Adapter and Invocation

An Input Adapter is an external component that converts outside-world input into
a Workflow Invocation.

Examples include API handlers, Pub/Sub consumers, webhook listeners, cron
schedulers, manual triggers, and calls from other Workflows.

Input Adapters may be long-running. They are not Workflow Runtime Sessions and
they are not ordinary Workflow nodes in the first design.

A Workflow Invocation is one call into a Workflow with explicit input. It selects
one entry node and provides the input payload used to start one Runtime Run.

An Invocation may contain:

- Workflow id and version
- Entry node id
- Input payload
- Optional session key
- Source metadata
- Idempotency metadata

If no session key is provided, Runtime may create a fresh session key. If a
session key is provided, Runtime may look up or create the corresponding Runtime
Session and start a new Runtime Run inside it.

```text
Input Adapter -> Invocation -> Runtime Session lookup/create -> Runtime Run
```

This keeps long-running listeners outside Workflow execution while still allowing
Workflows to respond to API calls, messages, schedules, and external events.

## Runtime Session

A Runtime Session is a durable state container for a Workflow execution context.

A Runtime Session may represent one short-lived program invocation, or it may
represent a longer-lived context such as a conversation, ticket, user task, or
business process. Session reuse is controlled by a session key supplied at
invocation time. If callers do not supply a session key, Runtime can generate a
new one, producing ordinary one-off execution behavior.

A Runtime Session owns state that should survive across invocations for the same
session key.

A Runtime Session may contain:

- Session identity: session id or session key, Workflow id, Workflow version,
  and lifecycle state.
- Runtime Context: durable data environment such as variables, conversation
  history, memory references, artifact references, and cross-run values.
- Run History: records of Runtime Runs created inside the session.
- Event Log: runtime events observed by the system, such as invocations,
  Operator results, errors, timeout signals, external callbacks, and human
  input.
- Resource Usage: runtime resource consumption across the session, such as time,
  tokens, cost, memory, or other resource counters.
- Checkpoint Data: persisted recovery data used to resume or recover execution.

Runtime Context is not the same as LLM message history. LLM history may be one
kind of data stored in or built from Runtime Context, but it is not the whole
context model.

Operating system analogy:

```text
Process / conversation / durable execution context -> Runtime Session
Process memory / runtime environment -> Runtime Context
```

## Runtime Run

A Runtime Run is one invocation execution inside a Runtime Session.

Each Invocation creates execution by creating a Runtime Run. A short script-like
Workflow may have one session with one run. A chatbot may have one session per
conversation and one run per incoming user message.

A Runtime Run owns dynamic state for one pass through the Workflow graph.

A Runtime Run may contain:

- Run identity: run id, entry node id, invocation id, and lifecycle state.
- Node State: pending, ready, running, completed, failed, skipped, waiting, or
  cancelled nodes for this run.
- Edge State: pending, satisfied, skipped, or failed edges for this run.
- Ready Queue: nodes ready to be scheduled.
- Running Nodes: nodes currently being executed.
- Execution History: executed nodes, attempted transitions, retries, and
  previous scheduling decisions for this run.
- Run-local resource usage and errors.

The distinction between Runtime Session and Runtime Run prevents repeated
invocations from overwriting each other while still allowing them to share
durable context.

```text
Runtime Session
    Runtime Context shared across runs
    Run 1: node/edge state for invocation 1
    Run 2: node/edge state for invocation 2
```

## Scheduler

The Scheduler decides what should happen next.

It reads the Workflow IR, Runtime Session, and Runtime Run, then decides which
node, transition, or runtime action should happen next.

The Scheduler does not execute computation directly. It produces scheduling
decisions for WorkflowExecutor.

A Scheduler may consider run state, ready nodes, completed nodes, failed nodes,
edge conditions, runtime context, events, and graph policies.

Conceptually, the Scheduler is separate from NodeExecutor because graph
readiness and node execution are different responsibilities.

Operating system analogy:

```text
OS scheduler -> AutoAgent OS Scheduler
```

## WorkflowExecutor and NodeExecutor

WorkflowExecutor drives a Runtime Run.

It receives decisions from Scheduler, applies proposed state changes through
StateManager, dispatches selected nodes to NodeExecutor, and stops when the run
waits, completes, or fails.

NodeExecutor executes selected Workflow nodes. It prepares node input from
Workflow IR and Runtime Context, checks execution policies, invokes Operators or
system capabilities, and returns state changes for completion, failure, or
waiting.

StateManager is the only component that applies and persists runtime state
changes. Scheduler and NodeExecutor should return proposed changes rather than
mutating Runtime Run objects directly.

Operating system analogy:

```text
Program runner / execution engine -> WorkflowExecutor
Device or process executor -> NodeExecutor
```

## Operator

An Operator is a reusable computation capability managed by AutoAgent OS.

Operators are static capabilities. They define what kind of computation can be
performed, but they do not represent one particular runtime execution and do not
own workflow control flow, Runtime Session state, or Runtime Run state.

Operators may include:

- LLMs
- Functions
- Python execution
- Browsers
- Search engines
- Databases
- MCP services
- Shell commands
- Future execution engines

From the operating system's perspective, an LLM and a function are both
Operators. One may be probabilistic and the other deterministic, but both are
managed through the same computation model.

Invoking an Operator is one kind of node execution. NodeExecutor prepares the
runtime input, dispatches the Operator, receives the result, and returns state
changes that write results back into Runtime Session or Runtime Run state
according to compiled bindings.

Operating system analogy:

```text
Device / executable computation capability -> Operator
```

## Node Granularity

A Workflow node is a management boundary, not a line-of-code boundary.

AutoAgent OS should not force every small program step through Scheduler and
NodeExecutor. Ordinary implementation details should stay inside Operators. A
step should become a node when the system benefits from managing it
independently, such as for retry, timeout, failure routing, observability,
parallelism, human approval, expensive execution, external side effects, or
optimizer patches.

This keeps the framework from turning ordinary function calls into unnecessarily
heavy runtime scheduling work.

## Optimizer

The Optimizer improves Workflow definitions using runtime evidence collected
from Runtime Sessions and Runtime Runs.

A Workflow is initially written by the developer, but it may be executed many
times across many Runtime Sessions and Runtime Runs. Those executions produce
data such as failures, timeouts, resource usage, Operator results, execution
history, event logs, and human feedback.

The Optimizer analyzes this runtime evidence and proposes changes to the
Workflow.

Possible optimization targets include:

- Replacing an Operator used by a node
- Adding a fallback path for frequent failures
- Adjusting retry or timeout behavior
- Splitting a node into smaller nodes
- Adding validation or preprocessing nodes
- Improving prompts or input mappings
- Removing or de-prioritizing rarely useful paths

The Optimizer should not directly mutate an actively running Runtime Session or
Runtime Run. Instead, it should produce a Workflow Patch: a structured proposal
for changing the Workflow.

A patch may be applied statically, affecting only future Runtime Runs, or it may
eventually support hot patching, where a running Runtime Run is migrated to a
modified Workflow. Static patching is the simpler and safer default. Hot patching
requires additional design around state migration, compatibility, and recovery.

The Optimizer is conceptually separate from the Compiler. The Compiler converts
a Workflow into Workflow IR. The Optimizer uses runtime evidence to propose how
the Workflow itself should change.

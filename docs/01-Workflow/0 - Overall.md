# Overall

This document defines the high-level design direction of the Workflow module.
Detailed data models are defined in the Workflow, Node, Edge, and Workflow
Builder documents.

## Purpose

A Workflow is the static program definition for AutoAgent OS.

It describes executable structure, but it does not execute. Execution is handled
later by Compiler, Input Adapter, Runtime Session, Runtime Run, Scheduler,
NodeExecutor, and Operators.

A Workflow should describe the program graph: what nodes exist, how they are
connected, how data should flow, and which policies apply to schedulable steps.
It should not own long-running listeners, trigger loops, or runtime state.

## Core Structure

A Workflow is primarily made of:

- Nodes: OS-visible execution units.
- Edges: control-flow, dependency, or transition relationships between nodes.

The Workflow model should stay structural.

Nodes should represent work that benefits from runtime management: independent
retry, timeout, failure routing, observability, parallelism, human approval,
external side effects, expensive computation, or optimizer patching.

Ordinary implementation details should remain inside Operators. AutoAgent OS
manages workflow-level execution, not every line of application logic.

## Input Boundary

A Workflow is invoked with explicit input.

External triggers are not ordinary Workflow execution in the first design. API
handlers, Pub/Sub consumers, webhook listeners, cron schedulers, and manual
triggers belong to an input adapter layer outside Runtime Session and Runtime
Run. These components may be long-running, but they are Input Adapters, not
Workflow nodes.

An Input Adapter creates a Workflow Invocation. Runtime uses that invocation to
find or create a Runtime Session and create a Runtime Run.

```text
Input Adapter -> Invocation -> Runtime Session -> Runtime Run
```

The Workflow begins when a Runtime Run starts at a selected entry node. The
long-running listener that produced the invocation does not become part of that
run.

Entry nodes do not own triggering behavior in the first design. They do not subscribe, listen, schedule, or decide how sessions are reused. They are internal
start locations selected by a Workflow Invocation.

## Runtime Session and Runtime Run

A Runtime Session stores durable state for a Workflow context.

A Runtime Run stores one invocation's pass through the Workflow graph.

This distinction matters for workflows such as chatbots. A conversation may use
one Runtime Session keyed by conversation id, while each incoming message creates
a new Runtime Run inside that session. The session preserves conversation
history; the run stores node states, edge states, and ready queues for that
message.

If the caller does not provide a session key, Runtime may generate a fresh key,
which gives ordinary one-off execution behavior. If the caller provides a stable
session key, Runtime can reuse the existing session.

## Entry

A Workflow may have zero, one, or many entry nodes.

Entry nodes represent internal start locations in the Workflow graph. They do
not represent long-running external listeners and they do not own triggering behavior.

Entry nodes may be explicitly marked, or inferred by the Compiler from graph
structure. Nodes with no incoming edges are entry candidates.

Multiple entries are useful when the same Workflow exposes different ways to
enter the same program graph. For example, one entry may start from a full API
request, another from a Pub/Sub event payload, and another from a manual
reprocess command. Each invocation selects one entry node for that Runtime Run.

Version 1 behavior:

```text
each invocation selects one entry node
each invocation creates one Runtime Run
session reuse is controlled by runtime session key
```

## Exit

A Workflow may have zero, one, or many exit nodes.

Exit nodes represent terminal points or terminal side-effect steps in a Runtime
Run. They are not long-running output daemons.

Nodes with no outgoing edges are exit candidates. Long-running or wait-heavy
Workflows may have no simple natural exit, but the first design should prefer
clear terminal graph structure where possible.

Multiple exit nodes are useful for ordinary program structure:

- Different branches may terminate in different places.
- A successful branch and a failure branch may have different terminal behavior.
- Several terminal side effects may be represented as separate nodes when they
  need independent logging, retry, timeout, or failure handling.

If several final side effects must all finish, the Workflow can model them as a
serial chain or parallel branches joined before completion. If a side effect is
fire-and-forget, the node's completion can mean successful dispatch rather than
remote completion. Waiting for a remote result should be modeled explicitly with
a wait or callback node.

Start and End nodes may exist as optional system nodes, but they are not required
Workflow primitives.

## Nested Workflow

Workflow nesting is supported through nodes.

A parent Workflow may contain a node that references another Workflow capability.
The preferred runtime model is child execution. The parent sees the nested
Workflow as one node execution result, not as internal child edges.

The exact child execution unit may be a child Runtime Session or a child Runtime
Run depending on runtime design. The important boundary is that the parent graph
does not directly schedule the child graph's internal nodes.

## Authoring Formats

Workflow should support multiple authoring frontends that converge on the same
canonical Workflow model:

- Python API for object-based, type-friendly authoring.
- YAML/JSON for serializable, declarative workflow definitions.
- Future UI or optimizer patch frontends.

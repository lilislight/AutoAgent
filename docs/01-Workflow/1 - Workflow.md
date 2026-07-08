# Workflow

This document defines the canonical Workflow data model.

A Workflow is a static program definition. It is compiled into Workflow IR before
execution.

## Python Model

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Workflow:
    """Static workflow program definition."""

    id: str
    version: str
    nodes: list["Node"]
    edges: list["Edge"]

    name: str | None = None
    description: str | None = None
    policy: "WorkflowPolicy | None" = None
    labels: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
```

## Fields

### id

Stable Workflow identifier used by registries, Compiler, Runtime, Optimizer, and
external references.

### version

Immutable version identifier. Once a Workflow is compiled or executed, changes
should produce a new version.

### nodes

Static node definitions. Workflow stores nodes as definitions only. Derived graph
indexes belong to Workflow IR.

A node should represent a step worth managing independently at the AutoAgent OS
layer. Ordinary helper logic should remain inside Operators.

### edges

Static edge definitions. Edges reference nodes and describe graph relationships.
Compiler validates and derives graph structure from nodes and edges.

### name

Optional human-readable name.

### description

Optional human-readable description.

### policy

Optional Workflow-level policy placeholder. Detailed WorkflowPolicy design is
deferred.

Workflow-level policy should not be used to model long-running external
listeners. API handlers, Pub/Sub consumers, webhook listeners, and schedulers are
input-adapter-layer components.

### labels

Small string key-value pairs for indexing, filtering, and management.

### metadata

Non-semantic auxiliary data for tooling, visualization, debugging, optimizer
notes, or external integrations.

If metadata starts affecting compilation or runtime behavior, it should become an
explicit typed field.

## Excluded Core Fields

The core Workflow object does not require top-level inputs, outputs, external
triggers, listener loops, Start node, or End node.

External triggers and listeners belong to the input adapter layer. They produce
Invocations that call a compiled Workflow with explicit input and a selected
entry node.
They are not ordinary Workflow execution in the first design.

Inputs are represented at runtime by invocation input payload, Runtime Context, and
node input mappings. Outputs and side effects are represented by nodes when they
need OS-level management, or by Operator implementation details when they do not.

Start and End nodes may exist as optional system nodes, but they are not required
Workflow primitives.

## Entry and Session Reuse

Workflow nodes may mark internal entry locations. The Compiler can use explicit
entry markers or graph inference to produce Workflow IR entry nodes.

Session reuse is not part of the static Workflow object. Runtime decides whether
to create or reuse a Runtime Session based on the invocation session key.

```text
no session key -> Runtime may generate a fresh session
same session key -> Runtime may reuse the existing session
```

Each invocation creates a Runtime Run inside the selected Runtime Session.

## Example

```python
workflow = Workflow(
    id="github_issue_triage",
    version="1.0.0",
    name="GitHub Issue Triage",
    labels={"domain": "github"},
    nodes=[],
    edges=[],
)
```

## Open Questions

- Should Workflow version be semantic version, content hash, or both?
- What should WorkflowPolicy include in the first implementation?
- Should labels remain on Workflow or move to a registry-level index model?
- Should Workflow support explicit source-level entry declarations beyond node entry markers?
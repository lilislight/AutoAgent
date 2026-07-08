# Overall

This document defines the high-level design direction of the Compiler module.

The Compiler transforms a canonical Workflow into Workflow IR.

```text
Workflow -> Compiler -> Workflow IR
```

Workflow IR is the static runtime-readable program structure used by Scheduler
and Kernel. It is not Runtime Session state and it is not Runtime Run state.

## Position

The Compiler sits between Workflow authoring and runtime execution.

```text
Authoring frontend -> Workflow -> Compiler -> Workflow IR -> Runtime
```

Authoring frontends include Python API, YAML, JSON, visual builders, and
Optimizer patches. They should produce the canonical Workflow model before
compilation.

The Compiler should not parse authoring formats directly, create Runtime
Sessions, create Runtime Runs, invoke Operators, or update runtime state.

## Output

Compiler output should be a structured result.

```text
CompileResult
    Workflow IR
    Diagnostics
    Source map
```

If compilation fails, `workflow_ir` may be absent while diagnostics explain why.

## Core Responsibilities

The Compiler prepares a Workflow for runtime execution.

It should:

- validate Workflow structure
- build graph indexes
- infer entry and exit nodes
- validate loops and reject dead loops
- resolve capability references
- compile conditions
- compile input plans and output bindings
- normalize policies
- check static schema compatibility where possible
- emit Workflow IR
- emit structured diagnostics

## Boundary

Workflow IR contains static program structure:

- compiled nodes and edges
- graph indexes
- entry and exit node ids
- resolved capability descriptors
- compiled conditions
- input plans and output bindings
- compiled policies

Workflow IR must not contain runtime state:

- node or edge runtime status
- Operator results
- Runtime Context values
- Event Log entries
- retry counters
- resource usage
- Scheduler decisions

Those belong to Runtime Session, Runtime Run, Scheduler, Kernel, or runtime
infrastructure.

## Design Goal

The Compiler should make runtime components simpler.

Scheduler should not repeatedly derive graph structure from raw Workflow objects.
Kernel should not parse authoring forms or loose capability strings. Runtime
Session and Runtime Run should not store static graph information.
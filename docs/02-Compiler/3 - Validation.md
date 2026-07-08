# Validation

This document defines Compiler validation responsibilities.

Loader validation checks whether a Workflow object can be created. Compiler
validation checks whether that Workflow can become executable Workflow IR.

## Diagnostic Model

Diagnostics should be structured and machine-readable.

```python
from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass(frozen=True)
class Diagnostic:
    code: str
    severity: Literal["error", "warning", "info"]
    message: str
    location: "SourceLocation | None" = None
    subject: str | None = None
    hints: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
```

Common code groups:

| Prefix | Area |
| --- | --- |
| `WF` | Workflow-level problems. |
| `NODE` | Node-level problems. |
| `EDGE` | Edge-level problems. |
| `CAP` | Capability resolution problems. |
| `MAP` | Input or output mapping problems. |
| `COND` | Condition problems. |
| `POLICY` | Policy problems. |
| `GRAPH` | Graph and loop problems. |
| `TARGET` | Target runtime support problems. |

Examples:

```text
NODE_DUPLICATE_ID
EDGE_UNKNOWN_NODE
CAP_NOT_FOUND
MAP_AMBIGUOUS_INPUT
COND_PARSE_ERROR
POLICY_JOIN_INVALID
GRAPH_DEAD_LOOP
TARGET_UNSUPPORTED_MAP_POLICY
```

## Structural Validation

Structural validation checks the graph and identifiers.

Rules:

- Workflow id and version must exist.
- Node ids must be unique.
- Edge ids must be unique.
- Edge endpoints must reference existing nodes.
- At least one entry node must exist after explicit markers or inference.
- Exit nodes should exist unless the Workflow intentionally waits externally.
- Loops must have a reachable exit path.

## Capability Validation

Each node capability must resolve to a static descriptor.

Compiler should distinguish:

- invalid capability kind
- invalid capability name
- missing capability
- missing version
- descriptor missing required schema or metadata

The Compiler resolves through registries, not live runtime instances.

## Mapping Validation

Input and output mappings should be checked where static information is
available.

Rules:

- Required node input must be provided by `InputPlan` or unambiguous inference.
- Ambiguous mapping inference should be an error.
- Referenced context paths should be syntactically valid.
- Static schema mismatches should be reported when detectable.
- Output binding targets should be valid runtime paths.

Edges do not automatically pass data. Data movement must be expressed through
input plans or output bindings.

## Condition Validation

Edge conditions are runtime expressions, but the Compiler should prepare them.

Rules:

- Condition syntax must parse.
- Unsafe operations must be rejected.
- Referenced paths should be extracted.
- Statically known node references should be checked.

The Compiler does not evaluate conditions against runtime data.

## Policy Validation

Policies should be checked for internal consistency.

Examples:

```text
JoinPolicy mode n requires count
JoinPolicy count must be positive
JoinPolicy count should not exceed incoming edge count
RetryPolicy max_attempts must be at least 1
TimeoutPolicy duration must parse
```

Routing policies may also require graph-specific checks, such as whether
`first_satisfied` has stable edge order.

## Loop Validation

Loops are valid Workflow structure.

Workflow IR should not store cycle metadata unless Scheduler or Kernel directly
needs it. The Compiler should analyze loops internally and reject dead loops.

A loop is invalid when it is statically clear that it cannot leave the cyclic
region.

Examples:

```text
invalid: every node in the loop only routes back into the loop
invalid: loop has no reachable exit node
valid: loop has a conditional edge that can leave the loop
valid: retry/revision loop can route to an exit or failure path
```

The first implementation can be conservative. If the Compiler cannot prove a loop
is safe, it should produce a diagnostic.

## Target Validation

Target validation checks whether the selected runtime supports the compiled
structure.

Examples:

- dynamic map is declared but unsupported
- resource policy is declared but resource accounting is unavailable
- nested Workflow capability is declared but child execution is unavailable

## Open Questions

- Which warnings should strict mode promote to errors?
- Should unresolved context paths be errors or warnings?
- How much schema compatibility can be checked statically?
- Should target validation live in Compiler or Runtime admission control?
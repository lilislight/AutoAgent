# Overall

This document defines the Operator module.

Operators are reusable computation capabilities. Workflow nodes reference
capabilities through stable ids, and NodeExecutor invokes the resolved
capability when executing selected nodes.

## Purpose

Operators keep implementation details out of Workflow graph structure while
still giving AutoAgent OS a unified execution boundary.

```text
Workflow Node -> CapabilityRef -> CapabilityDescriptor -> capability execution
```

A node should reference an Operator when the computation can be treated as one
managed execution step. Small helper logic should usually remain inside an
Operator instead of becoming separate Workflow nodes.

## Capability Kinds

Workflow nodes use `CapabilityRef` to reference executable capabilities.

```python
@dataclass(frozen=True)
class CapabilityRef:
    kind: Literal["operator", "system", "workflow"]
    name: str
    version: str | None = None
```

Capability kinds:

| Kind | Meaning |
| --- | --- |
| `operator` | Application or integration computation, such as LLM calls, functions, browsers, databases, shell commands, or MCP tools. |
| `system` | Runtime control capability that requires AutoAgent runtime participation. |
| `workflow` | Nested Workflow executed as a child execution unit. |

Most capabilities should be Operators.

## Operator Families

Operators can represent different kinds of computation behind the same
capability interface:

- function calls
- LLM calls
- tool calls
- browser automation
- database or search queries
- MCP tool wrappers
- internally complex agent-style procedures

Operator internals may be intelligent or multi-step, but externally an Operator
must behave like a managed capability with explicit input, output, error, usage,
and events.

## Registration

Most applications define Operators and Workflows together.

```python
from autoagent import operator


@operator(id="github.fetch_issue")
def fetch_issue(issue_id: int) -> dict:
    ...


app.register_operator(fetch_issue)
```

Registration should provide enough static information for Compiler to validate
node capability references before execution.

## Execution Boundary

NodeExecutor invokes resolved capabilities during node execution.

It should:

- build explicit input values from the compiled input plan
- enforce retry, timeout, and resource policy through execution guards
- call the Operator, system capability handler, or child Workflow execution
- collect output, error, events, artifacts, and resource usage
- return state changes for StateManager to apply

Operators should not own Workflow control flow, Scheduler decisions, Runtime Run
lifecycle, or session concurrency rules.

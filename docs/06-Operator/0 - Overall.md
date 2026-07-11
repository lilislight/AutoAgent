# Overall

This document defines the Operator module.

Operators are reusable computation capabilities. Workflow nodes may carry direct
Python functions or string capability references, and NodeExecutor invokes the
compiled executable capability.

## Purpose

Operators keep implementation details out of Workflow graph structure while
still giving AutoAgent OS a unified execution boundary.

```text
Workflow Node -> capability -> compiled executable binding -> capability execution
```

A node should reference an Operator when the computation can be treated as one
managed execution step. Small helper logic should usually remain inside an
Operator instead of becoming separate Workflow nodes.

## Capability References

Workflow nodes use direct functions when possible. They use string references or
when the capability must be resolved by name.

```python
Node(capability=create_repo)
Node(capability="wait_human_input")
```

System capabilities, nested Workflows, and externally registered Operators are
all resolved through the same capability reference mechanism when they are not
provided as direct callables.

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

Registration is only needed for referenced capabilities that are not already
stored on a node as direct callables.

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

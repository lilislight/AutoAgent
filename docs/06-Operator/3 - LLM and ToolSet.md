# LLM and ToolSet

This document defines how LLM Operators declare and use tools.

LLM calls are Operators. Tool calls are also Operators. A Workflow can expose
tools to an LLM node without hiding tool execution inside a black-box agent.

## LLM Operator

An LLM Operator should support the common inputs and outputs needed for AI
execution:

- messages or prompt fields
- model configuration
- structured output schema
- tool schemas
- tool choice constraints
- streaming support where available
- token and cost usage
- finish reason
- provider metadata

NodeExecutor treats the LLM like any other Operator: build input, invoke
capability, collect output, error, events, and resource usage.

## ToolSet

`ToolSet` defines the tools visible to an LLM node.

Tool availability should not be inferred from outgoing graph edges. Edges define
control flow. ToolSet defines LLM-visible tools.

```python
@dataclass(frozen=True)
class ToolRef:
    name: str
    capability: CapabilityRef
    description: str | None = None
    input_schema: "Schema | None" = None


@dataclass(frozen=True)
class ToolSet:
    tools: tuple[ToolRef, ...]
    parallel_tool_calls: bool = True
    max_tool_calls: int | None = None
```

An IR node for an LLM Operator may include a compiled ToolSet.

```python
@dataclass(frozen=True)
class IRNode:
    ...
    toolset: "CompiledToolSet | None" = None
```

## Compiler Responsibilities

Compiler should:

- resolve every tool reference through the Operator registry
- attach tool names, descriptions, and input schemas to the LLM IRNode
- verify that tool names are unique within the ToolSet
- check that edge conditions reference valid tool names when statically known
- emit diagnostics for ToolSet and graph mismatches

ToolSet and graph mismatch should usually be a warning, not an error.

Warnings:

- ToolSet contains a tool with no reachable tool node.
- A downstream tool node is reachable but not declared in the LLM ToolSet.
- LLM output includes tool calls but no edge routes those calls.

Errors:

- ToolSet references an unknown capability.
- Two tools in the same ToolSet use the same name.
- An edge condition references a tool name that does not exist in the ToolSet or
  registry.

## Runtime Output

LLM nodes that can choose tools should produce a structured decision output.

```python
@dataclass(frozen=True)
class ToolCall:
    name: str
    arguments: dict[str, Any]
    call_id: str | None = None


@dataclass(frozen=True)
class LLMDecisionOutput:
    tool_calls: tuple[ToolCall, ...] = ()
    final_answer: Any | None = None
```

Scheduler can route from the LLM node to tool nodes using edge conditions over
this output.

```text
nodes.reason.output.tool_calls contains "search_web"
nodes.reason.output.final_answer != null
```

If an LLM returns several tool calls and several tool edges are satisfied,
Scheduler may mark multiple tool nodes ready and dispatch them as a batch.

## Boundary

ToolSet is not a hidden dispatcher. It only tells the LLM what tools are
available and how to call them.

Actual tool execution should remain visible in the Workflow graph when the
developer wants ReAct-style execution to be observable and optimizable.

# Capability Descriptor

This document defines the static descriptor used by Compiler and NodeExecutor.

Compiler may resolve a node string capability to a `CapabilityDescriptor`. The
descriptor is static metadata. It is not one runtime execution.

## Model

```python
@dataclass(frozen=True)
class CapabilityDescriptor:
    name: str
    version: str | int | None = None

    input_schema: "Schema | None" = None
    output_schema: "Schema | None" = None

    operator_type: Literal[
        "function",
        "llm",
        "tool",
        "browser",
        "database",
        "search",
        "mcp",
        "agent",
        "workflow",
        "system",
    ] = "function"

    deterministic: bool | None = None
    side_effect: bool = False

    supports_streaming: bool = False
    supports_tools: bool = False
    supports_structured_output: bool = False
    supports_retry: bool = True
    supports_timeout: bool = True
    supports_cancellation: bool = False

    resource_profile: "ResourceProfile | None" = None
    security_profile: "SecurityProfile | None" = None
    metadata: dict[str, Any] = field(default_factory=dict)
```

## Purpose

The descriptor lets Compiler and runtime components reason about capabilities
without holding live execution objects.

Compiler uses descriptors to:

- validate capability references
- check input and output schemas
- compile input plans and output bindings
- validate toolsets
- validate policy compatibility

NodeExecutor uses descriptors to:

- invoke the correct capability handler
- enforce retry, timeout, and resource policy
- collect resource usage and artifacts
- emit structured observability events

## Resource Profile

`ResourceProfile` describes expected resource behavior.

Examples:

- token usage for LLM operators
- cost per call or cost estimate
- expected duration
- memory requirements
- external service quota category

Runtime policy uses actual resource usage when available. Profiles are for
planning, validation, and UI display.

## Security Profile

`SecurityProfile` describes permission and side-effect boundaries.

Examples:

- network access
- filesystem access
- browser access
- shell execution
- database write access
- external side effects

Security-sensitive capabilities should require explicit registration and policy
approval before execution.

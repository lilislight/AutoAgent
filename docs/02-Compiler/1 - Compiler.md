# Compiler

This document defines the Compiler API and related support types.

The Compiler is a deterministic static transformation:

```text
canonical Workflow -> CompileResult
```

## Python Model

```python
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CompilerConfig:
    strict: bool = True
    allow_inferred_entry: bool = True
    allow_inferred_mapping: bool = True
    include_source_map: bool = True
    target_ir_version: str = "0.1"


@dataclass(frozen=True)
class CompileResult:
    workflow_ir: "WorkflowIR | None"
    diagnostics: list["Diagnostic"] = field(default_factory=list)
    source_map: "SourceMap | None" = None

    @property
    def ok(self) -> bool:
        return self.workflow_ir is not None and not any(
            item.severity == "error" for item in self.diagnostics
        )


class WorkflowCompiler:
    def __init__(
        self,
        capability_resolver: "CapabilityResolver",
        config: CompilerConfig | None = None,
    ) -> None:
        self.capability_resolver = capability_resolver
        self.config = config or CompilerConfig()

    def compile(self, workflow: "Workflow") -> CompileResult:
        ...
```

## Configuration

`strict` controls whether risky warnings become errors.

`allow_inferred_entry` allows nodes with no incoming edges to become entry nodes
when no explicit entry exists.

`allow_inferred_mapping` allows input mapping inference only when unambiguous.

`include_source_map` controls whether diagnostic source locations are returned.

`target_ir_version` selects the Workflow IR schema version.

## Capability Resolution

The Compiler resolves `CapabilityRef` through registries instead of owning those
registries directly.

```python
class CapabilityResolver:
    def resolve(self, ref: "CapabilityRef") -> "CapabilityDescriptor":
        ...
```

Resolution may target:

- Operator registry
- system capability registry
- Workflow registry
- test or in-memory registries

The Compiler needs static descriptors, not live runtime Operator instances.

```python
@dataclass(frozen=True)
class CapabilityDescriptor:
    kind: str
    name: str
    version: str | None = None
    input_schema: "Schema | None" = None
    output_schema: "Schema | None" = None
    deterministic: bool | None = None
    side_effect: bool = False
    supports_timeout: bool = True
    supports_retry: bool = True
```

## Error Handling

Compilation problems should be returned as diagnostics.

Exceptions should be reserved for infrastructure failures, invalid compiler
configuration, or compiler implementation bugs.

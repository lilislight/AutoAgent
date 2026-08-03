from __future__ import annotations

from autoagent.ai import LLM_CALL_CAPABILITY
from autoagent.core.compiler import CompileResult, WorkflowCompiler, WorkflowPreview
from autoagent.core.operators import CapabilityRegistry, OperatorRegistry
from autoagent.core.workflow import Workflow


class ProjectCompiler:
    """Static authoring compiler that never requires runtime Provider secrets."""

    def __init__(self) -> None:
        capability_registry = CapabilityRegistry()
        capability_registry.register(LLM_CALL_CAPABILITY)
        operator_registry = OperatorRegistry(capability_registry)
        self.compiler = WorkflowCompiler(
            capability_registry=capability_registry,
            operator_registry=operator_registry,
            require_operator_bindings=False,
        )

    def compile(self, workflow: Workflow) -> CompileResult:
        return self.compiler.compile(workflow)

    def preview(self, workflow: Workflow) -> WorkflowPreview:
        """Compile once and expose all static Preview renderers."""

        return WorkflowPreview(self.compile(workflow))

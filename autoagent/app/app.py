from __future__ import annotations

from typing import Any
from uuid import uuid4

from autoagent.compiler import WorkflowCompiler, WorkflowIR
from autoagent.runtime import InMemoryRuntimeStore, Invocation, RuntimeStore, Session
from autoagent.workflow import Workflow


class WorkflowRegistryEntry:
    """Compiled Workflow stored by AutoAgentApp after successful validation."""

    def __init__(self, workflow: Workflow, workflow_ir: WorkflowIR) -> None:
        self.workflow = workflow
        self.workflow_ir = workflow_ir


class AutoAgentApp:
    """Application-level entry point for invoking workflows."""

    def __init__(self) -> None:
        self.namespace = "default"
        self.compiler = WorkflowCompiler()
        self.runtime_store: RuntimeStore = InMemoryRuntimeStore()
        self.workflow_registry: dict[str, WorkflowRegistryEntry] = {}

    def invoke(
        self,
        workflow: Workflow,
        input: dict[str, Any] | None = None,
        *,
        session_id: str | None = None,
        entry_node_id: str | None = None,
    ) -> Any:
        if workflow.id is None:
            raise ValueError("workflow.id is required before invoking a workflow.")

        registry_entry = self.workflow_registry.get(workflow.id)
        if registry_entry is None:
            compile_result = self.compiler.compile(workflow)
            if not compile_result.ok or compile_result.workflow_ir is None:
                diagnostics = [item.model_dump() for item in compile_result.diagnostics]
                raise ValueError(f"Workflow validation failed: {diagnostics}")
            workflow_ir = compile_result.workflow_ir
            registry_entry = WorkflowRegistryEntry(
                workflow=workflow,
                workflow_ir=workflow_ir,
            )
            self.workflow_registry[workflow.id] = registry_entry
        else:
            if registry_entry.workflow is not workflow:
                raise ValueError(f"Duplicate workflow id already registered: {workflow.id}")
            workflow_ir = registry_entry.workflow_ir

        selected_entry_node_id = entry_node_id or self._default_entry_node_id(workflow_ir)
        if selected_entry_node_id not in workflow_ir.entry_node_ids:
            raise ValueError(f"Invalid entry node id: {selected_entry_node_id}")

        session = self._get_or_create_session(
            workflow_id=workflow_ir.workflow_id,
            session_id=session_id,
        )
        invocation = Invocation(
            workflow_id=workflow_ir.workflow_id,
            workflow_version=workflow_ir.workflow_version,
            entry_node_id=selected_entry_node_id,
            input=input,
        )
        session.add_invocation(invocation)
        self.runtime_store.save_session(session)

        return invocation

    def _default_entry_node_id(self, workflow_ir: WorkflowIR) -> str:
        entry_count = len(workflow_ir.entry_node_ids)
        if entry_count == 0:
            raise ValueError("Workflow has no entry node.")
        if entry_count > 1:
            raise ValueError("entry_node_id is required for workflows with multiple entry nodes.")
        return workflow_ir.entry_node_ids[0]

    def _get_or_create_session(
        self,
        *,
        workflow_id: str,
        session_id: str | None,
    ) -> Session:
        return self.runtime_store.get_or_create_session(
            workflow_id=workflow_id,
            session_key=session_id or str(uuid4()),
            namespace=self.namespace,
        )

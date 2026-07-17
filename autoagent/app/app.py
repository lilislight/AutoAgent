from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from threading import RLock
from typing import Any, TypeVar
from uuid import UUID, uuid4

from autoagent.compiler import WorkflowCompiler, WorkflowIR, WorkflowVersionSnapshot
from autoagent.executor import NodeExecutor, WorkflowExecutor
from autoagent.operators import (
    Capability,
    CapabilityRegistry,
    Operator,
    OperatorRegistry,
    OperatorResolver,
    RecoveryMode,
)
from autoagent.operators.contract import ensure_callable_contract
from autoagent.runtime import (
    InMemoryRuntimeStore,
    Invocation,
    RuntimeStore,
    Session,
    SessionBusyError,
)
from autoagent.runtime.hooks import run_sync
from autoagent.workflow import Workflow


F = TypeVar("F", bound=Callable[..., Any])
_MISSING = object()


class WorkflowRegistryEntry:
    """Compiled Workflow stored by AutoAgentApp after successful validation."""

    def __init__(
        self,
        workflow: Workflow,
        workflow_ir: WorkflowIR,
        workflow_snapshot: WorkflowVersionSnapshot,
    ) -> None:
        self.workflow = workflow
        self.workflow_ir = workflow_ir
        self.workflow_snapshot = workflow_snapshot


class AutoAgentApp:
    """Application-level entry point for invoking workflows."""

    def __init__(
        self,
        *,
        namespace: str = "default",
        runtime_store: RuntimeStore | None = None,
    ) -> None:
        resolved_namespace = namespace.strip()
        if not resolved_namespace:
            raise ValueError("App namespace cannot be empty.")
        self.namespace = resolved_namespace
        self.capability_registry = CapabilityRegistry()
        self.operator_registry = OperatorRegistry(self.capability_registry)
        self.compiler = WorkflowCompiler(
            capability_registry=self.capability_registry,
            operator_registry=self.operator_registry,
        )
        self.runtime_store: RuntimeStore = runtime_store or InMemoryRuntimeStore()
        node_executor = NodeExecutor(
            operator_resolver=OperatorResolver(
                self.capability_registry,
                self.operator_registry,
            )
        )
        self.workflow_executor = WorkflowExecutor(
            node_executor=node_executor,
            runtime_store=self.runtime_store,
        )
        self.workflow_registry: dict[str, WorkflowRegistryEntry] = {}
        self._workflow_registry_lock = RLock()
        # Process-local liveness is deliberately not persisted. If an id is in
        # this set, its worker/control loop is still owned by this App and a
        # concurrent request must receive SessionBusyError rather than treating
        # the row as crash residue.
        self._live_invocation_ids: set[UUID] = set()
        self._live_invocation_lock = RLock()

    def close(self) -> None:
        """Release Store resources from synchronous application code."""

        run_sync(
            self.aclose(),
            api_name="close",
            async_api_name="aclose",
        )

    async def aclose(self) -> None:
        """Release database pools and other RuntimeStore resources."""

        await self.runtime_store.aclose()

    def register_capability(
        self,
        capability_id: str,
        *,
        description: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Capability:
        """Register one Capability identity in this App's isolated registry.

        This method does not accept or create a schema. The first associated
        Operator establishes the Capability contract from its Python callable;
        a CapabilityRef compiles only after that implementation is registered.
        """

        return self.capability_registry.register(
            Capability(
                id=capability_id,
                description=description,
                metadata=metadata or {},
            )
        )

    def register_operator(
        self,
        handler: Callable[..., Any],
        *,
        operator_id: str | None = None,
        capability_id: str | None = None,
        version: str | int = 1,
        recovery_mode: RecoveryMode = "never",
        priority: int = 0,
        enabled: bool = True,
        metadata: dict[str, Any] | None = None,
        default: bool = False,
    ) -> Operator:
        """Register one concrete callable implementation in this App.

        capability_id may be omitted for a standalone Operator selected only by
        OperatorRef. The contract is always inferred from the callable. When a
        Capability already has an implementation-derived contract, registry
        rejects structural mismatches immediately.
        """

        resolved_operator_id = operator_id or getattr(handler, "__name__", None)
        if not resolved_operator_id:
            raise ValueError("operator_id is required for unnamed callable objects.")
        if self.operator_registry.contains(resolved_operator_id):
            raise ValueError(f"Operator already registered: {resolved_operator_id}")
        if (
            capability_id is not None
            and not self.capability_registry.contains(capability_id)
        ):
            raise ValueError(
                f"Operator references an unknown capability: {capability_id}"
            )
        ensure_callable_contract(handler)
        return self.operator_registry.register(
            Operator(
                id=resolved_operator_id,
                handler=handler,
                capability_id=capability_id,
                version=version,
                recovery_mode=recovery_mode,
                priority=priority,
                enabled=enabled,
                metadata=metadata,
            ),
            default=default,
        )

    def capability(
        self,
        capability_id: str | None = None,
        *,
        operator_id: str | None = None,
        description: str | None = None,
        version: str | int = 1,
        recovery_mode: RecoveryMode = "never",
        priority: int = 0,
        metadata: dict[str, Any] | None = None,
    ) -> Callable[[F], F]:
        """Decorate a callable as a Capability's default implementation.

        The original callable is returned unchanged. Registration creates two
        separate records: a Capability contract and its default Operator.
        """

        def decorator(handler: F) -> F:
            resolved_capability_id = capability_id or getattr(handler, "__name__", None)
            if not resolved_capability_id:
                raise ValueError("capability_id is required for unnamed callables.")
            resolved_operator_id = operator_id or getattr(handler, "__name__", None)
            if not resolved_operator_id:
                raise ValueError("operator_id is required for unnamed callables.")
            if self.capability_registry.contains(resolved_capability_id):
                raise ValueError(
                    f"Capability already registered: {resolved_capability_id}"
                )
            if self.operator_registry.contains(resolved_operator_id):
                raise ValueError(f"Operator already registered: {resolved_operator_id}")
            ensure_callable_contract(handler)
            registered_operator = Operator(
                id=resolved_operator_id,
                handler=handler,
                capability_id=resolved_capability_id,
                version=version,
                recovery_mode=recovery_mode,
                priority=priority,
                metadata=metadata,
            )
            self.register_capability(
                resolved_capability_id,
                description=description,
                metadata=metadata,
            )
            self.operator_registry.register(
                registered_operator,
                default=True,
            )
            return handler

        return decorator

    def operator(
        self,
        operator_id: str | None = None,
        *,
        capability: str | None = None,
        version: str | int = 1,
        recovery_mode: RecoveryMode = "never",
        priority: int = 0,
        enabled: bool = True,
        metadata: dict[str, Any] | None = None,
    ) -> Callable[[F], F]:
        """Decorate and register a concrete Operator in this App."""

        def decorator(handler: F) -> F:
            self.register_operator(
                handler,
                operator_id=operator_id,
                capability_id=capability,
                version=version,
                recovery_mode=recovery_mode,
                priority=priority,
                enabled=enabled,
                metadata=metadata,
            )
            return handler

        return decorator

    def preview(
        self,
        workflow: Workflow,
        path: str | Path | None = None,
    ) -> Path:
        """Write Mermaid using this App's registered Operator environment."""

        return workflow.preview(path, compiler=self.compiler)

    def invoke(
        self,
        workflow: Workflow,
        input: dict[str, Any] | None = None,
        *,
        session_id: str | None = None,
        entry_node_id: str | None = None,
    ) -> Invocation:
        """Invoke a Workflow from synchronous code.

        Async applications must call await ainvoke(); synchronously blocking an
        active event loop is intentionally rejected.
        """

        return run_sync(
            self.ainvoke(
                workflow,
                input=input,
                session_id=session_id,
                entry_node_id=entry_node_id,
            ),
            api_name="invoke",
            async_api_name="ainvoke",
        )

    async def ainvoke(
        self,
        workflow: Workflow,
        input: dict[str, Any] | None = None,
        *,
        session_id: str | None = None,
        entry_node_id: str | None = None,
    ) -> Invocation:
        """Invoke a Workflow through the native async execution pipeline.

        Durable crash recovery is lazy because a restarted process cannot
        validate a historical invocation until this Workflow and its Operators
        have been registered again. After loading the Session, this method:

        1. rejects an invocation still owned by this App as concurrent work;
        2. automatically replays compatible persisted ``created``/``running``
           work and returns that historical invocation, without mixing new input;
        3. marks incompatible work ``interrupted`` and admits this request as a
           fresh invocation; and
        4. leaves ``waiting`` work reserved for :meth:`aresume`.

        There is intentionally no manual crash-recovery option in V1.
        """

        workflow_ir = self._get_or_compile_workflow(workflow)
        workflow_snapshot = self._refresh_workflow_snapshot(workflow.id)
        await self.runtime_store.asave_workflow_snapshot(
            self.namespace,
            workflow_snapshot,
        )

        selected_entry_node_id = entry_node_id or self._default_entry_node_id(workflow_ir)
        if selected_entry_node_id not in workflow_ir.entry_node_ids:
            raise ValueError(f"Invalid entry node id: {selected_entry_node_id}")

        session = await self._get_or_create_session(
            workflow_id=workflow_ir.workflow_id,
            session_id=session_id,
        )
        current = session.get_current_invocation()
        if current is not None and current.state in {"created", "running"}:
            if not self._claim_invocation_live(current.id):
                raise SessionBusyError(session, current)
            try:
                recovered = await self.workflow_executor.arecover(
                    workflow_ir=workflow_ir,
                    workflow_snapshot=workflow_snapshot,
                    session=session,
                    invocation=current,
                )
            finally:
                self._set_invocation_live(current.id, False)
            if recovered.state != "interrupted":
                # Recovery takes precedence over the new request. The caller can
                # submit its new input after this historical Invocation reaches a
                # stable state; silently mixing two inputs would violate Session
                # admission semantics.
                return recovered
            session = await self.runtime_store.aload_session(session.id) or session
        invocation = Invocation(
            workflow_id=workflow_ir.workflow_id,
            workflow_version=workflow_ir.workflow_version,
            workflow_definition_hash=workflow_ir.definition_hash,
            workflow_operator_manifest_hash=workflow_snapshot.operator_manifest_hash,
            entry_node_id=selected_entry_node_id,
            input=input,
        )
        self._set_invocation_live(invocation.id, True)
        try:
            session = await self.runtime_store.aadmit_invocation(session.id, invocation)
            return await self.workflow_executor.ainvoke(
                workflow_ir=workflow_ir,
                session=session,
                invocation=invocation,
            )
        finally:
            self._set_invocation_live(invocation.id, False)

    def resume(
        self,
        workflow: Workflow,
        *,
        session_id: str,
        wait_key: str,
        output: Any = _MISSING,
    ) -> Invocation:
        """Resume one wait from synchronous code."""

        return run_sync(
            self.aresume(
                workflow,
                session_id=session_id,
                wait_key=wait_key,
                output=output,
            ),
            api_name="resume",
            async_api_name="aresume",
        )

    async def aresume(
        self,
        workflow: Workflow,
        *,
        session_id: str,
        wait_key: str,
        output: Any = _MISSING,
    ) -> Invocation:
        """Atomically claim and resume one persisted external wait.

        The Store validates both Workflow structure and Operator environment
        before changing the invocation from ``waiting`` to ``running``. A
        consumed wait key cannot be resumed again, including after process
        restart when a durable RuntimeStore is used.
        """

        workflow_ir = self._get_or_compile_workflow(workflow)
        workflow_snapshot = self._refresh_workflow_snapshot(workflow.id)
        await self.runtime_store.asave_workflow_snapshot(
            self.namespace,
            workflow_snapshot,
        )
        session = await self.runtime_store.aclaim_waiting_session(
            namespace=self.namespace,
            workflow_id=workflow_ir.workflow_id,
            session_key=session_id,
            wait_key=wait_key,
            workflow_definition_hash=workflow_ir.definition_hash,
            workflow_operator_manifest_hash=workflow_snapshot.operator_manifest_hash,
        )
        invocation = session.get_current_invocation()
        if invocation is None:
            raise ValueError("Session does not have a current Invocation.")

        kwargs: dict[str, Any] = {
            "workflow_ir": workflow_ir,
            "session": session,
            "invocation": invocation,
            "wait_key": wait_key,
        }
        if output is not _MISSING:
            kwargs["output"] = output
        self._set_invocation_live(invocation.id, True)
        try:
            return await self.workflow_executor.aresume(**kwargs)
        finally:
            self._set_invocation_live(invocation.id, False)

    def _get_or_compile_workflow(self, workflow: Workflow) -> WorkflowIR:
        with self._workflow_registry_lock:
            registry_entry = self.workflow_registry.get(workflow.id)
            if registry_entry is None:
                compile_result = self.compiler.compile(workflow)
                if not compile_result.ok or compile_result.workflow_ir is None:
                    diagnostics = [
                        item.model_dump() for item in compile_result.diagnostics
                    ]
                    raise ValueError(f"Workflow validation failed: {diagnostics}")
                if compile_result.workflow_snapshot is None:
                    raise RuntimeError("Compiler omitted WorkflowVersionSnapshot.")
                registry_entry = WorkflowRegistryEntry(
                    workflow=workflow,
                    workflow_ir=compile_result.workflow_ir,
                    workflow_snapshot=compile_result.workflow_snapshot,
                )
                self.workflow_registry[workflow.id] = registry_entry
            elif registry_entry.workflow is not workflow:
                raise ValueError(
                    f"Duplicate workflow id already registered: {workflow.id}"
                )
            return registry_entry.workflow_ir

    def _default_entry_node_id(self, workflow_ir: WorkflowIR) -> str:
        entry_count = len(workflow_ir.entry_node_ids)
        if entry_count == 0:
            raise ValueError("Workflow has no entry node.")
        if entry_count > 1:
            raise ValueError("entry_node_id is required for workflows with multiple entry nodes.")
        return workflow_ir.entry_node_ids[0]

    def _refresh_workflow_snapshot(
        self,
        workflow_id: str,
    ) -> WorkflowVersionSnapshot:
        """Refresh late-bound Operator manifests without recompiling graph IR."""

        with self._workflow_registry_lock:
            entry = self.workflow_registry[workflow_id]
            snapshot = WorkflowVersionSnapshot.from_workflow_ir(
                entry.workflow_ir,
                operator_registry=self.operator_registry,
            )
            entry.workflow_snapshot = snapshot
            return snapshot

    async def _get_or_create_session(
        self,
        *,
        workflow_id: str,
        session_id: str | None,
    ) -> Session:
        return await self.runtime_store.aget_or_create_session(
            workflow_id=workflow_id,
            session_key=session_id or str(uuid4()),
            namespace=self.namespace,
        )

    def _claim_invocation_live(self, invocation_id: UUID) -> bool:
        """Atomically claim process-local ownership for execution or recovery."""

        with self._live_invocation_lock:
            if invocation_id in self._live_invocation_ids:
                return False
            self._live_invocation_ids.add(invocation_id)
            return True

    def _set_invocation_live(self, invocation_id: UUID, live: bool) -> None:
        with self._live_invocation_lock:
            if live:
                self._live_invocation_ids.add(invocation_id)
            else:
                self._live_invocation_ids.discard(invocation_id)

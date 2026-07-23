from __future__ import annotations

from collections.abc import Callable, Iterable
from pathlib import Path
from threading import RLock
from typing import Any, TypeVar
from uuid import UUID, uuid4

from pydantic import BaseModel

from autoagent.core.compiler import WorkflowCompiler, WorkflowIR, WorkflowVersionSnapshot
from autoagent.core.executor import NodeExecutor, WorkflowExecutor
from autoagent.core.operators import (
    Capability,
    CapabilityRegistry,
    Operator,
    OperatorRegistry,
    OperatorResolver,
)
from autoagent.core.operators.contract import ensure_callable_contract
from autoagent.core.runtime import (
    InMemoryRuntimeStore,
    Invocation,
    JsonRuntimeSerializer,
    RuntimeCodec,
    RuntimeStore,
    Session,
    SessionBusyError,
)
from autoagent.core.runtime.hooks import RuntimeEventLoop
from autoagent.core.workflow import Workflow


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


class _PreparedInvocation:
    """Internal admission result shared by blocking invoke and background submit."""

    def __init__(
        self,
        *,
        workflow_ir: WorkflowIR,
        workflow_snapshot: WorkflowVersionSnapshot,
        session: Session,
        invocation: Invocation,
        recover_existing: bool,
    ) -> None:
        self.workflow_ir = workflow_ir
        self.workflow_snapshot = workflow_snapshot
        self.session = session
        self.invocation = invocation
        self.recover_existing = recover_existing


class AutoAgentApp:
    """Application-level entry point for invoking workflows."""

    def __init__(
        self,
        *,
        namespace: str = "default",
        runtime_store: RuntimeStore | None = None,
        runtime_serializer: JsonRuntimeSerializer | None = None,
        runtime_codecs: Iterable[RuntimeCodec] = (),
        runtime_models: Iterable[type[BaseModel]] = (),
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
        if runtime_store is None:
            runtime_store = InMemoryRuntimeStore(serializer=runtime_serializer)
        elif (
            runtime_serializer is not None
            and runtime_store.serializer is not runtime_serializer
        ):
            raise ValueError(
                "runtime_serializer must be the serializer owned by runtime_store."
            )
        self.runtime_store = runtime_store
        self.runtime_serializer = runtime_store.serializer
        for codec in runtime_codecs:
            self.register_runtime_codec(codec)
        for model_type in runtime_models:
            self.register_runtime_model(model_type)
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
        self._runtime_loop = RuntimeEventLoop(
            name=f"autoagent-runtime-{self.namespace}"
        )
        self._closed = False

    def register_runtime_codec(self, codec: RuntimeCodec) -> None:
        """Register one trusted custom persistence codec before loading records."""

        self.runtime_serializer.register_codec(codec)

    def start(self) -> None:
        """Initialize App runtime resources from synchronous code."""

        if self._closed:
            raise RuntimeError("AutoAgentApp is closed.")
        self._runtime_loop.run(self.runtime_store.ainitialize())

    async def astart(self) -> None:
        """Initialize App runtime resources from asynchronous code."""

        if not self._runtime_loop.is_current():
            await self._runtime_loop.arun(self.astart())
            return
        if self._closed:
            raise RuntimeError("AutoAgentApp is closed.")
        await self.runtime_store.ainitialize()

    def register_runtime_model(
        self,
        model_type: type[BaseModel],
        *,
        type_id: str | None = None,
    ) -> str:
        """Register a trusted Pydantic type used by durable runtime values."""

        return self.runtime_serializer.register_pydantic_model(
            model_type,
            type_id=type_id,
        )

    def close(self) -> None:
        """Release Store resources from synchronous application code."""

        if self._closed:
            return
        self._runtime_loop.run(self._aclose_on_runtime_loop())
        self._runtime_loop.stop()
        self._closed = True

    async def aclose(self) -> None:
        """Release database pools and other RuntimeStore resources."""

        if self._closed:
            return
        if self._runtime_loop.is_current():
            await self._aclose_on_runtime_loop()
            self._closed = True
            return
        await self._runtime_loop.arun(self._aclose_on_runtime_loop())
        self._runtime_loop.stop()
        self._closed = True

    async def _aclose_on_runtime_loop(self) -> None:
        await self.runtime_store.aclose()

    def register_workflow(self, workflow: Workflow) -> WorkflowRegistryEntry:
        """Compile and cache a Workflow without invoking it.

        AutoAgentServer uses this registry to expose execution APIs by workflow
        id. Re-registering the same object is idempotent; reusing an id for a
        different Workflow object is rejected because runtime history is keyed
        by workflow id and compiled definition hash.
        """

        workflow_ir = self._get_or_compile_workflow(workflow)
        workflow_snapshot = self._refresh_workflow_snapshot(workflow.id)
        return self.workflow_registry[workflow.id]

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

        Sync and async entrypoints share the same App-owned Runtime loop.
        """

        self._ensure_open()
        return self._runtime_loop.run(
            self.ainvoke(
                workflow,
                input=input,
                session_id=session_id,
                entry_node_id=entry_node_id,
            ),
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

        if not self._runtime_loop.is_current():
            return await self._runtime_loop.arun(
                self.ainvoke(
                    workflow,
                    input=input,
                    session_id=session_id,
                    entry_node_id=entry_node_id,
                )
            )
        self._ensure_open()

        prepared = await self._prepare_invocation(
            workflow,
            input=input,
            session_id=session_id,
            entry_node_id=entry_node_id,
        )
        try:
            if prepared.recover_existing:
                recovered = await self.workflow_executor.arecover(
                    workflow_ir=prepared.workflow_ir,
                    workflow_snapshot=prepared.workflow_snapshot,
                    session=prepared.session,
                    invocation=prepared.invocation,
                )
                return recovered
            return await self.workflow_executor.ainvoke(
                workflow_ir=prepared.workflow_ir,
                session=prepared.session,
                invocation=prepared.invocation,
            )
        finally:
            self._set_invocation_live(prepared.invocation.id, False)

    async def _aadmit_invocation(
        self,
        workflow: Workflow,
        input: dict[str, Any] | None = None,
        *,
        session_id: str | None = None,
        entry_node_id: str | None = None,
    ) -> _PreparedInvocation:
        """Durably admit work without introducing background-task semantics."""

        if not self._runtime_loop.is_current():
            return await self._runtime_loop.arun(
                self._aadmit_invocation(
                    workflow,
                    input=input,
                    session_id=session_id,
                    entry_node_id=entry_node_id,
                )
            )
        self._ensure_open()
        return await self._prepare_invocation(
            workflow,
            input=input,
            session_id=session_id,
            entry_node_id=entry_node_id,
        )

    async def _aexecute_admitted(
        self,
        prepared: _PreparedInvocation,
        *,
        input: dict[str, Any] | None,
    ) -> Invocation:
        """Execute work already admitted by the Server."""

        if not self._runtime_loop.is_current():
            return await self._runtime_loop.arun(
                self._aexecute_admitted(prepared, input=input)
            )
        try:
            if prepared.recover_existing:
                recovered = await self.workflow_executor.arecover(
                    workflow_ir=prepared.workflow_ir,
                    workflow_snapshot=prepared.workflow_snapshot,
                    session=prepared.session,
                    invocation=prepared.invocation,
                )
                return recovered
            return await self.workflow_executor.ainvoke(
                workflow_ir=prepared.workflow_ir,
                session=prepared.session,
                invocation=prepared.invocation,
            )
        finally:
            self._set_invocation_live(prepared.invocation.id, False)

    def resume(
        self,
        workflow: Workflow,
        *,
        session_id: str,
        wait_key: str,
        output: Any = _MISSING,
    ) -> Invocation:
        """Resume one wait from synchronous code."""

        self._ensure_open()
        return self._runtime_loop.run(
            self.aresume(
                workflow,
                session_id=session_id,
                wait_key=wait_key,
                output=output,
            ),
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

        if not self._runtime_loop.is_current():
            return await self._runtime_loop.arun(
                self.aresume(
                    workflow,
                    session_id=session_id,
                    wait_key=wait_key,
                    output=output,
                )
            )
        self._ensure_open()

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
                workflow_ir, workflow_snapshot = self._compile_workflow_locked(workflow)
                registry_entry = WorkflowRegistryEntry(
                    workflow=workflow,
                    workflow_ir=workflow_ir,
                    workflow_snapshot=workflow_snapshot,
                )
                self.workflow_registry[workflow.id] = registry_entry
            elif registry_entry.workflow is not workflow:
                raise ValueError(
                    f"Duplicate workflow id already registered: {workflow.id}"
                )
            else:
                workflow_ir, workflow_snapshot = self._compile_workflow_locked(workflow)
                if workflow_ir.definition_hash != registry_entry.workflow_ir.definition_hash:
                    raise ValueError(
                        "Workflow source changed after it was compiled by this App. "
                        "Create a new Workflow id/version or wait for optimizer "
                        "hot-patch support instead of mutating an already compiled "
                        f"Workflow: {workflow.id}"
                    )
            return registry_entry.workflow_ir

    def _compile_workflow_locked(
        self,
        workflow: Workflow,
    ) -> tuple[WorkflowIR, WorkflowVersionSnapshot]:
        """Compile Workflow source while the App registry lock is held.

        AutoAgentApp caches compiled WorkflowIR for invocation speed. The source
        Workflow object is still mutable Python state, so existing registry
        entries are recompiled only to detect unsupported user mutation. Future
        optimizer hot-patch support should promote a new version explicitly
        instead of silently replacing active source definitions.
        """

        compile_result = self.compiler.compile(workflow)
        if not compile_result.ok or compile_result.workflow_ir is None:
            diagnostics = [
                item.model_dump() for item in compile_result.diagnostics
            ]
            raise ValueError(f"Workflow validation failed: {diagnostics}")
        if compile_result.workflow_snapshot is None:
            raise RuntimeError("Compiler omitted WorkflowVersionSnapshot.")
        return compile_result.workflow_ir, compile_result.workflow_snapshot

    async def _prepare_invocation(
        self,
        workflow: Workflow,
        input: dict[str, Any] | None,
        *,
        session_id: str | None,
        entry_node_id: str | None,
    ) -> _PreparedInvocation:
        """Compile, persist snapshot, check session admission, and claim liveness."""

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
            # Recovery always starts from the durable Genesis/snapshot + event
            # prefix, never from a mutable materialized database cache.
            session, current = await self.runtime_store.arebuild_execution(
                current.id
            )
            if not self._claim_invocation_live(current.id):
                raise SessionBusyError(session, current)
            return _PreparedInvocation(
                workflow_ir=workflow_ir,
                workflow_snapshot=workflow_snapshot,
                session=session,
                invocation=current,
                recover_existing=True,
            )
        return await self._prepare_fresh_invocation(
            workflow_ir=workflow_ir,
            workflow_snapshot=workflow_snapshot,
            session=session,
            entry_node_id=selected_entry_node_id,
            input=input,
        )

    async def _prepare_fresh_invocation(
        self,
        *,
        workflow_ir: WorkflowIR,
        workflow_snapshot: WorkflowVersionSnapshot,
        session: Session,
        entry_node_id: str,
        input: dict[str, Any] | None,
    ) -> _PreparedInvocation:
        invocation = Invocation(
            workflow_id=workflow_ir.workflow_id,
            workflow_version=workflow_ir.workflow_version,
            workflow_definition_hash=workflow_ir.definition_hash,
            workflow_operator_manifest_hash=workflow_snapshot.operator_manifest_hash,
            entry_node_id=entry_node_id,
            input=input,
        )
        self._set_invocation_live(invocation.id, True)
        try:
            session = await self.runtime_store.aadmit_invocation(session.id, invocation)
        except Exception:
            self._set_invocation_live(invocation.id, False)
            raise
        return _PreparedInvocation(
            workflow_ir=workflow_ir,
            workflow_snapshot=workflow_snapshot,
            session=session,
            invocation=invocation,
            recover_existing=False,
        )

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

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("AutoAgentApp is closed.")

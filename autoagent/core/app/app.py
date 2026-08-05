from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from copy import deepcopy
import logging
from pathlib import Path
from threading import RLock
from typing import Any, TypeVar, get_args, get_origin
from uuid import UUID, uuid4

from pydantic import BaseModel

from autoagent.core.app.settings import AutoAgentSettings
from autoagent.core.compiler import (
    WorkflowCompiler,
    WorkflowIR,
    WorkflowVersionSnapshot,
    workflow_revision_id,
)
from autoagent.core.executor import NodeExecutor, WorkflowExecutor
from autoagent.core.operators import (
    Capability,
    CapabilityRegistry,
    Operator,
    OperatorRegistry,
    OperatorResolver,
)
from autoagent.core.operators.contract import OperatorContract, ensure_callable_contract
from autoagent.core.runtime import (
    Invocation,
    JsonRuntimeSerializer,
    RuntimeCodec,
    RuntimeEventMode,
    RuntimeStore,
    Session,
    SessionContext,
    SessionBusyError,
)
from autoagent.core.runtime.hooks import RuntimeEventLoop
from autoagent.core.workflow import Workflow


F = TypeVar("F", bound=Callable[..., Any])
_MISSING = object()
logger = logging.getLogger(__name__)


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
        session: Session,
        invocation: Invocation,
    ) -> None:
        self.workflow_ir = workflow_ir
        self.session = session
        self.invocation = invocation


class _PreparedRerun:
    """Internal Rerun admission paired with its source boundary."""

    def __init__(
        self,
        *,
        prepared: _PreparedInvocation,
        source_invocation_id: UUID,
        source_workflow_revision_id: str,
    ) -> None:
        self.prepared = prepared
        self.source_invocation_id = source_invocation_id
        self.source_workflow_revision_id = source_workflow_revision_id


class AutoAgentApp:
    """Application-level entry point for invoking workflows."""

    def __init__(
        self,
        *,
        settings: AutoAgentSettings | None = None,
        runtime_store: RuntimeStore | None = None,
        runtime_serializer: JsonRuntimeSerializer | None = None,
        runtime_codecs: Iterable[RuntimeCodec] = (),
        runtime_models: Iterable[type[BaseModel]] = (),
    ) -> None:
        resolved_settings = settings or AutoAgentSettings.from_env()
        self.settings = resolved_settings
        self.capability_registry = CapabilityRegistry()
        self.operator_registry = OperatorRegistry(self.capability_registry)
        self.compiler = WorkflowCompiler(
            capability_registry=self.capability_registry,
            operator_registry=self.operator_registry,
        )
        if runtime_store is None:
            runtime_store = resolved_settings.runtime_store(
                serializer=runtime_serializer,
            )
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
            max_thread_workers=resolved_settings.executor_max_thread_workers,
            max_parallel_units=resolved_settings.executor_max_parallel_units,
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
        self._workflow_object_revision_ids: dict[int, str] = {}
        self._workflow_registry_lock = RLock()
        # Process-local liveness is deliberately not persisted. If an id is in
        # this set, its worker/control loop is still owned by this App and a
        # concurrent request must receive SessionBusyError rather than treating
        # the row as crash residue.
        self._live_invocation_ids: set[UUID] = set()
        self._live_invocation_lock = RLock()
        self._runtime_loop = RuntimeEventLoop(
            name="autoagent-runtime"
        )
        self._started = False
        self._start_lock: asyncio.Lock | None = None
        self._closed = False

    def register_runtime_codec(self, codec: RuntimeCodec) -> None:
        """Register one trusted custom persistence codec before loading records."""

        self.runtime_serializer.register_codec(codec)

    def start(self) -> None:
        """Initialize resources and recover registered Workflows."""

        if self._closed:
            raise RuntimeError("AutoAgentApp is closed.")
        self._runtime_loop.run(self.astart())

    async def astart(self) -> None:
        """Initialize resources and recover registered Workflows.

        Every Workflow that may have unfinished durable work must be registered
        before startup. Invocation and resume APIs never initialize or recover
        the App implicitly.
        """

        if not self._runtime_loop.is_current():
            await self._runtime_loop.arun(self.astart())
            return
        if self._closed:
            raise RuntimeError("AutoAgentApp is closed.")
        if self._started:
            return
        if self._start_lock is None:
            self._start_lock = asyncio.Lock()
        async with self._start_lock:
            if self._started:
                return
            await self.runtime_store.ainitialize()
            await self._arecover_registered_workflows()
            self._started = True

    async def _arecover_registered_workflows(self) -> None:
        with self._workflow_registry_lock:
            entries = tuple(self.workflow_registry.values())
        for entry in entries:
            await self.runtime_store.asave_workflow_snapshot(
                entry.workflow_snapshot,
            )
        recoverable_ids = (
            await self.runtime_store.alist_recoverable_invocation_ids(
                workflow_revision_ids=tuple(
                    workflow_revision_id(
                        entry.workflow_snapshot.workflow_id,
                        entry.workflow_snapshot.definition_hash,
                    )
                    for entry in entries
                ),
            )
        )
        entries_by_revision = {
            workflow_revision_id(
                entry.workflow_snapshot.workflow_id,
                entry.workflow_snapshot.definition_hash,
            ): entry
            for entry in entries
        }
        recoveries: list[
            tuple[WorkflowRegistryEntry, Session, Invocation]
        ] = []
        for invocation_id in recoverable_ids:
            session, invocation = await self.runtime_store.arebuild_execution(
                invocation_id
            )
            entry = entries_by_revision.get(invocation.workflow_revision_id)
            if entry is None:
                continue
            if invocation.state == "waiting":
                continue
            if invocation.state not in {"created", "running"}:
                continue
            if not self._claim_invocation_live(invocation.id):
                raise SessionBusyError(session, invocation)
            recoveries.append((entry, session, invocation))

        async def recover_one(
            entry: WorkflowRegistryEntry,
            session: Session,
            invocation: Invocation,
        ) -> None:
            try:
                try:
                    await self.workflow_executor.arecover(
                        workflow_ir=entry.workflow_ir,
                        workflow_snapshot=entry.workflow_snapshot,
                        session=session,
                        invocation=invocation,
                    )
                except Exception as exc:
                    await self.workflow_executor.afail_infrastructure(
                        session=session,
                        invocation=invocation,
                        error=exc,
                    )
                    logger.exception(
                        "Startup recovery failed: invocation_id=%s",
                        invocation.id,
                    )
            finally:
                self._set_invocation_live(invocation.id, False)

        await asyncio.gather(
            *(
                recover_one(entry, session, invocation)
                for entry, session, invocation in recoveries
            )
        )

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
        try:
            self._runtime_loop.run(self._aclose_on_runtime_loop())
        finally:
            self._runtime_loop.stop(
                timeout_s=self.settings.shutdown_grace_timeout_ms / 1000
            )
            self._closed = True

    async def aclose(self) -> None:
        """Release database pools and other RuntimeStore resources."""

        if self._closed:
            return
        if self._runtime_loop.is_current():
            try:
                await self._aclose_on_runtime_loop()
            finally:
                self._closed = True
                # Stop on the next loop turn so this close coroutine can finish
                # and publish its completion before _run cancels leftovers.
                self._runtime_loop.call_soon(self._runtime_loop.stop)
            return
        try:
            await self._runtime_loop.arun(self._aclose_on_runtime_loop())
        finally:
            self._runtime_loop.stop(
                timeout_s=self.settings.shutdown_grace_timeout_ms / 1000
            )
            self._closed = True

    async def _aclose_on_runtime_loop(self) -> None:
        try:
            await self.runtime_store.aclose()
        finally:
            self.workflow_executor.node_executor.close()

    def register_workflow(self, workflow: Workflow) -> WorkflowRegistryEntry:
        """Compile and cache a Workflow without invoking it.

        AutoAgentServer uses this registry to expose execution APIs by Workflow
        revision. Re-registering the same object is idempotent. Distinct
        revisions may share a human-readable ``workflow.id``.
        """

        workflow_ir = self._get_or_compile_workflow(workflow)
        workflow_snapshot = self._registered_workflow_snapshot(workflow)
        revision_id = workflow_revision_id(
            workflow_snapshot.workflow_id,
            workflow_snapshot.definition_hash,
        )
        if self._started:
            self.runtime_store.save_workflow_snapshot(
                workflow_snapshot,
            )
        return self.workflow_registry[revision_id]

    def register_capability(
        self,
        capability_id: str,
        *,
        contract: OperatorContract | None = None,
        description: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Capability:
        """Register one Capability identity in this App's isolated registry.

        Prefer an explicit contract when the Capability is a reusable public
        protocol. When omitted, the first associated Operator establishes the
        contract for simple application-local capabilities.
        """

        return self.capability_registry.register(
            Capability(
                id=capability_id,
                contract=contract,
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
                contract=registered_operator.contract,
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
        event_mode: RuntimeEventMode = "standard",
    ) -> Invocation:
        """Invoke a Workflow from synchronous code.

        Sync and async entrypoints share the same App-owned Runtime loop.
        """

        self._ensure_open()
        self._ensure_started()
        return self._runtime_loop.run(
            self.ainvoke(
                workflow,
                input=input,
                session_id=session_id,
                entry_node_id=entry_node_id,
                event_mode=event_mode,
            ),
        )

    async def ainvoke(
        self,
        workflow: Workflow,
        input: dict[str, Any] | None = None,
        *,
        session_id: str | None = None,
        entry_node_id: str | None = None,
        event_mode: RuntimeEventMode = "standard",
    ) -> Invocation:
        """Invoke a Workflow through the native async execution pipeline."""

        self._ensure_open()
        self._ensure_started()
        if not self._runtime_loop.is_current():
            return await self._runtime_loop.arun(
                self.ainvoke(
                    workflow,
                    input=input,
                    session_id=session_id,
                    entry_node_id=entry_node_id,
                    event_mode=event_mode,
                )
            )
        prepared = await self._prepare_invocation(
            workflow,
            input=input,
            session_id=session_id,
            entry_node_id=entry_node_id,
            event_mode=event_mode,
        )
        return await self._aexecute_prepared(prepared)

    async def _aadmit_invocation(
        self,
        workflow: Workflow,
        input: dict[str, Any] | None = None,
        *,
        session_id: str | None = None,
        entry_node_id: str | None = None,
        event_mode: RuntimeEventMode = "standard",
    ) -> _PreparedInvocation:
        """Durably admit work without introducing background-task semantics."""

        if not self._runtime_loop.is_current():
            return await self._runtime_loop.arun(
                self._aadmit_invocation(
                    workflow,
                    input=input,
                    session_id=session_id,
                    entry_node_id=entry_node_id,
                    event_mode=event_mode,
                )
            )
        self._ensure_open()
        self._ensure_started()
        return await self._prepare_invocation(
            workflow,
            input=input,
            session_id=session_id,
            entry_node_id=entry_node_id,
            event_mode=event_mode,
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
        return await self._aexecute_prepared(prepared)

    async def _aadmit_rerun(
        self,
        workflow: Workflow,
        *,
        source_invocation_id: UUID,
    ) -> _PreparedRerun:
        """Admit a same-mode Invocation from a Standard/Full start boundary."""

        if not self._runtime_loop.is_current():
            return await self._runtime_loop.arun(
                self._aadmit_rerun(
                    workflow,
                    source_invocation_id=source_invocation_id,
                )
            )
        self._ensure_open()
        self._ensure_started()
        seed = await self.runtime_store.aload_invocation_rerun_seed(
            source_invocation_id
        )
        if seed is None:
            raise KeyError(f"Unknown Invocation: {source_invocation_id}")
        if seed.state not in {
            "waiting",
            "completed",
            "failed",
            "interrupted",
            "cancelled",
        }:
            raise ValueError(
                "Rerun requires a waiting or terminal source Invocation; "
                f"current state is {seed.state}."
            )
        if seed.event_mode == "minimal":
            raise ValueError(
                "Rerun requires a Standard or Full source Invocation; "
                "Minimal mode has no executable Genesis Session Context."
            )
        workflow_ir = self._get_or_compile_workflow(workflow)
        if workflow_ir.workflow_id != seed.workflow_id:
            raise ValueError(
                "Rerun Workflow does not match the source Invocation: "
                f"source={seed.workflow_id}, candidate={workflow_ir.workflow_id}."
            )
        if seed.entry_node_id not in workflow_ir.entry_node_ids:
            raise ValueError(
                "Source entry node is not an entry in the candidate Workflow: "
                f"{seed.entry_node_id}"
            )
        workflow_snapshot = self._registered_workflow_snapshot(workflow)
        await self.runtime_store.asave_workflow_snapshot(workflow_snapshot)
        if seed.session_context is None:
            raise RuntimeError(
                "Source Invocation has no executable Genesis Session Context."
            )
        session_context = SessionContext.from_record(
            deepcopy(seed.session_context.to_record())
        )
        session = await self._get_or_create_session(
            workflow_id=workflow_ir.workflow_id,
            workflow_revision_id=workflow_revision_id(
                workflow_snapshot.workflow_id,
                workflow_snapshot.definition_hash,
            ),
            session_id=f"rerun:{source_invocation_id}:{uuid4()}",
        )
        session.context = session_context
        prepared = await self._prepare_fresh_invocation(
            workflow_ir=workflow_ir,
            workflow_snapshot=workflow_snapshot,
            session=session,
            entry_node_id=seed.entry_node_id,
            input=deepcopy(seed.input),
            event_mode=seed.event_mode,
        )
        return _PreparedRerun(
            prepared=prepared,
            source_invocation_id=source_invocation_id,
            source_workflow_revision_id=seed.workflow_revision_id,
        )

    async def _arerun(
        self,
        workflow: Workflow,
        *,
        source_invocation_id: UUID,
    ) -> tuple[_PreparedRerun, Invocation]:
        admitted = await self._aadmit_rerun(
            workflow,
            source_invocation_id=source_invocation_id,
        )
        invocation = await self._aexecute_admitted(
            admitted.prepared,
            input=admitted.prepared.invocation.input,
        )
        return admitted, invocation

    async def _aexecute_prepared(
        self,
        prepared: _PreparedInvocation,
    ) -> Invocation:
        """Execute one admitted Invocation and terminalize escaped failures."""

        try:
            return await self.workflow_executor.ainvoke(
                workflow_ir=prepared.workflow_ir,
                session=prepared.session,
                invocation=prepared.invocation,
            )
        except Exception as exc:
            logger.exception(
                "Invocation execution infrastructure failed: invocation_id=%s",
                prepared.invocation.id,
            )
            await self.workflow_executor.afail_infrastructure(
                session=prepared.session,
                invocation=prepared.invocation,
                error=exc,
            )
            return prepared.invocation
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
        self._ensure_started()
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
        """Atomically claim and resume one external wait.

        The Store validates the compiled Workflow definition before changing
        the invocation from ``waiting`` to ``running``.
        Standard and Full waits survive a process restart with a durable
        backend; Minimal waits intentionally exist only in current process
        memory.
        """

        self._ensure_open()
        self._ensure_started()
        if not self._runtime_loop.is_current():
            return await self._runtime_loop.arun(
                self.aresume(
                    workflow,
                    session_id=session_id,
                    wait_key=wait_key,
                    output=output,
                )
            )
        workflow_ir = self._get_or_compile_workflow(workflow)
        workflow_snapshot = self._registered_workflow_snapshot(workflow)
        await self.runtime_store.asave_workflow_snapshot(
            workflow_snapshot,
        )
        session = await self.runtime_store.aclaim_waiting_session(
            workflow_revision_id=workflow_revision_id(
                workflow_snapshot.workflow_id,
                workflow_snapshot.definition_hash,
            ),
            session_key=session_id,
            wait_key=wait_key,
            workflow_definition_hash=workflow_ir.definition_hash,
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
            object_key = id(workflow)
            revision_id = self._workflow_object_revision_ids.get(object_key)
            registry_entry = (
                self.workflow_registry.get(revision_id)
                if revision_id is not None
                else None
            )
            if registry_entry is not None:
                return registry_entry.workflow_ir

            workflow_ir, workflow_snapshot = self._compile_workflow_locked(workflow)
            revision_id = workflow_revision_id(
                workflow_snapshot.workflow_id,
                workflow_snapshot.definition_hash,
            )
            registry_entry = self.workflow_registry.get(revision_id)
            if registry_entry is None:
                registry_entry = WorkflowRegistryEntry(
                    workflow=workflow,
                    workflow_ir=workflow_ir,
                    workflow_snapshot=workflow_snapshot,
                )
                self.workflow_registry[revision_id] = registry_entry
            self._workflow_object_revision_ids[object_key] = revision_id
            return registry_entry.workflow_ir

    def _compile_workflow_locked(
        self,
        workflow: Workflow,
    ) -> tuple[WorkflowIR, WorkflowVersionSnapshot]:
        """Compile Workflow source while the App registry lock is held.

        AutoAgentApp compiles each Workflow object once. The registered
        WorkflowIR is immutable execution input; later source-object mutation
        is deliberately ignored. A changed definition must use a new Workflow
        id/version and be registered explicitly.
        """

        compile_result = self.compiler.compile(workflow)
        if not compile_result.ok or compile_result.workflow_ir is None:
            diagnostics = [
                item.model_dump() for item in compile_result.diagnostics
            ]
            raise ValueError(f"Workflow validation failed: {diagnostics}")
        if compile_result.workflow_snapshot is None:
            raise RuntimeError("Compiler omitted WorkflowVersionSnapshot.")
        self._register_runtime_models_from_workflow_ir(
            compile_result.workflow_ir
        )
        return compile_result.workflow_ir, compile_result.workflow_snapshot

    def _register_runtime_models_from_workflow_ir(
        self,
        workflow_ir: WorkflowIR,
    ) -> None:
        """Trust and register Pydantic types declared by compiled contracts.

        Runtime values written in one process must be decodable after restart.
        Compiled callable annotations are part of the executable application
        definition, so their Pydantic models are safe to register without
        allowing persisted data to import arbitrary Python types.
        """

        for node in workflow_ir.nodes.values():
            contracts = (
                node.input_contract,
                node.operator_output_contract,
                node.output_contract,
            )
            for contract in contracts:
                annotations = [contract.annotation, contract.extra_annotation]
                annotations.extend(
                    parameter.annotation for parameter in contract.parameters
                )
                for annotation in annotations:
                    for model_type in _pydantic_model_types(annotation):
                        self.register_runtime_model(model_type)
            capability = node.capability
            if isinstance(capability, Operator):
                annotations = getattr(
                    capability.handler,
                    "__autoagent_runtime_annotations__",
                    (),
                )
                for annotation in annotations:
                    for model_type in _pydantic_model_types(annotation):
                        self.register_runtime_model(model_type)

    async def _prepare_invocation(
        self,
        workflow: Workflow,
        input: dict[str, Any] | None,
        *,
        session_id: str | None,
        entry_node_id: str | None,
        event_mode: RuntimeEventMode,
    ) -> _PreparedInvocation:
        """Compile, persist snapshot, check session admission, and claim liveness."""

        if event_mode not in {"minimal", "standard", "full"}:
            raise ValueError(f"Invalid event_mode: {event_mode}")
        workflow_ir = self._get_or_compile_workflow(workflow)
        workflow_snapshot = self._registered_workflow_snapshot(workflow)
        await self.runtime_store.asave_workflow_snapshot(
            workflow_snapshot,
        )
        selected_entry_node_id = entry_node_id or self._default_entry_node_id(workflow_ir)
        if selected_entry_node_id not in workflow_ir.entry_node_ids:
            raise ValueError(f"Invalid entry node id: {selected_entry_node_id}")

        session = await self._get_or_create_session(
            workflow_id=workflow_ir.workflow_id,
            workflow_revision_id=workflow_revision_id(
                workflow_snapshot.workflow_id,
                workflow_snapshot.definition_hash,
            ),
            session_id=session_id,
        )
        current = session.get_current_invocation()
        if current is not None and current.state in {
            "created",
            "running",
            "waiting",
        }:
            raise SessionBusyError(session, current)
        return await self._prepare_fresh_invocation(
            workflow_ir=workflow_ir,
            workflow_snapshot=workflow_snapshot,
            session=session,
            entry_node_id=selected_entry_node_id,
            input=input,
            event_mode=event_mode,
        )

    async def _prepare_fresh_invocation(
        self,
        *,
        workflow_ir: WorkflowIR,
        workflow_snapshot: WorkflowVersionSnapshot,
        session: Session,
        entry_node_id: str,
        input: dict[str, Any] | None,
        event_mode: RuntimeEventMode,
    ) -> _PreparedInvocation:
        invocation = Invocation(
            workflow_id=workflow_ir.workflow_id,
            workflow_revision_id=session.workflow_revision_id,
            workflow_version=workflow_ir.workflow_version,
            workflow_definition_hash=workflow_ir.definition_hash,
            entry_node_id=entry_node_id,
            input=input,
            event_mode=event_mode,
        )
        self._set_invocation_live(invocation.id, True)
        try:
            session = await self.runtime_store.aadmit_invocation(session.id, invocation)
        except Exception:
            self._set_invocation_live(invocation.id, False)
            raise
        return _PreparedInvocation(
            workflow_ir=workflow_ir,
            session=session,
            invocation=invocation,
        )

    def _default_entry_node_id(self, workflow_ir: WorkflowIR) -> str:
        entry_count = len(workflow_ir.entry_node_ids)
        if entry_count == 0:
            raise ValueError("Workflow has no entry node.")
        if entry_count > 1:
            raise ValueError("entry_node_id is required for workflows with multiple entry nodes.")
        return workflow_ir.entry_node_ids[0]

    def _registered_workflow_snapshot(
        self,
        workflow: Workflow,
    ) -> WorkflowVersionSnapshot:
        """Return the immutable snapshot compiled with the registered Workflow."""

        with self._workflow_registry_lock:
            object_key = id(workflow)
            revision_id = self._workflow_object_revision_ids[object_key]
            return self.workflow_registry[revision_id].workflow_snapshot

    async def _get_or_create_session(
        self,
        *,
        workflow_id: str,
        workflow_revision_id: str,
        session_id: str | None,
    ) -> Session:
        return await self.runtime_store.aget_or_create_session(
            workflow_id=workflow_id,
            workflow_revision_id=workflow_revision_id,
            session_key=session_id or str(uuid4()),
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
        if not live:
            self.runtime_store.notify_user_event_execution_settled(
                invocation_id
            )

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("AutoAgentApp is closed.")

    def _ensure_started(self) -> None:
        if not self._started:
            raise RuntimeError(
                "AutoAgentApp is not started. Call app.start() from synchronous "
                "code or await app.astart() from asynchronous code before "
                "invoking, submitting, or resuming Workflows."
            )


def _pydantic_model_types(annotation: Any) -> tuple[type[BaseModel], ...]:
    """Return every Pydantic model reachable from one type annotation."""

    found: list[type[BaseModel]] = []
    visited: set[Any] = set()

    def visit(value: Any) -> None:
        try:
            if value in visited:
                return
            visited.add(value)
        except TypeError:
            return
        if isinstance(value, type) and issubclass(value, BaseModel):
            found.append(value)
            for field in value.model_fields.values():
                visit(field.annotation)
            return
        origin = get_origin(value)
        if origin is not None:
            for argument in get_args(value):
                visit(argument)

    visit(annotation)
    return tuple(found)

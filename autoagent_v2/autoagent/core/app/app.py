"""V2 composition-free Core App and flat Execution API."""

from __future__ import annotations

import asyncio
import threading
from typing import Any
from uuid import UUID, uuid4

from ..compiler import WorkflowCompiler
from ..errors import (
    AdmissionRejectedError,
    InvocationConflictError,
    InvocationStateError,
    RecoveryError,
    WorkflowNotRegisteredError,
    WorkflowRegistrationError,
)
from ..executor import InvocationExecution, NodeExecutor
from ..operators import WaitOperator
from ..runtime import (
    AsyncInvocationStream,
    AttachedChannel,
    EventMode,
    EventChannel,
    Invocation,
    InvocationState,
    InvocationStream,
    RecoveryCheckpoint,
    RuntimeSink,
    RuntimeLoop,
    Session,
    now_ms,
)
from ..runtime.serialization import encode_runtime_value
from ..workflow import Workflow, WorkflowIR


class AutoAgentApp:
    """Compile once, register one Revision per Workflow id, then execute."""

    def __init__(
        self,
        *,
        runtime_sink: RuntimeSink | None = None,
        admission_timeout: float | None = 5.0,
        max_thread_workers: int = 8,
        max_parallel_units: int = 8,
        max_node_executions_per_invocation: int = 1_000,
    ) -> None:
        if max_node_executions_per_invocation < 1:
            raise ValueError("max_node_executions_per_invocation must be positive.")
        self._compiler = WorkflowCompiler()
        self._workflow_registry: dict[str, WorkflowIR] = {}
        self._source_workflow_index: dict[int, tuple[Workflow, str]] = {}
        self._sessions: dict[str, Session] = {}
        self._active: dict[UUID, InvocationExecution] = {}
        self._reserved_sessions: set[str] = set()
        self._runtime_sink = runtime_sink
        self._node_executor = NodeExecutor(
            max_thread_workers=max_thread_workers,
            max_parallel_units=max_parallel_units,
        )
        self._admission_timeout = admission_timeout
        self._max_node_executions_per_invocation = (
            max_node_executions_per_invocation
        )
        self._runtime = RuntimeLoop()
        self._registry_lock = threading.RLock()
        self._closed = False

    def register_workflow(self, workflow: Workflow) -> None:
        """Compile and atomically register an immutable WorkflowIR."""

        if self._closed:
            raise RuntimeError("App is closed.")
        compiled = self._compiler.compile(workflow)
        with self._registry_lock:
            existing = self._workflow_registry.get(compiled.workflow_id)
            if existing is not None:
                if existing.workflow_revision_id != compiled.workflow_revision_id:
                    raise WorkflowRegistrationError(
                        f"Workflow {compiled.workflow_id!r} already has Revision "
                        f"{existing.workflow_revision_id!r} in this App."
                    )
                self._source_workflow_index[id(workflow)] = (
                    workflow,
                    compiled.workflow_id,
                )
                return
            self._workflow_registry[compiled.workflow_id] = compiled
            self._source_workflow_index[id(workflow)] = (
                workflow,
                compiled.workflow_id,
            )

    def invoke(
        self,
        workflow: Workflow | str,
        invocation_input: Any,
        *,
        session_id: str | None = None,
        event_mode: EventMode | str = EventMode.STANDARD,
    ) -> Invocation:
        return self._runtime.run(
            self._invoke(
                workflow,
                invocation_input,
                session_id=session_id,
                event_mode=event_mode,
                wait=True,
            )
        )

    async def ainvoke(
        self,
        workflow: Workflow | str,
        invocation_input: Any,
        *,
        session_id: str | None = None,
        event_mode: EventMode | str = EventMode.STANDARD,
    ) -> Invocation:
        return await self._runtime.await_result(
            self._invoke(
                workflow,
                invocation_input,
                session_id=session_id,
                event_mode=event_mode,
                wait=True,
            )
        )

    def submit_invoke(
        self,
        workflow: Workflow | str,
        invocation_input: Any,
        *,
        session_id: str | None = None,
        event_mode: EventMode | str = EventMode.STANDARD,
    ) -> Invocation:
        return self._runtime.run(
            self._invoke(
                workflow,
                invocation_input,
                session_id=session_id,
                event_mode=event_mode,
                wait=False,
            )
        )

    async def asubmit_invoke(
        self,
        workflow: Workflow | str,
        invocation_input: Any,
        *,
        session_id: str | None = None,
        event_mode: EventMode | str = EventMode.STANDARD,
    ) -> Invocation:
        return await self._runtime.await_result(
            self._invoke(
                workflow,
                invocation_input,
                session_id=session_id,
                event_mode=event_mode,
                wait=False,
            )
        )

    def stream_invoke(
        self,
        workflow: Workflow | str,
        invocation_input: Any,
        *,
        session_id: str | None = None,
        event_mode: EventMode | str = EventMode.STANDARD,
        event_channel: EventChannel = "user",
    ) -> InvocationStream:
        invocation, channel = self._runtime.run(
            self._stream_invoke(
                workflow,
                invocation_input,
                session_id=session_id,
                event_mode=event_mode,
                event_channel=event_channel,
            )
        )
        return self._sync_stream(invocation, channel)

    async def astream_invoke(
        self,
        workflow: Workflow | str,
        invocation_input: Any,
        *,
        session_id: str | None = None,
        event_mode: EventMode | str = EventMode.STANDARD,
        event_channel: EventChannel = "user",
    ) -> AsyncInvocationStream:
        invocation, channel = await self._runtime.await_result(
            self._stream_invoke(
                workflow,
                invocation_input,
                session_id=session_id,
                event_mode=event_mode,
                event_channel=event_channel,
            )
        )
        return self._async_stream(invocation, channel)

    def resume(
        self, invocation: Invocation | UUID, wait_id: UUID, response: Any
    ) -> Invocation:
        return self._runtime.run(
            self._resume(invocation, wait_id, response, wait=True)
        )

    async def aresume(
        self, invocation: Invocation | UUID, wait_id: UUID, response: Any
    ) -> Invocation:
        return await self._runtime.await_result(
            self._resume(invocation, wait_id, response, wait=True)
        )

    def submit_resume(
        self, invocation: Invocation | UUID, wait_id: UUID, response: Any
    ) -> Invocation:
        return self._runtime.run(
            self._resume(invocation, wait_id, response, wait=False)
        )

    async def asubmit_resume(
        self, invocation: Invocation | UUID, wait_id: UUID, response: Any
    ) -> Invocation:
        return await self._runtime.await_result(
            self._resume(invocation, wait_id, response, wait=False)
        )

    def stream_resume(
        self,
        invocation: Invocation | UUID,
        wait_id: UUID,
        response: Any,
        *,
        event_channel: EventChannel = "user",
    ) -> InvocationStream:
        handle, channel = self._runtime.run(
            self._stream_resume(invocation, wait_id, response, event_channel)
        )
        return self._sync_stream(handle, channel)

    async def astream_resume(
        self,
        invocation: Invocation | UUID,
        wait_id: UUID,
        response: Any,
        *,
        event_channel: EventChannel = "user",
    ) -> AsyncInvocationStream:
        handle, channel = await self._runtime.await_result(
            self._stream_resume(invocation, wait_id, response, event_channel)
        )
        return self._async_stream(handle, channel)

    def recover(
        self, checkpoint: RecoveryCheckpoint, *, event_mode: EventMode | str = EventMode.STANDARD
    ) -> Invocation:
        return self._runtime.run(
            self._recover(checkpoint, event_mode=event_mode, wait=True)
        )

    async def arecover(
        self, checkpoint: RecoveryCheckpoint, *, event_mode: EventMode | str = EventMode.STANDARD
    ) -> Invocation:
        return await self._runtime.await_result(
            self._recover(checkpoint, event_mode=event_mode, wait=True)
        )

    def submit_recover(
        self, checkpoint: RecoveryCheckpoint, *, event_mode: EventMode | str = EventMode.STANDARD
    ) -> Invocation:
        return self._runtime.run(
            self._recover(checkpoint, event_mode=event_mode, wait=False)
        )

    async def asubmit_recover(
        self, checkpoint: RecoveryCheckpoint, *, event_mode: EventMode | str = EventMode.STANDARD
    ) -> Invocation:
        return await self._runtime.await_result(
            self._recover(checkpoint, event_mode=event_mode, wait=False)
        )

    def stream_recover(
        self,
        checkpoint: RecoveryCheckpoint,
        *,
        event_mode: EventMode | str = EventMode.STANDARD,
        event_channel: EventChannel = "user",
    ) -> InvocationStream:
        invocation, channel = self._runtime.run(
            self._stream_recover(checkpoint, event_mode, event_channel)
        )
        return self._sync_stream(invocation, channel)

    async def astream_recover(
        self,
        checkpoint: RecoveryCheckpoint,
        *,
        event_mode: EventMode | str = EventMode.STANDARD,
        event_channel: EventChannel = "user",
    ) -> AsyncInvocationStream:
        invocation, channel = await self._runtime.await_result(
            self._stream_recover(checkpoint, event_mode, event_channel)
        )
        return self._async_stream(invocation, channel)

    def cancel(self, invocation: Invocation | UUID) -> Invocation:
        return self._runtime.run(self._cancel(invocation))

    async def acancel(self, invocation: Invocation | UUID) -> Invocation:
        return await self._runtime.await_result(self._cancel(invocation))

    def close(self) -> None:
        if self._closed:
            return
        self._runtime.run(self._close())
        self._runtime.close()
        self._closed = True

    async def aclose(self) -> None:
        if self._closed:
            return
        await self._runtime.await_result(self._close())
        self._runtime.close()
        self._closed = True

    async def _invoke(
        self,
        workflow: Workflow | str,
        invocation_input: Any,
        *,
        session_id: str | None = None,
        event_mode: EventMode | str = EventMode.STANDARD,
        wait: bool,
    ) -> Invocation:
        execution = await self._create_execution(
            workflow,
            invocation_input,
            session_id=session_id,
            event_mode=EventMode(event_mode),
            stream=None,
        )
        self._launch(execution)
        if wait:
            await execution.boundary.wait()
        return execution.invocation

    async def _stream_invoke(
        self,
        workflow: Workflow | str,
        invocation_input: Any,
        *,
        session_id: str | None,
        event_mode: EventMode | str,
        event_channel: EventChannel,
    ) -> tuple[Invocation, AttachedChannel]:
        channel = AttachedChannel(event_channel)
        execution = await self._create_execution(
            workflow,
            invocation_input,
            session_id=session_id,
            event_mode=EventMode(event_mode),
            stream=channel,
        )
        self._launch(execution)
        return execution.invocation, channel

    async def _create_execution(
        self,
        workflow: Workflow | str,
        invocation_input: Any,
        *,
        session_id: str | None,
        event_mode: EventMode,
        stream: AttachedChannel | None,
    ) -> InvocationExecution:
        self._ensure_open()
        encode_runtime_value(invocation_input)
        ir = self._resolve_workflow(workflow)
        identifier = session_id or str(uuid4())
        session = self._sessions.get(identifier)
        timestamp = now_ms()
        if identifier in self._reserved_sessions:
            raise InvocationConflictError(
                f"Session {identifier!r} is already accepting an Invocation."
            )
        if session is not None and session.workflow_id != ir.workflow_id:
            raise InvocationConflictError(
                f"Session {identifier!r} belongs to Workflow {session.workflow_id!r}."
            )
        if session is not None and session.invocation is not None and not session.invocation.done():
            raise InvocationConflictError(
                f"Session {identifier!r} already has an active Invocation."
            )
        self._reserved_sessions.add(identifier)
        try:
            await self._admit()
        finally:
            self._reserved_sessions.discard(identifier)
        session = self._sessions.get(identifier)
        created_session = session is None
        previous_invocation = session.invocation if session is not None else None
        if session is None:
            session = Session(
                id=identifier,
                workflow_id=ir.workflow_id,
                created_at_ms=timestamp,
                updated_at_ms=timestamp,
            )
            self._sessions[identifier] = session
        elif session.invocation is not None and not session.invocation.done():
            raise InvocationConflictError(
                f"Session {identifier!r} already has an active Invocation."
            )

        invocation = Invocation(
            invocation_id=uuid4(),
            workflow_id=ir.workflow_id,
            workflow_revision_id=ir.workflow_revision_id,
            session_id=session.id,
            created_at_ms=timestamp,
        )
        session.invocation = invocation
        session.updated_at_ms = timestamp
        execution = InvocationExecution(
            workflow=ir,
            session=session,
            invocation=invocation,
            invocation_input=invocation_input,
            event_mode=event_mode,
            sink=self._runtime_sink,
            stream=stream,
            node_executor=self._node_executor,
            default_max_node_executions=self._max_node_executions_per_invocation,
        )
        self._active[invocation.id] = execution
        try:
            execution._offer_checkpoint(InvocationState.CREATED)
        except BaseException:
            self._active.pop(invocation.id, None)
            if created_session:
                self._sessions.pop(identifier, None)
            else:
                session.invocation = previous_invocation
            raise
        return execution

    def _launch(self, execution: InvocationExecution, *, recovered: bool = False) -> None:
        execution.boundary = asyncio.Event()
        execution.task = asyncio.create_task(self._drive(execution, recovered=recovered))

    async def _drive(self, execution: InvocationExecution, *, recovered: bool) -> None:
        await execution.run(recovered=recovered)
        if execution.invocation.done():
            self._active.pop(execution.invocation.id, None)

    async def _resume(
        self,
        invocation: Invocation | UUID,
        wait_id: UUID,
        response: Any,
        *,
        wait: bool,
        stream: AttachedChannel | None = None,
    ) -> Invocation:
        execution = self._active_execution(invocation)
        if execution.invocation.state not in {
            InvocationState.RUNNING,
            InvocationState.WAITING,
        }:
            raise InvocationStateError("Only a Running or Waiting Invocation can be resumed.")
        active_wait = execution.waits.get(wait_id)
        if active_wait is None:
            raise InvocationStateError(f"Wait {wait_id} is not active.")
        wait_node = execution.workflow.node(
            execution.node_executions[active_wait.node_execution_id].node_id
        )
        if not isinstance(wait_node.operator, WaitOperator):
            raise InvocationStateError("Waiting Invocation does not reference a WaitOperator.")
        response = wait_node.operator.response_contract.validate(response)
        encode_runtime_value(response)
        if stream is not None:
            if execution.stream is not None:
                raise InvocationStateError(
                    "Invocation already has an attached Event stream."
                )
            execution.stream = stream
        try:
            execution.claim_wait(wait_id, response)
        except (KeyError, RuntimeError) as error:
            raise InvocationStateError(str(error)) from error
        if execution.task is None or execution.task.done():
            self._launch(execution)
        if wait:
            await execution.boundary.wait()
        return execution.invocation

    async def _stream_resume(
        self,
        invocation: Invocation | UUID,
        wait_id: UUID,
        response: Any,
        event_channel: EventChannel,
    ) -> tuple[Invocation, AttachedChannel]:
        channel = AttachedChannel(event_channel)
        handle = await self._resume(
            invocation, wait_id, response, wait=False, stream=channel
        )
        return handle, channel

    async def _recover(
        self,
        checkpoint: RecoveryCheckpoint,
        *,
        event_mode: EventMode | str,
        wait: bool,
        stream: AttachedChannel | None = None,
    ) -> Invocation:
        self._ensure_open()
        if checkpoint.schema_version != 1:
            raise RecoveryError("Unsupported RecoveryCheckpoint schema version.")
        ir = self._resolve_workflow(checkpoint.workflow_id)
        if ir.workflow_revision_id != checkpoint.workflow_revision_id:
            raise RecoveryError("Checkpoint Workflow Revision is not registered.")
        if checkpoint.invocation_state in {
            InvocationState.COMPLETED.value,
            InvocationState.FAILED.value,
            InvocationState.CANCELLED.value,
        }:
            raise RecoveryError("A terminal Checkpoint cannot be recovered.")
        self._validate_checkpoint(ir, checkpoint)
        existing = self._sessions.get(checkpoint.session_id)
        if checkpoint.session_id in self._reserved_sessions:
            raise InvocationConflictError(
                f"Session {checkpoint.session_id!r} is already accepting an Invocation."
            )
        if existing is not None and existing.workflow_id != checkpoint.workflow_id:
            raise InvocationConflictError(
                f"Session {checkpoint.session_id!r} belongs to Workflow {existing.workflow_id!r}."
            )
        if existing is not None and existing.invocation is not None and not existing.invocation.done():
            raise InvocationConflictError(
                f"Session {checkpoint.session_id!r} already has an active Invocation."
            )
        if checkpoint.invocation_id in self._active:
            raise InvocationConflictError("Checkpoint Invocation is already active.")
        self._reserved_sessions.add(checkpoint.session_id)
        try:
            await self._admit()
        finally:
            self._reserved_sessions.discard(checkpoint.session_id)
        timestamp = now_ms()
        session = Session(
            id=checkpoint.session_id,
            workflow_id=checkpoint.workflow_id,
            context=dict(checkpoint.session_context),
            created_at_ms=timestamp,
            updated_at_ms=timestamp,
        )
        invocation = Invocation(
            invocation_id=checkpoint.invocation_id,
            workflow_id=checkpoint.workflow_id,
            workflow_revision_id=checkpoint.workflow_revision_id,
            session_id=checkpoint.session_id,
            created_at_ms=timestamp,
        )
        session.invocation = invocation
        self._sessions[session.id] = session
        execution = InvocationExecution(
            workflow=ir,
            session=session,
            invocation=invocation,
            invocation_input=checkpoint.invocation_input,
            event_mode=EventMode(event_mode),
            sink=self._runtime_sink,
            stream=stream,
            node_executor=self._node_executor,
            default_max_node_executions=self._max_node_executions_per_invocation,
        )
        execution.restore(checkpoint)
        self._active[invocation.id] = execution
        invocation._update(
            state=InvocationState(checkpoint.invocation_state),
            checkpoint=checkpoint,
            updated_at_ms=timestamp,
        )
        if invocation.state is InvocationState.WAITING:
            execution.boundary = asyncio.Event()
            execution.task = asyncio.create_task(
                self._announce_recovered_waiting(execution)
            )
            if wait:
                await execution.boundary.wait()
        else:
            self._launch(execution, recovered=True)
            if wait:
                await execution.boundary.wait()
        return invocation

    async def _announce_recovered_waiting(
        self, execution: InvocationExecution
    ) -> None:
        await execution.announce_recovered_waiting()
        if execution.invocation.done():
            self._active.pop(execution.invocation.id, None)

    async def _stream_recover(
        self,
        checkpoint: RecoveryCheckpoint,
        event_mode: EventMode | str,
        event_channel: EventChannel,
    ) -> tuple[Invocation, AttachedChannel]:
        channel = AttachedChannel(event_channel)
        invocation = await self._recover(
            checkpoint,
            event_mode=event_mode,
            wait=False,
            stream=channel,
        )
        return invocation, channel

    async def _cancel(self, invocation: Invocation | UUID) -> Invocation:
        execution = self._active_execution(invocation)
        execution.cancel_requested = True
        if execution.stream is not None:
            execution.stream.abandon()
        for task in tuple(execution.worker_tasks):
            task.cancel()
        if execution.task is not None and not execution.task.done():
            execution.task.cancel()
        else:
            asyncio.create_task(execution.finish_cancelled())
        await execution.terminal.wait()
        self._active.pop(execution.invocation.id, None)
        return execution.invocation

    async def _admit(self) -> None:
        if self._runtime_sink is None:
            return
        try:
            accepted = await self._runtime_sink.wait_until_admissible(
                self._admission_timeout
            )
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            raise AdmissionRejectedError(
                "RuntimeSink failed while checking new-execution admission."
            ) from error
        if not accepted:
            raise AdmissionRejectedError(
                "RuntimeSink did not admit a new Invocation before the timeout."
            )

    @staticmethod
    def _validate_checkpoint(
        workflow: WorkflowIR, checkpoint: RecoveryCheckpoint
    ) -> None:
        allowed_states = {
            InvocationState.CREATED.value,
            InvocationState.RUNNING.value,
            InvocationState.WAITING.value,
        }
        if checkpoint.invocation_state not in allowed_states:
            raise RecoveryError(
                f"Checkpoint Invocation state {checkpoint.invocation_state!r} is invalid."
            )
        if checkpoint.runtime_event_sequence < 0 or checkpoint.user_event_sequence < 0:
            raise RecoveryError("Checkpoint Event sequences cannot be negative.")
        known_nodes = {node.id for node in workflow.nodes}
        known_edges = {edge.id for edge in workflow.edges}
        for request in checkpoint.scheduler_state.ready:
            if request.node_id not in known_nodes:
                raise RecoveryError(
                    f"Checkpoint references unknown ready Node {request.node_id!r}."
                )
            for activation in request.activations:
                if activation.edge_id not in known_edges:
                    raise RecoveryError(
                        f"Checkpoint references unknown Edge {activation.edge_id!r}."
                    )
                if activation.source_execution_id not in checkpoint.required_outputs:
                    raise RecoveryError(
                        "Checkpoint is missing an output required by its Scheduler."
                    )
        for _, resolution in checkpoint.scheduler_state.resolutions:
            if resolution.edge_id not in known_edges:
                raise RecoveryError(
                    f"Checkpoint references unknown Edge {resolution.edge_id!r}."
                )
        for node_id, execution_id in checkpoint.latest_output_ids.items():
            if node_id not in known_nodes or execution_id not in checkpoint.required_outputs:
                raise RecoveryError("Checkpoint latest-output index is inconsistent.")
        waits = checkpoint.waits
        if checkpoint.invocation_state == InvocationState.WAITING.value and not waits:
            raise RecoveryError("A Waiting Checkpoint must contain wait state.")
        wait_ids: set[UUID] = set()
        wait_execution_ids: set[UUID] = set()
        node_states = {item.execution_id: item for item in checkpoint.node_states}
        for wait in waits:
            if wait.id in wait_ids:
                raise RecoveryError("Checkpoint contains duplicate Wait ids.")
            wait_ids.add(wait.id)
            if wait.node_execution_id in wait_execution_ids:
                raise RecoveryError("Checkpoint contains duplicate Wait executions.")
            wait_execution_ids.add(wait.node_execution_id)
            if wait.request.node_id not in known_nodes:
                raise RecoveryError("Checkpoint wait state references an unknown Node.")
            node_state = node_states.get(wait.node_execution_id)
            if node_state is None:
                raise RecoveryError(
                    "Checkpoint wait state is missing its NodeExecution state."
                )
            if node_state.node_id != wait.request.node_id or node_state.state != "waiting":
                raise RecoveryError("Checkpoint Wait and NodeExecution state disagree.")
            node = workflow.node(wait.request.node_id)
            if not isinstance(node.operator, WaitOperator):
                raise RecoveryError("Checkpoint Wait does not reference a WaitOperator.")
            try:
                node.operator.request_contract.validate(wait.payload)
            except TypeError as error:
                raise RecoveryError("Checkpoint Wait payload is invalid.") from error

    def _resolve_workflow(self, workflow: Workflow | str) -> WorkflowIR:
        with self._registry_lock:
            if isinstance(workflow, Workflow):
                indexed = self._source_workflow_index.get(id(workflow))
                if indexed is None or indexed[0] is not workflow:
                    raise WorkflowNotRegisteredError(
                        "The source Workflow object is not registered in this App."
                    )
                workflow_id = indexed[1]
            else:
                workflow_id = workflow
            try:
                return self._workflow_registry[workflow_id]
            except KeyError as error:
                raise WorkflowNotRegisteredError(
                    f"Workflow {workflow_id!r} is not registered."
                ) from error

    def _active_execution(self, invocation: Invocation | UUID) -> InvocationExecution:
        invocation_id = invocation.id if isinstance(invocation, Invocation) else invocation
        try:
            return self._active[invocation_id]
        except KeyError as error:
            raise InvocationStateError("Invocation is not active in this App.") from error

    def _sync_stream(
        self, invocation: Invocation, channel: AttachedChannel
    ) -> InvocationStream:
        return InvocationStream(
            invocation=invocation,
            receive=lambda: self._runtime.run(channel.receive()),
            close=lambda: self.cancel(invocation),
        )

    def _async_stream(
        self, invocation: Invocation, channel: AttachedChannel
    ) -> AsyncInvocationStream:
        return AsyncInvocationStream(
            invocation=invocation,
            receive=lambda: self._runtime.await_result(channel.receive()),
            close=lambda: self.acancel(invocation),
        )

    async def _close(self) -> None:
        executions = tuple(self._active.values())
        for execution in executions:
            execution.cancel_requested = True
            if execution.stream is not None:
                execution.stream.abandon()
            for task in tuple(execution.worker_tasks):
                task.cancel()
            if execution.task is not None and not execution.task.done():
                execution.task.cancel()
            else:
                asyncio.create_task(execution.finish_cancelled())
        if executions:
            await asyncio.gather(
                *(execution.terminal.wait() for execution in executions)
            )
        self._active.clear()
        self._node_executor.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("App is closed.")

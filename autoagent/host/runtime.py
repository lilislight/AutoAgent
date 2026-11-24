"""Internal Core assembly, event bindings and automatic graph restoration."""

from __future__ import annotations

import asyncio
import time
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from types import MappingProxyType

from autoagent import Operator, Workflow
from autoagent.core.app import AutoAgentApp, InvocationRef
from autoagent.core.commands import CommandIR
from autoagent.core.compiler import WorkflowCompiler, WorkflowDefinitionSnapshot
from autoagent.core.operators import OperatorRegistration
from autoagent.core.runtime import (
    InMemoryUserEventJournal,
    RuntimeEvent,
    RuntimeGraphCheckpoint,
    RuntimeRepository,
    SessionCheckpoint,
    StateReducer,
)

from .models import HostError
from .persistence import OperatorDefinition


@dataclass
class HostSetup:
    """Project resource setup; declarations are staged before execution is admitted."""

    environment: object
    workflows: list
    operators: list
    capability_resolvers: dict = field(default_factory=dict)
    extensions: dict = field(default_factory=dict)
    applications: dict = field(default_factory=dict)
    providers: object = None

    def register_operator(
        self, operator, *, capability_id, priority=0, enabled=True, default=False
    ):
        if not isinstance(operator, Operator):
            operator = Operator(operator)
        self.operators.append(
            OperatorRegistration(operator, capability_id, priority, enabled, default)
        )
        return operator

    def register_capability_resolver(self, capability_id, resolver):
        """Stage a strategy for this Capability; others retain default selection."""
        if not isinstance(capability_id, str) or not capability_id.strip():
            raise ValueError("Capability id cannot be empty.")
        if not callable(resolver):
            raise TypeError("Capability resolver must be callable.")
        if capability_id in self.capability_resolvers:
            raise ValueError(f"Resolver already registered for {capability_id!r}.")
        self.capability_resolvers[capability_id] = resolver
        return resolver

    def register_workflow(self, value):
        if not isinstance(value, Workflow):
            raise TypeError("Expected Workflow.")
        self.workflows.append(value)
        return value


class Runtime:
    """Owned by Host; caller-loop coordination never enters Core private state."""

    def __init__(
        self, persistence, settings, workflows, operators, lifespan, environment, *, usage_policy=None, providers=None, extensions=None,
        on_user_event=None, applications=None, resource_scopes=(),
    ):
        self.settings = settings
        self.usage_policy = usage_policy
        self.persistence = persistence
        self.on_user_event = on_user_event
        self.setup = HostSetup(
            MappingProxyType(dict(environment)), list(workflows), list(operators)
        )
        self.lifespan = lifespan
        self.extensions = extensions
        self.setup.extensions = extensions.items
        self.setup.providers = providers
        self.resource_loop = None
        self.setup.applications.update(applications or {})
        self.resource_scopes = tuple(resource_scopes)
        self.journal = InMemoryUserEventJournal()
        self.definitions = {}
        self.workflow_definitions = {}
        self.revisions = {}
        self.workflow_irs = {}
        self.workflow_ids = {}
        self.resident = {}
        self.fork_tasks = {}
        self.owners = {}
        self.residency = None
        self.locks = {}
        self.recovery_tasks = {}
        self.recovery_requests = {}
        self.needs_recovery = set()
        self.repository = RuntimeRepository(sink=self)
        self.app = AutoAgentApp(
            max_operator_concurrency=settings.max_operator_concurrency,
            runtime_repository=self.repository,
            user_event_sink=self,
            user_event_journal=self.journal,
            runtime_lifespan=self._resources,
        )

        # Agent declarations belong to Host composition, never Core imports.
        # Register before project lifespan so conflicting contracts fail early.
        from autoagent.agent.integration import LLM_CALL, EMBEDDING

        self.app.register_capability(LLM_CALL)
        self.app.register_capability(EMBEDDING)

    @asynccontextmanager
    async def _resources(self):
        from autoagent.agent.recording import runtime_llm_recorder

        with runtime_llm_recorder(self.persistence, usage_policy=self.usage_policy):
            async with AsyncExitStack() as stack:
                import asyncio
                self.resource_loop = asyncio.get_running_loop()
                await self.extensions.enter_lifespans(self.setup, stack)
                if self.setup.providers is not None:
                    await stack.enter_async_context(self.setup.providers.lifespan(self.setup))
                if self.lifespan is not None:
                    await stack.enter_async_context(self.lifespan(self.setup))
                self.extensions.discover(self.setup.workflows)
                await self.extensions.enter_lifespans(self.setup, stack)
                if self.setup.providers is not None:
                    self.setup.providers.install_operators(self.setup)
                await self.extensions.enter_resources(self.setup, stack)
                for scope in self.resource_scopes:
                    await stack.enter_async_context(scope(self.setup))
                yield

    async def prepare(self):
        await self.app.aprepare()
        ids = set()
        compiler = WorkflowCompiler()
        closure = {}
        for workflow in self.setup.workflows:
            if workflow.id in ids:
                raise HostError(f"Duplicate Workflow id: {workflow.id}")
            ids.add(workflow.id)
            ir = compiler.compile_or_raise(workflow)
            self.workflow_ids[workflow.id] = ir.workflow_revision_id
            pending = [ir]
            while pending:
                current = pending.pop()
                if current.workflow_revision_id in closure:
                    continue
                closure[current.workflow_revision_id] = current
                for node in current.nodes:
                    if (
                        isinstance(node.executable, CommandIR)
                        and node.executable.workflow is not None
                    ):
                        pending.append(node.executable.workflow)
                    elif isinstance(node.executable, Operator):
                        self.workflow_definitions[current.workflow_revision_id, node.id] = OperatorDefinition.from_operator(node.executable)
        await self.extensions.call("prepare", closure, self.setup)
        for capability_id, resolver in self.setup.capability_resolvers.items():
            self.app.register_capability_resolver(capability_id, resolver)
        for registration in self.setup.operators:
            self._definition(registration.operator)
        # Persistence is complete before any definition becomes executable.
        for definition in (*self.definitions.values(), *self.workflow_definitions.values()):
            await self.persistence.register_operator(definition)
        for revision, ir in closure.items():
            snapshot = WorkflowDefinitionSnapshot.from_workflow_ir(ir)
            await self.persistence.register_workflow(snapshot)
            self.revisions[revision] = snapshot
        for workflow in self.setup.workflows:
            pending = [self.app.register_workflow(workflow)]
            seen = set()
            while pending:
                current = pending.pop()
                if current.workflow_revision_id in seen:
                    continue
                seen.add(current.workflow_revision_id)
                self.workflow_irs[current.workflow_revision_id] = current
                pending.extend(node.executable.workflow for node in current.nodes
                    if isinstance(node.executable, CommandIR) and node.executable.workflow is not None)
        for wid in {snapshot.workflow_id for snapshot in self.revisions.values()}:
            self.workflow_ids[wid] = self.app.workflow_definition_snapshot(wid).workflow_revision_id
        for registration in self.setup.operators:
            self.app.register_operator(
                registration.operator,
                capability_id=registration.capability_id,
                priority=registration.priority,
                enabled=registration.enabled,
                default=registration.default,
            )

    def stage_workflow(self, workflow):
        """Compile and validate a complete dynamic closure without making it executable."""
        from autoagent.core.workflow import Capability
        ir = WorkflowCompiler().compile_or_raise(workflow)
        closure, definitions, capabilities = {}, {}, []
        pending = [ir]
        while pending:
            current = pending.pop()
            if current.workflow_revision_id in closure:
                continue
            closure[current.workflow_revision_id] = current
            for node in current.nodes:
                if isinstance(node.executable, CommandIR) and node.executable.workflow is not None:
                    pending.append(node.executable.workflow)
                elif isinstance(node.executable, Operator):
                    if getattr(node.executable.handler, "__runtime_extension__", None) is not None:
                        raise HostError("Dynamic registration supports ordinary Workflows using existing Host resources; runtime extension lifespans require startup registration.")
                    definitions[current.workflow_revision_id, node.id] = OperatorDefinition.from_operator(node.executable)
                elif isinstance(node.executable, Capability):
                    capabilities.append(node.executable)
                    if not self.app.operator_registry.for_capability(node.executable.id):
                        raise HostError(f"No enabled Operator is registered for Capability {node.executable.id!r}.")
        self.app.operator_registry.validate_capabilities(tuple(capabilities))
        return ir, closure, definitions

    async def persist_registration(self, closure, definitions):
        for definition in definitions.values():
            await self.persistence.register_operator(definition)
        for ir in closure.values():
            await self.persistence.register_workflow(WorkflowDefinitionSnapshot.from_workflow_ir(ir))

    async def publish_registration(self, workflow, closure, definitions):
        prior_irs, prior_definitions, prior_revisions = dict(self.workflow_irs), dict(self.workflow_definitions), dict(self.revisions)
        self.workflow_definitions.update(definitions)
        self.workflow_irs.update(closure)
        try:
            await self.app.aregister_workflow(workflow)
        except BaseException:
            self.workflow_irs, self.workflow_definitions, self.revisions = prior_irs, prior_definitions, prior_revisions
            raise
        # Admission must not see a new Revision until Core can execute it.
        self.revisions.update({revision: WorkflowDefinitionSnapshot.from_workflow_ir(ir) for revision, ir in closure.items()})
        for revision, ir in closure.items():
            self.workflow_ids[ir.workflow_id] = revision

    def _definition(self, operator):
        definition = OperatorDefinition.from_operator(operator)
        previous = self.definitions.get(operator.id)
        if previous is not None and previous.definition_id != definition.definition_id:
            raise HostError(f"Conflicting Operator implementations: {operator.id}")
        self.definitions[operator.id] = definition

    async def append(self, event):
        await self.extensions.call("before_event", event)
        definition_id = None
        if event.event_name in ("operator_call.started", "capability.resolved"):
            operator_id = event.payload.operator_id
            if not operator_id.startswith("system_command:"):
                definition = self.definitions.get(operator_id)
                if event.event_name == "operator_call.started":
                    state = self.repository.state(event.session_id)
                    invocation = state.invocation
                    occurrence = invocation.scheduler.occurrences.get(event.payload.occurrence_id) if invocation else None
                    if occurrence is not None:
                        direct = self.workflow_definitions.get((invocation.workflow_revision_id, occurrence.node_id))
                        if direct is not None and direct.operator_id == operator_id:
                            definition = direct
                if definition is None:
                    raise HostError(
                        f"Operator has no registered historical definition: {operator_id}"
                    )
                definition_id = definition.definition_id
        size = await self.persistence.append_runtime_event(
            event, operator_definition_id=definition_id
        )
        await self.extensions.call("after_event", event)
        if self.residency is not None:
            units = tuple(unit.child_session_id for unit in event.payload.units) if event.event_name == "child_invocation.planned" else ()
            self.residency.loop.call_soon_threadsafe(
                self._notice, event.session_id, units, size
            )

    async def append_user_event(self, event):
        if event.persist:
            await self.persistence.append_user_event(event)
        if self.on_user_event is not None:
            self.on_user_event(event)

    def _notice(self, sid, children, size):
        root = self.owners.get(sid, sid)
        for child in children:
            self.owners[child] = root
        self.residency.note(sid, 1, size)

    @asynccontextmanager
    async def coordinated(self, sid):
        # Count holders and queued users before awaiting; never retire a lock
        # while another request can still acquire that same instance.
        item = self.locks.setdefault(sid, [asyncio.Lock(), 0])
        item[1] += 1
        try:
            async with item[0]:
                yield
        finally:
            item[1] -= 1
            if item[1] == 0 and self.locks.get(sid) is item:
                del self.locks[sid]

    def forget(self, sid):
        ref = self.resident.pop(sid, None)
        if ref is not None:
            self.needs_recovery.discard(ref.invocation_id)
            for extension in self.extensions.items.values():
                callback = getattr(extension, "forget", None)
                if callback is not None:
                    callback(ref.invocation_id)
        self.owners = {child: root for child, root in self.owners.items() if root != sid}

    async def reference(self, invocation_id):
        supplied = invocation_id if isinstance(invocation_id, InvocationRef) else None
        if supplied is not None:
            invocation_id = supplied.invocation_id
        cached = next((ref for ref in self.resident.values() if ref.invocation_id == invocation_id), None)
        if cached is not None:
            if supplied is not None and supplied != cached:
                raise HostError("InvocationRef identity mismatch.")
            return cached
        row = await self.persistence.get_invocation(invocation_id, view="accepted")
        if row is None:
            raise HostError(f"Unknown Invocation: {invocation_id}")
        if row["parent_invocation_id"] is not None:
            raise HostError("Control operations require a Root Invocation.")
        snapshot = await self.persistence.get_workflow(row["workflow_revision_id"])
        ref = InvocationRef(
            session_id=row["session_id"],
            invocation_id=invocation_id,
            workflow_id=snapshot["workflow_id"],
            workflow_revision_id=row["workflow_revision_id"],
        )
        if supplied is not None and supplied != ref:
            raise HostError("InvocationRef identity mismatch.")
        return ref

    async def ensure_resident(self, ref):
        """Caller owns Session coordination; loading never implicitly runs it."""
        if ref.session_id in self.resident:
            if self.resident[ref.session_id] != ref:
                raise HostError("This Invocation has been replaced by a newer execution.")
            return False
        session = await self.persistence.get_session(ref.session_id, view="accepted")
        if session is None or session["latest_invocation_id"] != ref.invocation_id:
            raise HostError("An historical Invocation cannot replace its Session's latest Invocation.")
        await self.residency.reserve(ref.session_id)
        started = time.monotonic()
        try:
            # Restoration only consumes a durable prefix, including any local
            # buffered progress already accepted before residency changed.
            await self.persistence.flush()
            bundle = await self.persistence.read_recovery_bundle(ref.invocation_id)
            graph = await asyncio.to_thread(self._restore_graph, bundle)
            for checkpoint in graph.sessions:
                inv = checkpoint.state.invocation
                if inv is not None and not inv.terminal and inv.workflow_revision_id not in self.revisions:
                    raise HostError(f"Required Workflow Revision is unavailable: {inv.workflow_revision_id}")
            required_calls={(inv.id,call.id) for checkpoint in graph.sessions
                if (inv:=checkpoint.state.invocation) is not None and inv.status in ('created','running','waiting','settling')
                for call in inv.scheduler.operator_calls.values() if call.status in ('running','lost')}
            implementations = {definition.definition_id for definition in
                (*self.definitions.values(), *self.workflow_definitions.values())}
            for historical in bundle["operator_bindings"]:
                if (historical['invocation_id'],historical['call_id']) not in required_calls:
                    continue
                if historical["definition_id"] not in implementations:
                    raise HostError(f"Required Operator implementation is unavailable: {historical['operator_id']}")
            kept = {item.state.invocation.id for item in graph.sessions if item.state.invocation}
            for iid, sequence in bundle["user_frontiers"].items():
                if iid in kept:
                    self.journal.continue_sequence(iid, sequence)
            await self.extensions.call("restored", graph)
            await self.app.aload_checkpoint(graph)
            root = next(item.state.invocation for item in graph.sessions if item.session_id == ref.session_id)
            if not root.terminal:
                self.needs_recovery.add(ref.invocation_id)
            self.resident[ref.session_id] = ref
            for item in graph.sessions:
                if item.session_id != ref.session_id:
                    self.owners[item.session_id] = ref.session_id
            anchor = RuntimeGraphCheckpoint.from_record(bundle["checkpoint"]) if bundle["checkpoint"] else None
            self.residency.installed(ref.session_id, version=anchor.version if anchor else None, load_ms=(time.monotonic() - started) * 1000)
            entry = self.residency.entry(ref.session_id)
            entry.tail_events = sum(len(events) for events in bundle["events"].values())
            from autoagent.core.app._residency import retained_size
            entry.tail_bytes = retained_size(event for events in bundle["events"].values() for event in events)
            entry.checkpoint_at = time.monotonic() - max(0, time.time() - max((s.captured_at_us for s in anchor.sessions), default=int(time.time() * 1e6)) / 1e6) if anchor else time.monotonic()
            return True
        finally:
            self.residency.loading.discard(ref.session_id)

    def _restore_graph(self, bundle):
        anchor = (
            RuntimeGraphCheckpoint.from_record(bundle["checkpoint"])
            if bundle["checkpoint"]
            else None
        )
        states = (
            {item.session_id: item.state for item in anchor.sessions} if anchor else {}
        )
        reducer = StateReducer()
        for sid, records in bundle["events"].items():
            restored = reducer.reduce(
                tuple(RuntimeEvent.from_record(record) for record in records),
                states.get(sid),
            )
            if restored.session is not None:
                states[sid] = restored
        # Old Child Sessions can remain in source history. Only current ownership
        # belongs in the restored graph; unopened planned children need no State.
        selected, pending = {}, [bundle["root_session_id"]]
        while pending:
            sid = pending.pop()
            if sid in selected:
                continue
            state = states[sid]
            selected[sid] = state
            if state.invocation is not None:
                pending.extend(
                    unit.session_id
                    for plan in state.invocation.child_plans.values()
                    for unit in plan.units
                    if unit.session_id in states
                )
        return RuntimeGraphCheckpoint(
            bundle["root_session_id"],
            tuple(SessionCheckpoint.from_state(state) for state in selected.values()),
        )

    async def prepare_session(self, session_id):
        if session_id in self.owners:
            raise HostError("A Child Session cannot be admitted as a Root.")
        if session_id in self.resident:
            return
        session = await self.persistence.get_session(session_id, view="accepted")
        if session is not None and session["latest_invocation_id"] is not None:
            await self.ensure_resident(await self.reference(session["latest_invocation_id"]))
        else:
            if session is not None:
                planned = await self.persistence.list_invocations(session_id=session_id, include_planned=True, limit=1, view="accepted")
                if planned.items and planned.items[0]["parent_invocation_id"] is not None:
                    raise HostError("A planned Child Session cannot be admitted as a Root.")
            await self.residency.reserve(session_id)

    async def activate(self, ref, *, force=False, request_id=None, expected_version=None):
        if not force and ref.invocation_id not in self.needs_recovery:
            return None
        status = await self.app.astatus(ref)
        if not force and (status.control_state != "active" or status.status in {"completed", "failed", "cancelled"}):
            return None
        if not self.extensions.allows_recovery(ref.workflow_revision_id, status,
                interrupted=ref.invocation_id in self.needs_recovery, automatic=not force):
            from .models import HostRunConflict
            raise HostRunConflict("recovery_not_allowed", "This task ended or was interrupted. Send a new message to continue.")
        task = self.recovery_tasks.get(ref.invocation_id)
        if task is None:
            async def recover():
                source = None
                if force and status.status in {'failed', 'cancelled'}:
                    from autoagent.core import RecoverySource
                    await self.persistence.flush()
                    bundle = await self.persistence.read_recovery_bundle(ref.invocation_id, full_history=True)
                    source = await asyncio.to_thread(RecoverySource.from_events,
                        (RuntimeEvent.from_record(record) for records in bundle['events'].values() for record in records))
                    del bundle
                operation = self.app.arecover(ref, source=source, request_id=request_id, expected_version=expected_version)
                source = None
                result = await operation
                for extension in self.extensions.items.values():
                    result = await extension.recovered(ref, result)
                return result
            # Install single-flight ownership before SQL reads or reconstruction
            # yield; a second request must not start another recovery plan.
            task = asyncio.create_task(recover())
            self.recovery_tasks[ref.invocation_id] = task
            self.recovery_requests[ref.invocation_id] = (request_id, expected_version)
            def finished(result):
                if self.recovery_tasks.get(ref.invocation_id) is result:
                    self.recovery_tasks.pop(ref.invocation_id, None)
                    self.recovery_requests.pop(ref.invocation_id, None)
                if not result.cancelled() and result.exception() is None:
                    self.needs_recovery.discard(ref.invocation_id)
            task.add_done_callback(finished)
        elif force and (request_id is None or self.recovery_requests.get(ref.invocation_id) != (request_id, expected_version)):
            from .models import HostRunConflict
            raise HostRunConflict('recovery_in_progress', 'Recovery is already in progress for this Root.')
        return await asyncio.shield(task)

    async def save_checkpoint(self, graph, *, reason="automatic", pinned=False):
        return await self.persistence.save_checkpoint(
            graph, reason=reason, pinned=pinned, keep_latest=self.settings.checkpoint_keep_latest
        )

    async def shutdown(self):
        cleanup_error = None
        try:
            checkpoint = await self.app.aclose(timeout=None, capture_checkpoint=True)
        except Exception as error:  # noqa: BLE001 -- finish cleanup and rethrow the original failure
            # Core can report a resource-exit failure after physically closing;
            # its repeated close returns the already captured checkpoint.
            cleanup_error = error
            checkpoint = await self.app.aclose(timeout=None, capture_checkpoint=True)
        # Core close settles running controls before its loop stops. Drain the
        # Host activation wrappers while Persistence is still available.
        if self.fork_tasks:
            await asyncio.gather(*tuple(self.fork_tasks.values()), return_exceptions=True)
        self.saved_shutdown_checkpoint = checkpoint
        if checkpoint is not None:
            for graph in checkpoint.graphs:
                await self.save_checkpoint(graph, reason="shutdown")
        await self.persistence.flush()
        if cleanup_error is not None:
            raise cleanup_error

"""Host-facing asynchronous DatabaseSink and its owned I/O lifecycle."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from autoagent.core.compiler.snapshot import WorkflowDefinitionSnapshot
from autoagent.core.operators import Operator
from autoagent.core.runtime.events import RuntimeEvent
from autoagent.core.runtime.graph_checkpoint import RuntimeGraphCheckpoint
from autoagent.core.runtime.user_events import UserEvent

from .checkpoints import Checkpoints, checkpoint_key
from .maintenance import Maintenance
from .models import ClosedError, OperatorDefinition, PersistenceConfig, ReadView
from .queries import Queries
from .exploration import RevisionComparison, SessionInformation
from .tracing import TraceQueries
from .timeline import TimelineQueries
from .writer import Writer
from .datasets import TraceDatasets
from .watching import TraceHeads


class DatabaseSink:
    """Host-owned database access; async methods can be called from different loops.

    Create with ``await DatabaseSink.open(url, config=...)``. Register historical
    definitions before connecting this object to Core's event sink ports. Host
    owns runtime residency and must unload a stopped Graph before purging it.
    ``read_only=True`` opens an existing store without a writer lease or worker;
    mutations are rejected and SQL transactions are read-only.
    """

    def __init__(self, url: str, *, config: PersistenceConfig | None = None, read_only=False, pricing=None):
        from autoagent.agent.cost import CostCalculator, LocalTokenPricing
        if pricing is not None and not isinstance(pricing, CostCalculator):
            raise TypeError("pricing must implement CostCalculator.")
        self.pricing = pricing if pricing is not None else LocalTokenPricing()
        self.config = config or PersistenceConfig()
        self._writer = Writer(url, self.config, read_only=read_only)
        self._local_projections = None
        self._projection_worker = None
        self._projection_builds = set()
        self._projection_executor = None
        self._projection_closing = False
        self._queries = Queries(self._writer)
        self._traces = TraceQueries(self._queries)
        self._maintenance = Maintenance(self._writer)
        self._checkpoints = Checkpoints(self._queries, self._maintenance)
        self._checkpoint_guard = None
        self._datasets = TraceDatasets(self._queries, self.config, url)
        self._llm_calls = None
        self._agent_context = None
        self._agent_queue = None

    async def agent_queue_operation(self, method, *args, **kwargs):
        from .agent_queue import AgentQueueStorage
        async def operation():
            if self._agent_queue is None:
                self._agent_queue = AgentQueueStorage(self._writer, self._queries)
            return await getattr(self._agent_queue, method)(*args, **kwargs)
        return await self._writer.run(operation)

    async def agent_context_operation(self, method, *args, **kwargs):
        """Serialize Host settings operations on the owned database loop."""
        from .agent_context import AgentContextStorage
        async def operation():
            if self._agent_context is None:
                self._agent_context = AgentContextStorage(self._writer, self._queries)
            return await getattr(self._agent_context, method)(*args, **kwargs)
        return await self._writer.run(operation)

    @classmethod
    async def open(cls, url: str, *, config: PersistenceConfig | None = None, read_only=False, pricing=None):
        result = cls(url, config=config, read_only=read_only, pricing=pricing)
        try:
            await result._writer.ready()
            result._start_projection_worker()
        except BaseException:
            await result.close()
            raise
        return result

    async def __aenter__(self):
        await self._writer.ready()
        self._start_projection_worker()
        return self

    def _start_projection_worker(self):
        if (not self._writer.read_only and self.config.projection_mode == 'background'
                and self.config.projection_worker_enabled and self._projection_worker is None):
            from .projection_worker import ProjectionWorker
            self._projection_worker = ProjectionWorker(self._writer)
            self._projection_worker.start()

    async def __aexit__(self, *_):
        await self.close()

    async def register_workflow(self, snapshot: WorkflowDefinitionSnapshot) -> str:
        if not isinstance(snapshot, WorkflowDefinitionSnapshot):
            raise TypeError("Expected WorkflowDefinitionSnapshot.")
        await self._writer.run(
            self._writer.accept,
            "workflow_revision",
            snapshot.workflow_revision_id,
            snapshot,
            durable=True,
        )
        return snapshot.workflow_revision_id

    async def register_operator(self, definition: OperatorDefinition | Operator) -> str:
        if isinstance(definition, Operator):
            definition = OperatorDefinition.from_operator(definition)
        if not isinstance(definition, OperatorDefinition):
            raise TypeError("Expected OperatorDefinition or Operator.")

        async def register():
            await self._writer.accept(
                "operator_definition",
                definition.definition_id,
                definition,
                durable=True,
            )
            self._writer.operator_bindings[definition.operator_id] = (
                definition.definition_id
            )

        await self._writer.run(register)
        return definition.definition_id

    async def append(self, event: RuntimeEvent) -> None:
        await self.append_runtime_event(event)

    async def append_runtime_event(
        self, event: RuntimeEvent, *, operator_definition_id: str | None = None
    ) -> int:
        """Accept an Event and return its retained byte charge for Host accounting."""
        if not isinstance(event, RuntimeEvent):
            raise TypeError("Expected RuntimeEvent.")

        if event.event_name not in ("operator_call.started", "capability.resolved") and operator_definition_id is not None:
            raise ValueError("Only CallStarted/CapabilityResolved can carry Operator bindings.")
        # Local admission uses the same owned queue as the Writer. Unregistered
        # bindings and cold/history checks still use its asynchronous slow path.
        binding = operator_definition_id
        size = self._writer.try_accept(
            "runtime_event", event.id, event, binding=binding,
            strict_binding=operator_definition_id is not None,
        )
        if size is not None:
            return size

        async def accept():
            binding = operator_definition_id
            if event.event_name in ("operator_call.started", "capability.resolved"):
                if binding is None:
                    binding = self._writer.operator_bindings.get(
                        event.payload.operator_id
                    )
            elif binding is not None:
                raise ValueError(
                    "Only CallStarted/CapabilityResolved can carry Operator bindings."
                )
            return await self._writer.accept(
                "runtime_event",
                event.id,
                event,
                binding=binding,
                strict_binding=operator_definition_id is not None,
            )

        return await self._writer.run(accept)

    async def append_user_event(self, event: UserEvent) -> None:
        if isinstance(event, UserEvent) and not event.persist:
            raise ValueError("Transient User Events cannot be persisted.")
        if not isinstance(event, UserEvent):
            raise TypeError("Expected UserEvent.")
        if self._writer.try_accept("user_event", event.id, event) is not None:
            return
        await self._writer.run(self._writer.accept, "user_event", event.id, event)

    async def record_llm_call(self, record) -> None:
        """Persist one model-call metadata update; no model SDK or query API required."""
        from autoagent.agent.recording import LLMCallRecord
        from .llm_calls import LLMCallStorage

        record = LLMCallRecord.model_validate(record)
        async def write():
            if self._llm_calls is None:
                self._llm_calls = LLMCallStorage(self._writer, pricing=self.pricing)
            await self._llm_calls.write(record)
        await self._writer.run(write)

    async def admit_llm_call(self, record, checks) -> None:
        """Atomically check all applicable settled budgets and register a call."""
        from autoagent.agent.recording import LLMCallRecord
        from .llm_calls import LLMCallStorage
        record = LLMCallRecord.model_validate(record)
        async def admit():
            if self._llm_calls is None:
                self._llm_calls = LLMCallStorage(self._writer, pricing=self.pricing)
            await self._llm_calls.admit(record, checks)
        await self._writer.run(admit)

    async def token_usage(self, *, query=None, **filters):
        """Aggregate recorded token facts; no optional table is created by reads."""
        from autoagent.agent.usage import TokenUsageQuery
        from .llm_calls import LLMCallStorage
        if query is not None and filters:
            raise ValueError("Pass a TokenUsageQuery or keyword filters, not both.")
        query = TokenUsageQuery(**filters) if query is None else query
        if not isinstance(query, TokenUsageQuery):
            raise TypeError("Expected TokenUsageQuery.")
        async def read():
            if self._llm_calls is None:
                self._llm_calls = LLMCallStorage(self._writer, pricing=self.pricing)
            return await self._llm_calls.summary(query)
        return await self._writer.run(read)

    async def mark_llm_call_unsettled(self, llm_call_id: str) -> None:
        """Forget a transient active call after interrupted admission/settlement."""
        async def mark():
            async with self._writer.mutation:
                if self._llm_calls is not None:
                    self._llm_calls.active.discard(llm_call_id)
        await self._writer.run(mark)

    def _checkpoint_lock(self):
        # Created and used only on the Writer loop, including cross-loop callers.
        if self._checkpoint_guard is None:
            self._checkpoint_guard = asyncio.Lock()
        return self._checkpoint_guard

    async def _checkpoint(self, checkpoint, anchor, *, pinned=False, reason="automatic", keep_latest=None):
        if not isinstance(checkpoint, RuntimeGraphCheckpoint):
            raise TypeError("Expected complete RuntimeGraphCheckpoint.")
        if type(pinned) is not bool:
            raise ValueError("pinned must be a boolean.")
        if keep_latest is not None and (type(keep_latest) is not int or keep_latest < 1):
            raise ValueError("keep_latest must be a positive integer.")
        if reason not in ("automatic", "manual", "unload", "shutdown", "import"):
            raise ValueError("Unknown Checkpoint creation reason.")

        async def save():
            async with self._checkpoint_lock():
                identity = checkpoint_key(checkpoint)
                exists = await self._checkpoints.exists(identity)
                if not exists:
                    # Validate ownership and causal closure only for new versions.
                    await self._writer.cpu(lambda: RuntimeGraphCheckpoint.from_record(checkpoint.to_record()))
                    await self._writer.accept("checkpoint", identity, checkpoint, anchor=anchor, durable=True)
                await self._maintenance.run(
                    self._checkpoints.configure, identity, pinned, reason if not exists else None, keep_latest
                )
                return identity
        return await self._writer.run(save)

    async def save_checkpoint(self, checkpoint, *, pinned=False, reason="automatic", keep_latest=None) -> str:
        return await self._checkpoint(checkpoint, False, pinned=pinned, reason=reason, keep_latest=keep_latest)

    async def import_checkpoint(self, checkpoint) -> str:
        return await self._checkpoint(checkpoint, True, reason="import")

    async def checkpoint_info(self, checkpoint_id):
        """Read metadata and safety dependencies without decoding snapshot bodies."""
        return await self._writer.run(self._checkpoints.info, checkpoint_id)

    async def read_checkpoint_position(self, session_id, sequence, *, full_history=False):
        return await self._writer.run(self._checkpoints.historical_bundle, session_id, sequence, full_history=full_history)

    async def initialize_fork(self, events):
        """Admit the entire new ownership graph in one durable transaction."""
        from types import SimpleNamespace
        from .storage import prepare
        entries = [SimpleNamespace(kind='runtime_event', identity=e.id, value=e,
            binding=None, anchor=False) for e in events]
        async def write(conn):
            # prepare() is batch bounded; every batch still belongs to this one
            # transaction. A conflict cannot leave a partially admitted family.
            pending = entries
            while pending:
                encoded = await self._writer.cpu(prepare, pending, self._writer.config)
                await self._writer.storage.write_transaction(conn, encoded)
                pending = pending[len(encoded):]
        return await self._writer.run(self._maintenance.run, write)

    async def fork_position(self, session_id, sequence):
        from sqlalchemy import select
        from .schema import meta
        async def read():
            async with self._queries.view(control=True) as (conn, _, _, _):
                row = (await conn.execute(select(meta.tables['runtime_events'].c.event_id,
                    meta.tables['runtime_events'].c.invocation_id,meta.tables['runtime_events'].c.event_name,
                    meta.tables['runtime_events'].c.payload_text).where(
                    meta.tables['runtime_events'].c.session_id == session_id,
                    meta.tables['runtime_events'].c.sequence == sequence))).mappings().first()
                if row is None:
                    return None
                import json
                data = dict(row)
                data['initialization_events'] = json.loads(data.pop('payload_text')).get('initialization_events',1)
                return data
        return await self._writer.run(read)

    async def set_checkpoint_pinned(self, checkpoint_id, pinned):
        if type(pinned) is not bool:
            raise ValueError("pinned must be a boolean.")
        async def change():
            async with self._checkpoint_lock():
                if not await self._maintenance.run(self._checkpoints.pin, checkpoint_id, pinned):
                    return None
                return await self._checkpoints.info(checkpoint_id)
        return await self._writer.run(change)

    async def get_checkpoint(self, checkpoint_id):
        record = await self._writer.run(
            self._queries.source_record, "checkpoint", checkpoint_id
        )
        return RuntimeGraphCheckpoint.from_record(record) if record else None

    async def compare_workflow_revisions(self, base, target, **options):
        return await self._writer.run(RevisionComparison(self._queries).page, base, target, **options)

    async def revision_comparison_object(self, base, target, kind, identity):
        return await self._writer.run(RevisionComparison(self._queries).object, base, target, kind, identity)

    async def session_information(self, session_id):
        return await self._writer.run(SessionInformation(self._queries).info, session_id)

    async def session_history(self, session_id, **options):
        return await self._writer.run(SessionInformation(self._queries).history, session_id, **options)

    async def session_workflows(self, session_id, **options):
        return await self._writer.run(SessionInformation(self._queries).workflows, session_id, **options)

    async def workflow_graph(self, revision_id):
        return await self._writer.run(self._traces.graph, revision_id)

    async def workflow_object(self, revision_id, kind, identity):
        return await self._writer.run(self._traces.definition, revision_id, kind, identity)

    async def trace_page(self, kind, invocation_id=None, **filters):
        return await self._writer.run(self._traces.page, kind, invocation_id, **filters)

    async def execution_sessions(self, **filters):
        return await self._writer.run(self._traces.sessions, **filters)

    async def execution_trace(self, invocation_id):
        return await self._writer.run(self._traces.execution, invocation_id)

    async def execution_family(self, invocation_id):
        return await self._writer.run(self._traces.family, invocation_id)

    async def trace_dataset(self, invocation_id, **options):
        return await self._writer.run(self._datasets.create, invocation_id, **options)

    async def trace_dataset_page(self, snapshot_id, kind, page):
        return await self._writer.run(self._datasets.page, snapshot_id, kind, page)

    async def release_trace_dataset(self, snapshot_id):
        await self._writer.run(self._datasets.release, snapshot_id)

    async def trace_head(self, invocation_id):
        return await self._writer.run(TraceHeads(self._queries).read, invocation_id)

    async def wait_trace_commit(self, revision, timeout):
        return await self._writer.run(self._writer.wait_committed, revision, timeout)

    @asynccontextmanager
    async def execution_notifications(self, invocation_id):
        """Scope commit notifications to the lifetime of an execution observer."""
        writer = self._writer
        subscription = asyncio.create_task(writer.run(writer.subscribe_execution, invocation_id))
        try:
            # Registration can finish on the Writer before its result reaches
            # this loop. Preserve that result so cancellation can still remove it.
            token, revision = await asyncio.shield(subscription)

            async def changed(after, timeout, root_id):
                return await writer.run(writer.wait_execution_change, token, after, timeout, root_id)

            yield revision, changed
        finally:
            try:
                token, _ = await subscription
                await writer.run(writer.unsubscribe_execution, token)
            except ClosedError:
                pass

    async def execution_timeline(self, invocation_id):
        return await self._writer.run(TimelineQueries(self._queries).overview, invocation_id)

    async def timeline_page(self, invocation_id, **filters):
        return await self._writer.run(TimelineQueries(self._queries).page, invocation_id, **filters)

    async def timeline_position(self, invocation_id, kind, identity, **filters):
        return await self._writer.run(TimelineQueries(self._queries).position, invocation_id, kind, identity, **filters)

    async def timeline_event_groups(self, invocation_id, **filters):
        return await self._writer.run(TimelineQueries(self._queries).event_groups, invocation_id, **filters)

    async def timeline_markers(self, invocation_id, **filters):
        return await self._writer.run(TimelineQueries(self._queries).markers, invocation_id, **filters)

    async def timeline_frame(self, invocation_id, **position):
        return await self._writer.run(TimelineQueries(self._queries).frame, invocation_id, **position)

    async def timeline_replay(self, invocation_id, **position):
        return await self._writer.run(TimelineQueries(self._queries).replay, invocation_id, **position)

    async def execution_record(self, invocation_id, kind, identity):
        return await self._writer.run(TimelineQueries(self._queries).record, invocation_id, kind, identity)

    async def history_identity(self):
        from .history import History
        return await self._writer.run(History(self._queries).identity)

    async def execution_diagnosis(self, invocation_id, *, limit=20):
        from .diagnostics import Diagnostics
        return await self._writer.run(Diagnostics(self._queries).read, invocation_id, limit=limit)

    async def invocation_summary(self, invocation_id):
        from .history import History
        return await self._writer.run(History(self._queries).invocation, invocation_id)

    async def capture_execution_evidence(self, invocation_id, *, limit=1000):
        from .evidence import capture
        await self.flush()
        return await self._writer.run(capture, self._queries, invocation_id, limit)

    async def business_history(self, member, kind, **options):
        from .business import BusinessHistory
        from .inspection import ReadBudget
        async def read():
            result = await BusinessHistory(self._queries, self.config).records(member, kind, **options)
            ReadBudget(self.config).check_result(result)
            return result
        return await self._writer.run(read)

    async def business_value(self, member, reference, *, pointer=None):
        from .business import BusinessHistory
        return await self._writer.run(BusinessHistory(self._queries, self.config).value, member, reference, pointer=pointer)

    async def business_order(self, snapshot, earlier, later, *, limit):
        from .business import BusinessHistory
        return await self._writer.run(BusinessHistory(self._queries, self.config).order, snapshot, earlier, later, limit=limit)

    async def business_position(self, snapshot, position):
        from .business import BusinessHistory
        return await self._writer.run(BusinessHistory(self._queries, self.config).at, snapshot, position)

    async def inspect_execution_state(self, invocation_id, *, sequence, scope=None, pointer=None, field=None):
        from .inspection import StateInspection
        return await self._writer.run(StateInspection(self._queries, self.config).read,
            invocation_id, sequence=sequence, scope=scope, pointer=pointer, field=field)

    async def execution_events(self, invocation_id, **options):
        from .history import History
        return await self._writer.run(History(self._queries).events, invocation_id, **options)

    async def execution_event(self, invocation_id, event_id, **options):
        from .history import History
        return await self._writer.run(History(self._queries).event, invocation_id, event_id, **options)

    async def trace_value(self, invocation_id, kind, identity, field, *, pointer=None):
        return await self._writer.run(self._traces.value, invocation_id, kind, identity, field, pointer=pointer)

    async def execution_stages(self, invocation_id, occurrence_id, **filters):
        return await self._writer.run(self._traces.stages, invocation_id, occurrence_id, **filters)

    async def execution_stage_value(self, invocation_id, occurrence_id, event_id, field, *, pointer=None):
        return await self._writer.run(self._traces.stage_value, invocation_id, occurrence_id, event_id, field, pointer=pointer)

    async def get_workflow(self, revision_id):
        return await self._writer.run(
            self._queries.source_record, "workflow_revision", revision_id
        )

    async def get_operator(self, definition_id):
        return await self._writer.run(
            self._queries.source_record, "operator_definition", definition_id
        )

    async def get_runtime_event(self, event_id):
        record = await self._writer.run(
            self._queries.source_record, "runtime_event", event_id
        )
        return RuntimeEvent.from_record(record) if record else None

    async def get_user_event(self, event_id):
        record = await self._writer.run(
            self._queries.source_record, "user_event", event_id
        )
        return UserEvent.from_record(record) if record else None

    async def list_workflows(self, *, workflow_id=None, limit=100, cursor=None):
        return await self._writer.run(
            self._queries.definitions, "workflow_revision", workflow_id, limit, cursor
        )

    async def workflow_catalog(self, *, registered_ids=(), search="", limit=100, cursor=None, registered_only=False):
        return await self._writer.run(self._queries.workflow_catalog, registered_ids, search, limit, cursor, registered_only)

    async def workflow_revisions(self, workflow_id, *, prefix=None, limit=100, cursor=None):
        return await self._writer.run(self._queries.workflow_revisions, workflow_id, prefix, limit, cursor)

    async def list_operators(self, *, operator_id=None, limit=100, cursor=None):
        return await self._writer.run(
            self._queries.definitions, "operator_definition", operator_id, limit, cursor
        )

    async def list_checkpoints(self, session_id, *, limit=100, cursor=None):
        return await self._writer.run(
            self._checkpoints.page, session_id, limit, cursor
        )

    async def runtime_events(
        self,
        session_id,
        *,
        after=0,
        limit=100,
        view: ReadView = "durable",
        invocation_id=None,
    ):
        return await self._writer.run(
            self._queries.events,
            "runtime_event",
            session_id,
            after,
            limit,
            view,
            None,
            invocation_id,
        )

    async def user_events(
        self,
        invocation_id,
        *,
        after=0,
        occurrence_id=None,
        limit=100,
        view: ReadView = "durable",
    ):
        return await self._writer.run(
            self._queries.events,
            "user_event",
            invocation_id,
            after,
            limit,
            view,
            occurrence_id,
        )

    async def list_sessions(
        self, *, limit=100, cursor=None, view: ReadView = "durable"
    ):
        return await self._writer.run(
            self._queries.list_rows, "sessions", {}, limit, cursor, view
        )

    async def list_invocations(
        self,
        *,
        workflow_revision_id=None,
        session_id=None,
        root_invocation_id=None,
        status=None,
        include_planned=False,
        limit=100,
        cursor=None,
        view: ReadView = "durable",
    ):
        filters = {
            k: v
            for k, v in {
                "workflow_revision_id": workflow_revision_id,
                "session_id": session_id,
                "root_invocation_id": root_invocation_id,
                "status": status,
            }.items()
            if v is not None
        }
        return await self._writer.run(
            self._queries.list_rows,
            "invocations",
            filters,
            limit,
            cursor,
            view,
            include_planned,
        )

    async def get_invocation(self, invocation_id, *, view: ReadView = "durable"):
        return await self._writer.run(
            self._queries.get_row, "invocations", {"invocation_id": invocation_id}, view
        )

    async def control_metadata(self, invocation_id, registered, **options):
        from .controls import ControlQueries
        return await self._writer.run(ControlQueries(self._queries).read, invocation_id, registered, **options)

    async def execution_state(self, invocation_id, *, settled_version=None):
        from .execution_state import ExecutionState
        return await self._writer.run(ExecutionState(self._queries).read, invocation_id,
                                      settled_version=settled_version)

    async def get_session(self, session_id, *, view: ReadView = "durable"):
        return await self._writer.run(
            self._queries.get_row, "sessions", {"session_id": session_id}, view
        )

    async def graph_changes(
        self,
        invocation_id,
        *,
        after_sequence=0,
        limit=100,
        root_graph=False,
        view: ReadView = "durable",
    ):
        return await self._writer.run(
            self._queries.changes,
            invocation_id,
            after_sequence,
            limit,
            root_graph,
            view,
        )

    async def list_nodes(
        self, invocation_id, *, limit=100, cursor=None, view: ReadView = "durable"
    ):
        return await self._writer.run(
            self._queries.list_rows,
            "node_executions",
            {"invocation_id": invocation_id},
            limit,
            cursor,
            view,
        )

    async def list_calls(
        self,
        invocation_id,
        *,
        occurrence_id=None,
        limit=100,
        cursor=None,
        view: ReadView = "durable",
    ):
        filters = {"invocation_id": invocation_id}
        if occurrence_id is not None:
            filters["occurrence_id"] = occurrence_id
        return await self._writer.run(
            self._queries.list_rows, "operator_calls", filters, limit, cursor, view
        )

    async def list_waits(
        self, invocation_id, *, limit=100, cursor=None, view: ReadView = "durable"
    ):
        return await self._writer.run(
            self._queries.list_rows,
            "waits",
            {"invocation_id": invocation_id},
            limit,
            cursor,
            view,
        )

    async def node_detail(
        self, invocation_id, occurrence_id, *, view: ReadView = "durable"
    ):
        return await self._writer.run(
            self._queries.detail,
            "node_executions",
            {"invocation_id": invocation_id, "occurrence_id": occurrence_id},
            view,
        )

    async def call_detail(self, invocation_id, call_id, *, view: ReadView = "durable"):
        return await self._writer.run(
            self._queries.detail,
            "operator_calls",
            {"invocation_id": invocation_id, "call_id": call_id},
            view,
        )

    async def wait_detail(self, invocation_id, wait_id, *, view: ReadView = "durable"):
        return await self._writer.run(
            self._queries.detail,
            "waits",
            {"invocation_id": invocation_id, "wait_id": wait_id},
            view,
        )

    async def edge_detail(
        self,
        invocation_id,
        edge_id,
        *,
        limit=100,
        view: ReadView = "durable",
        evaluations_cursor=None,
        changes_cursor=None,
    ):
        return await self._writer.run(
            self._queries.edge,
            invocation_id,
            edge_id,
            limit,
            view,
            evaluations_cursor,
            changes_cursor,
        )

    async def resolve(self, reference):
        return await self._writer.run(self._queries.resolve, reference)

    async def statistics(self, entity, *, workflow_revision_id=None):
        return await self._writer.run(
            self._queries.statistics, entity, workflow_revision_id
        )

    async def execution_snapshot(self, invocation_id, *, view="accepted"):
        return await self._writer.run(self._queries.execution_snapshot, invocation_id, view)

    async def activation_candidates(self, now_us, *, limit=32, cursor=None):
        return await self._writer.run(self._queries.activation_candidates, now_us, limit, cursor)

    async def read_recovery_bundle(self, root_invocation_id, *, full_history=False):
        return await self._writer.run(self._queries.recovery_bundle, root_invocation_id, full_history=full_history)

    async def flush(self):
        await self._writer.run(self._writer.flush)

    async def projection_cache_statistics(self):
        async def statistics():
            result = await self._queries.projection_cache.statistics()
            result["local"] = self._local_projections.statistics() if self._local_projections else {"enabled": False}
            # Periodic memory telemetry must stay free of database reads.
            result['database'] = dict(mode=self.config.projection_mode,
                worker=self._projection_worker.statistics() if self._projection_worker else {'running': False})
            return result
        return await self._writer.run(statistics)

    async def _projection_status(self):
        from .query_projections import progress
        from sqlalchemy import select
        from .schema import meta
        locked = self._writer.storage.memory
        if locked:
            await self._writer.mutation.acquire()
        try:
            async with self._writer.storage.query_engine.connect() as conn:
                if not self._writer.storage.sqlite:
                    conn = await conn.execution_options(isolation_level='REPEATABLE READ')
                header = (await conn.execute(select(meta.tables['persistence_meta']))).mappings().one()
                order = await progress(conn, header['store_id'])
        finally:
            if locked:
                self._writer.mutation.release()
        return dict(mode=self.config.projection_mode, processed_order=order,
                    durable_order=header['last_store_order'], lag_events=header['last_store_order']-(order or 0),
                    worker=self._projection_worker.statistics() if self._projection_worker else {'running': False})

    async def query_projection_status(self):
        return await self._writer.run(self._projection_status)

    async def build_query_projections(self, *, rebuild=False):
        """Explicit maintenance; ordinary reads never write projection rows."""
        if type(rebuild) is not bool:
            raise ValueError('rebuild must be a boolean.')
        async def check():
            self._writer.check_open()
        await self._writer.run(check)
        if rebuild:
            from .query_projections import reset
            await self._writer.run(self._maintenance.run, reset)
        from .projection_worker import ProjectionWorker
        worker = ProjectionWorker(self._writer)
        async def submit():
            from concurrent.futures import ThreadPoolExecutor
            self._writer.check_open()
            if self._projection_closing:
                raise ClosedError('Persistence is closing.')
            if self._projection_executor is None:
                self._projection_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='autoagent-projection-build')
            future = self._projection_executor.submit(lambda: asyncio.run(worker.build()))
            self._projection_builds.add(future)
            future.add_done_callback(lambda _: self._writer.loop.call_soon_threadsafe(self._projection_builds.discard, future))
            return future
        future = await self._writer.run(submit)
        count = await asyncio.shield(asyncio.wrap_future(future))
        return {**await self.query_projection_status(), 'processed_events': count}

    async def health(self):
        if self._writer._closed or self._writer._close_future is not None:
            from .models import PersistenceHealth

            return PersistenceHealth(
                self._writer.state,
                self._writer.pending_count,
                self._writer.retained,
                self._writer.accepted_ticket,
                self._writer.durable_ticket,
            )
        return await self._writer.run(self._writer.health)

    async def close(self):
        if self._projection_executor is not None:
            async def drain():
                self._projection_closing = True
                await asyncio.gather(*(asyncio.wrap_future(future) for future in tuple(self._projection_builds)), return_exceptions=True)
            await self._writer.run(drain)
            self._projection_executor.shutdown(wait=True)
            self._projection_executor = None
        if self._projection_worker is not None:
            await self._projection_worker.close()
            self._projection_worker = None
        if self._datasets.cleaner is None:
            await self._datasets.close()
        elif not self._writer._closed and self._writer._close_future is None:
            await self._writer.run(self._datasets.close)
        if not self._writer._closed and self._writer._close_future is None:
            await self._writer.run(self._queries.projection_cache.close)
        await self._writer.close()

    async def delete_checkpoint(self, checkpoint_id):
        async def remove():
            async with self._checkpoint_lock():
                return await self._maintenance.run(self._maintenance.delete_checkpoint, checkpoint_id)
        return await self._writer.run(remove)

    async def delete_workflow(self, revision_id):
        await self._writer.run(
            self._maintenance.run,
            self._maintenance.delete_definition,
            "workflow_revision",
            revision_id,
        )

    async def delete_operator(self, definition_id):
        await self._writer.run(
            self._maintenance.run,
            self._maintenance.delete_definition,
            "operator_definition",
            definition_id,
        )

    async def purge_root_graph(self, root_invocation_id):
        await self._writer.run(
            self._maintenance.run, self._maintenance.purge, root_invocation_id
        )

    async def rebuild_projections(self):
        await self._writer.run(self._maintenance.run, self._maintenance.rebuild)

    async def gc_artifacts(self):
        return await self._writer.run(self._maintenance.run, self._maintenance.gc)

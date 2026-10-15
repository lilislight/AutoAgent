"""Host-facing asynchronous DatabaseSink and its owned I/O lifecycle."""

from __future__ import annotations

from autoagent.core.compiler.snapshot import WorkflowDefinitionSnapshot
from autoagent.core.operators import Operator
from autoagent.core.runtime.events import RuntimeEvent
from autoagent.core.runtime.graph_checkpoint import RuntimeGraphCheckpoint
from autoagent.core.runtime.user_events import UserEvent

from .maintenance import Maintenance
from .models import OperatorDefinition, PersistenceConfig, ReadView
from .queries import Queries
from .tracing import TraceQueries
from .timeline import TimelineQueries
from .writer import Writer
from .datasets import TraceDatasets
from .watching import TraceHeads


class DatabaseSink:
    """One Host-owned writer; async methods can be called from different loops.

    Create with ``await DatabaseSink.open(url, config=...)``. Register historical
    definitions before connecting this object to Core's event sink ports. Host
    owns runtime residency and must unload a stopped Graph before purging it.
    """

    def __init__(self, url: str, *, config: PersistenceConfig | None = None):
        self.config = config or PersistenceConfig()
        self._writer = Writer(url, self.config)
        self._queries = Queries(self._writer)
        self._traces = TraceQueries(self._queries)
        self._maintenance = Maintenance(self._writer)
        self._datasets = TraceDatasets(self._queries, self.config, url)
        self._llm_calls = None

    @classmethod
    async def open(cls, url: str, *, config: PersistenceConfig | None = None):
        result = cls(url, config=config)
        try:
            await result._writer.ready()
        except BaseException:
            await result.close()
            raise
        return result

    async def __aenter__(self):
        await self._writer.ready()
        return self

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
    ) -> None:
        if not isinstance(event, RuntimeEvent):
            raise TypeError("Expected RuntimeEvent.")

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
            await self._writer.accept(
                "runtime_event",
                event.id,
                event,
                binding=binding,
                strict_binding=operator_definition_id is not None,
            )

        await self._writer.run(accept)

    async def append_user_event(self, event: UserEvent) -> None:
        if not isinstance(event, UserEvent):
            raise TypeError("Expected UserEvent.")
        await self._writer.run(self._writer.accept, "user_event", event.id, event)

    async def record_llm_call(self, record) -> None:
        """Persist one model-call metadata update; no model SDK or query API required."""
        from autoagent.agent.recording import LLMCallRecord
        from .llm_calls import LLMCallStorage

        record = LLMCallRecord.model_validate(record)
        async def write():
            if self._llm_calls is None:
                self._llm_calls = LLMCallStorage(self._writer)
            await self._llm_calls.write(record)
        await self._writer.run(write)

    async def _checkpoint(self, checkpoint, anchor):
        if not isinstance(checkpoint, RuntimeGraphCheckpoint):
            raise TypeError("Expected complete RuntimeGraphCheckpoint.")

        # Core's full bundle validation includes ownership and causal closure.
        def validate():
            return RuntimeGraphCheckpoint.from_record(
                checkpoint.to_record()
            ).to_record()["digest"]

        async def save():
            identity = await self._writer.cpu(validate)
            await self._writer.accept(
                "checkpoint", identity, checkpoint, anchor=anchor, durable=True
            )
            return identity

        return await self._writer.run(save)

    async def save_checkpoint(self, checkpoint) -> str:
        return await self._checkpoint(checkpoint, False)

    async def import_checkpoint(self, checkpoint) -> str:
        return await self._checkpoint(checkpoint, True)

    async def get_checkpoint(self, checkpoint_id):
        record = await self._writer.run(
            self._queries.source_record, "checkpoint", checkpoint_id
        )
        return RuntimeGraphCheckpoint.from_record(record) if record else None

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

    async def trace_value(self, invocation_id, kind, identity, field):
        return await self._writer.run(self._traces.value, invocation_id, kind, identity, field)

    async def execution_stages(self, invocation_id, occurrence_id, **filters):
        return await self._writer.run(self._traces.stages, invocation_id, occurrence_id, **filters)

    async def execution_stage_value(self, invocation_id, occurrence_id, event_id, field):
        return await self._writer.run(self._traces.stage_value, invocation_id, occurrence_id, event_id, field)

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

    async def workflow_catalog(self, *, registered_ids=(), search="", limit=100, cursor=None):
        return await self._writer.run(self._queries.workflow_catalog, registered_ids, search, limit, cursor)

    async def workflow_revisions(self, workflow_id, *, prefix=None, limit=100, cursor=None):
        return await self._writer.run(self._queries.workflow_revisions, workflow_id, prefix, limit, cursor)

    async def list_operators(self, *, operator_id=None, limit=100, cursor=None):
        return await self._writer.run(
            self._queries.definitions, "operator_definition", operator_id, limit, cursor
        )

    async def list_checkpoints(self, root_session_id, *, limit=100, cursor=None):
        return await self._writer.run(
            self._queries.checkpoints, root_session_id, limit, cursor
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

    async def read_recovery_bundle(self, root_invocation_id):
        return await self._writer.run(self._queries.recovery_bundle, root_invocation_id)

    async def flush(self):
        await self._writer.run(self._writer.flush)

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
        if self._datasets.cleaner is None:
            await self._datasets.close()
        elif not self._writer._closed and self._writer._close_future is None:
            await self._writer.run(self._datasets.close)
        await self._writer.close()

    async def delete_checkpoint(self, checkpoint_id):
        await self._writer.run(
            self._maintenance.run, self._maintenance.delete_checkpoint, checkpoint_id
        )

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

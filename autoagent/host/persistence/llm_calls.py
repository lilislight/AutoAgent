"""Optional model-call metadata storage, separate from recovery/event projections."""

from sqlalchemy import Boolean, CheckConstraint, Index, Integer, MetaData, Table, select

from autoagent.agent.recording import LLMCallRecord

from .models import ConflictError, FormatError
from .schema import B, c


call_meta = MetaData()
llm_calls = Table(
    "llm_calls", call_meta,
    c("llm_call_id", nullable=False, pk=True),
    c("workflow_id"), c("workflow_revision_id"), c("workflow_version"),
    c("session_id"), c("invocation_id"), c("occurrence_id"), c("node_id"),
    c("operator_id"), c("operator_call_id"), c("operator_definition_id"), c("unit_index", Integer), c("attempt", Integer, False),
    c("kind", nullable=False), c("provider_id", nullable=False), c("protocol", nullable=False),
    c("requested_model", nullable=False), c("response_model"), c("streaming", Boolean, False),
    c("status", nullable=False), c("started_at_us", B, False), c("completed_at_us", B), c("duration_ns", B),
    c("usage_reported", Boolean, False),
    *(c(name, B) for name in ("input_tokens", "output_tokens", "total_tokens", "reasoning_tokens", "cache_read_tokens")),
    c("provider_response_id"), c("provider_request_id"), c("finish_reason"),
    c("error_type"), c("error_code"), c("status_code", Integer),
    CheckConstraint("status IN ('running','succeeded','failed','cancelled')"),
    CheckConstraint("kind IN ('generation','embedding')"),
    CheckConstraint("attempt >= 1"),
)
Index("llm_calls_workflow_revision_started", llm_calls.c.workflow_id, llm_calls.c.workflow_revision_id, llm_calls.c.started_at_us)
Index("llm_calls_invocation_operator", llm_calls.c.invocation_id, llm_calls.c.operator_call_id)
Index("llm_calls_provider_model_started", llm_calls.c.provider_id, llm_calls.c.requested_model, llm_calls.c.started_at_us)

_IDENTITY = (
    "llm_call_id", "workflow_id", "workflow_revision_id", "workflow_version", "session_id", "invocation_id",
    "occurrence_id", "node_id", "operator_id", "operator_call_id", "operator_definition_id", "unit_index", "attempt", "kind",
    "provider_id", "protocol", "requested_model", "streaming", "started_at_us",
)


class LLMCallStorage:
    """Durable/idempotent start-to-terminal writes on the Persistence I/O loop.

    The optional table is created on first use. It is not part of the canonical
    recovery schema, projection rebuild, Event ordering or accepted frontiers.
    Association IDs are retained as metadata without requiring those entities
    to have been flushed or even registered by standalone callers.
    """

    def __init__(self, writer):
        self.writer = writer
        self._ready = False

    async def write(self, record: LLMCallRecord):
        from sqlalchemy import inspect

        row = record.model_dump(mode="python")
        if row["operator_id"] is not None:
            binding = self.writer.operator_bindings.get(row["operator_id"])
            if binding is not None:
                if row["operator_definition_id"] is not None and row["operator_definition_id"] != binding:
                    raise ConflictError("LLM call Operator binding does not match the registered definition.")
                row["operator_definition_id"] = binding
        self.writer.check_open()
        async with self.writer.mutation:
            self.writer.check_open()
            async with self.writer.storage.engine.begin() as conn:
                if not self._ready:
                    await conn.run_sync(lambda sync: llm_calls.create(sync, checkfirst=True))
                    columns = await conn.run_sync(lambda sync: {item["name"] for item in inspect(sync).get_columns("llm_calls")})
                    if columns != set(llm_calls.columns.keys()):
                        raise FormatError("LLM call storage columns do not match the current schema.")
                previous = (await conn.execute(select(llm_calls).where(llm_calls.c.llm_call_id == record.llm_call_id))).mappings().first()
                if previous is not None:
                    if any(previous[key] != row[key] for key in _IDENTITY):
                        raise ConflictError("LLM call identity conflicts with an existing record.")
                    if previous["status"] != "running":
                        if row["status"] == "running" or dict(previous) == row:
                            self._ready = True
                            return
                        raise ConflictError("A terminal LLM call record is immutable.")
                    if row["status"] == "running" and dict(previous) != row:
                        raise ConflictError("A running LLM call record is immutable until completion.")
                from sqlalchemy.dialects.sqlite import insert as sqlite_insert
                from sqlalchemy.dialects.postgresql import insert as pg_insert
                statement = (sqlite_insert if self.writer.storage.sqlite else pg_insert)(llm_calls).values(**row)
                statement = statement.on_conflict_do_update(
                    index_elements=["llm_call_id"], set_={key: statement.excluded[key] for key in row if key != "llm_call_id"})
                await conn.execute(statement)
            self._ready = True

"""Optional model-call metadata storage, separate from recovery/event projections."""

from sqlalchemy import Boolean, CheckConstraint, Index, Integer, MetaData, Table, select, case, func, and_, or_, true

from autoagent.agent.recording import LLMCallRecord
from autoagent.agent.usage import TOKEN_FIELDS, TokenUsageQuery, TokenUsageSummary, timestamp_us
from autoagent.agent.cost import CostEstimate, LocalTokenPricing
from autoagent.core.money import units_usd

from .models import ConflictError, FormatError
from .schema import B, J, c


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
    c("billing_details", J, False), c("cost_units", B), c("cost_metadata", J, False),
    CheckConstraint("cost_units IS NULL OR cost_units >= 0"),
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


Index("llm_calls_started", llm_calls.c.started_at_us)
Index("llm_calls_workflow_started", llm_calls.c.workflow_id, llm_calls.c.started_at_us)
Index("llm_calls_session_started", llm_calls.c.session_id, llm_calls.c.started_at_us)
Index("llm_calls_invocation_node", llm_calls.c.invocation_id, llm_calls.c.node_id)

class LLMCallStorage:
    """One database/project ledger; writer ownership serializes admissions.

    Active IDs are transient. Running rows left by a previous writer are unknown
    liabilities after restart, rather than zero-token calls. No tokens are
    reserved: concurrent admitted requests can exceed a settled allowance.
    """

    def __init__(self, writer, *, pricing=None):
        self.writer = writer
        self._ready = False
        self.active = set()
        self.pricing = pricing if pricing is not None else LocalTokenPricing()

    async def _prepare(self, conn):
        from sqlalchemy import inspect
        if not self._ready:
            await conn.run_sync(lambda sync: llm_calls.create(sync, checkfirst=True))
            columns = await conn.run_sync(lambda sync: {item["name"] for item in inspect(sync).get_columns("llm_calls")})
            if columns != set(llm_calls.columns.keys()):
                raise FormatError("LLM call storage columns do not match the current schema.")

    def _row(self, record):
        row = record.model_dump(mode="python")
        if row["operator_id"] is not None:
            binding = self.writer.operator_bindings.get(row["operator_id"])
            if binding is not None:
                if row["operator_definition_id"] is not None and row["operator_definition_id"] != binding:
                    raise ConflictError("LLM call Operator binding does not match the registered definition.")
                row["operator_definition_id"] = binding
        return row

    async def _write(self, conn, record):
        row = self._row(record)
        previous = (await conn.execute(select(llm_calls).where(llm_calls.c.llm_call_id == record.llm_call_id))).mappings().first()
        if previous is not None:
            if any(previous[key] != row[key] for key in _IDENTITY):
                raise ConflictError("LLM call identity conflicts with an existing record.")
            if previous["status"] != "running":
                if record.cost_units is None and not record.cost_metadata:
                    row.update(cost_units=previous["cost_units"], cost_metadata=previous["cost_metadata"])
                if row["status"] == "running" or dict(previous) == row:
                    return False
                raise ConflictError("A terminal LLM call record is immutable.")
            if row["status"] == "running" and dict(previous) != row:
                raise ConflictError("A running LLM call record is immutable until completion.")
        if record.status != "running" and record.cost_units is None and not record.cost_metadata:
            try:
                estimate = self.pricing.calculate(record)
                if not isinstance(estimate, CostEstimate):
                    raise TypeError("CostCalculator must return CostEstimate.")
                settled = LLMCallRecord.model_validate({**record.model_dump(mode="python"),
                    "cost_units": estimate.units, "cost_metadata": estimate.metadata})
                row.update(cost_units=settled.cost_units, cost_metadata=settled.cost_metadata)
            except Exception as error:
                row.update(cost_units=None, cost_metadata={"currency": "USD", "reason": "calculation_failed", "error_type": type(error).__name__})
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        statement = (sqlite_insert if self.writer.storage.sqlite else pg_insert)(llm_calls).values(**row)
        statement = statement.on_conflict_do_update(index_elements=["llm_call_id"],
            set_={key: statement.excluded[key] for key in row if key != "llm_call_id"})
        await conn.execute(statement)
        return True

    async def write(self, record: LLMCallRecord):
        self.writer.check_open()
        try:
            async with self.writer.mutation:
                self.writer.check_open()
                async with self.writer.storage.transaction() as conn:
                    await self._prepare(conn)
                    changed = await self._write(conn, record)
                self._ready = True
                if changed and record.status == "running":
                    self.active.add(record.llm_call_id)
        finally:
            if record.status != "running":
                self.active.discard(record.llm_call_id)

    async def admit(self, record, checks):
        from autoagent.core.usage import UsageAccountingError
        if record.status != "running":
            raise ValueError("Admission requires a new running call record.")
        self.writer.check_open()
        async with self.writer.mutation:
            self.writer.check_open()
            async with self.writer.storage.transaction() as conn:
                await self._prepare(conn)
                summaries, duplicate = await self._summaries(conn, tuple(check.query for check in checks), record.llm_call_id)
                if duplicate:
                    raise UsageAccountingError("LLM call was already admitted.")
                for check, summary in zip(checks, summaries, strict=True):
                    check.validate(summary)
                from sqlalchemy.dialects.sqlite import insert as sqlite_insert
                from sqlalchemy.dialects.postgresql import insert as pg_insert
                statement = (sqlite_insert if self.writer.storage.sqlite else pg_insert)(llm_calls).values(**self._row(record))
                statement = statement.on_conflict_do_nothing(index_elements=["llm_call_id"]).returning(llm_calls.c.llm_call_id)
                if (await conn.execute(statement)).scalar_one_or_none() is None:
                    raise UsageAccountingError("LLM call was already admitted.")
            self._ready = True
            self.active.add(record.llm_call_id)

    async def summary(self, query: TokenUsageQuery):
        from sqlalchemy import inspect
        async with self.writer.storage.engine.connect() as conn:
            exists = await conn.run_sync(lambda sync: inspect(sync).has_table("llm_calls"))
            if not exists:
                return TokenUsageSummary()
            return await self._summary(conn, query)

    @staticmethod
    def _predicate(query):
        clauses = []
        for key in ("invocation_id", "node_id", "session_id", "workflow_id", "workflow_revision_id", "kind", "provider_id", "requested_model"):
            value = getattr(query, key)
            if value is not None:
                clauses.append(llm_calls.c[key] == value)
        if query.start is not None:
            clauses.append(llm_calls.c.started_at_us >= timestamp_us(query.start))
        if query.end is not None:
            clauses.append(llm_calls.c.started_at_us < timestamp_us(query.end))
        return and_(*clauses) if clauses else true()

    async def _summary(self, conn, query):
        summaries, _ = await self._summaries(conn, (query,))
        return summaries[0]

    async def _summaries(self, conn, queries, duplicate_id=None):
        """One conditional aggregate scans the union of every applicable scope."""
        predicates = [self._predicate(query) for query in queries]
        running = llm_calls.c.status == "running"
        terminal = llm_calls.c.status != "running"
        orphaned = running & llm_calls.c.llm_call_id.not_in(tuple(self.active))
        columns = []
        for index, match in enumerate(predicates):
            def count(condition, name):
                return func.sum(case((and_(match, condition), 1), else_=0)).label(f"s{index}_{name}")
            columns.extend((count(true(), "call_count"), count(running, "pending_calls"),
                            count(orphaned, "unsettled_calls"),
                            count(terminal & llm_calls.c.usage_reported.is_(False), "missing_usage_calls")))
            for metric in TOKEN_FIELDS:
                columns.extend((func.sum(case((and_(match, terminal), llm_calls.c[metric]), else_=None)).label(f"s{index}_{metric}"),
                                count(terminal & llm_calls.c[metric].is_(None), "missing_" + metric)))
            columns.extend((func.sum(case((and_(match, terminal), llm_calls.c.cost_units), else_=None)).label(f"s{index}_cost_units"),
                            count(terminal & llm_calls.c.cost_units.is_(None), "missing_cost_calls")))
        if duplicate_id is not None:
            duplicate = llm_calls.c.llm_call_id == duplicate_id
            columns.append(func.max(case((duplicate, 1), else_=0)).label("duplicate"))
            predicates.append(duplicate)
        row = (await conn.execute(select(*columns).where(or_(*predicates)))).mappings().one()
        summaries = []
        for index in range(len(queries)):
            def get(name):
                return row[f"s{index}_{name}"]
            result = {name: int(get(name) or 0) for name in ("call_count", "pending_calls", "unsettled_calls", "missing_usage_calls")}
            result.update({metric: (int(get(metric)) if get(metric) is not None else 0 if not get("call_count") else None)
                           for metric in TOKEN_FIELDS})
            result["missing_tokens"] = {metric: int(get("missing_" + metric) or 0) for metric in TOKEN_FIELDS}
            result["cost_usd"] = units_usd(int(get("cost_units"))) if get("cost_units") is not None else "0" if not get("call_count") else None
            result["missing_cost_calls"] = int(get("missing_cost_calls") or 0)
            summaries.append(TokenUsageSummary(**result))
        return tuple(summaries), bool(row["duplicate"]) if duplicate_id is not None else False

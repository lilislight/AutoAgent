from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class RuntimeDatabaseBase(DeclarativeBase):
    """Shared SQLAlchemy metadata imported by RuntimeStore and Alembic."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class WorkflowVersionRow(RuntimeDatabaseBase):
    """Immutable compiled structure and known Operator manifests."""

    __tablename__ = "workflow_versions"
    __table_args__ = (
        UniqueConstraint(
            "namespace",
            "workflow_id",
            "definition_hash",
            "operator_manifest_hash",
            name="uq_workflow_versions_identity",
        ),
        Index("ix_workflow_versions_lookup", "namespace", "workflow_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    namespace: Mapped[str] = mapped_column(String(255), nullable=False)
    workflow_id: Mapped[str] = mapped_column(String(255), nullable=False)
    workflow_version_json: Mapped[str] = mapped_column(Text, nullable=False)
    definition_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    operator_manifest_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    ir_version: Mapped[str] = mapped_column(String(64), nullable=False)
    compiler_version: Mapped[str] = mapped_column(String(64), nullable=False)
    snapshot_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)


class SessionRow(RuntimeDatabaseBase):
    """Long-lived Session identity and cross-invocation user context."""

    __tablename__ = "sessions"
    __table_args__ = (
        UniqueConstraint(
            "namespace",
            "workflow_id",
            "session_key",
            name="uq_sessions_external_identity",
        ),
        Index("ix_sessions_lookup", "namespace", "workflow_id", "session_key"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    namespace: Mapped[str] = mapped_column(String(255), nullable=False)
    workflow_id: Mapped[str] = mapped_column(String(255), nullable=False)
    session_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    context_json: Mapped[str] = mapped_column(Text, nullable=False)
    # This is an intentionally non-FK pointer. Invocation already owns the
    # sessions FK, and avoiding a circular FK keeps creation/deletion portable.
    current_invocation_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    created_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)


class InvocationRow(RuntimeDatabaseBase):
    """One request state, scheduler cursor, contexts, and final result."""

    __tablename__ = "invocations"
    __table_args__ = (
        Index("ix_invocations_session_created", "session_id", "created_at_ms"),
        Index("ix_invocations_state", "state"),
        # Enforce the App invariant even if concurrent requests race admission.
        Index(
            "uq_invocations_one_active_per_session",
            "session_id",
            unique=True,
            sqlite_where=text("state IN ('created', 'running', 'waiting')"),
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    session_id: Mapped[str] = mapped_column(
        ForeignKey("sessions.id", ondelete="CASCADE"),
        nullable=False,
    )
    workflow_version_id: Mapped[str] = mapped_column(
        ForeignKey("workflow_versions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    workflow_id: Mapped[str] = mapped_column(String(255), nullable=False)
    workflow_version_json: Mapped[str] = mapped_column(Text, nullable=False)
    definition_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    operator_manifest_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    entry_node_id: Mapped[str] = mapped_column(String(255), nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    input_json: Mapped[str] = mapped_column(Text, nullable=False)
    context_json: Mapped[str] = mapped_column(Text, nullable=False)
    result_json: Mapped[str] = mapped_column(Text, nullable=False)
    scheduler_json: Mapped[str] = mapped_column(Text, nullable=False)
    error_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)


class NodeExecutionRow(RuntimeDatabaseBase):
    """Ordered logical node history; repeated loop executions get separate rows."""

    __tablename__ = "node_executions"
    __table_args__ = (
        UniqueConstraint(
            "invocation_id",
            "sequence",
            name="uq_node_executions_invocation_sequence",
        ),
        Index("ix_node_executions_invocation_node", "invocation_id", "node_id"),
        Index("ix_node_executions_state", "state"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    invocation_id: Mapped[str] = mapped_column(
        ForeignKey("invocations.id", ondelete="CASCADE"),
        nullable=False,
    )
    node_id: Mapped[str] = mapped_column(String(255), nullable=False)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    input_json: Mapped[str] = mapped_column(Text, nullable=False)
    output_json: Mapped[str] = mapped_column(Text, nullable=False)
    error_json: Mapped[str] = mapped_column(Text, nullable=False)
    idempotency_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    recovery_of_execution_id: Mapped[str | None] = mapped_column(String(36))
    recovery_attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    incoming_activations_json: Mapped[str] = mapped_column(Text, nullable=False)
    edge_evaluations_json: Mapped[str] = mapped_column(Text, nullable=False)
    resource_usage_json: Mapped[str] = mapped_column(Text, nullable=False)
    started_at_ms: Mapped[int | None] = mapped_column(BigInteger)
    ended_at_ms: Mapped[int | None] = mapped_column(BigInteger)
    created_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)


class OperatorCallRow(RuntimeDatabaseBase):
    """Concrete retry/fallback/map/replication call and recovery manifest."""

    __tablename__ = "operator_calls"
    __table_args__ = (
        UniqueConstraint(
            "node_execution_id",
            "call_no",
            name="uq_operator_calls_execution_call_no",
        ),
        Index("ix_operator_calls_operator", "operator_id"),
        Index("ix_operator_calls_state", "state"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    node_execution_id: Mapped[str] = mapped_column(
        ForeignKey("node_executions.id", ondelete="CASCADE"),
        nullable=False,
    )
    operator_id: Mapped[str] = mapped_column(String(255), nullable=False)
    operator_version_json: Mapped[str] = mapped_column(Text, nullable=False)
    manifest_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    operator_manifest_json: Mapped[str] = mapped_column(Text, nullable=False)
    call_no: Mapped[int] = mapped_column(Integer, nullable=False)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    item_index: Mapped[int | None] = mapped_column(Integer)
    replica_index: Mapped[int | None] = mapped_column(Integer)
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    input_json: Mapped[str] = mapped_column(Text, nullable=False)
    output_json: Mapped[str] = mapped_column(Text, nullable=False)
    error_json: Mapped[str] = mapped_column(Text, nullable=False)
    resource_usage_json: Mapped[str] = mapped_column(Text, nullable=False)
    started_at_ms: Mapped[int | None] = mapped_column(BigInteger)
    ended_at_ms: Mapped[int | None] = mapped_column(BigInteger)
    created_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)


class RuntimeEventRow(RuntimeDatabaseBase):
    """Immutable event stream ordered inside one Session."""

    __tablename__ = "runtime_events"
    __table_args__ = (
        UniqueConstraint(
            "session_id",
            "sequence",
            name="uq_runtime_events_session_sequence",
        ),
        Index("ix_runtime_events_invocation_sequence", "invocation_id", "sequence"),
        Index("ix_runtime_events_session_sequence", "session_id", "sequence"),
        Index("ix_runtime_events_type", "type"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    session_id: Mapped[str] = mapped_column(
        ForeignKey("sessions.id", ondelete="CASCADE"),
        nullable=False,
    )
    invocation_id: Mapped[str] = mapped_column(
        ForeignKey("invocations.id", ondelete="CASCADE"),
        nullable=False,
    )
    namespace: Mapped[str] = mapped_column(String(255), nullable=False)
    workflow_id: Mapped[str] = mapped_column(String(255), nullable=False)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    type: Mapped[str] = mapped_column(String(128), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    entity_id: Mapped[str | None] = mapped_column(String(255))
    node_id: Mapped[str | None] = mapped_column(String(255))
    edge_id: Mapped[str | None] = mapped_column(String(255))
    occurred_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    channel: Mapped[str] = mapped_column(String(32), nullable=False)
    visibility: Mapped[str] = mapped_column(String(32), nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)

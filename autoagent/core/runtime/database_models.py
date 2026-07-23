from __future__ import annotations

from sqlalchemy import BigInteger, Index, Integer, MetaData, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class RuntimeDatabaseBase(DeclarativeBase):
    """Dialect-neutral V1 Runtime schema for SQLite and PostgreSQL."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class WorkflowVersionRow(RuntimeDatabaseBase):
    __tablename__ = "workflow_versions"
    __table_args__ = (
        UniqueConstraint(
            "namespace", "workflow_id", "definition_hash",
            "operator_manifest_hash", name="uq_workflow_versions_identity",
        ),
        Index("ix_workflow_versions_lookup", "namespace", "workflow_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    namespace: Mapped[str] = mapped_column(String(255), nullable=False)
    workflow_id: Mapped[str] = mapped_column(String(255), nullable=False)
    definition_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    operator_manifest_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    snapshot_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)


class SessionRow(RuntimeDatabaseBase):
    __tablename__ = "sessions"
    __table_args__ = (
        UniqueConstraint(
            "namespace", "workflow_id", "session_key",
            name="uq_sessions_external_identity",
        ),
        Index("ix_sessions_lookup", "namespace", "workflow_id", "session_key"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    namespace: Mapped[str] = mapped_column(String(255), nullable=False)
    workflow_id: Mapped[str] = mapped_column(String(255), nullable=False)
    session_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    state_json: Mapped[str] = mapped_column(Text, nullable=False)
    current_invocation_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    created_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)


class InvocationRow(RuntimeDatabaseBase):
    __tablename__ = "invocations"
    __table_args__ = (
        Index("ix_invocations_session", "session_id", "created_at_ms"),
        Index("ix_invocations_state", "state"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    session_id: Mapped[str] = mapped_column(String(36), nullable=False)
    workflow_id: Mapped[str] = mapped_column(String(255), nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    execution_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    head_sequence: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    durable_sequence: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # Materialized latest image is a cache. Snapshots + boundary events remain
    # the authoritative rebuild history.
    state_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)


class RuntimeEventRow(RuntimeDatabaseBase):
    __tablename__ = "runtime_events"
    __table_args__ = (
        UniqueConstraint("invocation_id", "sequence", name="uq_runtime_event_sequence"),
        Index("ix_runtime_events_session", "session_id", "invocation_id", "sequence"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    namespace: Mapped[str] = mapped_column(String(255), nullable=False)
    workflow_id: Mapped[str] = mapped_column(String(255), nullable=False)
    session_id: Mapped[str] = mapped_column(String(36), nullable=False)
    invocation_id: Mapped[str] = mapped_column(String(36), nullable=False)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    role: Mapped[str] = mapped_column(String(16), nullable=False)
    boundary: Mapped[str | None] = mapped_column(String(64), nullable=True)
    commit_id: Mapped[str] = mapped_column(String(36), nullable=False)
    type: Mapped[str] = mapped_column(String(128), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    occurred_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    event_json: Mapped[str] = mapped_column(Text, nullable=False)


class RuntimeSnapshotRow(RuntimeDatabaseBase):
    __tablename__ = "runtime_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "invocation_id", "through_sequence",
            name="uq_runtime_snapshot_sequence",
        ),
        Index("ix_runtime_snapshots_lookup", "invocation_id", "through_sequence"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    namespace: Mapped[str] = mapped_column(String(255), nullable=False)
    workflow_id: Mapped[str] = mapped_column(String(255), nullable=False)
    session_id: Mapped[str] = mapped_column(String(36), nullable=False)
    invocation_id: Mapped[str] = mapped_column(String(36), nullable=False)
    through_sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    snapshot_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)


class RuntimeProjectionCheckpointRow(RuntimeDatabaseBase):
    """Disposable Trace/UI cache retained until the later UI refactor."""

    __tablename__ = "runtime_projection_checkpoints"
    __table_args__ = (
        UniqueConstraint(
            "invocation_id", "through_sequence",
            name="uq_runtime_projection_checkpoint",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    invocation_id: Mapped[str] = mapped_column(String(36), nullable=False)
    through_sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    projection_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)

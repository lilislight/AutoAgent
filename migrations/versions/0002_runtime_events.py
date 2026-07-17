"""Use UTC millisecond timestamps and add immutable Runtime Events.

Revision ID: 0002_runtime_events
Revises: 0001_runtime_tables
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0002_runtime_events"
down_revision: Union[str, Sequence[str], None] = "0001_runtime_tables"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_index("ix_invocations_session_created", table_name="invocations")

    _upgrade_timestamps("workflow_versions", required=("created_at",))
    _upgrade_timestamps(
        "sessions",
        required=("created_at", "updated_at"),
    )
    _upgrade_timestamps(
        "invocations",
        required=("created_at", "updated_at"),
    )
    _upgrade_timestamps(
        "node_executions",
        required=("created_at", "updated_at"),
        optional=("started_at", "ended_at"),
    )
    _upgrade_timestamps(
        "operator_calls",
        required=("created_at", "updated_at"),
        optional=("started_at", "ended_at"),
    )

    op.create_index(
        "ix_invocations_session_created",
        "invocations",
        ["session_id", "created_at_ms"],
    )
    op.create_table(
        "runtime_events",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "session_id",
            sa.String(36),
            sa.ForeignKey("sessions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "invocation_id",
            sa.String(36),
            sa.ForeignKey("invocations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("namespace", sa.String(255), nullable=False),
        sa.Column("workflow_id", sa.String(255), nullable=False),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("type", sa.String(128), nullable=False),
        sa.Column("entity_type", sa.String(64), nullable=False),
        sa.Column("entity_id", sa.String(255), nullable=True),
        sa.Column("node_id", sa.String(255), nullable=True),
        sa.Column("edge_id", sa.String(255), nullable=True),
        sa.Column("occurred_at_ms", sa.BigInteger(), nullable=False),
        sa.Column("channel", sa.String(32), nullable=False),
        sa.Column("visibility", sa.String(32), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.UniqueConstraint(
            "session_id",
            "sequence",
            name="uq_runtime_events_session_sequence",
        ),
    )
    op.create_index(
        "ix_runtime_events_invocation_sequence",
        "runtime_events",
        ["invocation_id", "sequence"],
    )
    op.create_index(
        "ix_runtime_events_session_sequence",
        "runtime_events",
        ["session_id", "sequence"],
    )
    op.create_index("ix_runtime_events_type", "runtime_events", ["type"])


def downgrade() -> None:
    op.drop_index("ix_runtime_events_type", table_name="runtime_events")
    op.drop_index(
        "ix_runtime_events_session_sequence",
        table_name="runtime_events",
    )
    op.drop_index(
        "ix_runtime_events_invocation_sequence",
        table_name="runtime_events",
    )
    op.drop_table("runtime_events")
    op.drop_index("ix_invocations_session_created", table_name="invocations")

    _downgrade_timestamps("workflow_versions", required=("created_at",))
    _downgrade_timestamps(
        "sessions",
        required=("created_at", "updated_at"),
    )
    _downgrade_timestamps(
        "invocations",
        required=("created_at", "updated_at"),
    )
    _downgrade_timestamps(
        "node_executions",
        required=("created_at", "updated_at"),
        optional=("started_at", "ended_at"),
    )
    _downgrade_timestamps(
        "operator_calls",
        required=("created_at", "updated_at"),
        optional=("started_at", "ended_at"),
    )
    op.create_index(
        "ix_invocations_session_created",
        "invocations",
        ["session_id", "created_at"],
    )


def _upgrade_timestamps(
    table: str,
    *,
    required: tuple[str, ...],
    optional: tuple[str, ...] = (),
) -> None:
    columns = required + optional
    with op.batch_alter_table(table) as batch:
        for column in columns:
            batch.add_column(sa.Column(f"{column}_ms", sa.BigInteger(), nullable=True))

    for column in columns:
        op.execute(
            sa.text(
                f"UPDATE {table} SET {column}_ms = "
                f"CAST((julianday({column}) - 2440587.5) * 86400000 AS BIGINT) "
                f"WHERE {column} IS NOT NULL"
            )
        )

    with op.batch_alter_table(table) as batch:
        for column in required:
            batch.alter_column(f"{column}_ms", existing_type=sa.BigInteger(), nullable=False)
        for column in columns:
            batch.drop_column(column)


def _downgrade_timestamps(
    table: str,
    *,
    required: tuple[str, ...],
    optional: tuple[str, ...] = (),
) -> None:
    columns = required + optional
    with op.batch_alter_table(table) as batch:
        for column in columns:
            batch.add_column(
                sa.Column(column, sa.DateTime(timezone=True), nullable=True)
            )

    for column in columns:
        op.execute(
            sa.text(
                f"UPDATE {table} SET {column} = "
                f"datetime({column}_ms / 1000.0, 'unixepoch') "
                f"WHERE {column}_ms IS NOT NULL"
            )
        )

    with op.batch_alter_table(table) as batch:
        for column in required:
            batch.alter_column(
                column,
                existing_type=sa.DateTime(timezone=True),
                nullable=False,
            )
        for column in columns:
            batch.drop_column(f"{column}_ms")

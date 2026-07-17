"""Create V1 RuntimeStore tables.

Revision ID: 0001_runtime_tables
Revises: None
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0001_runtime_tables"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "workflow_versions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("namespace", sa.String(255), nullable=False),
        sa.Column("workflow_id", sa.String(255), nullable=False),
        sa.Column("workflow_version_json", sa.Text(), nullable=False),
        sa.Column("definition_hash", sa.String(64), nullable=False),
        sa.Column("operator_manifest_hash", sa.String(64), nullable=False),
        sa.Column("ir_version", sa.String(64), nullable=False),
        sa.Column("compiler_version", sa.String(64), nullable=False),
        sa.Column("snapshot_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "namespace",
            "workflow_id",
            "definition_hash",
            "operator_manifest_hash",
            name="uq_workflow_versions_identity",
        ),
    )
    op.create_index(
        "ix_workflow_versions_lookup",
        "workflow_versions",
        ["namespace", "workflow_id"],
    )

    op.create_table(
        "sessions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("namespace", sa.String(255), nullable=False),
        sa.Column("workflow_id", sa.String(255), nullable=False),
        sa.Column("session_key", sa.String(512), nullable=True),
        sa.Column("context_json", sa.Text(), nullable=False),
        sa.Column("current_invocation_id", sa.String(36), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "namespace",
            "workflow_id",
            "session_key",
            name="uq_sessions_external_identity",
        ),
    )
    op.create_index(
        "ix_sessions_lookup",
        "sessions",
        ["namespace", "workflow_id", "session_key"],
    )

    op.create_table(
        "invocations",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "session_id",
            sa.String(36),
            sa.ForeignKey("sessions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "workflow_version_id",
            sa.String(36),
            sa.ForeignKey("workflow_versions.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("workflow_id", sa.String(255), nullable=False),
        sa.Column("workflow_version_json", sa.Text(), nullable=False),
        sa.Column("definition_hash", sa.String(64), nullable=False),
        sa.Column("operator_manifest_hash", sa.String(64), nullable=False),
        sa.Column("entry_node_id", sa.String(255), nullable=False),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("input_json", sa.Text(), nullable=False),
        sa.Column("context_json", sa.Text(), nullable=False),
        sa.Column("result_json", sa.Text(), nullable=False),
        sa.Column("scheduler_json", sa.Text(), nullable=False),
        sa.Column("error_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_invocations_session_created",
        "invocations",
        ["session_id", "created_at"],
    )
    op.create_index("ix_invocations_state", "invocations", ["state"])
    op.create_index(
        "uq_invocations_one_active_per_session",
        "invocations",
        ["session_id"],
        unique=True,
        sqlite_where=sa.text("state IN ('created', 'running', 'waiting')"),
    )

    op.create_table(
        "node_executions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "invocation_id",
            sa.String(36),
            sa.ForeignKey("invocations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("node_id", sa.String(255), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("input_json", sa.Text(), nullable=False),
        sa.Column("output_json", sa.Text(), nullable=False),
        sa.Column("error_json", sa.Text(), nullable=False),
        sa.Column("idempotency_key", sa.String(512), nullable=True),
        sa.Column("recovery_of_execution_id", sa.String(36), nullable=True),
        sa.Column("recovery_attempt", sa.Integer(), nullable=False),
        sa.Column("incoming_activations_json", sa.Text(), nullable=False),
        sa.Column("edge_evaluations_json", sa.Text(), nullable=False),
        sa.Column("resource_usage_json", sa.Text(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "invocation_id",
            "sequence",
            name="uq_node_executions_invocation_sequence",
        ),
    )
    op.create_index(
        "ix_node_executions_invocation_node",
        "node_executions",
        ["invocation_id", "node_id"],
    )
    op.create_index("ix_node_executions_state", "node_executions", ["state"])

    op.create_table(
        "operator_calls",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "node_execution_id",
            sa.String(36),
            sa.ForeignKey("node_executions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("operator_id", sa.String(255), nullable=False),
        sa.Column("operator_version_json", sa.Text(), nullable=False),
        sa.Column("manifest_hash", sa.String(64), nullable=True),
        sa.Column("operator_manifest_json", sa.Text(), nullable=False),
        sa.Column("call_no", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("item_index", sa.Integer(), nullable=True),
        sa.Column("replica_index", sa.Integer(), nullable=True),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("input_json", sa.Text(), nullable=False),
        sa.Column("output_json", sa.Text(), nullable=False),
        sa.Column("error_json", sa.Text(), nullable=False),
        sa.Column("resource_usage_json", sa.Text(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "node_execution_id",
            "call_no",
            name="uq_operator_calls_execution_call_no",
        ),
    )
    op.create_index("ix_operator_calls_operator", "operator_calls", ["operator_id"])
    op.create_index("ix_operator_calls_state", "operator_calls", ["state"])


def downgrade() -> None:
    op.drop_index("ix_operator_calls_state", table_name="operator_calls")
    op.drop_index("ix_operator_calls_operator", table_name="operator_calls")
    op.drop_table("operator_calls")
    op.drop_index("ix_node_executions_state", table_name="node_executions")
    op.drop_index("ix_node_executions_invocation_node", table_name="node_executions")
    op.drop_table("node_executions")
    op.drop_index(
        "uq_invocations_one_active_per_session",
        table_name="invocations",
    )
    op.drop_index("ix_invocations_state", table_name="invocations")
    op.drop_index("ix_invocations_session_created", table_name="invocations")
    op.drop_table("invocations")
    op.drop_index("ix_sessions_lookup", table_name="sessions")
    op.drop_table("sessions")
    op.drop_index("ix_workflow_versions_lookup", table_name="workflow_versions")
    op.drop_table("workflow_versions")

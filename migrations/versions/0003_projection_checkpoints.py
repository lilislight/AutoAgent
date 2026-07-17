"""Add rebuildable Runtime projection checkpoints.

Revision ID: 0003_projection_checkpoints
Revises: 0002_runtime_events
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0003_projection_checkpoints"
down_revision: Union[str, Sequence[str], None] = "0002_runtime_events"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "runtime_projection_checkpoints",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "invocation_id",
            sa.String(36),
            sa.ForeignKey("invocations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("through_sequence", sa.BigInteger(), nullable=False),
        sa.Column("projection_json", sa.Text(), nullable=False),
        sa.Column("created_at_ms", sa.BigInteger(), nullable=False),
        sa.UniqueConstraint(
            "invocation_id",
            "through_sequence",
            name="uq_runtime_projection_checkpoints_cursor",
        ),
    )
    op.create_index(
        "ix_runtime_projection_checkpoints_latest",
        "runtime_projection_checkpoints",
        ["invocation_id", "through_sequence"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_runtime_projection_checkpoints_latest",
        table_name="runtime_projection_checkpoints",
    )
    op.drop_table("runtime_projection_checkpoints")

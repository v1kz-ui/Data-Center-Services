"""Add connector-level snapshot scope and checkpoint uniqueness.

Revision ID: 20260418_0017
Revises: 20260418_0016
Create Date: 2026-04-18 21:35:00
"""

import sqlalchemy as sa
from alembic import op

revision = "20260418_0017"
down_revision = "20260418_0016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "source_snapshot",
        sa.Column("connector_key", sa.String(length=128), nullable=True),
    )
    op.create_index(
        "ix_source_snapshot_connector_scope_ts",
        "source_snapshot",
        ["connector_key", "snapshot_ts"],
    )

    with op.batch_alter_table("source_refresh_checkpoint") as batch_op:
        batch_op.drop_constraint(
            "uq_source_refresh_checkpoint_source_metro",
            type_="unique",
        )
        batch_op.create_unique_constraint(
            "uq_source_refresh_checkpoint_connector_key",
            ["connector_key"],
        )


def downgrade() -> None:
    with op.batch_alter_table("source_refresh_checkpoint") as batch_op:
        batch_op.drop_constraint(
            "uq_source_refresh_checkpoint_connector_key",
            type_="unique",
        )
        batch_op.create_unique_constraint(
            "uq_source_refresh_checkpoint_source_metro",
            ["source_id", "metro_id"],
        )

    op.drop_index(
        "ix_source_snapshot_connector_scope_ts",
        table_name="source_snapshot",
    )
    op.drop_column("source_snapshot", "connector_key")

"""Phase 7 UAT distribution packets and recipients.

Revision ID: 20260415_0009
Revises: 20260415_0008
Create Date: 2026-04-15 17:20:00
"""

import sqlalchemy as sa
from alembic import op

revision = "20260415_0009"
down_revision = "20260415_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "uat_distribution_packet",
        sa.Column("packet_id", sa.Uuid(), nullable=False),
        sa.Column("snapshot_id", sa.Uuid(), nullable=False),
        sa.Column("packet_name", sa.String(length=255), nullable=False),
        sa.Column("channel", sa.String(length=64), nullable=False),
        sa.Column("distribution_status", sa.String(length=32), nullable=False),
        sa.Column("ready_to_send", sa.Boolean(), nullable=False),
        sa.Column("subject_line", sa.String(length=255), nullable=False),
        sa.Column("summary_excerpt", sa.Text(), nullable=False),
        sa.Column("briefing_body", sa.Text(), nullable=False),
        sa.Column("distribution_notes", sa.Text(), nullable=True),
        sa.Column("created_by", sa.String(length=255), nullable=False),
        sa.Column("distributed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["snapshot_id"],
            ["uat_handoff_snapshot.snapshot_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("packet_id"),
        sa.UniqueConstraint(
            "snapshot_id",
            "packet_name",
            name="uq_uat_distribution_packet_snapshot_packet_name",
        ),
    )
    op.create_index(
        "ix_uat_distribution_packet_snapshot_id",
        "uat_distribution_packet",
        ["snapshot_id"],
        unique=False,
    )
    op.create_index(
        "ix_uat_distribution_packet_distribution_status",
        "uat_distribution_packet",
        ["distribution_status"],
        unique=False,
    )
    op.create_index(
        "ix_uat_distribution_packet_ready_to_send",
        "uat_distribution_packet",
        ["ready_to_send"],
        unique=False,
    )

    op.create_table(
        "uat_distribution_recipient",
        sa.Column("recipient_id", sa.Uuid(), nullable=False),
        sa.Column("packet_id", sa.Uuid(), nullable=False),
        sa.Column("recipient_name", sa.String(length=255), nullable=False),
        sa.Column("recipient_role", sa.String(length=255), nullable=True),
        sa.Column("recipient_organization", sa.String(length=255), nullable=True),
        sa.Column("recipient_contact", sa.String(length=255), nullable=True),
        sa.Column("required_for_ack", sa.Boolean(), nullable=False),
        sa.Column("delivery_status", sa.String(length=32), nullable=False),
        sa.Column("delivery_notes", sa.Text(), nullable=True),
        sa.Column("acknowledgement_notes", sa.Text(), nullable=True),
        sa.Column("acknowledged_by", sa.String(length=255), nullable=True),
        sa.Column("recorded_by", sa.String(length=255), nullable=False),
        sa.Column("last_status_updated_by", sa.String(length=255), nullable=False),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["packet_id"],
            ["uat_distribution_packet.packet_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("recipient_id"),
    )
    op.create_index(
        "ix_uat_distribution_recipient_packet_id",
        "uat_distribution_recipient",
        ["packet_id"],
        unique=False,
    )
    op.create_index(
        "ix_uat_distribution_recipient_delivery_status",
        "uat_distribution_recipient",
        ["delivery_status"],
        unique=False,
    )
    op.create_index(
        "ix_uat_distribution_recipient_required_for_ack",
        "uat_distribution_recipient",
        ["required_for_ack"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_uat_distribution_recipient_required_for_ack",
        table_name="uat_distribution_recipient",
    )
    op.drop_index(
        "ix_uat_distribution_recipient_delivery_status",
        table_name="uat_distribution_recipient",
    )
    op.drop_index(
        "ix_uat_distribution_recipient_packet_id",
        table_name="uat_distribution_recipient",
    )
    op.drop_table("uat_distribution_recipient")

    op.drop_index(
        "ix_uat_distribution_packet_ready_to_send",
        table_name="uat_distribution_packet",
    )
    op.drop_index(
        "ix_uat_distribution_packet_distribution_status",
        table_name="uat_distribution_packet",
    )
    op.drop_index(
        "ix_uat_distribution_packet_snapshot_id",
        table_name="uat_distribution_packet",
    )
    op.drop_table("uat_distribution_packet")

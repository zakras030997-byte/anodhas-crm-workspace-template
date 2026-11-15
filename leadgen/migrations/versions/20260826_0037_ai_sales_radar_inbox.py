"""Add the channel-neutral AI Sales Radar lead inbox.

Revision ID: 20260826_0037
Revises: 20260818_0036
"""

from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20260826_0037"
down_revision: str | None = "20260818_0036"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_match_traces_identity_context",
        "match_traces",
        ["id", "opportunity_id", "search_profile_id", "profile_revision"],
    )
    op.create_table(
        "commercial_score_snapshots",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("opportunity_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("policy_version", sa.String(64), nullable=False),
        sa.Column("input_sha256", sa.String(64), nullable=False),
        sa.Column("total_score", sa.SmallInteger(), nullable=False),
        sa.Column("band", sa.String(8), nullable=False),
        sa.Column("breakdown", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "policy_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
            name=op.f("ck_commercial_score_snapshots_policy_version_valid"),
        ),
        sa.CheckConstraint(
            "input_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_commercial_score_snapshots_input_sha256_valid"),
        ),
        sa.CheckConstraint(
            "total_score BETWEEN 0 AND 100",
            name=op.f("ck_commercial_score_snapshots_total_score_bounded"),
        ),
        sa.CheckConstraint(
            "band IN ('hot', 'warm', 'low')",
            name=op.f("ck_commercial_score_snapshots_band_valid"),
        ),
        sa.CheckConstraint(
            "(total_score >= 80 AND band = 'hot') OR "
            "(total_score >= 60 AND total_score < 80 AND band = 'warm') OR "
            "(total_score < 60 AND band = 'low')",
            name=op.f(
                "ck_commercial_score_snapshots_score_band_consistent"
            ),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(breakdown) = 'object'",
            name=op.f("ck_commercial_score_snapshots_breakdown_object"),
        ),
        sa.ForeignKeyConstraint(
            ["opportunity_id"],
            ["opportunities.id"],
            name=op.f(
                "fk_commercial_score_snapshots_opportunity_id_opportunities"
            ),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "id",
            name=op.f("pk_commercial_score_snapshots"),
        ),
        sa.UniqueConstraint(
            "opportunity_id",
            "policy_version",
            "input_sha256",
            name="uq_commercial_score_snapshots_opportunity_policy_input",
        ),
        sa.UniqueConstraint(
            "id",
            "opportunity_id",
            name="uq_commercial_score_snapshots_id_opportunity",
        ),
    )
    op.create_index(
        "ix_commercial_score_snapshots_opportunity_score",
        "commercial_score_snapshots",
        ["opportunity_id", "total_score"],
    )

    op.create_table(
        "lead_inbox_items",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("search_profile_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("opportunity_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("latest_match_trace_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "commercial_score_snapshot_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("profile_revision", sa.Integer(), nullable=False),
        sa.Column(
            "workflow_status",
            sa.String(16),
            nullable=False,
            server_default="new",
        ),
        sa.Column("availability", sa.String(16), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("first_matched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_matched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "profile_revision >= 1 AND revision >= 1",
            name=op.f("ck_lead_inbox_items_revisions_positive"),
        ),
        sa.CheckConstraint(
            "workflow_status IN ('new', 'opened', 'shortlisted', 'ignored', 'replied')",
            name=op.f("ck_lead_inbox_items_workflow_status_valid"),
        ),
        sa.CheckConstraint(
            "availability IN ('available', 'ineligible', 'inactive')",
            name=op.f("ck_lead_inbox_items_availability_valid"),
        ),
        sa.CheckConstraint(
            "last_matched_at >= first_matched_at",
            name=op.f("ck_lead_inbox_items_matched_window_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["search_profile_id"],
            ["search_profiles.id"],
            name=op.f("fk_lead_inbox_items_search_profile_id_search_profiles"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["opportunity_id"],
            ["opportunities.id"],
            name=op.f("fk_lead_inbox_items_opportunity_id_opportunities"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            [
                "latest_match_trace_id",
                "opportunity_id",
                "search_profile_id",
                "profile_revision",
            ],
            [
                "match_traces.id",
                "match_traces.opportunity_id",
                "match_traces.search_profile_id",
                "match_traces.profile_revision",
            ],
            name=op.f("fk_lead_inbox_items_latest_match_context_valid"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["commercial_score_snapshot_id", "opportunity_id"],
            [
                "commercial_score_snapshots.id",
                "commercial_score_snapshots.opportunity_id",
            ],
            name=op.f(
                "fk_lead_inbox_items_commercial_score_opportunity_valid"
            ),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_lead_inbox_items")),
        sa.UniqueConstraint(
            "search_profile_id",
            "opportunity_id",
            name="uq_lead_inbox_items_profile_opportunity",
        ),
        sa.UniqueConstraint(
            "id",
            "opportunity_id",
            "search_profile_id",
            name="uq_lead_inbox_items_identity_context",
        ),
    )
    op.create_index(
        "ix_lead_inbox_items_profile_status_score",
        "lead_inbox_items",
        ["search_profile_id", "workflow_status", "availability", "updated_at"],
    )

    op.create_table(
        "lead_inbox_status_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("inbox_item_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("from_status", sa.String(16), nullable=False),
        sa.Column("to_status", sa.String(16), nullable=False),
        sa.Column("actor_id", sa.String(128), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("idempotency_key", sa.String(64), nullable=False),
        sa.Column("result_revision", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "from_status IN ('new', 'opened', 'shortlisted', 'ignored', 'replied') "
            "AND to_status IN ('new', 'opened', 'shortlisted', 'ignored', 'replied') "
            "AND from_status <> to_status",
            name=op.f("ck_lead_inbox_status_events_status_change_valid"),
        ),
        sa.CheckConstraint(
            "actor_id = btrim(actor_id) AND actor_id <> '' "
            "AND reason = btrim(reason) AND reason <> ''",
            name=op.f("ck_lead_inbox_status_events_operator_evidence_valid"),
        ),
        sa.CheckConstraint(
            "idempotency_key ~ '^[0-9a-f]{64}$' AND result_revision >= 2",
            name=op.f(
                "ck_lead_inbox_status_events_event_identity_revision_valid"
            ),
        ),
        sa.ForeignKeyConstraint(
            ["inbox_item_id"],
            ["lead_inbox_items.id"],
            name=op.f(
                "fk_lead_inbox_status_events_inbox_item_id_lead_inbox_items"
            ),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "id",
            name=op.f("pk_lead_inbox_status_events"),
        ),
        sa.UniqueConstraint(
            "inbox_item_id",
            "idempotency_key",
            name="uq_lead_inbox_status_events_item_key",
        ),
    )
    op.create_index(
        "ix_lead_inbox_status_events_item_created",
        "lead_inbox_status_events",
        ["inbox_item_id", "created_at"],
    )

    op.create_table(
        "reply_drafts",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("inbox_item_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("match_trace_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("opportunity_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("search_profile_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("profile_revision", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("model", sa.String(128), nullable=False),
        sa.Column("prompt_version", sa.String(64), nullable=False),
        sa.Column("schema_version", sa.String(64), nullable=False),
        sa.Column("persona_sha256", sa.String(64), nullable=False),
        sa.Column("input_sha256", sa.String(64), nullable=False),
        sa.Column("idempotency_key", sa.String(64), nullable=False),
        sa.Column("result", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "profile_revision >= 1",
            name=op.f("ck_reply_drafts_profile_revision_valid"),
        ),
        sa.CheckConstraint(
            "provider = lower(provider) AND provider ~ '^[a-z][a-z0-9_-]{0,63}$'",
            name=op.f("ck_reply_drafts_provider_valid"),
        ),
        sa.CheckConstraint(
            "prompt_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$' "
            "AND schema_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
            name=op.f("ck_reply_drafts_versions_valid"),
        ),
        sa.CheckConstraint(
            "persona_sha256 ~ '^[0-9a-f]{64}$' "
            "AND input_sha256 ~ '^[0-9a-f]{64}$' "
            "AND idempotency_key ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_reply_drafts_hashes_valid"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(result) = 'object'",
            name=op.f("ck_reply_drafts_result_object"),
        ),
        sa.ForeignKeyConstraint(
            ["inbox_item_id", "opportunity_id", "search_profile_id"],
            [
                "lead_inbox_items.id",
                "lead_inbox_items.opportunity_id",
                "lead_inbox_items.search_profile_id",
            ],
            name=op.f("fk_reply_drafts_inbox_context_valid"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            [
                "match_trace_id",
                "opportunity_id",
                "search_profile_id",
                "profile_revision",
            ],
            [
                "match_traces.id",
                "match_traces.opportunity_id",
                "match_traces.search_profile_id",
                "match_traces.profile_revision",
            ],
            name=op.f("fk_reply_drafts_match_context_valid"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_reply_drafts")),
        sa.UniqueConstraint(
            "idempotency_key",
            name="uq_reply_drafts_idempotency_key",
        ),
    )
    op.create_index(
        "ix_reply_drafts_inbox_created",
        "reply_drafts",
        ["inbox_item_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_reply_drafts_inbox_created", table_name="reply_drafts")
    op.drop_table("reply_drafts")
    op.drop_index(
        "ix_lead_inbox_status_events_item_created",
        table_name="lead_inbox_status_events",
    )
    op.drop_table("lead_inbox_status_events")
    op.drop_index(
        "ix_lead_inbox_items_profile_status_score",
        table_name="lead_inbox_items",
    )
    op.drop_table("lead_inbox_items")
    op.drop_index(
        "ix_commercial_score_snapshots_opportunity_score",
        table_name="commercial_score_snapshots",
    )
    op.drop_table("commercial_score_snapshots")
    op.drop_constraint(
        "uq_match_traces_identity_context",
        "match_traces",
        type_="unique",
    )

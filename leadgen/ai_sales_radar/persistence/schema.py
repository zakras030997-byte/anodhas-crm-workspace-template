"""AI Sales Radar tables attached to the proven V2 PostgreSQL metadata."""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID

from freelancer_bot.persistence.schema import metadata


commercial_score_snapshots = sa.Table(
    "commercial_score_snapshots",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column(
        "opportunity_id",
        UUID(as_uuid=True),
        sa.ForeignKey("opportunities.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column("policy_version", sa.String(64), nullable=False),
    sa.Column("input_sha256", sa.String(64), nullable=False),
    sa.Column("total_score", sa.SmallInteger(), nullable=False),
    sa.Column("band", sa.String(8), nullable=False),
    sa.Column("breakdown", JSONB(), nullable=False),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "policy_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
        name="policy_version_valid",
    ),
    sa.CheckConstraint(
        "input_sha256 ~ '^[0-9a-f]{64}$'",
        name="input_sha256_valid",
    ),
    sa.CheckConstraint(
        "total_score BETWEEN 0 AND 100",
        name="total_score_bounded",
    ),
    sa.CheckConstraint(
        "band IN ('hot', 'warm', 'low')",
        name="band_valid",
    ),
    sa.CheckConstraint(
        "(total_score >= 80 AND band = 'hot') OR "
        "(total_score >= 60 AND total_score < 80 AND band = 'warm') OR "
        "(total_score < 60 AND band = 'low')",
        name="score_band_consistent",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(breakdown) = 'object'",
        name="breakdown_object",
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

sa.Index(
    "ix_commercial_score_snapshots_opportunity_score",
    commercial_score_snapshots.c.opportunity_id,
    commercial_score_snapshots.c.total_score,
)


lead_inbox_items = sa.Table(
    "lead_inbox_items",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column(
        "search_profile_id",
        UUID(as_uuid=True),
        sa.ForeignKey("search_profiles.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "opportunity_id",
        UUID(as_uuid=True),
        sa.ForeignKey("opportunities.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "latest_match_trace_id",
        UUID(as_uuid=True),
        nullable=False,
    ),
    sa.Column(
        "commercial_score_snapshot_id",
        UUID(as_uuid=True),
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
        name="revisions_positive",
    ),
    sa.CheckConstraint(
        "workflow_status IN ('new', 'opened', 'shortlisted', 'ignored', 'replied')",
        name="workflow_status_valid",
    ),
    sa.CheckConstraint(
        "availability IN ('available', 'ineligible', 'inactive')",
        name="availability_valid",
    ),
    sa.CheckConstraint(
        "last_matched_at >= first_matched_at",
        name="matched_window_valid",
    ),
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
        name="fk_lead_inbox_items_latest_match_context_valid",
        ondelete="RESTRICT",
    ),
    sa.ForeignKeyConstraint(
        ["commercial_score_snapshot_id", "opportunity_id"],
        ["commercial_score_snapshots.id", "commercial_score_snapshots.opportunity_id"],
        name="fk_lead_inbox_items_commercial_score_opportunity_valid",
        ondelete="RESTRICT",
    ),
)

sa.Index(
    "ix_lead_inbox_items_profile_status_score",
    lead_inbox_items.c.search_profile_id,
    lead_inbox_items.c.workflow_status,
    lead_inbox_items.c.availability,
    lead_inbox_items.c.updated_at,
)


lead_inbox_status_events = sa.Table(
    "lead_inbox_status_events",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column(
        "inbox_item_id",
        UUID(as_uuid=True),
        sa.ForeignKey("lead_inbox_items.id", ondelete="RESTRICT"),
        nullable=False,
    ),
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
        name="status_change_valid",
    ),
    sa.CheckConstraint(
        "actor_id = btrim(actor_id) AND actor_id <> '' "
        "AND reason = btrim(reason) AND reason <> ''",
        name="operator_evidence_valid",
    ),
    sa.CheckConstraint(
        "idempotency_key ~ '^[0-9a-f]{64}$' AND result_revision >= 2",
        name="event_identity_revision_valid",
    ),
    sa.UniqueConstraint(
        "inbox_item_id",
        "idempotency_key",
        name="uq_lead_inbox_status_events_item_key",
    ),
)

sa.Index(
    "ix_lead_inbox_status_events_item_created",
    lead_inbox_status_events.c.inbox_item_id,
    lead_inbox_status_events.c.created_at,
)


reply_drafts = sa.Table(
    "reply_drafts",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column(
        "inbox_item_id",
        UUID(as_uuid=True),
        nullable=False,
    ),
    sa.Column(
        "match_trace_id",
        UUID(as_uuid=True),
        nullable=False,
    ),
    sa.Column("opportunity_id", UUID(as_uuid=True), nullable=False),
    sa.Column("search_profile_id", UUID(as_uuid=True), nullable=False),
    sa.Column("profile_revision", sa.Integer(), nullable=False),
    sa.Column("provider", sa.String(64), nullable=False),
    sa.Column("model", sa.String(128), nullable=False),
    sa.Column("prompt_version", sa.String(64), nullable=False),
    sa.Column("schema_version", sa.String(64), nullable=False),
    sa.Column("persona_sha256", sa.String(64), nullable=False),
    sa.Column("input_sha256", sa.String(64), nullable=False),
    sa.Column("idempotency_key", sa.String(64), nullable=False),
    sa.Column("result", JSONB(), nullable=False),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    sa.CheckConstraint(
        "profile_revision >= 1",
        name="profile_revision_valid",
    ),
    sa.CheckConstraint(
        "provider = lower(provider) AND provider ~ '^[a-z][a-z0-9_-]{0,63}$'",
        name="provider_valid",
    ),
    sa.CheckConstraint(
        "prompt_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$' "
        "AND schema_version ~ '^[a-z0-9][a-z0-9._-]{0,63}$'",
        name="versions_valid",
    ),
    sa.CheckConstraint(
        "persona_sha256 ~ '^[0-9a-f]{64}$' "
        "AND input_sha256 ~ '^[0-9a-f]{64}$' "
        "AND idempotency_key ~ '^[0-9a-f]{64}$'",
        name="hashes_valid",
    ),
    sa.CheckConstraint(
        "jsonb_typeof(result) = 'object'",
        name="result_object",
    ),
    sa.UniqueConstraint(
        "idempotency_key",
        name="uq_reply_drafts_idempotency_key",
    ),
    sa.ForeignKeyConstraint(
        ["inbox_item_id", "opportunity_id", "search_profile_id"],
        [
            "lead_inbox_items.id",
            "lead_inbox_items.opportunity_id",
            "lead_inbox_items.search_profile_id",
        ],
        name="fk_reply_drafts_inbox_context_valid",
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
        name="fk_reply_drafts_match_context_valid",
        ondelete="RESTRICT",
    ),
)

sa.Index(
    "ix_reply_drafts_inbox_created",
    reply_drafts.c.inbox_item_id,
    reply_drafts.c.created_at,
)


__all__ = [
    "commercial_score_snapshots",
    "lead_inbox_items",
    "lead_inbox_status_events",
    "reply_drafts",
]

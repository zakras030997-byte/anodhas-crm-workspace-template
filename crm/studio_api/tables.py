"""Additive operational tables; existing bot tables are read through a separate metadata.

This module does NOT run migrations or change a production schema on import.
Commercial lead fields remain authoritative in the canonical Google spreadsheet.
"""
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID

metadata = sa.MetaData()
legacy_metadata = sa.MetaData()

requests = sa.Table('anodhas_orders', legacy_metadata,
    sa.Column('id', UUID(as_uuid=True), primary_key=True),
    sa.Column('bot_id', sa.BigInteger, nullable=False, server_default='8884933395'),
    sa.Column('client_telegram_id', sa.BigInteger, nullable=False),
    sa.Column('client_username', sa.String(32)),
    sa.Column('client_display_name', sa.String(255)),
    sa.Column('service_key', sa.String(64)), sa.Column('service_label', sa.String(255)),
    sa.Column('details', sa.Text), sa.Column('desired_result', sa.Text),
    sa.Column('deadline', sa.String(255)), sa.Column('contact', sa.String(255)),
    sa.Column('leadgen', JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
    sa.Column('calendar_plan', JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
    sa.Column('status', sa.String(24), nullable=False),
    sa.Column('revision', sa.Integer, nullable=False),
    sa.Column('google_sync_status', sa.String(24), nullable=False),
    sa.Column('submitted_at', sa.DateTime(timezone=True)),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
)
messages = sa.Table('anodhas_order_messages', legacy_metadata,
    sa.Column('id', UUID(as_uuid=True), primary_key=True),
    sa.Column('order_id', UUID(as_uuid=True), nullable=False),
    sa.Column('role', sa.String(16), nullable=False), sa.Column('body', sa.Text),
    sa.Column('telegram_message_id', sa.BigInteger),
    sa.Column('metadata', JSONB, nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
)

sessions = sa.Table('anodhas_crm_sessions', metadata,
    sa.Column('token_hash', sa.String(64), primary_key=True),
    sa.Column('owner_id', sa.BigInteger, nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('last_seen_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
)
logins = sa.Table('anodhas_crm_login_attempts', metadata,
    sa.Column('state_hash', sa.String(64), primary_key=True),
    sa.Column('binding_hash', sa.String(64), nullable=False),
    sa.Column('verifier', sa.String(128), nullable=False),
    sa.Column('nonce', sa.String(128), nullable=False),
    sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
)
miniapp_logins = sa.Table('studio_crm_miniapp_logins', metadata,
    sa.Column('data_hash', sa.String(64), primary_key=True),
    sa.Column('bot_id', sa.BigInteger, nullable=False),
    sa.Column('owner_id', sa.BigInteger, nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
)
sa.Index('ix_studio_crm_miniapp_logins_expiry',miniapp_logins.c.expires_at)
decisions = sa.Table('anodhas_crm_decisions', metadata,
    sa.Column('request_id', UUID(as_uuid=True), sa.ForeignKey(requests.c.id,ondelete='RESTRICT'), primary_key=True),
    sa.Column('decision', sa.String(16), nullable=False),
    sa.Column('revision', sa.Integer, nullable=False),
    sa.Column('payload_hash', sa.String(64), nullable=False),
    sa.Column('owner_id', sa.BigInteger, nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("decision IN ('approved','rejected')"),
)
drafts = sa.Table('anodhas_crm_drafts', metadata,
    sa.Column('request_id', UUID(as_uuid=True), sa.ForeignKey(requests.c.id,ondelete='RESTRICT'), primary_key=True),
    sa.Column('revision', sa.Integer, nullable=False),
    sa.Column('body', sa.Text, nullable=False),
    sa.Column('attachment_ids', JSONB, nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
)
jobs = sa.Table('anodhas_crm_jobs', metadata,
    sa.Column('id', UUID(as_uuid=True), primary_key=True),
    sa.Column('operation_key', UUID(as_uuid=True), nullable=False, unique=True),
    sa.Column('kind', sa.String(32), nullable=False),
    sa.Column('request_id', UUID(as_uuid=True), sa.ForeignKey(requests.c.id,ondelete='RESTRICT'), nullable=False),
    sa.Column('request_hash', sa.String(64), nullable=False),
    sa.Column('payload_hash', sa.String(64), nullable=False),
    sa.Column('payload', JSONB, nullable=False),
    sa.Column('state', sa.String(24), nullable=False),
    sa.Column('owner_id', sa.BigInteger, nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('error_code', sa.String(64)),
    sa.Column('result', JSONB, nullable=False),
    sa.CheckConstraint("kind IN ('lead_create','telegram_send')"),
    sa.CheckConstraint("state IN ('pending','ready','sending','confirmed','failed','unknown','needs_review')"),
)
sa.Index('ix_anodhas_crm_jobs_pending', jobs.c.state, jobs.c.created_at)
sa.Index('uq_anodhas_crm_single_lead_create', jobs.c.request_id, unique=True,
    postgresql_where=jobs.c.kind == 'lead_create')

# Every accepted client command key, including aliases and no-job decisions.
operations = sa.Table('anodhas_crm_operations', metadata,
    sa.Column('operation_key', UUID(as_uuid=True), primary_key=True),
    sa.Column('request_id', UUID(as_uuid=True), sa.ForeignKey(requests.c.id,ondelete='RESTRICT'), nullable=False),
    sa.Column('request_hash', sa.String(64), nullable=False),
    sa.Column('job_id', UUID(as_uuid=True), sa.ForeignKey(jobs.c.id,ondelete='RESTRICT')),
    sa.Column('result', JSONB, nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
)

stages = sa.Table('anodhas_crm_stages', metadata,
    sa.Column('id', UUID(as_uuid=True), primary_key=True),
    sa.Column('request_id', UUID(as_uuid=True), sa.ForeignKey(requests.c.id,ondelete='RESTRICT'), nullable=False),
    sa.Column('title', sa.String(160), nullable=False),
    sa.Column('state', sa.String(24), nullable=False),
    sa.Column('public_result', sa.Text, nullable=False),
    sa.Column('private_note', sa.Text, nullable=False),
    sa.Column('deadline', sa.String(64), nullable=False),
    sa.Column('revision', sa.Integer, nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("state IN ('not_started','in_progress','done')"),
)

# New isolated database only. These tables must never be migrated into the old bot DB.
runtime = sa.Table('studio_runtime', metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('bot_id', sa.BigInteger, nullable=False),
    sa.Column('schema_version', sa.Integer, nullable=False),
    sa.CheckConstraint('id = 1'),
)
chat_state = sa.Table('studio_chats', metadata,
    sa.Column('bot_id', sa.BigInteger, primary_key=True),
    sa.Column('chat_id', sa.BigInteger, primary_key=True),
    sa.Column('request_id', UUID(as_uuid=True), sa.ForeignKey(requests.c.id)),
    sa.Column('step', sa.String(32), nullable=False),
    sa.Column('verified_at', sa.DateTime(timezone=True), nullable=False),
)
updates = sa.Table('studio_telegram_updates', metadata,
    sa.Column('bot_id', sa.BigInteger, primary_key=True),
    sa.Column('update_id', sa.BigInteger, primary_key=True),
    sa.Column('processed_at', sa.DateTime(timezone=True), nullable=False),
)
attachments = sa.Table('studio_attachments', metadata,
    sa.Column('id', UUID(as_uuid=True), primary_key=True),
    sa.Column('request_id', UUID(as_uuid=True), sa.ForeignKey(requests.c.id,ondelete='RESTRICT'), nullable=False),
    sa.Column('bot_id', sa.BigInteger, nullable=False),
    sa.Column('kind', sa.String(16), nullable=False),
    sa.Column('filename', sa.String(200), nullable=False),
    sa.Column('mime', sa.String(100), nullable=False),
    sa.Column('size', sa.BigInteger, nullable=False),
    sa.Column('sha256', sa.String(64)),
    sa.Column('storage_key', sa.String(64)),
    sa.Column('telegram_file_id', sa.Text),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("kind IN ('photo','video','document')"),
)
bot_logins = sa.Table('studio_bot_logins', metadata,
    sa.Column('challenge_hash', sa.String(64), primary_key=True),
    sa.Column('binding_hash', sa.String(64), nullable=False),
    sa.Column('bot_id', sa.BigInteger, nullable=False),
    sa.Column('owner_id', sa.BigInteger, nullable=False),
    sa.Column('confirmation_message_id', sa.BigInteger),
    sa.Column('code_hash', sa.String(64)),
    sa.Column('state', sa.String(20), nullable=False),
    sa.Column('failures', sa.Integer, nullable=False),
    sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
)
bot_outbox = sa.Table('studio_bot_outbox',metadata,
    sa.Column('id',UUID(as_uuid=True),primary_key=True),
    sa.Column('bot_id',sa.BigInteger,nullable=False),
    sa.Column('chat_id',sa.BigInteger,nullable=False),
    sa.Column('request_id',UUID(as_uuid=True),sa.ForeignKey(requests.c.id)),
    sa.Column('payload',JSONB,nullable=False),
    sa.Column('state',sa.String(24),nullable=False),
    sa.Column('result',JSONB,nullable=False),
    sa.Column('error_code',sa.String(64)),
    sa.Column('created_at',sa.DateTime(timezone=True),nullable=False),
    sa.Column('updated_at',sa.DateTime(timezone=True),nullable=False),
)
uploads=sa.Table('studio_uploads',metadata,
    sa.Column('id',UUID(as_uuid=True),primary_key=True),
    sa.Column('request_id',UUID(as_uuid=True),sa.ForeignKey(requests.c.id),nullable=False),
    sa.Column('filename',sa.String(200),nullable=False),
    sa.Column('size',sa.BigInteger,nullable=False),
    sa.Column('received',sa.BigInteger,nullable=False),
    sa.Column('attachment_id',UUID(as_uuid=True),sa.ForeignKey(attachments.c.id)),
    sa.Column('expires_at',sa.DateTime(timezone=True),nullable=False),
)

# Additive migration only, never created implicitly in production app startup.
notifications=sa.Table('studio_owner_notifications', metadata,
    sa.Column('request_id', UUID(as_uuid=True), sa.ForeignKey(requests.c.id,ondelete='RESTRICT'), primary_key=True),
    sa.Column('state', sa.String(24), nullable=False),
    sa.Column('result', JSONB, nullable=False),
    sa.Column('error_code', sa.String(64)),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
)

# Independent committed AI budget. No FK: intake holds the request row lock
# while this one-shot reservation commits on a separate connection.
ai_attempts=sa.Table('studio_ai_attempts', metadata,
    sa.Column('bot_id', sa.BigInteger, primary_key=True),
    sa.Column('request_id', UUID(as_uuid=True), primary_key=True),
    sa.Column('update_id', sa.BigInteger, primary_key=True),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
)

# Source identity map only. Commercial data stays in the existing orders / Google flow.
leadgen_links = sa.Table('studio_leadgen_links', metadata,
    sa.Column('candidate_id', sa.String(64), primary_key=True),
    sa.Column('request_id', UUID(as_uuid=True), sa.ForeignKey(requests.c.id,ondelete='RESTRICT'), nullable=False, unique=True),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
)
leadgen_batches = sa.Table('studio_leadgen_batches', metadata,
    sa.Column('operation_key', UUID(as_uuid=True), primary_key=True),
    sa.Column('request_hash', sa.String(64), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
)

# Owner-written mail remains private operational state; Gmail is the send provider.
email_drafts = sa.Table('studio_email_drafts', metadata,
    sa.Column('request_id', UUID(as_uuid=True), sa.ForeignKey(requests.c.id,ondelete='RESTRICT'), primary_key=True),
    sa.Column('revision', sa.Integer, nullable=False),
    sa.Column('recipient', sa.String(254), nullable=False),
    sa.Column('subject', sa.String(200), nullable=False),
    sa.Column('body', sa.Text, nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
)
email_deliveries = sa.Table('studio_email_deliveries', metadata,
    sa.Column('id', UUID(as_uuid=True), primary_key=True),
    sa.Column('request_id', UUID(as_uuid=True), sa.ForeignKey(requests.c.id,ondelete='RESTRICT'), nullable=False),
    sa.Column('draft_revision', sa.Integer, nullable=False),
    sa.Column('payload_hash', sa.String(64), nullable=False),
    sa.Column('owner_id', sa.BigInteger, nullable=False),
    sa.Column('recipient', sa.String(254), nullable=False),
    sa.Column('subject', sa.String(200), nullable=False),
    sa.Column('body', sa.Text, nullable=False),
    sa.Column('state', sa.String(24), nullable=False),
    sa.Column('result', JSONB, nullable=False),
    sa.Column('error_code', sa.String(64)),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
    sa.UniqueConstraint('request_id','draft_revision'),
    sa.UniqueConstraint('request_id','payload_hash'),
    sa.CheckConstraint("state IN ('pending','sending','confirmed','failed','unknown')"),
)
email_operations = sa.Table('studio_email_operations', metadata,
    sa.Column('operation_key', UUID(as_uuid=True), primary_key=True),
    sa.Column('request_hash', sa.String(64), nullable=False),
    sa.Column('delivery_id', UUID(as_uuid=True), sa.ForeignKey(email_deliveries.c.id,ondelete='RESTRICT'), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
)

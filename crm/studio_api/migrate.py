"""Explicit initial migration for a NEW isolated studio database, never the old bot."""
import sqlalchemy as sa
from . import tables as t
from .config import Settings

def check_identity(engine, bot_id=8884933395, minimum_version=1):
    with engine.connect() as conn:
        row = conn.execute(sa.select(t.runtime).where(t.runtime.c.id==1)).mappings().one()
        if row['bot_id'] != bot_id or row['schema_version'] not in (1,2,3) or row['schema_version']<minimum_version:
            raise RuntimeError('Studio database identity/version mismatch')

def initialize(engine, bot_id=8884933395):
    if engine.url.database != 'anodhas_studio' or bot_id != 8884933395:
        raise RuntimeError('Migration requires the isolated anodhas_studio database')
    with engine.begin() as conn:
        conn.execute(sa.select(sa.func.pg_advisory_xact_lock(-8884933395)))
        names = sa.inspect(conn).get_table_names()
        if names:
            if 'studio_runtime' not in names:
                raise RuntimeError('Refusing existing non-studio database')
            row = conn.execute(sa.select(t.runtime)).mappings().one()
            if row['bot_id'] != bot_id or row['schema_version'] not in (1,2,3):
                raise RuntimeError('Existing schema requires an explicit new migration')
            return
        t.legacy_metadata.create_all(conn)
        t.metadata.create_all(conn)
        conn.execute(t.runtime.insert().values(id=1,bot_id=bot_id,schema_version=1))

def upgrade_anod(engine, bot_id=8884933395):
    """Explicit additive v2 migration, requires DB owner; no existing rows changed."""
    if engine.url.database != 'anodhas_studio' or bot_id != 8884933395:
        raise RuntimeError('Migration requires isolated studio database')
    with engine.begin() as conn:
        conn.execute(sa.select(sa.func.pg_advisory_xact_lock(-8884933395)))
        row=conn.execute(sa.select(t.runtime).where(t.runtime.c.id==1).with_for_update()).mappings().one()
        if row['bot_id']!=bot_id or row['schema_version'] not in (1,2,3):
            raise RuntimeError('Studio database identity/version mismatch')
        if row['schema_version']>=2:
            if not all(sa.inspect(conn).has_table(table.name) for table in (t.notifications,t.ai_attempts)):
                raise RuntimeError('Studio Anod schema missing')
            return
        t.notifications.create(conn,checkfirst=True)
        t.ai_attempts.create(conn,checkfirst=True)
        conn.execute(sa.text('GRANT SELECT, INSERT ON studio_owner_notifications TO anodhas_studio_runtime'))
        conn.execute(sa.text('GRANT SELECT, INSERT ON studio_ai_attempts TO anodhas_studio_runtime'))
        conn.execute(sa.text('GRANT UPDATE (state,result,error_code,updated_at) ON studio_owner_notifications TO anodhas_studio_runtime'))
        conn.execute(sa.text('GRANT UPDATE (client_display_name,service_key,service_label) ON anodhas_orders TO anodhas_studio_runtime'))
        conn.execute(t.runtime.update().where(t.runtime.c.id==1).values(schema_version=2))

def upgrade_calendar(engine, bot_id=8884933395):
    """Explicit additive v3 migration. No worker/OAuth/event creation here."""
    if engine.url.database != 'anodhas_studio' or bot_id != 8884933395:
        raise RuntimeError('Migration requires isolated studio database')
    with engine.begin() as conn:
        conn.execute(sa.select(sa.func.pg_advisory_xact_lock(-8884933395)))
        row=conn.execute(sa.select(t.runtime).where(t.runtime.c.id==1).with_for_update()).mappings().one()
        if row['bot_id']!=bot_id or row['schema_version'] not in (2,3):
            raise RuntimeError('Studio Anod v2 migration required first')
        columns={item['name']:item for item in sa.inspect(conn).get_columns('anodhas_orders')}
        if 'calendar_plan' not in columns:
            if row['schema_version']==3:
                raise RuntimeError('Studio Calendar schema missing')
            conn.exec_driver_sql("ALTER TABLE public.anodhas_orders ADD COLUMN calendar_plan jsonb NOT NULL DEFAULT '{}'::jsonb")
        column=next(item for item in sa.inspect(conn).get_columns('anodhas_orders') if item['name']=='calendar_plan')
        if not isinstance(column['type'],sa.dialects.postgresql.JSONB) or column['nullable']:
            raise RuntimeError('Studio Calendar column mismatch')
        conn.exec_driver_sql('GRANT UPDATE (calendar_plan) ON public.anodhas_orders TO anodhas_studio_runtime')
        conn.execute(t.runtime.update().where(t.runtime.c.id==1).values(schema_version=3))

if __name__ == '__main__':
    settings=Settings.from_env()
    engine=sa.create_engine(settings.database_url)
    initialize(engine,settings.bot_id)
    check_identity(engine,settings.bot_id)
    print('Isolated studio schema version 1 verified')


def upgrade_leadgen(engine, bot_id=8884933395):
    """Additive feature migration; v3 and the previous image remain compatible.

    No secrets, workers, external writes or modifications to existing rows.
    """
    if engine.url.database != 'anodhas_studio' or bot_id != 8884933395:
        raise RuntimeError('Migration requires isolated studio database')
    check_identity(engine,bot_id,minimum_version=3)
    with engine.begin() as conn:
        conn.execute(sa.select(sa.func.pg_advisory_xact_lock(-8884933395)))
        conn.exec_driver_sql("ALTER TABLE public.anodhas_orders ADD COLUMN IF NOT EXISTS leadgen jsonb NOT NULL DEFAULT '{}'::jsonb")
        column=next(item for item in sa.inspect(conn).get_columns('anodhas_orders') if item['name']=='leadgen')
        if not isinstance(column['type'],sa.dialects.postgresql.JSONB) or column['nullable']:
            raise RuntimeError('Studio lead generator column mismatch')
        t.leadgen_links.create(conn,checkfirst=True)
        t.leadgen_batches.create(conn,checkfirst=True)
        conn.exec_driver_sql('GRANT SELECT, INSERT ON public.studio_leadgen_batches TO anodhas_studio_runtime')
        conn.exec_driver_sql('GRANT SELECT, INSERT ON public.studio_leadgen_links TO anodhas_studio_runtime')
        conn.exec_driver_sql('GRANT UPDATE (leadgen) ON public.anodhas_orders TO anodhas_studio_runtime')


def upgrade_email(engine, bot_id=8884933395):
    """Additive feature tables; compatible with the previous v3 API on rollback."""
    if engine.url.database != 'anodhas_studio' or bot_id != 8884933395:
        raise RuntimeError('Migration requires isolated studio database')
    check_identity(engine,bot_id,minimum_version=3)
    with engine.begin() as conn:
        conn.execute(sa.select(sa.func.pg_advisory_xact_lock(-8884933395)))
        for table in (t.email_drafts,t.email_deliveries,t.email_operations):
            table.create(conn,checkfirst=True)
            conn.exec_driver_sql('GRANT SELECT, INSERT ON public.'+table.name+' TO anodhas_studio_runtime')
        conn.exec_driver_sql('GRANT UPDATE (revision,recipient,subject,body,updated_at) ON public.studio_email_drafts TO anodhas_studio_runtime')
        conn.exec_driver_sql('GRANT UPDATE (state,result,error_code,updated_at) ON public.studio_email_deliveries TO anodhas_studio_runtime')


def _check_crm_bot_table(conn):
    inspector=sa.inspect(conn)
    if not inspector.has_table(t.miniapp_logins.name):
        raise RuntimeError('Explicit CRM bot migration required')
    columns={item['name']:item for item in inspector.get_columns(t.miniapp_logins.name)}
    for name,kind in (('data_hash',sa.String),('bot_id',sa.BigInteger),('owner_id',sa.BigInteger),
                      ('created_at',sa.DateTime),('expires_at',sa.DateTime)):
        column=columns.get(name)
        if not column or not isinstance(column['type'],kind) or column['nullable']:
            raise RuntimeError('CRM bot replay schema mismatch')
        if (kind is sa.DateTime and not column['type'].timezone) or (name=='data_hash' and column['type'].length != 64):
            raise RuntimeError('CRM bot replay schema mismatch')
    if inspector.get_pk_constraint(t.miniapp_logins.name)['constrained_columns'] != ['data_hash']:
        raise RuntimeError('CRM bot replay key required')


def check_crm_bot_schema(engine):
    with engine.connect() as conn:
        _check_crm_bot_table(conn)


def upgrade_crm_bot(engine,bot_id=8884933395):
    """Explicit additive feature migration; existing identity/sessions unchanged.

    Run with the isolated database owner before enabling STUDIO_CRM_BOT_ENABLED.
    The previous v3 image remains compatible when this feature is disabled.
    """
    if engine.url.database != 'anodhas_studio' or bot_id != 8884933395:
        raise RuntimeError('Migration requires isolated studio database')
    check_identity(engine,bot_id,minimum_version=3)
    with engine.begin() as conn:
        conn.execute(sa.select(sa.func.pg_advisory_xact_lock(-8884933395)))
        t.miniapp_logins.create(conn,checkfirst=True)
        _check_crm_bot_table(conn)
        conn.exec_driver_sql('GRANT SELECT, INSERT, DELETE ON public.studio_crm_miniapp_logins TO anodhas_studio_runtime')

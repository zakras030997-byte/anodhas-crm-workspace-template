"""Secrets are private mounts, never Docker Env entries, CLI arguments or logs."""
import json
import os
from pathlib import Path
from .config import Settings

def main():
    import sys
    path=Path('/run/secrets/api.json')
    if path.stat().st_mode&0o077: raise SystemExit('Private configuration permissions required')
    values=json.loads(path.read_text())
    if not isinstance(values,dict) or any(not k.startswith('STUDIO_') or not isinstance(v,str) for k,v in values.items()):
        raise SystemExit('Invalid private configuration')
    os.environ.update(values)
    settings=Settings.from_env()
    if sys.argv[1:]==['migrate']:
        import sqlalchemy as sa
        from .migrate import initialize,check_identity
        engine=sa.create_engine(settings.database_url)
        initialize(engine,settings.bot_id);check_identity(engine,settings.bot_id)
        print('Isolated schema verified')
    elif sys.argv[1:]==['migrate-anod']:
        import sqlalchemy as sa
        from .migrate import upgrade_anod,check_identity
        engine=sa.create_engine(settings.database_url)
        upgrade_anod(engine,settings.bot_id);check_identity(engine,settings.bot_id,minimum_version=2)
        print('Isolated studio additive migration verified')
    elif sys.argv[1:]==['migrate-calendar']:
        import sqlalchemy as sa
        from .migrate import upgrade_calendar,check_identity
        engine=sa.create_engine(settings.database_url)
        try:
            upgrade_calendar(engine,settings.bot_id)
            check_identity(engine,settings.bot_id,minimum_version=3)
        finally:
            engine.dispose()
        print('Isolated studio calendar migration verified')
    elif sys.argv[1:]==['migrate-leadgen']:
        import sqlalchemy as sa
        from .migrate import upgrade_leadgen
        engine=sa.create_engine(settings.database_url)
        try: upgrade_leadgen(engine,settings.bot_id)
        finally: engine.dispose()
        print('Lead generator integration schema verified')
    elif sys.argv[1:]==['migrate-email']:
        import sqlalchemy as sa
        from .migrate import upgrade_email
        engine=sa.create_engine(settings.database_url)
        try: upgrade_email(engine,settings.bot_id)
        finally: engine.dispose()
        print('Gmail integration schema verified')
    elif sys.argv[1:]==['migrate-crm-bot']:
        import sqlalchemy as sa
        from .migrate import upgrade_crm_bot,check_crm_bot_schema
        engine=sa.create_engine(settings.database_url)
        try:
            upgrade_crm_bot(engine,settings.bot_id)
            check_crm_bot_schema(engine)
        finally: engine.dispose()
        print('CRM bot authentication schema verified')
    elif sys.argv[1:]: raise SystemExit('Unknown launch mode')
    else:
        import uvicorn
        uvicorn.run('studio_api.app:application',factory=True,host='0.0.0.0',port=8080,workers=1,access_log=False,log_level='warning')

if __name__=='__main__': main()

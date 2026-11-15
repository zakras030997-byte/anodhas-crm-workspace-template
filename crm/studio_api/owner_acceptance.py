"""One explicitly reviewed synthetic owner job; no web app, queues or retries.

inspect prepares a safe manifest. run claims that exact manifest once. readback
only reads PostgreSQL. The original Settings object remains owner-only/closed.
"""
from copy import deepcopy
from dataclasses import replace
import hashlib
from pathlib import Path
import re
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.engine import make_url

from . import tables as t
from .calendar_plan import normalize_plan
from .calendar_sync import Calendar, event_body
from .google_runtime import _credentials_info, SCOPES
from .service import Rejected, digest, now, request_payload
from .sheets import BOOK, Sheets, lead_values
from .telegram import DeliveryUnknown
from .workspace import ROOT, FOLDER, PARTS, safe_name, client_card, brief_document

BOT_ID = 8884933395
ORIGIN = 'https://example.invalid'
RUNTIME_ROLE = 'anodhas_studio_runtime'
CREDENTIAL_PATH = '/run/secrets/google-owner-oauth.json'
CLIENT_ID = '966442606468-nfg09av9lsqho7rlg01qhl1604tj5jd6.apps.googleusercontent.com'
CALENDAR_ID = 'contact003@example.invalid'
DESTINATIONS = {'lead_sheet': BOOK, 'client_root': ROOT, 'project_calendar': CALENDAR_ID, 'origin': ORIGIN}
MANIFEST_KEYS = {'version', 'job_id', 'request_id', 'owner_telegram_id', 'bot_id', 'decision_revision',
                 'client_consent_message_id', 'payload_sha256', 'decision_payload_sha256', 'destinations', 'synthetic_test'}
ERROR_CODES = {'acceptance_rejected', 'acceptance_delivery_unknown', 'acceptance_execution_error'}


class JournalStopped(RuntimeError):
    """A failed durable journal stops execution without another provider call."""


def require(value, code='acceptance_binding_invalid'):
    if not value:
        raise Rejected(code)


def uuid_text(value):
    try:
        parsed = str(UUID(str(value)))
        require(str(value) == parsed)
        return parsed
    except (ValueError, TypeError, AttributeError):
        raise Rejected('acceptance_identity_invalid') from None


def sha(value):
    require(isinstance(value, str) and bool(re.fullmatch('[0-9a-f]{64}', value)), 'acceptance_hash_invalid')
    return value


def checked_manifest(value):
    require(isinstance(value, dict) and set(value) == MANIFEST_KEYS)
    require(value['version'] == 1 and type(value['version']) is int and value['synthetic_test'] is True
            and value['destinations'] == DESTINATIONS and value['bot_id'] == BOT_ID
            and type(value['owner_telegram_id']) is int and value['owner_telegram_id'] > 0
            and type(value['decision_revision']) is int and value['decision_revision'] > 0)
    for key in ('job_id', 'request_id', 'client_consent_message_id'):
        uuid_text(value[key])
    for key in ('payload_sha256', 'decision_payload_sha256'):
        sha(value[key])
    return deepcopy(value)


def _verified(journal, service):
    return [row for row in journal if row['service'] == service and row['phase'] == 'readback_verified']


def _drive_expected(payload, marker, journal):
    client_marker = 'client:' + str(payload['client_telegram_id'])
    objects = {row['marker']: row['file_id'] for row in _verified(journal, 'drive')}
    content = None
    if marker == client_marker:
        parent, name = ROOT, safe_name(payload.get('client_display_name'))
    elif marker in [client_marker + ':part:' + str(i) for i in range(len(PARTS))]:
        require(client_marker in objects)
        parent, name = objects[client_marker], PARTS[int(marker.rsplit(':', 1)[1])]
    elif marker in ('card:' + payload['id'], 'brief:' + payload['id']):
        is_card = marker.startswith('card:')
        folder_marker = client_marker + ':part:' + str(PARTS.index('Карточка клиента' if is_card else 'Т.З'))
        require(folder_marker in objects)
        parent = objects[folder_marker]
        name = ('Карточка клиента — ' if is_card else 'ТЗ — ') + payload['id'][:8] + '.md'
        content = client_card(payload) if is_card else brief_document(payload)
    else:
        raise Rejected('acceptance_journal_invalid')
    meta = {'name': name, 'mimeType': FOLDER if content is None else 'text/markdown',
            'parents': [parent], 'appProperties': {'studio_marker': marker}}
    extra = {}
    if content is not None:
        extra['content_sha256'] = hashlib.sha256(content.encode()).hexdigest()
        meta['appProperties'].update(extra)
    return {'service': 'drive', 'operation': 'ensure_folder' if content is None else 'ensure_document',
            'parent_id': parent, 'marker': marker, 'body_sha256': digest(meta), **extra}


def _workspace_receipt(payload, journal):
    rows = _verified(journal, 'drive')
    marker = 'client:' + str(payload['client_telegram_id'])
    expected = {marker, *[marker + ':part:' + str(i) for i in range(len(PARTS))],
                'card:' + payload['id'], 'brief:' + payload['id']}
    require(len(rows) == len(expected) and {row['marker'] for row in rows} == expected,
            'acceptance_drive_incomplete')
    objects = {row['marker']: row['file_id'] for row in rows}
    require(len(set(objects.values())) == len(objects), 'acceptance_drive_incomplete')
    root = objects[marker]
    return {'verified': True, 'folder_id': root, 'folder_url': 'https://drive.google.com/drive/folders/' + root,
            'card_id': objects['card:' + payload['id']], 'brief_id': objects['brief:' + payload['id']],
            'folders': {part: objects[marker + ':part:' + str(i)] for i, part in enumerate(PARTS)}}


def checked_event(event, payload, previous):
    require(isinstance(event, dict) and len(previous) < 128, 'acceptance_journal_invalid')
    phase, service = event.get('phase'), event.get('service')
    require(phase in ('write_started', 'write_acknowledged', 'readback_verified'), 'acceptance_journal_invalid')
    if service == 'drive':
        expected = _drive_expected(payload, event.get('marker'), previous)
        extra = {'file_id'} if phase != 'write_started' else set()
        if extra:
            require(isinstance(event.get('file_id'), str) and bool(re.fullmatch('[A-Za-z0-9_-]{1,160}', event['file_id'])))
            require(event['file_id'] != expected['parent_id'])
    elif service == 'sheets':
        workspace = _workspace_receipt(payload, previous)
        expected = {'service': 'sheets', 'operation': 'append_lead', 'spreadsheet_id': BOOK,
            'range': "'Оперативная память'!A:I",
            'marker': f'ANODHAS Studio · request:{payload["id"]} · lead:{payload["lead_id"]}',
            'body_sha256': digest({'values': [lead_values(payload, workspace)]})}
        extra = {'updated_range'} if phase != 'write_started' else set()
        if extra:
            require(isinstance(event.get('updated_range'), str) and bool(re.fullmatch(
                r"'?Оперативная память'?!A([2-9]|[1-9][0-9]+):I\1", event['updated_range'])))
    elif service == 'calendar':
        bodies = {body['id']: body for body in [event_body(payload['id'], payload['calendar_plan'], item)
                  for item in payload['calendar_plan']['events']]}
        body = bodies.get(event.get('event_id'))
        require(body is not None)
        expected = {'service': 'calendar', 'operation': 'insert_event', 'calendar_id': CALENDAR_ID,
            'marker': 'anodhas-calendar-v1', 'event_id': body['id'],
            'event_key': body['extendedProperties']['private']['event_key'], 'body_sha256': digest(body)}
        extra = set()
    else:
        raise Rejected('acceptance_journal_invalid')
    if phase == 'readback_verified':
        extra.add('disposition')
        require(event.get('disposition') in ('created', 'reused'))
    require(set(event) == set(expected) | extra | {'phase'} and all(event.get(k) == v for k, v in expected.items()),
            'acceptance_journal_invalid')
    matching = [row for row in previous if all(row.get(k) == v for k, v in expected.items())]
    require(not any(row['phase'] == 'readback_verified' for row in matching), 'acceptance_journal_order_invalid')
    require(not any(row['phase'] == phase for row in matching), 'acceptance_journal_duplicate')
    if phase == 'write_acknowledged' or (phase == 'readback_verified' and event['disposition'] == 'created'):
        needed = 'write_started' if phase == 'write_acknowledged' else 'write_acknowledged'
        require(any(row['phase'] == needed for row in matching), 'acceptance_journal_order_invalid')
    for row in matching:
        for key in ('file_id', 'updated_range'):
            if key in row and key in event:
                require(row[key] == event[key], 'acceptance_journal_identity_changed')
    return deepcopy(event)


def complete_journal(payload, journal):
    _workspace_receipt(payload, journal)
    require(len(_verified(journal, 'sheets')) == 1, 'acceptance_sheet_incomplete')
    expected = {event_body(payload['id'], payload['calendar_plan'], item)['id']
                for item in payload['calendar_plan']['events']}
    events = _verified(journal, 'calendar')
    require(len(events) == len(expected) and {event['event_id'] for event in events} == expected,
            'acceptance_calendar_incomplete')


class OwnerAcceptance:
    def __init__(self, engine, settings):
        self.engine, self.settings = engine, settings

    def _environment(self, conn):
        settings = self.settings
        require(settings.origin == ORIGIN and settings.bot_id == BOT_ID and settings.bot_username == 'AnodhasStudioBot'
                and settings.pilot_owner_only is True and settings.dispatch_enabled is False
                and settings.sheets_credentials_file == CREDENTIAL_PATH and settings.google_calendar_id == CALENDAR_ID,
                'acceptance_config_invalid')
        require(make_url(settings.database_url) == self.engine.url, 'acceptance_database_invalid')
        url = self.engine.url
        throwaway = (url.database == 'postgres' and str(url.query.get('host', '')).startswith('/private/tmp/anodhas-crm-test.'))
        require(url.username == RUNTIME_ROLE and (url.database == 'anodhas_studio' or throwaway), 'acceptance_database_invalid')
        role = conn.execute(sa.text('''SELECT current_user AS identity, session_user AS login,
            current_database() AS database, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls,
            EXISTS(SELECT 1 FROM pg_auth_members WHERE member=r.oid) AS member,
            EXISTS(SELECT 1 FROM pg_database WHERE datdba=r.oid AND datname=current_database()) AS owns_database,
            EXISTS(SELECT 1 FROM pg_namespace WHERE nspowner=r.oid AND nspname='public') AS owns_schema,
            has_schema_privilege(current_user,'public','CREATE') AS creates_in_schema,
            EXISTS(SELECT 1 FROM pg_class WHERE relowner=r.oid AND relname IN
                ('anodhas_crm_jobs','anodhas_orders','anodhas_crm_decisions','anodhas_order_messages')) AS owns_tables
            FROM pg_roles r WHERE rolname=current_user''')).mappings().one()
        require(role['identity'] == role['login'] == RUNTIME_ROLE and role['database'] == url.database
                and not any(role[k] for k in ('rolsuper','rolcreatedb','rolcreaterole','rolreplication','rolbypassrls',
                    'member','owns_database','owns_schema','creates_in_schema','owns_tables')),
                'acceptance_runtime_role_invalid')
        identity = conn.execute(sa.select(t.runtime).where(t.runtime.c.id == 1)).mappings().one()
        require(identity['bot_id'] == BOT_ID and identity['schema_version'] == 3, 'acceptance_schema_invalid')

    def _credential(self):
        require(Path(CREDENTIAL_PATH).parent.resolve() == Path(CREDENTIAL_PATH).parent, 'acceptance_config_invalid')
        info = _credentials_info(CREDENTIAL_PATH)
        require(info.get('type') == 'authorized_user' and info.get('client_id') == CLIENT_ID
                and isinstance(info.get('scopes'), list)
                and set(info['scopes']) == {scope for group in SCOPES.values() for scope in group},
                'acceptance_credentials_invalid')

    def _job(self, conn, job_id, payload_hash, lock=False):
        query = sa.select(t.jobs).where(t.jobs.c.id == UUID(uuid_text(job_id)), t.jobs.c.payload_hash == sha(payload_hash))
        row = conn.execute(query.with_for_update() if lock else query).mappings().one_or_none()
        require(row is not None and row['kind'] == 'lead_create' and row['owner_id'] == self.settings.owner_id,
                'acceptance_job_invalid')
        require(isinstance(row['payload'], dict) and digest(row['payload']) == payload_hash, 'acceptance_payload_changed')
        return dict(row)

    def _manifest(self, conn, job, consent_id):
        payload = job['payload']
        request = conn.execute(sa.select(t.requests).where(t.requests.c.id == job['request_id'],
            t.requests.c.bot_id == BOT_ID).with_for_update()).mappings().one_or_none()
        decision = conn.execute(sa.select(t.decisions).where(t.decisions.c.request_id == job['request_id'])).mappings().one_or_none()
        message = conn.execute(sa.select(t.messages).where(t.messages.c.id == UUID(uuid_text(consent_id)),
            t.messages.c.order_id == job['request_id'])).mappings().one_or_none()
        require(request is not None and decision is not None and message is not None)
        require(request['client_telegram_id'] == payload.get('client_telegram_id') == self.settings.owner_id
                and payload.get('bot_id') == BOT_ID and payload.get('id') == str(request['id'])
                and request['status'] == 'submitted' and request['submitted_at'] is not None
                and request['google_sync_status'] == 'not_requested')
        plan = normalize_plan(payload.get('calendar_plan', {}))
        require(bool(plan) and payload['calendar_plan'] == plan, 'acceptance_calendar_plan_required')
        original = request_payload(payload)
        require(set(payload) == set(original) | {'lead_id', 'status'} and payload['status'] == 'Новый')
        uuid_text(payload['lead_id'])
        require(decision['decision'] == 'approved' and decision['owner_id'] == self.settings.owner_id
                and decision['revision'] == request['revision']
                and decision['payload_hash'] == digest(original) == digest(request_payload(request)))
        metadata = message['metadata']
        require(message['role'] == 'client' and isinstance(metadata, dict) and metadata.get('consent') == 'explicit_submit'
                and type(metadata.get('submitted_revision')) is int
                and metadata['submitted_revision'] == request['revision']
                and request['submitted_at'] <= message['created_at'] <= decision['created_at'], 'acceptance_consent_invalid')
        require(job['request_hash'] == digest(['decide', str(request['id']), request['revision'], 'approved']))
        return checked_manifest({'version': 1, 'job_id': str(job['id']), 'request_id': str(request['id']),
            'owner_telegram_id': self.settings.owner_id, 'bot_id': BOT_ID, 'decision_revision': decision['revision'],
            'client_consent_message_id': uuid_text(consent_id), 'payload_sha256': job['payload_hash'],
            'decision_payload_sha256': decision['payload_hash'], 'destinations': DESTINATIONS, 'synthetic_test': True})

    def inspect(self, job_id, payload_sha256, consent_message_id):
        with self.engine.begin() as conn:
            self._environment(conn)
            job = self._job(conn, job_id, payload_sha256, lock=True)
            manifest = self._manifest(conn, job, consent_message_id)
            return {'manifest': manifest, 'manifest_sha256': digest(manifest), 'state': job['state'],
                    'runnable': job['state'] == 'pending' and job['result'] == {}}

    def _update(self, conn, job, attempt_id, manifest_hash, previous, result, *, state='sending', error=None):
        predicate = (t.jobs.c.id == job['id'], t.jobs.c.payload_hash == job['payload_hash'], t.jobs.c.state == 'sending',
                     t.jobs.c.result == previous, t.jobs.c.result['acceptance']['attempt_id'].astext == attempt_id,
                     t.jobs.c.result['acceptance']['manifest_sha256'].astext == manifest_hash)
        count = conn.execute(t.jobs.update().where(*predicate).values(result=result, state=state,
            error_code=error, updated_at=now())).rowcount
        require(count == 1, 'acceptance_journal_write_failed')
        after = self._job(conn, job['id'], job['payload_hash'])
        require(after['state'] == state and after['result'] == result, 'acceptance_journal_readback_failed')

    def readback(self, job_id, payload_sha256):
        with self.engine.connect() as conn:
            self._environment(conn)
            job = self._job(conn, job_id, payload_sha256)
            result = job['result']
            public = {'job_id': str(job['id']), 'request_id': str(job['request_id']), 'payload_sha256': job['payload_hash'],
                      'state': job['state'], 'has_result': bool(result), 'live_acceptance_complete': False}
            if isinstance(result, dict) and 'acceptance' in result:
                acceptance = result['acceptance']
                manifest = checked_manifest(acceptance['manifest'])
                require(manifest['job_id'] == str(job['id']) and manifest['payload_sha256'] == job['payload_hash']
                        and manifest['request_id'] == str(job['request_id'])
                        and manifest['owner_telegram_id'] == self.settings.owner_id
                        and digest(manifest) == acceptance['manifest_sha256'])
                journal = []
                for event in acceptance['journal']:
                    journal.append(checked_event(event, job['payload'], journal))
                public['acceptance'] = {'attempt_id': uuid_text(acceptance['attempt_id']), 'manifest': manifest,
                    'manifest_sha256': digest(manifest), 'journal': journal}
                public['verified'] = result.get('verified') is True and job['state'] == 'confirmed'
                if public['verified']:
                    complete_journal(job['payload'], journal)
                public['live_acceptance_complete'] = public['verified']
            if job['error_code'] in ERROR_CODES:
                public['error_code'] = job['error_code']
            return public

    def run(self, manifest, expected_manifest_sha256):
        manifest = checked_manifest(manifest)
        require(digest(manifest) == sha(expected_manifest_sha256), 'acceptance_manifest_changed')
        with self.engine.begin() as conn:
            self._environment(conn)
            self._credential()
            job = self._job(conn, manifest['job_id'], manifest['payload_sha256'], lock=True)
            require(job['state'] == 'pending' and job['result'] == {} and job['error_code'] is None,
                    'acceptance_already_attempted')
            require(self._manifest(conn, job, manifest['client_consent_message_id']) == manifest, 'acceptance_manifest_changed')
            attempt = str(uuid4())
            claimed = {'verified': False, 'acceptance': {'attempt_id': attempt, 'manifest': manifest,
                'manifest_sha256': expected_manifest_sha256, 'journal': []}}
            count = conn.execute(t.jobs.update().where(t.jobs.c.id == job['id'], t.jobs.c.payload_hash == job['payload_hash'],
                t.jobs.c.state == 'pending', t.jobs.c.result == {}, t.jobs.c.error_code.is_(None)).values(
                state='sending', result=claimed, updated_at=now())).rowcount
            after_claim = self._job(conn, job['id'], job['payload_hash'])
            require(count == 1 and after_claim['result'] == claimed and after_claim['state'] == 'sending',
                    'acceptance_claim_failed')

        def journal(event):
            try:
                with self.engine.begin() as conn:
                    current = self._job(conn, job['id'], job['payload_hash'], lock=True)['result']
                    updated = deepcopy(current)
                    entries = updated['acceptance']['journal']
                    entries.append(checked_event(event, job['payload'], entries))
                    self._update(conn, job, attempt, expected_manifest_sha256, current, updated)
            except Exception:
                raise JournalStopped('acceptance_journal_stopped') from None

        settings = replace(self.settings, pilot_owner_only=False, dispatch_enabled=False,
            sheets_enabled=True, google_workspace_enabled=True, google_calendar_enabled=True, owner_email_enabled=False)
        state, error = 'confirmed', None
        try:
            sheet = Sheets(settings, journal=journal).create_lead(job['payload'])
            snapshot = self.readback(job['id'], job['payload_hash'])['acceptance']['journal']
            workspace = _workspace_receipt(job['payload'], snapshot)
            sheet_events = _verified(snapshot, 'sheets')
            require(isinstance(sheet, dict) and sheet.get('verified') is True and sheet.get('workspace') == workspace
                    and sheet.get('lead_id') == job['payload']['lead_id'] and len(sheet_events) == 1
                    and sheet.get('row') == int(re.search(r'!A(\d+):', sheet_events[0]['updated_range'])[1]),
                    'acceptance_sheet_incomplete')
            calendar = Calendar(settings, journal=journal).create(job['payload'])
            snapshot = self.readback(job['id'], job['payload_hash'])['acceptance']['journal']
            events = _verified(snapshot, 'calendar')
            expected_ids = {event_body(job['payload']['id'], job['payload']['calendar_plan'], item)['id']
                            for item in job['payload']['calendar_plan']['events']}
            require(isinstance(calendar, dict) and calendar.get('verified') is True and calendar.get('status') == 'synced'
                    and calendar.get('calendar_id') == CALENDAR_ID and len(events) == len(expected_ids)
                    and {event['event_id'] for event in events} == expected_ids
                    and isinstance(calendar.get('events'), list) and len(calendar['events']) == len(expected_ids)
                    and {event.get('id') for event in calendar['events']} == expected_ids
                    and all(event.get('verified') is True for event in calendar['events']), 'acceptance_calendar_incomplete')
        except JournalStopped:
            return self.readback(job['id'], job['payload_hash'])
        except DeliveryUnknown:
            state, error = 'unknown', 'acceptance_delivery_unknown'
        except Rejected:
            state, error = 'unknown', 'acceptance_rejected'
        except Exception:
            state, error = 'unknown', 'acceptance_execution_error'
        # Hard termination/SystemExit deliberately leaves sending and its committed journal.
        with self.engine.begin() as conn:
            current = self._job(conn, job['id'], job['payload_hash'], lock=True)['result']
            final = deepcopy(current)
            if state == 'confirmed':
                require(self._manifest(conn, job, manifest['client_consent_message_id']) == manifest, 'acceptance_manifest_changed')
                count = conn.execute(t.requests.update().where(t.requests.c.id == job['request_id'],
                    t.requests.c.bot_id == BOT_ID, t.requests.c.client_telegram_id == self.settings.owner_id,
                    t.requests.c.revision == manifest['decision_revision'], t.requests.c.status == 'submitted',
                    t.requests.c.google_sync_status == 'not_requested').values(google_sync_status='synced')).rowcount
                require(count == 1, 'acceptance_request_update_failed')
                require(conn.scalar(sa.select(t.requests.c.google_sync_status).where(t.requests.c.id == job['request_id'])) == 'synced')
                final['verified'] = True
            self._update(conn, job, attempt, expected_manifest_sha256, current, final, state=state, error=error)
        return self.readback(job['id'], job['payload_hash'])

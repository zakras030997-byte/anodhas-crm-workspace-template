"""Create private project events only after an immutable owner-approved dispatch."""
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import re
from urllib.parse import quote
from uuid import UUID
from zoneinfo import ZoneInfo

from .calendar_plan import KINDS, local_datetime, normalize_plan
from .google_runtime import call
from .service import Rejected, digest
from .telegram import DeliveryUnknown

CALENDAR_SUMMARY = 'ANODHAS — проекты'
CALENDAR_DESCRIPTION = 'ANODHAS_STUDIO_CALENDAR_V1;bot=8884933395;owner=contact001@example.invalid'
OWNER = 'contact001@example.invalid'
FIELDS = 'id,status,summary,description,start,end,visibility,transparency,reminders,extendedProperties,attendees,conferenceData,hangoutLink,recurrence,htmlLink'
BASE = 'https://www.googleapis.com/calendar/v3/'


def secondary_calendar_id(value):
    return isinstance(value, str) and bool(re.fullmatch(r'[a-z0-9][a-z0-9_-]{4,160}@group\.calendar\.google\.com', value))


def event_id(request_id, key):
    return 'anod' + hashlib.sha256(('anodhas-calendar-v1:' + request_id + ':' + key).encode()).hexdigest()


def event_body(request_id, plan, event):
    plan_hash = hashlib.sha256(json.dumps(plan, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    body = {'id': event_id(request_id, event['key']),
            'summary': KINDS[event['kind']] + ': ' + event['title'] + ' · ' + request_id[:8],
            'description': 'ANODHAS AI — согласованный план заявки ' + request_id + '.\n'
                           'Рабочая запись студии. Подробности находятся в закрытой CRM.',
            'visibility': 'private', 'transparency': 'opaque' if event['kind'] == 'meeting' else 'transparent',
            'extendedProperties': {'private': {'studio_marker': 'anodhas-calendar-v1',
                'request_id': request_id, 'event_key': event['key'], 'plan_sha256': plan_hash}},
            'reminders': {'useDefault': False, 'overrides': [
                {'method': 'popup', 'minutes': 30 if event['kind'] == 'meeting' else 720}]}}
    if event['kind'] == 'meeting':
        zone = ZoneInfo(plan['timezone'])
        for key in ('start', 'end'):
            body[key] = {'dateTime': local_datetime(event[key], zone).isoformat(), 'timeZone': plan['timezone']}
    else:
        body['start'] = {'date': event['date'], 'timeZone': plan['timezone']}
        body['end'] = {'date': (date.fromisoformat(event['date']) + timedelta(days=1)).isoformat(),
                       'timeZone': plan['timezone']}
    return body


def _same_event(actual, expected):
    if not isinstance(actual, dict) or actual.get('status') != 'confirmed':
        return False
    if any(actual.get(k) for k in ('attendees', 'conferenceData', 'hangoutLink', 'recurrence')):
        return False
    if actual.get('transparency', 'opaque') != expected['transparency']:
        return False
    for key in ('id', 'summary', 'description', 'visibility', 'reminders', 'extendedProperties'):
        if actual.get(key) != expected[key]:
            return False
    for key in ('start', 'end'):
        saved, wanted = actual.get(key), expected[key]
        if not isinstance(saved, dict):
            return False
        if 'date' in wanted:
            # Google may omit timeZone on all-day readback. The date remains exact.
            if saved.get('date') != wanted['date'] or 'dateTime' in saved:
                return False
            if saved.get('timeZone', wanted['timeZone']) != wanted['timeZone']:
                return False
        else:
            if 'date' in saved or saved.get('timeZone') != wanted['timeZone']:
                return False
            try:
                timestamp = datetime.fromisoformat(saved.get('dateTime', '').replace('Z', '+00:00'))
                wanted_timestamp = datetime.fromisoformat(wanted['dateTime'])
                if timestamp.tzinfo is None or timestamp.astimezone(timezone.utc) != wanted_timestamp.astimezone(timezone.utc):
                    return False
            except (ValueError, TypeError):
                return False
    return True


class Calendar:
    def __init__(self, settings, *, journal=None):
        self.settings, self.journal = settings, journal
        self._journal_error = None

    def _record(self, body, phase, **extra):
        if self.journal is not None:
            event = {'service': 'calendar', 'operation': 'insert_event', 'phase': phase,
                     'calendar_id': self.settings.google_calendar_id,
                     'marker': 'anodhas-calendar-v1', 'event_id': body['id'],
                     'event_key': body['extendedProperties']['private']['event_key'],
                     'body_sha256': digest(body), **extra}
            try:
                self.journal(event)
            except BaseException as exc:
                self._journal_error = exc
                raise

    def api(self, method, path, **kwargs):
        return call(self.settings.sheets_credentials_file, 'calendar', method, BASE + path, **kwargs)

    def _metadata(self, calendar_id):
        encoded = quote(calendar_id, safe='')
        entry = self.api('GET', 'users/me/calendarList/' + encoded,
                         params={'fields': 'id,primary,deleted,accessRole,timeZone'})
        calendar = self.api('GET', 'calendars/' + encoded,
                            params={'fields': 'id,summary,description,timeZone'})
        if (not isinstance(entry, dict) or entry.get('id') != calendar_id
                or entry.get('primary') or entry.get('deleted') or entry.get('accessRole') != 'owner'
                or not isinstance(calendar, dict) or calendar.get('id') != calendar_id
                or calendar.get('summary') != CALENDAR_SUMMARY
                or calendar.get('description') != CALENDAR_DESCRIPTION):
            raise Rejected('calendar_target_unverified', 503)
        try:
            if not entry.get('timeZone') or entry.get('timeZone') != calendar.get('timeZone'):
                raise ValueError()
            ZoneInfo(entry['timeZone'])
        except (ValueError, KeyError, TypeError):
            raise Rejected('calendar_target_unverified', 503) from None
        return encoded

    def _get(self, path):
        try:
            return self.api('GET', path, params={'fields': FIELDS})
        except Rejected as exc:
            if exc.code == 'calendar_event_not_found':
                return None
            raise

    def create(self, payload):
        plan = normalize_plan(payload.get('calendar_plan', {}))
        if not plan:
            return {'verified': True, 'status': 'needs_dates', 'events': []}
        calendar_id = getattr(self.settings, 'google_calendar_id', '')
        if (self.settings.pilot_owner_only or not getattr(self.settings, 'google_calendar_enabled', False)
                or not secondary_calendar_id(calendar_id) or calendar_id == OWNER
                or not self.settings.sheets_credentials_file):
            raise Rejected('calendar_not_configured', 503)
        try:
            request_id = str(UUID(str(payload['id'])))
        except (ValueError, TypeError, KeyError):
            raise Rejected('calendar_request_invalid', 422) from None
        if payload.get('bot_id') != 8884933395:
            raise Rejected('calendar_request_invalid', 422)
        encoded = self._metadata(calendar_id)
        path = 'calendars/' + encoded + '/events'
        expected = [event_body(request_id, plan, event) for event in plan['events']]
        existing = {}
        # Detect any existing/manual conflicts before making the first write.
        for body in expected:
            found = self._get(path + '/' + body['id'])
            if found is not None:
                if not _same_event(found, body):
                    raise Rejected('calendar_event_conflict', 409)
                existing[body['id']] = found
                self._record(body, 'readback_verified', disposition='reused')
        completed = []
        for body in expected:
            identity = body['id']
            write_attempted = False
            try:
                if identity in existing:
                    disposition = 'reused'
                else:
                    self._record(body, 'write_started')
                    write_attempted = True
                    try:
                        inserted = self.api('POST', path, body=body,
                            params={'sendUpdates': 'none', 'fields': 'id'})
                        if not isinstance(inserted, dict) or inserted.get('id') != identity:
                            raise DeliveryUnknown('calendar_create_unknown')
                        disposition = 'created'
                    except Rejected as exc:
                        if exc.code != 'calendar_event_conflict':
                            raise
                        # A concurrent same-ID insert is safe only after exact readback.
                        disposition = 'reused'
                    if disposition == 'created':
                        self._record(body, 'write_acknowledged')
                    saved = self._get(path + '/' + identity)
                    if not _same_event(saved, body):
                        raise DeliveryUnknown('calendar_readback_unknown')
                    self._record(body, 'readback_verified', disposition=disposition)
                completed.append({'id': identity, 'key': body['extendedProperties']['private']['event_key'],
                                  'verified': True, 'status': disposition})
            except (Rejected, DeliveryUnknown) as exc:
                if exc is self._journal_error:
                    raise
                receipt = {'verified': False, 'status': 'partial' if completed else 'not_confirmed',
                           'calendar_id': calendar_id, 'events': list(completed),
                           'in_flight': {'id': identity, 'write_attempted': write_attempted},
                           'error_code': getattr(exc, 'code', 'calendar_delivery_unknown')}
                if completed or isinstance(exc, DeliveryUnknown):
                    error = DeliveryUnknown('calendar_partial_delivery' if completed else 'calendar_delivery_unknown')
                    error.receipt = receipt
                    raise error from None
                exc.receipt = receipt
                raise
        return {'verified': True, 'status': 'synced', 'calendar_id': calendar_id, 'events': completed}

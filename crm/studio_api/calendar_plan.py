"""Explicit client supplied dates only; no inference or language-model parsing."""
from datetime import date, datetime, timezone
import hashlib
import json
import re
import unicodedata
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

DEFAULT_TIMEZONE = 'Europe/Moscow'
KINDS = {'deadline': 'Срок', 'milestone': 'Этап', 'meeting': 'Встреча', 'control': 'Контроль'}
_DATE = re.compile(r'\d{4}-\d{2}-\d{2}\Z')
_TIME = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}\Z')


def _reject():
    # service imports this module when preparing immutable approval snapshots.
    from .service import Rejected
    raise Rejected('calendar_plan_invalid', 422)


def _date(value):
    if not isinstance(value, str) or not _DATE.fullmatch(value):
        _reject()
    try:
        parsed = date.fromisoformat(value)
        # Google all-day end is exclusive: the next day must be representable.
        if parsed == date.max:
            _reject()
        return parsed
    except ValueError:
        _reject()


def local_datetime(value, zone):
    """Reject DST gaps and folds instead of silently selecting an instant."""
    if not isinstance(value, str) or not _TIME.fullmatch(value):
        _reject()
    try:
        naive = datetime.fromisoformat(value)
        candidates = []
        for fold in (0, 1):
            aware = naive.replace(tzinfo=zone, fold=fold)
            back = aware.astimezone(timezone.utc).astimezone(zone)
            if back.replace(tzinfo=None) == naive:
                candidates.append(aware)
        if not candidates or len({v.utcoffset() for v in candidates}) != 1:
            _reject()
        return candidates[0]
    except (ValueError, OverflowError):
        _reject()


def normalize_plan(value):
    if value == {} and isinstance(value, dict):
        return {}
    if not isinstance(value, dict) or set(value) != {'timezone', 'events'}:
        _reject()
    timezone_name = value['timezone']
    if not isinstance(timezone_name, str) or not 1 <= len(timezone_name) <= 80:
        _reject()
    try:
        zone = ZoneInfo(timezone_name)
    except (ZoneInfoNotFoundError, ValueError):
        _reject()
    events = value['events']
    if not isinstance(events, list) or not 1 <= len(events) <= 12:
        _reject()
    normalized, keys = [], set()
    for event in events:
        if not isinstance(event, dict):
            _reject()
        kind, key, title = (event.get(k) for k in ('kind', 'key', 'title'))
        if not isinstance(kind, str) or kind not in KINDS:
            _reject()
        if (not isinstance(key, str) or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,63}', key)
                or key in keys):
            _reject()
        if (not isinstance(title, str) or not 1 <= len(title) <= 120 or title != title.strip()
                or any(unicodedata.category(c).startswith('C') for c in title)):
            _reject()
        item = {'key': key, 'kind': kind, 'title': title}
        if kind == 'meeting':
            if set(event) != {'key', 'kind', 'title', 'start', 'end'}:
                _reject()
            start, end = (local_datetime(event.get(k), zone) for k in ('start', 'end'))
            if end.astimezone(timezone.utc) <= start.astimezone(timezone.utc):
                _reject()
            item.update(start=event['start'], end=event['end'])
        else:
            if set(event) != {'key', 'kind', 'title', 'date'}:
                _reject()
            _date(event.get('date'))
            item['date'] = event['date']
        keys.add(key)
        normalized.append(item)
    return {'timezone': timezone_name, 'events': normalized}


def exact_deadline(text):
    """Recognize a complete ISO/Russian date, optionally prefixed by 'до'."""
    if not isinstance(text, str):
        return None
    text = re.sub(r'^до\s+', '', text.strip(), flags=re.IGNORECASE)
    if re.fullmatch(r'\d{2}\.\d{2}\.\d{4}', text):
        day, month, year = text.split('.')
        text = '-'.join((year, month, day))
    if not _DATE.fullmatch(text):
        return None
    try:
        parsed = date.fromisoformat(text)
        return text if parsed != date.max else None
    except ValueError:
        return None


def parse_schedule(text, deadline=''):
    if not isinstance(text, str) or len(text) > 4000:
        _reject()
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if text.strip().casefold() in {'нет', 'не согласовано'}:
        lines = []
    timezone_name = DEFAULT_TIMEZONE
    if lines and lines[0].casefold().startswith('часовой пояс:'):
        timezone_name = lines.pop(0).split(':', 1)[1].strip()
    events = []
    reverse = {name.casefold(): kind for kind, name in KINDS.items()}
    for line in lines:
        parts = [part.strip() for part in line.split('|')]
        kind = reverse.get(parts[0].casefold())
        if kind is None or len(parts) != (4 if kind == 'meeting' else 3):
            _reject()
        event = {'kind': kind, 'title': parts[1]}
        if kind == 'meeting':
            event.update(start=parts[2].replace(' ', 'T'), end=parts[3].replace(' ', 'T'))
        else:
            event['date'] = parts[2]
        key_hash = hashlib.sha256(json.dumps(event, ensure_ascii=False, sort_keys=True).encode()).hexdigest()[:20]
        event['key'] = kind + '-' + key_hash
        events.append(event)
    deadline_date = exact_deadline(deadline)
    if deadline_date and not any(event['kind'] == 'deadline' for event in events):
        events.insert(0, {'key': 'deadline', 'kind': 'deadline',
                         'title': 'Срок реализации заказа', 'date': deadline_date})
    if not events:
        # Still validate an explicitly supplied timezone even without dates.
        try:
            ZoneInfo(timezone_name)
        except (ZoneInfoNotFoundError, ValueError):
            _reject()
        return {}
    return normalize_plan({'timezone': timezone_name, 'events': events})


def describe_plan(plan):
    plan = normalize_plan(plan)
    if not plan:
        return 'Даты пока не согласованы — события не будут созданы.'
    lines = ['Часовой пояс: ' + plan['timezone']]
    for event in plan['events']:
        when = event.get('date') or (event['start'].replace('T', ' ') + ' — ' + event['end'].replace('T', ' '))
        lines.append(KINDS[event['kind']] + ': ' + event['title'] + ' | ' + when)
    return '\n'.join(lines)

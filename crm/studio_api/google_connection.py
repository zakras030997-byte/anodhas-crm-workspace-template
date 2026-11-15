"""Explicit, read-only connection probe; no application/worker initialization.

Run only with the owner's dedicated OAuth file. The token refresh is the only
POST; every service request is GET. Output contains fixed codes and booleans,
never credentials, spreadsheet values, permissions, messages or event content.
This proves connection metadata only, not live acceptance or delivery.
"""
import argparse
import json
from urllib.parse import quote
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .google_runtime import connection_token, _call_with_token
from .service import Rejected
from .sheets import BOOK, TABS
from .telegram import DeliveryUnknown
from .workspace import ROOT, FOLDER

OWNER = 'contact001@example.invalid'
CALENDAR = 'primary'
DRIVE = 'https://www.googleapis.com/drive/v3/'
SHEETS = 'https://sheets.googleapis.com/v4/spreadsheets/' + BOOK
GMAIL_PROFILE = 'https://gmail.googleapis.com/gmail/v1/users/me/profile'
CALENDAR_ENTRY = 'https://www.googleapis.com/calendar/v3/users/me/calendarList/primary'
FILE_FIELDS = 'id,mimeType,trashed,ownedByMe,isAppAuthorized,capabilities(canEdit,canAddChildren)'
CHECK_NAMES = ('oauth', 'owner', 'client_root', 'lead_sheet', 'calendar')


def _require(condition, code):
    if not condition:
        raise Rejected(code, 503)


class _Probe:
    def __init__(self, token):
        self._token = token

    def get(self, service, url, params):
        value = _call_with_token(self._token, service, 'GET', url, params=params)
        _require(isinstance(value, dict), 'google_metadata_invalid')
        return value

    def owner(self):
        about = self.get('drive', DRIVE + 'about', {'fields': 'user(emailAddress,me)'})
        user = about.get('user', {})
        _require(isinstance(user, dict) and user.get('emailAddress') == OWNER
                 and user.get('me') is True, 'google_owner_mismatch')
        profile = self.get('gmail', GMAIL_PROFILE, {'fields': 'emailAddress'})
        _require(profile.get('emailAddress') == OWNER, 'google_owner_mismatch')

    def file(self, identity, mime, *, folder=False):
        value = self.get('drive', DRIVE + 'files/' + identity, {'fields': FILE_FIELDS})
        _require(value.get('id') == identity and value.get('mimeType') == mime
                 and value.get('trashed') is False, 'google_target_invalid')
        _require(value.get('isAppAuthorized') is True, 'google_target_app_access_unverified')
        capabilities = value.get('capabilities', {})
        _require(isinstance(capabilities, dict) and capabilities.get('canEdit') is True,
                 'google_target_not_writable')
        if folder:
            _require(value.get('ownedByMe') is True and capabilities.get('canAddChildren') is True,
                     'google_client_root_not_owner_writable')
        # permissions.list includes pagination; an incomplete permissions view
        # must never be called private. No principals or email fields requested.
        permissions = self.get('drive', DRIVE + 'files/' + identity + '/permissions',
            {'fields': 'nextPageToken,permissions(type,role)', 'pageSize': 100})
        entries = permissions.get('permissions')
        _require(not permissions.get('nextPageToken') and isinstance(entries, list) and bool(entries),
                 'google_permissions_unverified')
        _require(all(isinstance(p, dict) and p.get('type') in ('user', 'group')
                     and p.get('role') in ('owner', 'organizer', 'fileOrganizer', 'writer', 'commenter', 'reader')
                     for p in entries), 'google_target_not_private')

    def client_root(self):
        self.file(ROOT, FOLDER, folder=True)

    def lead_sheet(self):
        self.file(BOOK, 'application/vnd.google-apps.spreadsheet')
        spreadsheet = self.get('sheets', SHEETS,
            {'fields': 'spreadsheetId,sheets(properties(title,sheetType,gridProperties(columnCount)))',
             'includeGridData': 'false'})
        _require(spreadsheet.get('spreadsheetId') == BOOK, 'google_sheet_target_mismatch')
        sheets = spreadsheet.get('sheets')
        _require(isinstance(sheets, list), 'google_sheet_schema_changed')
        for kind, (title, headers) in TABS.items():
            matches = [s.get('properties') for s in sheets if isinstance(s, dict)
                       and isinstance(s.get('properties'), dict) and s['properties'].get('title') == title]
            _require(len(matches) == 1, 'google_sheet_schema_changed')
            properties = matches[0]
            grid = properties.get('gridProperties', {})
            _require(properties.get('sheetType') == 'GRID' and isinstance(grid, dict)
                     and type(grid.get('columnCount')) is int and grid['columnCount'] >= len(headers),
                     'google_sheet_schema_changed')
            last = 'I' if kind == 'leads' else 'M'
            bounded_range = "'" + title.replace("'", "''") + "'!A1:" + last + '1'
            row = self.get('sheets', SHEETS + '/values/' + quote(bounded_range, safe=''),
                {'fields': 'values', 'valueRenderOption': 'UNFORMATTED_VALUE', 'majorDimension': 'ROWS'})
            _require(row.get('values') == [headers], 'google_sheet_schema_changed')

    def calendar(self):
        # CalendarList metadata needs no permission to read or modify events.
        value = self.get('calendar', CALENDAR_ENTRY,
            {'fields': 'id,primary,accessRole,timeZone'})
        _require(value.get('id') == OWNER and value.get('primary') is True,
                 'google_calendar_target_mismatch')
        _require(value.get('accessRole') == 'owner', 'google_calendar_not_owner_writable')
        timezone = value.get('timeZone')
        _require(isinstance(timezone, str) and bool(timezone), 'google_calendar_timezone_unverified')
        try:
            ZoneInfo(timezone)
        except (ZoneInfoNotFoundError, ValueError):
            raise Rejected('google_calendar_timezone_unverified', 503) from None


def probe_connection(credentials_file):
    """Check fixed targets using explicit dedicated owner OAuth, without writes.

    A successful result verifies granted scopes and read-only metadata. It does
    not send mail, create events/folders/rows, modify settings or run workers.
    """
    result = {'connection_ready': False, 'read_only': True, 'live_acceptance': False,
              'checks': {name: {'status': 'not_checked'} for name in CHECK_NAMES}}

    def checked(name, action):
        try:
            value = action()
        except Rejected as exc:
            # Rejected codes originate in this probe/runtime, never Google text.
            result['checks'][name] = {'status': 'failed', 'code': exc.code}
            return False, None
        except DeliveryUnknown:
            result['checks'][name] = {'status': 'failed', 'code': 'google_connection_unavailable'}
            return False, None
        except Exception:
            result['checks'][name] = {'status': 'failed', 'code': 'google_connection_check_failed'}
            return False, None
        result['checks'][name] = {'status': 'verified'}
        return True, value

    ok, token = checked('oauth', lambda: connection_token(credentials_file))
    if not ok:
        return result
    client = _Probe(token)
    ok, _ = checked('owner', client.owner)
    if not ok:
        return result
    for name in ('client_root', 'lead_sheet', 'calendar'):
        checked(name, getattr(client, name))
    result['connection_ready'] = all(check['status'] == 'verified' for check in result['checks'].values())
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description='Read-only Google runtime metadata check; no live acceptance.')
    parser.add_argument('--credentials-file', required=True, help='Absolute path to dedicated private owner OAuth JSON')
    args = parser.parse_args(argv)
    result = probe_connection(args.credentials_file)
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0 if result['connection_ready'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

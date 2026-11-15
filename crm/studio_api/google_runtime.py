"""Explicit private runtime credentials; never reuse the desktop connector session."""
import json
import os
from pathlib import Path
import stat
import re
from urllib.parse import unquote, urlsplit
import httpx
from .service import Rejected
from .telegram import DeliveryUnknown

SCOPES = {
    # Sheets supports drive.file for the existing file selected in the Picker.
    # Do not request access to every spreadsheet in the owner's account.
    'sheets': ['https://www.googleapis.com/auth/drive.file'],
    'drive': ['https://www.googleapis.com/auth/drive.file'],
    # OwnerMail reads the profile and sent-message headers only, never bodies.
    'gmail': ['https://www.googleapis.com/auth/gmail.send', 'https://www.googleapis.com/auth/gmail.metadata'],
    # App-created secondary calendars only; never all personal calendar events.
    'calendar': ['https://www.googleapis.com/auth/calendar.calendarlist.readonly',
                 'https://www.googleapis.com/auth/calendar.app.created'],
}


def _credentials_info(path_string):
    """Read one owner-private regular file; never include its contents in errors."""
    try:
        if not path_string:
            raise ValueError('Missing explicit path')
        fd = os.open(path_string, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, encoding='utf-8') as stream:
            metadata = os.fstat(stream.fileno())
            if (not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o077
                    or metadata.st_uid != os.geteuid() or metadata.st_size > 65536):
                raise ValueError('Private owner file required')
            info = json.load(stream)
    except (OSError, ValueError, TypeError):
        raise Rejected('google_credentials_permissions', 503) from None
    try:
        if not isinstance(info, dict):
            raise ValueError('Credentials object required')
        if info.get('token_uri', 'https://oauth2.googleapis.com/token') != 'https://oauth2.googleapis.com/token':
            raise ValueError('Unreviewed token endpoint')
        return info
    except (ValueError, TypeError):
        raise Rejected('google_credentials_unavailable', 503) from None


def _refresh(info, scopes, service, *, verify_grants=False):
    try:
        from google.auth.transport.requests import Request
        if info.get('type') == 'authorized_user':
            from google.oauth2.credentials import Credentials
            credentials = Credentials.from_authorized_user_info(info, scopes=scopes)
        elif info.get('type') == 'service_account' and service == 'sheets':
            from google.oauth2.service_account import Credentials
            credentials = Credentials.from_service_account_info(info, scopes=scopes)
        else:
            # My Drive storage and Gmail require the owner's OAuth account.
            raise ValueError('Owner OAuth required')
        credentials.refresh(Request())
        # Google-auth otherwise only warns on partial grants. A read-only
        # connection check must not imply that an ungranted send scope works.
        if verify_grants and (not credentials.granted_scopes
                or not set(scopes).issubset(credentials.granted_scopes)):
            raise Rejected('google_connection_scopes_unverified', 503)
        if not isinstance(credentials.token, str) or not credentials.token:
            raise ValueError('Missing refreshed token')
        return credentials.token
    except Rejected:
        raise
    except Exception:
        raise Rejected('google_credentials_unavailable', 503) from None


def access_token(path_string, service):
    if service not in SCOPES:
        raise Rejected('google_service_rejected', 503)
    info = _credentials_info(path_string)
    scopes = SCOPES[service]
    if info.get('type') == 'service_account' and service == 'sheets':
        # Preserve the existing explicit service-account adapter. It is not
        # part of the new owner's OAuth consent or connection probe.
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
    return _refresh(info, scopes, service, verify_grants=(service == 'calendar'))


def connection_token(path_string):
    """Dedicated OAuth only; refresh all grants once without saving tokens."""
    if not path_string or not Path(path_string).is_absolute():
        raise Rejected('google_credentials_explicit_path_required', 503)
    info = _credentials_info(path_string)
    if info.get('type') != 'authorized_user':
        raise Rejected('google_owner_oauth_required', 503)
    scopes = sorted({scope for scopes in SCOPES.values() for scope in scopes})
    return _refresh(info, scopes, 'connection', verify_grants=True)


def _validate_endpoint(service, method, url):
    allowed = {'drive': ('https://www.googleapis.com/drive/v3/', 'https://www.googleapis.com/upload/drive/v3/'),
               'gmail': ('https://gmail.googleapis.com/gmail/v1/users/me/',),
               'sheets': ('https://sheets.googleapis.com/v4/spreadsheets/',),
               'calendar': ('https://www.googleapis.com/calendar/v3/',)}
    if service not in allowed or not url.startswith(allowed[service]):
        raise Rejected('google_endpoint_rejected', 503)
    if service == 'calendar':
        parsed = urlsplit(url)
        if parsed.query or parsed.fragment or parsed.username or parsed.password:
            raise Rejected('google_endpoint_rejected', 503)
        path = unquote(parsed.path)
        secondary = r'[a-z0-9][a-z0-9_-]{4,160}@group\.calendar\.google\.com'
        metadata = (path == '/calendar/v3/users/me/calendarList/primary'
            or re.fullmatch(r'/calendar/v3/users/me/calendarList/' + secondary, path)
            or re.fullmatch(r'/calendar/v3/calendars/' + secondary, path))
        event = re.fullmatch(r'/calendar/v3/calendars/' + secondary + r'/events/[a-v0-9]{5,1024}', path)
        insert = re.fullmatch(r'/calendar/v3/calendars/' + secondary + r'/events', path)
        if not ((method == 'GET' and (metadata or event)) or (method == 'POST' and insert)):
            raise Rejected('google_calendar_endpoint_rejected', 503)


def _call_with_token(token, service, method, url, *, params=None, body=None, content=None, content_type=None, raw=False):
    _validate_endpoint(service, method, url)
    headers = {'Authorization': 'Bearer ' + token}
    if content_type: headers['Content-Type'] = content_type
    try:
        with httpx.Client(timeout=25, follow_redirects=False) as client:
            response = client.request(method, url, params=params, json=body, content=content, headers=headers)
        if response.status_code >= 500:
            raise DeliveryUnknown('google_server_unknown')
        if service == 'calendar' and '/events/' in unquote(urlsplit(url).path) and method == 'GET' and response.status_code == 404:
            raise Rejected('calendar_event_not_found', 404)
        if service == 'calendar' and method == 'POST' and response.status_code == 409:
            raise Rejected('calendar_event_conflict', 409)
        if response.status_code < 200 or response.status_code >= 300:
            raise Rejected('google_request_rejected', 503)
        return response.content if raw else response.json()
    except (httpx.HTTPError, ValueError):
        raise DeliveryUnknown('google_transport_unknown') from None


def call(path, service, method, url, *, params=None, body=None, content=None, content_type=None, raw=False):
    _validate_endpoint(service, method, url)
    return _call_with_token(access_token(path, service), service, method, url,
        params=params, body=body, content=content, content_type=content_type, raw=raw)

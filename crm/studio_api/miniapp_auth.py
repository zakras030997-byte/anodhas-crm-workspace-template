"""Owner-only Telegram Mini App exchange. Never log or persist raw initData."""
from datetime import timedelta
from hashlib import sha256
import hmac
import json
import re
import secrets
from urllib.parse import parse_qsl

from sqlalchemy.dialects.postgresql import insert

from . import tables as t
from .auth import token_hash
from .service import Rejected, now

MAX_INIT_DATA = 16384
MAX_AGE_SECONDS = 300
FUTURE_SECONDS = 30


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('Duplicate JSON field')
        result[key] = value
    return result


def reject_constant(_value):
    raise ValueError('Invalid JSON constant')


def strict_json(value):
    return json.loads(value, object_pairs_hook=unique_object, parse_constant=reject_constant)


def validate_init_data(value, settings, stamp):
    """Return only a replay fingerprint and expiry after verifying the CRM bot."""
    try:
        if (not isinstance(value,str) or not value or len(value.encode('utf-8')) > MAX_INIT_DATA
                or re.search(r'%(?![0-9a-fA-F]{2})',value)):
            raise ValueError()
        pairs = parse_qsl(value, keep_blank_values=True, strict_parsing=True,
            encoding='utf-8', errors='strict', max_num_fields=32)
        fields = dict(pairs)
        if len(fields) != len(pairs) or not {'hash','auth_date','user'} <= fields.keys():
            raise ValueError()
        if any(not re.fullmatch(r'[a-z][a-z0-9_]{0,63}',key)
               or any(char in item for char in '\r\n\x00') for key,item in pairs):
            raise ValueError()
        supplied_hash = fields.pop('hash')
        if not re.fullmatch(r'[0-9a-f]{64}',supplied_hash):
            raise ValueError()
        # Telegram's bot-token HMAC includes signature when present; only hash
        # is omitted. The Ed25519 third-party verification uses another format.
        check_string = '\n'.join(f'{key}={fields[key]}' for key in sorted(fields))
        secret = hmac.new(b'WebAppData',settings.crm_bot_token.encode(),sha256).digest()
        expected = hmac.new(secret,check_string.encode(),sha256).hexdigest()
        if not hmac.compare_digest(supplied_hash,expected):
            raise ValueError()
        if not re.fullmatch(r'[0-9]{1,12}',fields['auth_date']):
            raise ValueError()
        auth_date = int(fields['auth_date'])
        age = stamp.timestamp() - auth_date
        if age > MAX_AGE_SECONDS or age < -FUTURE_SECONDS:
            raise Rejected('miniapp_data_expired',401)
        user = strict_json(fields['user'])
        if not isinstance(user,dict) or type(user.get('id')) is not int or user.get('is_bot',False) is not False:
            raise ValueError()
        if user['id'] != settings.owner_id:
            raise Rejected('owner_only',403)
        if fields.get('chat_type','sender') not in ('sender','private') or 'receiver' in fields:
            raise Rejected('owner_only',403)
        if 'chat' in fields:
            chat = strict_json(fields['chat'])
            if (not isinstance(chat,dict) or chat.get('type') != 'private'
                    or type(chat.get('id')) is not int or chat['id'] != settings.owner_id):
                raise Rejected('owner_only',403)
    except (ValueError,TypeError,UnicodeError,RecursionError):
        raise Rejected('miniapp_invalid_data',401) from None
    fingerprint = token_hash(str(settings.crm_bot_id)+':'+expected)
    return fingerprint, stamp + timedelta(seconds=MAX_AGE_SECONDS-age)


class MiniAppAuth:
    def __init__(self,engine,settings):
        self.engine,self.settings = engine,settings

    def authenticate(self,init_data,origin):
        if not self.settings.crm_bot_enabled:
            raise Rejected('miniapp_not_configured',503)
        if origin != self.settings.origin:
            raise Rejected('csrf_rejected',403)
        stamp = now()
        fingerprint,expiry = validate_init_data(init_data,self.settings,stamp)
        token = secrets.token_urlsafe(48)
        with self.engine.begin() as conn:
            # Unique insertion and session creation commit together across workers.
            # A lost HTTP response never permits exchanging the same data again.
            conn.execute(t.miniapp_logins.delete().where(t.miniapp_logins.c.expires_at < stamp))
            consumed = conn.scalar(insert(t.miniapp_logins).values(data_hash=fingerprint,
                bot_id=self.settings.crm_bot_id,owner_id=self.settings.owner_id,
                created_at=stamp,expires_at=expiry).on_conflict_do_nothing(
                    index_elements=[t.miniapp_logins.c.data_hash]).returning(t.miniapp_logins.c.data_hash))
            if not consumed:
                raise Rejected('miniapp_already_used',409)
            conn.execute(t.sessions.insert().values(token_hash=token_hash(token),owner_id=self.settings.owner_id,
                created_at=stamp,last_seen_at=stamp,expires_at=stamp+timedelta(hours=12)))
        return token

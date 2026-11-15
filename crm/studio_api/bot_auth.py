"""Separate owner login: browser binding + one-time secret delivered only in Telegram.

This is not OIDC and never acts as an automatic fallback from failed OIDC.
A forwarded deep link alone cannot log its creator in: the owner must also
transfer the one-time code from their private chat into the original browser.
"""
from datetime import timedelta
from hashlib import sha256
import hmac
import re
import secrets
import sqlalchemy as sa
from . import tables as t
from .auth import token_hash
from .service import now, Rejected

class BotAuth:
    def __init__(self, engine, settings, telegram):
        self.engine, self.settings, self.telegram = engine, settings, telegram

    def enabled(self):
        return bool(self.settings.bot_login_enabled and self.settings.bot_token)

    def start(self, origin):
        if not self.enabled():
            raise Rejected('bot_login_not_configured',503)
        if origin != self.settings.origin:
            raise Rejected('csrf_rejected',403)
        challenge, binding = secrets.token_urlsafe(24), secrets.token_urlsafe(32)
        with self.engine.begin() as conn:
            conn.execute(sa.select(sa.func.pg_advisory_xact_lock(-888493339501)))
            conn.execute(t.bot_logins.delete().where(t.bot_logins.c.expires_at < now()))
            if conn.scalar(sa.select(sa.func.count()).select_from(t.bot_logins)) >= 30:
                raise Rejected('login_rate_limited',429)
            conn.execute(t.bot_logins.insert().values(challenge_hash=token_hash(challenge),
                binding_hash=token_hash(binding),bot_id=self.settings.bot_id,owner_id=self.settings.owner_id,
                state='pending',failures=0,expires_at=now()+timedelta(minutes=5)))
        return {'challenge':challenge,'url':f'https://t.me/{self.settings.bot_username}?start=login_{challenge}'},binding

    def _row(self, conn, challenge):
        if not re.fullmatch(r'[A-Za-z0-9_-]{32}',challenge):
            raise Rejected('invalid_login',401)
        row=conn.execute(sa.select(t.bot_logins).where(t.bot_logins.c.challenge_hash==token_hash(challenge)).with_for_update()).mappings().first()
        if not row or row['bot_id']!=self.settings.bot_id or row['owner_id']!=self.settings.owner_id:
            raise Rejected('invalid_login',401)
        return row

    def entry(self, user_id, chat_id):
        if not self.enabled() or user_id!=self.settings.owner_id or chat_id!=user_id:
            raise Rejected('owner_only',403)
        self.telegram.call('sendMessage',{'chat_id':chat_id,'parse_mode':'HTML',
            'text':'<b>Кабинет владельца ANODHAS AI</b>\n\nОткройте кабинет и нажмите «Получить код через Telegram».',
            'link_preview_options':{'is_disabled':True},
            'reply_markup':{'inline_keyboard':[[{'text':'Открыть кабинет','url':self.settings.origin+'/admin/login/'}]]}})

    def prompt(self, user_id, chat_id, challenge):
        return self._issue(user_id,chat_id,challenge)

    def confirm(self, user_id, chat_id, message_id, challenge, cancel=False):
        # Retain support for an already delivered prompt from the previous version.
        return self._issue(user_id,chat_id,challenge,message_id=message_id,cancel=cancel,legacy=True)

    def _issue(self, user_id, chat_id, challenge, message_id=None, cancel=False, legacy=False):
        if not self.enabled() or user_id!=self.settings.owner_id or chat_id!=user_id:
            raise Rejected('owner_only',403)
        code=f'{secrets.randbelow(100000000):08d}'
        with self.engine.begin() as conn:
            row=self._row(conn,challenge)
            if row['expires_at']<=now():
                raise Rejected('login_expired',401)
            if legacy:
                if row['state']!='prompted' or row['confirmation_message_id']!=message_id:
                    raise Rejected('invalid_login',401)
            elif row['state'] not in ('pending','prompted'):
                raise Rejected('login_already_used',401)
            conn.execute(t.bot_logins.update().where(t.bot_logins.c.challenge_hash==row['challenge_hash']).values(
                state='cancelled' if cancel else 'code_sending',
                code_hash=None if cancel else self._code_hash(challenge,code),expires_at=now()+timedelta(minutes=2)))
        if cancel:
            return
        result=self.telegram.call('sendMessage',{'chat_id':chat_id,
            'parse_mode':'HTML','link_preview_options':{'is_disabled':True},
            'text':f'<b>Вход в кабинет ANODHAS AI</b>\n\n<b>Одноразовый код</b>\n<pre>{code}</pre>\n\nКод действует 2 минуты. Откройте кабинет в том же браузере, где начали вход, и введите код.\n\nПодтверждайте только вход, который начали сами. Никому не пересылайте код.',
            'reply_markup':{'inline_keyboard':[[{'text':'Ввести код в кабинете','url':self.settings.origin+'/admin/login/?challenge='+challenge}]]}})
        if result.get('chat',{}).get('id')!=chat_id or not result.get('message_id'):
            raise Rejected('login_delivery_unknown',503)
        with self.engine.begin() as conn:
            conn.execute(t.bot_logins.update().where(t.bot_logins.c.challenge_hash==token_hash(challenge),t.bot_logins.c.state=='code_sending').values(state='code_sent',confirmation_message_id=result['message_id']))

    def _code_hash(self, challenge, code):
        return hmac.new(self.settings.session_secret.encode(),('bot-login:'+challenge+':'+code).encode(),sha256).hexdigest()

    def _browser_row(self, conn, challenge, binding, origin):
        if not self.enabled() or origin!=self.settings.origin:
            raise Rejected('invalid_login',401)
        if not binding or len(binding)>256:
            raise Rejected('login_binding_mismatch',401)
        row=self._row(conn,challenge)
        if not hmac.compare_digest(row['binding_hash'],token_hash(binding)):
            raise Rejected('login_binding_mismatch',401)
        if row['expires_at']<=now():
            raise Rejected('login_expired',401)
        return row

    def resume(self, challenge, binding, origin):
        with self.engine.begin() as conn:
            row=self._browser_row(conn,challenge,binding,origin)
            return {'state':row['state'],'expires_at':row['expires_at'].isoformat()}

    def finish(self, challenge, code, binding, origin):
        failed=False
        with self.engine.begin() as conn:
            row=self._browser_row(conn,challenge,binding,origin)
            if row['state'] in ('consumed','cancelled'):
                raise Rejected('login_already_used',401)
            if row['state']!='code_sent':
                raise Rejected('login_code_not_ready',409)
            if row['failures']>=3:
                raise Rejected('login_attempts_exhausted',401)
            if not re.fullmatch(r'[0-9]{8}',code) or not hmac.compare_digest(row['code_hash'],self._code_hash(challenge,code)):
                conn.execute(t.bot_logins.update().where(t.bot_logins.c.challenge_hash==row['challenge_hash']).values(failures=row['failures']+1))
                failed=True
            else:
                token,stamp=secrets.token_urlsafe(48),now()
                conn.execute(t.bot_logins.update().where(t.bot_logins.c.challenge_hash==row['challenge_hash']).values(state='consumed',code_hash=None))
                conn.execute(t.sessions.insert().values(token_hash=token_hash(token),owner_id=self.settings.owner_id,
                    created_at=stamp,last_seen_at=stamp,expires_at=stamp+timedelta(hours=12)))
        if failed:
            raise Rejected('login_code_incorrect',401)
        return token

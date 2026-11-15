"""Separate owner-only entry/OTP handler; no intake, AI or client dispatch."""
import re

from sqlalchemy.dialects.postgresql import insert

from . import tables as t
from .service import Rejected, now


class CrmBot:
    def __init__(self,engine,settings,telegram,bot_auth):
        self.engine,self.settings,self.telegram,self.bot_auth = engine,settings,telegram,bot_auth

    def handle(self,update):
        if not self.settings.crm_bot_enabled:
            return {'accepted':False,'reason':'crm_bot_disabled'}
        if not isinstance(update,dict) or type(update.get('update_id')) is not int:
            raise Rejected('invalid_update',422)
        message = update.get('message')
        if not isinstance(message,dict):
            return {'accepted':False,'reason':'owner_message_required'}
        sender,chat = message.get('from'),message.get('chat')
        if (not isinstance(sender,dict) or not isinstance(chat,dict)
                or type(sender.get('id')) is not int or type(chat.get('id')) is not int
                or sender['id'] != self.settings.owner_id or chat['id'] != self.settings.owner_id
                or chat.get('type') != 'private' or sender.get('is_bot',False) is not False):
            return {'accepted':False,'reason':'owner_only'}
        text = message.get('text','')
        if not isinstance(text,str):
            return {'accepted':False,'reason':'unsupported_command'}
        parts = text.strip().split(maxsplit=1)
        command = parts[0] if parts else ''
        if '@' in command:
            command,username = command.split('@',1)
            if username.lower() != self.settings.crm_bot_username.lower():
                return {'accepted':False,'reason':'unsupported_command'}
        argument = parts[1] if len(parts) > 1 else ''
        challenge = argument[6:] if command == '/start' and argument.startswith('login_') else None
        if challenge is not None:
            if not re.fullmatch(r'[A-Za-z0-9_-]{32}',challenge):
                raise Rejected('invalid_login',401)
        elif command not in ('/start','/login','/admin') or argument:
            return {'accepted':False,'reason':'unsupported_command'}
        # Consume before sending: uncertain delivery must never be resent by a
        # duplicate webhook. OTP has its own durable one-shot state machine.
        with self.engine.begin() as conn:
            accepted = conn.scalar(insert(t.updates).values(bot_id=self.settings.crm_bot_id,
                update_id=update['update_id'],processed_at=now()).on_conflict_do_nothing(
                    index_elements=[t.updates.c.bot_id,t.updates.c.update_id]).returning(t.updates.c.update_id))
            if accepted is None:
                return {'accepted':True,'duplicate':True}
        if challenge is not None:
            self.bot_auth.prompt(sender['id'],chat['id'],challenge)
        else:
            self.telegram.call('sendMessage',{'chat_id':self.settings.owner_id,
                'text':'Кабинет владельца ANODHAS AI CRM. Откройте приложение для входа.',
                'reply_markup':{'inline_keyboard':[[{'text':'Открыть CRM',
                    'web_app':{'url':self.settings.miniapp_url}}]]}})
        return {'accepted':True,'scope':'owner_crm'}

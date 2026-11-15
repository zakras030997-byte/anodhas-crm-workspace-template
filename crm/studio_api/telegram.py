"""Strict single-bot Bot API transport. Never log tokens, URLs or request bodies."""
import httpx
import logging
from .service import Rejected

class DeliveryUnknown(Exception):
    pass

class Telegram:
    def __init__(self,settings):
        self.settings=settings
        # Transport diagnostics can contain credentials in URL paths or bodies.
        # Filters on parents do not cover descendants: disable every existing
        # transport logger and silence the default levels of future descendants.
        for name in ('httpx','httpcore',*tuple(logging.root.manager.loggerDict)):
            if name=='httpx' or name=='httpcore' or name.startswith(('httpx.','httpcore.')):
                logger=logging.getLogger(name)
                logger.setLevel(logging.CRITICAL+1)
                logger.disabled=True

    def call(self,method,payload,files=None):
        if not self.settings.bot_token:
            raise Rejected('telegram_not_configured',503)
        allowed={'getMe','getWebhookInfo','setWebhook','sendMessage','answerCallbackQuery','sendPhoto','sendVideo','sendDocument','getFile','setMyCommands','setMyDescription','setMyShortDescription'}
        if method not in allowed:
            raise Rejected('telegram_method_not_allowed',422)
        if self.settings.pilot_owner_only and method in {'sendMessage','sendPhoto','sendVideo','sendDocument'}:
            if type(payload.get('chat_id')) is not int or payload['chat_id']!=self.settings.owner_id:
                raise Rejected('pilot_owner_only',403)
        try:
            with httpx.Client(timeout=30,follow_redirects=False) as client:
                url=f'https://api.telegram.org/bot{self.settings.bot_token}/{method}'
                response=client.post(url,data=payload,files=files) if files else client.post(url,json=payload)
                data=response.json()
        except (httpx.HTTPError,ValueError):
            raise DeliveryUnknown('telegram_transport_unknown') from None
        if not isinstance(data,dict) or data.get('ok') is not True:
            # A service 429 is explicit rejection, never sleep/retry automatically.
            if isinstance(data,dict) and data.get('error_code')==429:
                raise Rejected('telegram_rate_limited',429)
            if response.status_code>=500:
                raise DeliveryUnknown('telegram_server_unknown')
            raise Rejected('telegram_rejected',502)
        return data.get('result')

    def verify(self):
        row=self.call('getMe',{})
        if row.get('id')!=self.settings.bot_id or row.get('username')!=self.settings.bot_username or row.get('is_bot') is not True:
            raise Rejected('telegram_identity_mismatch',503)
        return row

    def download(self,file_id):
        info=self.call('getFile',{'file_id':file_id})
        path=info.get('file_path','')
        if not path or path.startswith('/') or '..' in path or ':' in path:
            raise Rejected('invalid_telegram_file',502)
        try:
            with httpx.Client(timeout=45,follow_redirects=False) as client:
                with client.stream('GET',f'https://api.telegram.org/file/bot{self.settings.bot_token}/{path}') as response:
                    response.raise_for_status()
                    data=bytearray()
                    for chunk in response.iter_bytes():
                        data.extend(chunk)
                        if len(data)>20*1024*1024:
                            raise Rejected('file_too_large',413)
                    return bytes(data)
        except httpx.HTTPError:
            raise Rejected('telegram_file_unavailable',503) from None

"""Owner email only, durable one-shot delivery and exact Gmail readback."""
import base64
from datetime import timedelta
from email.message import EmailMessage
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from . import tables as t
from .google_runtime import call
from .service import now, Rejected
from .telegram import DeliveryUnknown

class OwnerMail:
    def __init__(self, settings): self.settings = settings

    def api(self, method, path, **kwargs):
        return call(self.settings.gmail_credentials_file, 'gmail', method,
            'https://gmail.googleapis.com/gmail/v1/users/me/'+path, **kwargs)

    def send(self, p):
        if not self.settings.owner_email_enabled: raise Rejected('owner_email_not_configured', 503)
        profile = self.api('GET', 'profile')
        if profile.get('emailAddress') != self.settings.owner_email:
            raise Rejected('owner_mailbox_mismatch', 403)
        message = EmailMessage()
        message['To'] = message['From'] = self.settings.owner_email
        message['Subject'] = 'ANODHAS — новая заявка №'+str(p['id'])[:8]
        message['Message-ID'] = '<anodhas-request-'+str(p['id'])+'@example.invalid>'
        message.set_content('В Telegram-боте поступила заявка. Необходимо ваше личное рассмотрение.\n\n'
            'Заявка №'+str(p['id'])+'\nОткрыть: '+self.settings.origin+'/admin/?request='+str(p['id'])
            +'\n\nВ базу клиентов она попадёт только после вашего одобрения. Контакты и подробности доступны в закрытом кабинете.')
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
        sent = self.api('POST', 'messages/send', body={'raw': raw})
        message_id = sent.get('id')
        if not isinstance(message_id, str) or not message_id.isalnum(): raise DeliveryUnknown('gmail_send_unknown')
        try:
            after = self.api('GET', 'messages/'+message_id, params={'format': 'metadata'})
            headers = {h['name'].lower():h['value'] for h in after.get('payload',{}).get('headers',[])}
            if after.get('id') != message_id or 'SENT' not in after.get('labelIds', []) or headers.get('to') != self.settings.owner_email or headers.get('subject') != message['Subject']:
                raise DeliveryUnknown('gmail_readback_mismatch')
        except (Rejected,DeliveryUnknown):
            error=DeliveryUnknown('gmail_readback_unknown')
            error.receipt={'message_id':message_id,'verified':False}
            raise error from None
        return {'message_id': message_id, 'verified': True}

class Notifications:
    def __init__(self,engine,settings,mail=None):
        self.engine,self.settings,self.mail=engine,settings,mail or OwnerMail(settings)

    def run(self,limit=5):
        if not self.settings.owner_email_enabled: return []
        results=[]
        for _ in range(limit):
            with self.engine.begin() as conn:
                if not conn.scalar(sa.select(sa.func.pg_try_advisory_xact_lock(-888493339504))): break
                conn.execute(t.notifications.update().where(t.notifications.c.state=='sending',t.notifications.c.updated_at<now()-timedelta(minutes=5)).values(state='unknown',error_code='worker_interrupted',updated_at=now()))
                if conn.scalar(sa.select(sa.func.count()).select_from(t.notifications).where(t.notifications.c.state=='sending')): break
                query=sa.select(t.requests.c.id).where(t.requests.c.bot_id==self.settings.bot_id,t.requests.c.submitted_at.is_not(None),
                    ~sa.exists(sa.select(t.notifications.c.request_id).where(t.notifications.c.request_id==t.requests.c.id)))
                if self.settings.pilot_owner_only: query=query.where(t.requests.c.client_telegram_id==self.settings.owner_id)
                ids=conn.scalars(query.limit(20)).all()
                for id in ids:
                    conn.execute(insert(t.notifications).values(request_id=id,state='pending',result={},created_at=now(),updated_at=now()).on_conflict_do_nothing())
                q=sa.select(t.notifications,t.requests.c.client_telegram_id).join(t.requests,t.requests.c.id==t.notifications.c.request_id).where(t.notifications.c.state=='pending')
                if self.settings.pilot_owner_only: q=q.where(t.requests.c.client_telegram_id==self.settings.owner_id)
                row=conn.execute(q.order_by(t.notifications.c.created_at).limit(1).with_for_update()).mappings().first()
                if not row: break
                id=row['request_id']
                conn.execute(t.notifications.update().where(t.notifications.c.request_id==id).values(state='sending',updated_at=now()))
            state,error,result='confirmed',None,{}
            try:
                result=self.mail.send({'id':id})
                if not isinstance(result,dict) or result.get('verified') is not True:
                    raise DeliveryUnknown('email_readback_not_verified')
            except DeliveryUnknown as exc:
                state,error,result='unknown','email_delivery_unknown',getattr(exc,'receipt',{})
            except Rejected as exc: state,error='failed',exc.code
            with self.engine.begin() as conn:
                conn.execute(t.notifications.update().where(t.notifications.c.request_id==id).values(state=state,error_code=error,result=result,updated_at=now()))
            results.append({'request_id':str(id),'state':state})
        return results

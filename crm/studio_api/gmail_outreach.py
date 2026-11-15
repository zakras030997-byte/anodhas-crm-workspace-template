"""Owner-reviewed single-recipient mail, immutable queue and no uncertain replay."""
import base64
from datetime import timedelta
from email.message import EmailMessage
from email.utils import parseaddr, format_datetime
from hashlib import sha256
import json
import re
from uuid import uuid4
import sqlalchemy as sa
from . import tables as t
from .service import CRM, Rejected, digest, now
from .telegram import DeliveryUnknown
from .google_runtime import _credentials_info, _refresh, _call_with_token, SCOPES


def recipient(value, *, empty=False):
    if not isinstance(value, str): raise Rejected('email_recipient_invalid', 422)
    value = value.strip()
    if empty and not value: return ''
    if len(value) > 254 or value.count('@') != 1 or any(ord(c)<33 or ord(c)>126 for c in value):
        raise Rejected('email_recipient_invalid', 422)
    local, domain = value.rsplit('@', 1)
    if not local or len(local)>64 or local.startswith('.') or local.endswith('.') or '..' in local \
            or not re.fullmatch(r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+", local):
        raise Rejected('email_recipient_invalid', 422)
    labels = domain.split('.')
    if len(labels)<2 or any(not re.fullmatch(r'[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?', x) for x in labels):
        raise Rejected('email_recipient_invalid', 422)
    return local + '@' + domain.lower()


def content(to, subject, body, *, sending=False):
    to = recipient(to, empty=not sending)
    if not isinstance(subject, str) or len(subject)>200 or any(ord(c)<32 or ord(c)==127 for c in subject):
        raise Rejected('email_subject_invalid', 422)
    if not isinstance(body, str) or len(body)>20000 or any((ord(c)<32 and c not in '\n\r\t') or ord(c)==127 for c in body):
        raise Rejected('email_body_invalid', 422)
    subject, body = subject.strip(), body.replace('\r\n','\n').replace('\r','\n').strip()
    if sending and (not subject or not body): raise Rejected('email_content_required', 422)
    return {'recipient':to, 'subject':subject, 'body':body}


def suggested_contact(row):
    value = row.get('contact') or ''
    try:
        fields = json.loads(value)
        value = fields.get('email','') if isinstance(fields,dict) else ''
    except (ValueError,TypeError): pass
    try: return recipient(value)
    except Rejected: return ''


def template(row):
    label = re.sub(r'\s+', ' ', row.get('service_label') or 'Автоматизация процессов').strip()[:100]
    return {'subject': 'ANODHAS AI: ' + label,
        'body': 'Здравствуйте!\n\nЯ owner, ANODHAS AI. Разрабатываю AI-ассистентов, ботов и автоматизацию для бизнеса.\n\n'
        'Хочу обсудить задачу по направлению «' + label + '». Какой процесс вы хотите улучшить и какой результат получить?\n\n'
        'owner\nANODHAS AI\nhttps://example.invalid\nTelegram: @owner'}


class GmailOutreach:
    def __init__(self, settings): self.settings = settings

    def connect(self):
        if not self.settings.gmail_outreach_enabled or not self.settings.gmail_credentials_file:
            raise Rejected('gmail_not_configured',503)
        token = _refresh(_credentials_info(self.settings.gmail_credentials_file), SCOPES['gmail'], 'gmail', verify_grants=True)
        profile = self.api(token, 'GET', 'profile', params={'fields':'emailAddress'})
        if not isinstance(profile,dict) or profile.get('emailAddress') != self.settings.owner_email:
            raise Rejected('gmail_owner_mismatch',403)
        return token

    def api(self, token, method, path, **kwargs):
        return _call_with_token(token, 'gmail', method, 'https://gmail.googleapis.com/gmail/v1/users/me/'+path, **kwargs)

    @staticmethod
    def message_id(delivery): return '<anodhas-mail-'+str(delivery['id'])+'@example.invalid>'

    def verify(self, delivery, message_id, *, token=None):
        if not isinstance(message_id,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', message_id):
            raise DeliveryUnknown('gmail_receipt_missing')
        token = token or self.connect()
        found = self.api(token,'GET','messages/'+message_id,params={'format':'metadata',
            'metadataHeaders':['From','To','Subject','Message-ID']})
        if not isinstance(found,dict): raise DeliveryUnknown('gmail_readback_mismatch')
        entries=found.get('payload',{}).get('headers',[])
        headers={}
        for entry in entries:
            key=entry.get('name','').lower()
            if key in headers: raise DeliveryUnknown('gmail_readback_mismatch')
            headers[key]=entry.get('value','')
        if found.get('id') != message_id or 'SENT' not in found.get('labelIds',[]) \
                or parseaddr(headers.get('from',''))[1].lower() != self.settings.owner_email.lower() \
                or headers.get('to','') != delivery['recipient'] \
                or headers.get('subject','') != delivery['subject']:
            raise DeliveryUnknown('gmail_readback_mismatch')
        # Gmail may rewrite the RFC Message-ID. Correlate with the provider ID
        # saved from this single POST, plus the exact sender, recipient and subject.
        return {'message_id':message_id,'verified':True}

    def send(self, delivery, save_receipt):
        # All potentially failing local/preflight operations precede the single POST.
        values=content(delivery['recipient'],delivery['subject'],delivery['body'],sending=True)
        token=self.connect()
        message=EmailMessage()
        message['From']=self.settings.owner_email
        message['To']=values['recipient']
        message['Subject']=values['subject']
        message['Message-ID']=self.message_id(delivery)
        message['Date']=format_datetime(delivery['created_at'])
        message.set_content(values['body'])
        sent=self.api(token,'POST','messages/send',body={'raw':base64.urlsafe_b64encode(message.as_bytes()).decode()})
        message_id=sent.get('id') if isinstance(sent,dict) else None
        if not isinstance(message_id,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',message_id):
            raise DeliveryUnknown('gmail_send_unknown')
        # Persist Google's ID before a slower verification or any further request.
        save_receipt({'message_id':message_id,'verified':False})
        try: return self.verify(delivery,message_id,token=token)
        except Exception:
            raise DeliveryUnknown('gmail_readback_unknown') from None


class MailService:
    def __init__(self,engine,settings,mail=None):
        self.engine,self.settings=engine,settings
        self.crm=CRM(engine,settings.owner_id,settings=settings)
        self.mail=mail or GmailOutreach(settings)

    def enabled(self):
        if not self.settings.gmail_outreach_enabled: raise Rejected('gmail_not_configured',503)

    def status(self):
        result={'enabled':self.settings.gmail_outreach_enabled,'connected':False,'sender':self.settings.owner_email}
        if not self.settings.gmail_outreach_enabled: return dict(result,error_code='gmail_not_configured')
        try: self.mail.connect(); return dict(result,connected=True)
        except Rejected as e: return dict(result,error_code=e.code)
        except Exception: return dict(result,error_code='gmail_connection_unavailable')

    @staticmethod
    def public(row):
        return {key:row[key] for key in ('id','request_id','draft_revision','recipient','subject','body','state','result','error_code','created_at','updated_at')}

    def detail(self,request_id):
        self.enabled()
        with self.engine.connect() as conn:
            row=self.crm._request(conn,request_id,lock=False)
            draft=conn.execute(sa.select(t.email_drafts).where(t.email_drafts.c.request_id==request_id)).mappings().first()
            history=conn.execute(sa.select(t.email_deliveries).where(t.email_deliveries.c.request_id==request_id)
                .order_by(t.email_deliveries.c.created_at.desc()).limit(100)).mappings().all()
            return {'draft':dict(draft) if draft else {'revision':0,'recipient':'','subject':'','body':''},
                'suggested_recipient':suggested_contact(row),'template':template(row),'history':[self.public(x) for x in history]}

    def save_draft(self,request_id,revision,recipient,subject,body):
        self.enabled(); values=content(recipient,subject,body)
        with self.engine.begin() as conn:
            self.crm._request(conn,request_id)
            draft=conn.execute(sa.select(t.email_drafts).where(t.email_drafts.c.request_id==request_id)).mappings().first()
            actual=draft['revision'] if draft else 0
            if actual!=revision: raise Rejected('email_draft_changed')
            if draft and all(draft[k]==v for k,v in values.items()): return dict(draft)
            values.update(revision=actual+1,updated_at=now())
            if draft: conn.execute(t.email_drafts.update().where(t.email_drafts.c.request_id==request_id).values(**values))
            else: conn.execute(t.email_drafts.insert().values(request_id=request_id,**values))
            return values

    def queue(self,request_id,revision,operation_key):
        self.enabled(); command=digest([str(request_id),revision])
        with self.engine.begin() as conn:
            lock_id=int.from_bytes(sha256(('mail:'+str(operation_key)).encode()).digest()[:8],signed=True)
            conn.execute(sa.select(sa.func.pg_advisory_xact_lock(lock_id)))
            row=self.crm._request(conn,request_id)
            repeated=conn.execute(sa.select(t.email_operations).where(t.email_operations.c.operation_key==operation_key)).mappings().first()
            if repeated:
                if repeated['request_hash']!=command: raise Rejected('operation_key_conflict')
                return self.public(conn.execute(sa.select(t.email_deliveries).where(t.email_deliveries.c.id==repeated['delivery_id'])).mappings().one())
            # New click IDs for the same saved revision return the original delivery.
            existing=conn.execute(sa.select(t.email_deliveries).where(t.email_deliveries.c.request_id==request_id,
                t.email_deliveries.c.draft_revision==revision)).mappings().first()
            if not existing:
                draft=conn.execute(sa.select(t.email_drafts).where(t.email_drafts.c.request_id==request_id)).mappings().first()
                if not draft or draft['revision']!=revision: raise Rejected('email_draft_changed')
                if not row['submitted_at']: raise Rejected('request_not_submitted')
                values=content(draft['recipient'],draft['subject'],draft['body'],sending=True)
                payload_hash=digest(values)
                existing=conn.execute(sa.select(t.email_deliveries).where(t.email_deliveries.c.request_id==request_id,
                    t.email_deliveries.c.payload_hash==payload_hash)).mappings().first()
                if not existing:
                    pending=conn.scalar(sa.select(sa.func.count()).select_from(t.email_deliveries).where(t.email_deliveries.c.request_id==request_id,
                        t.email_deliveries.c.state.in_(['pending','sending','unknown'])))
                    if pending: raise Rejected('email_delivery_unresolved')
                    stamp=now()
                    existing=dict(id=uuid4(),request_id=request_id,draft_revision=revision,payload_hash=payload_hash,
                        owner_id=self.settings.owner_id,**values,state='pending',result={},error_code=None,created_at=stamp,updated_at=stamp)
                    conn.execute(t.email_deliveries.insert().values(**existing))
            conn.execute(t.email_operations.insert().values(operation_key=operation_key,request_hash=command,delivery_id=existing['id'],created_at=now()))
            return self.public(existing)

    def check(self,request_id,delivery_id):
        self.enabled()
        with self.engine.connect() as conn:
            self.crm._request(conn,request_id,lock=False)
            delivery=conn.execute(sa.select(t.email_deliveries).where(t.email_deliveries.c.id==delivery_id,
                t.email_deliveries.c.request_id==request_id)).mappings().first()
            if not delivery: raise Rejected('not_found',404)
            delivery=dict(delivery)
        if delivery['state'] in ('confirmed','failed','pending','sending'): return self.public(delivery)
        message_id=delivery['result'].get('message_id')
        if not message_id: raise Rejected('gmail_receipt_missing')
        result=self.mail.verify(delivery,message_id)
        if result.get('verified') is not True: raise DeliveryUnknown('gmail_readback_unknown')
        with self.engine.begin() as conn:
            conn.execute(t.email_deliveries.update().where(t.email_deliveries.c.id==delivery_id,t.email_deliveries.c.state=='unknown')
                .values(state='confirmed',result=result,error_code=None,updated_at=now()))
            return self.public(conn.execute(sa.select(t.email_deliveries).where(t.email_deliveries.c.id==delivery_id)).mappings().one())

    def operation(self,request_id,operation_key):
        self.enabled()
        with self.engine.connect() as conn:
            self.crm._request(conn,request_id,lock=False)
            delivery=conn.execute(sa.select(t.email_deliveries).join(t.email_operations,
                t.email_operations.c.delivery_id==t.email_deliveries.c.id).where(
                t.email_operations.c.operation_key==operation_key,t.email_deliveries.c.request_id==request_id)).mappings().first()
            if not delivery: raise Rejected('not_found',404)
            return self.public(delivery)

    def run(self,limit=3):
        if not self.settings.gmail_outreach_enabled: return []
        results=[]
        for _ in range(limit):
            with self.engine.begin() as conn:
                if not conn.scalar(sa.select(sa.func.pg_try_advisory_xact_lock(-888493339506))): break
                conn.execute(t.email_deliveries.update().where(t.email_deliveries.c.state=='sending',
                    t.email_deliveries.c.updated_at<now()-timedelta(minutes=5)).values(state='unknown',error_code='worker_interrupted',updated_at=now()))
                if conn.scalar(sa.select(sa.func.count()).select_from(t.email_deliveries).where(t.email_deliveries.c.state=='sending')): break
                row=conn.execute(sa.select(t.email_deliveries).where(t.email_deliveries.c.state=='pending',
                    t.email_deliveries.c.owner_id==self.settings.owner_id).order_by(t.email_deliveries.c.created_at).limit(1).with_for_update()).mappings().first()
                if not row: break
                delivery=dict(row)
                conn.execute(t.email_deliveries.update().where(t.email_deliveries.c.id==row['id']).values(state='sending',updated_at=now()))
            def save_receipt(result):
                with self.engine.begin() as conn:
                    conn.execute(t.email_deliveries.update().where(t.email_deliveries.c.id==delivery['id'],t.email_deliveries.c.state.in_(['sending','unknown']))
                        .values(result=result,updated_at=now()))
            state,error,result='confirmed',None,None
            try:
                values=content(delivery['recipient'],delivery['subject'],delivery['body'],sending=True)
                if digest(values)!=delivery['payload_hash']: raise Rejected('email_payload_changed')
                result=self.mail.send(delivery,save_receipt)
                if not isinstance(result,dict) or result.get('verified') is not True: raise DeliveryUnknown('gmail_readback_unknown')
            except Rejected as exc: state,error='failed',exc.code
            except Exception: state,error='unknown','email_delivery_unknown'
            with self.engine.begin() as conn:
                values={'state':state,'error_code':error,'updated_at':now()}
                if state=='confirmed': values['result']=result
                conn.execute(t.email_deliveries.update().where(t.email_deliveries.c.id==delivery['id'],t.email_deliveries.c.state.in_(['sending','unknown'])).values(**values))
            results.append({'id':str(delivery['id']),'state':state})
        return results

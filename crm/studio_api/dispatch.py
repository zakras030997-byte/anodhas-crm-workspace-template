"""One-shot external worker. Unknown deliveries require human review, not retries."""
from datetime import timedelta
from uuid import UUID,uuid4
import sqlalchemy as sa
from . import tables as t
from .service import now,digest,request_payload,Rejected
from .telegram import DeliveryUnknown
from .media import manifest
from .outbox import BotOutbox

class Dispatcher:
    def __init__(self,engine,settings,telegram,media,sheets):
        self.engine,self.settings,self.telegram,self.media,self.sheets=engine,settings,telegram,media,sheets

    def _validate(self,conn,job):
        p=job['payload']
        row=conn.execute(sa.select(t.requests).where(t.requests.c.id==job['request_id'],t.requests.c.bot_id==self.settings.bot_id).with_for_update()).mappings().first()
        if not row or job['owner_id']!=self.settings.owner_id or p.get('bot_id')!=self.settings.bot_id or digest(p)!=job['payload_hash']:
            raise Rejected('job_binding_changed')
        if self.settings.pilot_owner_only:
            if job['kind']=='lead_create':
                raise Rejected('sheets_disabled_in_pilot',503)
            if row['client_telegram_id']!=self.settings.owner_id or p.get('recipient_id')!=self.settings.owner_id:
                raise Rejected('pilot_owner_only',403)
        if job['kind']=='lead_create':
            d=conn.execute(sa.select(t.decisions).where(t.decisions.c.request_id==row['id'])).mappings().first()
            original=request_payload(p)
            if not d or d['decision']!='approved' or d['owner_id']!=self.settings.owner_id or digest(original)!=d['payload_hash'] or digest(request_payload(row))!=d['payload_hash']:
                raise Rejected('decision_requires_review')
        elif job['kind']=='telegram_send':
            verified=conn.scalar(sa.select(t.chat_state.c.chat_id).where(t.chat_state.c.bot_id==self.settings.bot_id,t.chat_state.c.chat_id==row['client_telegram_id']))
            if not row['submitted_at'] or not verified or p['recipient_id']!=row['client_telegram_id']:
                raise Rejected('verified_chat_required')
            if manifest(conn,row['id'],self.settings.bot_id,p['attachment_ids'])!=p['attachments']:
                raise Rejected('attachment_changed')
        else: raise Rejected('invalid_job_kind')

    def run(self,limit=10,*,job_id=None):
        if not self.settings.dispatch_enabled: raise Rejected('dispatcher_not_configured',503)
        bot_notices=BotOutbox(self.engine,self.settings,self.telegram).flush() if job_id is None else 0
        results=[]
        for _ in range(limit):
            with self.engine.begin() as conn:
                if not conn.scalar(sa.select(sa.func.pg_try_advisory_xact_lock(-888493339503))): break
                conn.execute(t.jobs.update().where(t.jobs.c.state=='sending',t.jobs.c.updated_at<now()-timedelta(minutes=10)).values(state='unknown',error_code='worker_interrupted',updated_at=now()))
                if conn.scalar(sa.select(sa.func.count()).select_from(t.jobs).where(t.jobs.c.state=='sending')): break
                row=conn.execute(sa.select(t.jobs).where(t.jobs.c.state=='pending', *([t.jobs.c.id==job_id] if job_id else [])).order_by(t.jobs.c.created_at).limit(1).with_for_update(skip_locked=True)).mappings().first()
                if not row: break
                job=dict(row)
                try:
                    self._validate(conn,job)
                    if job['kind']=='lead_create' and not self.settings.sheets_enabled:
                        raise Rejected('sheets_not_configured',503)
                    if job['kind']=='lead_create' and job['payload'].get('calendar_plan') and not self.settings.google_calendar_enabled:
                        raise Rejected('calendar_not_configured',503)
                except Rejected as exc:
                    conn.execute(t.jobs.update().where(t.jobs.c.id==job['id']).values(state='needs_review',error_code=exc.code,updated_at=now()))
                    results.append({'id':str(job['id']),'state':'needs_review'});continue
                conn.execute(t.jobs.update().where(t.jobs.c.id==job['id']).values(state='sending',updated_at=now()))
            state,error,result='confirmed',None,None
            try:
                if job['kind']=='lead_create':
                    result=self.sheets.create_lead(job['payload'])
                    if not isinstance(result,dict) or result.get('verified') is not True:
                        raise DeliveryUnknown('lead_readback_not_verified')
                    if self.settings.google_workspace_enabled and not job['payload'].get('leadgen') and (not isinstance(result.get('workspace'),dict) or result['workspace'].get('verified') is not True):
                        raise DeliveryUnknown('workspace_readback_not_verified')
                    # Persist the confirmed Drive/Sheets receipt BEFORE entering
                    # another service. A crash can never erase prior delivery.
                    if self.settings.google_calendar_enabled and not job['payload'].get('leadgen'):
                        with self.engine.begin() as conn:
                            conn.execute(t.jobs.update().where(t.jobs.c.id==job['id']).values(
                                result={**result,'calendar':{'verified':False,'status':'in_flight'}},updated_at=now()))
                        try:
                            from .calendar_sync import Calendar
                            calendar = Calendar(self.settings).create(job['payload'])
                            if not isinstance(calendar,dict) or calendar.get('verified') is not True:
                                raise DeliveryUnknown('calendar_readback_not_verified')
                            result = {**result,'calendar':calendar}
                        except (Rejected,DeliveryUnknown) as exc:
                            partial = DeliveryUnknown('lead_calendar_partial_delivery')
                            partial.receipt = {**result,'verified':False,'failed_step':'calendar',
                                'calendar':getattr(exc,'receipt',{'verified':False}),
                                'error_code':getattr(exc,'code','delivery_unknown')}
                            raise partial from None
                else: result=self._send(job)
            except DeliveryUnknown as exc:
                state,error='unknown','delivery_unknown'
                if getattr(exc,'receipt',None): result=exc.receipt
            except Rejected as exc:
                state,error='failed',exc.code
            # Unexpected worker crash deliberately leaves sending; recovery marks
            # it unknown. It never resumes external calls automatically.
            with self.engine.begin() as conn:
                fields=dict(state=state,error_code=error,updated_at=now())
                if result is not None: fields['result']=result
                conn.execute(t.jobs.update().where(t.jobs.c.id==job['id']).values(**fields))
                if state=='confirmed' and job['kind']=='lead_create':
                    conn.execute(t.requests.update().where(t.requests.c.id==job['request_id']).values(google_sync_status='synced'))
            results.append({'id':str(job['id']),'state':state})
        return {'jobs':results,'bot_notices_processed':bot_notices}

    def _send(self,job):
        p=job['payload'];parts=[]
        if p['body'].strip(): parts.append(('sendMessage',{'text':p['body']},None,None))
        # Validate every byte before making the first external call.
        for a in p['attachments']:
            data=self.media.read_local(a)
            key={'photo':'photo','video':'video','document':'document'}[a['kind']]
            method={'photo':'sendPhoto','video':'sendVideo','document':'sendDocument'}[key]
            parts.append((method,{}, {key:(a['filename'],data,a['mime'])},a['id']))
        confirmations=[]
        for index,(method,payload,files,attachment_id) in enumerate(parts):
            # Journal the boundary before each part; a partial send is visible.
            with self.engine.begin() as conn:
                conn.execute(t.jobs.update().where(t.jobs.c.id==job['id']).values(result={'confirmed_parts':confirmations,'in_flight_part':index},updated_at=now()))
            try:
                result=self.telegram.call(method,{'chat_id':p['recipient_id'],**payload},files=files)
                if not isinstance(result,dict) or type(result.get('message_id')) is not int or result.get('chat',{}).get('id')!=p['recipient_id']:
                    raise DeliveryUnknown('telegram_result_mismatch')
            except (Rejected,DeliveryUnknown):
                # Preserve partial confirmations in DB; no automatic resend.
                if confirmations: raise DeliveryUnknown('partial_delivery') from None
                raise
            confirmations.append({'message_id':result['message_id'],'part':index,'attachment_id':attachment_id})
            with self.engine.begin() as conn:
                conn.execute(t.messages.insert().values(id=uuid4(),order_id=job['request_id'],role='owner',body=payload.get('text',''),telegram_message_id=result['message_id'],metadata={'attachment_ids':[attachment_id] if attachment_id else []},created_at=now()))
                conn.execute(t.jobs.update().where(t.jobs.c.id==job['id']).values(result={'confirmed_parts':confirmations},updated_at=now()))
        return {'confirmed_parts':confirmations}

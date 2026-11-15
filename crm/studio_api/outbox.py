"""Durable deterministic bot prompts, separate from owner-approved CRM replies."""
from datetime import timedelta
from uuid import UUID,uuid4
import sqlalchemy as sa
from . import tables as t
from .service import now,Rejected
from .telegram import DeliveryUnknown

def enqueue(conn,bot_id,chat_id,payload,request_id=None):
    id=uuid4()
    conn.execute(t.bot_outbox.insert().values(id=id,bot_id=bot_id,chat_id=chat_id,request_id=request_id,
        payload=payload,state='pending',result={},created_at=now(),updated_at=now()))
    return id

class BotOutbox:
    def __init__(self,engine,settings,telegram): self.engine,self.settings,self.telegram=engine,settings,telegram

    def flush(self,limit=20):
        count=0
        for _ in range(limit):
            with self.engine.begin() as conn:
                # One worker serializes prompts; never re-send a crash/timeout.
                if not conn.scalar(sa.select(sa.func.pg_try_advisory_xact_lock(-888493339502))): break
                conn.execute(t.bot_outbox.update().where(t.bot_outbox.c.state=='sending',t.bot_outbox.c.updated_at<now()-timedelta(minutes=5)).values(state='unknown',error_code='worker_interrupted',updated_at=now()))
                if conn.scalar(sa.select(sa.func.count()).select_from(t.bot_outbox).where(t.bot_outbox.c.state=='sending')): break
                row=conn.execute(sa.select(t.bot_outbox).where(t.bot_outbox.c.state=='pending',t.bot_outbox.c.bot_id==self.settings.bot_id).order_by(t.bot_outbox.c.created_at,t.bot_outbox.c.id).limit(1).with_for_update(skip_locked=True)).mappings().first()
                if not row: break
                if self.settings.pilot_owner_only and row['chat_id']!=self.settings.owner_id:
                    conn.execute(t.bot_outbox.update().where(t.bot_outbox.c.id==row['id']).values(state='needs_review',error_code='pilot_owner_only',updated_at=now()))
                    count+=1;continue
                prior=row['payload'].get('_depends_on')
                if prior and conn.scalar(sa.select(t.bot_outbox.c.state).where(t.bot_outbox.c.id==UUID(prior)))!='confirmed':
                    conn.execute(t.bot_outbox.update().where(t.bot_outbox.c.id==row['id']).values(state='needs_review',error_code='previous_part_unconfirmed',updated_at=now()))
                    count+=1;continue
                conn.execute(t.bot_outbox.update().where(t.bot_outbox.c.id==row['id']).values(state='sending',updated_at=now()))
            state,error,result='confirmed',None,{}
            try:
                payload={k:v for k,v in row['payload'].items() if k in ('text','reply_markup')}
                method='sendMessage'
                kind=row['payload'].get('_photo_kind')
                if kind:
                    if kind not in {'welcome','contact'}: raise Rejected('invalid_public_photo',422)
                    path=getattr(self.settings,'anod_'+kind+'_image','')
                    if path:
                        expected=self.settings.origin+('/assets/anod-welcome.png' if kind=='welcome' else '/assets/anod-contacts.png')
                        if path!=expected or len(payload.get('text',''))>1024:
                            raise Rejected('invalid_public_photo',422)
                        method='sendPhoto';payload['photo']=path;payload['caption']=payload.pop('text','')
                if method=='sendMessage' and row['payload'].get('_disable_preview') is True:
                    payload['link_preview_options']={'is_disabled':True}
                sent=self.telegram.call(method,{'chat_id':row['chat_id'],**payload})
                if not isinstance(sent,dict) or type(sent.get('message_id')) is not int or sent.get('chat',{}).get('id')!=row['chat_id']:
                    raise DeliveryUnknown('recipient_result_mismatch')
                result={'message_id':sent['message_id']}
            except DeliveryUnknown:
                state,error='unknown','telegram_delivery_unknown'
            except Rejected as exc:
                state,error='failed',exc.code
            with self.engine.begin() as conn:
                conn.execute(t.bot_outbox.update().where(t.bot_outbox.c.id==row['id']).values(state=state,result=result,error_code=error,updated_at=now()))
                if state=='confirmed' and row['request_id']:
                    conn.execute(t.messages.insert().values(id=uuid4(),order_id=row['request_id'],role='assistant',body=row['payload'].get('text',''),telegram_message_id=result['message_id'],metadata={'delivery_method':method},created_at=now()))
            count+=1
        return count

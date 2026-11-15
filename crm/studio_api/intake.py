"""Single-bot intake with opt-in bounded Anod interviews and isolated owner login.

Advice never grants authority; only explicit owner decisions create approval jobs.
"""
import json
import re
from uuid import UUID, uuid4, uuid5, NAMESPACE_URL
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from . import tables as t
from .service import now, Rejected, CRM
from .outbox import enqueue,BotOutbox
from .advisory import AI_NOTICE

SERVICES={
    'telegram_bot':'Telegram-бот', 'ai_bot':'AI-ассистент', 'vpn':'Личный VPN',
    'app':'Приложение', 'website':'Сайт', 'automation':'Автоматизация',
}
FIELDS={'details':('desired_result','Какой результат вы хотите получить?'),
        'desired_result':('deadline','Есть пожелания по срокам? Можно написать «Не согласовано».'),
        'deadline':('contact','Как с вами связаться? Можно указать этот Telegram.'),
        'contact':('confirm','Проверьте заявку и нажмите «Отправить заявку».')}
ANOD_SERVICES={'crm':'Интеграция CRM', 'ai_bot':'AI-ассистент', 'app':'Приложение / программа', 'vpn':'Личный VPN'}
ANOD_QUESTIONS={
    'crm':'Какие системы нужно связать и что сейчас делаете вручную?',
    'ai_bot':'Кому будет помогать ассистент и какую задачу он должен решать?',
    'app':'Кто будет пользоваться приложением и какую задачу оно решит?',
    'vpn':'Для каких устройств и скольких пользователей нужен личный VPN?',
}


def valid_name(value):
    value=' '.join(value.split())
    return value if 2<=len(value)<=120 and sum(c.isalpha() for c in value)>=2 and all(c.isalpha() or c in " -.'’" for c in value) else None


def valid_phone(value):
    value=re.sub(r'[\s()-]','',value)
    return value if re.fullmatch(r'\+[1-9][0-9]{7,14}',value) else None


def valid_email(value):
    value=value.strip()
    pattern=r"[A-Za-z0-9][A-Za-z0-9._%+\-]{0,63}@(?:[A-Za-z0-9](?:[A-Za-z0-9\-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z]{2,24}"
    if len(value)>180 or not re.fullmatch(pattern,value) or '..' in value or value.split('@')[0].endswith('.'):
        return None
    local,domain=value.rsplit('@',1)
    return local+'@'+domain.lower()


def contact_fields(value):
    try:
        fields=json.loads(value or '{}')
        return fields if isinstance(fields,dict) else {}
    except (ValueError,TypeError):
        return {}


def display_text(value):
    # Preserve raw client text in CRM; never turn it into bot actions or links.
    value=re.sub(r'https?://\S+|www\.\S+|t\.me/\S+', '[ссылка из описания]', str(value or ''), flags=re.I)
    return ''.join(c for c in value if c in '\n\t' or ord(c)>=32)


def interview_advice(settings,brief):
    from .advisory import interview_advice as advise
    return advise(settings,brief)


def estimate_brief(brief):
    from .advisory import estimate_brief as estimate
    return estimate(brief)

def buttons(items):
    return {'inline_keyboard':[[{'text':label,'callback_data':value}] for label,value in items]}

class Intake:
    def __init__(self,engine,settings,telegram,bot_auth):
        self.engine,self.settings,self.telegram,self.bot_auth=engine,settings,telegram,bot_auth

    def handle(self,update):
        update_id=update.get('update_id')
        if type(update_id) is not int:
            raise Rejected('invalid_update',422)
        callback=update.get('callback_query')
        message=(callback.get('message') if callback else update.get('message')) or {}
        sender=(callback.get('from') if callback else message.get('from')) or {}
        chat=message.get('chat',{})
        user_id,chat_id=sender.get('id'),chat.get('id')
        if type(user_id) is not int or type(chat_id) is not int or user_id!=chat_id or chat.get('type')!='private' or sender.get('is_bot'):
            return {'accepted':False,'reason':'private_user_chat_required'}
        # Acknowledge foreign updates without receipts, customer data or replies.
        if self.settings.pilot_owner_only and user_id!=self.settings.owner_id:
            return {'accepted':False,'reason':'pilot_owner_only'}
        text=message.get('text') or message.get('caption') or ''
        action=callback.get('data','') if callback else ''
        if self.settings.crm_bot_enabled:
            # Legacy owner capabilities cannot fall through into client AI/intake.
            # Bare /start remains the public client entry on the original bot.
            parts=text.strip().split(maxsplit=1)
            command=parts[0].split('@',1)[0] if parts else ''
            argument=parts[1] if len(parts)>1 else ''
            if (command in ('/login','/admin') or (command=='/start' and argument.startswith('login_'))
                    or action.startswith(('lc:','lx:','admin:','approve:','reject:'))):
                return {'accepted':False,'reason':'owner_commands_moved'}
        # Owner authentication never enters the customer request/AI pipeline.
        anod=getattr(self.settings,'anod_enabled',False)
        owner_entry=not self.settings.crm_bot_enabled and not callback and (text.strip()=='/login' or (
            not anod and self.settings.pilot_owner_only and user_id==self.settings.owner_id and text.strip()=='/start'))
        if owner_entry or text.startswith('/start login_') or action.startswith(('lc:','lx:')):
            # Auth has its own durable one-shot state machine. An uncertain send
            # must not repeat; the owner can start a fresh challenge after expiry.
            with self.engine.begin() as conn:
                if not self._receipt(conn,chat_id,update_id):
                    return {'accepted':True,'duplicate':True}
            if owner_entry:
                self.bot_auth.entry(user_id,chat_id)
            elif callback:
                self.bot_auth.confirm(user_id,chat_id,message.get('message_id'),action[3:],cancel=action.startswith('lx:'))
                self.telegram.call('answerCallbackQuery',{'callback_query_id':callback['id'],'text':'Готово'})
            else:
                self.bot_auth.prompt(user_id,chat_id,text.split('login_',1)[1].strip())
            return {'accepted':True,'scope':'owner_login'}
        if anod:
            if text.strip()=='/admin' or action.startswith(('admin:','approve:','reject:')):
                return self._anod_admin(update_id,user_id,chat_id,callback,action)
            return self._handle_anod(update_id,chat_id,sender,message,callback,text,action)
        with self.engine.begin() as conn:
            # Receipt and business data commit together. A failed transaction
            # leaves the update retryable, rather than silently losing it.
            if not self._receipt(conn,chat_id,update_id):
                return {'accepted':True,'duplicate':True}
            state=conn.execute(sa.select(t.chat_state).where(t.chat_state.c.bot_id==self.settings.bot_id,t.chat_state.c.chat_id==chat_id).with_for_update()).mappings().first()
            if not state:
                conn.execute(t.chat_state.insert().values(bot_id=self.settings.bot_id,chat_id=chat_id,step='menu',verified_at=now()))
                state={'request_id':None,'step':'menu'}
            request_id,step=state['request_id'],state['step']
            row=None
            if request_id:
                row=conn.execute(sa.select(t.requests).where(t.requests.c.id==request_id,t.requests.c.bot_id==self.settings.bot_id,t.requests.c.client_telegram_id==chat_id).with_for_update()).mappings().first()
            notices=[]
            markup=None
            response=''
            if text.startswith(('/start','/menu')) or action=='menu':
                response='ANODHAS AI — студия разработки и автоматизации.\nВыберите услугу. Здесь можно описать задачу и передать заявку владельцу.\n\nОтправляя заявку, вы передаёте контакт и описание задачи для ответа студии. Не присылайте пароли и ключи доступа. Политика: '+self.settings.origin+'/privacy/'
                markup=buttons([(label,'service:'+key) for key,label in SERVICES.items()]+[('Мои заявки','orders')])
            elif action.startswith('service:') and action[8:] in SERVICES:
                key=action[8:]
                # Selecting a service is an explicit new-request action, not a lead.
                request_id=uuid4(); step='details'; stamp=now()
                conn.execute(t.requests.insert().values(id=request_id,bot_id=self.settings.bot_id,
                    client_telegram_id=chat_id,client_username=sender.get('username'),
                    client_display_name=' '.join(filter(None,[sender.get('first_name'),sender.get('last_name')]))[:255],
                    service_key=key,service_label=SERVICES[key],status='draft',revision=1,
                    google_sync_status='not_requested',created_at=stamp,updated_at=stamp))
                response=f'Заявка №{str(request_id)[:8]} · {SERVICES[key]}\nКратко опишите задачу. Что нужно сделать?'
            elif text=='/orders' or action=='orders':
                rows=conn.execute(sa.select(t.requests.c.id,t.requests.c.service_label).where(t.requests.c.bot_id==self.settings.bot_id,t.requests.c.client_telegram_id==chat_id).order_by(t.requests.c.created_at.desc()).limit(20)).all()
                response='Выберите заявку для переписки:' if rows else 'Заявок пока нет. Выберите услугу в /menu.'
                markup=buttons([(f'№{str(r.id)[:8]} · {r.service_label}','order:'+str(r.id)) for r in rows]) if rows else None
            elif action.startswith('order:'):
                try: requested=UUID(action[6:])
                except ValueError: raise Rejected('invalid_order',422) from None
                row=conn.execute(sa.select(t.requests).where(t.requests.c.id==requested,t.requests.c.bot_id==self.settings.bot_id,t.requests.c.client_telegram_id==chat_id)).mappings().first()
                if not row: raise Rejected('order_not_found',404)
                request_id=requested;step='dialogue' if row['submitted_at'] else next((field for field in FIELDS if not row[field]),'confirm')
                response=f'Выбрана заявка №{str(request_id)[:8]} · {row["service_label"]}.\n'+('Ваши сообщения и вложения будут относиться к ней.' if row['submitted_at'] else 'Опишите задачу, чтобы продолжить черновик.')
            elif action.startswith('submit:'):
                if not row or action[7:]!=str(request_id) or step!='confirm' or row['submitted_at']:
                    raise Rejected('request_changed')
                conn.execute(t.requests.update().where(t.requests.c.id==request_id).values(status='submitted',submitted_at=now(),updated_at=now(),revision=row['revision']+1))
                step='dialogue'
                response=f'Заявка №{str(request_id)[:8]} передана владельцу. Ответ поступит сюда. Дополнения можно отправлять сообщениями; новую задачу оформляйте через /menu.'
                notices.append((self.settings.owner_id,{'text':f'Новая заявка №{str(request_id)[:8]} · {row["service_label"]}\nОжидает вашего одобрения. В базу лидов пока не записана.',
                    'reply_markup':{'inline_keyboard':[[{'text':'Открыть в CRM','url':self.settings.origin+'/admin/?request='+str(request_id)}]]}}))
            elif action.startswith('edit:') and row and action[5:]==str(request_id) and not row['submitted_at']:
                conn.execute(t.requests.update().where(t.requests.c.id==request_id).values(details=None,desired_result=None,deadline=None,contact=None,revision=row['revision']+1,updated_at=now()))
                step='details';response='Опишите задачу заново. Затем уточним результат, срок и контакт.'
            elif text=='/cancel' and row and not row['submitted_at']:
                step='menu';response='Заполнение приостановлено. Черновик сохранён. /menu — новая заявка.'
            elif not callback and row:
                reply_id=message.get('reply_to_message',{}).get('message_id')
                if reply_id:
                    linked=conn.execute(sa.select(t.requests).join(t.messages,t.messages.c.order_id==t.requests.c.id).where(t.messages.c.telegram_message_id==reply_id,t.requests.c.bot_id==self.settings.bot_id,t.requests.c.client_telegram_id==chat_id)).mappings().first()
                    if linked:
                        row=linked;request_id=row['id']
                        step='dialogue' if row['submitted_at'] else next((field for field in FIELDS if not row[field]),'confirm')
                if text and step in FIELDS and not row['submitted_at']:
                    limit=255 if step in {'contact','deadline'} else 4000
                    if len(text)>limit:
                        response=f'Пожалуйста, сократите ответ до {limit} символов.'
                    else:
                        conn.execute(t.requests.update().where(t.requests.c.id==request_id).values(**{step:text},revision=row['revision']+1,updated_at=now()))
                        step,response=FIELDS[step]
                        if step=='confirm':
                            changed=dict(row,contact=text)
                            response='Проверьте заявку №'+str(request_id)[:8]+'\n\n'+ '\n'.join(f'{label}: {changed.get(key) or "Не указано"}' for key,label in [('service_label','Услуга'),('details','Задача'),('desired_result','Результат'),('deadline','Срок'),('contact','Контакт')])
                            markup=buttons([('Отправить заявку','submit:'+str(request_id)),('Исправить','edit:'+str(request_id))])
                elif step=='dialogue':
                    response=''  # No automatic free-form reply after submission.
                elif step=='confirm':
                    response='Для передачи владельцу нажмите «Отправить заявку», либо «Исправить». '
                    markup=buttons([('Отправить заявку','submit:'+str(request_id)),('Исправить','edit:'+str(request_id))])
                else:
                    response='Для продолжения выберите заявку в /orders или услугу в /menu.'
            else:
                response='Начните с /menu или выберите текущую заявку в /orders.'
            if request_id and not callback:
                attachment_ids=self._attachments(conn,request_id,message)
                conn.execute(t.messages.insert().values(id=uuid4(),order_id=request_id,role='client',body=text[:16000],
                    telegram_message_id=message.get('message_id'),metadata={'attachment_ids':attachment_ids},created_at=now()))
                conn.execute(t.requests.update().where(t.requests.c.id==request_id).values(updated_at=now()))
            conn.execute(t.chat_state.update().where(t.chat_state.c.bot_id==self.settings.bot_id,t.chat_state.c.chat_id==chat_id).values(request_id=request_id,step=step,verified_at=now()))
            # Separate durable owner notice survives client delivery failure.
            for recipient,payload in notices:
                enqueue(conn,self.settings.bot_id,recipient,payload)
            if response:
                chunks=[response[i:i+2000] for i in range(0,len(response),2000)]
                previous=None
                for i,chunk in enumerate(chunks):
                    payload={'text':chunk}
                    if previous: payload['_depends_on']=str(previous)
                    if markup and i==len(chunks)-1: payload['reply_markup']=markup
                    previous=enqueue(conn,self.settings.bot_id,chat_id,payload,request_id)
        BotOutbox(self.engine,self.settings,self.telegram).flush()
        if callback:
            self.telegram.call('answerCallbackQuery',{'callback_query_id':callback['id']})
        return {'accepted':True}

    def _queue_anod(self,conn,chat_id,text,markup=None,request_id=None,photo=None):
        previous=None
        if photo and getattr(self.settings,'anod_'+photo+'_image',''):
            caption=text if len(text)<=900 else 'Ассистент Анод · проверьте данные заявки и контакты ниже.'
            payload={'text':caption,'_photo_kind':photo}
            if caption==text and markup: payload['reply_markup']=markup
            previous=enqueue(conn,self.settings.bot_id,chat_id,payload,request_id)
            if caption==text: return
        for offset in range(0,len(text),2000):
            payload={'text':text[offset:offset+2000],'_disable_preview':True}
            if previous: payload['_depends_on']=str(previous)
            if markup and offset+2000>=len(text): payload['reply_markup']=markup
            previous=enqueue(conn,self.settings.bot_id,chat_id,payload,request_id)

    def _brief(self,conn,row):
        metadata=conn.scalar(sa.select(t.messages.c.metadata).where(t.messages.c.order_id==row['id'],
            t.messages.c.metadata.op('?')('anod_brief')).order_by(t.messages.c.created_at.desc(),t.messages.c.id.desc()).limit(1))
        return dict(metadata['anod_brief']) if metadata else None

    def _advice(self,brief,request_id,update_id):
        fallback={'service_key':brief.get('service_key'),'question':'Что важно учесть для первого рабочего варианта?',
                  'summary':'','source':'fallback'}
        # This independent transaction must commit before any provider call.
        # No orders FK/row lock: the caller already holds that request row in
        # its business transaction, which may still roll back after HTTP.
        lock_id=int.from_bytes(uuid5(NAMESPACE_URL,f'anod-ai:{self.settings.bot_id}:{request_id}').bytes[:8],'big',signed=True)
        try:
            with self.engine.begin() as budget:
                budget.execute(sa.select(sa.func.pg_advisory_xact_lock(lock_id)))
                scope=(t.ai_attempts.c.bot_id==self.settings.bot_id,t.ai_attempts.c.request_id==request_id)
                count=budget.scalar(sa.select(sa.func.count()).select_from(t.ai_attempts).where(*scope))
                brief['ai_calls']=count  # Display-only; /edit cannot reset the ledger.
                duplicate=budget.execute(sa.select(t.ai_attempts.c.update_id).where(*scope,t.ai_attempts.c.update_id==update_id)).first()
                notice=budget.execute(sa.select(t.bot_outbox.c.id).where(t.bot_outbox.c.bot_id==self.settings.bot_id,
                    t.bot_outbox.c.request_id==request_id,t.bot_outbox.c.state=='confirmed',
                    t.bot_outbox.c.payload['text'].astext.contains(AI_NOTICE)).limit(1)).first()
                if duplicate or count>=3 or not notice: return fallback
                budget.execute(t.ai_attempts.insert().values(bot_id=self.settings.bot_id,request_id=request_id,
                    update_id=update_id,created_at=now()))
        except sa.exc.SQLAlchemyError:
            return fallback  # No durable reservation means no external call.
        brief['ai_calls']=count+1
        data={key:brief.get(key) for key in ('service_key','details','desired_result','deadline','clarifications')}
        data['ai_calls']=count
        try: result=interview_advice(self.settings,data)
        except Exception: return fallback
        if not isinstance(result,dict) or set(result)!={'service_key','question','summary','source'}: return fallback
        key=result.get('service_key')
        if key is not None and (not isinstance(key,str) or key not in ANOD_SERVICES): return fallback
        for key,limit in [('question',220),('summary',600)]:
            value=result.get(key,'')
            if not isinstance(value,str) or len(value)>limit or re.search(
                r'https?://|www\.|t\.me/|@|[\x00-\x08\x0b-\x1f]|парол|password|секрет|token|otp|одноразов|код.{0,12}вход|ключ.{0,12}доступ',value,re.I):
                return fallback
        return {'service_key':result.get('service_key'),'question':result.get('question',''),
                'summary':result.get('summary',''),'source':'ai' if result.get('source')=='ai' else 'fallback'}

    def _review(self,row,consent=True):
        contact=contact_fields(row.get('contact'))
        result=('Проверьте заявку №'+str(row['id'])[:8]+'\n\n'+'\n'.join(
            label+': '+display_text(value) for label,value in [
                ('Услуга',row.get('service_label')),('Задача и уточнения',row.get('details')),
                ('Нужный результат',row.get('desired_result')),('Срок',row.get('deadline')),
                ('Имя',row.get('client_display_name')),('Телефон',contact.get('phone')),('Email',contact.get('email'))]))
        if self.settings.google_calendar_enabled or row.get('calendar_plan'):
            from .calendar_plan import describe_plan
            result+='\n\nПлан календаря после одобрения владельцем:\n'+describe_plan(row.get('calendar_plan') or {})
        if consent:
            result+='\n\nНажимая «Отправить заявку», вы передаёте описание и контакты студии для ответа. Это не договор и не согласие на оплату. Политика: '+self.settings.origin+'/privacy/'
        return result

    def _owner_markup(self,row):
        if self.settings.crm_bot_enabled:
            return {'inline_keyboard':[[{'text':'Открыть CRM-бота',
                'url':'https://t.me/'+self.settings.crm_bot_username}]]}
        identity=str(row['id'])+':'+str(row['revision'])
        return {'inline_keyboard':[
            [{'text':'Одобрить','callback_data':'approve:'+identity},{'text':'Отклонить','callback_data':'reject:'+identity}],
            [{'text':'Открыть в CRM','url':self.settings.origin+'/admin/?request='+str(row['id'])}]]}

    def _owner_notice(self,row,estimate):
        text='Заявка владельцу: решение и синхронизация проверяются в CRM.\n\n'+self._review(row,consent=False)
        if isinstance(estimate,dict):
            text+='\n\nПредварительная оценка: '+display_text(estimate.get('price_label') or 'требует уточнения')
            if all(type(estimate.get(key)) is int for key in ('hours_min','hours_max','rubles_min','rubles_max')):
                text+=f'\nТрудоёмкость: {estimate["hours_min"]}–{estimate["hours_max"]} ч. Ориентир: {estimate["rubles_min"]}–{estimate["rubles_max"]} ₽.'
            for key,label in [('assumptions','Допущения'),('limitations','Ограничения')]:
                values=estimate.get(key,[])
                if isinstance(values,list): text+='\n'+label+': '+'; '.join(display_text(v) for v in values[:5])
        return text+'\nЭто внутренняя предварительная оценка, не измеренная средняя цена рынка и не коммерческое обещание.'

    def _anod_admin(self,update_id,user_id,chat_id,callback,action):
        if self.settings.crm_bot_enabled:
            return {'accepted':False,'reason':'owner_commands_moved'}
        if user_id!=self.settings.owner_id or chat_id!=self.settings.owner_id:
            return {'accepted':False,'reason':'owner_only'}
        response,markup='',None
        if action.startswith(('approve:','reject:')):
            match=re.fullmatch(r'(approve|reject):([0-9a-fA-F-]{36}):([0-9]{1,9})',action)
            if not match: raise Rejected('invalid_owner_action',422)
            try: request_id=UUID(match[2])
            except ValueError: raise Rejected('invalid_owner_action',422) from None
            operation=uuid5(NAMESPACE_URL,f'anod-owner:{self.settings.bot_id}:{update_id}')
            # Decision commits first with a stable operation key. A retry after a
            # crash before the receipt repeats the decision, never its side effects.
            try:
                CRM(self.engine,self.settings.owner_id,settings=self.settings).decide(
                    request_id,int(match[3]),'approved' if match[1]=='approve' else 'rejected',operation)
                response='Решение сохранено. /admin — остальные заявки.'
            except Rejected:
                response='Решение не изменено: заявка обновилась либо требует проверки. Откройте её заново через /admin.'
        with self.engine.begin() as conn:
            if not self._receipt(conn,chat_id,update_id): return {'accepted':True,'duplicate':True}
            if action.startswith('admin:'):
                try: request_id=UUID(action[6:])
                except ValueError: raise Rejected('invalid_owner_action',422) from None
                row=conn.execute(sa.select(t.requests).where(t.requests.c.id==request_id,
                    t.requests.c.bot_id==self.settings.bot_id,t.requests.c.submitted_at.is_not(None))).mappings().first()
                if not row: raise Rejected('not_found',404)
                stored=conn.scalar(sa.select(t.messages.c.metadata).where(t.messages.c.order_id==request_id,
                    t.messages.c.metadata.op('?')('anod_estimate')).order_by(t.messages.c.created_at.desc(),t.messages.c.id.desc()).limit(1))
                response=self._owner_notice(row,(stored or {}).get('anod_estimate',{})); markup=self._owner_markup(row)
            elif not action:
                rows=conn.execute(sa.select(t.requests.c.id,t.requests.c.service_label).where(
                    t.requests.c.bot_id==self.settings.bot_id,t.requests.c.status=='submitted',
                    ~sa.exists(sa.select(t.decisions.c.request_id).where(t.decisions.c.request_id==t.requests.c.id)))
                    .order_by(t.requests.c.submitted_at).limit(20)).all()
                response='Заявки, ожидающие решения:' if rows else 'Новых заявок, ожидающих решения, нет.'
                markup=buttons([(f'№{str(row.id)[:8]} · {row.service_label}','admin:'+str(row.id)) for row in rows]) if rows else None
            self._queue_anod(conn,chat_id,response,markup)
        BotOutbox(self.engine,self.settings,self.telegram).flush()
        if callback: self.telegram.call('answerCallbackQuery',{'callback_query_id':callback['id']})
        return {'accepted':True,'scope':'owner_admin'}

    def _handle_anod(self,update_id,chat_id,sender,message,callback,text,action):
        with self.engine.begin() as conn:
            if not self._receipt(conn,chat_id,update_id): return {'accepted':True,'duplicate':True}
            state=conn.execute(sa.select(t.chat_state).where(t.chat_state.c.bot_id==self.settings.bot_id,
                t.chat_state.c.chat_id==chat_id).with_for_update()).mappings().first()
            if not state:
                conn.execute(t.chat_state.insert().values(bot_id=self.settings.bot_id,chat_id=chat_id,step='menu',verified_at=now()))
                state={'request_id':None,'step':'menu'}
            request_id,step=state['request_id'],state['step']
            def owned(identity):
                row=conn.execute(sa.select(t.requests).where(t.requests.c.id==identity,t.requests.c.bot_id==self.settings.bot_id,
                    t.requests.c.client_telegram_id==chat_id).with_for_update()).mappings().first()
                return dict(row) if row else None
            row=owned(request_id) if request_id else None
            if not callback and message.get('reply_to_message',{}).get('message_id'):
                linked=conn.scalar(sa.select(t.requests.c.id).join(t.messages,t.messages.c.order_id==t.requests.c.id).where(
                    t.messages.c.telegram_message_id==message['reply_to_message']['message_id'],
                    t.requests.c.bot_id==self.settings.bot_id,t.requests.c.client_telegram_id==chat_id))
                if linked: row=owned(linked); request_id=linked; step='dialogue' if row['submitted_at'] else (self._brief(conn,row) or {}).get('next_step','legacy_draft')
            brief=self._brief(conn,row) if row else None
            response,markup,photo='',None,None
            estimate=None
            def change(**values):
                values.update(revision=row['revision']+1,updated_at=now())
                conn.execute(t.requests.update().where(t.requests.c.id==row['id']).values(**values))
                row.update(values)
            def review_markup():
                identity=str(row['id'])+':'+str(row['revision'])
                return buttons([('Отправить заявку','submit:'+identity),('Исправить','edit:'+identity)])
            if not callback and text.strip() in {'/start','/menu'} or action=='menu':
                request_id,row,brief,step=None,None,None,'menu'
                response=('Я — Ассистент Анод. Помогу коротко описать задачу для студии ANODHAS AI.\n'
                    'Выберите услугу или напишите своими словами, что хотите сделать.\n\n'
                    +AI_NOTICE+' Решение принимает владелец. '
                    'Контакты запросим отдельно перед отправкой. Политика: '+self.settings.origin+'/privacy/\n'
                    '/orders — ваши заявки; /login — вход владельца.')
                markup=buttons([(label,'service:'+key) for key,label in ANOD_SERVICES.items()]); photo='welcome'
            elif text.strip()=='/orders' and not callback or action=='orders':
                rows=conn.execute(sa.select(t.requests.c.id,t.requests.c.service_label).where(t.requests.c.bot_id==self.settings.bot_id,
                    t.requests.c.client_telegram_id==chat_id).order_by(t.requests.c.created_at.desc()).limit(20)).all()
                response='Выберите свою заявку:' if rows else 'Заявок пока нет. /start — начать.'
                markup=buttons([(f'№{str(r.id)[:8]} · {r.service_label}','order:'+str(r.id)) for r in rows]) if rows else None
            elif action.startswith('order:'):
                try: requested=UUID(action[6:])
                except ValueError: raise Rejected('invalid_order',422) from None
                row=owned(requested)
                if not row: raise Rejected('order_not_found',404)
                request_id=requested; brief=self._brief(conn,row)
                step='dialogue' if row['submitted_at'] else (brief or {}).get('next_step','legacy_draft')
                if step=='anod_confirm': response=self._review(row); markup=review_markup(); photo='contact'
                else: response=f'Выбрана заявка №{str(request_id)[:8]}. '+('Дополнения можно отправлять сообщениями.' if step=='dialogue' else self._step_prompt(step,row))
            elif action.startswith('service:') and action[8:] in ANOD_SERVICES or (
                    not callback and text.strip() and not text.startswith('/') and not request_id):
                key=action[8:] if callback else None
                if not callback and len(text.strip())>1800:
                    response='Сократите описание до 1800 символов; начнём с самого важного.'
                else:
                    request_id=uuid4(); stamp=now()
                    row=dict(id=request_id,bot_id=self.settings.bot_id,client_telegram_id=chat_id,
                        client_username=str(sender.get('username') or '')[:32] or None,client_display_name=None,
                        service_key=key,service_label=ANOD_SERVICES.get(key,'Уточняем услугу'),details=None,desired_result=None,
                        deadline=None,contact=None,calendar_plan={},status='draft',revision=1,google_sync_status='not_requested',
                        submitted_at=None,created_at=stamp,updated_at=stamp)
                    conn.execute(t.requests.insert().values(**row))
                    brief={'version':1,'service_key':key,'details':'','desired_result':'','deadline':'',
                           'clarifications':[],'ai_calls':0,'name_validated':False}
                    if key:
                        step='anod_details';response=AI_NOTICE+' Политика: '+self.settings.origin+'/privacy/\n\n'+ANOD_QUESTIONS[key]
                    else:
                        # A free-text first message has not seen the provider
                        # notice yet. Do not send it to AI before that notice.
                        brief['details']=text.strip();change(details=text.strip())
                        step='anod_service';photo='welcome'
                        response='Я — Ассистент Анод. Описание сохранено в черновике. '+AI_NOTICE+' Политика: '+self.settings.origin+'/privacy/\n\nКакая услуга ближе к вашей задаче?'
                        markup=buttons([(label,'as:'+str(request_id)+':'+key) for key,label in ANOD_SERVICES.items()])
            elif action.startswith('as:'):
                parts=action.split(':')
                if len(parts)!=3 or not row or parts[1]!=str(request_id) or parts[2] not in ANOD_SERVICES or step!='anod_service' or row['submitted_at']:
                    raise Rejected('request_changed')
                brief['service_key']=parts[2]; change(service_key=parts[2],service_label=ANOD_SERVICES[parts[2]])
                step='anod_result';response='Какой конкретный результат вы хотите получить?'
            elif action.startswith(('submit:','edit:')):
                parts=action.split(':')
                if (len(parts)!=3 or not row or parts[1]!=str(request_id) or parts[2]!=str(row['revision'])
                        or step!='anod_confirm' or row['submitted_at'] or not brief):
                    raise Rejected('request_changed')
                if parts[0]=='edit':
                    brief.update(details='',desired_result='',deadline='',clarifications=[],ai_calls=0,name_validated=False)
                    change(details=None,desired_result=None,deadline=None,contact=None,client_display_name=None,calendar_plan={})
                    step='anod_details';response=ANOD_QUESTIONS[row['service_key']]
                else:
                    contacts=contact_fields(row['contact'])
                    if (row['service_key'] not in ANOD_SERVICES or not all(row.get(key) for key in ('details','desired_result','deadline'))
                            or not brief.get('name_validated') or not valid_name(row.get('client_display_name') or '')
                            or not valid_phone(str(contacts.get('phone',''))) or not valid_email(str(contacts.get('email','')))):
                        raise Rejected('validated_contact_required',422)
                    estimate=estimate_brief({key:brief.get(key) for key in ('service_key','details','desired_result','deadline','clarifications')})
                    change(status='submitted',submitted_at=now());step='dialogue'
                    self._queue_anod(conn,self.settings.owner_id,self._owner_notice(row,estimate),self._owner_markup(row))
                    response=f'Заявка №{str(request_id)[:8]} отправлена владельцу на рассмотрение. Ответ придёт сюда. Это ещё не согласование работ или оплаты.'
            elif not callback and text.strip()=='/cancel':
                request_id,row,brief,step=None,None,None,'menu';response='Заполнение приостановлено, черновик сохранён. /orders — вернуться; /start — новая задача.'
            elif not callback and row and not text.startswith('/'):
                value=text.strip()
                if step=='dialogue': response=''
                elif not brief: response=self._step_prompt('legacy_draft',row)
                elif step=='anod_confirm': response=self._review(row);markup=review_markup()
                elif step=='anod_service':
                    response='Выберите услугу кнопкой.'
                    markup=buttons([(label,'as:'+str(request_id)+':'+key) for key,label in ANOD_SERVICES.items()])
                elif step in {'anod_details','anod_result','anod_clarify','anod_deadline'}:
                    limit={'anod_details':1800,'anod_result':1200,'anod_clarify':800,'anod_deadline':200}[step]
                    if not value or len(value)>limit: response=f'Нужен текст до {limit} символов. Не присылайте секреты.'
                    elif step=='anod_details':
                        brief['details']=value;change(details=value);step='anod_result';response='Какой конкретный результат будет для вас успешным?'
                    elif step=='anod_result':
                        brief['desired_result']=value;change(desired_result=value)
                        step='anod_deadline';response=self._step_prompt(step,row)
                    elif step=='anod_clarify':
                        brief['clarifications']=[*brief['clarifications'],brief.get('last_question','')+' '+value]
                        change(details=brief['details']+'\nУточнения:\n'+'\n'.join(brief['clarifications']))
                        advice=self._advice(brief,request_id,update_id) if len(brief['clarifications'])<2 else {'question':''}
                        question=advice.get('question','')
                        if question and question!=brief.get('last_question'):
                            brief['last_question']=question;response=question+' Можно ответить «Пока не знаю».'
                        else:
                            step='anod_schedule' if self.settings.google_calendar_enabled else 'anod_name'
                            response=self._step_prompt(step,row);photo='contact'
                    else:
                        brief['deadline']=value;change(deadline=value)
                        advice=self._advice(brief,request_id,update_id);brief['summary']=advice['summary'];brief['source']=advice['source']
                        if advice['question']:
                            brief['last_question']=advice['question'];step='anod_clarify';response=advice['question']+' Можно ответить «Пока не знаю».'
                        else:
                            step='anod_schedule' if self.settings.google_calendar_enabled else 'anod_name'
                            response=self._step_prompt(step,row);photo='contact'
                elif step=='anod_schedule':
                    from .calendar_plan import parse_schedule
                    try:
                        plan=parse_schedule(value,deadline=row.get('deadline') or '')
                    except Rejected:
                        response='Не удалось однозначно определить даты. '+self._step_prompt(step,row)
                    else:
                        change(calendar_plan=plan)
                        step='anod_name';response=self._step_prompt(step,row)
                elif step=='anod_name':
                    name=valid_name(value)
                    if not name: response='Напишите настоящее имя текстом: от 2 до 120 символов, без цифр, ссылок и эмодзи.'
                    else:
                        brief['name_validated']=True;change(client_display_name=name);step='anod_phone';response=self._step_prompt(step,row)
                elif step=='anod_phone':
                    phone=valid_phone(value)
                    if not phone: response='Укажите телефон в международном формате, например +70000000001 (от 8 до 15 цифр).'
                    else:
                        change(contact=json.dumps({'phone':phone},separators=(',',':')));step='anod_email';response=self._step_prompt(step,row)
                elif step=='anod_email':
                    email=valid_email(value);contacts=contact_fields(row['contact'])
                    encoded=json.dumps(dict(contacts,email=email),separators=(',',':'))
                    if not email or len(encoded)>255: response='Проверьте email, например contact002@example.invalid; без пробелов и дополнительных строк.'
                    else:
                        change(contact=encoded);step='anod_confirm';response=self._review(row);markup=review_markup();photo='contact'
                else: response=self._step_prompt(step,row)
            else: response='Для новой задачи напишите описание или /start. /orders — ваши заявки; /login — вход владельца.'
            if request_id and row:
                metadata={}
                if brief:
                    brief['next_step']=step;metadata['anod_brief']=brief
                if estimate is not None:
                    metadata.update(anod_estimate=estimate,submitted_revision=row['revision'],consent='explicit_submit')
                if not callback: metadata['attachment_ids']=self._attachments(conn,request_id,message)
                conn.execute(t.messages.insert().values(id=uuid4(),order_id=request_id,role='client',
                    body=text[:16000] if not callback else '',telegram_message_id=message.get('message_id') if not callback else None,
                    metadata=metadata,created_at=now()))
                conn.execute(t.requests.update().where(t.requests.c.id==request_id).values(updated_at=now()))
            conn.execute(t.chat_state.update().where(t.chat_state.c.bot_id==self.settings.bot_id,t.chat_state.c.chat_id==chat_id)
                .values(request_id=request_id,step=step,verified_at=now()))
            if response: self._queue_anod(conn,chat_id,response,markup,request_id,photo)
        BotOutbox(self.engine,self.settings,self.telegram).flush()
        if callback: self.telegram.call('answerCallbackQuery',{'callback_query_id':callback['id']})
        return {'accepted':True}

    @staticmethod
    def _step_prompt(step,row):
        return {'anod_details':ANOD_QUESTIONS.get(row.get('service_key'),'Кратко опишите задачу.'),
            'anod_result':'Какой конкретный результат нужен?', 'anod_deadline':'Какой желаемый срок? Можно написать «Не согласовано».',
            'anod_name':'Как к вам обращаться? Напишите имя отдельно.',
            'anod_phone':'Укажите телефон в международном формате, например +70000000001.',
            'anod_email':'Укажите email отдельным сообщением.',
            'anod_schedule':('Есть согласованные этапы, встречи или важные даты? Укажите их отдельными строками, например:\n'
                'Этап | Прототип | 2026-10-15\n'
                'Встреча | Обсуждение | 2026-10-16 14:00 | 2026-10-16 14:30\n'
                'Контроль | Проверка доступа | 2026-10-17\n\n'
                'Время московское (Europe/Moscow). Для другого часового пояса добавьте строку «Часовой пояс: Europe/Berlin». '
                'Если дат пока нет или дополнительных событий не нужно — напишите «Нет». '
                'Точный срок из предыдущего ответа также войдёт в план. Все записи покажу перед отправкой заявки.'),
            'legacy_draft':'Прежний черновик сохранён без изменений. /start — оформить новую задачу в Аноде.'}.get(step,'Продолжите ответ или /orders — выберите заявку.')

    def _receipt(self,conn,chat_id,update_id):
        conn.execute(sa.select(sa.func.pg_advisory_xact_lock(chat_id)))
        return conn.execute(insert(t.updates).values(bot_id=self.settings.bot_id,update_id=update_id,processed_at=now()).on_conflict_do_nothing().returning(t.updates.c.update_id)).first()

    def _attachments(self,conn,request_id,message):
        item,kind=None,None
        if message.get('photo'): item,kind=message['photo'][-1],'photo'
        elif message.get('video'): item,kind=message['video'],'video'
        elif message.get('document'): item,kind=message['document'],'document'
        if not item or not isinstance(item.get('file_id'),str): return []
        attachment_id=uuid4()
        conn.execute(t.attachments.insert().values(id=attachment_id,request_id=request_id,bot_id=self.settings.bot_id,
            kind=kind,filename=str(item.get('file_name') or f'{kind}-{attachment_id}')[:200],
            mime=str(item.get('mime_type') or ('image/jpeg' if kind=='photo' else 'application/octet-stream'))[:100],
            size=max(0,int(item.get('file_size') or 0)),telegram_file_id=item['file_id'],created_at=now()))
        return [str(attachment_id)]

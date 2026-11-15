"""Transactional owner decisions and immutable queues, without external calls."""
from datetime import UTC, datetime
from hashlib import sha256
import json
from uuid import UUID, uuid4
import sqlalchemy as sa
from . import tables as t


def now():
    return datetime.now(UTC)

def digest(value):
    return sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), default=str).encode()).hexdigest()

class Rejected(Exception):
    def __init__(self, code, status=409):
        self.code, self.status = code, status
        super().__init__(code)

def request_payload(row):
    # Sync counters/timestamps do not change an approved client brief.
    payload = {key: row[key] for key in ('id','bot_id','client_telegram_id','client_username','client_display_name','service_key','service_label','details','desired_result','deadline','contact')}
    # Empty plans preserve already-approved historical payload hashes.
    if row.get('calendar_plan'):
        from .calendar_plan import normalize_plan
        payload['calendar_plan'] = normalize_plan(row['calendar_plan'])
    if row.get('leadgen'):
        payload['leadgen'] = row['leadgen']
    return payload

class CRM:
    def __init__(self, engine, owner_id, bot_id=8884933395, settings=None):
        self.engine, self.owner_id, self.bot_id = engine, owner_id, bot_id
        self.settings = settings

    def _request(self, conn, request_id, lock=True):
        query = sa.select(t.requests).where(t.requests.c.id == request_id,t.requests.c.bot_id==self.bot_id)
        row = conn.execute(query.with_for_update() if lock else query).mappings().first()
        if row is None:
            raise Rejected('not_found', 404)
        return dict(row)

    def _repeat(self, conn, operation_key, request_hash):
        # Serialize identical keys even across different requests/processes.
        # Acquire this lock BEFORE request row locks to keep the order consistent.
        lock_id = int.from_bytes(sha256(str(operation_key).encode()).digest()[:8],signed=True)
        conn.execute(sa.select(sa.func.pg_advisory_xact_lock(lock_id)))
        row = conn.execute(sa.select(t.operations).where(t.operations.c.operation_key == operation_key)).mappings().first()
        if row:
            if row['request_hash'] != request_hash:
                raise Rejected('operation_key_conflict')
            if row['job_id']:
                job = conn.execute(sa.select(t.jobs).where(t.jobs.c.id==row['job_id'])).mappings().one()
                return self.job_public(job)
            return row['result']

    def _bind(self, conn, request_id, operation_key, request_hash, result):
        job_id = result.get('id')
        conn.execute(t.operations.insert().values(operation_key=operation_key,request_id=request_id,
            request_hash=request_hash,job_id=job_id,result={} if job_id else result,created_at=now()))
        return result

    @staticmethod
    def job_public(row):
        return {key: row[key] for key in ('id','kind','request_id','state','created_at','updated_at','error_code','result')}

    def _job(self, conn, request_id, operation_key, kind, request_hash, payload):
        stamp = now()
        row = dict(id=uuid4(), operation_key=operation_key, kind=kind,
            request_id=request_id, request_hash=request_hash, payload_hash=digest(payload),
            payload=payload, state='pending', owner_id=self.owner_id,
            created_at=stamp, updated_at=stamp, error_code=None, result={})
        conn.execute(t.jobs.insert().values(**row))
        return self.job_public(row)

    def list_requests(self):
        with self.engine.connect() as conn:
            rows = conn.execute(sa.select(t.requests).where(t.requests.c.bot_id==self.bot_id,t.requests.c.leadgen=={}).order_by(t.requests.c.updated_at.desc()).limit(200)).mappings().all()
            decisions = {row['request_id']:dict(row) for row in conn.execute(sa.select(t.decisions)).mappings()}
            return [dict(row, decision=decisions.get(row['id'])) for row in rows]

    def detail(self, request_id):
        with self.engine.connect() as conn:
            row = self._request(conn, request_id, lock=False)
            decision = conn.execute(sa.select(t.decisions).where(t.decisions.c.request_id == request_id)).mappings().first()
            history = self._history(conn, request_id)
            draft = conn.execute(sa.select(t.drafts).where(t.drafts.c.request_id == request_id)).mappings().first()
            stages = conn.execute(sa.select(t.stages).where(t.stages.c.request_id == request_id).order_by(t.stages.c.updated_at,t.stages.c.id)).mappings().all()
            jobs = conn.execute(sa.select(t.jobs).where(t.jobs.c.request_id == request_id).order_by(t.jobs.c.created_at.desc()).limit(100)).mappings().all()
            from .media import public
            from .advisory import estimate_brief
            media=conn.execute(sa.select(t.attachments).where(t.attachments.c.request_id==request_id,t.attachments.c.bot_id==self.bot_id)).mappings().all()
            return dict(request=row, estimate=estimate_brief(request_payload(row)),decision=dict(decision) if decision else None,attachments=[public(a) for a in media],
                messages=history['items'],messages_has_more=history['has_more'],messages_next_cursor=history['next_cursor'],
                draft=dict(draft) if draft else dict(revision=0,body='',attachment_ids=[]),
                stages=[dict(s) for s in stages], jobs=[self.job_public(j) for j in jobs])

    def _history(self, conn, request_id, before=None):
        query=sa.select(t.messages.c.id,t.messages.c.role,t.messages.c.body,t.messages.c.telegram_message_id,t.messages.c.created_at,t.messages.c.metadata).where(t.messages.c.order_id==request_id)
        if before:
            cursor=conn.execute(sa.select(t.messages.c.created_at,t.messages.c.id).where(t.messages.c.order_id==request_id,t.messages.c.id==before)).first()
            if cursor is None:
                raise Rejected('invalid_history_cursor',422)
            query=query.where(sa.tuple_(t.messages.c.created_at,t.messages.c.id)<sa.tuple_(cursor.created_at,cursor.id))
        rows=conn.execute(query.order_by(t.messages.c.created_at.desc(),t.messages.c.id.desc()).limit(501)).mappings().all()
        visible=list(reversed(rows[:500]))
        return dict(items=[dict(m) for m in visible],has_more=len(rows)>500,next_cursor=visible[0]['id'] if len(rows)>500 else None)

    def history(self, request_id, before=None):
        with self.engine.connect() as conn:
            self._request(conn,request_id,lock=False)
            return self._history(conn,request_id,before)

    def decide(self, request_id, expected_revision, decision, operation_key):
        command_hash = digest(['decide', str(request_id), expected_revision, decision])
        with self.engine.begin() as conn:
            repeated = self._repeat(conn, operation_key, command_hash)
            if repeated:
                return repeated
            row = self._request(conn, request_id)
            if row['revision'] != expected_revision:
                raise Rejected('request_changed')
            if row['status'] == 'draft' or not row['submitted_at']:
                raise Rejected('request_not_submitted')
            previous = conn.execute(sa.select(t.decisions).where(t.decisions.c.request_id == request_id)).mappings().first()
            if previous:
                # Same brief + same decision is idempotent even with a new click ID.
                if previous['decision'] != decision or previous['payload_hash'] != digest(request_payload(row)):
                    raise Rejected('decision_requires_review')
                if decision == 'rejected':
                    return self._bind(conn,request_id,operation_key,command_hash,{'decision':'rejected'})
                existing = conn.execute(sa.select(t.jobs).where(t.jobs.c.request_id == request_id,t.jobs.c.kind == 'lead_create')).mappings().one()
                return self._bind(conn,request_id,operation_key,command_hash,self.job_public(existing))
            if row['google_sync_status'] != 'not_requested':
                raise Rejected('legacy_sync_requires_review')
            payload = request_payload(row)
            conn.execute(t.decisions.insert().values(request_id=request_id,decision=decision,
                revision=expected_revision,payload_hash=digest(payload),owner_id=self.owner_id,created_at=now()))
            if decision == 'rejected':
                return self._bind(conn,request_id,operation_key,command_hash,{'decision':'rejected'})
            # Fixed target status; agreement is a separate owner action.
            payload = json.loads(json.dumps(payload,default=str))
            payload.update(lead_id=str(uuid4()), status='Новый')
            job=self._job(conn, request_id, operation_key, 'lead_create', command_hash, payload)
            if self.settings and self.settings.anod_enabled and not row.get('leadgen'):
                if not self.settings.pilot_owner_only or row['client_telegram_id'] == self.owner_id:
                    from .outbox import enqueue
                    enqueue(conn,self.bot_id,row['client_telegram_id'],{
                        'text':'Ваша заявка рассмотрена и передана в отдел разработки. С вами свяжется специалист в ближайшее время.'},request_id)
            return self._bind(conn,request_id,operation_key,command_hash,job)

    def save_draft(self, request_id, expected_revision, body, attachment_ids):
        if len(body) > 4000:
            raise Rejected('message_too_long', 422)
        with self.engine.begin() as conn:
            self._request(conn, request_id)
            from .media import manifest
            manifest(conn,request_id,self.bot_id,attachment_ids)
            row = conn.execute(sa.select(t.drafts).where(t.drafts.c.request_id == request_id).with_for_update()).mappings().first()
            if (row['revision'] if row else 0) != expected_revision:
                raise Rejected('draft_changed')
            fields = dict(body=body, attachment_ids=list(map(str,attachment_ids)), revision=expected_revision+1,updated_at=now())
            if row:
                conn.execute(t.drafts.update().where(t.drafts.c.request_id == request_id).values(**fields))
            else:
                conn.execute(t.drafts.insert().values(request_id=request_id,**fields))
            return dict(request_id=request_id,**fields)

    def queue_message(self, request_id, draft_revision, operation_key):
        command_hash = digest(['send', str(request_id), draft_revision])
        with self.engine.begin() as conn:
            repeated = self._repeat(conn,operation_key,command_hash)
            if repeated:
                return repeated
            request = self._request(conn,request_id)
            row = conn.execute(sa.select(t.drafts).where(t.drafts.c.request_id == request_id).with_for_update()).mappings().first()
            if row is None or row['revision'] != draft_revision:
                raise Rejected('draft_changed')
            if not row['body'].strip() and not row['attachment_ids']:
                raise Rejected('empty_message',422)
            from .media import manifest
            media=manifest(conn,request_id,self.bot_id,row['attachment_ids'])
            verified=conn.execute(sa.select(t.chat_state).where(t.chat_state.c.bot_id==self.bot_id,t.chat_state.c.chat_id==request['client_telegram_id'])).first()
            if not request['submitted_at'] or request['client_telegram_id'] <= 0 or not verified:
                raise Rejected('verified_chat_required')
            duplicate = conn.execute(sa.select(t.jobs).where(t.jobs.c.kind=='telegram_send',t.jobs.c.request_hash==command_hash)).mappings().first()
            if duplicate:
                return self._bind(conn,request_id,operation_key,command_hash,self.job_public(duplicate))
            # The browser never supplies recipient identity. Copy exact draft once.
            job=self._job(conn,request_id,operation_key,'telegram_send',command_hash,
                dict(request_id=str(request_id),bot_id=self.bot_id, recipient_id=request['client_telegram_id'],body=row['body'],attachment_ids=list(row['attachment_ids']),attachments=media,draft_revision=draft_revision))
            return self._bind(conn,request_id,operation_key,command_hash,job)

    def save_stage(self, request_id, stage_id, expected_revision, fields):
        with self.engine.begin() as conn:
            self._request(conn,request_id)
            row = conn.execute(sa.select(t.stages).where(t.stages.c.id==stage_id,t.stages.c.request_id==request_id).with_for_update()).mappings().first()
            if (row['revision'] if row else 0) != expected_revision:
                raise Rejected('stage_changed')
            if fields['state']=='done' and not fields['public_result'].strip():
                raise Rejected('completed_stage_needs_result',422)
            data = dict(**fields,revision=expected_revision+1,updated_at=now())
            if row:
                conn.execute(t.stages.update().where(t.stages.c.id==stage_id,t.stages.c.request_id==request_id).values(**data))
            else:
                conn.execute(t.stages.insert().values(id=stage_id,request_id=request_id,**data))
            return dict(id=stage_id,request_id=request_id,**data)

    def stage_report(self, request_id):
        detail = self.detail(request_id)
        lines = [f"По заявке №{str(request_id)[:8]}:"]
        for stage in detail['stages']:
            if stage['state']=='done':
                lines.append(f"✓ {stage['title']}: {stage['public_result']}")
        if len(lines)==1:
            raise Rejected('no_completed_stages',422)
        return {'body':'\n'.join(lines)}  # No private notes; no draft overwrite; no send.

    def list_jobs(self):
        with self.engine.connect() as conn:
            jobs=[self.job_public(r) for r in conn.execute(sa.select(t.jobs).order_by(t.jobs.c.created_at.desc()).limit(200)).mappings()]
            for row in conn.execute(sa.select(t.bot_outbox).where(t.bot_outbox.c.bot_id==self.bot_id,t.bot_outbox.c.state.in_(['failed','unknown','needs_review'])).order_by(t.bot_outbox.c.created_at.desc()).limit(100)).mappings():
                jobs.append({k:row[k] for k in ('id','request_id','state','created_at','updated_at','error_code','result')}|{'kind':'bot_notice'})
            if self.settings and self.settings.anod_enabled:
                for row in conn.execute(sa.select(t.notifications).where(t.notifications.c.state!='not_required').order_by(t.notifications.c.created_at.desc()).limit(100)).mappings():
                    jobs.append(dict(row)|{'id':row['request_id'],'kind':'owner_email'})
            return sorted(jobs,key=lambda j:j['created_at'],reverse=True)

"""Owner-only bridge and selection into the existing approved CRM/Sheets pipeline."""
import json
import socket
from uuid import UUID, NAMESPACE_URL, uuid5
import sqlalchemy as sa
from . import tables as t
from .service import CRM, Rejected, digest, now
from .sheets import lead_values, BOOK

STATUSES = ['Новый', 'На проверке', 'В работе', 'Отложен']

class RadarClient:
    def __init__(self, path='/run/leadgen/research.sock'): self.path=path
    def call(self, action, **params):
        try:
            with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as connection:
                connection.settimeout(6)
                connection.connect(self.path)
                connection.sendall(json.dumps({'action':action,**params}).encode()+b'\n')
                data=bytearray()
                while not data.endswith(b'\n'):
                    chunk=connection.recv(65536)
                    if not chunk or len(data)+len(chunk)>2*1024*1024: raise ValueError()
                    data.extend(chunk)
                response=json.loads(data)
            if not response.get('ok'):
                code=response.get('error','research_unavailable')
                raise Rejected(code,409 if code in {'research_busy','operation_key_conflict'} else 503)
            return response['result']
        except (OSError,ValueError,KeyError):
            raise Rejected('research_unavailable',503) from None

class Leadgen:
    def __init__(self,engine,settings,*,radar=None,sheets=None):
        self.engine,self.settings=engine,settings
        self.radar=radar or RadarClient()
        self.crm=CRM(engine,settings.owner_id,settings=settings)
        self.sheets=sheets

    def enabled(self):
        if not self.settings.leadgen_enabled: raise Rejected('leadgen_not_configured',503)

    def catalog(self):
        self.enabled()
        return {**self.radar.call('catalog'),'assignee_default':'owner','statuses':STATUSES}

    def runs(self):
        self.enabled(); return self.radar.call('list')

    def run(self,run_id):
        self.enabled(); run=self.radar.call('get',run_id=run_id)
        with self.engine.connect() as conn:
            for card in run.get('cards',[]):
                link=conn.execute(sa.select(t.leadgen_links).where(t.leadgen_links.c.candidate_id==card['id'])).mappings().first()
                if link: card['sync']=self._selection(conn,link['request_id'])
        return run

    def start(self,direction_id,mode,operation_key):
        self.enabled()
        return self.radar.call('start',direction_id=direction_id,mode=mode,operation_key=str(operation_key))

    def cancel(self,run_id):
        self.enabled(); return self.radar.call('cancel',run_id=run_id)

    def _selection(self,conn,request_id):
        row=self.crm._request(conn,request_id,lock=False)
        job=conn.execute(sa.select(t.jobs).where(t.jobs.c.request_id==request_id,t.jobs.c.kind=='lead_create')).mappings().first()
        return {'request_id':str(request_id),'assignee':row['leadgen']['assignee'],
            'status':row['leadgen']['status'],'job':self.crm.job_public(job) if job else None}

    def import_cards(self,run_id,items,operation_key):
        self.enabled()
        run=self.radar.call('get',run_id=run_id)
        if run['status'] in {'queued','running'}: raise Rejected('research_still_running')
        cards={c['id']:c for c in run['cards']}
        if len({i['candidate_id'] for i in items})!=len(items): raise Rejected('duplicate_candidate',422)
        signature=digest([run_id,sorted(items,key=lambda i:i['candidate_id'])])
        with self.engine.begin() as conn:
            conn.execute(sa.select(sa.func.pg_advisory_xact_lock(int.from_bytes(UUID(str(operation_key)).bytes[:8],signed=True))))
            old=conn.execute(sa.select(t.leadgen_batches).where(t.leadgen_batches.c.operation_key==operation_key)).mappings().first()
            if old and old['request_hash']!=signature: raise Rejected('operation_key_conflict')
            if not old: conn.execute(t.leadgen_batches.insert().values(operation_key=operation_key,request_hash=signature,created_at=now()))
        results=[]
        for item in items:
            cid=item['candidate_id']
            try:
                if cid not in cards: raise Rejected('candidate_not_in_run',404)
                request_id=self._select(cards[cid],run,item)
                subkey=uuid5(UUID(str(operation_key)),cid)
                self.crm.decide(request_id,1,'approved',subkey)
                with self.engine.connect() as conn:
                    results.append({'candidate_id':cid,**self._selection(conn,request_id)})
            except Rejected as exc:
                results.append({'candidate_id':cid,'error':exc.code})
        return {'items':results}

    def _select(self,card,run,item):
        cid=card['id'];stamp=now()
        from urllib.parse import urlsplit
        parsed=urlsplit(card['url'])
        if parsed.scheme!='https' or not parsed.hostname or parsed.username or parsed.password:
            raise Rejected('candidate_source_invalid',422)
        request_id=uuid5(NAMESPACE_URL,'anodhas:radar:'+cid)
        with self.engine.begin() as conn:
            conn.execute(sa.select(sa.func.pg_advisory_xact_lock(int(cid[:16],16)-(1<<63))))
            link=conn.execute(sa.select(t.leadgen_links).where(t.leadgen_links.c.candidate_id==cid)).mappings().first()
            if link:
                row=self.crm._request(conn,link['request_id'])
                if row['leadgen']['assignee']!=item['assignee'] or row['leadgen']['status']!=item['status']:
                    raise Rejected('existing_selection_changed')
                return link['request_id']
            meta={'candidate_id':cid,'run_id':run['id'],'url':card['url'],'platform':card['platform'],
                'published_at':card.get('published_at'),'checked_at':card['checked_at'],
                'assignee':item['assignee'],'status':item['status'],'evidence':card.get('request_status'),
                'content_sha256':digest(card['text'])}
            # A public demand signal is not a Telegram customer or verified chat.
            conn.execute(t.requests.insert().values(id=request_id,bot_id=self.settings.bot_id,
                client_telegram_id=0,client_username=None,
                client_display_name=(card.get('author_username') or card.get('author') or card.get('decision_maker') or 'Кандидат из '+card['platform'])[:255],
                service_key='leadgen',service_label=run['direction'][:255],details=card['text'],
                desired_result='',deadline='',contact='',leadgen=meta,
                status='submitted',revision=1,google_sync_status='not_requested',submitted_at=stamp,
                created_at=stamp,updated_at=stamp))
            conn.execute(t.leadgen_links.insert().values(candidate_id=cid,request_id=request_id,created_at=stamp))
            # Also prevents an old image from mailing imported candidates after rollback.
            conn.execute(t.notifications.insert().values(request_id=request_id,state='not_required',
                result={'reason':'owner_research_selection'},created_at=stamp,updated_at=stamp))
        return request_id

    def recover(self,request_id,worker):
        """Read-back first. Uncertain absence never triggers a blind append."""
        self.enabled()
        retry=False
        with self.engine.begin() as conn:
            conn.execute(sa.select(sa.func.pg_advisory_xact_lock(-888493339503)))
            row=self.crm._request(conn,request_id)
            if not row.get('leadgen'): raise Rejected('not_research_selection',404)
            job=conn.execute(sa.select(t.jobs).where(t.jobs.c.request_id==request_id,t.jobs.c.kind=='lead_create').with_for_update()).mappings().first()
            if not job: raise Rejected('not_found',404)
            job=dict(job)
            if job['state'] in {'confirmed','pending'}: return self.crm.job_public(job)
            if job['state']=='sending': raise Rejected('sync_in_progress')
            worker._validate(conn,job)
            table=self.sheets.table('leads')
            p=job['payload'];marker=f'ANODHAS Studio · request:{p["id"]} · lead:{p["lead_id"]}'
            found=[r for r in table['rows'] if marker in r['values'][8].split('\n')]
            if len(found)>1: raise Rejected('duplicate_lead_marker')
            if found:
                if found[0]['values']!=lead_values(p,None): raise Rejected('existing_lead_changed_requires_review')
                result={'sheet':table['sheet'],'row':found[0]['row'],'lead_id':p['lead_id'],'verified':True,'existing':True,'workspace':None}
                conn.execute(t.jobs.update().where(t.jobs.c.id==job['id']).values(state='confirmed',error_code=None,result=result,updated_at=now()))
                conn.execute(t.requests.update().where(t.requests.c.id==request_id).values(google_sync_status='synced'))
            elif job['state']=='failed':
                # Definitively rejected attempt + full marker read. Same job and identity.
                conn.execute(t.jobs.update().where(t.jobs.c.id==job['id']).values(state='pending',error_code=None,updated_at=now()))
                retry=True
            else:
                raise Rejected('sync_absence_requires_review')
        if retry: worker.run(1,job_id=job['id'])
        with self.engine.connect() as conn:
            return self.crm.job_public(conn.execute(sa.select(t.jobs).where(t.jobs.c.id==job['id'])).mappings().one())

"""Read-only AF_UNIX requests on the collector's existing Telethon client."""
import asyncio,json,os,stat,socket,struct,fcntl
from contextlib import asynccontextmanager
from pathlib import Path
MAX_RESPONSE=1100000
APPLICATION_LEDGER='/app/.local/codex-bridge/job-applications.sqlite'
async def execute_legacy(client, request, owner_id):
    if not isinstance(request,dict) or set(request)-{'action','limit','dialog_id'}:
        return {'ok':False,'error':'invalid_request'}
    action=request.get('action')
    if action not in ('status','dialogs','messages','sources','source_messages','prepare_application','send_application'):
        return {'ok':False,'error':'unsupported_action'}
    if not await client.is_user_authorized():
        return {'ok':False,'authorized':False,'error':'server_session_unauthorized'}
    me=await client.get_me()
    if me is None or type(owner_id) is not int or getattr(me,'id',None)!=owner_id:
        return {'ok':False,'error':'owner_mismatch'}
    if action=='status':
        return {'ok':True,'authorized':True,'connection_source':'anodhas_leadgen_server','owner_username':'owner','read_only':True,'collector_pid':os.getpid()}
    limit=request.get('limit',10)
    if type(limit) is not int or not 1<=limit<=30:
        return {'ok':False,'error':'invalid_limit'}
    # Resolve only existing one-to-one human dialogs. Never resolve arbitrary peers.
    dialogs=[]
    target=None
    peer=request.get('dialog_id')
    if action=='messages' and (type(peer) is not int or peer<=0):
        return {'ok':False,'error':'invalid_dialog_id'}
    async for d in client.iter_dialogs(limit=200):
        e=d.entity
        if not d.is_user or getattr(e,'bot',False) or e.id==me.id or getattr(e,'deleted',False):
            continue
        if action=='messages':
            if e.id==peer:
                target=e
                break
        else:
            dialogs.append({'dialog_id':e.id,'name':d.name,'username':getattr(e,'username',None)})
            if len(dialogs)>=limit: break
    if action=='dialogs':
        return {'ok':True,'dialogs':dialogs,'search_window':200,'read_only':True}
    if target is None:
        return {'ok':False,'error':'existing_human_dialog_not_found_in_window'}
    messages=[]
    async for m in client.iter_messages(target,limit=limit):
        messages.append({'message_id':m.id,'outgoing':bool(m.out),'date':m.date.isoformat() if m.date else None,'text':(m.message or '')[:8000]})
    return {'ok':True,'dialog_id':peer,'messages':messages,'read_only':True}


async def execute(client, request, owner_id):
    action=request.get('action') if isinstance(request,dict) else None
    if action in ('status','dialogs','messages'):
        return await execute_legacy(client,request,owner_id)
    fields={'action','limit','query','source_id','message_id','recipient','text','draft_id','confirm'}
    if not isinstance(request,dict) or set(request)-fields:return {'ok':False,'error':'invalid_request'}
    if action not in ('sources','source_messages','prepare_application','send_application'):return {'ok':False,'error':'unsupported_action'}
    if not await client.is_user_authorized():return {'ok':False,'error':'server_session_unauthorized'}
    me=await client.get_me()
    if me is None or type(owner_id) is not int or me.id!=owner_id:return {'ok':False,'error':'owner_mismatch'}
    import re,sqlite3,time,secrets
    from telethon import functions,types
    limit=request.get('limit',20)
    if type(limit)is not int or not 1<=limit<=30:return {'ok':False,'error':'invalid_limit'}
    if action=='sources':
        q=request.get('query','ваканс')
        if not isinstance(q,str) or not 2<=len(q)<=80:return {'ok':False,'error':'invalid_query'}
        found=[]
        async for d in client.iter_dialogs(limit=500):
            if (d.is_group or d.is_channel) and q.casefold() in d.name.casefold():
                found.append({'source_id':d.id,'title':d.name,'username':getattr(d.entity,'username',None),'group':bool(d.is_group)})
                if len(found)>=limit:break
        return {'ok':True,'sources':found,'search_window':500,'read_only':True}
    ledger=sqlite3.connect(APPLICATION_LEDGER)
    ledger.execute('CREATE TABLE IF NOT EXISTS applications(id TEXT PRIMARY KEY, created INTEGER, source INTEGER, post INTEGER, recipient TEXT, text TEXT, random_id INTEGER, state TEXT, sent_id INTEGER, UNIQUE(source,post,recipient))')
    os.chmod(APPLICATION_LEDGER,0o600)
    try:
        if action=='send_application':
            draft=request.get('draft_id')
            if not isinstance(draft,str) or not re.fullmatch('[a-f0-9]{32}',draft):return {'ok':False,'error':'invalid_draft'}
            ledger.execute('BEGIN IMMEDIATE')
            row=ledger.execute('SELECT * FROM applications WHERE id=?',(draft,)).fetchone()
            if not row:return {'ok':False,'error':'draft_not_found'}
            if row[7]=='sent':return {'ok':True,'already_sent':True,'message_id':row[8]}
            if row[7]!='prepared':return {'ok':False,'error':'delivery_unknown_do_not_retry'}
            if time.time()-row[1]>900:return {'ok':False,'error':'draft_expired'}
            if request.get('confirm')!=draft:return {'ok':False,'error':'exact_preview_confirmation_required'}
            ledger.execute("UPDATE applications SET state='unknown' WHERE id=?",(draft,));ledger.commit()
            peer=await client.get_input_entity(row[4])
            entity=await client.get_entity(peer)
            if not isinstance(entity,types.User) or getattr(entity,'bot',False):return {'ok':False,'error':'human_recipient_required'}
            result=await client(functions.messages.SendMessageRequest(peer=peer,message=row[5],random_id=row[6],no_webpage=True))
            mid=getattr(result,'id',None)
            for update in getattr(result,'updates',[]):
                if getattr(update,'random_id',None)==row[6]:mid=getattr(update,'id',mid)
                message=getattr(update,'message',None)
                if message is not None and getattr(message,'out',False):mid=getattr(message,'id',mid)
            if mid is None:return {'ok':False,'error':'delivery_unknown_do_not_retry'}
            ledger.execute("UPDATE applications SET state='sent',sent_id=? WHERE id=?",(mid,draft));ledger.commit()
            return {'ok':True,'message_id':mid,'recipient':row[4]}
        sid=request.get('source_id');target=None
        if type(sid)is not int or sid>=0:return {'ok':False,'error':'existing_group_or_channel_required'}
        async for d in client.iter_dialogs(limit=500):
            if d.id==sid and (d.is_group or d.is_channel):target=d.entity;break
        if target is None:return {'ok':False,'error':'source_not_in_existing_dialogs'}
        if action=='source_messages':
            q=request.get('query','')
            if not isinstance(q,str) or len(q)>80:return {'ok':False,'error':'invalid_query'}
            items=[]
            async for m in client.iter_messages(target,limit=limit,search=q or None):
                items.append({'message_id':m.id,'date':m.date.isoformat() if m.date else None,'text':(m.message or '')[:8000]})
            return {'ok':True,'source_id':sid,'messages':items,'read_only':True}
        post=request.get('message_id');recipient=request.get('recipient','').lstrip('@');text=request.get('text')
        if type(post)is not int or post<=0 or not re.fullmatch('[A-Za-z][A-Za-z0-9_]{4,31}',recipient) or not isinstance(text,str) or not 10<=len(text)<=5000:return {'ok':False,'error':'invalid_application'}
        original=await client.get_messages(target,ids=post)
        body=(getattr(original,'message',None) or '')
        contacts=re.findall(r'(?:@|https?://t\.me/)([A-Za-z][A-Za-z0-9_]{4,31})',body)
        if recipient.casefold() not in [c.casefold() for c in contacts]:return {'ok':False,'error':'recipient_not_in_source_post'}
        entity=await client.get_entity(recipient)
        if not isinstance(entity,types.User) or getattr(entity,'bot',False):return {'ok':False,'error':'human_recipient_required'}
        existing=ledger.execute('SELECT id,state FROM applications WHERE source=? AND post=? AND recipient=?',(sid,post,recipient.casefold())).fetchone()
        if existing:return {'ok':False,'error':'application_already_recorded','draft_id':existing[0],'state':existing[1]}
        draft=secrets.token_hex(16)
        ledger.execute('INSERT INTO applications VALUES(?,?,?,?,?,?,?,?,?)',(draft,int(time.time()),sid,post,recipient.casefold(),text,secrets.randbits(63),'prepared',None));ledger.commit()
        return {'ok':True,'draft_id':draft,'recipient':'@'+recipient,'text':text,'source_id':sid,'message_id':post,'sent':False,'expires_seconds':900}
    finally:
        ledger.close()


class CollectorBridge:
    def __init__(self, client, owner_id, path, governor=None):
        self.client,self.owner_id,self.path=client,owner_id,Path(path)
        self.governor=governor
        self.server=None;self.tasks=set();self.lock_fd=None;self.operation=asyncio.Lock()
    async def start(self):
        self.path.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
        parent=self.path.parent.lstat()
        if not stat.S_ISDIR(parent.st_mode) or parent.st_uid!=os.getuid() or parent.st_mode&0o077:
            raise RuntimeError('bridge_directory_not_private')
        lock=self.path.with_suffix('.lock')
        self.lock_fd=os.open(lock,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
        try:
            fcntl.flock(self.lock_fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            if self.path.is_symlink():raise RuntimeError('bridge_path_symlink')
            if self.path.exists():
                old=self.path.lstat()
                if not stat.S_ISSOCK(old.st_mode) or old.st_uid!=os.getuid():
                    raise RuntimeError('bridge_path_not_owned_socket')
                self.path.unlink()
            # The 0700 parent prevents access before chmod completes.
            self.server=await asyncio.start_unix_server(self.handle,path=str(self.path),limit=4097)
            os.chmod(self.path,0o600)
            self.inode=self.path.stat().st_ino
        except BaseException:
            os.close(self.lock_fd);self.lock_fd=None;raise
    async def close(self):
        if self.server:
            self.server.close()
        pending=tuple(self.tasks)
        for t in pending:t.cancel()
        if pending:await asyncio.gather(*pending,return_exceptions=True)
        if self.server:await self.server.wait_closed()
        if self.server and self.path.exists() and self.path.lstat().st_ino==self.inode:
            self.path.unlink()
        if self.lock_fd is not None:
            os.close(self.lock_fd);self.lock_fd=None
    async def handle(self,reader,writer):
        task=asyncio.current_task();self.tasks.add(task)
        try:
            peer=writer.get_extra_info('socket')
            if hasattr(socket,'SO_PEERCRED'):
                _,uid,_=struct.unpack('3i',peer.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,12))
                if uid not in (0,os.getuid()):raise ValueError('peer_denied')
            if len(self.tasks)>2 or self.operation.locked():
                result={'ok':False,'error':'bridge_busy'}
            else:
                async with self.operation,asyncio.timeout(40):
                    raw=await asyncio.wait_for(reader.readline(),3)
                    if len(raw)>4096 or not raw.endswith(b'\n'):raise ValueError('invalid_frame')
                    request=json.loads(raw)
                    # Validate before spending a shared account crawl reservation.
                    if not isinstance(request,dict) or request.get('action') not in ('status','dialogs','messages','sources','source_messages','prepare_application','send_application'):
                        result={'ok':False,'error':'invalid_request'}
                    elif self.governor is None:
                        result=await execute(self.client,request,self.owner_id)
                    else:
                        async def operation():return await execute(self.client,request,self.owner_id)
                        category='history' if request['action'] in ('messages','source_messages') else 'entity_access'
                        result=await self.governor.run(category,operation)
            encoded=json.dumps(result,ensure_ascii=False).encode()+b'\n'
            if len(encoded)>MAX_RESPONSE:raise ValueError('response_too_large')
            writer.write(encoded);await asyncio.wait_for(writer.drain(),3)
        except asyncio.CancelledError:raise
        except Exception as e:
            try:
                writer.write(json.dumps({'ok':False,'error':'bridge_request_failed','error_class':type(e).__name__}).encode()+b'\n')
                await asyncio.wait_for(writer.drain(),1)
            except Exception:pass
        finally:
            writer.close()
            try:await asyncio.wait_for(writer.wait_closed(),1)
            except Exception:pass
            self.tasks.discard(task)

def configured_bridge(client, governor):
    if os.environ.get('CODEX_TELEGRAM_BRIDGE')!='1':return None
    from .config import OperatorBotSettings
    owner=OperatorBotSettings.from_env().owner_user_id
    if type(owner) is not int or owner<=0:raise RuntimeError('bridge_owner_not_configured')
    return CollectorBridge(client,owner,Path('/app/.local/codex-bridge/telegram.sock'),governor)

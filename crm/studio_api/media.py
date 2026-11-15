"""Private, immutable, order-scoped media. Nothing is served by the public site."""
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from uuid import UUID,uuid4
import os
import re
import warnings
from datetime import timedelta
from contextlib import nullcontext
import sqlalchemy as sa
from PIL import Image
from . import tables as t
from .service import Rejected,now

MAX_FILE=20*1024*1024

def public(row):
    return {key:row[key] for key in ('id','request_id','kind','filename','mime','size','created_at')}

def manifest(conn,request_id,bot_id,ids):
    if len(ids)>10 or len(set(map(str,ids)))!=len(ids): raise Rejected('invalid_attachments',422)
    result=[]
    for id in ids:
        row=conn.execute(sa.select(t.attachments).where(t.attachments.c.id==UUID(str(id)),t.attachments.c.request_id==request_id,t.attachments.c.bot_id==bot_id)).mappings().first()
        if not row or not (row['storage_key'] and row['sha256']):
            raise Rejected('attachment_not_ready',422)
        result.append({k:str(row[k]) if isinstance(row[k],UUID) else row[k] for k in ('id','request_id','bot_id','kind','filename','mime','size','sha256','storage_key')})
    return result

class Media:
    def __init__(self,engine,settings,telegram):
        self.engine,self.settings,self.telegram=engine,settings,telegram

    def _request(self,conn,id):
        if not conn.scalar(sa.select(t.requests.c.id).where(t.requests.c.id==id,t.requests.c.bot_id==self.settings.bot_id)):
            raise Rejected('not_found',404)

    def save(self,request_id,filename,data,connection=None):
        if not data or len(data)>MAX_FILE: raise Rejected('file_too_large',413)
        filename=re.sub(r'[^\w .()-]','_',str(filename))[:150].strip('.') or 'attachment'
        ext=Path(filename).suffix.lower()
        kind,mime='document','application/octet-stream'
        if ext in ('.png','.jpg','.jpeg','.webp'):
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('error',Image.DecompressionBombWarning)
                    with Image.open(BytesIO(data)) as im:
                        if im.format not in ('JPEG','PNG','WEBP'): raise ValueError()
                        w,h=im.size
                        mime=Image.MIME[im.format]
                        im.verify()
                # Telegram photo constraints; larger images remain original files.
                kind='photo' if len(data)<=10*1024*1024 and w+h<=10000 and max(w/h,h/w)<=20 else 'document'
            except (OSError,ValueError,Image.DecompressionBombError,Image.DecompressionBombWarning):
                raise Rejected('invalid_image',422) from None
        elif ext=='.mp4' and len(data)>=12 and data[4:8]==b'ftyp':
            kind,mime='video','video/mp4'
        elif ext=='.pdf' and data.startswith(b'%PDF-'):
            mime='application/pdf'
        else:
            raise Rejected('unsupported_file',422)
        with self.engine.begin() if connection is None else nullcontext(connection) as conn:
            self._request(conn,request_id)
            conn.execute(sa.select(sa.func.pg_advisory_xact_lock(int.from_bytes(request_id.bytes[:8],signed=True))))
            count=conn.scalar(sa.select(sa.func.count()).select_from(t.attachments).where(t.attachments.c.request_id==request_id,t.attachments.c.storage_key.is_not(None)))
            if count>=200: raise Rejected('order_media_limit',413)
            id=uuid4(); key=id.hex
            root=Path(self.settings.media_root)
            root.mkdir(mode=0o700,parents=True,exist_ok=True)
            path=root/key
            fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
            with os.fdopen(fd,'wb') as stream:
                stream.write(data);stream.flush();os.fsync(stream.fileno())
            row=dict(id=id,request_id=request_id,bot_id=self.settings.bot_id,kind=kind,filename=filename,mime=mime,size=len(data),sha256=sha256(data).hexdigest(),storage_key=key,created_at=now())
            conn.execute(t.attachments.insert().values(**row))
            return public(row)

    def read_local(self,row):
        key=row.get('storage_key','')
        if not re.fullmatch(r'[0-9a-f]{32}',key): raise Rejected('invalid_storage_key',422)
        root=Path(self.settings.media_root).resolve()
        path=root/key
        if path.is_symlink() or path.resolve().parent!=root: raise Rejected('invalid_storage_key',422)
        try:
            with os.fdopen(os.open(path,os.O_RDONLY|os.O_NOFOLLOW),'rb') as stream: data=stream.read(MAX_FILE+1)
        except OSError: raise Rejected('attachment_unavailable',503) from None
        if len(data)!=row['size'] or sha256(data).hexdigest()!=row['sha256']:
            raise Rejected('attachment_changed',409)
        return data

    def download(self,request_id,attachment_id):
        with self.engine.connect() as conn:
            self._request(conn,request_id)
            row=conn.execute(sa.select(t.attachments).where(t.attachments.c.id==attachment_id,t.attachments.c.request_id==request_id,t.attachments.c.bot_id==self.settings.bot_id)).mappings().first()
        if not row: raise Rejected('not_found',404)
        if row['storage_key']: return self.read_local(row),row['filename']
        if row['size']>MAX_FILE: raise Rejected('telegram_download_limit',413)
        if not row['telegram_file_id']: raise Rejected('attachment_unavailable',503)
        return self.telegram.download(row['telegram_file_id']),row['filename']

    def start_upload(self,request_id,filename,size):
        if size<1 or size>MAX_FILE: raise Rejected('file_too_large',413)
        with self.engine.begin() as conn:
            self._request(conn,request_id)
            conn.execute(sa.select(sa.func.pg_advisory_xact_lock(int.from_bytes(request_id.bytes[:8],signed=True))))
            count=conn.scalar(sa.select(sa.func.count()).select_from(t.uploads).where(t.uploads.c.request_id==request_id,t.uploads.c.attachment_id.is_(None),t.uploads.c.expires_at>now()))
            if count>=8: raise Rejected('too_many_uploads',429)
            id=uuid4()
            conn.execute(t.uploads.insert().values(id=id,request_id=request_id,filename=filename[:200],size=size,received=0,expires_at=now()+timedelta(minutes=30)))
            return {'id':str(id),'received':0,'chunk_size':512*1024}

    def upload_chunk(self,request_id,upload_id,offset,data):
        if not data or len(data)>512*1024: raise Rejected('invalid_chunk_size',413)
        with self.engine.begin() as conn:
            self._request(conn,request_id)
            row=conn.execute(sa.select(t.uploads).where(t.uploads.c.id==upload_id,t.uploads.c.request_id==request_id).with_for_update()).mappings().first()
            if not row or row['expires_at']<=now() or row['attachment_id']: raise Rejected('upload_expired',409)
            if offset!=row['received'] or offset+len(data)>row['size']: raise Rejected('upload_offset_changed',409)
            root=Path(self.settings.media_root);root.mkdir(mode=0o700,parents=True,exist_ok=True)
            path=root/(upload_id.hex+'.part')
            fd=os.open(path,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
            with os.fdopen(fd,'r+b') as stream:
                if os.fstat(stream.fileno()).st_size<offset: raise Rejected('upload_incomplete',409)
                # Only an uncommitted tail of this temporary upload is truncated.
                stream.truncate(offset);stream.seek(offset);stream.write(data);stream.flush();os.fsync(stream.fileno())
            received=offset+len(data)
            conn.execute(t.uploads.update().where(t.uploads.c.id==upload_id).values(received=received))
            return {'received':received,'complete':received==row['size']}

    def finish_upload(self,request_id,upload_id):
        # Serialize finalization. A lost HTTP result can resolve to the same file.
        with self.engine.begin() as conn:
            self._request(conn,request_id)
            row=conn.execute(sa.select(t.uploads).where(t.uploads.c.id==upload_id,t.uploads.c.request_id==request_id).with_for_update()).mappings().first()
            if not row or row['expires_at']<=now(): raise Rejected('upload_expired',409)
            if row['attachment_id']:
                return public(conn.execute(sa.select(t.attachments).where(t.attachments.c.id==row['attachment_id'])).mappings().one())
            if row['received']!=row['size']: raise Rejected('upload_incomplete',409)
            path=Path(self.settings.media_root)/(upload_id.hex+'.part')
            try:
                with os.fdopen(os.open(path,os.O_RDONLY|os.O_NOFOLLOW),'rb') as stream: data=stream.read(MAX_FILE+1)
            except OSError: raise Rejected('upload_incomplete',409) from None
            if len(data)!=row['size']: raise Rejected('upload_incomplete',409)
            result=self.save(request_id,row['filename'],data,connection=conn)
            conn.execute(t.uploads.update().where(t.uploads.c.id==upload_id).values(attachment_id=result['id']))
            return result

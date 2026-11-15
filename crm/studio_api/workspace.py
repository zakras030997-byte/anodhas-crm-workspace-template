"""Private Google customer folders, readback and stable markers; no local reports."""
import hashlib
import json
import re
import unicodedata
from uuid import UUID, uuid4
from .google_runtime import call
from .service import Rejected, digest
from .telegram import DeliveryUnknown

ROOT = '1SJdKpw_SQ-GhXcXY_xIUKz9fHofA3t4L'
FOLDER = 'application/vnd.google-apps.folder'
PARTS = ('Референсы', 'Договор', 'Т.З', 'Разработка продукта', 'Карточка клиента')
FIELDS = 'id,name,mimeType,parents,appProperties,permissions(type,role),trashed'

def safe_name(value):
    value = unicodedata.normalize('NFKC', str(value or 'Клиент Telegram'))
    value = ''.join(c for c in value if not unicodedata.category(c).startswith('C'))
    return re.sub(r'[\\/:*?"<>|]+', ' ', value).strip(' .')[:100] or 'Клиент Telegram'

def contact_fields(value):
    try:
        data = json.loads(value or '{}')
        if isinstance(data, dict):
            return {k: str(data.get(k) or 'Не уточнено') for k in ('phone', 'email')}
    except (ValueError, TypeError):
        pass
    return {'phone': 'Не уточнено', 'email': 'Не уточнено'}

def client_card(p):
    contacts = contact_fields(p.get('contact'))
    return '\n'.join(['# Карточка клиента', '',
        'Имя: '+str(p.get('client_display_name') or 'Не уточнено'),
        'Telegram: '+('@'+p['client_username'] if p.get('client_username') else str(p['client_telegram_id'])),
        'Телефон: '+contacts['phone'], 'Почта: '+contacts['email'],
        'Источник: заявка Telegram №'+p['id'], 'Ответственный: owner / ANODHAS AI',
        'Статус: заявка рассмотрена; договор, цена, срок и оплата ещё не согласованы.',
        '', 'Конфиденциально. Для внутренней работы студии.'])

def brief_document(p):
    return '\n'.join(['# Техническое задание — черновик клиента', '',
        'Заявка: '+p['id'], 'Направление: '+str(p.get('service_label') or 'Не уточнено'),
        'Описание:\n'+str(p.get('details') or ''), 'Желаемый результат:\n'+str(p.get('desired_result') or ''),
        'Пожелания по сроку: '+str(p.get('deadline') or 'Не уточнено'),
        '', 'Принято владельцем к рассмотрению и дальнейшему обсуждению. Не договор и не обещание цены/срока.',
        '', client_card(p)])

class Workspace:
    def __init__(self, settings, *, journal=None):
        self.settings, self.journal = settings, journal
        self._journal_error = None

    def _record(self, event):
        if self.journal is not None:
            try:
                self.journal(event)
            except BaseException as exc:
                # Preserve callback failures even when they use provider error types.
                self._journal_error = exc
                raise

    def api(self, method, path, **kwargs):
        return call(self.settings.sheets_credentials_file, 'drive', method,
                    'https://www.googleapis.com/drive/v3/'+path, **kwargs)

    def private(self, metadata):
        if metadata.get('trashed') or any(p.get('type') in ('anyone', 'domain') for p in metadata.get('permissions', [])):
            raise Rejected('client_folder_not_private', 409)
        if 'permissions' not in metadata:
            raise Rejected('client_folder_permissions_unverified', 409)

    def ensure(self, parent, name, marker, content=None):
        # Markers contain only server-derived numeric ID / UUID / fixed part name.
        if not re.fullmatch(r'[a-zA-Z0-9:_-]{1,160}', marker):
            raise Rejected('workspace_marker_invalid')
        if not re.fullmatch(r'[A-Za-z0-9_-]+', parent): raise Rejected('workspace_parent_invalid')
        query = f"'{parent}' in parents and trashed = false and appProperties has {{ key='studio_marker' and value='{marker}' }}"
        found = self.api('GET', 'files', params={'q': query, 'pageSize': 100, 'fields': 'nextPageToken,files('+FIELDS+')'})
        if found.get('nextPageToken') or len(found.get('files', [])) > 1:
            raise Rejected('duplicate_workspace_marker', 409)
        expected_type = FOLDER if content is None else 'text/markdown'
        meta = {'name': name, 'mimeType': expected_type, 'parents': [parent],
                'appProperties': {'studio_marker': marker}}
        if content is not None:
            meta['appProperties']['content_sha256'] = hashlib.sha256(content.encode()).hexdigest()
        receipt = {'service': 'drive', 'operation': 'ensure_folder' if content is None else 'ensure_document',
                   'parent_id': parent, 'marker': marker, 'body_sha256': digest(meta)}
        if content is not None:
            receipt['content_sha256'] = meta['appProperties']['content_sha256']
        existing = found.get('files', [])
        if existing:
            file = existing[0]
        else:
            self._record({**receipt, 'phase': 'write_started'})
            if content is None:
                file = self.api('POST', 'files', body=meta, params={'fields': 'id'})
            else:
                boundary = 'anodhas_'+uuid4().hex
                data = ('--'+boundary+'\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n'+json.dumps(meta, ensure_ascii=False)
                        +'\r\n--'+boundary+'\r\nContent-Type: text/markdown; charset=UTF-8\r\n\r\n'+content+'\r\n--'+boundary+'--\r\n').encode()
                file = call(self.settings.sheets_credentials_file, 'drive', 'POST',
                    'https://www.googleapis.com/upload/drive/v3/files', params={'uploadType': 'multipart', 'fields': 'id'},
                    content=data, content_type='multipart/related; boundary='+boundary)
            if not re.fullmatch(r'[A-Za-z0-9_-]+', str(file.get('id', ''))):
                raise DeliveryUnknown('workspace_create_unknown')
            self._record({**receipt, 'phase': 'write_acknowledged', 'file_id': file['id']})
        try:
            file = self.api('GET', 'files/'+file['id'], params={'fields': FIELDS})
            self.private(file)
            properties = file.get('appProperties')
            if (file.get('name') != name or file.get('mimeType') != expected_type
                    or file.get('parents') != [parent] or not isinstance(properties, dict)
                    or any(properties.get(key) != value for key, value in meta['appProperties'].items())):
                raise DeliveryUnknown('workspace_readback_mismatch')
            if content is not None:
                saved = self.api('GET', 'files/'+file['id'], params={'alt': 'media'}, raw=True)
                if saved != content.encode():
                    raise DeliveryUnknown('workspace_content_mismatch')
        except (Rejected, DeliveryUnknown) as exc:
            error = DeliveryUnknown('workspace_readback_unknown')
            error.receipt = {'file_id': file.get('id'), 'verified': False}
            raise error from None
        self._record({**receipt, 'phase': 'readback_verified', 'file_id': file['id'],
                      'disposition': 'reused' if existing else 'created'})
        return file['id']

    def create(self, p):
        if self.settings.pilot_owner_only or not self.settings.google_workspace_enabled:
            raise Rejected('workspace_not_configured', 503)
        UUID(p['id'])
        client_id = p['client_telegram_id']
        if type(client_id) is not int or client_id <= 0: raise Rejected('workspace_client_invalid')
        parent = self.api('GET', 'files/'+ROOT, params={'fields': FIELDS})
        self.private(parent)
        if parent.get('mimeType') != FOLDER: raise Rejected('workspace_root_invalid')
        marker = 'client:'+str(client_id)
        completed={}
        def tracked(label,*args):
            try:
                identity=self.ensure(*args)
            except (Rejected,DeliveryUnknown) as exc:
                if exc is self._journal_error:
                    raise
                if not completed: raise
                error=DeliveryUnknown('workspace_partial_delivery')
                error.receipt={'verified':False,'failed_step':label,
                    'verified_objects':dict(completed),'in_flight':getattr(exc,'receipt',{}),
                    'error_code':getattr(exc,'code','delivery_unknown')}
                raise error from None
            completed[label]=identity
            return identity
        root = tracked('client_folder',ROOT, safe_name(p.get('client_display_name')), marker)
        folders = {name: tracked('folder_'+str(i),root, name, marker+':part:'+str(i)) for i, name in enumerate(PARTS)}
        # Each accepted request retains its own immutable contact snapshot/brief;
        # a repeat or another order never overwrites previous client material.
        card = tracked('client_card',folders['Карточка клиента'], 'Карточка клиента — '+p['id'][:8]+'.md',
                           'card:'+p['id'], client_card(p))
        brief = tracked('client_brief',folders['Т.З'], 'ТЗ — '+p['id'][:8]+'.md', 'brief:'+p['id'], brief_document(p))
        return {'verified': True, 'folder_id': root, 'folder_url': 'https://drive.google.com/drive/folders/'+root,
                'card_id': card, 'brief_id': brief, 'folders': folders}

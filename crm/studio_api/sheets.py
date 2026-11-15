"""One canonical spreadsheet; RAW values, stable per-request markers, read-back."""
from pathlib import Path
from urllib.parse import quote
import httpx
from .service import Rejected,digest
from .telegram import DeliveryUnknown

BOOK='1oOmW98RVS7VrZudu1JX2MCPLar0r5Ix9ay0rZ3y3OsY'
TABS={'leads':('Оперативная память',['Лид','Telegram','Заказ / потребность','Последнее событие','Статус','Что ждём от лида','Следующее действие','Контроль','Источник']),
      'deals':('Сделки',['Клиент','Telegram','Проект','ТЗ / ссылка','Срок','Стоимость','Предоплата / условия','Ресурсы','Стадия','Следующий шаг','Договор / ссылка','Карточка заказа','Обновлено'])}

def lead_values(p,workspace):
    """Exact immutable RAW row, shared with the owner acceptance preview."""
    if p.get('leadgen'):
        return research_lead_values(p)
    marker=f'ANODHAS Studio · request:{p["id"]} · lead:{p["lead_id"]}'
    identity=('@'+p['client_username']) if p.get('client_username') else str(p['client_telegram_id'])
    contact=identity+' · '+str(p['contact']) if p.get('contact') and p['contact']!=identity else identity
    from .workspace import contact_fields
    contacts=contact_fields(p.get('contact'))
    if contacts['phone']!='Не уточнено' or contacts['email']!='Не уточнено':
        contact=identity+' · Телефон: '+contacts['phone']+' · Почта: '+contacts['email']
    brief=str(p.get('service_label') or '')+' · '+str(p.get('details') or '')
    if p.get('desired_result'): brief+='\nРезультат: '+str(p['desired_result'])
    if workspace: brief+='\nКарточка проекта: '+workspace['folder_url']
    return [p.get('client_display_name') or 'Клиент Telegram',contact,
        brief,'Заявка одобрена владельцем','Новый',
        'Уточнение задачи','Ответить по заявке',p.get('deadline') or 'Не согласовано',marker]

class Sheets:
    def __init__(self,settings,*,journal=None):
        self.settings,self.journal=settings,journal
        self._journal_error=None

    def _record(self,event):
        if self.journal is not None:
            try:
                self.journal(event)
            except BaseException as exc:
                self._journal_error=exc
                raise

    def _token(self):
        if self.settings.pilot_owner_only:
            raise Rejected('sheets_disabled_in_pilot',503)
        if not self.settings.sheets_enabled or not self.settings.sheets_credentials_file:
            raise Rejected('sheets_not_configured',503)
        from .google_runtime import access_token
        return access_token(self.settings.sheets_credentials_file, 'sheets')

    def call(self,method,path,*,body=None,params=None):
        token=self._token()
        try:
            with httpx.Client(timeout=30,follow_redirects=False) as client:
                r=client.request(method,f'https://sheets.googleapis.com/v4/spreadsheets/{BOOK}/{path}',headers={'Authorization':'Bearer '+token},json=body,params=params)
            if r.status_code>=500: raise DeliveryUnknown('google_server_unknown')
            if r.status_code>=400: raise Rejected('google_request_rejected',503)
            return r.json()
        except (httpx.HTTPError,ValueError):
            raise DeliveryUnknown('google_transport_unknown') from None

    def values(self,range):
        return self.call('GET','values/'+quote(range,safe=''),params={'valueRenderOption':'UNFORMATTED_VALUE','dateTimeRenderOption':'FORMATTED_STRING'}).get('values',[])

    def table(self,kind):
        name,headers=TABS[kind];last='I' if kind=='leads' else 'M'
        values=self.values(f"'{name}'!A1:{last}10000")
        if not values or values[0]!=headers: raise Rejected('sheets_schema_changed',409)
        rows=[]
        for i,row in enumerate(values[1:],2):
            if not any(str(v).strip() for v in row): continue
            padded=(list(row)+['']*len(headers))[:len(headers)]
            rows.append({'row':i,'values':padded,'revision':digest(padded)})
        if len(values)>=10000: raise Rejected('sheets_row_limit_requires_review',409)
        return {'sheet':name,'headers':headers,'rows':rows,'url':f'https://docs.google.com/spreadsheets/d/{BOOK}/edit','source':'google_sheets'}

    def create_lead(self,p):
        table=self.table('leads')
        workspace = None
        if self.settings.google_workspace_enabled and not p.get('leadgen'):
            from .workspace import Workspace
            workspace = Workspace(self.settings,journal=self.journal).create(p)
        try:
            return self._create_lead(p,table,workspace)
        except (Rejected,DeliveryUnknown) as exc:
            if exc is self._journal_error: raise
            if not workspace: raise
            # Drive succeeded already. A rejected Sheets write is not an empty
            # failure: preserve its verified objects for explicit reconciliation.
            error=DeliveryUnknown('lead_partial_delivery')
            error.receipt={'verified':False,'failed_step':'sheets_lead',
                'workspace':workspace,'sheets':getattr(exc,'receipt',{}),
                'error_code':getattr(exc,'code','delivery_unknown')}
            raise error from None

    def _create_lead(self,p,table,workspace):
        marker=f'ANODHAS Studio · request:{p["id"]} · lead:{p["lead_id"]}'
        existing=[r for r in table['rows'] if marker in r['values'][8].split('\n')]
        if len(existing)>1: raise Rejected('duplicate_lead_marker',409)
        values=lead_values(p,workspace)
        receipt={'service':'sheets','operation':'append_lead','spreadsheet_id':BOOK,
            'range':"'Оперативная память'!A:I",'marker':marker,'body_sha256':digest({'values':[values]})}
        if existing:
            if existing[0]['values']!=values:
                raise Rejected('existing_lead_changed_requires_review',409)
            row=existing[0]['row']
            self._record({**receipt,'phase':'readback_verified',
                'updated_range':f"'Оперативная память'!A{row}:I{row}",'disposition':'reused'})
            return {'sheet':table['sheet'],'row':existing[0]['row'],'lead_id':p['lead_id'],'verified':True,'existing':True,'workspace':workspace}
        # RAW prevents client text from becoming formulas. Whole new row only;
        # no contact-based update of an existing lead and no agreed-deal status.
        self._record({**receipt,'phase':'write_started'})
        result=self.call('POST','values/'+quote("'Оперативная память'!A:I",safe='')+':append',body={'values':[values]},params={'valueInputOption':'RAW','insertDataOption':'INSERT_ROWS','includeValuesInResponse':'true'})
        target=result.get('updates',{}).get('updatedRange','')
        import re
        match=re.fullmatch(r"'?Оперативная память'?!A(\d+):I\1",target)
        if not match or int(match[1])<2: raise DeliveryUnknown('google_write_range_unknown')
        self._record({**receipt,'phase':'write_acknowledged','updated_range':target})
        try:
            after=self.values(target)
            if after!=[values]: raise DeliveryUnknown('google_readback_mismatch')
        except (Rejected,DeliveryUnknown):
            error=DeliveryUnknown('google_readback_unknown')
            error.receipt={'sheet':table['sheet'],'updated_range':target,'lead_id':p['lead_id'],'write_acknowledged':True,'verified':False}
            raise error from None
        self._record({**receipt,'phase':'readback_verified','updated_range':target,'disposition':'created'})
        return {'sheet':table['sheet'],'row':int(match[1]),'lead_id':p['lead_id'],'verified':True,'workspace':workspace}


def research_lead_values(p):
    source=p['leadgen']
    marker=f'ANODHAS Studio · request:{p["id"]} · lead:{p["lead_id"]}'
    return [p['client_display_name'],p.get('contact') or 'Не указан',
        p['service_label']+' · '+p['details'],
        'Кандидат выбран владельцем. Ответственный: '+source['assignee'],source['status'],
        'Проверка контекста и актуальности запроса', 'Проверить первоисточник',
        p.get('deadline') or 'Не согласовано',
        source['url']+'\n'+marker+'\nAI Sales Radar · candidate:'+source['candidate_id']]

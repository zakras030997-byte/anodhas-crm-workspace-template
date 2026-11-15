from typing import Literal
from uuid import UUID
import hmac
import json
import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, Request
from fastapi.responses import JSONResponse, RedirectResponse, HTMLResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from .config import Settings
from .auth import Auth, COOKIE, LOGIN_COOKIE
from .service import CRM, Rejected
from .preview import render_preview
from .bot_auth import BotAuth
from .miniapp_auth import MiniAppAuth, MAX_INIT_DATA, strict_json
from .crm_bot import CrmBot
from .telegram import Telegram, DeliveryUnknown
from .intake import Intake
from .media import Media,MAX_FILE
from .sheets import Sheets
from .dispatch import Dispatcher
from .notifications import Notifications
from fastapi.responses import Response
from urllib.parse import quote

BOT_LOGIN_COOKIE='__Host-anodhas_bot_login'

class BotLoginResume(BaseModel):
    model_config=ConfigDict(extra='forbid')
    challenge: str = Field(pattern=r'^[A-Za-z0-9_-]{32}$')

class BotLoginFinish(BotLoginResume):
    code: str = Field(min_length=1,max_length=16)


class Body(BaseModel):
    model_config = ConfigDict(extra='forbid')

class DispatchBody(Body):
    dry_run: bool = True
    limit: int = Field(default=3,ge=1,le=5)

class UploadStart(Body):
    filename: str = Field(min_length=1,max_length=200)
    size: int = Field(ge=1,le=20*1024*1024)

class Decision(Body):
    revision: int = Field(ge=1)
    decision: Literal['approved','rejected']
    operation_key: UUID

class Draft(Body):
    revision: int = Field(ge=0)
    body: str = Field(max_length=4000)
    attachment_ids: list[UUID] = Field(default_factory=list,max_length=10)

class Send(Body):
    draft_revision: int = Field(ge=1)
    operation_key: UUID

class EmailDraft(Body):
    revision: int = Field(ge=0)
    recipient: str = Field(max_length=254)
    subject: str = Field(max_length=200)
    body: str = Field(max_length=20000)

class Stage(Body):
    id: UUID
    revision: int = Field(ge=0)
    title: str = Field(min_length=1,max_length=160)
    state: Literal['not_started','in_progress','done']
    public_result: str = Field(default='',max_length=2000)
    private_note: str = Field(default='',max_length=4000)
    deadline: str = Field(default='',max_length=64)

class LeadgenStart(Body):
    direction_id: int = Field(ge=1,le=11,strict=True)
    mode: Literal['tg','global','threads']
    operation_key: UUID

class LeadgenItem(Body):
    candidate_id: str = Field(pattern=r'^[a-f0-9]{64}$')
    assignee: str = Field(min_length=1,max_length=80,pattern=r'^\S(?:[^\r\n]*\S)?$')
    status: Literal['Новый','На проверке','В работе','Отложен']

class LeadgenImport(Body):
    run_id: str = Field(pattern=r'^[a-f0-9]{32}$')
    operation_key: UUID
    items: list[LeadgenItem] = Field(min_length=1,max_length=10)


def create_app(settings: Settings, *, engine=None, identity=None, telegram=None, crm_telegram=None, radar=None, sheets_adapter=None, gmail=None):
    # Schema changes belong to reviewed migrations, never app startup.
    production=engine is None
    engine = engine or create_engine(settings.database_url,pool_pre_ping=True,pool_size=5,max_overflow=5)
    auth, crm = Auth(engine,settings,identity), CRM(engine,settings.owner_id,settings=settings)
    telegram=telegram or Telegram(settings)
    crm_telegram=(crm_telegram or Telegram(settings.auth_bot)) if settings.crm_bot_enabled else None
    bot_auth=BotAuth(engine,settings.auth_bot,crm_telegram if settings.crm_bot_enabled else telegram)
    miniapp_auth=MiniAppAuth(engine,settings)
    crm_bot=CrmBot(engine,settings,crm_telegram,bot_auth)
    intake=Intake(engine,settings,telegram,bot_auth)
    media=Media(engine,settings,telegram)
    sheets=sheets_adapter or Sheets(settings)
    from .leadgen import Leadgen
    leadgen=Leadgen(engine,settings,radar=radar,sheets=sheets)
    worker=Dispatcher(engine,settings,telegram,media,sheets)
    notifications=Notifications(engine,settings)
    from .gmail_outreach import MailService
    mail=MailService(engine,settings,mail=gmail)
    async def automation_loop():
        while True:
            try:
                if settings.dispatch_enabled:
                    await asyncio.to_thread(worker.run,3)
                else:
                    from .outbox import BotOutbox
                    await asyncio.to_thread(BotOutbox(engine,settings,telegram).flush)
                if settings.owner_email_enabled:
                    await asyncio.to_thread(notifications.run,3)
            except Exception:
                # Durable queues preserve uncertain sends; never log tokens,
                # request details or raw transport/database exceptions.
                logging.getLogger(__name__).warning('Studio automation requires queue review')
            await asyncio.sleep(5)
    async def mail_loop():
        # Slow Gmail/OAuth must not delay Telegram login delivery or CRM workers.
        while True:
            try: await asyncio.to_thread(mail.run,1)
            except Exception:
                logging.getLogger(__name__).warning('Studio email queue requires review')
            await asyncio.sleep(5)
    @asynccontextmanager
    async def lifespan(_app):
        task=None
        mail_task=None
        if production:
            from .migrate import check_identity
            check_identity(engine,settings.bot_id,minimum_version=3)
            if settings.crm_bot_enabled:
                from .migrate import check_crm_bot_schema
                check_crm_bot_schema(engine)
                crm_telegram.verify()
            if settings.gmail_outreach_enabled:
                import sqlalchemy as sa
                from . import tables as t
                with engine.connect() as conn:
                    if not all(sa.inspect(conn).has_table(table.name) for table in (t.email_drafts,t.email_deliveries,t.email_operations)):
                        raise RuntimeError('Explicit Gmail migration required')
            if settings.anod_enabled and not settings.pilot_owner_only:
                if not all((settings.ai_enabled,settings.ai_api_key,settings.sheets_enabled,
                    settings.google_workspace_enabled,settings.sheets_credentials_file,
                    settings.owner_email_enabled,settings.gmail_credentials_file,settings.dispatch_enabled)):
                    raise RuntimeError('Public intake requires reviewed runtime integrations')
            if settings.bot_token:
                telegram.verify()
            if settings.anod_enabled:
                task=asyncio.create_task(automation_loop())
            if settings.gmail_outreach_enabled:
                mail_task=asyncio.create_task(mail_loop())
        try:
            yield
        finally:
            for pending in (task,mail_task):
                if pending:
                    pending.cancel()
                    try: await pending
                    except asyncio.CancelledError: pass
    app = FastAPI(title='ANODHAS owner API',docs_url=None,redoc_url=None,openapi_url=None,lifespan=lifespan)
    app.state.auth, app.state.crm = auth, crm
    app.state.leadgen, app.state.worker = leadgen, worker
    app.state.mail=mail
    app.state.bot_auth,app.state.intake=bot_auth,intake
    app.state.miniapp_auth,app.state.crm_bot=miniapp_auth,crm_bot

    @app.exception_handler(Rejected)
    async def rejected(_request, exc):
        return JSONResponse({'error':exc.code},status_code=exc.status)

    @app.exception_handler(SQLAlchemyError)
    async def database_unavailable(_request, _exc):
        # No DSN, SQL bound data or raw exception in client response.
        return JSONResponse({'error':'database_unavailable'},status_code=503)

    @app.exception_handler(DeliveryUnknown)
    async def delivery_unknown(_request,_exc):
        return JSONResponse({'error':'delivery_unknown'},status_code=503)

    @app.middleware('http')
    async def private_headers(request, call_next):
        response = await call_next(request)
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['Referrer-Policy'] = 'no-referrer'
        return response

    def owner(request: Request):
        return auth.require(request.cookies.get(COOKIE))

    def change(request: Request):
        return auth.require(request.cookies.get(COOKIE),origin=request.headers.get('origin'),
            csrf=request.headers.get('x-csrf-token'),mutation=True)

    @app.get('/api/health')
    def health():
        return {'status':'up','scope':'api_process_only'}

    @app.get('/api/ready')
    def ready():
        from .migrate import check_identity
        check_identity(engine,settings.bot_id,minimum_version=3)
        if settings.crm_bot_enabled:
            from .migrate import check_crm_bot_schema
            check_crm_bot_schema(engine)
        return {'status':'ready','scope':'isolated_api_and_database'}

    @app.get('/api/auth/config')
    def config():
        return {'configured':settings.login_configured,'bot_login_configured':bot_auth.enabled(),
            'miniapp_configured':settings.crm_bot_enabled,'bot_username':settings.auth_bot.bot_username,
            'miniapp_url':settings.miniapp_url}

    @app.post('/api/auth/miniapp')
    async def miniapp_login(request: Request):
        if not settings.crm_bot_enabled:
            raise Rejected('miniapp_not_configured',503)
        origin=request.headers.get('origin')
        if origin != settings.origin:
            raise Rejected('csrf_rejected',403)
        # Parse privately: framework validation errors can echo rejected bodies.
        raw=bytearray()
        async for chunk in request.stream():
            raw.extend(chunk)
            if len(raw)>MAX_INIT_DATA*2:
                raise Rejected('miniapp_invalid_data',401)
        try:
            body=strict_json(raw)
            if not isinstance(body,dict) or set(body)!={'init_data'} or not isinstance(body['init_data'],str):
                raise ValueError()
        except (ValueError,TypeError,UnicodeError,RecursionError):
            raise Rejected('miniapp_invalid_data',401) from None
        from starlette.concurrency import run_in_threadpool
        token=await run_in_threadpool(miniapp_auth.authenticate,body['init_data'],origin)
        response=JSONResponse({'authenticated':True})
        response.set_cookie(COOKIE,token,secure=True,httponly=True,samesite='lax',path='/',max_age=43200)
        return response

    @app.post('/api/auth/bot/start')
    def bot_start(request: Request):
        result,binding=bot_auth.start(request.headers.get('origin'))
        response=JSONResponse(result)
        response.set_cookie(BOT_LOGIN_COOKIE,binding,secure=True,httponly=True,samesite='strict',path='/',max_age=420)
        return response

    @app.post('/api/auth/bot/finish')
    def bot_finish(request: Request,body: BotLoginFinish):
        token=bot_auth.finish(body.challenge,body.code,request.cookies.get(BOT_LOGIN_COOKIE),request.headers.get('origin'))
        response=JSONResponse({'authenticated':True})
        response.delete_cookie(BOT_LOGIN_COOKIE,path='/',secure=True,httponly=True,samesite='strict')
        response.set_cookie(COOKIE,token,secure=True,httponly=True,samesite='lax',path='/',max_age=43200)
        return response

    @app.post('/api/auth/bot/resume')
    def bot_resume(request: Request,body: BotLoginResume):
        return bot_auth.resume(body.challenge,request.cookies.get(BOT_LOGIN_COOKIE),request.headers.get('origin'))

    @app.post('/api/telegram/webhook')
    async def webhook(request: Request):
        supplied=request.headers.get('x-telegram-bot-api-secret-token','')
        if len(settings.webhook_secret)<32 or not hmac.compare_digest(supplied,settings.webhook_secret):
            raise Rejected('webhook_access_required',403)
        raw=bytearray()
        async for chunk in request.stream():
            raw.extend(chunk)
            if len(raw)>256*1024:
                raise Rejected('update_too_large',413)
        try:
            update=json.loads(raw)
            if not isinstance(update,dict): raise ValueError()
        except (ValueError,UnicodeDecodeError):
            raise Rejected('invalid_update',422) from None
        # Business data and the update receipt commit atomically. Telegram retries
        # transient DB failures; an uncertain outgoing call is never resent here.
        from starlette.concurrency import run_in_threadpool
        return await run_in_threadpool(intake.handle,update)

    @app.post('/api/telegram/crm-webhook')
    async def crm_webhook(request: Request):
        supplied=request.headers.get('x-telegram-bot-api-secret-token','')
        if (not settings.crm_bot_enabled or len(settings.crm_bot_webhook_secret)<32
                or not hmac.compare_digest(supplied.encode(),settings.crm_bot_webhook_secret.encode())):
            raise Rejected('webhook_access_required',403)
        raw=bytearray()
        async for chunk in request.stream():
            raw.extend(chunk)
            if len(raw)>256*1024:
                raise Rejected('update_too_large',413)
        try:
            update=strict_json(raw)
            if not isinstance(update,dict): raise ValueError()
        except (ValueError,TypeError,UnicodeError,RecursionError):
            raise Rejected('invalid_update',422) from None
        from starlette.concurrency import run_in_threadpool
        return await run_in_threadpool(crm_bot.handle,update)

    @app.get('/api/auth/telegram/start')
    def start():
        location,binding = auth.start()
        response = RedirectResponse(location,status_code=303)
        response.set_cookie(LOGIN_COOKIE,binding,secure=True,httponly=True,samesite='lax',path='/',max_age=300)
        return response

    @app.get('/api/auth/telegram/callback')
    def callback(request: Request,state: str='',code: str=''):
        token = auth.callback(state,code,request.cookies.get(LOGIN_COOKIE))
        response = RedirectResponse('/admin/',status_code=303)
        response.delete_cookie(LOGIN_COOKIE,path='/',secure=True,httponly=True,samesite='lax')
        response.set_cookie(COOKIE,token,secure=True,httponly=True,samesite='lax',path='/',max_age=43200)
        return response

    @app.get('/api/session')
    def session(current=Depends(owner)):
        return current

    @app.post('/api/logout',dependencies=[Depends(change)])
    def logout(request: Request):
        auth.logout(request.cookies[COOKIE])
        response = JSONResponse({'logged_out':True})
        response.delete_cookie(COOKIE,path='/',secure=True,httponly=True,samesite='lax')
        return response

    @app.get('/api/leadgen/catalog',dependencies=[Depends(owner)])
    def leadgen_catalog(): return leadgen.catalog()

    @app.get('/api/leadgen/runs',dependencies=[Depends(owner)])
    def leadgen_runs(): return leadgen.runs()

    @app.get('/api/leadgen/runs/{run_id}',dependencies=[Depends(owner)])
    def leadgen_run(run_id: str): return leadgen.run(run_id)

    @app.post('/api/leadgen/runs',dependencies=[Depends(change)],status_code=202)
    def leadgen_start(body: LeadgenStart):
        return leadgen.start(body.direction_id,body.mode,body.operation_key)

    @app.post('/api/leadgen/runs/{run_id}/cancel',dependencies=[Depends(change)])
    def leadgen_cancel(run_id: str): return leadgen.cancel(run_id)

    @app.post('/api/leadgen/import',dependencies=[Depends(change)],status_code=202)
    def leadgen_import(body: LeadgenImport):
        return leadgen.import_cards(body.run_id,[i.model_dump() for i in body.items],body.operation_key)

    @app.post('/api/leadgen/requests/{request_id}/recover',dependencies=[Depends(change)])
    def leadgen_recover(request_id: UUID): return leadgen.recover(request_id,worker)

    @app.get('/api/requests',dependencies=[Depends(owner)])
    def requests():
        return {'items':crm.list_requests()}

    @app.get('/api/requests/{request_id}',dependencies=[Depends(owner)])
    def request_detail(request_id: UUID):
        return crm.detail(request_id)

    @app.post('/api/requests/{request_id}/decision',dependencies=[Depends(change)])
    def decision(request_id: UUID,body: Decision):
        return crm.decide(request_id,body.revision,body.decision,body.operation_key)

    @app.get('/api/requests/{request_id}/messages',dependencies=[Depends(owner)])
    def history(request_id: UUID,before: UUID | None=None):
        return crm.history(request_id,before)

    @app.post('/api/requests/{request_id}/attachments',dependencies=[Depends(change)],status_code=201)
    async def upload(request_id: UUID,request: Request,filename: str='attachment'):
        data=bytearray()
        async for chunk in request.stream():
            data.extend(chunk)
            if len(data)>MAX_FILE: raise Rejected('file_too_large',413)
        from starlette.concurrency import run_in_threadpool
        return await run_in_threadpool(media.save,request_id,filename,bytes(data))

    @app.get('/api/requests/{request_id}/attachments/{attachment_id}',dependencies=[Depends(owner)])
    def download(request_id: UUID,attachment_id: UUID):
        data,filename=media.download(request_id,attachment_id)
        return Response(data,media_type='application/octet-stream',headers={
            'Content-Disposition':"attachment; filename*=UTF-8''"+quote(filename,safe=''),
            'Content-Security-Policy':"default-src 'none'; sandbox",
        })

    @app.post('/api/requests/{request_id}/uploads',dependencies=[Depends(change)])
    def upload_start(request_id: UUID,body: UploadStart):
        return media.start_upload(request_id,body.filename,body.size)

    @app.post('/api/requests/{request_id}/uploads/{upload_id}/chunk',dependencies=[Depends(change)])
    async def upload_chunk(request_id: UUID,upload_id: UUID,request: Request,offset: int):
        data=bytearray()
        async for chunk in request.stream():
            data.extend(chunk)
            if len(data)>512*1024: raise Rejected('invalid_chunk_size',413)
        from starlette.concurrency import run_in_threadpool
        return await run_in_threadpool(media.upload_chunk,request_id,upload_id,offset,bytes(data))

    @app.post('/api/requests/{request_id}/uploads/{upload_id}/finish',dependencies=[Depends(change)])
    def upload_finish(request_id: UUID,upload_id: UUID):
        return media.finish_upload(request_id,upload_id)

    @app.put('/api/requests/{request_id}/draft',dependencies=[Depends(change)])
    def draft(request_id: UUID,body: Draft):
        return crm.save_draft(request_id,body.revision,body.body,body.attachment_ids)

    @app.post('/api/requests/{request_id}/send',dependencies=[Depends(change)],status_code=202)
    def send(request_id: UUID,body: Send):
        return crm.queue_message(request_id,body.draft_revision,body.operation_key)

    @app.put('/api/requests/{request_id}/stages',dependencies=[Depends(change)])
    def stage(request_id: UUID,body: Stage):
        return crm.save_stage(request_id,body.id,body.revision,body.model_dump(exclude={'id','revision'}))

    @app.get('/api/requests/{request_id}/stage-report',dependencies=[Depends(owner)])
    def stage_report(request_id: UUID):
        return crm.stage_report(request_id)

    @app.get('/api/jobs',dependencies=[Depends(owner)])
    def jobs():
        return {'items':crm.list_jobs()}

    @app.get('/api/mail/status',dependencies=[Depends(owner)])
    def mail_status(): return mail.status()

    @app.get('/api/requests/{request_id}/email',dependencies=[Depends(owner)])
    def email_detail(request_id: UUID): return mail.detail(request_id)

    @app.put('/api/requests/{request_id}/email/draft',dependencies=[Depends(change)])
    def email_draft(request_id: UUID,body: EmailDraft):
        return mail.save_draft(request_id,body.revision,body.recipient,body.subject,body.body)

    @app.post('/api/requests/{request_id}/email/send',dependencies=[Depends(change)],status_code=202)
    def email_send(request_id: UUID,body: Send):
        return mail.queue(request_id,body.draft_revision,body.operation_key)

    @app.get('/api/requests/{request_id}/email/operations/{operation_key}',dependencies=[Depends(owner)])
    def email_operation(request_id: UUID,operation_key: UUID):
        return mail.operation(request_id,operation_key)

    @app.post('/api/requests/{request_id}/email/deliveries/{delivery_id}/check',dependencies=[Depends(change)])
    def email_check(request_id: UUID,delivery_id: UUID): return mail.check(request_id,delivery_id)

    @app.get('/api/registry/{kind}',dependencies=[Depends(owner)])
    def registry(kind: Literal['leads','deals']):
        return sheets.table(kind)

    @app.get('/api/preview',dependencies=[Depends(owner)])
    def preview(path: str='/',theme: Literal['light','dark']='light'):
        return HTMLResponse(render_preview(settings.public_root,path,theme),headers={
            'Content-Security-Policy':"default-src 'none'; style-src 'self' 'unsafe-inline'; img-src 'self'; font-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'self'; sandbox",
            'X-Frame-Options':'SAMEORIGIN',
        })

    @app.post('/internal/jobs/dispatch')
    def dispatch(request: Request,body: DispatchBody=DispatchBody()):
        supplied = request.headers.get('authorization','')
        if len(settings.dispatch_secret)<32 or not hmac.compare_digest(supplied,'Bearer '+settings.dispatch_secret):
            raise Rejected('service_access_required',403)
        if body.dry_run: return {'state':'dry_run','external_actions':0}
        return worker.run(body.limit)

    return app

def application():
    return create_app(Settings.from_env())

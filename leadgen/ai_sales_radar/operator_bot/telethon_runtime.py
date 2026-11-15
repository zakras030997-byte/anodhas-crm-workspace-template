"""Telethon transport and lifecycle for the private local operator bot."""

from __future__ import annotations

from collections.abc import Callable
import asyncio
import logging
from typing import Any, Protocol

from telethon import Button, TelegramClient, events
from telethon.errors import MessageNotModifiedError

from freelancer_bot.observability import Redactor, configure_structured_logger, log_event
from freelancer_bot.persistence.database import Database
from freelancer_bot.telegram_session import TelegramSessionFileLock

from ai_sales_radar.config import (
    DatabaseSettings,
    LOCAL_RUNTIME_ROOT,
    OperatorBotCredentials,
    OperatorBotSettings,
    OperatorSourceDiscoverySettings,
    ReplyDraftSettings,
)
from ai_sales_radar.telegram_auth import (
    disconnect_client_fully,
    prepare_private_session_storage,
    private_umask,
    protect_session_files,
)

from .auth import OwnerGate
from .callbacks import SignedCallbackCodec
from .controller import OperatorBotController
from .models import OperatorButton, OperatorResponse
from .service import DatabaseOperatorCore
from ai_sales_radar.research.service import ResearchService


class OperatorTelegramClient(Protocol):
    def add_event_handler(self, callback: Any, event: Any = None) -> Any: ...

    async def start(self, *, bot_token: str) -> Any: ...

    async def get_me(self) -> Any: ...

    async def run_until_disconnected(self) -> Any: ...

    async def send_message(self, entity: int, message: str, **kwargs: Any) -> Any: ...

    async def disconnect(self) -> Any: ...


class OperatorBotIdentityError(RuntimeError):
    """The Telethon session is not authorized as the configured bot."""


class TelethonOperatorTransport:
    def __init__(
        self,
        client: OperatorTelegramClient,
        controller: OperatorBotController,
        *,
        owner_chat_id: int,
        logger: logging.Logger,
    ) -> None:
        self._client = client
        self._controller = controller
        self._owner_chat_id = owner_chat_id
        self._logger = logger
        self._research_tasks: set[asyncio.Task] = set()

    def register(self) -> None:
        self._client.add_event_handler(
            self._on_message,
            events.NewMessage(incoming=True),
        )
        self._client.add_event_handler(
            self._on_callback,
            events.CallbackQuery(),
        )

    async def _on_message(self, event: Any) -> None:
        identity = _event_identity(event)
        if not self._controller.gate.allows(**identity):
            return
        text = getattr(event, "raw_text", "")
        if not isinstance(text, str):
            text = ""
        try:
            responses = await self._controller.handle_message(
                **identity,
                text=text,
            )
            await self._send_all(responses)
        except Exception as exc:
            await self._report_owner_failure("message", exc)

    async def _on_callback(self, event: Any) -> None:
        identity = _event_identity(event)
        if not self._controller.gate.allows(**identity):
            await _safe_callback_answer(event, "Недоступно", alert=True)
            return
        await _safe_callback_answer(event, "Принято")
        data = getattr(event, "data", b"")
        if not isinstance(data, bytes):
            data = b""
        try:
            outcome = await self._controller.handle_callback(
                **identity,
                data=data,
            )
            if outcome is not None:
                if outcome.replace_message and len(outcome.responses) == 1:
                    response = outcome.responses[0]
                    try:
                        await event.edit(
                            response.text, parse_mode=response.parse_mode,
                            buttons=_telethon_buttons(response.buttons), link_preview=False,
                        )
                    except MessageNotModifiedError:
                        pass
                else:
                    await self._send_all(outcome.responses)
                if outcome.background_job:
                    task = asyncio.create_task(self._finish_research(outcome.background_job))
                    self._research_tasks.add(task)
                    task.add_done_callback(self._research_tasks.discard)
        except Exception as exc:
            await self._report_owner_failure("callback", exc)

    async def _finish_research(self, run_id):
        try:
            responses = await self._controller.complete_research(run_id)
            await self._send_all(responses)
        except Exception as exc:
            await self._report_owner_failure("research", exc)

    async def close(self):
        for task in self._research_tasks:
            task.cancel()
        await asyncio.gather(*self._research_tasks, return_exceptions=True)

    async def _send_all(self, responses: tuple[OperatorResponse, ...]) -> None:
        for response in responses:
            await self._client.send_message(
                self._owner_chat_id,
                response.text,
                parse_mode=response.parse_mode,
                buttons=_telethon_buttons(response.buttons),
                link_preview=False,
            )

    async def _report_owner_failure(self, event_kind: str, exc: Exception) -> None:
        log_event(
            self._logger,
            logging.ERROR,
            "operator_bot.request_failed",
            event_kind=event_kind,
            error_type=type(exc).__name__,
        )
        try:
            await self._client.send_message(
                self._owner_chat_id,
                "Операция не выполнена. Подробности скрыты для защиты данных.",
                link_preview=False,
            )
        except Exception:
            log_event(
                self._logger,
                logging.ERROR,
                "operator_bot.failure_notice_failed",
                event_kind=event_kind,
            )


async def run_operator_bot(
    database_settings: DatabaseSettings,
    bot_settings: OperatorBotSettings,
    reply_settings: ReplyDraftSettings,
    *,
    source_discovery_settings: OperatorSourceDiscoverySettings | None = None,
    database_factory: Callable[..., Database] = Database,
    client_factory: Callable[[OperatorBotCredentials], OperatorTelegramClient]
    | None = None,
    lock_factory: Callable[..., TelegramSessionFileLock] = TelegramSessionFileLock,
    stop_event: asyncio.Event | None = None,
    research_factory=ResearchService,
    research_reader=None,
) -> None:
    database_url = database_settings.require_url()
    credentials = bot_settings.require_credentials()
    redactor = Redactor(
        {
            database_url,
            credentials.api_hash,
            credentials.bot_token,
            str(credentials.owner_user_id),
            str(credentials.owner_chat_id),
            str(credentials.session_path),
            str(bot_settings.collector_session_path),
        }
    )
    logger = configure_structured_logger(
        "ai_sales_radar.operator_bot",
        redactor=redactor,
    )
    prepare_private_session_storage(
        credentials.session_path,
        runtime_root=LOCAL_RUNTIME_ROOT,
    )
    protect_session_files(
        credentials.session_path,
        runtime_root=LOCAL_RUNTIME_ROOT,
    )
    lock = lock_factory(credentials.session_path, role="ai_sales_radar_operator_bot")
    try:
        lock.acquire()
    except BaseException:
        lock.release()
        raise
    client: OperatorTelegramClient | None = None
    database: Database | None = None
    transport = None
    research_bridge = None
    try:
        with private_umask():
            client = (client_factory or _default_client_factory)(credentials)
        database = database_factory(database_url, pool_size=3, max_overflow=1)
        await client.start(bot_token=credentials.bot_token)
        account = await client.get_me()
        if (
            getattr(account, "bot", None) is not True
            or getattr(account, "id", None) != credentials.bot_id
        ):
            raise OperatorBotIdentityError(
                "Локальная bot-сессия принадлежит другому Telegram-боту"
            )
        gate = OwnerGate(
            credentials.owner_user_id,
            credentials.owner_chat_id,
        )
        research = research_factory(database, telegram_reader=research_reader)
        controller = OperatorBotController(
            DatabaseOperatorCore(
                database,
                reply_settings,
                source_discovery_settings=source_discovery_settings,
            ),
            gate=gate,
            callback_codec=SignedCallbackCodec(
                credentials.bot_token,
                ttl_seconds=bot_settings.callback_ttl_seconds,
            ),
            research=research,
            max_leads=bot_settings.max_leads_per_command,
            reads_per_minute=bot_settings.reads_per_minute,
            mutations_per_minute=bot_settings.mutations_per_minute,
            drafts_per_hour=bot_settings.drafts_per_hour,
        )
        transport = TelethonOperatorTransport(
            client,
            controller,
            owner_chat_id=credentials.owner_chat_id,
            logger=logger,
        )
        transport.register()
        import os
        bridge_path = os.environ.get('ANODHAS_RESEARCH_SOCKET', '')
        if bridge_path:
            if bridge_path != '/run/leadgen/research.sock':
                raise RuntimeError('Unexpected research socket location')
            from ai_sales_radar.research.bridge import ResearchBridge
            research_bridge = ResearchBridge(research, bridge_path)
            await research_bridge.start()
        log_event(
            logger,
            logging.INFO,
            "operator_bot.started",
            private_owner_configured=True,
            automatic_client_sending_enabled=False,
            notifications_enabled=bot_settings.notifications_enabled,
        )
        if stop_event is None:
            await client.run_until_disconnected()
        else:
            disconnected = asyncio.ensure_future(client.run_until_disconnected())
            stopping = asyncio.create_task(stop_event.wait())
            try:
                done, _ = await asyncio.wait(
                    {disconnected, stopping}, return_when=asyncio.FIRST_COMPLETED
                )
                if disconnected in done:
                    await disconnected
                    if not stop_event.is_set():
                        raise RuntimeError("Operator transport disconnected")
            finally:
                for task in (disconnected, stopping):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(disconnected, stopping, return_exceptions=True)
    finally:
        try:
            if research_bridge is not None:
                await research_bridge.close()
            if transport is not None:
                await transport.close()
            if client is not None:
                await disconnect_client_fully(client)
        finally:
            try:
                protect_session_files(
                    credentials.session_path,
                    runtime_root=LOCAL_RUNTIME_ROOT,
                )
            finally:
                try:
                    if database is not None:
                        await database.close()
                finally:
                    lock.release()


def _default_client_factory(
    credentials: OperatorBotCredentials,
) -> OperatorTelegramClient:
    return TelegramClient(
        str(credentials.session_path),
        credentials.api_id,
        credentials.api_hash,
    )


def _event_identity(event: Any) -> dict[str, Any]:
    return {
        "is_private": getattr(event, "is_private", False) is True,
        "sender_id": getattr(event, "sender_id", None),
        "chat_id": getattr(event, "chat_id", None),
    }


def _telethon_buttons(
    rows: tuple[tuple[OperatorButton, ...], ...],
) -> list[list[Any]] | None:
    if not rows:
        return None
    result: list[list[Any]] = []
    for row in rows:
        converted: list[Any] = []
        for button in row:
            if button.callback_data is not None:
                converted.append(Button.inline(button.label, data=button.callback_data))
            else:
                converted.append(Button.url(button.label, button.url))
        result.append(converted)
    return result


async def _safe_callback_answer(
    event: Any,
    text: str,
    *,
    alert: bool = False,
) -> None:
    try:
        await event.answer(text, alert=alert)
    except Exception:
        pass

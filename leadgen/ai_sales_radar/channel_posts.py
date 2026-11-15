"""Exact-target publishing for the public Flowforge AI Telegram channel."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import stat
import sys
from typing import Any, Awaitable, Callable, Protocol

from telethon import TelegramClient, functions, types
from telethon.errors import FloodWaitError, RPCError

from freelancer_bot.telegram_session import (
    TelegramSessionFileLock,
    TelegramSessionInUseError,
)

from .channel_branding import DEFAULT_CHANNEL_TITLE, DEFAULT_CHANNEL_USERNAME
from .config import ConfigurationError, TelegramAuthSettings, TelegramCredentials
from .telegram_auth import (
    disconnect_client_fully,
    prepare_private_session_storage,
    private_umask,
    protect_session_files,
)


FLOWFORGE_CHANNEL_ID = 4_263_406_607
FLOWFORGE_DISCUSSION_ID = 4_499_724_585
FLOWFORGE_CHANNEL_USERNAME = DEFAULT_CHANNEL_USERNAME
MAX_TEXT_LENGTH = 4_000
MAX_MEDIA_CAPTION_LENGTH = 950
MAX_IMAGE_BYTES = 10 * 1024 * 1024
ALLOWED_IMAGE_SUFFIXES = frozenset({".jpg", ".jpeg", ".png", ".webp"})


class ChannelPostError(RuntimeError):
    """Fail-closed error safe for operator output."""


class ChannelPostClient(Protocol):
    async def connect(self) -> Any: ...

    async def disconnect(self) -> Any: ...

    async def is_user_authorized(self) -> bool: ...

    async def get_me(self) -> Any: ...

    async def get_entity(self, entity: Any) -> Any: ...

    async def get_messages(self, entity: Any, *, ids: int) -> Any: ...

    async def send_message(self, entity: Any, message: str, **kwargs: Any) -> Any: ...

    async def send_file(self, entity: Any, file: Any, **kwargs: Any) -> Any: ...

    def __call__(self, request: Any) -> Awaitable[Any]: ...


ClientFactory = Callable[[TelegramCredentials], ChannelPostClient]
LockFactory = Callable[..., TelegramSessionFileLock]


@dataclass(frozen=True)
class ChannelPostTarget:
    account_id: int
    account_username: str | None
    channel_id: int
    channel_title: str
    channel_username: str
    channel_owned: bool
    linked_discussion_id: int | None
    ready: bool


@dataclass(frozen=True)
class ChannelPostResult:
    channel_id: int
    channel_username: str
    message_id: int
    message_url: str
    attached_image: bool
    verified: bool


class ChannelPostService:
    def __init__(
        self,
        credentials: TelegramCredentials,
        *,
        client_factory: ClientFactory | None = None,
        lock_factory: LockFactory = TelegramSessionFileLock,
    ) -> None:
        self._credentials = credentials
        self._client_factory = client_factory or _default_client_factory
        self._lock_factory = lock_factory

    async def check(self) -> ChannelPostTarget:
        return await self._with_client(self._resolve_target)

    async def publish(
        self,
        *,
        text: str,
        image_path: Path | None,
        confirm_channel_id: int,
    ) -> ChannelPostResult:
        selected_text = _post_text(text, has_image=image_path is not None)
        selected_image = None if image_path is None else _image_path(image_path)
        if confirm_channel_id != FLOWFORGE_CHANNEL_ID:
            raise ChannelPostError("Подтверждение ID канала не совпало")

        async def operation(client: ChannelPostClient) -> ChannelPostResult:
            target = await self._resolve_target(client)
            if not target.ready:
                raise ChannelPostError(
                    "Канал не готов: точная discussion group ещё не привязана"
                )
            entity = await client.get_entity(f"@{FLOWFORGE_CHANNEL_USERNAME}")
            if selected_image is None:
                sent = await client.send_message(
                    entity,
                    selected_text,
                    parse_mode=None,
                    link_preview=False,
                )
            else:
                sent = await client.send_file(
                    entity,
                    file=str(selected_image),
                    caption=selected_text,
                    parse_mode=None,
                    force_document=False,
                )
            message_id = _positive_id(sent, "Telegram message")
            verified = await client.get_messages(entity, ids=message_id)
            if verified is None or getattr(verified, "id", None) != message_id:
                raise ChannelPostError("Telegram не подтвердил опубликованный пост")
            return ChannelPostResult(
                channel_id=target.channel_id,
                channel_username=target.channel_username,
                message_id=message_id,
                message_url=(
                    f"https://t.me/{target.channel_username}/{message_id}"
                ),
                attached_image=selected_image is not None,
                verified=True,
            )

        return await self._with_client(operation)

    async def _resolve_target(self, client: ChannelPostClient) -> ChannelPostTarget:
        account = await client.get_me()
        account_id = _positive_id(account, "Telegram account")
        raw_account_username = getattr(account, "username", None)
        account_username = (
            raw_account_username if isinstance(raw_account_username, str) else None
        )

        entity = await client.get_entity(f"@{FLOWFORGE_CHANNEL_USERNAME}")
        if not isinstance(entity, types.Channel) or entity.broadcast is not True:
            raise ChannelPostError("Публичный адрес не является broadcast channel")
        channel_id = _positive_id(entity, "Telegram channel")
        if channel_id != FLOWFORGE_CHANNEL_ID:
            raise ChannelPostError("Публичный username указывает на другой channel ID")
        if getattr(entity, "title", None) != DEFAULT_CHANNEL_TITLE:
            raise ChannelPostError("Название канала не совпадает с Flowforge AI")
        username = getattr(entity, "username", None)
        if not isinstance(username, str) or username.casefold() != FLOWFORGE_CHANNEL_USERNAME:
            raise ChannelPostError("Username канала не совпадает с Flowforge AI")
        owned = getattr(entity, "creator", None) is True
        if not owned:
            raise ChannelPostError("Telegram-сессия не является владельцем канала")

        full = await client(
            functions.channels.GetFullChannelRequest(channel=entity)
        )
        linked = getattr(full.full_chat, "linked_chat_id", None)
        linked_id = (
            linked
            if isinstance(linked, int) and not isinstance(linked, bool) and linked > 0
            else None
        )
        return ChannelPostTarget(
            account_id=account_id,
            account_username=account_username,
            channel_id=channel_id,
            channel_title=entity.title,
            channel_username=username,
            channel_owned=owned,
            linked_discussion_id=linked_id,
            ready=linked_id == FLOWFORGE_DISCUSSION_ID,
        )

    async def _with_client(self, operation: Callable[[ChannelPostClient], Awaitable[Any]]):
        path = self._credentials.session_path
        prepare_private_session_storage(path)
        protect_session_files(path)
        lock = self._lock_factory(path, role="ai_sales_radar_channel_post")
        lock.acquire()
        client: ChannelPostClient | None = None
        try:
            with private_umask():
                client = self._client_factory(self._credentials)
            await client.connect()
            if not await client.is_user_authorized():
                raise ChannelPostError("Telegram user-session не авторизована")
            return await operation(client)
        except FloodWaitError as exc:
            raise ChannelPostError(
                f"Telegram установил FloodWait: повторите через {max(0, int(exc.seconds))} сек."
            ) from None
        except RPCError as exc:
            raise ChannelPostError(
                f"Telegram отклонил операцию ({type(exc).__name__})"
            ) from None
        finally:
            try:
                if client is not None:
                    await disconnect_client_fully(client)
            finally:
                protect_session_files(path)
                lock.release()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Безопасная публикация одного поста в Flowforge AI",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("check", help="проверить точный канал без публикации")
    publish = subcommands.add_parser("publish", help="опубликовать ровно один пост")
    publish.add_argument("--text-file", type=Path, required=True)
    publish.add_argument("--image", type=Path)
    publish.add_argument("--confirm-channel-id", type=int, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        credentials = TelegramAuthSettings.from_env().require_credentials()
        service = ChannelPostService(credentials)
        if args.command == "check":
            output = asyncio.run(service.check())
        else:
            text = _text_file(args.text_file)
            output = asyncio.run(
                service.publish(
                    text=text,
                    image_path=args.image,
                    confirm_channel_id=args.confirm_channel_id,
                )
            )
        print(json.dumps(asdict(output), ensure_ascii=False, sort_keys=True))
        return 0
    except (ConfigurationError, ChannelPostError, OSError, UnicodeError) as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 2
    except TelegramSessionInUseError:
        print("Ошибка: Telegram-сессия уже используется другим процессом.", file=sys.stderr)
        return 3
    except Exception as exc:
        print(
            "Операция Telegram не выполнена. Подробности скрыты "
            f"({type(exc).__name__}).",
            file=sys.stderr,
        )
        return 1


def _text_file(path: Path) -> str:
    source = path.expanduser().absolute()
    try:
        mode = os.lstat(source).st_mode
    except FileNotFoundError:
        raise ChannelPostError("Файл с текстом поста не найден") from None
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise ChannelPostError("Текст поста должен быть обычным файлом")
    selected = source.resolve(strict=True)
    return selected.read_text(encoding="utf-8")


def _post_text(value: str, *, has_image: bool) -> str:
    selected = value.strip()
    if not selected:
        raise ChannelPostError("Текст поста пуст")
    limit = MAX_MEDIA_CAPTION_LENGTH if has_image else MAX_TEXT_LENGTH
    if len(selected) > limit:
        raise ChannelPostError(f"Текст поста длиннее допустимых {limit} символов")
    return selected


def _image_path(path: Path) -> Path:
    source = path.expanduser().absolute()
    try:
        mode = os.lstat(source).st_mode
    except FileNotFoundError:
        raise ChannelPostError("Изображение не найдено") from None
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise ChannelPostError("Изображение должно быть обычным файлом")
    selected = source.resolve(strict=True)
    if selected.suffix.lower() not in ALLOWED_IMAGE_SUFFIXES:
        raise ChannelPostError("Поддерживаются только JPG, PNG и WEBP")
    if selected.stat().st_size > MAX_IMAGE_BYTES:
        raise ChannelPostError("Изображение превышает лимит 10 МБ")
    return selected


def _positive_id(entity: Any, label: str) -> int:
    value = getattr(entity, "id", None)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ChannelPostError(f"{label} не вернул корректный ID")
    return value


def _default_client_factory(credentials: TelegramCredentials) -> ChannelPostClient:
    return TelegramClient(
        str(credentials.session_path),
        credentials.api_id,
        credentials.api_hash,
        flood_sleep_threshold=0,
    )


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ChannelPostError",
    "ChannelPostResult",
    "ChannelPostService",
    "ChannelPostTarget",
    "FLOWFORGE_CHANNEL_ID",
    "FLOWFORGE_CHANNEL_USERNAME",
    "FLOWFORGE_DISCUSSION_ID",
    "build_parser",
    "main",
]

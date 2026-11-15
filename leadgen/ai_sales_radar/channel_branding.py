"""Safe plan/apply setup for the public Flowforge AI developer channel."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass, replace
import json
import re
import sys
from typing import Any, Awaitable, Callable, Protocol, Sequence
import unicodedata

from telethon import TelegramClient, functions, types, utils
from telethon.errors import FloodWaitError, RPCError

from freelancer_bot.telegram_session import (
    TelegramSessionFileLock,
    TelegramSessionInUseError,
)

from .config import ConfigurationError, TelegramAuthSettings, TelegramCredentials
from .telegram_auth import (
    disconnect_client_fully,
    prepare_private_session_storage,
    private_umask,
    protect_session_files,
)


DEFAULT_CHANNEL_TITLE = "Flowforge_AI | Автоматизация бизнеса"
DEFAULT_CHANNEL_USERNAME = "owner_ai"
DEFAULT_DISCUSSION_TITLE = "Flowforge AI | Обсуждения"
DEFAULT_DISCUSSION_ABOUT = (
    "Обсуждение AI-продуктов, ассистентов и автоматизаций Flowforge AI."
)
DEFAULT_CHANNEL_ABOUT = (
    "AI-ассистенты, Telegram-боты и автоматизация процессов для малого бизнеса. "
    "Реальные проекты, цифры, ошибки и решения. Автор — owner."
)
_USERNAME_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]{4,31}")


class ChannelBrandingError(RuntimeError):
    """Fail-closed error safe for operator output."""


class ChannelBrandingClient(Protocol):
    async def connect(self) -> Any: ...

    async def disconnect(self) -> Any: ...

    async def is_user_authorized(self) -> bool: ...

    async def get_me(self) -> Any: ...

    async def get_dialogs(self, limit: int | None = None) -> Sequence[Any]: ...

    def __call__(self, request: Any) -> Awaitable[Any]: ...


ClientFactory = Callable[[TelegramCredentials], ChannelBrandingClient]
LockFactory = Callable[..., TelegramSessionFileLock]


@dataclass(frozen=True)
class ChannelBrandingReport:
    mode: str
    account_id: int
    account_username: str | None
    channel_title: str
    requested_username: str
    discussion_title: str
    discussion_exists: bool
    discussion_id: int | None
    discussion_kind: str | None
    discussion_owned: bool
    discussion_history_hidden: bool | None
    channel_id: int | None
    channel_exists: bool
    channel_owned: bool
    current_channel_username: str | None
    username_available: bool | None
    linked_discussion_id: int | None
    would_create_channel: bool
    would_create_discussion: bool
    would_set_username: bool
    would_migrate_discussion: bool
    would_make_history_visible: bool
    would_link_discussion: bool
    blocked_reasons: tuple[str, ...]
    created_channel: bool = False
    set_username: bool = False
    migrated_discussion: bool = False
    made_history_visible: bool = False
    linked_discussion: bool = False
    verified: bool = False


@dataclass(frozen=True)
class _Plan:
    report: ChannelBrandingReport
    discussion: Any
    channel: Any | None


class ChannelBrandingService:
    def __init__(
        self,
        credentials: TelegramCredentials,
        *,
        channel_title: str = DEFAULT_CHANNEL_TITLE,
        channel_username: str = DEFAULT_CHANNEL_USERNAME,
        channel_about: str = DEFAULT_CHANNEL_ABOUT,
        discussion_title: str = DEFAULT_DISCUSSION_TITLE,
        discussion_about: str = DEFAULT_DISCUSSION_ABOUT,
        client_factory: ClientFactory | None = None,
        lock_factory: LockFactory = TelegramSessionFileLock,
    ) -> None:
        self._credentials = credentials
        self._channel_title = _required_text(channel_title, "название канала", 128)
        self._channel_username = _username(channel_username)
        self._channel_about = _required_text(channel_about, "описание канала", 255)
        self._discussion_title = _required_text(
            discussion_title,
            "название группы обсуждений",
            128,
        )
        self._discussion_about = _required_text(
            discussion_about,
            "описание группы обсуждений",
            255,
        )
        self._client_factory = client_factory or _default_client_factory
        self._lock_factory = lock_factory

    async def plan(self) -> ChannelBrandingReport:
        return await self._with_client(self._plan_only)

    async def apply(
        self,
        *,
        confirm_title: str,
        confirm_username: str,
        confirm_discussion_title: str,
        allow_group_migration: bool,
        allow_history_visibility_change: bool,
    ) -> ChannelBrandingReport:
        if confirm_title != self._channel_title:
            raise ChannelBrandingError("Подтверждение названия канала не совпало")
        if _username(confirm_username) != self._channel_username:
            raise ChannelBrandingError("Подтверждение публичного username не совпало")
        if confirm_discussion_title != self._discussion_title:
            raise ChannelBrandingError(
                "Подтверждение названия группы обсуждений не совпало"
            )

        async def operation(client: ChannelBrandingClient) -> ChannelBrandingReport:
            plan = await self._build_plan(client, mode="apply")
            if plan.report.blocked_reasons:
                raise ChannelBrandingError(
                    "Операция заблокирована: " + ", ".join(plan.report.blocked_reasons)
                )
            if plan.report.would_migrate_discussion and not allow_group_migration:
                raise ChannelBrandingError(
                    "Группа является basic group; требуется отдельный флаг "
                    "--allow-group-migration"
                )
            if (
                plan.report.would_make_history_visible
                and not allow_history_visibility_change
            ):
                raise ChannelBrandingError(
                    "У группы скрыта старая история; требуется отдельный флаг "
                    "--allow-history-visibility-change"
                )

            channel = plan.channel
            created = False
            username_set = False
            migrated = False
            history_made_visible = False
            linked = False
            discussion_created = False

            if channel is None:
                response = await client(
                    functions.channels.CreateChannelRequest(
                        title=self._channel_title,
                        about=self._channel_about,
                        broadcast=True,
                        megagroup=False,
                    )
                )
                channel = _single_created_channel(response)
                created = True

            channel_input = utils.get_input_channel(channel)
            current_username = _entity_username(channel)
            if current_username is None:
                available = await client(
                    functions.channels.CheckUsernameRequest(
                        channel=channel_input,
                        username=self._channel_username,
                    )
                )
                if available is not True:
                    raise ChannelBrandingError(
                        "Telegram не подтвердил доступность публичного username"
                    )
                updated = await client(
                    functions.channels.UpdateUsernameRequest(
                        channel=channel_input,
                        username=self._channel_username,
                    )
                )
                if updated is not True:
                    raise ChannelBrandingError("Telegram не подтвердил установку username")
                username_set = True
            elif current_username.casefold() != self._channel_username.casefold():
                raise ChannelBrandingError(
                    "Существующий канал имеет другой публичный username"
                )

            discussion = plan.discussion
            if discussion is None:
                response = await client(
                    functions.channels.CreateChannelRequest(
                        title=self._discussion_title,
                        about=self._discussion_about,
                        broadcast=False,
                        megagroup=True,
                    )
                )
                discussion = _single_created_group(response)
                discussion_created = True
            if isinstance(discussion, types.Chat):
                migrated_response = await client(
                    functions.messages.MigrateChatRequest(chat_id=discussion.id)
                )
                discussion = _single_migrated_group(migrated_response)
                migrated = True
            discussion_input = utils.get_input_channel(discussion)
            discussion_history_hidden = plan.report.discussion_history_hidden
            if discussion_created:
                discussion_full = await client(
                    functions.channels.GetFullChannelRequest(
                        channel=discussion_input
                    )
                )
                discussion_history_hidden = (
                    getattr(
                        discussion_full.full_chat,
                        "hidden_prehistory",
                        None,
                    )
                    is True
                )
            if discussion_history_hidden is True:
                await client(
                    functions.channels.TogglePreHistoryHiddenRequest(
                        channel=discussion_input,
                        enabled=False,
                    )
                )
                history_made_visible = True

            full_before = await client(
                functions.channels.GetFullChannelRequest(channel=channel_input)
            )
            linked_before = _linked_chat_id(full_before)
            if linked_before is None:
                updated = await client(
                    functions.channels.SetDiscussionGroupRequest(
                        broadcast=channel_input,
                        group=discussion_input,
                    )
                )
                if updated is not True:
                    raise ChannelBrandingError(
                        "Telegram не подтвердил привязку группы обсуждений"
                    )
                linked = True
            elif linked_before != discussion.id:
                raise ChannelBrandingError(
                    "К каналу уже привязана другая группа обсуждений"
                )

            full_after = await client(
                functions.channels.GetFullChannelRequest(channel=channel_input)
            )
            linked_after = _linked_chat_id(full_after)
            if linked_after != discussion.id:
                raise ChannelBrandingError("Проверка привязки обсуждений не прошла")

            return replace(
                plan.report,
                discussion_id=discussion.id,
                discussion_exists=True,
                discussion_kind="supergroup",
                channel_id=channel.id,
                channel_exists=True,
                current_channel_username=self._channel_username,
                linked_discussion_id=linked_after,
                would_create_channel=False,
                would_create_discussion=False,
                would_set_username=False,
                would_migrate_discussion=False,
                would_make_history_visible=False,
                would_link_discussion=False,
                created_channel=created,
                set_username=username_set,
                migrated_discussion=migrated,
                made_history_visible=history_made_visible,
                linked_discussion=linked,
                verified=True,
            )

        return await self._with_client(operation)

    async def _plan_only(self, client: ChannelBrandingClient) -> ChannelBrandingReport:
        return (await self._build_plan(client, mode="plan")).report

    async def _build_plan(
        self,
        client: ChannelBrandingClient,
        *,
        mode: str,
    ) -> _Plan:
        account = await client.get_me()
        account_id = _positive_id(account, "Telegram account")
        account_username = _entity_username(account)
        dialogs = await client.get_dialogs(limit=None)

        discussion_matches = _matching_entities(dialogs, self._discussion_title)
        channel_matches = _matching_entities(dialogs, self._channel_title)
        blocked: list[str] = []

        if len(discussion_matches) != 1:
            if discussion_matches:
                raise ChannelBrandingError(
                    "Найдено несколько диалогов с названием группы обсуждений"
                )
        discussion = discussion_matches[0] if discussion_matches else None
        discussion_kind = (
            None if discussion is None else _discussion_kind(discussion)
        )
        if discussion is not None and discussion_kind is None:
            blocked.append("discussion_is_not_group")
        discussion_owned = (
            discussion is None or getattr(discussion, "creator", None) is True
        )
        if discussion is not None and not discussion_owned:
            blocked.append("discussion_not_owned")

        discussion_history_hidden: bool | None = None
        if isinstance(discussion, types.Channel) and discussion.megagroup is True:
            discussion_full = await client(
                functions.channels.GetFullChannelRequest(
                    channel=utils.get_input_channel(discussion)
                )
            )
            discussion_history_hidden = (
                getattr(discussion_full.full_chat, "hidden_prehistory", None) is True
            )

        valid_channels = [
            item
            for item in channel_matches
            if isinstance(item, types.Channel) and item.broadcast is True
        ]
        if len(valid_channels) > 1:
            blocked.append("duplicate_target_channel")
        channel = valid_channels[0] if len(valid_channels) == 1 else None
        if channel_matches and channel is None:
            blocked.append("target_title_used_by_non_channel")
        channel_owned = channel is None or getattr(channel, "creator", None) is True
        if not channel_owned:
            blocked.append("channel_not_owned")

        current_username = None if channel is None else _entity_username(channel)
        if (
            current_username is not None
            and current_username.casefold() != self._channel_username.casefold()
        ):
            blocked.append("channel_username_conflict")

        username_available: bool | None
        if current_username is not None:
            username_available = current_username.casefold() == self._channel_username.casefold()
        else:
            target = types.InputChannelEmpty() if channel is None else utils.get_input_channel(channel)
            try:
                username_available = await client(
                    functions.channels.CheckUsernameRequest(
                        channel=target,
                        username=self._channel_username,
                    )
                )
            except RPCError:
                username_available = None
            if username_available is False:
                blocked.append("username_unavailable")

        linked_id = None
        if channel is not None:
            full = await client(
                functions.channels.GetFullChannelRequest(
                    channel=utils.get_input_channel(channel)
                )
            )
            linked_id = _linked_chat_id(full)
            if (
                linked_id is not None
                and (discussion is None or linked_id != discussion.id)
            ):
                blocked.append("different_discussion_already_linked")

        report = ChannelBrandingReport(
            mode=mode,
            account_id=account_id,
            account_username=account_username,
            channel_title=self._channel_title,
            requested_username=self._channel_username,
            discussion_title=self._discussion_title,
            discussion_exists=discussion is not None,
            discussion_id=None if discussion is None else discussion.id,
            discussion_kind=discussion_kind,
            discussion_owned=discussion_owned,
            discussion_history_hidden=discussion_history_hidden,
            channel_id=None if channel is None else channel.id,
            channel_exists=channel is not None,
            channel_owned=channel_owned,
            current_channel_username=current_username,
            username_available=username_available,
            linked_discussion_id=linked_id,
            would_create_channel=channel is None,
            would_create_discussion=discussion is None,
            would_set_username=current_username is None,
            would_migrate_discussion=discussion_kind == "basic_group",
            would_make_history_visible=discussion_history_hidden is True,
            would_link_discussion=(
                linked_id is None
                or discussion is None
                or linked_id != discussion.id
            ),
            blocked_reasons=tuple(blocked),
        )
        return _Plan(report=report, discussion=discussion, channel=channel)

    async def _with_client(self, operation: Callable[[ChannelBrandingClient], Awaitable[Any]]):
        path = self._credentials.session_path
        prepare_private_session_storage(path)
        protect_session_files(path)
        lock = self._lock_factory(path, role="ai_sales_radar_channel_branding")
        lock.acquire()
        client: ChannelBrandingClient | None = None
        try:
            with private_umask():
                client = self._client_factory(self._credentials)
            await client.connect()
            if not await client.is_user_authorized():
                raise ChannelBrandingError("Telegram user-session не авторизована")
            return await operation(client)
        except FloodWaitError as exc:
            raise ChannelBrandingError(
                f"Telegram установил FloodWait: повторите через {max(0, int(exc.seconds))} сек."
            ) from None
        except RPCError as exc:
            raise ChannelBrandingError(
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
        description="Безопасное создание публичного AI-канала и группы обсуждений",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("plan", help="проверить точные цели без изменений")
    apply = subcommands.add_parser("apply", help="создать канал и связать обсуждения")
    apply.add_argument("--confirm-title", required=True)
    apply.add_argument("--confirm-username", required=True)
    apply.add_argument("--confirm-discussion-title", required=True)
    apply.add_argument("--allow-group-migration", action="store_true")
    apply.add_argument("--allow-history-visibility-change", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        credentials = TelegramAuthSettings.from_env().require_credentials()
        service = ChannelBrandingService(credentials)
        if args.command == "plan":
            report = asyncio.run(service.plan())
        else:
            report = asyncio.run(
                service.apply(
                    confirm_title=args.confirm_title,
                    confirm_username=args.confirm_username,
                    confirm_discussion_title=args.confirm_discussion_title,
                    allow_group_migration=args.allow_group_migration,
                    allow_history_visibility_change=(
                        args.allow_history_visibility_change
                    ),
                )
            )
        print(json.dumps(asdict(report), ensure_ascii=False, sort_keys=True))
        return 0
    except (ConfigurationError, ChannelBrandingError) as exc:
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


def _matching_entities(dialogs: Sequence[Any], title: str) -> list[Any]:
    key = _title_key(title)
    return [
        entity
        for dialog in dialogs
        if (entity := getattr(dialog, "entity", None)) is not None
        and _title_key(getattr(entity, "title", None)) == key
    ]


def _discussion_kind(entity: Any) -> str | None:
    if isinstance(entity, types.Chat):
        return "basic_group"
    if isinstance(entity, types.Channel) and entity.megagroup is True:
        return "supergroup"
    return None


def _single_created_channel(response: Any) -> types.Channel:
    matches = [
        item
        for item in getattr(response, "chats", ())
        if isinstance(item, types.Channel) and item.broadcast is True
    ]
    if len(matches) != 1:
        raise ChannelBrandingError("Telegram не вернул созданный broadcast channel")
    return matches[0]


def _single_migrated_group(response: Any) -> types.Channel:
    matches = [
        item
        for item in getattr(response, "chats", ())
        if isinstance(item, types.Channel) and item.megagroup is True
    ]
    if len(matches) != 1:
        raise ChannelBrandingError("Telegram не вернул мигрированную supergroup")
    return matches[0]


def _single_created_group(response: Any) -> types.Channel:
    matches = [
        item
        for item in getattr(response, "chats", ())
        if isinstance(item, types.Channel) and item.megagroup is True
    ]
    if len(matches) != 1:
        raise ChannelBrandingError("Telegram не вернул созданную supergroup")
    return matches[0]


def _linked_chat_id(response: Any) -> int | None:
    value = getattr(getattr(response, "full_chat", None), "linked_chat_id", None)
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else None


def _positive_id(entity: Any, label: str) -> int:
    value = getattr(entity, "id", None)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ChannelBrandingError(f"{label} не вернул корректный ID")
    return value


def _entity_username(entity: Any) -> str | None:
    value = getattr(entity, "username", None)
    return value if isinstance(value, str) and value else None


def _title_key(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    return unicodedata.normalize("NFC", value).strip().casefold()


def _required_text(value: str, label: str, maximum: int) -> str:
    selected = unicodedata.normalize("NFC", value).strip()
    if not selected:
        raise ValueError(f"Не указано {label}")
    if len(selected) > maximum:
        raise ValueError(f"Слишком длинное {label}")
    return selected


def _username(value: str) -> str:
    selected = value.strip().removeprefix("@").lower()
    if not _USERNAME_RE.fullmatch(selected):
        raise ValueError("Некорректный публичный username")
    return selected


def _default_client_factory(credentials: TelegramCredentials) -> ChannelBrandingClient:
    return TelegramClient(
        str(credentials.session_path),
        credentials.api_id,
        credentials.api_hash,
        flood_sleep_threshold=0,
    )


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ChannelBrandingError",
    "ChannelBrandingReport",
    "ChannelBrandingService",
    "DEFAULT_CHANNEL_ABOUT",
    "DEFAULT_CHANNEL_TITLE",
    "DEFAULT_CHANNEL_USERNAME",
    "DEFAULT_DISCUSSION_ABOUT",
    "DEFAULT_DISCUSSION_TITLE",
    "build_parser",
    "main",
]

"""Add-only management of the private Telegram folder for Flowforge work."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import asdict, dataclass, replace
import json
from pathlib import Path
import sys
from typing import Any, Protocol

from telethon import TelegramClient, functions, types
from telethon.errors import FloodWaitError, RPCError

from freelancer_bot.telegram_session import (
    TelegramSessionFileLock,
    TelegramSessionInUseError,
)

from .config import (
    ConfigurationError,
    LOCAL_RUNTIME_ROOT,
    OperatorBotSettings,
    TelegramAuthSettings,
    TelegramCredentials,
)
from .telegram_auth import (
    disconnect_client_fully,
    prepare_private_session_storage,
    private_umask,
    protect_session_files,
)
from .telegram_folder import (
    MAX_FOLDER_PEERS,
    TelegramFolderError,
    dialog_peer_key,
    merge_include_peers_add_only,
    verify_dialog_filter_post_update,
)


FLOWFORGE_OWNER_ID = 0
FLOWFORGE_FOLDER_TITLE = "🤖Flowforge"
FLOWFORGE_CHANNEL_ID = 4_263_406_607
FLOWFORGE_DISCUSSION_ID = 4_499_724_585
FLOWFORGE_PROJECT_GROUP_ID = 4_324_224_634
_RESERVED_FILTER_IDS = frozenset({0, 1})


class FlowforgeFolderError(RuntimeError):
    """Fail-closed error safe for operator output."""


class FlowforgeFolderClient(Protocol):
    async def connect(self) -> Any: ...

    async def disconnect(self) -> Any: ...

    async def is_user_authorized(self) -> bool: ...

    async def get_me(self) -> Any: ...

    async def get_dialogs(self, limit: int | None = None) -> Sequence[Any]: ...

    def __call__(self, request: Any) -> Awaitable[Any]: ...


ClientFactory = Callable[[TelegramCredentials], FlowforgeFolderClient]
LockFactory = Callable[..., TelegramSessionFileLock]


@dataclass(frozen=True)
class FlowforgeFolderTarget:
    peer_id: int
    title: str
    kind: str
    result: str


@dataclass(frozen=True)
class FlowforgeFolderReport:
    mode: str
    account_id: int
    folder_title: str
    folder_id: int | None
    folder_exists: bool
    existing_peer_count: int
    resulting_peer_count: int
    planned_add_count: int
    would_create_folder: bool
    would_update: bool
    blocked_reasons: tuple[str, ...]
    targets: tuple[FlowforgeFolderTarget, ...]
    created_folder: bool = False
    updated: bool = False
    verified: bool = False


@dataclass(frozen=True)
class _ResolvedTarget:
    report: FlowforgeFolderTarget
    peer: Any | None


@dataclass(frozen=True)
class _Plan:
    report: FlowforgeFolderReport
    current: types.DialogFilter | None
    additions: tuple[Any, ...]


class FlowforgeFolderService:
    def __init__(
        self,
        credentials: TelegramCredentials,
        *,
        operator_bot_id: int,
        additional_bot_ids: Sequence[int] = (),
        client_factory: ClientFactory | None = None,
        lock_factory: LockFactory = TelegramSessionFileLock,
        runtime_root: Path = LOCAL_RUNTIME_ROOT,
    ) -> None:
        if isinstance(operator_bot_id, bool) or operator_bot_id <= 0:
            raise FlowforgeFolderError("Некорректный ID operator bot")
        selected_additional_ids = tuple(dict.fromkeys(additional_bot_ids))
        if any(
            isinstance(bot_id, bool) or not isinstance(bot_id, int) or bot_id <= 0
            for bot_id in selected_additional_ids
        ):
            raise FlowforgeFolderError("Некорректный ID дополнительного Flowforge bot")
        self._credentials = credentials
        self._operator_bot_id = operator_bot_id
        self._additional_bot_ids = tuple(
            bot_id for bot_id in selected_additional_ids if bot_id != operator_bot_id
        )
        self._client_factory = client_factory or _default_client_factory
        self._lock_factory = lock_factory
        self._runtime_root = runtime_root

    async def plan(self) -> FlowforgeFolderReport:
        return await self._with_client(self._plan_only)

    async def sync(self) -> FlowforgeFolderReport:
        async def operation(client: FlowforgeFolderClient) -> FlowforgeFolderReport:
            initial = await self._build_plan(client, mode="sync")
            _require_unblocked(initial)
            if not initial.report.would_update:
                return replace(initial.report, verified=True)

            # Narrow the no-CAS window by resolving all exact identities again.
            current = await self._build_plan(client, mode="sync")
            _require_unblocked(current)
            if not current.report.would_update:
                return replace(current.report, verified=True)

            created = current.current is None
            if created:
                folder_id = await _next_filter_id(client)
                expected = _new_filter(folder_id, current.additions)
            else:
                expected = merge_include_peers_add_only(
                    current.current,
                    current.additions,
                )

            result = await client(
                functions.messages.UpdateDialogFilterRequest(
                    id=expected.id,
                    filter=expected,
                )
            )
            if result is not True:
                raise FlowforgeFolderError(
                    "Telegram не подтвердил изменение папки Flowforge"
                )
            actual = await _read_exact_folder(client)
            if actual is None:
                raise FlowforgeFolderError("Папка Flowforge не найдена после записи")
            verify_flowforge_dialog_filter_post_update(expected, actual)
            completed = await self._build_plan(client, mode="sync")
            _require_unblocked(completed)
            if completed.report.would_update:
                raise FlowforgeFolderError(
                    "Проверка состава папки Flowforge после записи не прошла"
                )
            return replace(
                completed.report,
                created_folder=created,
                updated=True,
                verified=True,
            )

        return await self._with_client(operation)

    async def _plan_only(
        self,
        client: FlowforgeFolderClient,
    ) -> FlowforgeFolderReport:
        return (await self._build_plan(client, mode="plan")).report

    async def _build_plan(
        self,
        client: FlowforgeFolderClient,
        *,
        mode: str,
    ) -> _Plan:
        dialogs = await client.get_dialogs(limit=None)
        targets = _resolve_targets(
            dialogs,
            operator_bot_id=self._operator_bot_id,
            additional_bot_ids=self._additional_bot_ids,
        )
        current = await _read_exact_folder(client)
        existing = set() if current is None else {
            dialog_peer_key(peer)
            for peer in (*current.pinned_peers, *current.include_peers)
        }
        excluded = set() if current is None else {
            dialog_peer_key(peer) for peer in current.exclude_peers
        }

        blocked: list[str] = []
        additions: list[Any] = []
        target_reports: list[FlowforgeFolderTarget] = []
        resulting = set(existing)
        for target in targets:
            if target.peer is None:
                blocked.append(f"missing_dialog:{target.report.peer_id}")
                target_reports.append(target.report)
                continue
            key = dialog_peer_key(target.peer)
            if key in excluded:
                blocked.append(f"excluded:{target.report.peer_id}")
                target_reports.append(replace(target.report, result="excluded"))
            elif key in existing:
                target_reports.append(replace(target.report, result="already_present"))
            else:
                additions.append(target.peer)
                resulting.add(key)
                target_reports.append(replace(target.report, result="would_add"))

        if len(resulting) > MAX_FOLDER_PEERS:
            blocked.append("capacity_exceeded")
        would_update = bool(additions) and not blocked
        report = FlowforgeFolderReport(
            mode=mode,
            account_id=FLOWFORGE_OWNER_ID,
            folder_title=FLOWFORGE_FOLDER_TITLE,
            folder_id=None if current is None else current.id,
            folder_exists=current is not None,
            existing_peer_count=len(existing),
            resulting_peer_count=len(resulting),
            planned_add_count=len(additions),
            would_create_folder=current is None,
            would_update=would_update,
            blocked_reasons=tuple(blocked),
            targets=tuple(target_reports),
        )
        return _Plan(report=report, current=current, additions=tuple(additions))

    async def _with_client(
        self,
        operation: Callable[[FlowforgeFolderClient], Awaitable[Any]],
    ) -> Any:
        path = self._credentials.session_path
        prepare_private_session_storage(path, runtime_root=self._runtime_root)
        protect_session_files(path, runtime_root=self._runtime_root)
        lock = self._lock_factory(path, role="ai_sales_radar_flowforge_folder")
        lock.acquire()
        client: FlowforgeFolderClient | None = None
        try:
            with private_umask():
                client = self._client_factory(self._credentials)
            await client.connect()
            if not await client.is_user_authorized():
                raise FlowforgeFolderError("Telegram user-session не авторизована")
            account = await client.get_me()
            if (
                getattr(account, "bot", None) is True
                or getattr(account, "id", None) != FLOWFORGE_OWNER_ID
            ):
                raise FlowforgeFolderError(
                    "Telegram-сессия не принадлежит владельцу Flowforge"
                )
            return await operation(client)
        except FloodWaitError as exc:
            raise FlowforgeFolderError(
                f"Telegram установил FloodWait: повторите через {max(0, int(exc.seconds))} сек."
            ) from None
        except RPCError as exc:
            raise FlowforgeFolderError(
                f"Telegram отклонил операцию ({type(exc).__name__})"
            ) from None
        finally:
            try:
                if client is not None:
                    await disconnect_client_fully(client)
            finally:
                protect_session_files(path, runtime_root=self._runtime_root)
                lock.release()


def _resolve_targets(
    dialogs: Sequence[Any],
    *,
    operator_bot_id: int,
    additional_bot_ids: Sequence[int] = (),
) -> tuple[_ResolvedTarget, ...]:
    specs = [
        (FLOWFORGE_CHANNEL_ID, "Flowforge_AI | Автоматизация бизнеса", "channel"),
        (FLOWFORGE_DISCUSSION_ID, "Flowforge AI | Обсуждения", "group"),
        (FLOWFORGE_PROJECT_GROUP_ID, "Flowforge AI | Лаборатория", "group"),
        (operator_bot_id, "AI Sales Radar operator bot", "bot"),
    ]
    specs.extend(
        (bot_id, "Flowforge Order Bot", "bot") for bot_id in additional_bot_ids
    )
    by_id: dict[int, list[Any]] = {}
    for dialog in dialogs:
        entity = getattr(dialog, "entity", None)
        entity_id = getattr(entity, "id", None)
        if isinstance(entity_id, int) and not isinstance(entity_id, bool):
            by_id.setdefault(entity_id, []).append(dialog)

    resolved: list[_ResolvedTarget] = []
    for peer_id, expected_title, kind in specs:
        matches = by_id.get(peer_id, [])
        if len(matches) != 1:
            resolved.append(
                _ResolvedTarget(
                    report=FlowforgeFolderTarget(
                        peer_id=peer_id,
                        title=expected_title,
                        kind=kind,
                        result="missing_dialog",
                    ),
                    peer=None,
                )
            )
            continue
        dialog = matches[0]
        entity = getattr(dialog, "entity", None)
        peer = getattr(dialog, "input_entity", None)
        if not _target_identity_matches(entity, peer, peer_id=peer_id, kind=kind):
            raise FlowforgeFolderError(
                f"Диалог {peer_id} не прошёл точную проверку типа"
            )
        title = _entity_title(entity)
        if kind != "bot" and title != expected_title:
            raise FlowforgeFolderError(
                f"Название Flowforge-диалога {peer_id} не совпало"
            )
        resolved.append(
            _ResolvedTarget(
                report=FlowforgeFolderTarget(
                    peer_id=peer_id,
                    title=title,
                    kind=kind,
                    result="resolved",
                ),
                peer=peer,
            )
        )
    return tuple(resolved)


def _target_identity_matches(
    entity: Any,
    peer: Any,
    *,
    peer_id: int,
    kind: str,
) -> bool:
    if kind == "bot":
        return (
            isinstance(entity, types.User)
            and entity.bot is True
            and entity.id == peer_id
            and isinstance(peer, types.InputPeerUser)
            and peer.user_id == peer_id
        )
    if not (
        isinstance(entity, types.Channel)
        and isinstance(peer, types.InputPeerChannel)
        and entity.id == peer_id
        and peer.channel_id == peer_id
        and entity.creator is True
    ):
        return False
    return (
        entity.broadcast is True
        if kind == "channel"
        else entity.megagroup is True
    )


def _entity_title(entity: Any) -> str:
    if isinstance(entity, types.User):
        username = getattr(entity, "username", None)
        return f"@{username}" if isinstance(username, str) and username else str(entity.id)
    title = getattr(entity, "title", None)
    return title if isinstance(title, str) and title else str(entity.id)


async def _read_filters(client: FlowforgeFolderClient) -> Sequence[Any]:
    response = await client(functions.messages.GetDialogFiltersRequest())
    filters = getattr(response, "filters", None)
    if not isinstance(filters, Sequence) or isinstance(
        filters,
        (str, bytes, bytearray),
    ):
        raise FlowforgeFolderError("Telegram вернул некорректный список папок")
    return filters


async def _read_exact_folder(
    client: FlowforgeFolderClient,
) -> types.DialogFilter | None:
    matches = [
        item
        for item in await _read_filters(client)
        if isinstance(getattr(item, "title", None), types.TextWithEntities)
        and item.title.text == FLOWFORGE_FOLDER_TITLE
    ]
    if len(matches) > 1:
        raise FlowforgeFolderError("Найдено несколько папок 🤖Flowforge")
    if not matches:
        return None
    selected = matches[0]
    if type(selected) is not types.DialogFilter:
        raise FlowforgeFolderError("Папка 🤖Flowforge имеет неподдерживаемый тип")
    return selected


async def _next_filter_id(client: FlowforgeFolderClient) -> int:
    used = {
        item.id
        for item in await _read_filters(client)
        if isinstance(getattr(item, "id", None), int)
    }
    for candidate in range(2, 256):
        if candidate not in used and candidate not in _RESERVED_FILTER_IDS:
            return candidate
    raise FlowforgeFolderError("Нет свободного ID для новой Telegram-папки")


def _new_filter(folder_id: int, peers: Sequence[Any]) -> types.DialogFilter:
    return types.DialogFilter(
        id=folder_id,
        title=types.TextWithEntities(text=FLOWFORGE_FOLDER_TITLE, entities=[]),
        pinned_peers=[],
        include_peers=list(peers),
        exclude_peers=[],
        contacts=False,
        non_contacts=False,
        groups=False,
        broadcasts=False,
        bots=False,
        exclude_muted=False,
        exclude_read=False,
        exclude_archived=False,
        title_noanimate=False,
        emoticon="🤖",
    )


def verify_flowforge_dialog_filter_post_update(
    expected: types.DialogFilter,
    actual: types.DialogFilter,
) -> None:
    """Accept Telegram's include-peer reordering while verifying exact membership."""
    try:
        verify_dialog_filter_post_update(expected, actual)
        return
    except TelegramFolderError:
        pass

    if type(actual) is not types.DialogFilter or actual.id != expected.id:
        raise FlowforgeFolderError("Проверка Telegram-папки после записи не пройдена")
    if bytes(actual.title) != bytes(expected.title):
        raise FlowforgeFolderError("Название Telegram-папки изменилось при записи")
    for field in (
        "contacts",
        "non_contacts",
        "groups",
        "broadcasts",
        "bots",
        "exclude_muted",
        "exclude_read",
        "exclude_archived",
        "title_noanimate",
        "emoticon",
        "color",
    ):
        if getattr(actual, field) != getattr(expected, field):
            raise FlowforgeFolderError("Флаги Telegram-папки изменились при записи")
    if tuple(dialog_peer_key(peer) for peer in actual.pinned_peers) != tuple(
        dialog_peer_key(peer) for peer in expected.pinned_peers
    ):
        raise FlowforgeFolderError("Закрепления Telegram-папки изменились при записи")
    for field in ("include_peers", "exclude_peers"):
        actual_keys = tuple(dialog_peer_key(peer) for peer in getattr(actual, field))
        expected_keys = tuple(dialog_peer_key(peer) for peer in getattr(expected, field))
        if len(actual_keys) != len(expected_keys) or set(actual_keys) != set(expected_keys):
            raise FlowforgeFolderError("Состав Telegram-папки не подтверждён после записи")


def _require_unblocked(plan: _Plan) -> None:
    if plan.report.blocked_reasons:
        raise FlowforgeFolderError(
            "Операция заблокирована: " + ", ".join(plan.report.blocked_reasons)
        )


def _default_client_factory(credentials: TelegramCredentials) -> FlowforgeFolderClient:
    return TelegramClient(
        str(credentials.session_path),
        credentials.api_id,
        credentials.api_hash,
        flood_sleep_threshold=0,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Безопасное ведение папки Telegram 🤖Flowforge",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("plan", help="проверить состав без изменений")
    commands.add_parser("sync", help="добавить точные Flowforge-диалоги")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        credentials = TelegramAuthSettings.from_env().require_credentials()
        bot = OperatorBotSettings.from_env().require_credentials()
        from flowforge_order_bot.config import OrderBotSettings

        order_settings = OrderBotSettings.from_env()
        additional_bot_ids: tuple[int, ...] = ()
        if order_settings.configured():
            additional_bot_ids = (order_settings.require_credentials().bot_id,)
        service = FlowforgeFolderService(
            credentials,
            operator_bot_id=bot.bot_id,
            additional_bot_ids=additional_bot_ids,
        )
        report = asyncio.run(
            service.plan() if args.command == "plan" else service.sync()
        )
        print(json.dumps(asdict(report), ensure_ascii=False, sort_keys=True))
        return 0
    except (ConfigurationError, FlowforgeFolderError) as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 2
    except TelegramSessionInUseError:
        print("Ошибка: Telegram-сессия уже используется другим процессом.", file=sys.stderr)
        return 3
    except Exception as exc:
        print(
            "Операция с папкой Flowforge не выполнена. Подробности скрыты "
            f"({type(exc).__name__}).",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "FLOWFORGE_CHANNEL_ID",
    "FLOWFORGE_DISCUSSION_ID",
    "FLOWFORGE_FOLDER_TITLE",
    "FLOWFORGE_OWNER_ID",
    "FLOWFORGE_PROJECT_GROUP_ID",
    "FlowforgeFolderError",
    "FlowforgeFolderReport",
    "FlowforgeFolderService",
    "FlowforgeFolderTarget",
    "build_parser",
    "main",
    "verify_flowforge_dialog_filter_post_update",
]

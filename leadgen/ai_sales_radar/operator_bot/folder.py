"""Add-only placement of the private operator bot in the project folder."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
import re
from typing import Any, Protocol

from telethon import TelegramClient, functions, types

from freelancer_bot.telegram_session import TelegramSessionFileLock

from ai_sales_radar.config import (
    LOCAL_RUNTIME_ROOT,
    OperatorBotCredentials,
    TelegramCredentials,
)
from ai_sales_radar.telegram_auth import (
    disconnect_client_fully,
    prepare_private_session_storage,
    private_umask,
    protect_session_files,
)
from ai_sales_radar.telegram_folder import (
    MAX_FOLDER_PEERS,
    TelegramFolderError,
    dialog_peer_key,
    merge_include_peers_add_only,
    verify_dialog_filter_post_update,
)


EXACT_PROJECT_FOLDER_TITLE = "Ист_Лид_АИ"
SYNC_CONCURRENT_EDIT_WARNING = (
    "Во время sync не редактируйте папку параллельно в Telegram "
    "Desktop/mobile: Telegram API не поддерживает CAS для dialog filters."
)
_PUBLIC_BOT_USERNAME_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]{3,31}")


class OperatorBotFolderError(TelegramFolderError):
    """A fail-closed folder error safe for operator output."""


class OperatorBotFolderClient(Protocol):
    async def connect(self) -> Any: ...

    async def disconnect(self) -> Any: ...

    async def is_user_authorized(self) -> bool: ...

    async def get_me(self) -> Any: ...

    async def get_dialogs(self, limit: int | None = None) -> Sequence[Any]: ...

    def __call__(self, request: Any) -> Awaitable[Any]: ...


ClientFactory = Callable[[TelegramCredentials], OperatorBotFolderClient]
LockFactory = Callable[..., TelegramSessionFileLock]


@dataclass(frozen=True)
class OperatorBotFolderReport:
    mode: str
    folder_id: int
    folder_title: str
    bot_username: str
    capacity: int
    existing_peer_count: int
    resulting_peer_count: int
    result: str
    blocked_reasons: tuple[str, ...]
    would_update: bool
    updated: bool
    verified: bool


@dataclass(frozen=True)
class _BotFolderPlan:
    report: OperatorBotFolderReport
    addition: types.InputPeerUser | None


@dataclass(frozen=True)
class _ResolvedBotDialog:
    peer: types.InputPeerUser
    username: str


class OperatorBotFolderReconciler:
    """Plan or explicitly append one already-open bot dialog to the folder."""

    def __init__(
        self,
        user_credentials: TelegramCredentials,
        bot_credentials: OperatorBotCredentials,
        *,
        client_factory: ClientFactory | None = None,
        lock_factory: LockFactory = TelegramSessionFileLock,
        runtime_root: Path = LOCAL_RUNTIME_ROOT,
    ) -> None:
        if user_credentials.session_path == bot_credentials.session_path:
            raise OperatorBotFolderError(
                "Для папки нужна пользовательская collector-сессия, а не bot-сессия"
            )
        if user_credentials.api_id != bot_credentials.api_id:
            raise OperatorBotFolderError(
                "Telegram API ID пользовательской и bot-конфигурации различаются"
            )
        self._user_credentials = user_credentials
        self._bot_credentials = bot_credentials
        self._client_factory = client_factory or _default_client_factory
        self._lock_factory = lock_factory
        self._runtime_root = runtime_root

    async def plan(self) -> OperatorBotFolderReport:
        async def operation(
            client: OperatorBotFolderClient,
        ) -> OperatorBotFolderReport:
            target = await self._resolve_open_bot_dialog(client)
            current = await _read_exact_project_filter(client)
            return _build_plan(current, target=target, mode="plan").report

        return await self._with_locked_client(operation)

    async def sync(self) -> OperatorBotFolderReport:
        async def operation(
            client: OperatorBotFolderClient,
        ) -> OperatorBotFolderReport:
            initial = _build_plan(
                await _read_exact_project_filter(client),
                target=await self._resolve_open_bot_dialog(client),
                mode="sync",
            )
            _require_unblocked(initial)
            if initial.addition is None:
                return _completed_report(initial.report, updated=False)

            # Telegram has no compare-and-swap for dialog filters. Re-resolve
            # the immutable bot identity and re-read the entire filter directly
            # before the only write, narrowing the concurrent-edit window.
            target = await self._resolve_open_bot_dialog(client)
            current = await _read_exact_project_filter(client)
            current_plan = _build_plan(current, target=target, mode="sync")
            _require_unblocked(current_plan)
            if current_plan.addition is None:
                return _completed_report(current_plan.report, updated=False)

            expected = merge_include_peers_add_only(
                current,
                (current_plan.addition,),
            )
            result = await client(
                functions.messages.UpdateDialogFilterRequest(
                    id=current.id,
                    filter=expected,
                )
            )
            if result is not True:
                raise OperatorBotFolderError(
                    "Telegram не подтвердил добавление operator bot в папку"
                )

            actual = await _read_exact_project_filter(client)
            verify_dialog_filter_post_update(expected, actual)
            return _completed_report(current_plan.report, updated=True)

        return await self._with_locked_client(operation)

    async def _resolve_open_bot_dialog(
        self,
        client: OperatorBotFolderClient,
    ) -> _ResolvedBotDialog:
        dialogs = await client.get_dialogs(limit=None)
        matches: list[_ResolvedBotDialog] = []
        identity_conflict = False

        for dialog in dialogs:
            entity = getattr(dialog, "entity", None)
            peer = getattr(dialog, "input_entity", None)
            entity_id = getattr(entity, "id", None)
            peer_id = getattr(peer, "user_id", None)
            if (
                entity_id != self._bot_credentials.bot_id
                and peer_id != self._bot_credentials.bot_id
            ):
                continue
            if not _is_exact_bot_dialog(
                entity,
                peer,
                bot_id=self._bot_credentials.bot_id,
            ):
                identity_conflict = True
                continue
            matches.append(
                _ResolvedBotDialog(
                    peer=peer,
                    username=entity.username,
                )
            )

        if identity_conflict:
            raise OperatorBotFolderError(
                "Открытый Telegram-диалог не прошёл проверку личности operator bot"
            )
        if not matches:
            raise OperatorBotFolderError(
                "Чат operator bot не найден среди открытых диалогов; "
                "сначала отправьте боту /start"
            )
        unique = {dialog_peer_key(item.peer) for item in matches}
        if len(matches) != 1 or len(unique) != 1:
            raise OperatorBotFolderError(
                "Найдено несколько диалогов с личностью operator bot"
            )
        return matches[0]

    async def _with_locked_client(
        self,
        operation: Callable[
            [OperatorBotFolderClient], Awaitable[OperatorBotFolderReport]
        ],
    ) -> OperatorBotFolderReport:
        session_path = self._user_credentials.session_path
        prepare_private_session_storage(
            session_path,
            runtime_root=self._runtime_root,
        )
        protect_session_files(
            session_path,
            runtime_root=self._runtime_root,
        )
        lock = self._lock_factory(
            session_path,
            role="ai_sales_radar_operator_bot_folder",
        )
        try:
            lock.acquire()
        except BaseException:
            lock.release()
            raise

        client: OperatorBotFolderClient | None = None
        outcome: OperatorBotFolderReport | None = None
        operation_error: BaseException | None = None
        try:
            with private_umask():
                client = self._client_factory(self._user_credentials)
            await client.connect()
            if not await client.is_user_authorized():
                raise OperatorBotFolderError(
                    "Пользовательская Telegram-сессия не авторизована"
                )
            account = await client.get_me()
            account_id = getattr(account, "id", None)
            if (
                getattr(account, "bot", None) is True
                or isinstance(account_id, bool)
                or not isinstance(account_id, int)
                or account_id != self._bot_credentials.owner_user_id
            ):
                raise OperatorBotFolderError(
                    "Пользовательская Telegram-сессия не принадлежит владельцу бота"
                )
            outcome = await operation(client)
        except BaseException as exc:
            operation_error = exc
        finally:
            cleanup_error: BaseException | None = None
            try:
                if client is not None:
                    try:
                        await disconnect_client_fully(client)
                    except BaseException as exc:
                        cleanup_error = exc
                try:
                    protect_session_files(
                        session_path,
                        runtime_root=self._runtime_root,
                    )
                except BaseException as exc:
                    if cleanup_error is None:
                        cleanup_error = exc
            finally:
                try:
                    lock.release()
                except BaseException as exc:
                    if cleanup_error is None:
                        cleanup_error = exc

        if operation_error is not None:
            raise operation_error.with_traceback(operation_error.__traceback__)
        if cleanup_error is not None:
            raise cleanup_error.with_traceback(cleanup_error.__traceback__)
        if outcome is None:
            raise OperatorBotFolderError(
                "Операция с папкой operator bot не завершена"
            )
        return outcome


def _is_exact_bot_dialog(
    entity: Any,
    peer: Any,
    *,
    bot_id: int,
) -> bool:
    if not isinstance(entity, types.User) or not isinstance(peer, types.InputPeerUser):
        return False
    if entity.bot is not True or getattr(entity, "deleted", None) is True:
        return False
    if entity.id != bot_id or peer.user_id != bot_id:
        return False
    entity_hash = getattr(entity, "access_hash", None)
    peer_hash = getattr(peer, "access_hash", None)
    if (
        isinstance(entity_hash, bool)
        or not isinstance(entity_hash, int)
        or entity_hash == 0
        or isinstance(peer_hash, bool)
        or not isinstance(peer_hash, int)
        or peer_hash != entity_hash
    ):
        return False
    username = getattr(entity, "username", None)
    return (
        isinstance(username, str)
        and _PUBLIC_BOT_USERNAME_RE.fullmatch(username) is not None
        and username.casefold().endswith("bot")
    )


async def _read_exact_project_filter(
    client: OperatorBotFolderClient,
) -> types.DialogFilter:
    response = await client(functions.messages.GetDialogFiltersRequest())
    raw_filters = getattr(response, "filters", None)
    if not isinstance(raw_filters, Sequence) or isinstance(
        raw_filters,
        (str, bytes, bytearray),
    ):
        raise OperatorBotFolderError(
            "Telegram вернул некорректный список папок"
        )

    matches = [
        dialog_filter
        for dialog_filter in raw_filters
        if isinstance(getattr(dialog_filter, "title", None), types.TextWithEntities)
        and dialog_filter.title.text == EXACT_PROJECT_FOLDER_TITLE
    ]
    if not matches:
        raise OperatorBotFolderError(
            f'Папка Telegram не найдена: "{EXACT_PROJECT_FOLDER_TITLE}"'
        )
    if len(matches) != 1:
        raise OperatorBotFolderError(
            "Найдено несколько папок с точным названием; изменение остановлено"
        )
    selected = matches[0]
    if isinstance(selected, types.DialogFilterChatlist):
        raise OperatorBotFolderError(
            "Общая папка chatlist не поддерживает безопасное изменение"
        )
    if type(selected) is not types.DialogFilter:
        raise OperatorBotFolderError(
            "Найден неподдерживаемый тип Telegram-папки"
        )
    return selected


def _build_plan(
    current: types.DialogFilter,
    *,
    target: _ResolvedBotDialog,
    mode: str,
) -> _BotFolderPlan:
    target_key = dialog_peer_key(target.peer)
    pinned_keys = {dialog_peer_key(peer) for peer in current.pinned_peers}
    include_keys = {dialog_peer_key(peer) for peer in current.include_peers}
    exclude_keys = {dialog_peer_key(peer) for peer in current.exclude_peers}
    existing_keys = pinned_keys | include_keys

    blocked: list[str] = []
    addition: types.InputPeerUser | None = None
    if target_key in exclude_keys:
        result = "excluded_conflict"
        blocked.append("target_in_exclude")
    elif target_key in existing_keys:
        result = "already_present"
    else:
        result = "would_add"
        addition = target.peer

    resulting_count = len(existing_keys | {target_key})
    if (
        len(existing_keys) > MAX_FOLDER_PEERS
        or resulting_count > MAX_FOLDER_PEERS
    ):
        blocked.append("capacity_exceeded")

    report = OperatorBotFolderReport(
        mode=mode,
        folder_id=current.id,
        folder_title=current.title.text,
        bot_username=f"@{target.username}",
        capacity=MAX_FOLDER_PEERS,
        existing_peer_count=len(existing_keys),
        resulting_peer_count=resulting_count,
        result=result,
        blocked_reasons=tuple(blocked),
        would_update=addition is not None and not blocked,
        updated=False,
        verified=False,
    )
    return _BotFolderPlan(report=report, addition=addition)


def _require_unblocked(plan: _BotFolderPlan) -> None:
    if not plan.report.blocked_reasons:
        return
    if "target_in_exclude" in plan.report.blocked_reasons:
        raise OperatorBotFolderError(
            "Operator bot находится в exclude_peers; изменение остановлено"
        )
    raise OperatorBotFolderError(
        f"Лимит папки {MAX_FOLDER_PEERS} диалогов будет превышен"
    )


def _completed_report(
    report: OperatorBotFolderReport,
    *,
    updated: bool,
) -> OperatorBotFolderReport:
    values = asdict(report)
    values["updated"] = updated
    values["verified"] = True
    return OperatorBotFolderReport(**values)


def _default_client_factory(
    credentials: TelegramCredentials,
) -> OperatorBotFolderClient:
    return TelegramClient(
        str(credentials.session_path),
        credentials.api_id,
        credentials.api_hash,
        flood_sleep_threshold=0,
    )


__all__ = [
    "EXACT_PROJECT_FOLDER_TITLE",
    "OperatorBotFolderError",
    "OperatorBotFolderReconciler",
    "OperatorBotFolderReport",
    "SYNC_CONCURRENT_EDIT_WARNING",
]

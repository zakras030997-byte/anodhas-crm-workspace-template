"""Explicit, add-only reconciliation of one Telegram dialog folder.

``plan`` never sends a mutating Telegram request.  ``sync`` is deliberately a
separate operator action and can only append already-open dialogs selected by
the approved PostgreSQL source adapter to an existing private dialog filter.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import re
import sys
from typing import Any, Protocol
import unicodedata
from urllib.parse import urlparse

import sqlalchemy as sa
from telethon import TelegramClient, functions, types

from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.collector_accounts import CollectorAccountRecord
from freelancer_bot.persistence.schema import (
    collector_accounts,
    telegram_source_validations,
)
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from freelancer_bot.telegram_collector import (
    ApprovedTelegramSourceAdapter,
    TelegramCollectorSource,
    TelegramCollectorSourceSnapshot,
)
from freelancer_bot.telegram_session import (
    TelegramSessionFileLock,
    TelegramSessionInUseError,
)

from .config import (
    ConfigurationError,
    DatabaseSettings,
    LOCAL_RUNTIME_ROOT,
    TelegramAuthSettings,
    TelegramCredentials,
)
from .telegram_auth import (
    prepare_private_session_storage,
    private_umask,
    protect_session_files,
)


FOLDER_ALIASES = frozenset({"Ист_Лид_АИ"})
FOLDER_ALIAS_KEYS = frozenset(
    unicodedata.normalize("NFC", value).strip().casefold()
    for value in FOLDER_ALIASES
)
MAX_FOLDER_PEERS = 100
SYNC_CONCURRENT_EDIT_WARNING = (
    "Во время sync не редактируйте папку параллельно в Telegram Desktop/mobile: "
    "Telegram API не поддерживает CAS для dialog filters."
)
ALLOWED_SOURCE_STATUSES = frozenset(
    {SourceStatus.APPROVED, SourceStatus.ACTIVE, SourceStatus.DEGRADED}
)
_USERNAME_RE = re.compile(r"[A-Za-z0-9_]{1,64}")
_CANONICAL_PEER_RE = re.compile(r"peer:([1-9][0-9]*)")
_TRUSTED_VALIDATION_STATES = frozenset({"accessible", "approved"})
_PRESERVED_FIELDS = (
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
)


class TelegramFolderError(RuntimeError):
    """A fail-closed folder reconciliation error safe for operator output."""


class TelegramFolderClient(Protocol):
    async def connect(self) -> Any: ...

    async def disconnect(self) -> Any: ...

    async def is_user_authorized(self) -> bool: ...

    async def get_me(self) -> Any: ...

    async def get_dialogs(self, limit: int | None = None) -> Sequence[Any]: ...

    def __call__(self, request: Any) -> Awaitable[Any]: ...


class SourceSnapshotLoader(Protocol):
    async def list_for_session(
        self,
        client: TelegramFolderClient,
    ) -> TelegramCollectorSourceSnapshot: ...


@dataclass(frozen=True)
class TelegramSourceIdentityBinding:
    source_id: int
    state: str
    canonical_peer_id: int | None


class IdentityBindingLoader(Protocol):
    async def load(
        self,
        *,
        source_ids: Sequence[int],
        collector_account_id: int,
    ) -> Mapping[int, TelegramSourceIdentityBinding]: ...


ClientFactory = Callable[[TelegramCredentials], TelegramFolderClient]
LockFactory = Callable[..., TelegramSessionFileLock]
PeerKey = tuple[str, int]


@dataclass(frozen=True)
class TelegramFolderSourceResult:
    source_id: int
    display_name: str
    handle: str | None
    lifecycle_status: str
    result: str


@dataclass(frozen=True)
class TelegramFolderReport:
    mode: str
    folder_id: int
    folder_title: str
    capacity: int
    existing_peer_count: int
    resulting_peer_count: int
    approved_source_count: int
    resolved_source_count: int
    unique_dialog_target_count: int
    missing_dialog_count: int
    unvalidated_identity_count: int
    identity_mismatch_count: int
    already_present_count: int
    planned_add_count: int
    exclude_conflict_count: int
    blocked_reasons: tuple[str, ...]
    would_update: bool
    updated: bool
    verified: bool
    sources: tuple[TelegramFolderSourceResult, ...]


@dataclass(frozen=True)
class _ResolvedSource:
    source: TelegramCollectorSource
    peer: Any | None
    result: str


@dataclass(frozen=True)
class _DialogIdentity:
    peer: Any
    entity: Any


@dataclass(frozen=True)
class _FolderPlan:
    report: TelegramFolderReport
    additions: tuple[Any, ...]


class PostgresReadOnlyApprovedSourceLoader:
    """Load the existing account and eligible sources using SELECTs only."""

    def __init__(
        self,
        database: Database,
        *,
        repository: SourceRepository | None = None,
    ) -> None:
        self._database = database
        self._repository = repository or SourceRepository()

    async def list_for_session(
        self,
        client: TelegramFolderClient,
    ) -> TelegramCollectorSourceSnapshot:
        session = await client.get_me()
        external_account_id = _session_account_id(session)
        async with self._database.connect() as connection:
            row = (
                await connection.execute(
                    sa.select(collector_accounts).where(
                        collector_accounts.c.platform == "telegram",
                        collector_accounts.c.external_account_id
                        == external_account_id,
                    )
                )
            ).mappings().one_or_none()
            if row is None:
                raise TelegramFolderError(
                    "Telegram collector account отсутствует в локальном каталоге; "
                    "сначала выполните авторизацию/bootstrap"
                )
            account = CollectorAccountRecord(
                id=int(row["id"]),
                platform=str(row["platform"]),
                external_account_id=str(row["external_account_id"]),
                display_name=str(row["display_name"]),
                is_active=bool(row["is_active"]),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            records = await self._repository.list_for_collector(
                connection,
                collector_account_id=account.id,
                platform="telegram",
            )

        targets = tuple(
            TelegramCollectorSource(record=record, lookup=lookup)
            for record in records
            if (lookup := _approved_source_lookup(record)) is not None
        )
        return TelegramCollectorSourceSnapshot(
            collector_account=account,
            sources=targets,
        )


class PostgresTelegramIdentityBindingLoader:
    """Read stable peer bindings recorded by Telegram source validation."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def load(
        self,
        *,
        source_ids: Sequence[int],
        collector_account_id: int,
    ) -> Mapping[int, TelegramSourceIdentityBinding]:
        if (
            isinstance(collector_account_id, bool)
            or not isinstance(collector_account_id, int)
            or collector_account_id <= 0
            or any(
                isinstance(value, bool)
                or not isinstance(value, int)
                or value <= 0
                for value in source_ids
            )
        ):
            raise TelegramFolderError("Некорректная identity-привязка источника")
        selected_ids = tuple(dict.fromkeys(source_ids))
        if not selected_ids:
            return {}
        async with self._database.connect() as connection:
            rows = (
                await connection.execute(
                    sa.select(
                        telegram_source_validations.c.source_id,
                        telegram_source_validations.c.state,
                        telegram_source_validations.c.canonical_peer_identity,
                    ).where(
                        telegram_source_validations.c.collector_account_id
                        == collector_account_id,
                        telegram_source_validations.c.source_id.in_(selected_ids),
                    )
                )
            ).mappings().all()
        return {
            int(row["source_id"]): TelegramSourceIdentityBinding(
                source_id=int(row["source_id"]),
                state=str(row["state"]),
                canonical_peer_id=_canonical_peer_id(
                    row["canonical_peer_identity"]
                ),
            )
            for row in rows
        }


class TelegramFolderReconciler:
    """Plan or explicitly apply an add-only merge to a Telegram folder."""

    def __init__(
        self,
        database: Database,
        credentials: TelegramCredentials,
        *,
        source_adapter: SourceSnapshotLoader | None = None,
        plan_source_loader: SourceSnapshotLoader | None = None,
        identity_binding_loader: IdentityBindingLoader | None = None,
        client_factory: ClientFactory | None = None,
        lock_factory: LockFactory = TelegramSessionFileLock,
        runtime_root: Path = LOCAL_RUNTIME_ROOT,
    ) -> None:
        self._credentials = credentials
        self._source_adapter = source_adapter or ApprovedTelegramSourceAdapter(database)
        self._plan_source_loader = (
            plan_source_loader
            or PostgresReadOnlyApprovedSourceLoader(database)
        )
        self._identity_binding_loader = (
            identity_binding_loader
            or PostgresTelegramIdentityBindingLoader(database)
        )
        self._client_factory = client_factory or _default_client_factory
        self._lock_factory = lock_factory
        self._runtime_root = runtime_root

    async def plan(self) -> TelegramFolderReport:
        async def operation(client: TelegramFolderClient) -> TelegramFolderReport:
            sources, resolved = await self._load_targets(
                client,
                source_loader=self._plan_source_loader,
            )
            current = await _read_target_filter(client)
            return _build_plan(
                current,
                sources=sources,
                resolved=resolved,
                mode="plan",
            ).report

        return await self._with_locked_client(operation)

    async def sync(self) -> TelegramFolderReport:
        async def operation(client: TelegramFolderClient) -> TelegramFolderReport:
            sources, resolved = await self._load_targets(client)

            # First read produces the same safety plan an operator can inspect.
            initial = _build_plan(
                await _read_target_filter(client),
                sources=sources,
                resolved=resolved,
                mode="sync",
            )
            _require_unblocked(initial)
            if not initial.additions:
                return _completed_report(initial.report, updated=False)

            # Re-read both authorization/catalog eligibility and the folder
            # immediately before the only mutating request, so a source
            # paused, rejected, or disabled between the two snapshots is
            # removed from this attempt.
            #
            # This is intentionally not presented as an atomic DB/Telegram
            # guarantee: holding PostgreSQL row locks across an unbounded
            # external RPC would create an unsafe long transaction, and the
            # current lifecycle/access/validation writers do not share one
            # lease protocol.  A later architecture can add a durable intent
            # plus writer-wide fencing; this command only narrows the window.
            sources, resolved = await self._load_targets(
                client,
                eligibility_last=True,
            )
            current = await _read_target_filter(client)
            current_plan = _build_plan(
                current,
                sources=sources,
                resolved=resolved,
                mode="sync",
            )
            _require_unblocked(current_plan)
            if not current_plan.additions:
                return _completed_report(current_plan.report, updated=False)

            # Telegram exposes no filter version/hash and UpdateDialogFilter is
            # a full replacement.  The immediate reread limits stale writes,
            # but cannot provide CAS against Desktop/mobile edits made after
            # this point; the CLI emits an explicit operator warning.
            expected = _merge_include_peers(current, current_plan.additions)
            result = await client(
                functions.messages.UpdateDialogFilterRequest(
                    id=current.id,
                    filter=expected,
                )
            )
            if result is not True:
                raise TelegramFolderError(
                    "Telegram не подтвердил обновление папки"
                )

            actual = await _read_target_filter(client)
            _verify_post_update(expected, actual)
            return _completed_report(current_plan.report, updated=True)

        return await self._with_locked_client(operation)

    async def _load_targets(
        self,
        client: TelegramFolderClient,
        *,
        eligibility_last: bool = False,
        source_loader: SourceSnapshotLoader | None = None,
    ) -> tuple[TelegramCollectorSourceSnapshot, tuple[_ResolvedSource, ...]]:
        dialogs: Sequence[Any] | None = None
        if eligibility_last:
            # Dialog pagination may be the slowest read.  Finish it before the
            # final approved-source snapshot so a pause/reject is checked as
            # close to UpdateDialogFilter as the API permits.
            dialogs = await client.get_dialogs(limit=None)

        loader = source_loader or self._source_adapter
        snapshot = await loader.list_for_session(client)
        if not snapshot.collector_account.is_active:
            raise TelegramFolderError(
                "Telegram collector account отключён в локальном каталоге"
            )
        for source in snapshot.sources:
            if source.record.lifecycle_status not in ALLOWED_SOURCE_STATUSES:
                raise TelegramFolderError(
                    "Адаптер одобренных источников вернул недопустимый статус"
                )

        bindings = await self._identity_binding_loader.load(
            source_ids=tuple(source.record.id for source in snapshot.sources),
            collector_account_id=snapshot.collector_account.id,
        )

        if dialogs is None:
            dialogs = await client.get_dialogs(limit=None)
        resolved = _resolve_sources_from_dialogs(
            snapshot.sources,
            dialogs,
            bindings=bindings,
        )
        return snapshot, resolved

    async def _with_locked_client(
        self,
        operation: Callable[[TelegramFolderClient], Awaitable[TelegramFolderReport]],
    ) -> TelegramFolderReport:
        session_path = self._credentials.session_path
        prepare_private_session_storage(
            session_path,
            runtime_root=self._runtime_root,
        )
        # Reject unsafe existing session/companion files before opening the
        # sidecar lock or constructing Telethon's SQLite-backed client.
        protect_session_files(
            session_path,
            runtime_root=self._runtime_root,
        )
        lock = self._lock_factory(session_path, role="ai_sales_radar_folder")
        try:
            lock.acquire()
        except BaseException:
            # TelegramSessionFileLock records ownership before writing its
            # metadata.  If that metadata write fails, release any partial
            # acquisition before propagating the error.
            lock.release()
            raise
        client: TelegramFolderClient | None = None
        outcome: TelegramFolderReport | None = None
        operation_error: BaseException | None = None
        try:
            with private_umask():
                client = self._client_factory(self._credentials)
            await client.connect()
            if not await client.is_user_authorized():
                raise TelegramFolderError(
                    "Telegram-сессия не авторизована; сначала выполните login"
                )
            outcome = await operation(client)
        except BaseException as exc:
            operation_error = exc
        finally:
            cleanup_error: BaseException | None = None
            try:
                if client is not None:
                    try:
                        await _disconnect_fully(client)
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
            raise TelegramFolderError("Операция с Telegram-папкой не завершена")
        return outcome


async def _disconnect_fully(client: TelegramFolderClient) -> None:
    """Drain Telethon's shielded SQLite shutdown before releasing the lock."""
    waiter = asyncio.ensure_future(client.disconnect())
    cancellation: asyncio.CancelledError | None = None
    while not waiter.done():
        try:
            await asyncio.shield(waiter)
        except asyncio.CancelledError as exc:
            # shield keeps the saved waiter alive.  Delay propagation until
            # its session writes have finished under TelegramSessionFileLock.
            if cancellation is None:
                cancellation = exc
        except BaseException:
            # A non-cancellation failure completes the waiter; retrieve and
            # propagate that same failure below.
            break

    waiter_error: BaseException | None = None
    try:
        waiter.result()
    except BaseException as exc:
        waiter_error = exc

    if cancellation is not None:
        raise cancellation
    if waiter_error is not None:
        raise waiter_error.with_traceback(waiter_error.__traceback__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Безопасная сверка Telegram-папки AI Sales Radar",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser(
        "plan",
        help="только показать add-only план без изменения Telegram",
    )
    sync = subcommands.add_parser(
        "sync",
        help=(
            "явно добавить источники; не редактируйте папку параллельно "
            "в Telegram Desktop/mobile"
        ),
        description=SYNC_CONCURRENT_EDIT_WARNING,
    )
    sync.set_defaults(concurrent_edit_warning=SYNC_CONCURRENT_EDIT_WARNING)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "sync":
            _emit_warning(SYNC_CONCURRENT_EDIT_WARNING)
        credentials = TelegramAuthSettings.from_env().require_credentials()
        database_url = DatabaseSettings.from_env().require_url()
        report = asyncio.run(
            _execute(
                args.command,
                credentials=credentials,
                database_url=database_url,
            )
        )
        print(json.dumps(asdict(report), ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    except (ConfigurationError, TelegramFolderError) as exc:
        _emit_error(str(exc))
        return 2
    except TelegramSessionInUseError:
        _emit_error("Telegram-сессия уже используется другим процессом")
        return 3
    except Exception as exc:
        _emit_error(
            "Операция с Telegram-папкой не выполнена; подробности скрыты "
            f"для защиты секретов ({type(exc).__name__})"
        )
        return 1


async def _execute(
    command: str,
    *,
    credentials: TelegramCredentials,
    database_url: str,
) -> TelegramFolderReport:
    database = Database(database_url, pool_size=2, max_overflow=0)
    try:
        reconciler = TelegramFolderReconciler(database, credentials)
        if command == "plan":
            return await reconciler.plan()
        if command == "sync":
            return await reconciler.sync()
        raise TelegramFolderError("Неизвестная команда Telegram-папки")
    finally:
        await database.close()


async def _read_target_filter(client: TelegramFolderClient) -> types.DialogFilter:
    response = await client(functions.messages.GetDialogFiltersRequest())
    raw_filters = getattr(response, "filters", None)
    if not isinstance(raw_filters, Sequence) or isinstance(
        raw_filters,
        (str, bytes, bytearray),
    ):
        raise TelegramFolderError("Telegram вернул некорректный список папок")

    matches: list[Any] = []
    for dialog_filter in raw_filters:
        title = getattr(dialog_filter, "title", None)
        if (
            isinstance(title, types.TextWithEntities)
            and _folder_title_key(title.text) in FOLDER_ALIAS_KEYS
        ):
            matches.append(dialog_filter)

    if not matches:
        aliases = " / ".join(sorted(FOLDER_ALIASES))
        raise TelegramFolderError(f"Папка Telegram не найдена: {aliases}")
    if len(matches) != 1:
        raise TelegramFolderError(
            "Найдено несколько папок с допустимыми названиями; синхронизация "
            "остановлена"
        )
    selected = matches[0]
    if isinstance(selected, types.DialogFilterChatlist):
        raise TelegramFolderError(
            "Общая папка chatlist не поддерживает безопасную синхронизацию"
        )
    if not isinstance(selected, types.DialogFilter):
        raise TelegramFolderError("Найден неподдерживаемый тип Telegram-папки")
    return selected


def _folder_title_key(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    return unicodedata.normalize("NFC", value).strip().casefold()


def _session_account_id(session: Any) -> str:
    value = getattr(session, "id", None)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise TelegramFolderError(
            "Telegram-сессия не вернула корректный ID аккаунта"
        )
    return str(value)


def _approved_source_lookup(record: Any) -> str | None:
    for value in (getattr(record, "handle", None), getattr(record, "canonical_url", None)):
        if isinstance(value, str) and _lookup_username(value) is not None:
            return value
    return None


def _resolve_sources_from_dialogs(
    sources: Sequence[TelegramCollectorSource],
    dialogs: Sequence[Any],
    *,
    bindings: Mapping[int, TelegramSourceIdentityBinding],
) -> tuple[_ResolvedSource, ...]:
    by_username: dict[str, _DialogIdentity] = {}
    for dialog in dialogs:
        peer = getattr(dialog, "input_entity", None)
        if not _is_supported_peer(peer):
            continue
        identity = _DialogIdentity(
            peer=peer,
            entity=getattr(dialog, "entity", None),
        )
        for username in _dialog_usernames(identity.entity):
            existing = by_username.get(username)
            if (
                existing is not None
                and _peer_key(existing.peer) != _peer_key(peer)
            ):
                raise TelegramFolderError(
                    "Один username соответствует нескольким открытым диалогам"
                )
            by_username[username] = identity

    resolved: list[_ResolvedSource] = []
    for source in sources:
        binding = bindings.get(source.record.id)
        if (
            binding is None
            or isinstance(binding.source_id, bool)
            or not isinstance(binding.source_id, int)
            or binding.source_id != source.record.id
            or binding.state not in _TRUSTED_VALIDATION_STATES
            or isinstance(binding.canonical_peer_id, bool)
            or not isinstance(binding.canonical_peer_id, int)
            or binding.canonical_peer_id <= 0
        ):
            resolved.append(
                _ResolvedSource(
                    source=source,
                    peer=None,
                    result="unvalidated_identity",
                )
            )
            continue
        username = _lookup_username(source.lookup)
        identity = None if username is None else by_username.get(username)
        if identity is None:
            resolved.append(
                _ResolvedSource(source=source, peer=None, result="missing_dialog")
            )
            continue
        if (
            not _is_public_channel_dialog(identity)
            or _peer_numeric_id(identity.peer) != binding.canonical_peer_id
        ):
            resolved.append(
                _ResolvedSource(
                    source=source,
                    peer=None,
                    result="identity_mismatch",
                )
            )
            continue
        resolved.append(
            _ResolvedSource(
                source=source,
                peer=identity.peer,
                result="resolved",
            )
        )
    return tuple(resolved)


def _is_public_channel_dialog(identity: _DialogIdentity) -> bool:
    peer = identity.peer
    entity = identity.entity
    return (
        isinstance(peer, types.InputPeerChannel)
        and isinstance(entity, types.Channel)
        and not isinstance(entity.id, bool)
        and isinstance(entity.id, int)
        and entity.id > 0
        and entity.id == peer.channel_id
        and (entity.broadcast is True or entity.megagroup is True)
        and bool(_dialog_usernames(entity))
    )


def _dialog_usernames(entity: Any) -> tuple[str, ...]:
    values: list[str] = []
    primary = _normalize_username(getattr(entity, "username", None))
    if primary is not None:
        values.append(primary)
    for item in getattr(entity, "usernames", None) or ():
        # In Telethon TL flags, an absent optional bool decodes as None.  Only
        # an explicit True marks a secondary username as currently active.
        if getattr(item, "active", None) is not True:
            continue
        username = _normalize_username(getattr(item, "username", None))
        if username is not None and username not in values:
            values.append(username)
    return tuple(values)


def _lookup_username(value: str) -> str | None:
    selected = value.strip()
    if selected.startswith("@"):
        return _normalize_username(selected[1:])
    parsed = urlparse(selected)
    if parsed.scheme.lower() != "https" or parsed.netloc.lower() not in {
        "t.me",
        "telegram.me",
    }:
        return None
    path = parsed.path.strip("/").removeprefix("@")
    if "/" in path or parsed.query or parsed.fragment:
        return None
    return _normalize_username(path)


def _normalize_username(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    selected = value.strip()
    if not _USERNAME_RE.fullmatch(selected):
        return None
    return selected.lower()


def _build_plan(
    current: types.DialogFilter,
    *,
    sources: TelegramCollectorSourceSnapshot,
    resolved: Sequence[_ResolvedSource],
    mode: str,
) -> _FolderPlan:
    pinned_keys = {_peer_key(peer) for peer in current.pinned_peers}
    include_keys = {_peer_key(peer) for peer in current.include_peers}
    exclude_keys = {_peer_key(peer) for peer in current.exclude_peers}
    existing_keys = pinned_keys | include_keys

    additions: list[Any] = []
    addition_keys: set[PeerKey] = set()
    outcomes: list[TelegramFolderSourceResult] = []
    resolved_count = 0
    missing_count = 0
    unvalidated_count = 0
    mismatch_count = 0
    already_present_count = 0
    conflict_count = 0

    for item in resolved:
        source = item.source
        if item.result == "unvalidated_identity":
            result = item.result
            unvalidated_count += 1
        elif item.result == "identity_mismatch":
            result = item.result
            mismatch_count += 1
        elif item.result == "missing_dialog":
            result = item.result
            missing_count += 1
        else:
            if item.peer is None:
                raise TelegramFolderError(
                    "Некорректный результат identity-сверки источника"
                )
            resolved_count += 1
            key = _peer_key(item.peer)
            if key in exclude_keys:
                result = "excluded_conflict"
                conflict_count += 1
            elif key in existing_keys:
                result = "already_present"
                already_present_count += 1
            else:
                result = "would_add"
                if key not in addition_keys:
                    additions.append(item.peer)
                    addition_keys.add(key)
        outcomes.append(_source_result(source, result))

    resulting_keys = existing_keys | addition_keys
    blocked: list[str] = []
    if conflict_count:
        blocked.append("target_in_exclude")
    if len(existing_keys) > MAX_FOLDER_PEERS or len(resulting_keys) > MAX_FOLDER_PEERS:
        blocked.append("capacity_exceeded")

    report = TelegramFolderReport(
        mode=mode,
        folder_id=current.id,
        folder_title=current.title.text,
        capacity=MAX_FOLDER_PEERS,
        existing_peer_count=len(existing_keys),
        resulting_peer_count=len(resulting_keys),
        approved_source_count=len(sources.sources),
        resolved_source_count=resolved_count,
        unique_dialog_target_count=len(
            {_peer_key(item.peer) for item in resolved if item.peer is not None}
        ),
        missing_dialog_count=missing_count,
        unvalidated_identity_count=unvalidated_count,
        identity_mismatch_count=mismatch_count,
        already_present_count=already_present_count,
        planned_add_count=len(additions),
        exclude_conflict_count=conflict_count,
        blocked_reasons=tuple(blocked),
        would_update=bool(additions) and not blocked,
        updated=False,
        verified=False,
        sources=tuple(outcomes),
    )
    return _FolderPlan(report=report, additions=tuple(additions))


def _source_result(
    source: TelegramCollectorSource,
    result: str,
) -> TelegramFolderSourceResult:
    username = _lookup_username(source.lookup)
    return TelegramFolderSourceResult(
        source_id=source.record.id,
        display_name=source.record.display_name,
        handle=None if username is None else f"@{username}",
        lifecycle_status=source.record.lifecycle_status.value,
        result=result,
    )


def _require_unblocked(plan: _FolderPlan) -> None:
    if not plan.report.blocked_reasons:
        return
    if "target_in_exclude" in plan.report.blocked_reasons:
        raise TelegramFolderError(
            "Одобренный источник находится в exclude_peers; изменение остановлено"
        )
    raise TelegramFolderError(
        f"Лимит папки {MAX_FOLDER_PEERS} диалогов будет превышен"
    )


def _merge_include_peers(
    current: types.DialogFilter,
    additions: Sequence[Any],
) -> types.DialogFilter:
    kwargs = {field: getattr(current, field) for field in _PRESERVED_FIELDS}
    return types.DialogFilter(
        id=current.id,
        title=current.title,
        pinned_peers=list(current.pinned_peers),
        include_peers=[*current.include_peers, *additions],
        exclude_peers=list(current.exclude_peers),
        **kwargs,
    )


def merge_include_peers_add_only(
    current: types.DialogFilter,
    additions: Sequence[Any],
) -> types.DialogFilter:
    """Public add-only merge primitive shared by narrowly scoped folder tools."""

    return _merge_include_peers(current, additions)


def _verify_post_update(
    expected: types.DialogFilter,
    actual: types.DialogFilter,
) -> None:
    if actual.id != expected.id or type(actual) is not types.DialogFilter:
        raise TelegramFolderError("Проверка Telegram-папки после записи не пройдена")
    if _tl_signature(actual.title) != _tl_signature(expected.title):
        raise TelegramFolderError("Название Telegram-папки изменилось при записи")
    for field in _PRESERVED_FIELDS:
        if getattr(actual, field) != getattr(expected, field):
            raise TelegramFolderError(
                "Флаги Telegram-папки изменились при записи"
            )
    for field in ("pinned_peers", "include_peers", "exclude_peers"):
        if tuple(_peer_key(peer) for peer in getattr(actual, field)) != tuple(
            _peer_key(peer) for peer in getattr(expected, field)
        ):
            raise TelegramFolderError(
                "Состав Telegram-папки не подтверждён после записи"
            )


def verify_dialog_filter_post_update(
    expected: types.DialogFilter,
    actual: types.DialogFilter,
) -> None:
    """Verify that Telegram persisted the complete expected filter snapshot."""

    _verify_post_update(expected, actual)


def _completed_report(
    report: TelegramFolderReport,
    *,
    updated: bool,
) -> TelegramFolderReport:
    values = asdict(report)
    values["updated"] = updated
    values["verified"] = True
    values["sources"] = report.sources
    return TelegramFolderReport(**values)


def _is_supported_peer(peer: Any) -> bool:
    return isinstance(
        peer,
        (types.InputPeerUser, types.InputPeerChat, types.InputPeerChannel),
    )


def _peer_key(peer: Any) -> PeerKey:
    if isinstance(peer, types.InputPeerUser):
        return ("user", _positive_peer_id(peer.user_id))
    if isinstance(peer, types.InputPeerChat):
        return ("chat", _positive_peer_id(peer.chat_id))
    if isinstance(peer, types.InputPeerChannel):
        return ("channel", _positive_peer_id(peer.channel_id))
    raise TelegramFolderError("Telegram-папка содержит неподдерживаемый peer")


def dialog_peer_key(peer: Any) -> PeerKey:
    """Return a strict peer type/id key without exposing an access hash."""

    return _peer_key(peer)


def _peer_numeric_id(peer: Any) -> int:
    return _peer_key(peer)[1]


def _canonical_peer_id(value: Any) -> int | None:
    if not isinstance(value, str):
        return None
    match = _CANONICAL_PEER_RE.fullmatch(value.strip())
    if match is None:
        return None
    return int(match.group(1))


def _positive_peer_id(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise TelegramFolderError("Telegram вернул некорректный peer")
    return value


def _tl_signature(value: Any) -> Any:
    try:
        return bytes(value)
    except (AttributeError, TypeError, ValueError):
        return value


def _default_client_factory(credentials: TelegramCredentials) -> TelegramFolderClient:
    return TelegramClient(
        str(credentials.session_path),
        credentials.api_id,
        credentials.api_hash,
        flood_sleep_threshold=0,
    )


def _emit_error(message: str) -> None:
    print(
        json.dumps(
            {"ok": False, "error": message},
            ensure_ascii=False,
            sort_keys=True,
        ),
        file=sys.stderr,
    )


def _emit_warning(message: str) -> None:
    print(
        json.dumps(
            {"warning": message},
            ensure_ascii=False,
            sort_keys=True,
        ),
        file=sys.stderr,
    )


if __name__ == "__main__":
    raise SystemExit(main())

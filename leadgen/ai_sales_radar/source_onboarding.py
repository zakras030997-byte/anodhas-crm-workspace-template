"""Explicit, bounded onboarding of the fixed Telegram lead-source set.

The command deliberately separates a read-only ``plan`` from ``apply``.  It
never reads message history, sends messages, audits sources, edits Telegram
folders, or touches unselected sources.  Telegram membership is external and
cannot be rolled back atomically with PostgreSQL; a failed apply is safe to
rerun and never auto-leaves a channel joined by an earlier partial attempt.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys
from typing import Any, Protocol

import sqlalchemy as sa
from telethon import TelegramClient, functions, types
from telethon.errors import (
    FloodWaitError,
    InviteRequestSentError,
    UserAlreadyParticipantError,
)

from freelancer_bot.config import (
    ConfigurationError as CoreConfigurationError,
    RuntimeConfig,
    RuntimeMode,
)
from freelancer_bot.persistence.collector_accounts import CollectorAccountRecord
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.discovery_campaigns import DiscoveryCampaignRepository
from freelancer_bot.persistence.schema import (
    collector_accounts,
    source_reference_aliases,
    sources,
    telegram_source_validations,
)
from freelancer_bot.persistence.source_repository import (
    SourceRecord,
    SourceRepository,
    SourceStatus,
)
from freelancer_bot.telegram_references import (
    InvalidTelegramReference,
    normalize_telegram_reference,
)
from freelancer_bot.telegram_request_governor import (
    TelegramRequestCategory,
    TelegramRequestGovernor,
)
from freelancer_bot.telegram_session import (
    TelegramSessionFileLock,
    TelegramSessionInUseError,
)

from .config import (
    ConfigurationError,
    DatabaseSettings,
    DEFAULT_ENV_FILE,
    LOCAL_RUNTIME_ROOT,
    TelegramAuthSettings,
    TelegramCredentials,
)
from .telegram_auth import (
    disconnect_client_fully,
    prepare_private_session_storage,
    private_umask,
    protect_session_files,
)


MAX_SELECTED_SOURCES = 16
PLAN_REQUEST_DELAY_SECONDS = 1.0
CANARY_CONFIRMATION = "1,3,4,5,6,7,9,10,12,13,14,15,16,17,18,19"
CANARY_SELECTION = (
    1,
    3,
    4,
    5,
    6,
    7,
    9,
    10,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19,
)
CANARY_SORTED_IDS = tuple(sorted(CANARY_SELECTION))
CANARY_EXPECTED_IDENTITIES: Mapping[int, tuple[str, str]] = {
    1: ("freelansim_ru", "username:freelansim_ru"),
    3: ("digitaltender", "username:digitaltender"),
    4: ("freelancetaverna", "username:freelancetaverna"),
    5: ("frilans", "username:frilans"),
    6: ("job_developer", "username:job_developer"),
    7: ("search_techspec", "username:search_techspec"),
    9: ("freevacanciesit", "username:freevacanciesit"),
    10: ("freelance_dev_work", "username:freelance_dev_work"),
    12: ("pixeltechspec", "username:pixeltechspec"),
    13: ("webfrl", "username:webfrl"),
    14: ("workayte", "username:workayte"),
    15: ("freelancehuntprojects", "username:freelancehuntprojects"),
    16: ("freefounders", "username:freefounders"),
    17: ("job_for_bots", "username:job_for_bots"),
    18: ("zerocode_jobs", "username:zerocode_jobs"),
    19: ("tgram_jobs", "username:tgram_jobs"),
}
ALLOWED_LIFECYCLE_STATUSES = frozenset(
    {
        SourceStatus.CANDIDATE,
        SourceStatus.APPROVED,
        SourceStatus.ACTIVE,
        SourceStatus.DEGRADED,
    }
)
_ACTOR_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}")
_USERNAME_RE = re.compile(r"[A-Za-z0-9_]{5,32}")


class SourceOnboardingError(RuntimeError):
    """A fail-closed onboarding error safe to show to the operator."""


class TelegramOnboardingClient(Protocol):
    async def connect(self) -> Any: ...

    async def disconnect(self) -> Any: ...

    async def is_user_authorized(self) -> bool: ...

    async def get_me(self) -> Any: ...

    async def get_entity(self, entity: Any) -> Any: ...

    async def get_dialogs(self, limit: int | None = None) -> Sequence[Any]: ...

    def __call__(self, request: Any) -> Awaitable[Any]: ...


class RequestGovernor(Protocol):
    async def run(
        self,
        category: str,
        operation: Callable[[], Awaitable[Any]],
        **kwargs: Any,
    ) -> Any: ...


@dataclass(frozen=True)
class OnboardingSnapshot:
    collector_account: CollectorAccountRecord
    sources: tuple[SourceRecord, ...]


@dataclass(frozen=True)
class VerifiedTelegramBinding:
    source_id: int
    configured_username: str
    canonical_peer_id: int


@dataclass(frozen=True)
class FinalizeOnboardingResult:
    approved_source_ids: tuple[int, ...]
    unchanged_source_ids: tuple[int, ...]


class OnboardingStore(Protocol):
    async def load_selected(
        self,
        *,
        external_account_id: str,
        source_ids: Sequence[int],
    ) -> OnboardingSnapshot: ...

    async def finalize(
        self,
        *,
        snapshot: OnboardingSnapshot,
        bindings: Sequence[VerifiedTelegramBinding],
        actor: str,
        reason: str,
    ) -> FinalizeOnboardingResult: ...


@dataclass(frozen=True)
class SourceOnboardingResult:
    source_id: int
    title: str
    handle: str
    lifecycle_before: str
    membership: str
    result: str


@dataclass(frozen=True)
class SourceOnboardingReport:
    mode: str
    selected_source_count: int
    resolved_source_count: int
    already_member_count: int
    join_required_count: int
    joined_count: int
    validated_count: int
    approved_count: int
    unchanged_count: int
    sources: tuple[SourceOnboardingResult, ...]


@dataclass(frozen=True)
class _ResolvedTarget:
    source: SourceRecord
    entity: types.Channel
    binding: VerifiedTelegramBinding
    already_member: bool


class PostgresSourceOnboardingStore:
    """PostgreSQL boundary for scoped reads and one atomic final commit."""

    def __init__(
        self,
        database: Database,
        *,
        source_repository: SourceRepository | None = None,
        library_repository: DiscoveryCampaignRepository | None = None,
    ) -> None:
        self._database = database
        self._sources = source_repository or SourceRepository()
        self._library = library_repository or DiscoveryCampaignRepository()

    async def load_selected(
        self,
        *,
        external_account_id: str,
        source_ids: Sequence[int],
    ) -> OnboardingSnapshot:
        selected_ids = _selected_canary_ids(source_ids)
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
                raise SourceOnboardingError(
                    "Активный Telegram collector account не найден в PostgreSQL"
                )
            account = _collector_account(row)
            if not account.is_active:
                raise SourceOnboardingError(
                    "Telegram collector account отключён в PostgreSQL"
                )
            loaded_records: list[SourceRecord] = []
            for source_id in selected_ids:
                loaded_records.append(await self._sources.get(connection, source_id))
            records = tuple(loaded_records)

        for record in records:
            _validate_source_record(record)
        return OnboardingSnapshot(collector_account=account, sources=records)

    async def finalize(
        self,
        *,
        snapshot: OnboardingSnapshot,
        bindings: Sequence[VerifiedTelegramBinding],
        actor: str,
        reason: str,
    ) -> FinalizeOnboardingResult:
        selected_actor = _safe_actor(actor)
        selected_reason = _safe_reason(reason)
        expected_ids = _selected_canary_ids(
            tuple(source.id for source in snapshot.sources)
        )
        binding_by_id = _binding_map(bindings, expected_ids=expected_ids)
        approved: list[int] = []
        unchanged: list[int] = []

        async with self._database.transaction() as connection:
            account_row = (
                await connection.execute(
                    sa.select(collector_accounts).where(
                        collector_accounts.c.id == snapshot.collector_account.id,
                        collector_accounts.c.platform == "telegram",
                        collector_accounts.c.external_account_id
                        == snapshot.collector_account.external_account_id,
                    ).with_for_update()
                )
            ).mappings().one_or_none()
            if account_row is None or not bool(account_row["is_active"]):
                raise SourceOnboardingError(
                    "Telegram collector account изменился до фиксации onboarding"
                )

            locked_ids = tuple(
                int(value)
                for value in (
                    await connection.execute(
                        sa.select(sources.c.id)
                        .where(sources.c.id.in_(expected_ids))
                        .order_by(sources.c.id)
                        .with_for_update()
                    )
                ).scalars()
            )
            if locked_ids != tuple(sorted(expected_ids)):
                raise SourceOnboardingError(
                    "Набор источников изменился до фиксации onboarding"
                )
            current_values: list[SourceRecord] = []
            for source_id in sorted(expected_ids):
                current_values.append(
                    await self._sources.get(connection, source_id)
                )
            current_records = tuple(current_values)
            for record in current_records:
                _validate_source_record(record)
                binding = binding_by_id[record.id]
                if (
                    not isinstance(binding.configured_username, str)
                    or binding.configured_username.casefold()
                    != _configured_username(record)
                ):
                    raise SourceOnboardingError(
                        f"Telegram username binding источника {record.id} "
                        "не совпал с PostgreSQL"
                    )
                previous = next(
                    source for source in snapshot.sources if source.id == record.id
                )
                if _source_identity_signature(record) != _source_identity_signature(previous):
                    raise SourceOnboardingError(
                        f"Источник {record.id} изменился после Telegram preflight"
                    )

            validation_rows = (
                await connection.execute(
                    sa.select(
                        telegram_source_validations.c.source_id,
                        telegram_source_validations.c.canonical_peer_identity,
                    ).where(
                        telegram_source_validations.c.source_id.in_(expected_ids),
                        telegram_source_validations.c.canonical_peer_identity.is_not(
                            None
                        ),
                    )
                )
            ).mappings().all()
            for row in validation_rows:
                canonical = row["canonical_peer_identity"]
                expected = _canonical_peer_identity(
                    binding_by_id[int(row["source_id"])].canonical_peer_id
                )
                if canonical is not None and str(canonical) != expected:
                    raise SourceOnboardingError(
                        f"Сохранённая Telegram identity источника {row['source_id']} "
                        "не совпала со свежей проверкой"
                    )

            alias_identity_rows = (
                await connection.execute(
                    sa.select(
                        source_reference_aliases.c.source_id,
                        source_reference_aliases.c.canonical_peer_identity,
                    ).where(
                        source_reference_aliases.c.source_id.in_(expected_ids),
                        source_reference_aliases.c.canonical_peer_identity.is_not(
                            None
                        ),
                    )
                )
            ).mappings().all()
            for row in alias_identity_rows:
                expected = _canonical_peer_identity(
                    binding_by_id[int(row["source_id"])].canonical_peer_id
                )
                if str(row["canonical_peer_identity"]) != expected:
                    raise SourceOnboardingError(
                        f"Сохранённая Telegram alias identity источника "
                        f"{row['source_id']} не совпала со свежей проверкой"
                    )

            # Validate every peer and alias collision before the first write.
            alias_references: dict[int, str] = {}
            for source_id in sorted(expected_ids):
                binding = binding_by_id[source_id]
                canonical = _canonical_peer_identity(binding.canonical_peer_id)
                owner = await self._library.source_for_canonical_peer(
                    connection,
                    platform="telegram",
                    canonical_peer_identity=canonical,
                )
                if owner is not None and int(owner) != source_id:
                    raise SourceOnboardingError(
                        f"Telegram identity источника {source_id} уже принадлежит "
                        "другому источнику"
                    )
                reference = _normalized_username_reference(
                    binding.configured_username
                )
                alias_owner = await self._library.source_for_alias(
                    connection,
                    platform="telegram",
                    normalized_reference=reference,
                )
                if alias_owner is not None and int(alias_owner) != source_id:
                    raise SourceOnboardingError(
                        f"Telegram username источника {source_id} уже принадлежит "
                        "другому источнику"
                    )
                alias_references[source_id] = reference

            checked_at = datetime.now(timezone.utc)
            for source_id in sorted(expected_ids):
                binding = binding_by_id[source_id]
                canonical = _canonical_peer_identity(binding.canonical_peer_id)
                await self._library.record_alias(
                    connection,
                    source_id=source_id,
                    platform="telegram",
                    normalized_reference=alias_references[source_id],
                    reference_kind="username",
                    canonical_peer_identity=canonical,
                    seen_at=checked_at,
                )
                await self._library.upsert_validation(
                    connection,
                    source_id=source_id,
                    collector_account_id=snapshot.collector_account.id,
                    state="accessible",
                    access_mode="joined",
                    canonical_peer_identity=canonical,
                    failure_code=None,
                    checked_at=checked_at,
                    checked_by=selected_actor,
                )

            # Candidate approvals are one transaction and deterministic.  No
            # source_collector_access rows are needed for public sources.
            for record in current_records:
                if record.lifecycle_status is SourceStatus.CANDIDATE:
                    await self._sources.override(
                        connection,
                        record.id,
                        SourceStatus.APPROVED,
                        operator_id=selected_actor,
                        reason=selected_reason,
                    )
                    approved.append(record.id)
                else:
                    unchanged.append(record.id)

        return FinalizeOnboardingResult(
            approved_source_ids=tuple(approved),
            unchanged_source_ids=tuple(unchanged),
        )


class TelegramSourceOnboarding:
    """Freshly resolve, join, verify, validate, then atomically approve."""

    def __init__(
        self,
        database: Database,
        credentials: TelegramCredentials,
        *,
        store: OnboardingStore | None = None,
        client_factory: Callable[[TelegramCredentials], TelegramOnboardingClient]
        | None = None,
        governor_factory: Callable[[int], RequestGovernor] | None = None,
        lock_factory: Callable[..., TelegramSessionFileLock] = TelegramSessionFileLock,
        runtime_root: Path = LOCAL_RUNTIME_ROOT,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        plan_request_delay_seconds: float = PLAN_REQUEST_DELAY_SECONDS,
    ) -> None:
        if plan_request_delay_seconds < 0:
            raise ValueError("plan_request_delay_seconds must not be negative")
        self._credentials = credentials
        self._store = store or PostgresSourceOnboardingStore(database)
        self._client_factory = client_factory or _default_client_factory
        self._governor_factory = governor_factory
        self._lock_factory = lock_factory
        self._runtime_root = runtime_root
        self._sleep = sleep or asyncio.sleep
        self._plan_request_delay_seconds = plan_request_delay_seconds

    async def plan(self, *, source_ids: Sequence[int]) -> SourceOnboardingReport:
        selected_ids = _selected_canary_ids(source_ids)

        async def operation(client: TelegramOnboardingClient) -> SourceOnboardingReport:
            snapshot, resolved = await self._preflight(
                client,
                source_ids=selected_ids,
                apply=False,
            )
            return _report(
                mode="plan",
                snapshot=snapshot,
                targets=resolved,
                joined_ids=frozenset(),
                finalize=None,
            )

        return await self._with_locked_client(operation)

    async def apply(
        self,
        *,
        source_ids: Sequence[int],
        actor: str,
        reason: str,
        confirm_exact: str,
    ) -> SourceOnboardingReport:
        selected_ids = _selected_canary_ids(source_ids)
        selected_actor = _safe_actor(actor)
        selected_reason = _safe_reason(reason)
        if confirm_exact != CANARY_CONFIRMATION:
            raise SourceOnboardingError(
                "Для apply требуется точное подтверждение --confirm-exact "
                f"{CANARY_CONFIRMATION}"
            )

        async def operation(client: TelegramOnboardingClient) -> SourceOnboardingReport:
            snapshot, targets = await self._preflight(
                client,
                source_ids=selected_ids,
                apply=True,
            )
            governor = self._required_governor(snapshot.collector_account.id)
            joined_ids: set[int] = set()
            for target in targets:
                if target.already_member:
                    continue
                join_started = False

                async def send_join(
                    selected: _ResolvedTarget = target,
                ) -> Any:
                    nonlocal join_started
                    join_started = True
                    return await client(
                        functions.channels.JoinChannelRequest(selected.entity)
                    )

                try:
                    await governor.run(
                        TelegramRequestCategory.MEMBERSHIP,
                        send_join,
                    )
                except UserAlreadyParticipantError:
                    # Another Telegram client may have joined after our dialog
                    # snapshot.  The mandatory post-join dialog verification
                    # below is still authoritative for this apply attempt.
                    pass
                except InviteRequestSentError:
                    raise SourceOnboardingError(
                        f"Источник {target.source.id} создал запрос на вступление, "
                        "но membership ещё не подтверждён"
                    ) from None
                except FloodWaitError:
                    # The governor has persisted the authoritative wait.  Do
                    # not issue a reconciliation request during FloodWait.
                    raise
                except (ConnectionError, TimeoutError, OSError):
                    if not join_started:
                        # Reservation refusal (paused/FloodWait-active/etc.)
                        # happened before JoinChannelRequest.  Do not bypass
                        # the governor with a reconciliation RPC.
                        raise
                    # A transport/RPC error may arrive after Telegram applied
                    # the join.  Refresh only this observable membership state
                    # before failing; never infer success from the exception.
                    refreshed = await _read_apply_dialogs(client, governor)
                    if not _dialogs_contain_binding(refreshed, target.binding):
                        raise
                joined_ids.add(target.source.id)

            refreshed_dialogs = await _read_apply_dialogs(client, governor)
            for target in targets:
                if not _dialogs_contain_binding(refreshed_dialogs, target.binding):
                    raise SourceOnboardingError(
                        f"Источник {target.source.id} не прошёл post-join проверку"
                    )

            finalize = await self._store.finalize(
                snapshot=snapshot,
                bindings=tuple(target.binding for target in targets),
                actor=selected_actor,
                reason=selected_reason,
            )
            return _report(
                mode="apply",
                snapshot=snapshot,
                targets=targets,
                joined_ids=frozenset(joined_ids),
                finalize=finalize,
            )

        return await self._with_locked_client(operation)

    async def _preflight(
        self,
        client: TelegramOnboardingClient,
        *,
        source_ids: Sequence[int],
        apply: bool,
    ) -> tuple[OnboardingSnapshot, tuple[_ResolvedTarget, ...]]:
        me = await client.get_me()
        external_account_id = _session_account_id(me)
        snapshot = await self._store.load_selected(
            external_account_id=external_account_id,
            source_ids=source_ids,
        )
        _validate_snapshot_account(
            snapshot.collector_account,
            external_account_id=external_account_id,
        )
        if tuple(source.id for source in snapshot.sources) != CANARY_SORTED_IDS:
            raise SourceOnboardingError(
                "PostgreSQL вернул неожиданный набор canary-источников"
            )
        for source in snapshot.sources:
            _validate_source_record(source)

        governor = (
            self._required_governor(snapshot.collector_account.id) if apply else None
        )
        resolved_pairs: list[tuple[SourceRecord, types.Channel, VerifiedTelegramBinding]] = []
        for index, source in enumerate(snapshot.sources):
            if not apply and index > 0 and self._plan_request_delay_seconds > 0:
                await self._sleep(self._plan_request_delay_seconds)
            lookup = f"@{_configured_username(source)}"
            try:
                if governor is None:
                    entity = await client.get_entity(lookup)
                else:
                    entity = await governor.run(
                        TelegramRequestCategory.ENTITY_ACCESS,
                        lambda lookup=lookup: client.get_entity(lookup),
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                raise SourceOnboardingError(
                    f"Источник {source.id} не прошёл Telegram preflight "
                    f"({type(exc).__name__})"
                ) from None
            binding = _validate_resolved_entity(source, entity)
            resolved_pairs.append((source, entity, binding))

        peer_ids = [binding.canonical_peer_id for _, _, binding in resolved_pairs]
        if len(set(peer_ids)) != len(peer_ids):
            raise SourceOnboardingError(
                "Несколько выбранных источников разрешились в один Telegram peer"
            )

        # Membership is inspected only after every selected handle has passed
        # fresh entity preflight: no join can happen on partial preflight.
        if governor is None:
            dialogs = await client.get_dialogs(limit=None)
        else:
            dialogs = await _read_apply_dialogs(client, governor)
        targets = tuple(
            _ResolvedTarget(
                source=source,
                entity=entity,
                binding=binding,
                already_member=_dialogs_contain_binding(dialogs, binding),
            )
            for source, entity, binding in resolved_pairs
        )
        return snapshot, targets

    def _required_governor(self, collector_account_id: int) -> RequestGovernor:
        if self._governor_factory is None:
            raise SourceOnboardingError(
                "Apply требует настроенный Telegram request governor"
            )
        return self._governor_factory(collector_account_id)

    async def _with_locked_client(
        self,
        operation: Callable[[TelegramOnboardingClient], Awaitable[SourceOnboardingReport]],
    ) -> SourceOnboardingReport:
        session_path = self._credentials.session_path
        prepare_private_session_storage(session_path, runtime_root=self._runtime_root)
        protect_session_files(session_path, runtime_root=self._runtime_root)
        lock = self._lock_factory(session_path, role="ai_sales_radar_source_onboarding")
        try:
            lock.acquire()
        except BaseException:
            lock.release()
            raise

        client: TelegramOnboardingClient | None = None
        outcome: SourceOnboardingReport | None = None
        operation_error: BaseException | None = None
        try:
            with private_umask():
                client = self._client_factory(self._credentials)
            await client.connect()
            if not await client.is_user_authorized():
                raise SourceOnboardingError(
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
                        await disconnect_client_fully(client)
                    except BaseException as exc:
                        cleanup_error = exc
                try:
                    protect_session_files(session_path, runtime_root=self._runtime_root)
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
            raise SourceOnboardingError("Telegram onboarding не завершён")
        return outcome


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Безопасный onboarding фиксированного набора Telegram-источников",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    plan = subcommands.add_parser(
        "plan",
        help="свежая проверка фиксированного набора без join и PostgreSQL-записей",
    )
    _add_source_ids(plan)

    apply = subcommands.add_parser(
        "apply",
        help="явно join, verify, validate и атомарно approve фиксированный набор",
    )
    _add_source_ids(apply)
    apply.add_argument("--actor", required=True)
    apply.add_argument("--reason", required=True)
    apply.add_argument(
        "--confirm-exact",
        required=True,
        help=f"должно быть буквально: {CANARY_CONFIRMATION}",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        credentials = TelegramAuthSettings.from_env().require_credentials()
        database_url = DatabaseSettings.from_env().require_url()
        runtime_config = RuntimeConfig.from_env(
            mode=RuntimeMode.CHECK_SOURCES,
            env_file=DEFAULT_ENV_FILE,
        )
        report = asyncio.run(
            _execute(
                args,
                credentials=credentials,
                database_url=database_url,
                runtime_config=runtime_config,
            )
        )
        print(json.dumps(asdict(report), ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    except (
        ConfigurationError,
        CoreConfigurationError,
        SourceOnboardingError,
    ) as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 2
    except TelegramSessionInUseError:
        print("Ошибка: Telegram-сессия уже используется другим процессом", file=sys.stderr)
        return 3
    except Exception as exc:
        print(
            "Telegram onboarding не выполнен; подробности скрыты для защиты "
            f"секретов ({type(exc).__name__})",
            file=sys.stderr,
        )
        return 1


async def _execute(
    args: argparse.Namespace,
    *,
    credentials: TelegramCredentials,
    database_url: str,
    runtime_config: RuntimeConfig,
) -> SourceOnboardingReport:
    database = Database(database_url, pool_size=2, max_overflow=0)
    try:
        service = TelegramSourceOnboarding(
            database,
            credentials,
            governor_factory=lambda collector_account_id: TelegramRequestGovernor(
                database,
                collector_account_id,
                runtime_config,
            ),
        )
        if args.command == "plan":
            return await service.plan(source_ids=args.source_id)
        if args.command == "apply":
            return await service.apply(
                source_ids=args.source_id,
                actor=args.actor,
                reason=args.reason,
                confirm_exact=args.confirm_exact,
            )
        raise SourceOnboardingError("Неизвестная команда onboarding")
    finally:
        await database.close()


def _add_source_ids(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--source-id",
        action="append",
        type=_positive_source_id,
        required=True,
        help=f"повторите ровно для IDs {CANARY_CONFIRMATION}",
    )


def _selected_canary_ids(source_ids: Sequence[int]) -> tuple[int, ...]:
    if isinstance(source_ids, (str, bytes, bytearray)):
        raise SourceOnboardingError("source_ids должен быть списком ID")
    values = tuple(source_ids)
    if not 1 <= len(values) <= MAX_SELECTED_SOURCES:
        raise SourceOnboardingError(
            f"Требуется от 1 до {MAX_SELECTED_SOURCES} уникальных source ID"
        )
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value <= 0
        for value in values
    ):
        raise SourceOnboardingError("source ID должен быть положительным целым числом")
    if len(set(values)) != len(values):
        raise SourceOnboardingError("source ID не должны повторяться")
    if set(values) != set(CANARY_SELECTION):
        raise SourceOnboardingError(
            f"Этот canary ограничен источниками {CANARY_CONFIRMATION}"
        )
    return tuple(sorted(values))


def _positive_source_id(value: str) -> int:
    try:
        selected = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("source ID должен быть целым числом") from None
    if selected <= 0:
        raise argparse.ArgumentTypeError("source ID должен быть положительным")
    return selected


def _validate_source_record(source: SourceRecord) -> None:
    expected = CANARY_EXPECTED_IDENTITIES.get(source.id)
    if expected is None:
        raise SourceOnboardingError(f"Источник {source.id} не входит в canary")
    expected_username, expected_external_id = expected
    if source.platform != "telegram" or source.access_type != "public":
        raise SourceOnboardingError(
            f"Источник {source.id} должен быть публичным Telegram-источником"
        )
    if source.lifecycle_status not in ALLOWED_LIFECYCLE_STATUSES:
        raise SourceOnboardingError(
            f"Источник {source.id} имеет недопустимый lifecycle status"
        )
    if source.external_id.casefold() != expected_external_id.casefold():
        raise SourceOnboardingError(
            f"Источник {source.id} не совпал с ожидаемой external identity"
        )
    if _configured_username(source) != expected_username.casefold():
        raise SourceOnboardingError(
            f"Источник {source.id} не совпал с ожидаемым username"
        )


def _configured_username(source: SourceRecord) -> str:
    handle = source.handle
    if not isinstance(handle, str):
        raise SourceOnboardingError(f"Источник {source.id} не имеет username")
    username = handle.strip().removeprefix("@").casefold()
    if not _USERNAME_RE.fullmatch(username):
        raise SourceOnboardingError(
            f"Источник {source.id} имеет некорректный username"
        )
    return username


def _validate_resolved_entity(
    source: SourceRecord,
    entity: Any,
) -> VerifiedTelegramBinding:
    if not isinstance(entity, types.Channel):
        raise SourceOnboardingError(
            f"Источник {source.id} разрешился не в Telegram channel"
        )
    if entity.broadcast is not True and entity.megagroup is not True:
        raise SourceOnboardingError(
            f"Источник {source.id} не является channel/megagroup"
        )
    if any(getattr(entity, flag, False) is True for flag in ("scam", "fake", "restricted")):
        raise SourceOnboardingError(
            f"Источник {source.id} помечен Telegram как небезопасный/ограниченный"
        )
    peer_id = getattr(entity, "id", None)
    if isinstance(peer_id, bool) or not isinstance(peer_id, int) or peer_id <= 0:
        raise SourceOnboardingError(
            f"Источник {source.id} не имеет стабильной Telegram identity"
        )
    username = _configured_username(source)
    if username not in _active_entity_usernames(entity):
        raise SourceOnboardingError(
            f"Источник {source.id} не совпал с активным Telegram username"
        )
    return VerifiedTelegramBinding(
        source_id=source.id,
        configured_username=username,
        canonical_peer_id=peer_id,
    )


def _active_entity_usernames(entity: types.Channel) -> frozenset[str]:
    values: set[str] = set()
    primary = getattr(entity, "username", None)
    if isinstance(primary, str) and _USERNAME_RE.fullmatch(primary.strip()):
        values.add(primary.strip().casefold())
    secondary = getattr(entity, "usernames", None)
    if isinstance(secondary, Sequence) and not isinstance(
        secondary, (str, bytes, bytearray)
    ):
        for value in secondary:
            username = getattr(value, "username", None)
            if (
                getattr(value, "active", None) is True
                and isinstance(username, str)
                and _USERNAME_RE.fullmatch(username.strip())
            ):
                values.add(username.strip().casefold())
    return frozenset(values)


def _dialogs_contain_binding(
    dialogs: Sequence[Any],
    binding: VerifiedTelegramBinding,
) -> bool:
    for dialog in dialogs:
        entity = getattr(dialog, "entity", None)
        input_entity = getattr(dialog, "input_entity", None)
        if not isinstance(entity, types.Channel):
            continue
        if not isinstance(input_entity, types.InputPeerChannel):
            continue
        peer_id = getattr(entity, "id", None)
        if (
            peer_id != binding.canonical_peer_id
            or input_entity.channel_id != binding.canonical_peer_id
        ):
            continue
        if (
            (entity.broadcast is True or entity.megagroup is True)
            and binding.configured_username in _active_entity_usernames(entity)
            and getattr(entity, "left", None) is not True
            and not any(
                getattr(entity, flag, False) is True
                for flag in ("scam", "fake", "restricted")
            )
        ):
            return True
    return False


async def _read_apply_dialogs(
    client: TelegramOnboardingClient,
    governor: RequestGovernor,
) -> Sequence[Any]:
    return await governor.run(
        TelegramRequestCategory.ENTITY_ACCESS,
        lambda: client.get_dialogs(limit=None),
    )


def _binding_map(
    bindings: Sequence[VerifiedTelegramBinding],
    *,
    expected_ids: Sequence[int],
) -> Mapping[int, VerifiedTelegramBinding]:
    values: dict[int, VerifiedTelegramBinding] = {}
    for binding in bindings:
        if binding.source_id in values:
            raise SourceOnboardingError("Повторная Telegram identity в onboarding")
        if (
            isinstance(binding.canonical_peer_id, bool)
            or not isinstance(binding.canonical_peer_id, int)
            or binding.canonical_peer_id <= 0
        ):
            raise SourceOnboardingError("Некорректная Telegram identity в onboarding")
        values[binding.source_id] = binding
    if set(values) != set(expected_ids):
        raise SourceOnboardingError("Не все источники имеют проверенную Telegram identity")
    if len({value.canonical_peer_id for value in values.values()}) != len(values):
        raise SourceOnboardingError("Telegram identity источников не уникальны")
    return values


def _source_identity_signature(source: SourceRecord) -> tuple[Any, ...]:
    return (
        source.platform,
        source.external_id.casefold(),
        source.access_type,
        source.lifecycle_status,
        _configured_username(source),
    )


def _normalized_username_reference(username: str) -> str:
    try:
        return normalize_telegram_reference(
            f"https://t.me/{username}"
        ).source_key
    except InvalidTelegramReference:
        raise SourceOnboardingError("Не удалось нормализовать Telegram username") from None


def _canonical_peer_identity(peer_id: int) -> str:
    if isinstance(peer_id, bool) or not isinstance(peer_id, int) or peer_id <= 0:
        raise SourceOnboardingError("Некорректная Telegram identity")
    return f"peer:{peer_id}"


def _session_account_id(session: Any) -> str:
    value = getattr(session, "id", None)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise SourceOnboardingError(
            "Telegram-сессия не вернула корректный ID аккаунта"
        )
    return str(value)


def _validate_snapshot_account(
    account: CollectorAccountRecord,
    *,
    external_account_id: str,
) -> None:
    account_id = getattr(account, "id", None)
    if (
        isinstance(account_id, bool)
        or not isinstance(account_id, int)
        or account_id <= 0
        or getattr(account, "platform", None) != "telegram"
        or getattr(account, "external_account_id", None) != external_account_id
        or not bool(getattr(account, "is_active", False))
    ):
        raise SourceOnboardingError(
            "Telegram collector account неактивен или не совпал с сессией"
        )


def _collector_account(row: Mapping[str, Any]) -> CollectorAccountRecord:
    return CollectorAccountRecord(
        id=int(row["id"]),
        platform=str(row["platform"]),
        external_account_id=str(row["external_account_id"]),
        display_name=str(row["display_name"]),
        is_active=bool(row["is_active"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _safe_actor(value: str) -> str:
    if not isinstance(value, str) or not _ACTOR_RE.fullmatch(value):
        raise SourceOnboardingError(
            "actor должен соответствовать [A-Za-z0-9_.:@-] и быть не длиннее 128"
        )
    return value


def _safe_reason(value: str) -> str:
    if not isinstance(value, str):
        raise SourceOnboardingError("reason должен быть текстом")
    selected = value.strip()
    if not selected or len(selected) > 500 or any(ord(char) < 32 for char in selected):
        raise SourceOnboardingError(
            "reason должен быть непустым безопасным текстом до 500 символов"
        )
    return selected


def _report(
    *,
    mode: str,
    snapshot: OnboardingSnapshot,
    targets: Sequence[_ResolvedTarget],
    joined_ids: frozenset[int],
    finalize: FinalizeOnboardingResult | None,
) -> SourceOnboardingReport:
    already_member = sum(target.already_member for target in targets)
    approved_ids = (
        frozenset() if finalize is None else frozenset(finalize.approved_source_ids)
    )
    unchanged_ids = (
        frozenset() if finalize is None else frozenset(finalize.unchanged_source_ids)
    )
    results: list[SourceOnboardingResult] = []
    for target in targets:
        membership = (
            "present"
            if target.already_member
            else ("joined" if mode == "apply" else "missing")
        )
        if mode == "plan":
            result = "already_member" if target.already_member else "join_required"
        elif target.source.id in approved_ids:
            result = (
                "approved_existing_member"
                if target.already_member
                else "approved_joined"
            )
        elif target.source.id in unchanged_ids:
            result = (
                "unchanged_existing_member"
                if target.already_member
                else "unchanged_joined"
            )
        else:
            raise SourceOnboardingError("Некорректный итог onboarding")
        results.append(
            SourceOnboardingResult(
                source_id=target.source.id,
                title=target.source.display_name,
                handle=f"@{target.binding.configured_username}",
                lifecycle_before=target.source.lifecycle_status.value,
                membership=membership,
                result=result,
            )
        )
    return SourceOnboardingReport(
        mode=mode,
        selected_source_count=len(snapshot.sources),
        resolved_source_count=len(targets),
        already_member_count=already_member,
        join_required_count=len(targets) - already_member,
        joined_count=len(joined_ids),
        validated_count=0 if finalize is None else len(targets),
        approved_count=len(approved_ids),
        unchanged_count=len(unchanged_ids),
        sources=tuple(results),
    )


def _default_client_factory(
    credentials: TelegramCredentials,
) -> TelegramOnboardingClient:
    return TelegramClient(
        str(credentials.session_path),
        credentials.api_id,
        credentials.api_hash,
        flood_sleep_threshold=0,
    )


__all__ = [
    "CANARY_CONFIRMATION",
    "CANARY_SELECTION",
    "FinalizeOnboardingResult",
    "OnboardingSnapshot",
    "PostgresSourceOnboardingStore",
    "SourceOnboardingError",
    "SourceOnboardingReport",
    "TelegramSourceOnboarding",
    "VerifiedTelegramBinding",
    "build_parser",
    "main",
]

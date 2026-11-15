"""Read-only Telegram availability probe for candidate catalog sources.

The probe deliberately does less than the collector: it resolves public
candidate identities one by one and emits safe metadata.  It never joins a
chat, reads messages, records access, or changes source lifecycle state.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
import json
import math
from pathlib import Path
import re
import sys
from typing import Any, Protocol
from urllib.parse import urlparse

from telethon import TelegramClient
from telethon.errors import (
    ChannelInvalidError,
    ChannelPrivateError,
    ChannelPublicGroupNaError,
    ChatIdInvalidError,
    FloodWaitError,
    InviteHashEmptyError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    PeerIdInvalidError,
    UserBannedInChannelError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)

from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.source_repository import (
    SourceLineageRecord,
    SourceRecord,
    SourceRepository,
    SourceStatus,
)
from freelancer_bot.sources import HANDLE_RE
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
    disconnect_client_fully,
    prepare_private_session_storage,
    private_umask,
    protect_session_files,
)


MAX_SOURCE_LIMIT = 1_000
DEFAULT_SOURCE_LIMIT = 100
DEFAULT_REQUEST_DELAY_SECONDS = 1.0
MAX_REQUEST_DELAY_SECONDS = 10.0
SEED_PROVIDER = "ai_sales_radar_seed"

SOURCE_SPECIFIC_TELEGRAM_ERRORS = (
    ChannelInvalidError,
    ChannelPrivateError,
    ChannelPublicGroupNaError,
    ChatIdInvalidError,
    InviteHashEmptyError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    PeerIdInvalidError,
    UserBannedInChannelError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
    ValueError,
)


class TelegramSourceProbeError(RuntimeError):
    """A source-probe failure whose message is safe for JSON output."""


class SourceProbeClient(Protocol):
    async def connect(self) -> Any: ...

    async def disconnect(self) -> Any: ...

    async def is_user_authorized(self) -> bool: ...

    async def get_me(self) -> Any: ...

    async def get_entity(self, entity: Any) -> Any: ...


class CandidateRepository(Protocol):
    async def list_sources(
        self,
        connection: Any,
        *,
        status: SourceStatus | str | None = None,
        platform: str | None = None,
        limit: int = DEFAULT_SOURCE_LIMIT,
    ) -> tuple[SourceRecord, ...]: ...

    async def list_lineage(
        self,
        connection: Any,
        source_id: int,
    ) -> Sequence[SourceLineageRecord]: ...


ClientFactory = Callable[[TelegramCredentials], SourceProbeClient]
LockFactory = Callable[..., TelegramSessionFileLock]
Sleep = Callable[[float], Awaitable[None]]


@dataclass(frozen=True)
class SourceProbeOptions:
    source_limit: int = DEFAULT_SOURCE_LIMIT
    request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS

    def __post_init__(self) -> None:
        if isinstance(self.source_limit, bool) or not (
            1 <= self.source_limit <= MAX_SOURCE_LIMIT
        ):
            raise ValueError(
                f"source_limit must be between 1 and {MAX_SOURCE_LIMIT}"
            )
        if (
            isinstance(self.request_delay_seconds, bool)
            or not math.isfinite(self.request_delay_seconds)
            or not 0 <= self.request_delay_seconds <= MAX_REQUEST_DELAY_SECONDS
        ):
            raise ValueError(
                "request_delay_seconds must be between 0 and "
                f"{MAX_REQUEST_DELAY_SECONDS}"
            )


@dataclass(frozen=True)
class CandidateSourceProbeResult:
    source_id: int
    configured_title: str
    configured_handle: str | None
    configured_url: str | None
    seed_enabled: bool | None
    tags: tuple[str, ...]
    reason: str | None
    available: bool
    entity_kind: str | None = None
    entity_title: str | None = None
    entity_username: str | None = None
    participants_count: int | None = None
    error_type: str | None = None


@dataclass(frozen=True)
class SourceProbeReport:
    candidate_count: int
    available_count: int
    unavailable_count: int
    results: tuple[CandidateSourceProbeResult, ...]


@dataclass(frozen=True)
class _SeedMetadata:
    enabled: bool | None = None
    tags: tuple[str, ...] = ()
    reason: str | None = None


@dataclass(frozen=True)
class _CandidateTarget:
    source: SourceRecord
    seed: _SeedMetadata


class TelegramCandidateSourceProbe:
    """Resolve candidate sources without mutating catalog or Telegram state."""

    def __init__(
        self,
        database: Database,
        credentials: TelegramCredentials,
        *,
        repository: CandidateRepository | None = None,
        client_factory: ClientFactory | None = None,
        lock_factory: LockFactory = TelegramSessionFileLock,
        runtime_root: Path = LOCAL_RUNTIME_ROOT,
        sleep: Sleep | None = None,
    ) -> None:
        self._database = database
        self._credentials = credentials
        self._repository = repository or SourceRepository()
        self._client_factory = client_factory or _default_client_factory
        self._lock_factory = lock_factory
        self._runtime_root = runtime_root
        self._sleep = sleep or asyncio.sleep
        self._active = False

    async def run(
        self,
        *,
        options: SourceProbeOptions | None = None,
    ) -> SourceProbeReport:
        selected = options or SourceProbeOptions()
        targets = await self._load_candidates(limit=selected.source_limit)
        results: list[CandidateSourceProbeResult] = []
        request_was_sent = False

        async with self._connected_client() as client:
            for target in targets:
                lookup = _candidate_lookup(target.source)
                if lookup is None:
                    results.append(
                        _unavailable_result(
                            target,
                            error_type="LookupUnavailable",
                        )
                    )
                    continue

                if request_was_sent and selected.request_delay_seconds > 0:
                    await self._sleep(selected.request_delay_seconds)
                request_was_sent = True
                try:
                    entity = await client.get_entity(lookup)
                except asyncio.CancelledError:
                    raise
                except FloodWaitError:
                    # FloodWait applies to the account/request stream, not one
                    # catalog row.  Returning a partial report as if it were a
                    # source failure would be unsafe.
                    raise
                except SOURCE_SPECIFIC_TELEGRAM_ERRORS as exc:
                    results.append(
                        _unavailable_result(
                            target,
                            error_type=_safe_type_name(type(exc)),
                        )
                    )
                    continue
                results.append(_available_result(target, entity))

        available_count = sum(result.available for result in results)
        return SourceProbeReport(
            candidate_count=len(targets),
            available_count=available_count,
            unavailable_count=len(results) - available_count,
            results=tuple(results),
        )

    async def _load_candidates(self, *, limit: int) -> tuple[_CandidateTarget, ...]:
        targets: list[_CandidateTarget] = []
        async with self._database.connect() as connection:
            records = await self._repository.list_sources(
                connection,
                status=SourceStatus.CANDIDATE,
                platform="telegram",
                limit=limit,
            )
            for source in records:
                if (
                    source.platform != "telegram"
                    or source.lifecycle_status is not SourceStatus.CANDIDATE
                ):
                    raise TelegramSourceProbeError(
                        "Каталог вернул источник вне candidate Telegram scope"
                    )
                lineage = await self._repository.list_lineage(
                    connection,
                    source.id,
                )
                targets.append(
                    _CandidateTarget(
                        source=source,
                        seed=_seed_metadata(lineage),
                    )
                )
        return tuple(targets)

    @asynccontextmanager
    async def _connected_client(self):
        if self._active:
            raise TelegramSourceProbeError(
                "Проверка Telegram уже запущена в этом процессе"
            )

        session_path = self._credentials.session_path
        prepare_private_session_storage(
            session_path,
            runtime_root=self._runtime_root,
        )
        protect_session_files(session_path, runtime_root=self._runtime_root)
        lock = self._lock_factory(session_path, role="ai_sales_radar_source_probe")
        lock.acquire()
        self._active = True
        client: SourceProbeClient | None = None
        operation_error: BaseException | None = None
        try:
            # Do not let Telethon open its SQLite session before the process
            # lock is held, and create any companion file with private mode.
            with private_umask():
                client = self._client_factory(self._credentials)
            await client.connect()
            if not await client.is_user_authorized():
                raise TelegramSourceProbeError(
                    "Telegram-сессия не авторизована; сначала выполните login"
                )
            _validate_account(await client.get_me())
            yield client
        except BaseException as exc:
            operation_error = exc
            raise
        finally:
            cleanup_error = await self._cleanup(client, session_path, lock)
            self._active = False
            if cleanup_error is not None and operation_error is None:
                stage, error = cleanup_error
                raise TelegramSourceProbeError(
                    "Не удалось безопасно завершить source probe "
                    f"на этапе {stage} ({_safe_type_name(type(error))})"
                ) from None

    async def _cleanup(
        self,
        client: SourceProbeClient | None,
        session_path: Path,
        lock: TelegramSessionFileLock,
    ) -> tuple[str, BaseException] | None:
        failures: list[tuple[str, BaseException]] = []
        if client is not None:
            try:
                await disconnect_client_fully(client)
            except BaseException as exc:
                failures.append(("disconnect", exc))
        try:
            protect_session_files(session_path, runtime_root=self._runtime_root)
        except BaseException as exc:
            failures.append(("protect_session", exc))
        try:
            lock.release()
        except BaseException as exc:
            failures.append(("release_lock", exc))
        return failures[0] if failures else None


def _default_client_factory(credentials: TelegramCredentials) -> SourceProbeClient:
    return TelegramClient(
        str(credentials.session_path),
        credentials.api_id,
        credentials.api_hash,
        flood_sleep_threshold=0,
    )


def _candidate_lookup(source: SourceRecord) -> str | None:
    handle = source.handle
    if isinstance(handle, str):
        selected = handle.strip()
        if HANDLE_RE.fullmatch(selected):
            return f"@{selected.removeprefix('@')}"

    canonical_url = source.canonical_url
    if not isinstance(canonical_url, str):
        return None
    parsed = urlparse(canonical_url.strip())
    path = parsed.path.strip("/")
    username = path.removeprefix("@")
    if (
        parsed.scheme.lower() != "https"
        or parsed.netloc.lower() not in {"t.me", "telegram.me"}
        or parsed.params
        or parsed.query
        or parsed.fragment
        or "/" in path
        or not HANDLE_RE.fullmatch(username)
    ):
        return None
    return f"@{username}"


def _seed_metadata(lineage: Sequence[SourceLineageRecord]) -> _SeedMetadata:
    for record in reversed(lineage):
        if record.provider != SEED_PROVIDER or not isinstance(record.context, Mapping):
            continue
        context = record.context
        raw_enabled = context.get("enabled")
        enabled = raw_enabled if isinstance(raw_enabled, bool) else None
        reason = _safe_text(context.get("reason"), max_length=2_000)
        raw_tags = context.get("tags")
        tags: list[str] = []
        if isinstance(raw_tags, (list, tuple)) and len(raw_tags) <= 64:
            for raw_tag in raw_tags:
                tag = _safe_text(raw_tag, max_length=128)
                if tag is not None and tag not in tags:
                    tags.append(tag)
        return _SeedMetadata(enabled=enabled, tags=tuple(tags), reason=reason)
    return _SeedMetadata()


def _available_result(
    target: _CandidateTarget,
    entity: Any,
) -> CandidateSourceProbeResult:
    source = target.source
    raw_participants = _safe_attribute(entity, "participants_count")
    participants_count = (
        raw_participants
        if isinstance(raw_participants, int)
        and not isinstance(raw_participants, bool)
        and 0 <= raw_participants <= 2_147_483_647
        else None
    )
    raw_username = _safe_attribute(entity, "username")
    entity_username = (
        raw_username
        if isinstance(raw_username, str)
        and re.fullmatch(r"[A-Za-z0-9_]{1,64}", raw_username)
        else None
    )
    return CandidateSourceProbeResult(
        **_configured_fields(target),
        available=True,
        entity_kind=_safe_type_name(type(entity)),
        entity_title=_safe_text(
            _safe_attribute(entity, "title"),
            max_length=512,
        ),
        entity_username=entity_username,
        participants_count=participants_count,
    )


def _unavailable_result(
    target: _CandidateTarget,
    *,
    error_type: str,
) -> CandidateSourceProbeResult:
    return CandidateSourceProbeResult(
        **_configured_fields(target),
        available=False,
        error_type=error_type,
    )


def _configured_fields(target: _CandidateTarget) -> dict[str, Any]:
    return {
        "source_id": target.source.id,
        "configured_title": target.source.display_name,
        "configured_handle": target.source.handle,
        "configured_url": target.source.canonical_url,
        "seed_enabled": target.seed.enabled,
        "tags": target.seed.tags,
        "reason": target.seed.reason,
    }


def _safe_attribute(value: Any, name: str) -> Any:
    try:
        return getattr(value, name, None)
    except Exception:
        return None


def _safe_text(value: Any, *, max_length: int) -> str | None:
    if not isinstance(value, str):
        return None
    selected = value.strip()
    if not selected or len(selected) > max_length:
        return None
    # JSON escapes ASCII controls, but format/surrogate characters can still
    # alter terminal presentation.  Omit provider metadata containing them.
    if any(
        ord(character) < 32
        or 0xD800 <= ord(character) <= 0xDFFF
        or character in {"\u202a", "\u202b", "\u202c", "\u202d", "\u202e"}
        for character in selected
    ):
        return None
    return selected


def _safe_type_name(error_type: type[Any]) -> str:
    name = error_type.__name__
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", name):
        return name
    return "ProviderError"


def _validate_account(account: Any) -> None:
    account_id = _safe_attribute(account, "id")
    if (
        isinstance(account_id, bool)
        or not isinstance(account_id, int)
        or account_id <= 0
    ):
        raise TelegramSourceProbeError(
            "Telegram не вернул корректный ID авторизованного аккаунта"
        )


def _bounded_source_limit(value: str) -> int:
    try:
        selected = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("ожидается целое число") from None
    if not 1 <= selected <= MAX_SOURCE_LIMIT:
        raise argparse.ArgumentTypeError(
            f"значение должно быть от 1 до {MAX_SOURCE_LIMIT}"
        )
    return selected


def _bounded_delay(value: str) -> float:
    try:
        selected = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("ожидается число") from None
    if not math.isfinite(selected) or not 0.1 <= selected <= MAX_REQUEST_DELAY_SECONDS:
        raise argparse.ArgumentTypeError(
            f"значение должно быть от 0.1 до {MAX_REQUEST_DELAY_SECONDS}"
        )
    return selected


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only проверка доступности Telegram-кандидатов "
            "из PostgreSQL"
        ),
    )
    parser.add_argument(
        "--source-limit",
        type=_bounded_source_limit,
        default=DEFAULT_SOURCE_LIMIT,
        help=f"проверить не более N кандидатов (максимум {MAX_SOURCE_LIMIT})",
    )
    parser.add_argument(
        "--request-delay-seconds",
        type=_bounded_delay,
        default=DEFAULT_REQUEST_DELAY_SECONDS,
        help="пауза между Telegram-запросами (по умолчанию 1.0 сек.)",
    )
    return parser


async def _run_command(args: argparse.Namespace) -> SourceProbeReport:
    database = Database(
        DatabaseSettings.from_env().require_url(),
        pool_size=2,
        max_overflow=0,
    )
    try:
        credentials = TelegramAuthSettings.from_env().require_credentials()
        service = TelegramCandidateSourceProbe(database, credentials)
        return await service.run(
            options=SourceProbeOptions(
                source_limit=args.source_limit,
                request_delay_seconds=args.request_delay_seconds,
            )
        )
    finally:
        await database.close()


def _report_payload(report: SourceProbeReport) -> dict[str, Any]:
    return {
        "ok": True,
        "candidate_count": report.candidate_count,
        "available_count": report.available_count,
        "unavailable_count": report.unavailable_count,
        "results": [asdict(result) for result in report.results],
    }


def _emit_json(payload: Mapping[str, Any], *, stream: Any = None) -> None:
    selected_stream = sys.stdout if stream is None else stream
    print(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2),
        file=selected_stream,
    )


def _error_payload(error_type: type[Any], message: str | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ok": False,
        "error_type": _safe_type_name(error_type),
    }
    if message is not None:
        payload["message"] = message
    return payload


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        report = asyncio.run(_run_command(args))
        _emit_json(_report_payload(report))
        return 0
    except (ConfigurationError, TelegramSourceProbeError) as exc:
        _emit_json(_error_payload(type(exc), str(exc)), stream=sys.stderr)
        return 2
    except TelegramSessionInUseError:
        _emit_json(
            _error_payload(
                TelegramSessionInUseError,
                "Telegram-сессия уже используется другим процессом",
            ),
            stream=sys.stderr,
        )
        return 3
    except FloodWaitError:
        _emit_json(_error_payload(FloodWaitError), stream=sys.stderr)
        return 4
    except KeyboardInterrupt:
        _emit_json(_error_payload(KeyboardInterrupt), stream=sys.stderr)
        return 130
    except Exception as exc:
        # Do not serialize exception text: provider/database errors may echo
        # credentials, peer identifiers, or other private details.
        _emit_json(_error_payload(type(exc)), stream=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

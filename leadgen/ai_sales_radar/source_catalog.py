"""Operator-controlled PostgreSQL source catalog for AI Sales Radar.

The repository JSON is only an initial catalogue suggestion.  Importing it
never approves a Telegram source: every new seed enters the lifecycle as a
candidate and must be approved by an explicit operator transition.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime
from enum import Enum
import hashlib
import json
from pathlib import Path
import sys
from typing import Any
from uuid import UUID

import sqlalchemy as sa

from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.source_repository import (
    InvalidSourceTransition,
    SeedSource,
    SourceLifecycleEvent,
    SourceLineageRecord,
    SourceNotFound,
    SourceRecord,
    SourceRepository,
    SourceStatus,
)
from freelancer_bot.sources import Source, load_sources

from .config import ConfigurationError, DatabaseSettings, PROJECT_ROOT


DEFAULT_SOURCES_PATH = PROJECT_ROOT / "config" / "sources.json"
SEED_PROVIDER = "ai_sales_radar_seed"
SEED_LOCK_NAME = "ai_sales_radar:source_catalog_seed"
ALLOWED_OPERATOR_TARGETS = frozenset(
    {SourceStatus.APPROVED, SourceStatus.PAUSED, SourceStatus.REJECTED}
)


class SourceCatalogError(RuntimeError):
    """A source-catalog failure whose message is safe to show to an operator."""


@dataclass(frozen=True)
class SourceSeedResult:
    total: int
    created: int
    updated: int
    unchanged: int
    lineage_created: int
    approved_entries: int
    candidate_entries: int
    snapshot_sha256: str


@dataclass(frozen=True)
class SourceListResult:
    count: int
    sources: tuple[SourceRecord, ...]


@dataclass(frozen=True)
class SourceDetails:
    source: SourceRecord
    lineage: tuple[SourceLineageRecord, ...]
    lifecycle_events: tuple[SourceLifecycleEvent, ...]


@dataclass(frozen=True)
class SourceTransitionResult:
    changed: bool
    previous_status: SourceStatus
    source: SourceRecord


class SourceCatalogService:
    """Small channel-neutral facade over the proven source repository."""

    def __init__(
        self,
        database: Database,
        repository: SourceRepository | None = None,
    ) -> None:
        self._database = database
        self._repository = repository or SourceRepository()

    async def import_seed(self, path: Path = DEFAULT_SOURCES_PATH) -> SourceSeedResult:
        selected_path = _project_seed_path(path)
        snapshot_sha256 = hashlib.sha256(selected_path.read_bytes()).hexdigest()
        configured_sources = load_sources(selected_path)
        entries = tuple(
            _seed_source(source, snapshot_sha256=snapshot_sha256)
            for source in configured_sources
        )

        created = 0
        updated = 0
        unchanged = 0
        lineage_created = 0
        async with self._database.transaction() as connection:
            await connection.execute(
                sa.text("SELECT pg_advisory_xact_lock(hashtextextended(:name, 0))"),
                {"name": SEED_LOCK_NAME},
            )
            for entry in entries:
                outcome = await self._repository.upsert_seed(connection, entry)
                created += int(outcome.created)
                updated += int(outcome.updated)
                unchanged += int(not outcome.created and not outcome.updated)
                lineage_created += int(outcome.lineage_created)

        return SourceSeedResult(
            total=len(entries),
            created=created,
            updated=updated,
            unchanged=unchanged,
            lineage_created=lineage_created,
            approved_entries=0,
            candidate_entries=len(entries),
            snapshot_sha256=snapshot_sha256,
        )

    async def list_sources(
        self,
        *,
        status: SourceStatus | str | None = None,
        platform: str | None = "telegram",
        limit: int = 100,
    ) -> SourceListResult:
        async with self._database.connect() as connection:
            records = await self._repository.list_sources(
                connection,
                status=status,
                platform=platform,
                limit=limit,
            )
        return SourceListResult(count=len(records), sources=tuple(records))

    async def show_source(self, source_id: int) -> SourceDetails:
        selected_id = _positive_source_id(source_id)
        async with self._database.connect() as connection:
            source = await self._repository.get(connection, selected_id)
            lineage = await self._repository.list_lineage(connection, selected_id)
            events = await self._repository.list_lifecycle_events(
                connection,
                selected_id,
            )
        return SourceDetails(
            source=source,
            lineage=tuple(lineage),
            lifecycle_events=tuple(events),
        )

    async def transition_source(
        self,
        source_id: int,
        target: SourceStatus | str,
        *,
        actor: str,
        reason: str,
    ) -> SourceTransitionResult:
        selected_id = _positive_source_id(source_id)
        selected_target = _operator_target(target)
        selected_actor = _required_text(actor, "actor", max_length=128)
        selected_reason = _required_text(reason, "reason")

        async with self._database.transaction() as connection:
            current = await self._repository.get(connection, selected_id)
            if current.lifecycle_status is selected_target:
                return SourceTransitionResult(
                    changed=False,
                    previous_status=current.lifecycle_status,
                    source=current,
                )
            updated = await self._repository.override(
                connection,
                selected_id,
                selected_target,
                operator_id=selected_actor,
                reason=selected_reason,
            )
        return SourceTransitionResult(
            changed=True,
            previous_status=current.lifecycle_status,
            source=updated,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Каталог Telegram-источников AI Sales Radar",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    seed = subcommands.add_parser(
        "seed",
        help="импортировать config/sources.json только как кандидатов",
    )
    seed.add_argument("--sources-json", type=Path, default=DEFAULT_SOURCES_PATH)

    listing = subcommands.add_parser("list", help="показать каталог источников")
    listing.add_argument(
        "--status",
        choices=tuple(status.value for status in SourceStatus),
    )
    listing.add_argument("--platform", default="telegram")
    listing.add_argument("--limit", type=int, default=100)

    show = subcommands.add_parser("show", help="показать источник и его историю")
    show.add_argument("source_id", type=int)

    transition = subcommands.add_parser(
        "transition",
        help="явно изменить решение оператора по источнику",
    )
    transition.add_argument("source_id", type=int)
    transition.add_argument(
        "target",
        choices=tuple(sorted(status.value for status in ALLOWED_OPERATOR_TARGETS)),
    )
    transition.add_argument("--actor", required=True)
    transition.add_argument("--reason", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        database_url = DatabaseSettings.from_env().require_url()
        result = asyncio.run(_execute(args, database_url=database_url))
        _emit(result)
        return 0
    except (
        ConfigurationError,
        InvalidSourceTransition,
        SourceCatalogError,
        SourceNotFound,
        ValueError,
    ) as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(
            "Операция с каталогом источников не выполнена. Подробности скрыты "
            f"для защиты секретов ({type(exc).__name__}).",
            file=sys.stderr,
        )
        return 1


async def _execute(args: argparse.Namespace, *, database_url: str) -> Any:
    database = Database(database_url, pool_size=2, max_overflow=0)
    try:
        service = SourceCatalogService(database)
        if args.command == "seed":
            return await service.import_seed(args.sources_json)
        if args.command == "list":
            return await service.list_sources(
                status=args.status,
                platform=args.platform,
                limit=args.limit,
            )
        if args.command == "show":
            return await service.show_source(args.source_id)
        if args.command == "transition":
            return await service.transition_source(
                args.source_id,
                args.target,
                actor=args.actor,
                reason=args.reason,
            )
        raise SourceCatalogError("Неизвестная команда каталога источников")
    finally:
        await database.close()


def _seed_source(source: Source, *, snapshot_sha256: str) -> SeedSource:
    handle = source.handle.lower()
    username = handle.removeprefix("@")
    return SeedSource(
        platform="telegram",
        external_id=f"username:{username}",
        access_type="public",
        initial_status=SourceStatus.CANDIDATE,
        display_name=source.title,
        handle=handle,
        canonical_url=f"https://t.me/{username}",
        provider=SEED_PROVIDER,
        lineage_key=f"config-sources-json:{username}",
        provider_run_id=snapshot_sha256,
        seed_reference=handle,
        context={
            "enabled": source.enabled,
            "reason": source.reason,
            "seed_handle": handle,
            "tags": list(source.tags),
        },
    )


def _project_seed_path(path: Path) -> Path:
    candidate = path.expanduser()
    if not candidate.is_absolute():
        candidate = PROJECT_ROOT / candidate
    try:
        resolved = candidate.resolve(strict=True)
    except OSError:
        raise SourceCatalogError("Файл источников не найден внутри проекта") from None
    if not resolved.is_relative_to(PROJECT_ROOT.resolve(strict=True)):
        raise SourceCatalogError("Файл источников должен находиться внутри проекта")
    if not resolved.is_file():
        raise SourceCatalogError("Путь источников не является файлом")
    return resolved


def _positive_source_id(value: int) -> int:
    if isinstance(value, bool) or value <= 0:
        raise SourceCatalogError("source_id должен быть положительным числом")
    return value


def _operator_target(value: SourceStatus | str) -> SourceStatus:
    try:
        selected = SourceStatus(value)
    except ValueError:
        raise SourceCatalogError("Недопустимый целевой статус источника") from None
    if selected not in ALLOWED_OPERATOR_TARGETS:
        allowed = ", ".join(sorted(item.value for item in ALLOWED_OPERATOR_TARGETS))
        raise SourceCatalogError(f"Целевой статус должен быть одним из: {allowed}")
    return selected


def _required_text(value: str, field: str, *, max_length: int | None = None) -> str:
    selected = value.strip()
    if not selected:
        raise SourceCatalogError(f"{field} не должен быть пустым")
    if max_length is not None and len(selected) > max_length:
        raise SourceCatalogError(f"{field} не должен быть длиннее {max_length} символов")
    return selected


def _emit(value: Any) -> None:
    print(json.dumps(_jsonable(value), ensure_ascii=False, indent=2, sort_keys=True))


def _jsonable(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _jsonable(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_jsonable(item) for item in value]
    return value


if __name__ == "__main__":
    raise SystemExit(main())

"""JSON CLI for profile matching and the channel-neutral Lead Inbox."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import fields, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
import json
import sys
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from freelancer_bot.persistence.database import Database

from .config import ConfigurationError, DatabaseSettings
from .inbox import LeadInboxService
from .persistence.inbox import (
    InboxAvailability,
    InboxStatus,
    LeadInboxItemNotFound,
    LeadInboxStatusConflict,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Локальный Lead Inbox AI Sales Radar",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    sync = commands.add_parser(
        "sync",
        help="пересчитать matching для профиля и обновить Inbox",
    )
    sync.add_argument("--profile-id", type=UUID, required=True)
    sync.add_argument("--limit", type=int, default=500)

    listing = commands.add_parser("list", help="показать лиды профиля")
    listing.add_argument("--profile-id", type=UUID, required=True)
    listing.add_argument(
        "--status",
        choices=tuple(item.value for item in InboxStatus),
    )
    listing.add_argument(
        "--availability",
        choices=tuple(item.value for item in InboxAvailability),
    )
    listing.add_argument("--tier", choices=("hot", "warm", "low"))
    listing.add_argument("--limit", type=int, default=100)

    show = commands.add_parser(
        "show",
        help="показать лид, Opportunity, matching evidence и историю",
    )
    show.add_argument("--lead-id", type=UUID, required=True)

    status = commands.add_parser(
        "status",
        help="изменить ручной workflow-статус лида",
    )
    status.add_argument("--lead-id", type=UUID, required=True)
    status.add_argument(
        "--to",
        choices=tuple(item.value for item in InboxStatus),
        required=True,
    )
    status.add_argument("--expected-revision", type=int, required=True)
    status.add_argument("--actor", default="local-operator")
    status.add_argument("--reason", required=True)
    status.add_argument("--request-id", required=True)
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
        LeadInboxItemNotFound,
        LeadInboxStatusConflict,
        LookupError,
        ValueError,
    ) as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(
            "Операция с Lead Inbox не выполнена. Подробности скрыты "
            f"для защиты секретов ({type(exc).__name__}).",
            file=sys.stderr,
        )
        return 1


async def _execute(args: argparse.Namespace, *, database_url: str) -> Any:
    database = Database(database_url, pool_size=3, max_overflow=1)
    try:
        service = LeadInboxService(database)
        if args.command == "sync":
            return await service.sync_profile(
                args.profile_id,
                opportunity_limit=args.limit,
            )
        if args.command == "list":
            records = await service.list_leads(
                profile_id=args.profile_id,
                status=args.status,
                availability=args.availability,
                tier=args.tier,
                limit=args.limit,
            )
            return {"count": len(records), "leads": records}
        if args.command == "show":
            return await service.show_lead(args.lead_id)
        if args.command == "status":
            return await service.update_status(
                args.lead_id,
                args.to,
                expected_revision=args.expected_revision,
                actor_id=args.actor,
                reason=args.reason,
                request_id=args.request_id,
            )
        raise ValueError("Неизвестная команда Lead Inbox")
    finally:
        await database.close()


def _emit(value: Any) -> None:
    print(json.dumps(_jsonable(value), ensure_ascii=False, indent=2, sort_keys=True))


def _jsonable(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return _jsonable(value.model_dump(mode="json"))
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _jsonable(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return [_jsonable(item) for item in value]
    return value


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["build_parser", "main"]

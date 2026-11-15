"""Explicit, manual CLI for AI Sales Radar reply drafts."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, fields, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
import json
import sys
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from freelancer_bot.persistence.database import Database

from .config import ConfigurationError, DatabaseSettings, ReplyDraftSettings
from .persistence.inbox import LeadInboxItemNotFound
from .persistence.reply_drafts import ReplyDraftConflict
from .reply_drafts import (
    ReplyDraftError,
    ReplyDraftService,
    load_private_persona,
)


@dataclass(frozen=True)
class ReplyDraftCheck:
    database_configured: bool
    database_driver_valid: bool
    ai_provider_configured: bool
    external_ai_data_processing_approved: bool
    provider: str
    model: str
    persona_path: str
    persona_customized: bool
    persona_valid: bool
    automatic_sending_enabled: bool = False


def inspect_reply_draft_configuration(
    database: DatabaseSettings,
    drafts: ReplyDraftSettings,
) -> ReplyDraftCheck:
    database_valid = False
    if database.configured():
        database.require_url()
        database_valid = True
    load_private_persona(drafts.persona_path)
    return ReplyDraftCheck(
        database_configured=database.configured(),
        database_driver_valid=database_valid,
        ai_provider_configured=drafts.configured(),
        external_ai_data_processing_approved=bool(
            getattr(drafts, "external_ai_data_processing_approved", False)
        ),
        provider=drafts.provider,
        model=drafts.model,
        persona_path=str(drafts.persona_path),
        persona_customized=drafts.persona_path.is_file(),
        persona_valid=True,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ручные AI-черновики ответов AI Sales Radar",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "check",
        help="проверить настройки без подключения к БД и внешнему AI",
    )
    create = commands.add_parser(
        "create",
        help="явно создать или переиспользовать черновик для лида",
    )
    create.add_argument("--lead-id", type=UUID, required=True)

    listing = commands.add_parser("list", help="показать черновики лида")
    listing.add_argument("--lead-id", type=UUID, required=True)
    listing.add_argument("--limit", type=int, default=100)

    show = commands.add_parser("show", help="показать сохранённый черновик")
    show.add_argument("--draft-id", type=UUID, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        database = DatabaseSettings.from_env()
        drafts = ReplyDraftSettings.from_env()
        if args.command == "check":
            status = inspect_reply_draft_configuration(database, drafts)
            print(json.dumps(asdict(status), ensure_ascii=False, sort_keys=True))
            return (
                0
                if (
                    status.database_configured
                    and status.database_driver_valid
                    and status.ai_provider_configured
                    and status.external_ai_data_processing_approved
                )
                else 1
            )
        result = asyncio.run(
            _execute(
                args,
                database_url=database.require_url(),
                settings=drafts,
            )
        )
        _emit(result)
        return 0
    except (
        ConfigurationError,
        LeadInboxItemNotFound,
        LookupError,
        ReplyDraftConflict,
        ReplyDraftError,
        ValueError,
    ) as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(
            "Операция с AI-черновиком не выполнена. Подробности скрыты "
            f"для защиты секретов ({type(exc).__name__}).",
            file=sys.stderr,
        )
        return 1


async def _execute(
    args: argparse.Namespace,
    *,
    database_url: str,
    settings: ReplyDraftSettings,
) -> Any:
    database = Database(database_url, pool_size=2, max_overflow=0)
    try:
        service = ReplyDraftService(database, settings)
        if args.command == "create":
            return await service.create(args.lead_id)
        if args.command == "list":
            records = await service.list_for_lead(
                args.lead_id,
                limit=args.limit,
            )
            return {"count": len(records), "drafts": records}
        if args.command == "show":
            return await service.get(args.draft_id)
        raise ValueError("Неизвестная команда AI-черновиков")
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


__all__ = [
    "ReplyDraftCheck",
    "build_parser",
    "inspect_reply_draft_configuration",
    "main",
]

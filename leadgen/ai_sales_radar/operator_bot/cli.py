"""Offline check and local runner for the private operator bot."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass
import json
import sys

from ai_sales_radar.config import (
    ConfigurationError,
    DatabaseSettings,
    OperatorBotSettings,
    OperatorSourceDiscoverySettings,
    ReplyDraftSettings,
)
from freelancer_bot.telegram_session import TelegramSessionInUseError

from .telethon_runtime import OperatorBotIdentityError, run_operator_bot


@dataclass(frozen=True)
class OperatorBotCheck:
    enabled: bool
    database_configured: bool
    database_driver_valid: bool
    telegram_api_configured: bool
    bot_token_configured: bool
    owner_configured: bool
    private_chat_only: bool
    session_isolated_from_collector: bool
    transport: str = "local_telethon"
    notifications_enabled: bool = False
    collector_session_reused: bool = False
    automatic_client_sending_enabled: bool = False

    @property
    def ready(self) -> bool:
        return (
            self.enabled
            and self.database_configured
            and self.database_driver_valid
            and self.telegram_api_configured
            and self.bot_token_configured
            and self.owner_configured
            and self.private_chat_only
            and self.session_isolated_from_collector
        )


def inspect_operator_bot_configuration(
    database: DatabaseSettings,
    bot: OperatorBotSettings,
) -> OperatorBotCheck:
    database_valid = False
    if database.configured():
        database.require_url()
        database_valid = True
    owner_configured = bot.owner_configured()
    isolated = bot.session_path != bot.collector_session_path
    return OperatorBotCheck(
        enabled=bot.enabled,
        database_configured=database.configured(),
        database_driver_valid=database_valid,
        telegram_api_configured=bot.telegram_api_configured(),
        bot_token_configured=bot.bot_token_configured(),
        owner_configured=owner_configured,
        private_chat_only=owner_configured,
        session_isolated_from_collector=isolated,
        notifications_enabled=bot.notifications_enabled,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Приватный Telegram operator bot для AI Sales Radar",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "check",
        help="проверить конфигурацию без БД, Telegram и AI-запросов",
    )
    commands.add_parser(
        "run",
        help="запустить только приватный operator bot",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        database = DatabaseSettings.from_env()
        bot = OperatorBotSettings.from_env()
        if args.command == "check":
            report = inspect_operator_bot_configuration(database, bot)
            payload = {**asdict(report), "ready": report.ready}
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return 0 if report.ready else 1
        if args.command == "run":
            asyncio.run(
                run_operator_bot(
                    database,
                    bot,
                    ReplyDraftSettings.from_env(),
                    source_discovery_settings=(
                        OperatorSourceDiscoverySettings.from_env()
                    ),
                )
            )
            return 0
    except (
        ConfigurationError,
        OperatorBotIdentityError,
        TelegramSessionInUseError,
        ValueError,
    ) as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(
            "Operator bot остановлен из-за ошибки. Подробности скрыты "
            f"для защиты данных ({type(exc).__name__}).",
            file=sys.stderr,
        )
        return 1
    return 2


__all__ = [
    "OperatorBotCheck",
    "build_parser",
    "inspect_operator_bot_configuration",
    "main",
]

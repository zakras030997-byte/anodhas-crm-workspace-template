"""CLI for explicit operator-bot folder plan and sync actions."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import json
import sys

from freelancer_bot.telegram_session import TelegramSessionInUseError

from ai_sales_radar.config import (
    ConfigurationError,
    OperatorBotCredentials,
    OperatorBotSettings,
    TelegramAuthSettings,
    TelegramCredentials,
)
from ai_sales_radar.telegram_folder import TelegramFolderError

from .folder import (
    OperatorBotFolderError,
    OperatorBotFolderReconciler,
    OperatorBotFolderReport,
    SYNC_CONCURRENT_EDIT_WARNING,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Безопасное размещение operator bot в папке Ист_Лид_АИ",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "plan",
        help="проверить чат и папку без изменения Telegram",
    )
    sync = commands.add_parser(
        "sync",
        help=(
            "явно добавить чат бота; не редактируйте папку параллельно "
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
        user_credentials = TelegramAuthSettings.from_env().require_credentials()
        bot_credentials = OperatorBotSettings.from_env().require_credentials()
        report = asyncio.run(
            _execute(
                args.command,
                user_credentials=user_credentials,
                bot_credentials=bot_credentials,
            )
        )
        print(json.dumps(asdict(report), ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    except (ConfigurationError, TelegramFolderError) as exc:
        _emit_error(str(exc))
        return 2
    except TelegramSessionInUseError:
        _emit_error(
            "Пользовательская Telegram-сессия уже используется другим процессом"
        )
        return 3
    except Exception as exc:
        _emit_error(
            "Операция с папкой operator bot не выполнена; подробности скрыты "
            f"для защиты секретов ({type(exc).__name__})"
        )
        return 1


async def _execute(
    command: str,
    *,
    user_credentials: TelegramCredentials,
    bot_credentials: OperatorBotCredentials,
) -> OperatorBotFolderReport:
    reconciler = OperatorBotFolderReconciler(
        user_credentials,
        bot_credentials,
    )
    if command == "plan":
        return await reconciler.plan()
    if command == "sync":
        return await reconciler.sync()
    raise OperatorBotFolderError("Неизвестная команда папки operator bot")


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


__all__ = ["build_parser", "main"]

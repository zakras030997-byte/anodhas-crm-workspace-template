"""Local bootstrap and explicit PostgreSQL migration commands."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import stat
import sys
from typing import Callable

from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
import sqlalchemy as sa

from freelancer_bot.persistence.database import Database

from .config import (
    ConfigurationError,
    DatabaseSettings,
    OpportunityAISettings,
    PROJECT_ROOT,
    TelegramAuthSettings,
)


ENV_TEMPLATE = """# Local AI Sales Radar settings. Never commit this file.
DATABASE_URL=
TELEGRAM_API_ID=
TELEGRAM_API_HASH=
AI_SALES_RADAR_SESSION_PATH=.local/telegram/collector.session
OPPORTUNITY_ANALYSIS_PROVIDER=groq
OPPORTUNITY_ANALYSIS_MODEL=openai/gpt-oss-20b
OPPORTUNITY_ANALYSIS_PRICING_VERSION=groq-gpt-oss-20b-2026-08-27
OPPORTUNITY_ANALYSIS_INPUT_USD_PER_MILLION=0.075
OPPORTUNITY_ANALYSIS_OUTPUT_USD_PER_MILLION=0.30
OPPORTUNITY_ANALYSIS_BUDGET_RESERVE_OUTPUT_TOKENS=800
EXTERNAL_AI_DATA_PROCESSING_APPROVED=false
OPENAI_API_KEY=
GROQ_API_KEY=
"""


@dataclass(frozen=True)
class LocalLayout:
    root: Path = PROJECT_ROOT

    @property
    def env_file(self) -> Path:
        return self.root / ".env"

    @property
    def runtime_directories(self) -> tuple[Path, ...]:
        local = self.root / ".local"
        return (
            local,
            local / "telegram",
            local / "cache",
            local / "logs",
        )


@dataclass(frozen=True)
class BootstrapStatus:
    project_root: str
    python_version: str
    required_python: str
    python_matches: bool
    env_file_exists: bool
    database_configured: bool
    telegram_configured: bool
    provider_configured: bool
    external_ai_data_processing_approved: bool
    provider: str
    telegram_session_path: str
    alembic_files_present: bool


@dataclass(frozen=True)
class DatabaseStatus:
    reachable: bool
    current_heads: tuple[str, ...]
    expected_heads: tuple[str, ...]

    @property
    def migration_current(self) -> bool:
        return self.reachable and self.current_heads == self.expected_heads


def initialize_local_layout(layout: LocalLayout = LocalLayout()) -> bool:
    """Create private runtime directories and a non-overwriting `.env` template."""
    root = layout.root.resolve(strict=True)
    for directory in layout.runtime_directories:
        _ensure_private_directory(directory, root=root)

    _validate_private_file_path(layout.env_file, root=root)
    created = _create_private_file(layout.env_file, ENV_TEMPLATE)
    _validate_private_file_path(layout.env_file, root=root, must_exist=True)
    os.chmod(layout.env_file, 0o600, follow_symlinks=False)
    return created


def inspect_bootstrap(
    *,
    layout: LocalLayout = LocalLayout(),
    env_file: Path | None = None,
) -> BootstrapStatus:
    selected_env = env_file if env_file is not None else layout.env_file
    database = DatabaseSettings.from_env(selected_env)
    telegram = TelegramAuthSettings.from_env(selected_env)
    ai = OpportunityAISettings.from_env(selected_env)
    required_python = (layout.root / ".python-version").read_text(
        encoding="utf-8"
    ).strip()
    current_python = ".".join(str(part) for part in sys.version_info[:3])
    return BootstrapStatus(
        project_root=str(layout.root.resolve()),
        python_version=current_python,
        required_python=required_python,
        python_matches=current_python == required_python,
        env_file_exists=selected_env.exists(),
        database_configured=database.configured(),
        telegram_configured=telegram.configured(),
        provider_configured=ai.configured(),
        external_ai_data_processing_approved=(
            ai.external_ai_data_processing_approved
        ),
        provider=ai.provider,
        telegram_session_path=str(telegram.session_path),
        alembic_files_present=(
            (layout.root / "alembic.ini").is_file()
            and (layout.root / "migrations" / "env.py").is_file()
        ),
    )


def alembic_config(database_url: str | None = None) -> Config:
    config = Config(str(PROJECT_ROOT / "alembic.ini"))
    if database_url is not None:
        config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    return config


def expected_migration_heads() -> tuple[str, ...]:
    return tuple(sorted(ScriptDirectory.from_config(alembic_config()).get_heads()))


async def inspect_database(
    settings: DatabaseSettings,
    *,
    database_factory: Callable[..., Database] = Database,
) -> DatabaseStatus:
    database = database_factory(
        settings.require_url(),
        pool_size=1,
        max_overflow=0,
    )
    try:
        async with database.connect() as connection:
            await connection.execute(sa.text("SELECT 1"))
            current = await connection.run_sync(
                lambda sync_connection: tuple(
                    sorted(
                        MigrationContext.configure(sync_connection).get_current_heads()
                    )
                )
            )
        return DatabaseStatus(
            reachable=True,
            current_heads=current,
            expected_heads=expected_migration_heads(),
        )
    finally:
        await database.close()


def migrate_database(settings: DatabaseSettings) -> None:
    """Run migrations only when the operator explicitly selects `migrate`."""
    command.upgrade(alembic_config(settings.require_url()), "head")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Локальная подготовка AI Sales Radar",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("init", help="создать приватные локальные папки и .env")
    subcommands.add_parser("check", help="проверить локальную конфигурацию без БД")
    subcommands.add_parser("db-status", help="проверить PostgreSQL и миграции")
    subcommands.add_parser("migrate", help="явно применить миграции PostgreSQL")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "init":
            created = initialize_local_layout()
            print("Локальное окружение готово.")
            print("Файл .env создан." if created else "Существующий .env сохранён.")
            return 0
        if args.command == "check":
            _emit(inspect_bootstrap())
            return 0

        settings = DatabaseSettings.from_env()
        if args.command == "db-status":
            status = asyncio.run(inspect_database(settings))
            _emit(status)
            return 0 if status.migration_current else 1
        if args.command == "migrate":
            migrate_database(settings)
            status = asyncio.run(inspect_database(settings))
            _emit(status)
            return 0 if status.migration_current else 1
    except ConfigurationError as exc:
        print(f"Ошибка конфигурации: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(
            "Операция не выполнена. Подробности скрыты для защиты секретов "
            f"({type(exc).__name__}).",
            file=sys.stderr,
        )
        return 1
    return 2


def _emit(value: BootstrapStatus | DatabaseStatus) -> None:
    payload = asdict(value)
    if isinstance(value, DatabaseStatus):
        payload["migration_current"] = value.migration_current
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _create_private_file(path: Path, content: str) -> bool:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError:
        return False
    try:
        os.write(descriptor, content.encode("utf-8"))
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return True


def _ensure_private_directory(path: Path, *, root: Path) -> None:
    resolved = path.resolve(strict=False)
    if path.is_symlink() or not resolved.is_relative_to(root):
        raise ValueError("runtime directory escapes the project root")
    resolved.mkdir(parents=True, exist_ok=True, mode=0o700)
    resolved_after_create = path.resolve(strict=True)
    if path.is_symlink() or not resolved_after_create.is_relative_to(root):
        raise ValueError("runtime directory escapes the project root")
    os.chmod(path, 0o700, follow_symlinks=False)


def _validate_private_file_path(
    path: Path,
    *,
    root: Path,
    must_exist: bool = False,
) -> None:
    if path.is_symlink():
        raise ValueError(".env не должен быть символической ссылкой")
    resolved = path.resolve(strict=False)
    if not resolved.is_relative_to(root):
        raise ValueError(".env выходит за пределы проекта")
    if must_exist or path.exists():
        try:
            mode = os.lstat(path).st_mode
        except FileNotFoundError:
            if must_exist:
                raise ValueError("не удалось создать .env") from None
            return
        if not stat.S_ISREG(mode):
            raise ValueError(".env должен быть обычным файлом")


if __name__ == "__main__":
    raise SystemExit(main())

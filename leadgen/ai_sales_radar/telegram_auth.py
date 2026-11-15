"""Explicit, interactive Telegram user-session authorization."""

from __future__ import annotations

import argparse
import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
import getpass
import os
from pathlib import Path
import re
import stat
import sys
from typing import Any, Callable, Protocol

from telethon import TelegramClient
from telethon.errors import (
    ApiIdInvalidError,
    FloodWaitError,
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberBannedError,
    PhoneNumberInvalidError,
    RPCError,
    SessionPasswordNeededError,
)

from freelancer_bot.telegram_session import (
    TelegramSessionFileLock,
    TelegramSessionInUseError,
)

from .config import (
    ConfigurationError,
    LOCAL_RUNTIME_ROOT,
    TelegramAuthSettings,
    TelegramCredentials,
)


class TelegramAuthError(RuntimeError):
    """A user-safe Telegram authorization failure."""


class TelegramAuthClient(Protocol):
    async def connect(self) -> Any: ...

    async def disconnect(self) -> Any: ...

    async def is_user_authorized(self) -> bool: ...

    async def get_me(self) -> Any: ...

    async def send_code_request(self, phone: str) -> Any: ...

    async def sign_in(self, *args: Any, **kwargs: Any) -> Any: ...

    async def log_out(self) -> Any: ...


@dataclass(frozen=True)
class TelegramAccount:
    user_id: int
    username: str | None


@dataclass(frozen=True)
class TelegramAuthStatus:
    authorized: bool
    account: TelegramAccount | None = None


ClientFactory = Callable[[TelegramCredentials], TelegramAuthClient]
SecretProvider = Callable[[], str]


class TelegramAuthService:
    def __init__(
        self,
        credentials: TelegramCredentials,
        *,
        client_factory: ClientFactory | None = None,
        lock_factory: Callable[..., TelegramSessionFileLock] = TelegramSessionFileLock,
        runtime_root: Path = LOCAL_RUNTIME_ROOT,
    ) -> None:
        self._credentials = credentials
        self._client_factory = client_factory or _default_client_factory
        self._lock_factory = lock_factory
        self._runtime_root = runtime_root

    async def status(self) -> TelegramAuthStatus:
        async def operation(client: TelegramAuthClient) -> TelegramAuthStatus:
            if not await client.is_user_authorized():
                return TelegramAuthStatus(authorized=False)
            return TelegramAuthStatus(
                authorized=True,
                account=await _read_account(client),
            )

        return await self._with_locked_client(operation)

    async def login(
        self,
        *,
        phone_provider: SecretProvider,
        code_provider: SecretProvider,
        password_provider: SecretProvider,
    ) -> TelegramAuthStatus:
        async def operation(client: TelegramAuthClient) -> TelegramAuthStatus:
            if await client.is_user_authorized():
                return TelegramAuthStatus(
                    authorized=True,
                    account=await _read_account(client),
                )

            phone = _required_private_input(phone_provider(), "номер телефона")
            sent_code = await client.send_code_request(phone)
            phone_code_hash = getattr(sent_code, "phone_code_hash", None)
            if not isinstance(phone_code_hash, str) or not phone_code_hash:
                raise TelegramAuthError("Telegram не вернул идентификатор кода входа")
            code = _required_private_input(code_provider(), "код Telegram")
            try:
                await client.sign_in(
                    phone=phone,
                    code=code,
                    phone_code_hash=phone_code_hash,
                )
            except SessionPasswordNeededError:
                password = _required_private_input(
                    password_provider(),
                    "пароль двухэтапной защиты",
                )
                await client.sign_in(password=password)

            if not await client.is_user_authorized():
                raise TelegramAuthError("Telegram-сессия не была авторизована")
            return TelegramAuthStatus(
                authorized=True,
                account=await _read_account(client),
            )

        return await self._with_locked_client(operation)

    async def logout(self) -> bool:
        async def operation(client: TelegramAuthClient) -> bool:
            if not await client.is_user_authorized():
                return False
            if await client.log_out() is not True:
                raise TelegramAuthError("Telegram не подтвердил отзыв локальной сессии")
            return True

        return await self._with_locked_client(operation)

    async def _with_locked_client(self, operation: Callable[[TelegramAuthClient], Any]):
        path = self._credentials.session_path
        prepare_private_session_storage(path, runtime_root=self._runtime_root)
        protect_session_files(path, runtime_root=self._runtime_root)
        lock = self._lock_factory(path, role="ai_sales_radar_auth")
        try:
            lock.acquire()
        except BaseException:
            lock.release()
            raise
        client: TelegramAuthClient | None = None
        try:
            with private_umask():
                client = self._client_factory(self._credentials)
            try:
                await client.connect()
                return await operation(client)
            except SessionPasswordNeededError:
                raise TelegramAuthError(
                    "Для аккаунта включена двухэтапная защита, но вход не завершён"
                ) from None
            except RPCError as exc:
                raise _safe_rpc_error(exc) from None
        finally:
            try:
                if client is not None:
                    await disconnect_client_fully(client)
            finally:
                try:
                    protect_session_files(path, runtime_root=self._runtime_root)
                finally:
                    lock.release()


async def disconnect_client_fully(client: Any) -> None:
    """Finish Telethon's shielded session flush before releasing its lock.

    Cancellation is delayed until the saved disconnect waiter completes.  This
    prevents a second process from opening the SQLite session while Telethon's
    inner shutdown task is still persisting entity/update state.
    """

    waiter = asyncio.ensure_future(client.disconnect())
    cancellation: asyncio.CancelledError | None = None
    while not waiter.done():
        try:
            await asyncio.shield(waiter)
        except asyncio.CancelledError as exc:
            if cancellation is None:
                cancellation = exc
        except BaseException:
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


def prepare_private_session_storage(
    session_path: Path,
    *,
    runtime_root: Path = LOCAL_RUNTIME_ROOT,
) -> None:
    _validate_runtime_session_path(session_path, runtime_root=runtime_root)
    session_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    _validate_runtime_session_path(session_path, runtime_root=runtime_root)
    os.chmod(session_path.parent, 0o700, follow_symlinks=False)


def protect_session_files(
    session_path: Path,
    *,
    runtime_root: Path = LOCAL_RUNTIME_ROOT,
) -> None:
    """Restrict the Telethon database and all SQLite companion files."""
    _validate_runtime_session_path(session_path, runtime_root=runtime_root)
    candidates = (
        session_path,
        Path(f"{session_path}-journal"),
        Path(f"{session_path}-wal"),
        Path(f"{session_path}-shm"),
        Path(f"{session_path}.lock"),
    )
    for candidate in candidates:
        try:
            mode = os.lstat(candidate).st_mode
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(mode):
            raise TelegramAuthError(
                "Файл Telegram-сессии не должен быть символической ссылкой"
            )
        if not stat.S_ISREG(mode):
            raise TelegramAuthError("Обнаружен некорректный файл Telegram-сессии")
        os.chmod(candidate, 0o600, follow_symlinks=False)


def _validate_runtime_session_path(
    session_path: Path,
    *,
    runtime_root: Path,
) -> None:
    nominal_root = runtime_root.absolute()
    if nominal_root.is_symlink():
        raise TelegramAuthError("Каталог Telegram-сессии не должен быть ссылкой")
    resolved_root = nominal_root.resolve(strict=False)
    expected_root = nominal_root.parent.resolve(strict=True) / nominal_root.name
    if resolved_root != expected_root:
        raise TelegramAuthError("Каталог Telegram-сессии выходит за разрешённый путь")
    if session_path.suffix != ".session":
        raise TelegramAuthError("Путь Telegram-сессии должен оканчиваться на .session")
    if session_path.is_symlink():
        raise TelegramAuthError("Файл Telegram-сессии не должен быть ссылкой")
    resolved_session = session_path.resolve(strict=False)
    if not resolved_session.is_relative_to(resolved_root):
        raise TelegramAuthError("Telegram-сессия выходит за разрешённый путь")


@contextmanager
def private_umask():
    previous = os.umask(0o077)
    try:
        yield
    finally:
        os.umask(previous)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Авторизация Telegram-сессии AI Sales Radar",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("status", help="проверить локальную сессию")
    subcommands.add_parser("login", help="войти с кодом Telegram и optional 2FA")
    logout = subcommands.add_parser("logout", help="отозвать локальную сессию")
    logout.add_argument(
        "--yes",
        action="store_true",
        help="не запрашивать дополнительное подтверждение",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        credentials = TelegramAuthSettings.from_env().require_credentials()
        service = TelegramAuthService(credentials)
        if args.command == "status":
            _print_status(asyncio.run(service.status()))
            return 0
        if args.command == "login":
            status = asyncio.run(
                service.login(
                    phone_provider=lambda: input("Номер телефона Telegram: "),
                    code_provider=lambda: getpass.getpass("Код из Telegram: "),
                    password_provider=lambda: getpass.getpass(
                        "Пароль двухэтапной защиты: "
                    ),
                )
            )
            _print_status(status)
            return 0
        if args.command == "logout":
            if not args.yes:
                confirmation = input("Отозвать локальную Telegram-сессию? [y/N]: ")
                if confirmation.strip().lower() not in {"y", "yes", "д", "да"}:
                    print("Операция отменена.")
                    return 0
            removed = asyncio.run(service.logout())
            print("Сессия отозвана." if removed else "Активной сессии нет.")
            return 0
    except (ConfigurationError, TelegramAuthError) as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 2
    except TelegramSessionInUseError:
        print("Ошибка: Telegram-сессия уже используется другим процессом.", file=sys.stderr)
        return 3
    except Exception as exc:
        print(
            "Операция Telegram не выполнена. Подробности скрыты для защиты "
            f"секретов ({type(exc).__name__}).",
            file=sys.stderr,
        )
        return 1
    return 2


def _default_client_factory(credentials: TelegramCredentials) -> TelegramAuthClient:
    return TelegramClient(
        str(credentials.session_path),
        credentials.api_id,
        credentials.api_hash,
        flood_sleep_threshold=0,
    )


async def _read_account(client: TelegramAuthClient) -> TelegramAccount:
    account = await client.get_me()
    user_id = getattr(account, "id", None)
    if isinstance(user_id, bool) or not isinstance(user_id, int) or user_id <= 0:
        raise TelegramAuthError("Telegram не вернул корректный ID аккаунта")
    raw_username = getattr(account, "username", None)
    username = raw_username if isinstance(raw_username, str) else None
    if username is not None and not re.fullmatch(r"[A-Za-z0-9_]{1,64}", username):
        username = None
    return TelegramAccount(user_id=user_id, username=username)


def _required_private_input(value: str, label: str) -> str:
    selected = value.strip()
    if not selected:
        raise TelegramAuthError(f"Не указан {label}")
    return selected


def _safe_rpc_error(error: RPCError) -> TelegramAuthError:
    if isinstance(error, PhoneNumberInvalidError):
        message = "Некорректный номер телефона Telegram"
    elif isinstance(error, PhoneNumberBannedError):
        message = "Telegram заблокировал этот номер телефона"
    elif isinstance(error, PhoneCodeInvalidError):
        message = "Некорректный код Telegram"
    elif isinstance(error, PhoneCodeExpiredError):
        message = "Код Telegram истёк; запустите login заново"
    elif isinstance(error, PasswordHashInvalidError):
        message = "Некорректный пароль двухэтапной защиты"
    elif isinstance(error, ApiIdInvalidError):
        message = "Telegram API ID или API Hash отклонены"
    elif isinstance(error, FloodWaitError):
        seconds = max(0, int(getattr(error, "seconds", 0)))
        message = f"Telegram временно ограничил вход; повторите через {seconds} сек."
    else:
        message = "Telegram отклонил запрос авторизации"
    return TelegramAuthError(message)


def _print_status(status: TelegramAuthStatus) -> None:
    if not status.authorized or status.account is None:
        print("Telegram-сессия не авторизована.")
        return
    username = f"@{status.account.username}" if status.account.username else "без username"
    print(f"Telegram-сессия активна: ID {status.account.user_id}, {username}.")


if __name__ == "__main__":
    raise SystemExit(main())

"""Exact-target installation of the Flowforge AI Telegram avatar suite."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import asdict, dataclass, replace
import json
import os
from pathlib import Path
import stat
import sys
from typing import Any, Protocol

from telethon import TelegramClient, functions, types, utils
from telethon.errors import FloodWaitError, RPCError

from freelancer_bot.telegram_session import (
    TelegramSessionFileLock,
    TelegramSessionInUseError,
)

from .config import ConfigurationError, TelegramAuthSettings, TelegramCredentials
from .telegram_auth import (
    disconnect_client_fully,
    prepare_private_session_storage,
    private_umask,
    protect_session_files,
)


FLOWFORGE_OWNER_ID = 0
FLOWFORGE_AVATAR_SUITE = "forge-core-v1"
MAX_AVATAR_BYTES = 10 * 1024 * 1024
_BRAND_ROOT = Path(__file__).resolve().parents[1] / "assets" / "brand"


class BrandAvatarError(RuntimeError):
    """Fail-closed error safe for operator output."""


class BrandAvatarClient(Protocol):
    async def connect(self) -> Any: ...

    async def disconnect(self) -> Any: ...

    async def is_user_authorized(self) -> bool: ...

    async def get_me(self) -> Any: ...

    async def get_dialogs(self, limit: int | None = None) -> Sequence[Any]: ...

    async def upload_file(self, file: Any, **kwargs: Any) -> Any: ...

    def __call__(self, request: Any) -> Awaitable[Any]: ...


ClientFactory = Callable[[TelegramCredentials], BrandAvatarClient]
LockFactory = Callable[..., TelegramSessionFileLock]


@dataclass(frozen=True)
class _TargetSpec:
    key: str
    peer_id: int
    expected_title: str
    kind: str
    filename: str


_TARGETS = (
    _TargetSpec(
        key="channel",
        peer_id=4_263_406_607,
        expected_title="Flowforge_AI | Автоматизация бизнеса",
        kind="channel",
        filename="flowforge-avatar-channel-v1.png",
    ),
    _TargetSpec(
        key="discussion",
        peer_id=4_499_724_585,
        expected_title="Flowforge AI | Обсуждения",
        kind="group",
        filename="flowforge-avatar-discussion-v1.png",
    ),
    _TargetSpec(
        key="project",
        peer_id=4_324_224_634,
        expected_title="Flowforge AI | Лаборатория",
        kind="group",
        filename="flowforge-avatar-project-v1.png",
    ),
    _TargetSpec(
        key="sales_radar_bot",
        peer_id=8_938_603_912,
        expected_title="AI_Sales_Radar_Bot",
        kind="bot",
        filename="flowforge-avatar-ai-sales-radar-v1.png",
    ),
)


@dataclass(frozen=True)
class BrandAvatarTarget:
    key: str
    peer_id: int
    title: str
    kind: str
    filename: str
    file_size: int
    current_photo_id: int | None
    owned: bool
    would_update: bool
    updated: bool = False
    verified: bool = False
    new_photo_id: int | None = None


@dataclass(frozen=True)
class BrandAvatarReport:
    mode: str
    account_id: int
    suite: str
    target_count: int
    planned_update_count: int
    updated_count: int
    blocked_reasons: tuple[str, ...]
    targets: tuple[BrandAvatarTarget, ...]
    verified: bool


@dataclass(frozen=True)
class _ResolvedTarget:
    spec: _TargetSpec
    entity: Any
    peer: Any
    path: Path
    report: BrandAvatarTarget


class BrandAvatarService:
    def __init__(
        self,
        credentials: TelegramCredentials,
        *,
        client_factory: ClientFactory | None = None,
        lock_factory: LockFactory = TelegramSessionFileLock,
    ) -> None:
        self._credentials = credentials
        self._client_factory = client_factory or _default_client_factory
        self._lock_factory = lock_factory

    async def plan(self) -> BrandAvatarReport:
        async def operation(client: BrandAvatarClient) -> BrandAvatarReport:
            return (await self._resolve(client, mode="plan"))[0]

        return await self._with_client(operation)

    async def apply(
        self,
        *,
        confirm_account_id: int,
        confirm_suite: str,
        only_keys: tuple[str, ...] = (),
    ) -> BrandAvatarReport:
        if confirm_account_id != FLOWFORGE_OWNER_ID:
            raise BrandAvatarError("Подтверждение Telegram account ID не совпало")
        if confirm_suite != FLOWFORGE_AVATAR_SUITE:
            raise BrandAvatarError("Подтверждение комплекта аватаров не совпало")
        allowed_keys = {item.key for item in _TARGETS}
        selected_keys = set(only_keys) if only_keys else allowed_keys
        if not selected_keys or not selected_keys.issubset(allowed_keys):
            raise BrandAvatarError("Некорректный список целей комплекта")

        async def operation(client: BrandAvatarClient) -> BrandAvatarReport:
            report, targets = await self._resolve(client, mode="apply")
            if report.blocked_reasons:
                raise BrandAvatarError(
                    "Операция заблокирована: " + ", ".join(report.blocked_reasons)
                )

            selected_targets = tuple(
                target for target in targets if target.spec.key in selected_keys
            )
            if len(selected_targets) != len(selected_keys):
                raise BrandAvatarError("Не все выбранные цели прошли проверку")
            completed: list[BrandAvatarTarget] = []
            for target in selected_targets:
                uploaded = await client.upload_file(
                    str(target.path),
                    file_name=target.path.name,
                )
                if target.spec.kind == "bot":
                    await client(
                        functions.photos.UploadProfilePhotoRequest(
                            bot=utils.get_input_user(target.entity),
                            file=uploaded,
                        )
                    )
                else:
                    await client(
                        functions.channels.EditPhotoRequest(
                            channel=utils.get_input_channel(target.entity),
                            photo=types.InputChatUploadedPhoto(file=uploaded),
                        )
                    )

                new_photo_id = await _fresh_photo_id(client, target)
                if new_photo_id is None or new_photo_id == target.report.current_photo_id:
                    changed = ", ".join(item.key for item in completed) or "нет"
                    raise BrandAvatarError(
                        f"Telegram не подтвердил новый аватар {target.spec.key}; "
                        f"уже обновлено: {changed}"
                    )
                completed.append(
                    replace(
                        target.report,
                        would_update=False,
                        updated=True,
                        verified=True,
                        new_photo_id=new_photo_id,
                    )
                )

            return BrandAvatarReport(
                mode="apply",
                account_id=FLOWFORGE_OWNER_ID,
                suite=FLOWFORGE_AVATAR_SUITE,
                target_count=len(selected_targets),
                planned_update_count=0,
                updated_count=len(completed),
                blocked_reasons=(),
                targets=tuple(completed),
                verified=len(completed) == len(selected_targets),
            )

        return await self._with_client(operation)

    async def _resolve(
        self,
        client: BrandAvatarClient,
        *,
        mode: str,
    ) -> tuple[BrandAvatarReport, tuple[_ResolvedTarget, ...]]:
        dialogs = await client.get_dialogs(limit=None)
        by_id: dict[int, list[Any]] = {}
        for dialog in dialogs:
            entity_id = getattr(getattr(dialog, "entity", None), "id", None)
            if isinstance(entity_id, int) and not isinstance(entity_id, bool):
                by_id.setdefault(entity_id, []).append(dialog)

        blocked: list[str] = []
        resolved: list[_ResolvedTarget] = []
        reports: list[BrandAvatarTarget] = []
        for spec in _TARGETS:
            matches = by_id.get(spec.peer_id, [])
            if len(matches) != 1:
                blocked.append(f"missing_or_duplicate:{spec.key}")
                continue
            dialog = matches[0]
            entity = getattr(dialog, "entity", None)
            peer = getattr(dialog, "input_entity", None)
            owned = _is_exact_owned_target(entity, peer, spec)
            if not owned:
                blocked.append(f"identity_or_ownership:{spec.key}")
                continue
            path = _avatar_path(_BRAND_ROOT / spec.filename)
            title = _entity_title(entity)
            item = BrandAvatarTarget(
                key=spec.key,
                peer_id=spec.peer_id,
                title=title,
                kind=spec.kind,
                filename=path.name,
                file_size=path.stat().st_size,
                current_photo_id=_photo_id(entity),
                owned=True,
                would_update=True,
            )
            reports.append(item)
            resolved.append(
                _ResolvedTarget(
                    spec=spec,
                    entity=entity,
                    peer=peer,
                    path=path,
                    report=item,
                )
            )

        report = BrandAvatarReport(
            mode=mode,
            account_id=FLOWFORGE_OWNER_ID,
            suite=FLOWFORGE_AVATAR_SUITE,
            target_count=len(reports),
            planned_update_count=len(reports) if not blocked else 0,
            updated_count=0,
            blocked_reasons=tuple(blocked),
            targets=tuple(reports),
            verified=False,
        )
        return report, tuple(resolved)

    async def _with_client(
        self,
        operation: Callable[[BrandAvatarClient], Awaitable[Any]],
    ) -> Any:
        path = self._credentials.session_path
        prepare_private_session_storage(path)
        protect_session_files(path)
        lock = self._lock_factory(path, role="ai_sales_radar_brand_avatars")
        lock.acquire()
        client: BrandAvatarClient | None = None
        try:
            with private_umask():
                client = self._client_factory(self._credentials)
            await client.connect()
            if not await client.is_user_authorized():
                raise BrandAvatarError("Telegram user-session не авторизована")
            account = await client.get_me()
            if (
                getattr(account, "bot", None) is True
                or getattr(account, "id", None) != FLOWFORGE_OWNER_ID
            ):
                raise BrandAvatarError(
                    "Telegram-сессия не принадлежит владельцу Flowforge"
                )
            return await operation(client)
        except FloodWaitError as exc:
            raise BrandAvatarError(
                f"Telegram установил FloodWait: повторите через {max(0, int(exc.seconds))} сек."
            ) from None
        except RPCError as exc:
            raise BrandAvatarError(
                f"Telegram отклонил операцию ({type(exc).__name__})"
            ) from None
        finally:
            try:
                if client is not None:
                    await disconnect_client_fully(client)
            finally:
                protect_session_files(path)
                lock.release()


def _is_exact_owned_target(entity: Any, peer: Any, spec: _TargetSpec) -> bool:
    if spec.kind == "bot":
        return (
            isinstance(entity, types.User)
            and isinstance(peer, types.InputPeerUser)
            and entity.id == spec.peer_id
            and peer.user_id == spec.peer_id
            and entity.bot is True
            and entity.bot_can_edit is True
            and getattr(entity, "username", None) == spec.expected_title
        )
    if not (
        isinstance(entity, types.Channel)
        and isinstance(peer, types.InputPeerChannel)
        and entity.id == spec.peer_id
        and peer.channel_id == spec.peer_id
        and entity.creator is True
        and getattr(entity, "title", None) == spec.expected_title
    ):
        return False
    return (
        entity.broadcast is True
        if spec.kind == "channel"
        else entity.megagroup is True
    )


def _avatar_path(path: Path) -> Path:
    source = path.absolute()
    try:
        mode = os.lstat(source).st_mode
    except FileNotFoundError:
        raise BrandAvatarError(f"Файл аватара не найден: {path.name}") from None
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise BrandAvatarError(f"Аватар должен быть обычным файлом: {path.name}")
    resolved = source.resolve(strict=True)
    size = resolved.stat().st_size
    if size <= 0 or size > MAX_AVATAR_BYTES:
        raise BrandAvatarError(f"Некорректный размер аватара: {path.name}")
    if resolved.suffix.lower() not in {".jpg", ".jpeg", ".png", ".webp"}:
        raise BrandAvatarError(f"Некорректный формат аватара: {path.name}")
    return resolved


def _photo_id(entity: Any) -> int | None:
    value = getattr(getattr(entity, "photo", None), "photo_id", None)
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _entity_title(entity: Any) -> str:
    if isinstance(entity, types.User):
        username = getattr(entity, "username", None)
        return f"@{username}" if isinstance(username, str) else str(entity.id)
    title = getattr(entity, "title", None)
    return title if isinstance(title, str) else str(entity.id)


async def _fresh_photo_id(
    client: BrandAvatarClient,
    target: _ResolvedTarget,
) -> int | None:
    dialogs = await client.get_dialogs(limit=None)
    matches = [
        getattr(dialog, "entity", None)
        for dialog in dialogs
        if getattr(getattr(dialog, "entity", None), "id", None)
        == target.spec.peer_id
    ]
    if len(matches) != 1:
        raise BrandAvatarError(
            f"Telegram не вернул точную цель после обновления: {target.spec.key}"
        )
    return _photo_id(matches[0])


def _default_client_factory(credentials: TelegramCredentials) -> BrandAvatarClient:
    return TelegramClient(
        str(credentials.session_path),
        credentials.api_id,
        credentials.api_hash,
        flood_sleep_threshold=0,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Безопасная установка комплекта аватаров Flowforge AI",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("plan", help="проверить точные цели без изменений")
    apply = commands.add_parser("apply", help="установить четыре аватара")
    apply.add_argument("--confirm-account-id", type=int, required=True)
    apply.add_argument("--confirm-suite", required=True)
    apply.add_argument(
        "--only",
        action="append",
        choices=tuple(item.key for item in _TARGETS),
        default=[],
        help="обновить только выбранную точную цель; можно повторять",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        credentials = TelegramAuthSettings.from_env().require_credentials()
        service = BrandAvatarService(credentials)
        if args.command == "plan":
            report = asyncio.run(service.plan())
        else:
            report = asyncio.run(
                service.apply(
                    confirm_account_id=args.confirm_account_id,
                    confirm_suite=args.confirm_suite,
                    only_keys=tuple(args.only),
                )
            )
        print(json.dumps(asdict(report), ensure_ascii=False, sort_keys=True))
        return 0
    except (ConfigurationError, BrandAvatarError, OSError) as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 2
    except TelegramSessionInUseError:
        print("Ошибка: Telegram-сессия уже используется другим процессом.", file=sys.stderr)
        return 3
    except Exception as exc:
        print(
            "Операция с аватарами не выполнена. Подробности скрыты "
            f"({type(exc).__name__}).",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "BrandAvatarError",
    "BrandAvatarReport",
    "BrandAvatarService",
    "BrandAvatarTarget",
    "FLOWFORGE_AVATAR_SUITE",
    "FLOWFORGE_OWNER_ID",
    "build_parser",
    "main",
]

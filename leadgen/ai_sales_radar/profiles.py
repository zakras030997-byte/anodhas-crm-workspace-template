"""Local, side-effect-free Search Profile management for AI Sales Radar."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
import json
from pathlib import Path
import sys
from typing import Any
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection

from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.schema import search_profiles, users
from freelancer_bot.persistence.search_profiles import (
    SearchProfileActivationConflict,
    SearchProfileActivationError,
    SearchProfileConflict,
    SearchProfileConfirmationStatus,
    SearchProfileEditConflict,
    SearchProfileNotFound,
    SearchProfileOwnershipError,
    SearchProfileRecord,
    SearchProfileRepository,
    UserNotFound,
    UserRepository,
)
from freelancer_bot.search_profiles import (
    BudgetPolicy,
    OpportunityType,
    SearchProfilePreferences,
    WorkMode,
    empty_search_profile_preferences,
    parse_search_profile,
    parse_search_profile_preferences,
)

from .config import ConfigurationError, DatabaseSettings


LOCAL_USER_PLATFORM = "local"
LOCAL_USER_EXTERNAL_ID = "ai-studio"


class ProfilesError(RuntimeError):
    """A profile error whose message is safe to show to the local operator."""


@dataclass(frozen=True)
class ProfileCreateResult:
    user_created: bool
    profile_created: bool
    profile: SearchProfileRecord


@dataclass(frozen=True)
class ProfileListResult:
    count: int
    profiles: tuple[SearchProfileRecord, ...]


@dataclass(frozen=True)
class LocalProfileActivationResult:
    changed: bool
    profile: SearchProfileRecord
    trial_started: bool = False
    jobs_enqueued: int = 0


class LocalProfileStateRepository:
    """Activate one primary profile without trial, billing, jobs or discovery."""

    def __init__(self, profiles: SearchProfileRepository | None = None) -> None:
        self._profiles = profiles or SearchProfileRepository()

    async def activate_primary(
        self,
        connection: AsyncConnection,
        *,
        profile_id: UUID,
        user_id: UUID,
        expected_revision: int,
    ) -> LocalProfileActivationResult:
        if expected_revision < 1:
            raise ValueError("expected_revision must be positive")

        locked_user_id = await connection.scalar(
            sa.select(users.c.id).where(users.c.id == user_id).with_for_update()
        )
        if locked_user_id is None:
            raise UserNotFound("Local AI Sales Radar user does not exist")

        current = await self._profiles.get(connection, profile_id)
        if current.user_id != user_id:
            raise SearchProfileOwnershipError(
                "search profile belongs to another user"
            )
        if current.is_active and current.is_primary:
            await self._deactivate_other_profiles(
                connection,
                user_id=user_id,
                profile_id=profile_id,
            )
            return LocalProfileActivationResult(changed=False, profile=current)
        if (
            current.confirmation_status
            is not SearchProfileConfirmationStatus.CONFIRMED
        ):
            raise SearchProfileActivationError(
                "search profile must be confirmed before activation"
            )
        if not (current.roles or current.skills or current.categories):
            raise SearchProfileActivationError(
                "search profile must contain a role, skill or category"
            )
        if current.revision != expected_revision:
            raise SearchProfileActivationConflict(
                "search profile changed before activation"
            )

        await self._deactivate_other_profiles(
            connection,
            user_id=user_id,
            profile_id=profile_id,
        )
        activated_id = await connection.scalar(
            sa.update(search_profiles)
            .where(
                search_profiles.c.id == profile_id,
                search_profiles.c.user_id == user_id,
                search_profiles.c.confirmation_status
                == SearchProfileConfirmationStatus.CONFIRMED.value,
                search_profiles.c.revision == expected_revision,
            )
            .values(
                is_active=True,
                is_primary=True,
                activated_at=sa.func.coalesce(
                    search_profiles.c.activated_at,
                    sa.func.now(),
                ),
                deactivated_at=None,
                revision=search_profiles.c.revision + 1,
                updated_at=sa.func.now(),
            )
            .returning(search_profiles.c.id)
        )
        if activated_id is None:
            raise SearchProfileActivationConflict(
                "search profile changed before activation"
            )
        activated = await self._profiles.get(connection, profile_id)
        return LocalProfileActivationResult(changed=True, profile=activated)

    async def _deactivate_other_profiles(
        self,
        connection: AsyncConnection,
        *,
        user_id: UUID,
        profile_id: UUID,
    ) -> None:
        await connection.execute(
            sa.update(search_profiles)
            .where(
                search_profiles.c.user_id == user_id,
                search_profiles.c.id != profile_id,
                sa.or_(
                    search_profiles.c.is_active.is_(True),
                    search_profiles.c.is_primary.is_(True),
                ),
            )
            .values(
                is_active=False,
                is_primary=False,
                deactivated_at=sa.func.now(),
                revision=search_profiles.c.revision + 1,
                updated_at=sa.func.now(),
            )
        )


class LocalProfilesService:
    """One-user profile service with no Telegram or external API dependencies."""

    def __init__(
        self,
        database: Database,
        *,
        users_repository: UserRepository | None = None,
        profiles_repository: SearchProfileRepository | None = None,
        state_repository: LocalProfileStateRepository | None = None,
    ) -> None:
        self._database = database
        self._users = users_repository or UserRepository()
        self._profiles = profiles_repository or SearchProfileRepository()
        self._state = state_repository or LocalProfileStateRepository(self._profiles)

    async def create_profile(
        self,
        *,
        semantic_text: str,
        roles: Sequence[str],
        skills: Sequence[str],
        categories: Sequence[str],
        preferences: SearchProfilePreferences | None = None,
    ) -> ProfileCreateResult:
        parsed = parse_search_profile(
            roles=roles,
            skills=skills,
            categories=categories,
            semantic_text=semantic_text,
        )
        if not (parsed.roles or parsed.skills or parsed.categories):
            raise ProfilesError("Нужна хотя бы одна роль, компетенция или категория")
        selected_preferences = preferences or empty_search_profile_preferences()

        async with self._database.transaction() as connection:
            user = await self._users.ensure(
                connection,
                platform=LOCAL_USER_PLATFORM,
                external_user_id=LOCAL_USER_EXTERNAL_ID,
            )
            outcome = await self._profiles.create(
                connection,
                user_id=user.user.id,
                parsed_profile=parsed,
            )
            profile = outcome.profile
            if selected_preferences != empty_search_profile_preferences():
                profile = await self._profiles.update_preferences(
                    connection,
                    profile_id=profile.id,
                    user_id=user.user.id,
                    preferences=selected_preferences,
                    expected_revision=profile.revision,
                )
        return ProfileCreateResult(
            user_created=user.created,
            profile_created=outcome.created,
            profile=profile,
        )

    async def edit_profile(
        self,
        profile_id: UUID,
        *,
        expected_revision: int,
        roles: Sequence[str],
        skills: Sequence[str],
        categories: Sequence[str],
    ) -> SearchProfileRecord:
        async with self._database.transaction() as connection:
            user = await self._local_user(connection)
            current = await self._profiles.get(connection, profile_id)
            _require_owner(current, user.id)
            parsed = parse_search_profile(
                roles=roles,
                skills=skills,
                categories=categories,
                semantic_text=current.semantic_text_original,
            )
            if not (parsed.roles or parsed.skills or parsed.categories):
                raise ProfilesError(
                    "Нельзя удалить из профиля все роли, компетенции и категории"
                )
            return await self._profiles.edit(
                connection,
                profile_id=profile_id,
                user_id=user.id,
                parsed_profile=parsed,
                expected_revision=expected_revision,
            )

    async def list_profiles(self) -> ProfileListResult:
        async with self._database.connect() as connection:
            try:
                user = await self._local_user(connection)
            except UserNotFound:
                return ProfileListResult(count=0, profiles=())
            profiles = await self._profiles.list_for_user(
                connection,
                user_id=user.id,
            )
        return ProfileListResult(count=len(profiles), profiles=tuple(profiles))

    async def show_profile(self, profile_id: UUID) -> SearchProfileRecord:
        async with self._database.connect() as connection:
            user = await self._local_user(connection)
            profile = await self._profiles.get(connection, profile_id)
            _require_owner(profile, user.id)
            return profile

    async def confirm_profile(
        self,
        profile_id: UUID,
        *,
        expected_revision: int,
    ) -> SearchProfileRecord:
        async with self._database.transaction() as connection:
            user = await self._local_user(connection)
            return await self._profiles.confirm(
                connection,
                profile_id=profile_id,
                user_id=user.id,
                expected_revision=expected_revision,
            )

    async def activate_profile(
        self,
        profile_id: UUID,
        *,
        expected_revision: int,
    ) -> LocalProfileActivationResult:
        async with self._database.transaction() as connection:
            user = await self._local_user(connection)
            return await self._state.activate_primary(
                connection,
                profile_id=profile_id,
                user_id=user.id,
                expected_revision=expected_revision,
            )

    async def _local_user(self, connection: AsyncConnection):
        return await self._users.get_by_identity(
            connection,
            platform=LOCAL_USER_PLATFORM,
            external_user_id=LOCAL_USER_EXTERNAL_ID,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Локальные профили поиска AI Sales Radar",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    create = commands.add_parser("create", help="создать ручной draft-профиль")
    create.add_argument("--semantic-text", required=True)
    _add_term_arguments(create)
    _add_preference_arguments(create)

    edit = commands.add_parser(
        "edit",
        help="заменить роли, компетенции и категории draft-профиля",
    )
    edit.add_argument("profile_id", type=UUID)
    edit.add_argument("--revision", type=int, required=True)
    _add_term_arguments(edit)

    commands.add_parser("list", help="показать локальные профили")

    show = commands.add_parser("show", help="показать один локальный профиль")
    show.add_argument("profile_id", type=UUID)

    confirm = commands.add_parser("confirm", help="подтвердить draft-профиль")
    confirm.add_argument("profile_id", type=UUID)
    confirm.add_argument("--revision", type=int, required=True)

    activate = commands.add_parser(
        "activate",
        help="сделать подтверждённый профиль локальным primary",
    )
    activate.add_argument("profile_id", type=UUID)
    activate.add_argument("--revision", type=int, required=True)
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
        ProfilesError,
        SearchProfileActivationError,
        SearchProfileConflict,
        SearchProfileEditConflict,
        SearchProfileNotFound,
        SearchProfileOwnershipError,
        UserNotFound,
        TypeError,
        ValueError,
    ) as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(
            "Операция с профилями не выполнена. Подробности скрыты для защиты "
            f"секретов ({type(exc).__name__}).",
            file=sys.stderr,
        )
        return 1


async def _execute(args: argparse.Namespace, *, database_url: str) -> Any:
    database = Database(database_url, pool_size=2, max_overflow=0)
    try:
        service = LocalProfilesService(database)
        if args.command == "create":
            return await service.create_profile(
                semantic_text=args.semantic_text,
                roles=tuple(args.role),
                skills=tuple(args.skill),
                categories=tuple(args.category),
                preferences=_preferences_from_arguments(args),
            )
        if args.command == "edit":
            return await service.edit_profile(
                args.profile_id,
                expected_revision=args.revision,
                roles=tuple(args.role),
                skills=tuple(args.skill),
                categories=tuple(args.category),
            )
        if args.command == "list":
            return await service.list_profiles()
        if args.command == "show":
            return await service.show_profile(args.profile_id)
        if args.command == "confirm":
            return await service.confirm_profile(
                args.profile_id,
                expected_revision=args.revision,
            )
        if args.command == "activate":
            return await service.activate_profile(
                args.profile_id,
                expected_revision=args.revision,
            )
        raise ProfilesError("Неизвестная команда профилей")
    finally:
        await database.close()


def _add_term_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--role", action="append", default=[])
    parser.add_argument("--skill", action="append", default=[])
    parser.add_argument("--category", action="append", default=[])


def _add_preference_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--work-type",
        action="append",
        choices=tuple(item.value for item in OpportunityType),
    )
    parser.add_argument("--minimum-budget")
    parser.add_argument("--currency")
    parser.add_argument(
        "--budget-policy",
        choices=tuple(item.value for item in BudgetPolicy),
    )
    parser.add_argument("--language", action="append")
    parser.add_argument("--geography", action="append")
    parser.add_argument(
        "--work-mode",
        action="append",
        choices=tuple(item.value for item in WorkMode),
    )
    parser.add_argument("--exclude-category", action="append")


def _preferences_from_arguments(args: argparse.Namespace) -> SearchProfilePreferences:
    return parse_search_profile_preferences(
        work_types=args.work_type,
        minimum_budget=args.minimum_budget,
        currency=args.currency,
        budget_policy=args.budget_policy,
        languages=args.language,
        geographies=args.geography,
        work_modes=args.work_mode,
        excluded_categories=args.exclude_category,
    )


def _require_owner(profile: SearchProfileRecord, user_id: UUID) -> None:
    if profile.user_id != user_id:
        raise SearchProfileOwnershipError("search profile belongs to another user")


def _emit(value: Any) -> None:
    print(json.dumps(_jsonable(value), ensure_ascii=False, indent=2, sort_keys=True))


def _jsonable(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _jsonable(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return str(value)
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

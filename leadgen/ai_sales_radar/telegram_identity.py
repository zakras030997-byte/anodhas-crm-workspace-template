"""Stable Telegram source identity bindings shared by local runtime services.

Public Telegram usernames are mutable.  A source is therefore trusted only
when a validation row for the current collector account binds it to a positive
``peer:<id>`` identity and the entity returned by Telegram still matches that
binding.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import re
from typing import Any, Protocol
from urllib.parse import urlparse

import sqlalchemy as sa
from telethon import types

from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.schema import telegram_source_validations


TRUSTED_TELEGRAM_VALIDATION_STATES = frozenset({"accessible", "approved"})
_CANONICAL_PEER_RE = re.compile(r"peer:([1-9][0-9]*)")
_USERNAME_RE = re.compile(r"[A-Za-z0-9_]{1,64}")


class TelegramIdentityBindingError(RuntimeError):
    """Invalid identity-loader input safe to expose to a local operator."""


@dataclass(frozen=True)
class TelegramSourceIdentityBinding:
    source_id: int
    collector_account_id: int
    state: str
    canonical_peer_id: int | None


class TelegramIdentityBindingLoader(Protocol):
    async def load(
        self,
        *,
        source_ids: Sequence[int],
        collector_account_id: int,
    ) -> Mapping[int, TelegramSourceIdentityBinding]: ...


class PostgresTelegramIdentityBindingLoader:
    """Load validation bindings for only the selected sources and account."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def load(
        self,
        *,
        source_ids: Sequence[int],
        collector_account_id: int,
    ) -> Mapping[int, TelegramSourceIdentityBinding]:
        if not _is_positive_int(collector_account_id) or any(
            not _is_positive_int(source_id) for source_id in source_ids
        ):
            raise TelegramIdentityBindingError(
                "Некорректный запрос identity-привязок Telegram"
            )
        selected_ids = tuple(dict.fromkeys(source_ids))
        if not selected_ids:
            return {}

        async with self._database.connect() as connection:
            rows = (
                await connection.execute(
                    sa.select(
                        telegram_source_validations.c.source_id,
                        telegram_source_validations.c.collector_account_id,
                        telegram_source_validations.c.state,
                        telegram_source_validations.c.canonical_peer_identity,
                    ).where(
                        telegram_source_validations.c.collector_account_id
                        == collector_account_id,
                        telegram_source_validations.c.source_id.in_(selected_ids),
                    )
                )
            ).mappings().all()

        return {
            int(row["source_id"]): TelegramSourceIdentityBinding(
                source_id=int(row["source_id"]),
                collector_account_id=int(row["collector_account_id"]),
                state=str(row["state"]),
                canonical_peer_id=parse_canonical_peer_id(
                    row["canonical_peer_identity"]
                ),
            )
            for row in rows
        }


class TelegramEntityIdentityVerifier(Protocol):
    def verify(
        self,
        *,
        lookup: str,
        entity: Any,
        binding: TelegramSourceIdentityBinding,
    ) -> Any | None: ...


class StrictTelegramChannelIdentityVerifier:
    """Return an immutable channel input peer only after strict verification."""

    def verify(
        self,
        *,
        lookup: str,
        entity: Any,
        binding: TelegramSourceIdentityBinding,
    ) -> types.InputChannel | None:
        expected_username = lookup_username(lookup)
        entity_id = getattr(entity, "id", None)
        access_hash = getattr(entity, "access_hash", None)
        if (
            expected_username is None
            or not isinstance(entity, types.Channel)
            or not _is_positive_int(entity_id)
            or entity_id != binding.canonical_peer_id
            or isinstance(access_hash, bool)
            or not isinstance(access_hash, int)
            or access_hash == 0
            or not (entity.broadcast is True or entity.megagroup is True)
            or entity.left is True
            or entity.scam is True
            or entity.fake is True
            or entity.restricted is True
            or expected_username not in active_entity_usernames(entity)
        ):
            return None
        return types.InputChannel(channel_id=entity_id, access_hash=access_hash)


def is_trusted_binding(
    binding: Any,
    *,
    source_id: int,
    collector_account_id: int,
) -> bool:
    return (
        isinstance(binding, TelegramSourceIdentityBinding)
        and _is_positive_int(source_id)
        and binding.source_id == source_id
        and binding.collector_account_id == collector_account_id
        and binding.state in TRUSTED_TELEGRAM_VALIDATION_STATES
        and _is_positive_int(binding.canonical_peer_id)
    )


def parse_canonical_peer_id(value: Any) -> int | None:
    if not isinstance(value, str):
        return None
    match = _CANONICAL_PEER_RE.fullmatch(value.strip())
    if match is None:
        return None
    return int(match.group(1))


def lookup_username(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    selected = value.strip()
    if selected.startswith("@"):
        return normalize_username(selected[1:])
    parsed = urlparse(selected)
    if parsed.scheme.lower() != "https" or parsed.netloc.lower() not in {
        "t.me",
        "telegram.me",
    }:
        return None
    path = parsed.path.strip("/").removeprefix("@")
    if "/" in path or parsed.query or parsed.fragment:
        return None
    return normalize_username(path)


def normalize_username(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    selected = value.strip()
    if not _USERNAME_RE.fullmatch(selected):
        return None
    return selected.casefold()


def active_entity_usernames(entity: Any) -> tuple[str, ...]:
    values: list[str] = []
    primary = normalize_username(getattr(entity, "username", None))
    if primary is not None:
        values.append(primary)
    for item in getattr(entity, "usernames", None) or ():
        # Telethon decodes an absent optional flag as None.  Only True proves
        # that a secondary username is currently active.
        if getattr(item, "active", None) is not True:
            continue
        username = normalize_username(getattr(item, "username", None))
        if username is not None and username not in values:
            values.append(username)
    return tuple(values)


def _is_positive_int(value: Any) -> bool:
    return not isinstance(value, bool) and isinstance(value, int) and value > 0

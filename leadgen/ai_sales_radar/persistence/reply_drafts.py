"""Append-only persistence for manually requested AI reply drafts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .schema import reply_drafts


_HASH = re.compile(r"^[0-9a-f]{64}$")
_VERSION = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_PROVIDER = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")


class ReplyDraftConflict(RuntimeError):
    pass


@dataclass(frozen=True)
class ReplyDraftRecord:
    id: UUID
    inbox_item_id: UUID
    match_trace_id: UUID
    opportunity_id: UUID
    search_profile_id: UUID
    profile_revision: int
    provider: str
    model: str
    prompt_version: str
    schema_version: str
    persona_sha256: str
    input_sha256: str
    idempotency_key: str
    result: Mapping[str, Any]
    created_at: datetime


class ReplyDraftRepository:
    async def ensure(
        self,
        connection: AsyncConnection,
        *,
        inbox_item_id: UUID,
        match_trace_id: UUID,
        opportunity_id: UUID,
        search_profile_id: UUID,
        profile_revision: int,
        provider: str,
        model: str,
        prompt_version: str,
        schema_version: str,
        persona_sha256: str,
        input_sha256: str,
        idempotency_key: str,
        result: Mapping[str, Any],
    ) -> ReplyDraftRecord:
        values = {
            "id": uuid4(),
            "inbox_item_id": inbox_item_id,
            "match_trace_id": match_trace_id,
            "opportunity_id": opportunity_id,
            "search_profile_id": search_profile_id,
            "profile_revision": _positive(profile_revision, "profile_revision"),
            "provider": _provider(provider),
            "model": _required(model, "model", 128),
            "prompt_version": _version(prompt_version, "prompt_version"),
            "schema_version": _version(schema_version, "schema_version"),
            "persona_sha256": _hash(persona_sha256, "persona_sha256"),
            "input_sha256": _hash(input_sha256, "input_sha256"),
            "idempotency_key": _hash(idempotency_key, "idempotency_key"),
            "result": dict(result),
        }
        inserted = await connection.scalar(
            pg_insert(reply_drafts)
            .values(**values)
            .on_conflict_do_nothing(
                constraint="uq_reply_drafts_idempotency_key"
            )
            .returning(reply_drafts.c.id)
        )
        row = (
            await connection.execute(
                sa.select(reply_drafts).where(
                    reply_drafts.c.idempotency_key == values["idempotency_key"]
                )
            )
        ).mappings().one()
        if inserted is None and any(
            row[key] != value
            for key, value in values.items()
            if key not in {"id", "result"}
        ):
            raise ReplyDraftConflict(
                "Reply draft idempotency key has another persisted result"
            )
        return _record(row)

    async def get(
        self,
        connection: AsyncConnection,
        draft_id: UUID,
    ) -> ReplyDraftRecord | None:
        row = (
            await connection.execute(
                sa.select(reply_drafts).where(reply_drafts.c.id == draft_id)
            )
        ).mappings().one_or_none()
        return None if row is None else _record(row)

    async def get_by_idempotency_key(
        self,
        connection: AsyncConnection,
        idempotency_key: str,
    ) -> ReplyDraftRecord | None:
        selected = _hash(idempotency_key, "idempotency_key")
        row = (
            await connection.execute(
                sa.select(reply_drafts).where(
                    reply_drafts.c.idempotency_key == selected
                )
            )
        ).mappings().one_or_none()
        return None if row is None else _record(row)

    async def list_for_inbox(
        self,
        connection: AsyncConnection,
        inbox_item_id: UUID,
        *,
        limit: int = 100,
    ) -> tuple[ReplyDraftRecord, ...]:
        if not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")
        rows = await connection.execute(
            sa.select(reply_drafts)
            .where(reply_drafts.c.inbox_item_id == inbox_item_id)
            .order_by(reply_drafts.c.created_at.desc(), reply_drafts.c.id)
            .limit(limit)
        )
        return tuple(_record(row) for row in rows.mappings())


def _record(row: Mapping[str, Any]) -> ReplyDraftRecord:
    return ReplyDraftRecord(
        id=row["id"],
        inbox_item_id=row["inbox_item_id"],
        match_trace_id=row["match_trace_id"],
        opportunity_id=row["opportunity_id"],
        search_profile_id=row["search_profile_id"],
        profile_revision=int(row["profile_revision"]),
        provider=row["provider"],
        model=row["model"],
        prompt_version=row["prompt_version"],
        schema_version=row["schema_version"],
        persona_sha256=row["persona_sha256"],
        input_sha256=row["input_sha256"],
        idempotency_key=row["idempotency_key"],
        result=dict(row["result"]),
        created_at=row["created_at"],
    )


def _hash(value: str, name: str) -> str:
    selected = value.strip().lower()
    if not _HASH.fullmatch(selected):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return selected


def _version(value: str, name: str) -> str:
    selected = value.strip()
    if not _VERSION.fullmatch(selected):
        raise ValueError(f"{name} is invalid")
    return selected


def _provider(value: str) -> str:
    selected = value.strip().lower()
    if not _PROVIDER.fullmatch(selected):
        raise ValueError("provider is invalid")
    return selected


def _required(value: str, name: str, limit: int) -> str:
    selected = value.strip()
    if not selected or len(selected) > limit:
        raise ValueError(f"{name} is invalid")
    return selected


def _positive(value: int, name: str) -> int:
    if isinstance(value, bool) or value < 1:
        raise ValueError(f"{name} must be positive")
    return value


__all__ = [
    "ReplyDraftConflict",
    "ReplyDraftRecord",
    "ReplyDraftRepository",
]

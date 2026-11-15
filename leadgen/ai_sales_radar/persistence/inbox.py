"""Persistence primitives for Commercial Score snapshots and the local inbox."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import re
from typing import Any
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .schema import (
    commercial_score_snapshots,
    lead_inbox_items,
    lead_inbox_status_events,
)


_HASH = re.compile(r"^[0-9a-f]{64}$")
_VERSION = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")


class InboxStatus(str, Enum):
    NEW = "new"
    OPENED = "opened"
    SHORTLISTED = "shortlisted"
    IGNORED = "ignored"
    REPLIED = "replied"


class InboxAvailability(str, Enum):
    AVAILABLE = "available"
    INELIGIBLE = "ineligible"
    INACTIVE = "inactive"


class CommercialScoreConflict(RuntimeError):
    pass


class LeadInboxStatusConflict(RuntimeError):
    pass


class LeadInboxItemNotFound(LookupError):
    pass


@dataclass(frozen=True)
class CommercialScoreSnapshotRecord:
    id: UUID
    opportunity_id: UUID
    policy_version: str
    input_sha256: str
    total_score: int
    band: str
    breakdown: Mapping[str, Any]
    created_at: datetime


@dataclass(frozen=True)
class InboxItemRecord:
    id: UUID
    search_profile_id: UUID
    opportunity_id: UUID
    latest_match_trace_id: UUID
    commercial_score_snapshot_id: UUID
    profile_revision: int
    workflow_status: InboxStatus
    availability: InboxAvailability
    revision: int
    first_matched_at: datetime
    last_matched_at: datetime
    commercial_score: int
    commercial_band: str
    commercial_breakdown: Mapping[str, Any]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class LeadInboxStatusEventRecord:
    id: UUID
    inbox_item_id: UUID
    from_status: InboxStatus
    to_status: InboxStatus
    actor_id: str
    reason: str
    idempotency_key: str
    result_revision: int
    created_at: datetime


class CommercialScoreSnapshotRepository:
    async def ensure(
        self,
        connection: AsyncConnection,
        *,
        opportunity_id: UUID,
        policy_version: str,
        input_sha256: str,
        total_score: int,
        band: str,
        breakdown: Mapping[str, Any],
    ) -> CommercialScoreSnapshotRecord:
        selected_version = _version(policy_version)
        selected_hash = _sha256(input_sha256, "input_sha256")
        selected_score = _bounded_score(total_score)
        selected_band = _band(band)
        if selected_band != _band_for_score(selected_score):
            raise ValueError("band does not match total_score")
        selected_breakdown = dict(breakdown)

        snapshot_id = uuid4()
        inserted = await connection.scalar(
            pg_insert(commercial_score_snapshots)
            .values(
                id=snapshot_id,
                opportunity_id=opportunity_id,
                policy_version=selected_version,
                input_sha256=selected_hash,
                total_score=selected_score,
                band=selected_band,
                breakdown=selected_breakdown,
            )
            .on_conflict_do_nothing(
                constraint=(
                    "uq_commercial_score_snapshots_opportunity_policy_input"
                )
            )
            .returning(commercial_score_snapshots.c.id)
        )
        row = (
            await connection.execute(
                sa.select(commercial_score_snapshots).where(
                    commercial_score_snapshots.c.opportunity_id == opportunity_id,
                    commercial_score_snapshots.c.policy_version == selected_version,
                    commercial_score_snapshots.c.input_sha256 == selected_hash,
                )
            )
        ).mappings().one()
        if inserted is None and (
            row["total_score"] != selected_score
            or row["band"] != selected_band
            or dict(row["breakdown"]) != selected_breakdown
        ):
            raise CommercialScoreConflict(
                "Commercial Score input already has a different persisted result"
            )
        return _commercial_score_record(row)

    async def get(
        self,
        connection: AsyncConnection,
        snapshot_id: UUID,
    ) -> CommercialScoreSnapshotRecord | None:
        row = (
            await connection.execute(
                sa.select(commercial_score_snapshots).where(
                    commercial_score_snapshots.c.id == snapshot_id
                )
            )
        ).mappings().one_or_none()
        return None if row is None else _commercial_score_record(row)


class LeadInboxRepository:
    async def apply_match(
        self,
        connection: AsyncConnection,
        *,
        match_trace_id: UUID,
        opportunity_id: UUID,
        search_profile_id: UUID,
        profile_revision: int,
        eligible: bool,
        opportunity_active: bool,
        matched_at: datetime,
        commercial_score_snapshot_id: UUID | None = None,
    ) -> InboxItemRecord | None:
        if profile_revision < 1:
            raise ValueError("profile_revision must be positive")
        selected_time = _aware(matched_at, "matched_at")
        availability = (
            InboxAvailability.INACTIVE
            if not opportunity_active
            else (
                InboxAvailability.AVAILABLE
                if eligible
                else InboxAvailability.INELIGIBLE
            )
        )

        # SELECT FOR UPDATE cannot lock a row that does not exist yet.  The
        # transaction-scoped pair lock makes concurrent first materialization
        # idempotent before either process attempts the unique insert.
        await connection.execute(
            sa.text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
            {
                "key": (
                    "ai-sales-radar-inbox:"
                    f"{search_profile_id}:{opportunity_id}"
                )
            },
        )

        existing_row = (
            await connection.execute(
                sa.select(lead_inbox_items)
                .where(
                    lead_inbox_items.c.search_profile_id == search_profile_id,
                    lead_inbox_items.c.opportunity_id == opportunity_id,
                )
                .with_for_update()
            )
        ).mappings().one_or_none()

        if existing_row is None:
            if availability is not InboxAvailability.AVAILABLE:
                return None
            if commercial_score_snapshot_id is None:
                raise ValueError(
                    "eligible inbox item requires a Commercial Score snapshot"
                )
            item_id = uuid4()
            await connection.execute(
                lead_inbox_items.insert().values(
                    id=item_id,
                    search_profile_id=search_profile_id,
                    opportunity_id=opportunity_id,
                    latest_match_trace_id=match_trace_id,
                    commercial_score_snapshot_id=commercial_score_snapshot_id,
                    profile_revision=profile_revision,
                    workflow_status=InboxStatus.NEW.value,
                    availability=availability.value,
                    revision=1,
                    first_matched_at=selected_time,
                    last_matched_at=selected_time,
                )
            )
            return await self.get(connection, item_id)

        existing = await self._get_from_base_row(connection, existing_row)
        if selected_time < existing.last_matched_at:
            return existing
        selected_snapshot = (
            commercial_score_snapshot_id
            if availability is InboxAvailability.AVAILABLE
            else existing.commercial_score_snapshot_id
        )
        if selected_snapshot is None:
            raise ValueError("inbox item cannot lose its Commercial Score snapshot")
        if (
            existing.latest_match_trace_id == match_trace_id
            and existing.profile_revision == profile_revision
            and existing.availability is availability
            and existing.commercial_score_snapshot_id == selected_snapshot
            and existing.last_matched_at == selected_time
        ):
            return existing

        await connection.execute(
            lead_inbox_items.update()
            .where(
                lead_inbox_items.c.id == existing.id,
                lead_inbox_items.c.revision == existing.revision,
            )
            .values(
                latest_match_trace_id=match_trace_id,
                commercial_score_snapshot_id=selected_snapshot,
                profile_revision=profile_revision,
                availability=availability.value,
                revision=existing.revision + 1,
                last_matched_at=selected_time,
                updated_at=sa.func.now(),
            )
        )
        updated = await self.get(connection, existing.id)
        if updated is None:
            raise LeadInboxItemNotFound("Lead Inbox item disappeared during rematch")
        return updated

    async def get(
        self,
        connection: AsyncConnection,
        item_id: UUID,
    ) -> InboxItemRecord | None:
        row = (
            await connection.execute(
                _item_select().where(lead_inbox_items.c.id == item_id)
            )
        ).mappings().one_or_none()
        return None if row is None else _inbox_item_record(row)

    async def get_by_pair(
        self,
        connection: AsyncConnection,
        *,
        search_profile_id: UUID,
        opportunity_id: UUID,
    ) -> InboxItemRecord | None:
        row = (
            await connection.execute(
                _item_select().where(
                    lead_inbox_items.c.search_profile_id == search_profile_id,
                    lead_inbox_items.c.opportunity_id == opportunity_id,
                )
            )
        ).mappings().one_or_none()
        return None if row is None else _inbox_item_record(row)

    async def list_items(
        self,
        connection: AsyncConnection,
        *,
        search_profile_id: UUID,
        status: InboxStatus | str | None = None,
        availability: InboxAvailability | str | None = None,
        band: str | None = None,
        limit: int = 100,
    ) -> tuple[InboxItemRecord, ...]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        statement = _item_select().where(
            lead_inbox_items.c.search_profile_id == search_profile_id
        )
        if status is not None:
            statement = statement.where(
                lead_inbox_items.c.workflow_status == InboxStatus(status).value
            )
        if availability is not None:
            statement = statement.where(
                lead_inbox_items.c.availability
                == InboxAvailability(availability).value
            )
        if band is not None:
            statement = statement.where(
                commercial_score_snapshots.c.band == _band(band)
            )
        rows = await connection.execute(
            statement.order_by(
                commercial_score_snapshots.c.total_score.desc(),
                lead_inbox_items.c.last_matched_at.desc(),
                lead_inbox_items.c.id,
            ).limit(limit)
        )
        return tuple(_inbox_item_record(row) for row in rows.mappings())

    async def list_opportunity_ids_for_profile(
        self,
        connection: AsyncConnection,
        search_profile_id: UUID,
    ) -> tuple[UUID, ...]:
        """Return every Opportunity already represented in this local Inbox."""

        values = await connection.execute(
            sa.select(lead_inbox_items.c.opportunity_id)
            .where(lead_inbox_items.c.search_profile_id == search_profile_id)
            .order_by(
                lead_inbox_items.c.last_matched_at.desc(),
                lead_inbox_items.c.opportunity_id,
            )
        )
        return tuple(values.scalars())

    async def update_status(
        self,
        connection: AsyncConnection,
        *,
        item_id: UUID,
        target: InboxStatus | str,
        expected_revision: int,
        actor_id: str,
        reason: str,
        idempotency_key: str,
    ) -> InboxItemRecord:
        selected_target = InboxStatus(target)
        selected_actor = _required_text(actor_id, "actor_id", 128)
        selected_reason = _required_text(reason, "reason")
        selected_key = _sha256(idempotency_key, "idempotency_key")
        if expected_revision < 1:
            raise ValueError("expected_revision must be positive")

        # Lock the item before reading the request event.  Concurrent retries
        # then observe the winner's committed event instead of failing on the
        # now-stale expected revision.
        row = (
            await connection.execute(
                sa.select(lead_inbox_items)
                .where(lead_inbox_items.c.id == item_id)
                .with_for_update()
            )
        ).mappings().one_or_none()
        if row is None:
            raise LeadInboxItemNotFound("Lead Inbox item does not exist")
        event_row = (
            await connection.execute(
                sa.select(lead_inbox_status_events).where(
                    lead_inbox_status_events.c.inbox_item_id == item_id,
                    lead_inbox_status_events.c.idempotency_key == selected_key,
                )
            )
        ).mappings().one_or_none()
        if event_row is not None:
            if event_row["to_status"] != selected_target.value:
                raise LeadInboxStatusConflict(
                    "Status idempotency key was used for another target"
                )
            item = await self.get(connection, item_id)
            if item is None:
                raise LeadInboxItemNotFound("Lead Inbox item does not exist")
            return item
        current = InboxStatus(row["workflow_status"])
        revision = int(row["revision"])
        if revision != expected_revision:
            raise LeadInboxStatusConflict(
                "Lead Inbox item revision changed; reload before updating"
            )
        if current is selected_target:
            item = await self.get(connection, item_id)
            if item is None:
                raise LeadInboxItemNotFound("Lead Inbox item does not exist")
            return item

        result_revision = revision + 1
        await connection.execute(
            lead_inbox_items.update()
            .where(
                lead_inbox_items.c.id == item_id,
                lead_inbox_items.c.revision == revision,
            )
            .values(
                workflow_status=selected_target.value,
                revision=result_revision,
                updated_at=sa.func.now(),
            )
        )
        await connection.execute(
            lead_inbox_status_events.insert().values(
                id=uuid4(),
                inbox_item_id=item_id,
                from_status=current.value,
                to_status=selected_target.value,
                actor_id=selected_actor,
                reason=selected_reason,
                idempotency_key=selected_key,
                result_revision=result_revision,
            )
        )
        item = await self.get(connection, item_id)
        if item is None:
            raise LeadInboxItemNotFound("Lead Inbox item disappeared after update")
        return item

    async def list_status_events(
        self,
        connection: AsyncConnection,
        item_id: UUID,
    ) -> tuple[LeadInboxStatusEventRecord, ...]:
        rows = await connection.execute(
            sa.select(lead_inbox_status_events)
            .where(lead_inbox_status_events.c.inbox_item_id == item_id)
            .order_by(
                lead_inbox_status_events.c.result_revision,
                lead_inbox_status_events.c.created_at,
                lead_inbox_status_events.c.id,
            )
        )
        return tuple(_status_event_record(row) for row in rows.mappings())

    async def _get_from_base_row(
        self,
        connection: AsyncConnection,
        row: Mapping[str, Any],
    ) -> InboxItemRecord:
        item = await self.get(connection, row["id"])
        if item is None:
            raise LeadInboxItemNotFound("Lead Inbox item disappeared")
        return item


def _item_select() -> sa.Select:
    return (
        sa.select(
            lead_inbox_items,
            commercial_score_snapshots.c.total_score.label(
                "commercial_score_total"
            ),
            commercial_score_snapshots.c.band.label("commercial_score_band"),
            commercial_score_snapshots.c.breakdown.label(
                "commercial_score_breakdown"
            ),
        )
        .select_from(
            lead_inbox_items.join(
                commercial_score_snapshots,
                commercial_score_snapshots.c.id
                == lead_inbox_items.c.commercial_score_snapshot_id,
            )
        )
    )


def _commercial_score_record(row: Mapping[str, Any]) -> CommercialScoreSnapshotRecord:
    return CommercialScoreSnapshotRecord(
        id=row["id"],
        opportunity_id=row["opportunity_id"],
        policy_version=row["policy_version"],
        input_sha256=row["input_sha256"],
        total_score=int(row["total_score"]),
        band=row["band"],
        breakdown=dict(row["breakdown"]),
        created_at=row["created_at"],
    )


def _inbox_item_record(row: Mapping[str, Any]) -> InboxItemRecord:
    return InboxItemRecord(
        id=row["id"],
        search_profile_id=row["search_profile_id"],
        opportunity_id=row["opportunity_id"],
        latest_match_trace_id=row["latest_match_trace_id"],
        commercial_score_snapshot_id=row["commercial_score_snapshot_id"],
        profile_revision=int(row["profile_revision"]),
        workflow_status=InboxStatus(row["workflow_status"]),
        availability=InboxAvailability(row["availability"]),
        revision=int(row["revision"]),
        first_matched_at=row["first_matched_at"],
        last_matched_at=row["last_matched_at"],
        commercial_score=int(row["commercial_score_total"]),
        commercial_band=row["commercial_score_band"],
        commercial_breakdown=dict(row["commercial_score_breakdown"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _status_event_record(row: Mapping[str, Any]) -> LeadInboxStatusEventRecord:
    return LeadInboxStatusEventRecord(
        id=row["id"],
        inbox_item_id=row["inbox_item_id"],
        from_status=InboxStatus(row["from_status"]),
        to_status=InboxStatus(row["to_status"]),
        actor_id=row["actor_id"],
        reason=row["reason"],
        idempotency_key=row["idempotency_key"],
        result_revision=int(row["result_revision"]),
        created_at=row["created_at"],
    )


def _version(value: str) -> str:
    selected = value.strip()
    if not _VERSION.fullmatch(selected):
        raise ValueError("policy_version is invalid")
    return selected


def _sha256(value: str, name: str) -> str:
    selected = value.strip().lower()
    if not _HASH.fullmatch(selected):
        raise ValueError(f"{name} must be a lowercase SHA-256 hex digest")
    return selected


def _bounded_score(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 100:
        raise ValueError("total_score must be an integer between 0 and 100")
    return value


def _band(value: str) -> str:
    selected = value.strip().lower()
    if selected not in {"hot", "warm", "low"}:
        raise ValueError("band must be hot, warm or low")
    return selected


def _band_for_score(value: int) -> str:
    if value >= 80:
        return "hot"
    if value >= 60:
        return "warm"
    return "low"


def _required_text(value: str, name: str, limit: int | None = None) -> str:
    selected = value.strip()
    if not selected:
        raise ValueError(f"{name} must not be blank")
    if limit is not None and len(selected) > limit:
        raise ValueError(f"{name} is too long")
    return selected


def _aware(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")
    return value


__all__ = [
    "CommercialScoreConflict",
    "CommercialScoreSnapshotRecord",
    "CommercialScoreSnapshotRepository",
    "InboxAvailability",
    "InboxItemRecord",
    "InboxStatus",
    "LeadInboxItemNotFound",
    "LeadInboxRepository",
    "LeadInboxStatusConflict",
    "LeadInboxStatusEventRecord",
]

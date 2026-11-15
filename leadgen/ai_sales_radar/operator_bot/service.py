"""Application facade from the private operator UI to the proven core."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from decimal import Decimal
from typing import Protocol
from uuid import UUID

import sqlalchemy as sa

from freelancer_bot.lead_renderer import LeadCardRenderError, render_telegram_lead_card
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.schema import (
    discovery_results,
    discovery_runs,
    durable_jobs,
    source_profile_relevance,
    sources as source_rows,
)
from freelancer_bot.persistence.search_profiles import SearchProfileConfirmationStatus
from freelancer_bot.persistence.source_repository import SourceStatus

from ai_sales_radar.config import (
    OperatorSourceDiscoverySettings,
    ReplyDraftSettings,
)
from ai_sales_radar.inbox import LeadInboxService
from ai_sales_radar.persistence.inbox import (
    InboxAvailability,
    InboxStatus,
    LeadInboxItemNotFound,
    LeadInboxStatusConflict,
)
from ai_sales_radar.profiles import LocalProfilesService
from ai_sales_radar.readiness import inspect_readiness_database
from ai_sales_radar.reply_drafts import ReplyDraftService
from ai_sales_radar.source_catalog import SourceCatalogService
from ai_sales_radar.source_discovery import (
    OPERATOR_SOURCE_DISCOVERY_JOB_TYPE,
    OPERATOR_SOURCE_DISCOVERY_KEY_PREFIX,
    OPERATOR_SOURCE_DISCOVERY_PROVIDER,
    OperatorSourceDiscoveryBlocked,
    enqueue_source_discovery,
)

from .models import (
    OperatorBotActionConflict,
    OperatorBotError,
    OperatorDraft,
    OperatorDiscoveredSource,
    OperatorLead,
    OperatorSource,
    OperatorSourceDiscoveryReport,
    OperatorSourceDiscoveryStart,
    OperatorStatusSnapshot,
)


VISIBLE_LEAD_STATUSES = frozenset(
    {InboxStatus.NEW, InboxStatus.OPENED, InboxStatus.SHORTLISTED}
)


class OperatorCore(Protocol):
    async def status(self) -> OperatorStatusSnapshot: ...

    async def list_sources(
        self,
        *,
        status: str | None,
        limit: int,
    ) -> tuple[OperatorSource, ...]: ...

    async def list_leads(
        self,
        *,
        tier: str | None,
        include_closed: bool,
        limit: int,
    ) -> tuple[OperatorLead, ...]: ...

    async def show_lead(self, lead_id: UUID) -> OperatorLead: ...

    async def update_lead_status(
        self,
        lead_id: UUID,
        target: InboxStatus,
        *,
        expected_revision: int,
        actor_id: str,
        reason: str,
        request_id: str,
    ) -> OperatorLead: ...

    async def create_reply_draft(
        self,
        lead_id: UUID,
        *,
        expected_revision: int,
    ) -> OperatorDraft: ...

    async def start_source_discovery(self) -> OperatorSourceDiscoveryStart: ...

    async def latest_source_discovery(
        self,
    ) -> OperatorSourceDiscoveryReport | None: ...


class DatabaseOperatorCore:
    """Narrow, local-user-only facade. It never starts collector or workers."""

    def __init__(
        self,
        database: Database,
        reply_settings: ReplyDraftSettings,
        *,
        clock: Callable[[], datetime] | None = None,
        inbox: LeadInboxService | None = None,
        profiles: LocalProfilesService | None = None,
        sources: SourceCatalogService | None = None,
        drafts: ReplyDraftService | None = None,
        source_discovery_settings: OperatorSourceDiscoverySettings | None = None,
    ) -> None:
        self._database = database
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._inbox = inbox or LeadInboxService(database)
        self._profiles = profiles or LocalProfilesService(database)
        self._sources = sources or SourceCatalogService(database)
        self._drafts = drafts or ReplyDraftService(database, reply_settings)
        self._source_discovery_settings = (
            source_discovery_settings
            or OperatorSourceDiscoverySettings(_env_file=None)
        )

    async def status(self) -> OperatorStatusSnapshot:
        report = await inspect_readiness_database(
            self._database,
            now=self._aware_now(),
        )
        jobs_by_type: dict[str, dict[str, int]] = {}
        queued = running = failed = 0
        if report.jobs is not None:
            for job_type, counts in report.jobs.by_type.items():
                values = {
                    "queued": counts.queued,
                    "running": counts.running,
                    "completed": counts.completed,
                    "failed": counts.failed,
                }
                jobs_by_type[job_type] = values
                queued += counts.queued
                running += counts.running
                failed += counts.failed
        records = report.records
        spend = report.ai_spend
        profiles = report.profiles
        return OperatorStatusSnapshot(
            database_reachable=report.reachable,
            migration_current=report.migration_current,
            aggregates_readable=report.aggregates_readable,
            active_primary_profiles=(
                0 if profiles is None else profiles.active_primary_confirmed
            ),
            queued_jobs=queued,
            running_jobs=running,
            failed_jobs=failed,
            raw_messages=0 if records is None else records.raw_messages,
            opportunities=0 if records is None else records.opportunities,
            inbox_items=0 if records is None else records.inbox_items,
            today_ai_calls=0 if spend is None else spend.today_call_count,
            today_ai_spend_usd=(
                "0" if spend is None else spend.today_estimated_usd
            ),
            month_ai_calls=0 if spend is None else spend.month_call_count,
            month_ai_spend_usd=(
                "0" if spend is None else spend.month_estimated_usd
            ),
            jobs_by_type=jobs_by_type,
        )

    async def list_sources(
        self,
        *,
        status: str | None,
        limit: int,
    ) -> tuple[OperatorSource, ...]:
        selected_status = None if status is None else SourceStatus(status)
        result = await self._sources.list_sources(
            status=selected_status,
            platform="telegram",
            limit=limit,
        )
        return tuple(
            OperatorSource(
                source_id=item.id,
                display_name=item.display_name,
                handle=item.handle,
                status=item.lifecycle_status.value,
            )
            for item in result.sources
        )

    async def list_leads(
        self,
        *,
        tier: str | None,
        include_closed: bool,
        limit: int,
    ) -> tuple[OperatorLead, ...]:
        profile = await self._require_active_primary_profile()
        if include_closed:
            records = await self._inbox.list_leads(
                profile_id=profile.id,
                availability=InboxAvailability.AVAILABLE,
                tier=tier,
                limit=limit,
            )
        else:
            active_records = []
            # Filter in PostgreSQL before LIMIT. Otherwise old ignored/replied
            # high-score rows could fill the window and hide active leads.
            for status in sorted(VISIBLE_LEAD_STATUSES, key=lambda item: item.value):
                active_records.extend(
                    await self._inbox.list_leads(
                        profile_id=profile.id,
                        status=status,
                        availability=InboxAvailability.AVAILABLE,
                        tier=tier,
                        limit=limit,
                    )
                )
            records = tuple(
                sorted(
                    active_records,
                    key=lambda item: (
                        -item.commercial_score,
                        -item.last_matched_at.timestamp(),
                        item.id.hex,
                    ),
                )[:limit]
            )
        leads: list[OperatorLead] = []
        for item in records:
            try:
                leads.append(await self._load_current_lead(item.id, profile.id))
            except OperatorBotActionConflict:
                continue
        return tuple(leads)

    async def show_lead(self, lead_id: UUID) -> OperatorLead:
        profile = await self._require_active_primary_profile()
        return await self._load_current_lead(lead_id, profile.id)

    async def update_lead_status(
        self,
        lead_id: UUID,
        target: InboxStatus,
        *,
        expected_revision: int,
        actor_id: str,
        reason: str,
        request_id: str,
    ) -> OperatorLead:
        current = await self.show_lead(lead_id)
        if current.revision != expected_revision:
            raise OperatorBotActionConflict(
                "Карточка устарела. Обновите список лидов."
            )
        try:
            updated = await self._inbox.update_status(
                lead_id,
                target,
                expected_revision=expected_revision,
                actor_id=actor_id,
                reason=reason,
                request_id=request_id,
            )
        except (LeadInboxStatusConflict, LeadInboxItemNotFound):
            raise OperatorBotActionConflict(
                "Карточка устарела. Обновите список лидов."
            ) from None
        profile = await self._require_active_primary_profile()
        return await self._load_current_lead(updated.id, profile.id)

    async def create_reply_draft(
        self,
        lead_id: UUID,
        *,
        expected_revision: int,
    ) -> OperatorDraft:
        current = await self.show_lead(lead_id)
        if current.revision != expected_revision:
            raise OperatorBotActionConflict(
                "Карточка устарела. Обновите список лидов."
            )
        record = await self._drafts.create(lead_id)
        result = record.result
        return OperatorDraft(
            lead_id=lead_id,
            proposal_draft=_required_result_text(result, "proposal_draft"),
            short_reply=_required_result_text(result, "short_reply"),
            questions_to_client=_result_items(result, "questions_to_client"),
            risks=_result_items(result, "risks"),
        )

    async def start_source_discovery(self) -> OperatorSourceDiscoveryStart:
        profile = await self._require_active_primary_profile()
        requested_at = self._aware_now()
        try:
            job_id = await enqueue_source_discovery(
                self._database,
                profile_id=profile.id,
                profile_revision=profile.revision,
                requested_at=requested_at,
                settings=self._source_discovery_settings,
            )
        except OperatorSourceDiscoveryBlocked as exc:
            raise OperatorBotError(str(exc)) from None
        return OperatorSourceDiscoveryStart(
            job_id=job_id,
            minimum_participants=(
                self._source_discovery_settings.minimum_participants
            ),
            cooldown_seconds=self._source_discovery_settings.cooldown_seconds,
        )

    async def latest_source_discovery(
        self,
    ) -> OperatorSourceDiscoveryReport | None:
        profile = await self._require_active_primary_profile()
        async with self._database.connect() as connection:
            job = (
                await connection.execute(
                    sa.select(durable_jobs)
                    .where(
                        durable_jobs.c.job_type
                        == OPERATOR_SOURCE_DISCOVERY_JOB_TYPE,
                        durable_jobs.c.idempotency_key.like(
                            f"{OPERATOR_SOURCE_DISCOVERY_KEY_PREFIX}:"
                            f"{profile.id}:r{profile.revision}:b%"
                        ),
                    )
                    .order_by(
                        durable_jobs.c.created_at.desc(),
                        durable_jobs.c.id.desc(),
                    )
                    .limit(1)
                )
            ).mappings().one_or_none()
            if job is None:
                return None
            run = (
                await connection.execute(
                    sa.select(discovery_runs).where(
                        discovery_runs.c.provider
                        == OPERATOR_SOURCE_DISCOVERY_PROVIDER,
                        discovery_runs.c.run_key == job["idempotency_key"],
                    )
                )
            ).mappings().one_or_none()
            if run is None:
                return OperatorSourceDiscoveryReport(
                    status=str(job["state"]),
                    requested_at=job["created_at"],
                    finished_at=job["failed_at"],
                    minimum_participants=(
                        self._source_discovery_settings.minimum_participants
                    ),
                    candidates=(),
                    failure_code=job["failure_code"],
                )
            rows = (
                await connection.execute(
                    sa.select(
                        discovery_results,
                        source_rows.c.lifecycle_status.label("source_status"),
                        source_profile_relevance.c.relevance_score,
                        source_profile_relevance.c.relevance_class,
                    )
                    .join(
                        source_rows,
                        source_rows.c.id == discovery_results.c.source_id,
                    )
                    .outerjoin(
                        source_profile_relevance,
                        sa.and_(
                            source_profile_relevance.c.source_id
                            == discovery_results.c.source_id,
                            source_profile_relevance.c.search_profile_id
                            == profile.id,
                            source_profile_relevance.c.profile_revision
                            == profile.revision,
                        ),
                    )
                    .where(discovery_results.c.run_id == run["id"])
                )
            ).mappings().all()
        candidates = tuple(
            sorted(
                (_discovered_source(row) for row in rows),
                key=lambda item: (
                    -Decimal(item.relevance_score),
                    -item.participants_count,
                    -item.query_hit_count,
                    item.source_id,
                ),
            )
        )
        return OperatorSourceDiscoveryReport(
            status=str(run["status"]),
            requested_at=job["created_at"],
            finished_at=run["finished_at"],
            minimum_participants=_minimum_participants(
                run["request"],
                fallback=self._source_discovery_settings.minimum_participants,
            ),
            candidates=candidates,
            failure_code=run["failure_code"],
        )

    async def _require_active_primary_profile(self):
        result = await self._profiles.list_profiles()
        selected = tuple(
            profile
            for profile in result.profiles
            if (
                profile.is_active
                and profile.is_primary
                and profile.confirmation_status
                is SearchProfileConfirmationStatus.CONFIRMED
            )
        )
        if len(selected) != 1:
            raise OperatorBotError(
                "Нужен ровно один активный подтверждённый профиль поиска."
            )
        return selected[0]

    async def _load_current_lead(
        self,
        lead_id: UUID,
        profile_id: UUID,
    ) -> OperatorLead:
        try:
            view = await self._inbox.show_lead(lead_id)
        except (LookupError, LeadInboxItemNotFound):
            raise OperatorBotActionConflict(
                "Лид недоступен. Обновите список."
            ) from None
        if (
            view.item.search_profile_id != profile_id
            or view.item.availability is not InboxAvailability.AVAILABLE
        ):
            raise OperatorBotActionConflict(
                "Лид недоступен. Обновите список."
            )
        try:
            card = render_telegram_lead_card(
                view.opportunity,
                view.match,
                rendered_at=self._aware_now(),
            )
        except LeadCardRenderError:
            raise OperatorBotActionConflict(
                "Карточка лида устарела. Запустите matching и обновите список."
            ) from None
        return OperatorLead(
            item_id=view.item.id,
            revision=view.item.revision,
            workflow_status=view.item.workflow_status.value,
            commercial_score=view.item.commercial_score,
            commercial_band=view.item.commercial_band,
            body_html=card.body_html,
            source_url=card.source_url,
        )

    def _aware_now(self) -> datetime:
        selected = self._clock()
        if selected.tzinfo is None or selected.utcoffset() is None:
            raise ValueError("operator bot clock must include a timezone")
        return selected.astimezone(timezone.utc)


def _required_result_text(result: Mapping[str, object], name: str) -> str:
    value = result.get(name)
    if not isinstance(value, str) or not value.strip():
        raise OperatorBotError("Сохранённый AI-черновик повреждён.")
    return value.strip()


def _result_items(result: Mapping[str, object], name: str) -> tuple[str, ...]:
    value = result.get(name, ())
    if not isinstance(value, (list, tuple)):
        raise OperatorBotError("Сохранённый AI-черновик повреждён.")
    items: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise OperatorBotError("Сохранённый AI-черновик повреждён.")
        items.append(item.strip())
    return tuple(items)


def _discovered_source(row: Mapping[str, object]) -> OperatorDiscoveredSource:
    context = row.get("context")
    values = context if isinstance(context, Mapping) else {}
    participants = values.get("participants_count")
    query_hits = values.get("query_hit_count", 0)
    message_hits = values.get("message_hit_count", 0)
    relevance = row.get("relevance_score")
    url = row.get("canonical_url")
    safe_url = (
        str(url)
        if isinstance(url, str) and url.startswith("https://t.me/")
        else None
    )
    return OperatorDiscoveredSource(
        source_id=int(row["source_id"]),
        display_name=str(row["display_name"]),
        handle=None if row.get("handle") is None else str(row["handle"]),
        canonical_url=safe_url,
        status=str(row["source_status"]),
        peer_type=str(values.get("source_kind") or "community"),
        participants_count=(
            int(participants)
            if isinstance(participants, int) and not isinstance(participants, bool)
            else 0
        ),
        relevance_score=(
            "0.00" if relevance is None else f"{Decimal(relevance):.2f}"
        ),
        relevance_class=str(row.get("relevance_class") or "weak"),
        query_hit_count=(
            int(query_hits)
            if isinstance(query_hits, int) and not isinstance(query_hits, bool)
            else 0
        ),
        message_hit_count=(
            int(message_hits)
            if isinstance(message_hits, int) and not isinstance(message_hits, bool)
            else 0
        ),
    )


def _minimum_participants(
    request: object,
    *,
    fallback: int,
) -> int:
    if not isinstance(request, Mapping):
        return fallback
    parameters = request.get("parameters")
    if not isinstance(parameters, Mapping):
        return fallback
    profile_discovery = parameters.get("profile_discovery")
    if not isinstance(profile_discovery, Mapping):
        return fallback
    value = profile_discovery.get("minimum_participants")
    return (
        int(value)
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0
        else fallback
    )

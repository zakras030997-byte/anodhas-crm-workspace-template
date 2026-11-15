"""Channel-neutral matching materialization and Lead Inbox services."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from hashlib import sha256
import json
from uuid import UUID

import sqlalchemy as sa

from freelancer_bot.match_decisions import MatchTraceDraft
from freelancer_bot.matching_service import (
    CandidateMatchingService,
    MatchGenerationOutcome,
)
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.matches import MatchTraceRecord, MatchTraceRepository
from freelancer_bot.persistence.opportunities import (
    CanonicalOpportunityRecord,
    CanonicalOpportunityRepository,
    OpportunityNotFound,
)
from freelancer_bot.persistence.search_profiles import (
    SearchProfileConfirmationStatus,
    SearchProfileNotFound,
    SearchProfileRecord,
    SearchProfileRepository,
    UserNotFound,
    UserRepository,
)
from freelancer_bot.persistence.schema import search_profiles

from .commercial_score import (
    CommercialIntent,
    CommercialScore,
    CommercialScoreInput,
    CommercialUrgency,
    Contactability,
    calculate_commercial_score,
)
from .persistence.inbox import (
    CommercialScoreSnapshotRepository,
    InboxAvailability,
    InboxItemRecord,
    InboxStatus,
    LeadInboxRepository,
    LeadInboxStatusEventRecord,
)
from .profiles import LOCAL_USER_EXTERNAL_ID, LOCAL_USER_PLATFORM


@dataclass(frozen=True)
class InboxMaterializationReport:
    trace_count: int
    created_count: int
    updated_count: int
    unchanged_count: int
    skipped_ineligible_count: int
    available_count: int


@dataclass(frozen=True)
class ProfileInboxSyncReport:
    profile_id: UUID
    profile_revision: int
    opportunity_count: int
    match_run_id: UUID | None
    match_run_created: bool
    materialization: InboxMaterializationReport


@dataclass(frozen=True)
class LeadView:
    item: InboxItemRecord
    opportunity: CanonicalOpportunityRecord
    match: MatchTraceRecord
    status_events: tuple[LeadInboxStatusEventRecord, ...]


class LeadInboxAccessError(LookupError):
    """The requested profile or item is outside the local AI STUDIO scope."""


class LeadInboxService:
    def __init__(
        self,
        database: Database,
        *,
        matching: CandidateMatchingService | None = None,
        opportunities: CanonicalOpportunityRepository | None = None,
        profiles: SearchProfileRepository | None = None,
        users: UserRepository | None = None,
        matches: MatchTraceRepository | None = None,
        scores: CommercialScoreSnapshotRepository | None = None,
        inbox: LeadInboxRepository | None = None,
    ) -> None:
        self._database = database
        self._matching = matching or CandidateMatchingService(database)
        self._opportunities = opportunities or CanonicalOpportunityRepository()
        self._profiles = profiles or SearchProfileRepository()
        self._users = users or UserRepository()
        self._matches = matches or MatchTraceRepository()
        self._scores = scores or CommercialScoreSnapshotRepository()
        self._inbox = inbox or LeadInboxRepository()

    async def materialize(
        self,
        outcome: MatchGenerationOutcome,
    ) -> InboxMaterializationReport:
        created = 0
        updated = 0
        unchanged = 0
        skipped = 0
        available = 0
        async with self._database.transaction() as connection:
            try:
                local_user = await self._local_user(connection)
            except UserNotFound:
                return InboxMaterializationReport(
                    trace_count=len(outcome.persistence.traces),
                    created_count=0,
                    updated_count=0,
                    unchanged_count=0,
                    skipped_ineligible_count=len(outcome.persistence.traces),
                    available_count=0,
                )
            current_profiles: dict[UUID, SearchProfileRecord | None] = {}
            for match_record in outcome.persistence.traces:
                trace = match_record.trace
                if trace.search_profile_id not in current_profiles:
                    current_profiles[trace.search_profile_id] = (
                        await self._locked_profile(
                            connection,
                            trace.search_profile_id,
                        )
                    )
                current_profile = current_profiles[trace.search_profile_id]
                if not _profile_accepts_trace(
                    current_profile,
                    local_user_id=local_user.id,
                    trace=trace,
                ):
                    skipped += 1
                    continue
                opportunity = await self._opportunities.get(
                    connection,
                    trace.opportunity_id,
                )
                if opportunity is None:
                    raise OpportunityNotFound(
                        f"Opportunity {trace.opportunity_id} does not exist"
                    )
                active = opportunity.lifecycle_status.value == "active"
                before = await self._inbox.get_by_pair(
                    connection,
                    search_profile_id=trace.search_profile_id,
                    opportunity_id=trace.opportunity_id,
                )
                snapshot_id = None
                if trace.eligible and active:
                    score = commercial_score_for_match(opportunity, trace)
                    snapshot = await self._scores.ensure(
                        connection,
                        opportunity_id=opportunity.id,
                        policy_version=score.policy_version,
                        input_sha256=score.input_hash,
                        total_score=score.score,
                        band=score.tier.value.lower(),
                        breakdown=score.breakdown.model_dump(mode="json"),
                    )
                    snapshot_id = snapshot.id
                item = await self._inbox.apply_match(
                    connection,
                    match_trace_id=match_record.id,
                    opportunity_id=trace.opportunity_id,
                    search_profile_id=trace.search_profile_id,
                    profile_revision=trace.profile_revision,
                    eligible=trace.eligible,
                    opportunity_active=active,
                    matched_at=trace.evaluated_at,
                    commercial_score_snapshot_id=snapshot_id,
                )
                if item is None:
                    skipped += 1
                    continue
                if item.availability is InboxAvailability.AVAILABLE:
                    available += 1
                if before is None:
                    created += 1
                elif item.revision != before.revision:
                    updated += 1
                else:
                    unchanged += 1
        return InboxMaterializationReport(
            trace_count=len(outcome.persistence.traces),
            created_count=created,
            updated_count=updated,
            unchanged_count=unchanged,
            skipped_ineligible_count=skipped,
            available_count=available,
        )

    async def sync_profile(
        self,
        profile_id: UUID,
        *,
        evaluated_at: datetime | None = None,
        opportunity_limit: int = 500,
    ) -> ProfileInboxSyncReport:
        selected_time = evaluated_at or datetime.now(timezone.utc)
        if selected_time.tzinfo is None or selected_time.utcoffset() is None:
            raise ValueError("evaluated_at must include a timezone")
        async with self._database.connect() as connection:
            profile = await self._require_local_profile(
                connection,
                profile_id,
                require_matchable=True,
            )
            opportunities = await self._opportunities.list_recent(
                connection,
                limit=opportunity_limit,
            )
            existing_ids = await self._inbox.list_opportunity_ids_for_profile(
                connection,
                profile.id,
            )
            known_ids = {opportunity.id for opportunity in opportunities}
            retained = list(opportunities)
            for opportunity_id in existing_ids:
                if opportunity_id in known_ids:
                    continue
                existing = await self._opportunities.get(
                    connection,
                    opportunity_id,
                )
                if existing is None:
                    raise OpportunityNotFound(
                        f"Opportunity {opportunity_id} does not exist"
                    )
                retained.append(existing)
                known_ids.add(opportunity_id)
            opportunities = tuple(retained)
        if not opportunities:
            return ProfileInboxSyncReport(
                profile_id=profile.id,
                profile_revision=profile.revision,
                opportunity_count=0,
                match_run_id=None,
                match_run_created=False,
                materialization=_empty_materialization(),
            )
        outcome = await self._matching.generate_matches(
            tuple(opportunity.id for opportunity in opportunities),
            evaluated_at=selected_time,
            profile_id=profile.id,
            profile_revision=profile.revision,
        )
        materialization = await self.materialize(outcome)
        return ProfileInboxSyncReport(
            profile_id=profile.id,
            profile_revision=profile.revision,
            opportunity_count=len(opportunities),
            match_run_id=outcome.persistence.run.id,
            match_run_created=outcome.persistence.created,
            materialization=materialization,
        )

    async def list_leads(
        self,
        *,
        profile_id: UUID,
        status: InboxStatus | str | None = None,
        availability: InboxAvailability | str | None = None,
        tier: str | None = None,
        limit: int = 100,
    ) -> tuple[InboxItemRecord, ...]:
        async with self._database.connect() as connection:
            await self._require_local_profile(connection, profile_id)
            return await self._inbox.list_items(
                connection,
                search_profile_id=profile_id,
                status=status,
                availability=availability,
                band=tier,
                limit=limit,
            )

    async def show_lead(self, item_id: UUID) -> LeadView:
        async with self._database.connect() as connection:
            item = await self._inbox.get(connection, item_id)
            if item is None:
                raise LookupError("Lead Inbox item does not exist")
            await self._require_local_profile(
                connection,
                item.search_profile_id,
            )
            opportunity = await self._opportunities.get(
                connection,
                item.opportunity_id,
            )
            if opportunity is None:
                raise OpportunityNotFound(
                    f"Opportunity {item.opportunity_id} does not exist"
                )
            match = await self._matches.get(
                connection,
                item.latest_match_trace_id,
            )
            if match is None:
                raise LookupError("Lead Inbox match trace does not exist")
            events = await self._inbox.list_status_events(connection, item.id)
        return LeadView(
            item=item,
            opportunity=opportunity,
            match=match,
            status_events=events,
        )

    async def update_status(
        self,
        item_id: UUID,
        target: InboxStatus | str,
        *,
        expected_revision: int,
        actor_id: str,
        reason: str,
        request_id: str,
    ) -> InboxItemRecord:
        key = status_request_key(
            item_id=item_id,
            target=InboxStatus(target),
            request_id=request_id,
        )
        async with self._database.transaction() as connection:
            item = await self._inbox.get(connection, item_id)
            if item is None:
                raise LeadInboxAccessError("Lead Inbox item does not exist")
            await self._require_local_profile(
                connection,
                item.search_profile_id,
            )
            return await self._inbox.update_status(
                connection,
                item_id=item_id,
                target=target,
                expected_revision=expected_revision,
                actor_id=actor_id,
                reason=reason,
                idempotency_key=key,
            )

    async def _local_user(self, connection):
        return await self._users.get_by_identity(
            connection,
            platform=LOCAL_USER_PLATFORM,
            external_user_id=LOCAL_USER_EXTERNAL_ID,
        )

    async def _require_local_profile(
        self,
        connection,
        profile_id: UUID,
        *,
        require_matchable: bool = False,
    ) -> SearchProfileRecord:
        try:
            local_user = await self._local_user(connection)
            profile = await self._profiles.get(connection, profile_id)
        except (SearchProfileNotFound, UserNotFound):
            raise LeadInboxAccessError(
                "Search Profile is not available to local AI STUDIO"
            ) from None
        if profile.user_id != local_user.id:
            raise LeadInboxAccessError(
                "Search Profile is not available to local AI STUDIO"
            )
        if require_matchable and not _profile_is_matchable(profile):
            raise LeadInboxAccessError(
                "Search Profile is not an active local primary profile"
            )
        return profile

    async def _locked_profile(
        self,
        connection,
        profile_id: UUID,
    ) -> SearchProfileRecord | None:
        locked_id = await connection.scalar(
            sa.select(search_profiles.c.id)
            .where(search_profiles.c.id == profile_id)
            .with_for_update()
        )
        if locked_id is None:
            return None
        try:
            return await self._profiles.get(connection, profile_id)
        except SearchProfileNotFound:
            return None


def commercial_score_for_match(
    opportunity: CanonicalOpportunityRecord,
    trace: MatchTraceDraft,
) -> CommercialScore:
    if opportunity.id != trace.opportunity_id:
        raise ValueError("match trace belongs to another Opportunity")
    relevance = _normalized_signal(
        trace.combined_relevance_score
        if trace.combined_relevance_score is not None
        else trace.user_relevance_score,
        default=Decimal("0"),
        name="relevance",
    )
    budget = _budget_component(trace)
    analysis = opportunity.analysis
    contact = analysis.contact
    if any((contact.telegram, contact.email, contact.url)):
        contactability = Contactability.DIRECT_CONTACT
    elif opportunity.preferred_source is not None:
        contactability = Contactability.SOURCE_ONLY
    else:
        contactability = Contactability.NONE

    return calculate_commercial_score(
        CommercialScoreInput(
            relevance=float(relevance),
            intent=CommercialIntent(analysis.intent_stage.value),
            budget_component=float(budget),
            urgency=_urgency(analysis),
            actionability=float(
                _normalized_signal(
                    Decimal(str(analysis.quality.actionability)),
                    name="actionability",
                )
            ),
            contactability=contactability,
            source_quality=(
                None
                if trace.source_quality_score is None
                else float(
                    _normalized_signal(
                        trace.source_quality_score,
                        name="source_quality",
                    )
                )
            ),
            red_flags=analysis.red_flags,
        )
    )


def status_request_key(
    *,
    item_id: UUID,
    target: InboxStatus,
    request_id: str,
) -> str:
    # Validate the requested state, but deliberately do not bind it into the
    # request identity.  Reusing one operator request id for another target
    # must hit the same persisted event and be rejected as a conflict.
    InboxStatus(target)
    selected_request = request_id.strip()
    if not selected_request:
        raise ValueError("request_id must not be blank")
    payload = {
        "item_id": str(item_id),
        "request_id": selected_request,
        "version": "lead-inbox-status-request.v1",
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _profile_is_matchable(profile: SearchProfileRecord) -> bool:
    return (
        profile.confirmation_status is SearchProfileConfirmationStatus.CONFIRMED
        and profile.is_active
        and profile.is_primary
    )


def _profile_accepts_trace(
    profile: SearchProfileRecord | None,
    *,
    local_user_id: UUID,
    trace: MatchTraceDraft,
) -> bool:
    return bool(
        profile is not None
        and profile.user_id == local_user_id
        and _profile_is_matchable(profile)
        and profile.revision == trace.profile_revision
    )


def _budget_component(trace: MatchTraceDraft) -> Decimal:
    for component in trace.structured_components:
        if component.get("name") != "budget":
            continue
        value = component.get("score")
        if value is None:
            return Decimal("0")
        try:
            return _normalized_signal(
                Decimal(str(value)),
                name="budget_component",
            )
        except InvalidOperation:
            raise ValueError("budget component is not numeric") from None
    return Decimal("0")


def _urgency(analysis: object) -> CommercialUrgency:
    value = getattr(analysis, "urgency", None)
    if value is None:
        return CommercialUrgency.UNKNOWN
    level = getattr(value, "level", value)
    if isinstance(level, Enum):
        level = level.value
    try:
        return CommercialUrgency(str(level).lower())
    except ValueError:
        return CommercialUrgency.UNKNOWN


def _normalized_signal(
    value: Decimal | None,
    *,
    default: Decimal | None = None,
    name: str,
) -> Decimal:
    selected = default if value is None else Decimal(value)
    if selected is None or not selected.is_finite():
        raise ValueError(f"{name} must be a finite normalized score")
    if not Decimal("0") <= selected <= Decimal("1"):
        raise ValueError(f"{name} must be between 0 and 1")
    return selected


def _empty_materialization() -> InboxMaterializationReport:
    return InboxMaterializationReport(
        trace_count=0,
        created_count=0,
        updated_count=0,
        unchanged_count=0,
        skipped_ineligible_count=0,
        available_count=0,
    )


__all__ = [
    "InboxMaterializationReport",
    "LeadInboxAccessError",
    "LeadInboxService",
    "LeadView",
    "ProfileInboxSyncReport",
    "commercial_score_for_match",
    "status_request_key",
]

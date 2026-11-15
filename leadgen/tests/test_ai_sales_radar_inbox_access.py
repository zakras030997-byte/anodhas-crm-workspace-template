from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

from ai_sales_radar.inbox import (
    InboxMaterializationReport,
    LeadInboxAccessError,
    LeadInboxService,
)
from ai_sales_radar.persistence.inbox import InboxStatus
from freelancer_bot.persistence.search_profiles import (
    SearchProfileConfirmationStatus,
    SearchProfileNotFound,
    UserNotFound,
)


LOCAL_USER_ID = UUID("10000000-0000-0000-0000-000000000001")
FOREIGN_USER_ID = UUID("20000000-0000-0000-0000-000000000002")


class _Connection:
    def __init__(self, locked_profile_id: UUID | None) -> None:
        self.locked_profile_id = locked_profile_id
        self.scalar_calls = 0

    async def scalar(self, _statement):
        self.scalar_calls += 1
        return self.locked_profile_id


class _AsyncConnectionContext:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    async def __aenter__(self) -> _Connection:
        return self.connection

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        return None


class _Database:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    def connect(self) -> _AsyncConnectionContext:
        return _AsyncConnectionContext(self.connection)

    def transaction(self) -> _AsyncConnectionContext:
        return _AsyncConnectionContext(self.connection)


def _profile(
    *,
    profile_id: UUID | None = None,
    user_id: UUID = LOCAL_USER_ID,
    revision: int = 3,
    confirmed: bool = True,
    active: bool = True,
    primary: bool = True,
):
    return SimpleNamespace(
        id=profile_id or uuid4(),
        user_id=user_id,
        revision=revision,
        confirmation_status=(
            SearchProfileConfirmationStatus.CONFIRMED
            if confirmed
            else SearchProfileConfirmationStatus.DRAFT
        ),
        is_active=active,
        is_primary=primary,
    )


def _outcome(
    *,
    profile_id: UUID,
    profile_revision: int,
    count: int = 1,
    eligible: bool = False,
):
    records = tuple(
        SimpleNamespace(
            id=uuid4(),
            trace=SimpleNamespace(
                search_profile_id=profile_id,
                profile_revision=profile_revision,
                opportunity_id=uuid4(),
                eligible=eligible,
                evaluated_at=datetime(2026, 8, 26, tzinfo=timezone.utc),
            ),
        )
        for _ in range(count)
    )
    return SimpleNamespace(
        persistence=SimpleNamespace(traces=records),
    )


def _service_fixture(
    *,
    profile=None,
    local_user_exists: bool = True,
    item=None,
    opportunity=None,
):
    connection = _Connection(
        None if profile is None else profile.id,
    )
    users = SimpleNamespace(get_by_identity=AsyncMock())
    if local_user_exists:
        users.get_by_identity.return_value = SimpleNamespace(id=LOCAL_USER_ID)
    else:
        users.get_by_identity.side_effect = UserNotFound(
            "local AI STUDIO user does not exist"
        )

    profiles = SimpleNamespace(get=AsyncMock())
    if profile is None:
        profiles.get.side_effect = SearchProfileNotFound(
            "Search Profile does not exist"
        )
    else:
        profiles.get.return_value = profile

    opportunities = SimpleNamespace(
        get=AsyncMock(return_value=opportunity),
        list_recent=AsyncMock(return_value=()),
    )
    matching = SimpleNamespace(generate_matches=AsyncMock())
    matches = SimpleNamespace(get=AsyncMock())
    scores = SimpleNamespace(ensure=AsyncMock())
    inbox = SimpleNamespace(
        apply_match=AsyncMock(return_value=None),
        get=AsyncMock(return_value=item),
        get_by_pair=AsyncMock(return_value=None),
        list_items=AsyncMock(return_value=()),
        list_status_events=AsyncMock(return_value=()),
        list_opportunity_ids_for_profile=AsyncMock(return_value=()),
        update_status=AsyncMock(),
    )
    service = LeadInboxService(
        _Database(connection),
        matching=matching,
        opportunities=opportunities,
        profiles=profiles,
        users=users,
        matches=matches,
        scores=scores,
        inbox=inbox,
    )
    dependencies = SimpleNamespace(
        connection=connection,
        users=users,
        profiles=profiles,
        opportunities=opportunities,
        matching=matching,
        matches=matches,
        scores=scores,
        inbox=inbox,
    )
    return service, dependencies


class LeadInboxMaterializationAccessTest(unittest.IsolatedAsyncioTestCase):
    async def test_missing_local_user_skips_all_traces_without_writes(self):
        profile = _profile()
        service, dependencies = _service_fixture(
            profile=profile,
            local_user_exists=False,
        )
        outcome = _outcome(
            profile_id=profile.id,
            profile_revision=profile.revision,
            count=2,
        )

        report = await service.materialize(outcome)

        self.assertEqual(report.trace_count, 2)
        self.assertEqual(report.skipped_ineligible_count, 2)
        self.assertEqual(report.created_count, 0)
        self.assertEqual(report.updated_count, 0)
        self.assertEqual(report.unchanged_count, 0)
        dependencies.profiles.get.assert_not_awaited()
        dependencies.opportunities.get.assert_not_awaited()
        dependencies.scores.ensure.assert_not_awaited()
        dependencies.inbox.apply_match.assert_not_awaited()

    async def test_stale_deactivated_or_nonlocal_profile_is_skipped(self):
        cases = (
            ("stale revision", _profile(revision=4), 3),
            ("deactivated", _profile(active=False), 3),
            ("unconfirmed", _profile(confirmed=False), 3),
            ("secondary", _profile(primary=False), 3),
            ("nonlocal owner", _profile(user_id=FOREIGN_USER_ID), 3),
        )
        for label, profile, trace_revision in cases:
            with self.subTest(label=label):
                service, dependencies = _service_fixture(profile=profile)
                outcome = _outcome(
                    profile_id=profile.id,
                    profile_revision=trace_revision,
                )

                report = await service.materialize(outcome)

                self.assertEqual(report.trace_count, 1)
                self.assertEqual(report.skipped_ineligible_count, 1)
                self.assertEqual(report.created_count, 0)
                self.assertEqual(dependencies.connection.scalar_calls, 1)
                dependencies.opportunities.get.assert_not_awaited()
                dependencies.scores.ensure.assert_not_awaited()
                dependencies.inbox.apply_match.assert_not_awaited()

    async def test_current_local_primary_profile_reaches_materialization(self):
        profile = _profile()
        opportunity = SimpleNamespace(
            id=uuid4(),
            lifecycle_status=SimpleNamespace(value="active"),
        )
        service, dependencies = _service_fixture(
            profile=profile,
            opportunity=opportunity,
        )
        outcome = _outcome(
            profile_id=profile.id,
            profile_revision=profile.revision,
            eligible=False,
        )
        outcome.persistence.traces[0].trace.opportunity_id = opportunity.id

        report = await service.materialize(outcome)

        self.assertEqual(report.skipped_ineligible_count, 1)
        dependencies.opportunities.get.assert_awaited_once()
        dependencies.inbox.apply_match.assert_awaited_once()
        dependencies.scores.ensure.assert_not_awaited()


class LeadInboxOperationAccessTest(unittest.IsolatedAsyncioTestCase):
    async def test_sync_retains_existing_items_outside_recent_window(self):
        profile = _profile()
        recent = SimpleNamespace(id=uuid4())
        older = SimpleNamespace(id=uuid4())
        service, dependencies = _service_fixture(profile=profile)
        dependencies.opportunities.list_recent.return_value = (recent,)
        dependencies.inbox.list_opportunity_ids_for_profile.return_value = (
            older.id,
        )
        dependencies.opportunities.get.return_value = older
        matching_outcome = SimpleNamespace(
            persistence=SimpleNamespace(
                run=SimpleNamespace(id=uuid4()),
                created=True,
            )
        )
        dependencies.matching.generate_matches.return_value = matching_outcome
        service.materialize = AsyncMock(
            return_value=InboxMaterializationReport(
                trace_count=0,
                created_count=0,
                updated_count=0,
                unchanged_count=0,
                skipped_ineligible_count=0,
                available_count=0,
            )
        )

        report = await service.sync_profile(profile.id)

        dependencies.matching.generate_matches.assert_awaited_once()
        identifiers = dependencies.matching.generate_matches.await_args.args[0]
        self.assertEqual(identifiers, (recent.id, older.id))
        self.assertEqual(report.opportunity_count, 2)

    async def test_list_and_sync_reject_foreign_profile_uuid(self):
        foreign_profile = _profile(user_id=FOREIGN_USER_ID)
        service, dependencies = _service_fixture(profile=foreign_profile)

        with self.assertRaises(LeadInboxAccessError):
            await service.list_leads(profile_id=foreign_profile.id)
        with self.assertRaises(LeadInboxAccessError):
            await service.sync_profile(foreign_profile.id)

        dependencies.inbox.list_items.assert_not_awaited()
        dependencies.opportunities.list_recent.assert_not_awaited()
        dependencies.matching.generate_matches.assert_not_awaited()

    async def test_show_and_status_reject_foreign_item_uuid(self):
        foreign_profile = _profile(user_id=FOREIGN_USER_ID)
        foreign_item = SimpleNamespace(
            id=uuid4(),
            search_profile_id=foreign_profile.id,
            opportunity_id=uuid4(),
            latest_match_trace_id=uuid4(),
        )
        service, dependencies = _service_fixture(
            profile=foreign_profile,
            item=foreign_item,
        )

        with self.assertRaises(LeadInboxAccessError):
            await service.show_lead(foreign_item.id)
        with self.assertRaises(LeadInboxAccessError):
            await service.update_status(
                foreign_item.id,
                InboxStatus.OPENED,
                expected_revision=1,
                actor_id="local-operator",
                reason="reviewed",
                request_id="foreign-item-attempt",
            )

        dependencies.opportunities.get.assert_not_awaited()
        dependencies.matches.get.assert_not_awaited()
        dependencies.inbox.list_status_events.assert_not_awaited()
        dependencies.inbox.update_status.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()

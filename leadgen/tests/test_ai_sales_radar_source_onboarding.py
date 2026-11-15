from __future__ import annotations

import asyncio
import contextlib
from dataclasses import asdict
from datetime import UTC, datetime
import io
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

import sqlalchemy as sa
from telethon import functions, types
from telethon.errors import InviteRequestSentError

from ai_sales_radar.config import TelegramCredentials
from ai_sales_radar.source_onboarding import (
    CANARY_CONFIRMATION,
    CANARY_SELECTION,
    CANARY_SORTED_IDS,
    FinalizeOnboardingResult,
    OnboardingSnapshot,
    PostgresSourceOnboardingStore,
    SourceOnboardingError,
    TelegramSourceOnboarding,
    build_parser,
    main,
)
from ai_sales_radar.source_catalog import DEFAULT_SOURCES_PATH, SourceCatalogService
from freelancer_bot.persistence.collector_accounts import CollectorAccountRepository
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.discovery_campaigns import DiscoveryCampaignRepository
from freelancer_bot.persistence.schema import (
    source_collector_access,
    source_lifecycle_events,
    source_reference_aliases,
    sources,
    telegram_source_validations,
)
from freelancer_bot.persistence.source_repository import (
    SourceRecord,
    SourceRepository,
    SourceStatus,
)
from freelancer_bot.telegram_request_governor import TelegramRequestCategory
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


NOW = datetime(2026, 8, 27, 12, 0, tzinfo=UTC)
LEADGEN_SOURCES_PATH = DEFAULT_SOURCES_PATH.with_name(
    "sources.leadgen.20260828.json"
)
EXPECTED = {
    1: "freelansim_ru",
    3: "digitaltender",
    4: "freelancetaverna",
    5: "frilans",
    6: "job_developer",
    7: "search_techspec",
    9: "freevacanciesit",
    10: "freelance_dev_work",
    12: "pixeltechspec",
    13: "webfrl",
    14: "workayte",
    15: "freelancehuntprojects",
    16: "freefounders",
    17: "job_for_bots",
    18: "zerocode_jobs",
    19: "tgram_jobs",
}


def source_record(
    source_id: int,
    *,
    status: SourceStatus = SourceStatus.CANDIDATE,
    username: str | None = None,
    platform: str = "telegram",
    access_type: str = "public",
    external_id: str | None = None,
) -> SourceRecord:
    selected_username = username or EXPECTED[source_id]
    return SourceRecord(
        id=source_id,
        platform=platform,
        external_id=external_id or f"username:{selected_username}",
        access_type=access_type,
        lifecycle_status=status,
        display_name=f"Source {source_id}",
        handle=f"@{selected_username}",
        canonical_url=f"https://t.me/{selected_username}",
        created_at=NOW,
        updated_at=NOW,
    )


def channel(
    source_id: int,
    *,
    username: str | None = None,
    broadcast: bool = True,
    megagroup: bool = False,
) -> types.Channel:
    return types.Channel(
        id=100_000 + source_id,
        title=f"Channel {source_id}",
        photo=types.ChatPhotoEmpty(),
        date=None,
        broadcast=broadcast,
        megagroup=megagroup,
        access_hash=900_000 + source_id,
        username=username or EXPECTED[source_id],
        usernames=[],
    )


def dialog(entity, *, input_channel_id=None):
    return SimpleNamespace(
        entity=entity,
        input_entity=types.InputPeerChannel(
            channel_id=entity.id if input_channel_id is None else input_channel_id,
            access_hash=entity.access_hash,
        ),
    )


class FakeLock:
    def __init__(self, _path, *, role, events):
        self.role = role
        self.events = events

    def acquire(self):
        self.events.append("lock.acquire")

    def release(self):
        self.events.append("lock.release")


class FakeGovernor:
    def __init__(self, events):
        self.events = events

    async def run(self, category, operation, **_kwargs):
        self.events.append(f"governor:{category}")
        return await operation()


class FailBeforeMembershipGovernor(FakeGovernor):
    async def run(self, category, operation, **_kwargs):
        self.events.append(f"governor:{category}")
        if category == TelegramRequestCategory.MEMBERSHIP:
            raise RuntimeError("governor paused before operation")
        return await operation()


class FakeStore:
    def __init__(
        self,
        records,
        events,
        *,
        active=True,
        finalize_error: Exception | None = None,
    ):
        self.records = tuple(sorted(records, key=lambda value: value.id))
        self.events = events
        self.finalize_error = finalize_error
        self.load_calls = []
        self.finalize_calls = []
        self.validation_writes = 0
        self.approval_writes = 0
        self.snapshot = OnboardingSnapshot(
            collector_account=SimpleNamespace(
                id=41,
                platform="telegram",
                external_account_id="70001",
                display_name="Collector",
                is_active=active,
                created_at=NOW,
                updated_at=NOW,
            ),
            sources=self.records,
        )

    async def load_selected(self, *, external_account_id, source_ids):
        self.events.append("store.load")
        self.load_calls.append((external_account_id, tuple(source_ids)))
        if not self.snapshot.collector_account.is_active:
            raise SourceOnboardingError("collector disabled")
        return self.snapshot

    async def finalize(self, *, snapshot, bindings, actor, reason):
        self.events.append("store.finalize")
        self.finalize_calls.append(
            (snapshot, tuple(bindings), actor, reason)
        )
        if self.finalize_error is not None:
            raise self.finalize_error
        # Model the production store's single transaction boundary: both
        # counters change only after every binding has been accepted.
        if {binding.source_id for binding in bindings} != set(EXPECTED):
            raise SourceOnboardingError("validation mismatch")
        approved = tuple(
            record.id
            for record in self.records
            if record.lifecycle_status is SourceStatus.CANDIDATE
        )
        unchanged = tuple(
            record.id
            for record in self.records
            if record.lifecycle_status is not SourceStatus.CANDIDATE
        )
        self.validation_writes += len(bindings)
        self.approval_writes += len(approved)
        return FinalizeOnboardingResult(approved, unchanged)


class FakeClient:
    def __init__(
        self,
        entities,
        dialogs,
        events,
        *,
        authorized=True,
        fail_join_source_id=None,
        omit_joined_source_id=None,
        ambiguous_join_source_id=None,
        invite_request_source_id=None,
        disconnect_gate=None,
        resolve_gate=None,
    ):
        self.entities = dict(entities)
        self.dialogs = list(dialogs)
        self.events = events
        self.authorized = authorized
        self.fail_join_source_id = fail_join_source_id
        self.omit_joined_source_id = omit_joined_source_id
        self.ambiguous_join_source_id = ambiguous_join_source_id
        self.invite_request_source_id = invite_request_source_id
        self.disconnect_gate = disconnect_gate
        self.resolve_gate = resolve_gate
        self.joined_source_ids = []

    async def connect(self):
        self.events.append("client.connect")

    async def disconnect(self):
        if self.disconnect_gate is None:
            self.events.append("client.disconnect")
            return
        self.events.append("client.disconnect.started")
        await self.disconnect_gate.wait()
        self.events.append("client.disconnect.completed")

    async def is_user_authorized(self):
        self.events.append("client.authorized")
        return self.authorized

    async def get_me(self):
        self.events.append("client.get_me")
        return SimpleNamespace(id=70001)

    async def get_entity(self, lookup):
        username = lookup.removeprefix("@").casefold()
        self.events.append(f"resolve:{username}")
        if self.resolve_gate is not None:
            await self.resolve_gate.wait()
        return self.entities[username]

    async def get_dialogs(self, limit=None):
        self.events.append("client.get_dialogs")
        self.assert_limit = limit
        return tuple(self.dialogs)

    async def __call__(self, request):
        if not isinstance(request, functions.channels.JoinChannelRequest):
            raise AssertionError(type(request).__name__)
        entity = request.channel
        source_id = int(entity.id) - 100_000
        self.events.append(f"join:{source_id}")
        if source_id == self.fail_join_source_id:
            raise RuntimeError("simulated join failure")
        if source_id == self.invite_request_source_id:
            raise InviteRequestSentError(request)
        self.joined_source_ids.append(source_id)
        if source_id != self.omit_joined_source_id:
            self.dialogs.append(dialog(entity))
        if source_id == self.ambiguous_join_source_id:
            raise ConnectionError("simulated response loss after join")
        return SimpleNamespace()


class SourceOnboardingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.runtime_root = Path(self.temp.name) / ".local"
        self.credentials = TelegramCredentials(
            api_id=12345,
            api_hash="secret",
            session_path=self.runtime_root / "telegram" / "collector.session",
        )
        self.events = []

    def tearDown(self):
        self.temp.cleanup()

    def make_service(
        self,
        *,
        records=None,
        entities=None,
        dialogs=(),
        store=None,
        client=None,
    ):
        selected_records = records or [source_record(value) for value in EXPECTED]
        selected_entities = entities or {
            username: channel(source_id)
            for source_id, username in EXPECTED.items()
        }
        selected_store = store or FakeStore(selected_records, self.events)
        selected_client = client or FakeClient(
            selected_entities,
            dialogs,
            self.events,
        )
        service = TelegramSourceOnboarding(
            object(),
            self.credentials,
            store=selected_store,
            client_factory=lambda _credentials: selected_client,
            governor_factory=lambda _account_id: FakeGovernor(self.events),
            lock_factory=lambda path, *, role: FakeLock(
                path,
                role=role,
                events=self.events,
            ),
            runtime_root=self.runtime_root,
            plan_request_delay_seconds=0,
        )
        return service, selected_store, selected_client

    async def test_plan_is_fresh_read_only_and_safe_to_serialize(self):
        existing = [dialog(channel(3)), dialog(channel(19))]
        service, store, client = self.make_service(dialogs=existing)

        report = await service.plan(source_ids=CANARY_SELECTION)

        self.assertEqual(report.mode, "plan")
        self.assertEqual(report.selected_source_count, len(EXPECTED))
        self.assertEqual(report.already_member_count, 2)
        self.assertEqual(report.join_required_count, len(EXPECTED) - 2)
        self.assertEqual(report.joined_count, 0)
        self.assertEqual(store.finalize_calls, [])
        self.assertEqual(client.joined_source_ids, [])
        self.assertEqual(self.events.count("lock.acquire"), 1)
        self.assertEqual(self.events.count("lock.release"), 1)
        payload = json.dumps(asdict(report))
        self.assertNotIn("canonical_peer", payload)
        self.assertNotIn("access_hash", payload)

    async def test_apply_preflights_all_before_first_join_and_scopes_exactly(self):
        service, store, client = self.make_service()

        report = await service.apply(
            source_ids=tuple(reversed(CANARY_SELECTION)),
            actor="owner",
            reason="first_canary",
            confirm_exact=CANARY_CONFIRMATION,
        )

        first_join = next(i for i, value in enumerate(self.events) if value.startswith("join:"))
        resolve_positions = [
            i for i, value in enumerate(self.events) if value.startswith("resolve:")
        ]
        self.assertEqual(len(resolve_positions), len(EXPECTED))
        self.assertLess(max(resolve_positions), first_join)
        self.assertEqual(client.joined_source_ids, list(CANARY_SORTED_IDS))
        self.assertEqual(store.load_calls, [("70001", CANARY_SORTED_IDS)])
        self.assertEqual(len(store.finalize_calls), 1)
        self.assertEqual(report.approved_count, len(EXPECTED))
        self.assertEqual(store.validation_writes, len(EXPECTED))
        self.assertEqual(store.approval_writes, len(EXPECTED))
        categories = [
            event.removeprefix("governor:")
            for event in self.events
            if event.startswith("governor:")
        ]
        self.assertEqual(
            categories.count(TelegramRequestCategory.ENTITY_ACCESS),
            len(EXPECTED) + 2,
        )
        self.assertEqual(
            categories.count(TelegramRequestCategory.MEMBERSHIP),
            len(EXPECTED),
        )

    async def test_apply_joins_only_missing_selected_channels(self):
        present_ids = {3, 7, 19}
        present = [dialog(channel(source_id)) for source_id in present_ids]
        service, _, client = self.make_service(dialogs=present)

        report = await service.apply(
            source_ids=CANARY_SELECTION,
            actor="operator",
            reason="bounded_canary",
            confirm_exact=CANARY_CONFIRMATION,
        )

        self.assertEqual(
            client.joined_source_ids,
            [
                source_id
                for source_id in CANARY_SORTED_IDS
                if source_id not in present_ids
            ],
        )
        self.assertEqual(report.already_member_count, 3)
        self.assertEqual(report.joined_count, len(EXPECTED) - 3)
        self.assertEqual(
            [
                item.membership
                for item in report.sources
                if item.source_id not in present_ids
            ],
            ["joined"] * (len(EXPECTED) - len(present_ids)),
        )

    async def test_dialog_membership_requires_input_peer_binding_and_not_left(self):
        left_entity = channel(3)
        left_entity.left = True
        present = [dialog(channel(source_id)) for source_id in EXPECTED]
        present[CANARY_SORTED_IDS.index(3)] = dialog(left_entity)
        present[CANARY_SORTED_IDS.index(9)] = dialog(
            channel(9), input_channel_id=999_999
        )
        service, _, _ = self.make_service(dialogs=present)

        report = await service.plan(source_ids=CANARY_SELECTION)

        by_id = {item.source_id: item for item in report.sources}
        self.assertEqual(by_id[3].result, "join_required")
        self.assertEqual(by_id[9].result, "join_required")
        self.assertEqual(by_id[7].result, "already_member")

    async def test_wrong_entity_type_fails_before_any_join(self):
        entities = {
            username: channel(source_id)
            for source_id, username in EXPECTED.items()
        }
        entities["job_for_bots"] = types.User(
            id=999,
            first_name="Wrong",
            username="job_for_bots",
        )
        service, store, client = self.make_service(entities=entities)

        with self.assertRaisesRegex(SourceOnboardingError, "не в Telegram channel"):
            await service.apply(
                source_ids=CANARY_SELECTION,
                actor="operator",
                reason="bounded_canary",
                confirm_exact=CANARY_CONFIRMATION,
            )
        self.assertEqual(client.joined_source_ids, [])
        self.assertEqual(store.finalize_calls, [])

    async def test_chat_like_channel_is_rejected(self):
        entities = {
            username: channel(source_id)
            for source_id, username in EXPECTED.items()
        }
        entities["digitaltender"] = channel(3, broadcast=False, megagroup=False)
        service, store, client = self.make_service(entities=entities)

        with self.assertRaisesRegex(SourceOnboardingError, "channel/megagroup"):
            await service.plan(source_ids=CANARY_SELECTION)
        self.assertEqual(client.joined_source_ids, [])
        self.assertEqual(store.finalize_calls, [])

    async def test_wrong_username_is_rejected(self):
        entities = {
            username: channel(source_id)
            for source_id, username in EXPECTED.items()
        }
        entities["search_techspec"] = channel(7, username="transferred_name")
        service, store, client = self.make_service(entities=entities)

        with self.assertRaisesRegex(SourceOnboardingError, "активным Telegram username"):
            await service.apply(
                source_ids=CANARY_SELECTION,
                actor="operator",
                reason="bounded_canary",
                confirm_exact=CANARY_CONFIRMATION,
            )
        self.assertEqual(client.joined_source_ids, [])
        self.assertEqual(store.finalize_calls, [])

    async def test_scam_fake_and_restricted_entities_are_rejected(self):
        for flag in ("scam", "fake", "restricted"):
            with self.subTest(flag=flag):
                entities = {
                    username: channel(source_id)
                    for source_id, username in EXPECTED.items()
                }
                setattr(entities["pixeltechspec"], flag, True)
                service, store, client = self.make_service(entities=entities)
                with self.assertRaisesRegex(SourceOnboardingError, "небезопасный"):
                    await service.plan(source_ids=CANARY_SELECTION)
                self.assertEqual(client.joined_source_ids, [])
                self.assertEqual(store.finalize_calls, [])

    async def test_bad_database_identity_fails_before_telegram_resolution(self):
        records = [
            source_record(3, external_id="username:not_digitaltender")
            if value == 3
            else source_record(value)
            for value in EXPECTED
        ]
        service, store, client = self.make_service(records=records)

        with self.assertRaisesRegex(SourceOnboardingError, "external identity"):
            await service.plan(source_ids=CANARY_SELECTION)
        self.assertFalse(any(value.startswith("resolve:") for value in self.events))
        self.assertEqual(store.finalize_calls, [])
        self.assertEqual(client.joined_source_ids, [])

    async def test_inactive_collector_account_fails_before_resolution(self):
        store = FakeStore(
            [source_record(value) for value in EXPECTED],
            self.events,
            active=False,
        )
        service, _, client = self.make_service(store=store)

        with self.assertRaises(SourceOnboardingError):
            await service.plan(source_ids=CANARY_SELECTION)
        self.assertFalse(any(value.startswith("resolve:") for value in self.events))
        self.assertEqual(client.joined_source_ids, [])

    async def test_partial_join_failure_never_finalizes_or_auto_leaves(self):
        client = FakeClient(
            {username: channel(source_id) for source_id, username in EXPECTED.items()},
            (),
            self.events,
            fail_join_source_id=7,
        )
        service, store, _ = self.make_service(client=client)

        with self.assertRaises(RuntimeError):
            await service.apply(
                source_ids=CANARY_SELECTION,
                actor="operator",
                reason="bounded_canary",
                confirm_exact=CANARY_CONFIRMATION,
            )
        self.assertEqual(
            client.joined_source_ids,
            [source_id for source_id in CANARY_SORTED_IDS if source_id < 7],
        )
        self.assertEqual(store.finalize_calls, [])
        self.assertEqual(store.approval_writes, 0)

    async def test_governor_refusal_before_join_does_not_bypass_with_dialog_read(self):
        service, store, client = self.make_service()
        service._governor_factory = lambda _account_id: FailBeforeMembershipGovernor(
            self.events
        )

        with self.assertRaisesRegex(RuntimeError, "governor paused"):
            await service.apply(
                source_ids=CANARY_SELECTION,
                actor="operator",
                reason="bounded_canary",
                confirm_exact=CANARY_CONFIRMATION,
            )

        # One governed dialog snapshot belongs to all-source preflight.  There
        # is no ungoverned/extra reconciliation after reservation refusal.
        self.assertEqual(self.events.count("client.get_dialogs"), 1)
        self.assertEqual(client.joined_source_ids, [])
        self.assertEqual(store.finalize_calls, [])

    async def test_ambiguous_join_error_is_reconciled_from_fresh_dialogs(self):
        client = FakeClient(
            {username: channel(source_id) for source_id, username in EXPECTED.items()},
            (),
            self.events,
            ambiguous_join_source_id=9,
        )
        service, store, _ = self.make_service(client=client)

        report = await service.apply(
            source_ids=CANARY_SELECTION,
            actor="operator",
            reason="bounded_canary",
            confirm_exact=CANARY_CONFIRMATION,
        )

        self.assertEqual(report.approved_count, len(EXPECTED))
        self.assertEqual(store.approval_writes, len(EXPECTED))
        self.assertGreaterEqual(self.events.count("client.get_dialogs"), 3)

    async def test_invite_request_sent_is_not_treated_as_membership(self):
        client = FakeClient(
            {username: channel(source_id) for source_id, username in EXPECTED.items()},
            (),
            self.events,
            invite_request_source_id=9,
        )
        service, store, _ = self.make_service(client=client)

        with self.assertRaisesRegex(SourceOnboardingError, "ещё не подтверждён"):
            await service.apply(
                source_ids=CANARY_SELECTION,
                actor="operator",
                reason="bounded_canary",
                confirm_exact=CANARY_CONFIRMATION,
            )
        self.assertEqual(store.finalize_calls, [])
        self.assertEqual(store.approval_writes, 0)

    async def test_post_join_mismatch_prevents_validation_and_approval(self):
        client = FakeClient(
            {username: channel(source_id) for source_id, username in EXPECTED.items()},
            (),
            self.events,
            omit_joined_source_id=12,
        )
        service, store, _ = self.make_service(client=client)

        with self.assertRaisesRegex(SourceOnboardingError, "post-join"):
            await service.apply(
                source_ids=CANARY_SELECTION,
                actor="operator",
                reason="bounded_canary",
                confirm_exact=CANARY_CONFIRMATION,
            )
        self.assertEqual(store.finalize_calls, [])
        self.assertEqual(store.validation_writes, 0)
        self.assertEqual(store.approval_writes, 0)

    async def test_validation_mismatch_produces_no_approvals(self):
        store = FakeStore(
            [source_record(value) for value in EXPECTED],
            self.events,
            finalize_error=SourceOnboardingError("saved identity mismatch"),
        )
        service, _, _ = self.make_service(store=store)

        with self.assertRaisesRegex(SourceOnboardingError, "identity mismatch"):
            await service.apply(
                source_ids=CANARY_SELECTION,
                actor="operator",
                reason="bounded_canary",
                confirm_exact=CANARY_CONFIRMATION,
            )
        self.assertEqual(store.validation_writes, 0)
        self.assertEqual(store.approval_writes, 0)

    async def test_idempotent_already_approved_sources_are_unchanged(self):
        records = [
            source_record(source_id, status=SourceStatus.APPROVED)
            for source_id in EXPECTED
        ]
        dialogs = [dialog(channel(source_id)) for source_id in EXPECTED]
        service, store, client = self.make_service(records=records, dialogs=dialogs)

        report = await service.apply(
            source_ids=CANARY_SELECTION,
            actor="operator",
            reason="rerun_canary",
            confirm_exact=CANARY_CONFIRMATION,
        )
        self.assertEqual(client.joined_source_ids, [])
        self.assertEqual(report.approved_count, 0)
        self.assertEqual(report.unchanged_count, len(EXPECTED))
        self.assertEqual(store.validation_writes, len(EXPECTED))
        self.assertEqual(store.approval_writes, 0)

    async def test_disconnect_finishes_before_lock_release_on_cancellation(self):
        resolve_gate = asyncio.Event()
        disconnect_gate = asyncio.Event()
        client = FakeClient(
            {username: channel(source_id) for source_id, username in EXPECTED.items()},
            (),
            self.events,
            resolve_gate=resolve_gate,
            disconnect_gate=disconnect_gate,
        )
        service, store, _ = self.make_service(client=client)
        task = asyncio.create_task(service.plan(source_ids=CANARY_SELECTION))
        while "resolve:freelansim_ru" not in self.events:
            await asyncio.sleep(0)
        task.cancel()
        while "client.disconnect.started" not in self.events:
            await asyncio.sleep(0)
        self.assertNotIn("lock.release", self.events)
        disconnect_gate.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertLess(
            self.events.index("client.disconnect.completed"),
            self.events.index("lock.release"),
        )
        self.assertEqual(store.finalize_calls, [])

    async def test_exact_confirmation_and_safe_actor_reason_are_required(self):
        service, store, client = self.make_service()
        for kwargs in (
            {"actor": "bad actor", "reason": "ok", "confirm_exact": CANARY_CONFIRMATION},
            {"actor": "operator", "reason": "\n", "confirm_exact": CANARY_CONFIRMATION},
            {
                "actor": "operator",
                "reason": "ok",
                "confirm_exact": ",".join(
                    str(value) for value in reversed(CANARY_SORTED_IDS)
                ),
            },
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(SourceOnboardingError):
                    await service.apply(source_ids=CANARY_SELECTION, **kwargs)
        self.assertEqual(store.load_calls, [])
        self.assertEqual(client.joined_source_ids, [])

    async def test_source_scope_rejects_missing_duplicate_and_unselected_ids(self):
        service, store, client = self.make_service()
        for values in (
            CANARY_SELECTION[:-1],
            (*CANARY_SELECTION[:-1], CANARY_SELECTION[-2]),
            (*CANARY_SELECTION[:-1], 99),
            tuple(range(1, 12)),
        ):
            with self.subTest(values=values):
                with self.assertRaises(SourceOnboardingError):
                    await service.plan(source_ids=values)
        self.assertEqual(store.load_calls, [])
        self.assertEqual(client.joined_source_ids, [])


class ParserTests(unittest.TestCase):
    def test_parser_requires_repeated_positive_ids(self):
        parser = build_parser()
        argv = ["apply"]
        for source_id in CANARY_SELECTION:
            argv.extend(("--source-id", str(source_id)))
        argv.extend(
            (
                "--actor",
                "operator",
                "--reason",
                "first_canary",
                "--confirm-exact",
                CANARY_CONFIRMATION,
            )
        )
        args = parser.parse_args(argv)
        self.assertEqual(args.source_id, list(CANARY_SELECTION))
        with self.assertRaises(SystemExit):
            parser.parse_args(["plan"])
        with self.assertRaises(SystemExit):
            parser.parse_args(["plan", "--source-id", "0"])

    def test_unexpected_value_error_is_redacted(self):
        secret = "sensitive-unexpected-value"
        telegram_settings = SimpleNamespace(
            require_credentials=lambda: TelegramCredentials(
                api_id=123,
                api_hash="hidden",
                session_path=Path(".local/telegram/collector.session"),
            )
        )
        database_settings = SimpleNamespace(
            require_url=lambda: "postgresql+psycopg://local/test"
        )
        stderr = io.StringIO()
        argv = ["plan"]
        for source_id in CANARY_SELECTION:
            argv.extend(("--source-id", str(source_id)))
        with (
            patch(
                "ai_sales_radar.source_onboarding.TelegramAuthSettings.from_env",
                return_value=telegram_settings,
            ),
            patch(
                "ai_sales_radar.source_onboarding.DatabaseSettings.from_env",
                return_value=database_settings,
            ),
            patch(
                "ai_sales_radar.source_onboarding.RuntimeConfig.from_env",
                return_value=SimpleNamespace(),
            ),
            patch(
                "ai_sales_radar.source_onboarding._execute",
                AsyncMock(side_effect=ValueError(secret)),
            ),
            contextlib.redirect_stderr(stderr),
        ):
            exit_code = main(argv)

        self.assertEqual(exit_code, 1)
        self.assertNotIn(secret, stderr.getvalue())
        self.assertIn("ValueError", stderr.getvalue())


class FailingValidationRepository(DiscoveryCampaignRepository):
    def __init__(self, fail_on_call: int):
        self.calls = 0
        self.fail_on_call = fail_on_call

    async def upsert_validation(self, connection, **kwargs):
        self.calls += 1
        await super().upsert_validation(connection, **kwargs)
        if self.calls == self.fail_on_call:
            raise RuntimeError("simulated validation persistence failure")


class FailingApprovalRepository(SourceRepository):
    def __init__(self, fail_on_call: int):
        self.calls = 0
        self.fail_on_call = fail_on_call

    async def override(self, connection, source_id, target, **kwargs):
        result = await super().override(
            connection,
            source_id,
            target,
            **kwargs,
        )
        self.calls += 1
        if self.calls == self.fail_on_call:
            raise RuntimeError("simulated approval persistence failure")
        return result


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class SourceOnboardingPostgresIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=2, max_overflow=0)

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def seed_snapshot(self):
        await SourceCatalogService(self.database).import_seed(DEFAULT_SOURCES_PATH)
        await SourceCatalogService(self.database).import_seed(LEADGEN_SOURCES_PATH)
        async with self.database.transaction() as connection:
            account = await CollectorAccountRepository().ensure(
                connection,
                platform="telegram",
                external_account_id="70001",
                display_name="Test collector",
            )
        store = PostgresSourceOnboardingStore(self.database)
        snapshot = await store.load_selected(
            external_account_id=account.external_account_id,
            source_ids=CANARY_SELECTION,
        )
        bindings = tuple(
            SimpleNamespace(
                source_id=source.id,
                configured_username=EXPECTED[source.id],
                canonical_peer_id=100_000 + source.id,
            )
            for source in snapshot.sources
        )
        return store, snapshot, bindings

    async def test_finalize_persists_joined_validations_and_atomic_approvals(self):
        store, snapshot, bindings = await self.seed_snapshot()

        result = await store.finalize(
            snapshot=snapshot,
            bindings=bindings,
            actor="integration-test",
            reason="fixed_canary",
        )

        self.assertEqual(result.approved_source_ids, CANARY_SORTED_IDS)
        async with self.database.connect() as connection:
            statuses = (
                await connection.execute(
                    sa.select(sources.c.id, sources.c.lifecycle_status)
                    .where(sources.c.id.in_(CANARY_SORTED_IDS))
                    .order_by(sources.c.id)
                )
            ).all()
            validations = (
                await connection.execute(
                    sa.select(
                        telegram_source_validations.c.source_id,
                        telegram_source_validations.c.state,
                        telegram_source_validations.c.access_mode,
                    )
                    .where(
                        telegram_source_validations.c.source_id.in_(CANARY_SORTED_IDS)
                    )
                    .order_by(telegram_source_validations.c.source_id)
                )
            ).all()
            access_count = int(
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(source_collector_access)
                )
                or 0
            )
            override_count = int(
                await connection.scalar(
                    sa.select(sa.func.count())
                    .select_from(source_lifecycle_events)
                    .where(
                        source_lifecycle_events.c.source_id.in_(CANARY_SORTED_IDS),
                        source_lifecycle_events.c.is_override.is_(True),
                    )
                )
                or 0
            )
        self.assertEqual(
            statuses,
            [(source_id, "approved") for source_id in CANARY_SORTED_IDS],
        )
        self.assertEqual(
            validations,
            [
                (source_id, "accessible", "joined")
                for source_id in CANARY_SORTED_IDS
            ],
        )
        self.assertEqual(access_count, 0)
        self.assertEqual(override_count, len(EXPECTED))

    async def test_existing_validation_identity_mismatch_approves_nothing(self):
        store, snapshot, bindings = await self.seed_snapshot()
        async with self.database.transaction() as connection:
            await DiscoveryCampaignRepository().upsert_validation(
                connection,
                source_id=3,
                collector_account_id=snapshot.collector_account.id,
                state="accessible",
                access_mode="joined",
                canonical_peer_identity="peer:999999",
                checked_at=NOW,
                checked_by="integration-test",
            )

        with self.assertRaisesRegex(SourceOnboardingError, "не совпала"):
            await store.finalize(
                snapshot=snapshot,
                bindings=bindings,
                actor="integration-test",
                reason="fixed_canary",
            )

        async with self.database.connect() as connection:
            statuses = (
                await connection.execute(
                    sa.select(sources.c.lifecycle_status).where(
                        sources.c.id.in_(CANARY_SORTED_IDS)
                    )
                )
            ).scalars().all()
            validation_count = int(
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(
                        telegram_source_validations
                    )
                )
                or 0
            )
        self.assertEqual(set(statuses), {"candidate"})
        self.assertEqual(validation_count, 1)

    async def test_validation_write_failure_rolls_back_aliases_and_approvals(self):
        _, snapshot, bindings = await self.seed_snapshot()
        library = FailingValidationRepository(fail_on_call=3)
        store = PostgresSourceOnboardingStore(
            self.database,
            library_repository=library,
        )

        with self.assertRaisesRegex(RuntimeError, "persistence failure"):
            await store.finalize(
                snapshot=snapshot,
                bindings=bindings,
                actor="integration-test",
                reason="fixed_canary",
            )

        async with self.database.connect() as connection:
            statuses = (
                await connection.execute(
                    sa.select(sources.c.lifecycle_status).where(
                        sources.c.id.in_(CANARY_SORTED_IDS)
                    )
                )
            ).scalars().all()
            validation_count = int(
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(
                        telegram_source_validations
                    )
                )
                or 0
            )
            alias_count = int(
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(source_reference_aliases)
                )
                or 0
            )
        self.assertEqual(set(statuses), {"candidate"})
        self.assertEqual(validation_count, 0)
        self.assertEqual(alias_count, 0)

    async def test_mid_approval_failure_rolls_back_all_statuses_and_events(self):
        _, snapshot, bindings = await self.seed_snapshot()
        source_repository = FailingApprovalRepository(fail_on_call=3)
        store = PostgresSourceOnboardingStore(
            self.database,
            source_repository=source_repository,
        )

        with self.assertRaisesRegex(RuntimeError, "approval persistence failure"):
            await store.finalize(
                snapshot=snapshot,
                bindings=bindings,
                actor="integration-test",
                reason="fixed_canary",
            )

        async with self.database.connect() as connection:
            statuses = (
                await connection.execute(
                    sa.select(sources.c.lifecycle_status).where(
                        sources.c.id.in_(CANARY_SORTED_IDS)
                    )
                )
            ).scalars().all()
            validation_count = int(
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(
                        telegram_source_validations
                    )
                )
                or 0
            )
            alias_count = int(
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(source_reference_aliases)
                )
                or 0
            )
            override_count = int(
                await connection.scalar(
                    sa.select(sa.func.count())
                    .select_from(source_lifecycle_events)
                    .where(
                        source_lifecycle_events.c.source_id.in_(CANARY_SORTED_IDS),
                        source_lifecycle_events.c.is_override.is_(True),
                    )
                )
                or 0
            )
        self.assertEqual(set(statuses), {"candidate"})
        self.assertEqual(validation_count, 0)
        self.assertEqual(alias_count, 0)
        self.assertEqual(override_count, 0)


if __name__ == "__main__":
    unittest.main()

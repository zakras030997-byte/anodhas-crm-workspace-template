from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import io
import json
import logging
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch
from uuid import UUID

from ai_sales_radar.config import (
    ConfigurationError,
    DatabaseSettings,
    OperatorBotSettings,
    ReplyDraftSettings,
)
from ai_sales_radar.operator_bot.auth import OwnerGate
from ai_sales_radar.operator_bot.callbacks import (
    CallbackAction,
    CallbackDecodeError,
    SignedCallbackCodec,
)
from ai_sales_radar.operator_bot.cli import (
    inspect_operator_bot_configuration,
    main,
)
from ai_sales_radar.operator_bot.controller import OperatorBotController
from ai_sales_radar.operator_bot.models import (
    OperatorDraft,
    OperatorDiscoveredSource,
    OperatorLead,
    OperatorSource,
    OperatorSourceDiscoveryReport,
    OperatorSourceDiscoveryStart,
    OperatorStatusSnapshot,
)
from ai_sales_radar.operator_bot.presenters import sources_response
from ai_sales_radar.operator_bot.telethon_runtime import (
    OperatorBotIdentityError,
    TelethonOperatorTransport,
    run_operator_bot,
)
from ai_sales_radar.operator_bot.service import DatabaseOperatorCore
from ai_sales_radar.persistence.inbox import InboxStatus


NOW = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)
OWNER_ID = 70001
LEAD_ID = UUID("11111111-2222-3333-4444-555555555555")
BOT_TOKEN = "123456789:" + "A" * 35


def status_fixture() -> OperatorStatusSnapshot:
    return OperatorStatusSnapshot(
        database_reachable=True,
        migration_current=True,
        aggregates_readable=True,
        active_primary_profiles=1,
        queued_jobs=0,
        running_jobs=0,
        failed_jobs=1,
        raw_messages=5,
        opportunities=4,
        inbox_items=1,
        today_ai_calls=5,
        today_ai_spend_usd="0.005",
        month_ai_calls=5,
        month_ai_spend_usd="0.005",
        jobs_by_type={},
    )


def lead_fixture() -> OperatorLead:
    return OperatorLead(
        item_id=LEAD_ID,
        revision=3,
        workflow_status="new",
        commercial_score=84,
        commercial_band="hot",
        body_html="<b>AI automation</b>\n\nИсточник",
        source_url="https://t.me/example_channel_012/10",
    )


class FakeCore:
    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.lead = lead_fixture()

    async def status(self):
        self.calls.append(("status",))
        return status_fixture()

    async def list_sources(self, *, status, limit):
        self.calls.append(("sources", status, limit))
        return (
            OperatorSource(3, "Digital Tender", "@digitaltender", "approved"),
        )

    async def list_leads(self, *, tier, include_closed, limit):
        self.calls.append(("leads", tier, include_closed, limit))
        return (self.lead,)

    async def show_lead(self, lead_id):
        self.calls.append(("show", lead_id))
        return self.lead

    async def update_lead_status(
        self,
        lead_id,
        target,
        *,
        expected_revision,
        actor_id,
        reason,
        request_id,
    ):
        self.calls.append(
            (
                "update",
                lead_id,
                target.value,
                expected_revision,
                actor_id,
                reason,
                request_id,
            )
        )
        self.lead = replace(
            self.lead,
            revision=self.lead.revision + 1,
            workflow_status=target.value,
        )
        return self.lead

    async def create_reply_draft(self, lead_id, *, expected_revision):
        self.calls.append(("draft", lead_id, expected_revision))
        return OperatorDraft(
            lead_id=lead_id,
            proposal_draft="Готов обсудить задачу и предложить безопасный план работ.",
            short_reply="Готов обсудить детали проекта.",
            questions_to_client=("Какой срок?",),
            risks=("Не уточнён доступ к API.",),
        )

    async def start_source_discovery(self):
        self.calls.append(("source_discovery_start",))
        return OperatorSourceDiscoveryStart(
            job_id=UUID("22222222-3333-4444-5555-666666666666"),
            minimum_participants=500,
            cooldown_seconds=1800,
        )

    async def latest_source_discovery(self):
        self.calls.append(("source_discovery_results",))
        return OperatorSourceDiscoveryReport(
            status="completed",
            requested_at=NOW,
            finished_at=NOW,
            minimum_participants=500,
            candidates=(
                OperatorDiscoveredSource(
                    source_id=25,
                    display_name="AI заказчики & исполнители",
                    handle="@ai_marketplace",
                    canonical_url="https://t.me/example_channel_013",
                    status="candidate",
                    peer_type="supergroup",
                    participants_count=12_500,
                    relevance_score="0.86",
                    relevance_class="strong",
                    query_hit_count=4,
                    message_hit_count=7,
                ),
            ),
        )


def controller_for(core: FakeCore) -> OperatorBotController:
    return OperatorBotController(
        core,
        gate=OwnerGate(OWNER_ID, OWNER_ID),
        callback_codec=SignedCallbackCodec(BOT_TOKEN, ttl_seconds=900),
        clock=lambda: NOW,
    )


class OwnerGateTest(unittest.TestCase):
    def test_only_exact_private_owner_is_allowed(self):
        gate = OwnerGate(OWNER_ID, OWNER_ID)
        self.assertTrue(
            gate.allows(
                is_private=True,
                sender_id=OWNER_ID,
                chat_id=OWNER_ID,
            )
        )
        denied = (
            (False, OWNER_ID, OWNER_ID),
            (True, OWNER_ID + 1, OWNER_ID),
            (True, OWNER_ID, OWNER_ID + 1),
            (True, None, OWNER_ID),
            (True, OWNER_ID, None),
            (True, True, True),
        )
        for is_private, sender_id, chat_id in denied:
            with self.subTest(
                is_private=is_private,
                sender_id=sender_id,
                chat_id=chat_id,
            ):
                self.assertFalse(
                    gate.allows(
                        is_private=is_private,
                        sender_id=sender_id,
                        chat_id=chat_id,
                    )
                )


class SignedCallbackCodecTest(unittest.TestCase):
    def test_round_trip_is_authenticated_bounded_and_expires(self):
        codec = SignedCallbackCodec(BOT_TOKEN, ttl_seconds=900)
        encoded = codec.encode(
            CallbackAction.SHORTLIST,
            LEAD_ID,
            3,
            now=NOW,
        )
        self.assertLessEqual(len(encoded), 64)
        decoded = codec.decode(encoded, now=NOW + timedelta(seconds=899))
        self.assertEqual(decoded.action, CallbackAction.SHORTLIST)
        self.assertEqual(decoded.lead_id, LEAD_ID)
        self.assertEqual(decoded.expected_revision, 3)
        with self.assertRaises(CallbackDecodeError):
            codec.decode(encoded, now=NOW + timedelta(seconds=901))

    def test_tamper_and_wrong_bot_token_are_rejected(self):
        codec = SignedCallbackCodec(BOT_TOKEN)
        encoded = codec.encode(CallbackAction.SKIP, LEAD_ID, 3, now=NOW)
        tampered = encoded[:-1] + bytes([encoded[-1] ^ 1])
        with self.assertRaises(CallbackDecodeError):
            codec.decode(tampered, now=NOW)
        with self.assertRaises(CallbackDecodeError):
            SignedCallbackCodec("987654321:" + "B" * 35).decode(
                encoded,
                now=NOW,
            )
        with self.assertRaises(CallbackDecodeError):
            codec.decode(encoded + b"=", now=NOW)


class OperatorControllerTest(unittest.IsolatedAsyncioTestCase):
    async def test_unauthorized_updates_never_reach_core(self):
        core = FakeCore()
        controller = controller_for(core)
        responses = await controller.handle_message(
            is_private=True,
            sender_id=OWNER_ID + 1,
            chat_id=OWNER_ID + 1,
            text="/status",
        )
        callback = await controller.handle_callback(
            is_private=False,
            sender_id=OWNER_ID,
            chat_id=OWNER_ID,
            data=b"anything",
        )
        self.assertEqual(responses, ())
        self.assertIsNone(callback)
        self.assertEqual(core.calls, [])

    async def test_owner_can_read_status_sources_and_signed_leads(self):
        core = FakeCore()
        controller = controller_for(core)
        identity = {
            "is_private": True,
            "sender_id": OWNER_ID,
            "chat_id": OWNER_ID,
        }
        status = await controller.handle_message(**identity, text="/status")
        sources = await controller.handle_message(**identity, text="/sources")
        leads = await controller.handle_message(**identity, text="/leads hot")
        self.assertIn("не доказывает", status[0].text)
        self.assertIn("@digitaltender", sources[0].text)
        self.assertEqual(leads[0].parse_mode, "html")
        callbacks = [
            button.callback_data
            for row in leads[0].buttons
            for button in row
            if button.callback_data is not None
        ]
        self.assertEqual(len(callbacks), 7)
        self.assertTrue(all(len(value) <= 64 for value in callbacks))
        self.assertIn(("leads", "hot", False, 5), core.calls)

    async def test_draft_requires_second_explicit_confirmation(self):
        core = FakeCore()
        controller = controller_for(core)
        identity = {
            "is_private": True,
            "sender_id": OWNER_ID,
            "chat_id": OWNER_ID,
        }
        prompt_data = SignedCallbackCodec(BOT_TOKEN).encode(
            CallbackAction.DRAFT_PROMPT,
            LEAD_ID,
            3,
            now=NOW,
        )
        prompt = await controller.handle_callback(**identity, data=prompt_data)
        self.assertIsNotNone(prompt)
        self.assertNotIn("draft", [call[0] for call in core.calls])
        confirm_data = prompt.responses[0].buttons[0][0].callback_data
        result = await controller.handle_callback(**identity, data=confirm_data)
        self.assertIsNotNone(result)
        self.assertEqual(
            [call[0] for call in core.calls].count("draft"),
            1,
        )
        self.assertIn("Автоматическая отправка отключена", result.responses[-1].text)

    async def test_status_callback_uses_revision_and_fixed_reason(self):
        core = FakeCore()
        controller = controller_for(core)
        data = SignedCallbackCodec(BOT_TOKEN).encode(
            CallbackAction.NOT_FIT,
            LEAD_ID,
            3,
            now=NOW,
        )
        result = await controller.handle_callback(
            is_private=True,
            sender_id=OWNER_ID,
            chat_id=OWNER_ID,
            data=data,
        )
        self.assertIsNotNone(result)
        update = next(call for call in core.calls if call[0] == "update")
        self.assertEqual(update[2:7], ("ignored", 3, "telegram-operator", "operator_not_fit", update[6]))
        self.assertEqual(len(update[6]), 64)

    async def test_start_menu_has_source_search_button_and_callback_enqueues_job(self):
        core = FakeCore()
        controller = controller_for(core)
        identity = {
            "is_private": True,
            "sender_id": OWNER_ID,
            "chat_id": OWNER_ID,
        }
        menu = await controller.handle_message(**identity, text="/source_catalog")
        search_button = next(
            button
            for row in menu[0].buttons
            for button in row
            if "Найти источники" in button.label
        )
        outcome = await controller.handle_callback(
            **identity,
            data=search_button.callback_data,
        )
        self.assertIsNotNone(outcome)
        self.assertIn("поставлен в очередь", outcome.responses[0].text)
        self.assertIn(("source_discovery_start",), core.calls)

    async def test_source_results_button_shows_members_and_candidate_only_state(self):
        core = FakeCore()
        controller = controller_for(core)
        identity = {
            "is_private": True,
            "sender_id": OWNER_ID,
            "chat_id": OWNER_ID,
        }
        menu = await controller.handle_message(**identity, text="/source_catalog")
        results_button = next(
            button
            for row in menu[0].buttons
            for button in row
            if "Результаты" in button.label
        )
        outcome = await controller.handle_callback(
            **identity,
            data=results_button.callback_data,
        )
        self.assertIsNotNone(outcome)
        self.assertIn("12 500 участников", outcome.responses[0].text)
        self.assertIn("только как кандидаты", outcome.responses[0].text)


class OperatorCoreLeadSelectionTest(unittest.IsolatedAsyncioTestCase):
    async def test_active_workflow_statuses_are_filtered_before_limit(self):
        profile_id = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        records = {
            InboxStatus.NEW: SimpleNamespace(
                id=UUID("00000000-0000-0000-0000-000000000001"),
                commercial_score=10,
                last_matched_at=NOW,
            ),
            InboxStatus.OPENED: SimpleNamespace(
                id=UUID("00000000-0000-0000-0000-000000000002"),
                commercial_score=20,
                last_matched_at=NOW,
            ),
            InboxStatus.SHORTLISTED: SimpleNamespace(
                id=UUID("00000000-0000-0000-0000-000000000003"),
                commercial_score=30,
                last_matched_at=NOW,
            ),
        }
        inbox = Mock()
        inbox.list_leads = AsyncMock(
            side_effect=lambda **kwargs: (records[kwargs["status"]],)
        )
        core = DatabaseOperatorCore(
            Mock(),
            ReplyDraftSettings(_env_file=None),
            inbox=inbox,
            profiles=Mock(),
            sources=Mock(),
            drafts=Mock(),
        )
        core._require_active_primary_profile = AsyncMock(
            return_value=SimpleNamespace(id=profile_id)
        )
        by_id = {
            record.id: replace(
                lead_fixture(),
                item_id=record.id,
                commercial_score=record.commercial_score,
            )
            for record in records.values()
        }
        core._load_current_lead = AsyncMock(
            side_effect=lambda item_id, _profile_id: by_id[item_id]
        )

        leads = await core.list_leads(
            tier=None,
            include_closed=False,
            limit=2,
        )

        self.assertEqual([item.commercial_score for item in leads], [30, 20])
        queried_statuses = {
            call.kwargs["status"] for call in inbox.list_leads.await_args_list
        }
        self.assertEqual(queried_statuses, {
            InboxStatus.NEW,
            InboxStatus.OPENED,
            InboxStatus.SHORTLISTED,
        })
        self.assertTrue(
            all(call.kwargs["limit"] == 2 for call in inbox.list_leads.await_args_list)
        )


class PresentationSecurityTest(unittest.TestCase):
    def test_source_text_is_html_escaped(self):
        response = sources_response(
            (
                OperatorSource(
                    1,
                    "<script>alert(1)</script>",
                    "@x<&",
                    "approved",
                ),
            ),
            requested_status="approved",
        )
        self.assertNotIn("<script>", response.text)
        self.assertIn("&lt;&amp;", response.text)


class OperatorConfigTest(unittest.TestCase):
    def test_ready_check_contains_no_ids_token_or_path(self):
        bot = OperatorBotSettings(
            enabled=True,
            api_id=12345,
            api_hash="api-hash-secret",
            bot_token=BOT_TOKEN,
            owner_user_id=OWNER_ID,
            owner_chat_id=OWNER_ID,
            _env_file=None,
        )
        database = DatabaseSettings(
            database_url="postgresql+psycopg://local/radar",
            _env_file=None,
        )
        report = inspect_operator_bot_configuration(database, bot)
        serialized = json.dumps({**report.__dict__, "ready": report.ready})
        self.assertTrue(report.ready)
        for private in (BOT_TOKEN, str(OWNER_ID), "api-hash-secret", ".local"):
            self.assertNotIn(private, serialized)
        self.assertNotIn(BOT_TOKEN, repr(bot.require_credentials()))

    def test_from_env_hides_invalid_token_value(self):
        secret = "12345:THIS!VALUE_MUST_NEVER_BE_REPEATED"
        with tempfile.TemporaryDirectory() as directory:
            env_path = Path(directory) / ".env"
            env_path.write_text(
                "AI_SALES_RADAR_OPERATOR_BOT_TOKEN=" + secret + "\n",
                encoding="utf-8",
            )
            with self.assertRaises(ConfigurationError) as caught:
                OperatorBotSettings.from_env(env_path)
        self.assertNotIn(secret, str(caught.exception))

    def test_unimplemented_proactive_notifications_are_rejected(self):
        with self.assertRaises(Exception):
            OperatorBotSettings(
                notifications_enabled=True,
                _env_file=None,
            )

    def test_cli_check_is_offline_and_does_not_expose_secrets(self):
        stdout = io.StringIO()
        with (
            patch("sys.stdout", stdout),
            patch("ai_sales_radar.operator_bot.cli.run_operator_bot") as run,
        ):
            code = main(["check"])
        run.assert_not_called()
        payload = json.loads(stdout.getvalue())
        self.assertFalse(payload["automatic_client_sending_enabled"])
        self.assertFalse(payload["collector_session_reused"])
        self.assertIn(code, {0, 1})


class FakeTelegramClient:
    def __init__(self, *, bot_id=123456789, events_log=None):
        self.bot_id = bot_id
        self.events_log = events_log if events_log is not None else []
        self.handlers = []
        self.sent = []

    def add_event_handler(self, callback, event=None):
        self.handlers.append((callback, event))

    async def start(self, *, bot_token):
        self.events_log.append("client.start")

    async def get_me(self):
        return SimpleNamespace(id=self.bot_id, bot=True)

    async def run_until_disconnected(self):
        self.events_log.append("client.run")

    async def send_message(self, entity, message, **kwargs):
        self.sent.append((entity, message, kwargs))

    async def disconnect(self):
        self.events_log.append("client.disconnect")


class FakeEvent:
    def __init__(self, *, sender_id, chat_id, is_private, text="", data=b""):
        self.sender_id = sender_id
        self.chat_id = chat_id
        self.is_private = is_private
        self.raw_text = text
        self.data = data
        self.answer = AsyncMock()


class TelethonTransportTest(unittest.IsolatedAsyncioTestCase):
    async def test_transport_ignores_foreign_message_and_sends_only_to_owner(self):
        core = FakeCore()
        controller = controller_for(core)
        client = FakeTelegramClient()
        logger = logging.getLogger("operator-bot-test")
        logger.handlers = [logging.NullHandler()]
        transport = TelethonOperatorTransport(
            client,
            controller,
            owner_chat_id=OWNER_ID,
            logger=logger,
        )
        await transport._on_message(
            FakeEvent(
                sender_id=OWNER_ID + 1,
                chat_id=OWNER_ID + 1,
                is_private=True,
                text="/status",
            )
        )
        self.assertEqual(client.sent, [])
        self.assertEqual(core.calls, [])
        await transport._on_message(
            FakeEvent(
                sender_id=OWNER_ID,
                chat_id=OWNER_ID,
                is_private=True,
                text="/status",
            )
        )
        self.assertTrue(client.sent)
        self.assertTrue(all(item[0] == OWNER_ID for item in client.sent))


class FakeLock:
    def __init__(self, events_log):
        self.events_log = events_log

    def acquire(self):
        self.events_log.append("lock.acquire")

    def release(self):
        self.events_log.append("lock.release")


class FakeDatabase:
    def __init__(self, events_log):
        self.events_log = events_log

    async def close(self):
        self.events_log.append("database.close")


class OperatorRuntimeTest(unittest.IsolatedAsyncioTestCase):
    def settings(self):
        return OperatorBotSettings(
            enabled=True,
            api_id=12345,
            api_hash="api-hash-secret",
            bot_token=BOT_TOKEN,
            owner_user_id=OWNER_ID,
            owner_chat_id=OWNER_ID,
            _env_file=None,
        )

    async def test_runtime_locks_before_client_and_closes_every_resource(self):
        events_log = []
        client = FakeTelegramClient(events_log=events_log)

        def lock_factory(_path, *, role):
            self.assertEqual(role, "ai_sales_radar_operator_bot")
            return FakeLock(events_log)

        def client_factory(_credentials):
            events_log.append("client.factory")
            return client

        with (
            patch(
                "ai_sales_radar.operator_bot.telethon_runtime.prepare_private_session_storage"
            ),
            patch(
                "ai_sales_radar.operator_bot.telethon_runtime.protect_session_files"
            ),
        ):
            await run_operator_bot(
                DatabaseSettings(
                    database_url="postgresql+psycopg://local/radar",
                    _env_file=None,
                ),
                self.settings(),
                ReplyDraftSettings(_env_file=None),
                research_factory=lambda db, **kw: None,
                database_factory=lambda *_args, **_kwargs: FakeDatabase(events_log),
                client_factory=client_factory,
                lock_factory=lock_factory,
            )
        self.assertLess(events_log.index("lock.acquire"), events_log.index("client.factory"))
        self.assertEqual(
            events_log[-3:],
            ["client.disconnect", "database.close", "lock.release"],
        )

    async def test_runtime_rejects_stale_session_for_another_bot(self):
        events_log = []
        client = FakeTelegramClient(bot_id=999999999, events_log=events_log)
        with (
            patch(
                "ai_sales_radar.operator_bot.telethon_runtime.prepare_private_session_storage"
            ),
            patch(
                "ai_sales_radar.operator_bot.telethon_runtime.protect_session_files"
            ),
            self.assertRaises(OperatorBotIdentityError),
        ):
            await run_operator_bot(
                DatabaseSettings(
                    database_url="postgresql+psycopg://local/radar",
                    _env_file=None,
                ),
                self.settings(),
                ReplyDraftSettings(_env_file=None),
                research_factory=lambda db, **kw: None,
                database_factory=lambda *_args, **_kwargs: FakeDatabase(events_log),
                client_factory=lambda _credentials: client,
                lock_factory=lambda *_args, **_kwargs: FakeLock(events_log),
            )
        self.assertEqual(events_log[-1], "lock.release")


if __name__ == "__main__":
    unittest.main()

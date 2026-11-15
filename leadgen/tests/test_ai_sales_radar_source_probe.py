from __future__ import annotations

import contextlib
from datetime import UTC, datetime
import io
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from telethon.errors import FloodWaitError

from ai_sales_radar.config import TelegramCredentials
from ai_sales_radar.source_probe import (
    CandidateSourceProbeResult,
    SourceProbeOptions,
    SourceProbeReport,
    TelegramCandidateSourceProbe,
    build_parser,
    main,
)
from freelancer_bot.persistence.source_repository import (
    SourceLineageRecord,
    SourceRecord,
    SourceStatus,
)


NOW = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)


def source_record(
    source_id: int,
    handle: str | None,
    *,
    canonical_url: str | None = None,
    status: SourceStatus = SourceStatus.CANDIDATE,
) -> SourceRecord:
    return SourceRecord(
        id=source_id,
        platform="telegram",
        external_id=f"username:probe_{source_id}",
        access_type="public",
        lifecycle_status=status,
        display_name=f"Configured source {source_id}",
        handle=handle,
        canonical_url=canonical_url,
        created_at=NOW,
        updated_at=NOW,
    )


def seed_lineage(source_id: int, **context) -> SourceLineageRecord:
    return SourceLineageRecord(
        id=source_id * 10,
        source_id=source_id,
        provider="ai_sales_radar_seed",
        lineage_key=f"config-sources-json:probe_{source_id}",
        provider_run_id="fixture",
        discovery_run_id=None,
        seed_source_id=None,
        seed_reference=f"@probe_{source_id}",
        discovered_at=NOW,
        context=context,
    )


class AsyncContext:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class FakeDatabase:
    def __init__(self, events):
        self.events = events
        self.connection = object()
        self.connect_count = 0
        self.transaction_count = 0

    def connect(self):
        self.connect_count += 1
        self.events.append("database.connect")
        return AsyncContext(self.connection)

    def transaction(self):
        self.transaction_count += 1
        raise AssertionError("source probe must not open a write transaction")


class FakeRepository:
    def __init__(self, sources, lineage, events):
        self.sources = tuple(sources)
        self.lineage = lineage
        self.events = events
        self.list_arguments = None
        self.lineage_calls = []

    async def list_sources(self, _connection, **kwargs):
        self.events.append("repository.list_sources")
        self.list_arguments = kwargs
        return self.sources

    async def list_lineage(self, _connection, source_id):
        self.events.append(f"repository.list_lineage:{source_id}")
        self.lineage_calls.append(source_id)
        return self.lineage.get(source_id, ())


class FakeLock:
    def __init__(self, _path, *, role, events):
        self.role = role
        self.events = events

    def acquire(self):
        self.events.append("lock.acquire")

    def release(self):
        self.events.append("lock.release")


class FakeEntity:
    def __init__(self, *, title, username, participants_count):
        self.title = title
        self.username = username
        self.participants_count = participants_count
        self.id = 987654321
        self.access_hash = 123456789


class FakeClient:
    def __init__(self, responses, events, *, authorized=True):
        self.responses = responses
        self.events = events
        self.authorized = authorized

    async def connect(self):
        self.events.append("client.connect")

    async def disconnect(self):
        self.events.append("client.disconnect")

    async def is_user_authorized(self):
        self.events.append("client.is_user_authorized")
        return self.authorized

    async def get_me(self):
        self.events.append("client.get_me")
        return SimpleNamespace(id=70001)

    async def get_entity(self, lookup):
        self.events.append(f"client.get_entity:{lookup}")
        response = self.responses[lookup]
        if isinstance(response, BaseException):
            raise response
        return response

    def iter_messages(self, *_args, **_kwargs):
        raise AssertionError("source probe must not read messages")

    async def join(self, *_args, **_kwargs):
        raise AssertionError("source probe must not join sources")


def build_service(root, database, repository, client, events, sleep):
    credentials = TelegramCredentials(
        api_id=12345,
        api_hash="private-api-hash",
        session_path=root / "collector.session",
    )

    def lock_factory(path, *, role):
        return FakeLock(path, role=role, events=events)

    def client_factory(_credentials):
        events.append("client.factory")
        return client

    return TelegramCandidateSourceProbe(
        database,
        credentials,
        repository=repository,
        client_factory=client_factory,
        lock_factory=lock_factory,
        runtime_root=root,
        sleep=sleep,
    )


class SourceProbeOptionsTest(unittest.TestCase):
    def test_limits_and_cli_defaults_are_bounded(self):
        self.assertEqual(SourceProbeOptions().source_limit, 100)
        self.assertEqual(SourceProbeOptions().request_delay_seconds, 1.0)
        with self.assertRaises(ValueError):
            SourceProbeOptions(source_limit=1_001)
        with self.assertRaises(ValueError):
            SourceProbeOptions(request_delay_seconds=float("nan"))

        parser = build_parser()
        self.assertEqual(parser.parse_args([]).source_limit, 100)
        self.assertEqual(
            parser.parse_args(["--source-limit", "1000"]).source_limit,
            1_000,
        )
        with self.assertRaises(SystemExit):
            parser.parse_args(["--source-limit", "1001"])
        with self.assertRaises(SystemExit):
            parser.parse_args(["--request-delay-seconds", "0"])


class TelegramCandidateSourceProbeTest(unittest.IsolatedAsyncioTestCase):
    async def test_probe_is_read_only_sequential_safe_and_complete(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            sources = (
                source_record(
                    1,
                    "@probe_one",
                    canonical_url="https://t.me/example_channel_017",
                ),
                source_record(2, "@probe_two"),
                source_record(3, None, canonical_url=None),
            )
            repository = FakeRepository(
                sources,
                {
                    1: (
                        seed_lineage(
                            1,
                            enabled=True,
                            tags=["freelance", "development"],
                            reason="Seed reason",
                        ),
                    ),
                },
                events,
            )
            database = FakeDatabase(events)
            client = FakeClient(
                {
                    "@probe_one": FakeEntity(
                        title="Live title",
                        username="probe_one",
                        participants_count=321,
                    ),
                    "@probe_two": ValueError("PRIVATE PROVIDER CANARY"),
                },
                events,
            )

            async def fake_sleep(delay):
                events.append(f"sleep:{delay}")

            service = build_service(
                root,
                database,
                repository,
                client,
                events,
                fake_sleep,
            )
            report = await service.run(
                options=SourceProbeOptions(
                    source_limit=3,
                    request_delay_seconds=0.25,
                )
            )

        self.assertEqual(
            repository.list_arguments,
            {
                "status": SourceStatus.CANDIDATE,
                "platform": "telegram",
                "limit": 3,
            },
        )
        self.assertEqual(repository.lineage_calls, [1, 2, 3])
        self.assertEqual(database.connect_count, 1)
        self.assertEqual(database.transaction_count, 0)
        self.assertEqual(
            (report.candidate_count, report.available_count, report.unavailable_count),
            (3, 1, 2),
        )
        first, second, third = report.results
        self.assertEqual(first.configured_title, "Configured source 1")
        self.assertEqual(first.configured_handle, "@probe_one")
        self.assertEqual(first.configured_url, "https://t.me/example_channel_017")
        self.assertEqual(first.seed_enabled, True)
        self.assertEqual(first.tags, ("freelance", "development"))
        self.assertEqual(first.reason, "Seed reason")
        self.assertTrue(first.available)
        self.assertEqual(first.entity_kind, "FakeEntity")
        self.assertEqual(first.entity_title, "Live title")
        self.assertEqual(first.entity_username, "probe_one")
        self.assertEqual(first.participants_count, 321)
        self.assertFalse(second.available)
        self.assertEqual(second.error_type, "ValueError")
        self.assertFalse(third.available)
        self.assertEqual(third.error_type, "LookupUnavailable")
        self.assertNotIn("PRIVATE PROVIDER CANARY", repr(report))
        self.assertEqual(events.count("sleep:0.25"), 1)
        self.assertLess(events.index("lock.acquire"), events.index("client.factory"))
        telegram_calls = [
            event for event in events if event.startswith("client.")
        ]
        self.assertEqual(
            telegram_calls,
            [
                "client.factory",
                "client.connect",
                "client.is_user_authorized",
                "client.get_me",
                "client.get_entity:@probe_one",
                "client.get_entity:@probe_two",
                "client.disconnect",
            ],
        )
        self.assertEqual(events[-2:], ["client.disconnect", "lock.release"])

    async def test_canonical_public_url_is_resolved_without_joining(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            source = source_record(
                4,
                None,
                canonical_url="https://t.me/example_channel_018",
            )
            repository = FakeRepository((source,), {}, events)
            database = FakeDatabase(events)
            client = FakeClient(
                {
                    "@probe_four": FakeEntity(
                        title="Four",
                        username="probe_four",
                        participants_count=None,
                    )
                },
                events,
            )

            async def no_sleep(_delay):
                self.fail("one request must not sleep")

            report = await build_service(
                root,
                database,
                repository,
                client,
                events,
                no_sleep,
            ).run()

        self.assertTrue(report.results[0].available)
        self.assertIn("client.get_entity:@probe_four", events)

    async def test_flood_wait_aborts_whole_probe_and_releases_session(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            sources = (
                source_record(1, "@probe_one"),
                source_record(2, "@probe_two"),
                source_record(3, "@probe_three"),
            )
            repository = FakeRepository(sources, {}, events)
            database = FakeDatabase(events)
            client = FakeClient(
                {
                    "@probe_one": FakeEntity(
                        title="One",
                        username="probe_one",
                        participants_count=1,
                    ),
                    "@probe_two": FloodWaitError(request=None, capture=30),
                    "@probe_three": FakeEntity(
                        title="Three",
                        username="probe_three",
                        participants_count=3,
                    ),
                },
                events,
            )

            async def fake_sleep(delay):
                events.append(f"sleep:{delay}")

            service = build_service(
                root,
                database,
                repository,
                client,
                events,
                fake_sleep,
            )

            with self.assertRaises(FloodWaitError):
                await service.run(
                    options=SourceProbeOptions(request_delay_seconds=0.1)
                )

        self.assertNotIn("client.get_entity:@probe_three", events)
        self.assertEqual(events[-2:], ["client.disconnect", "lock.release"])

    async def test_unexpected_account_error_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            sources = (
                source_record(1, "@probe_one"),
                source_record(2, "@probe_two"),
            )
            repository = FakeRepository(sources, {}, events)
            database = FakeDatabase(events)
            error = RuntimeError("ACCOUNT SECRET CANARY")
            client = FakeClient({"@probe_one": error}, events)

            async def no_sleep(_delay):
                self.fail("probe must abort before another request")

            service = build_service(
                root,
                database,
                repository,
                client,
                events,
                no_sleep,
            )
            with self.assertRaisesRegex(RuntimeError, "ACCOUNT SECRET CANARY"):
                await service.run()

        self.assertNotIn("client.get_entity:@probe_two", events)
        self.assertEqual(events[-2:], ["client.disconnect", "lock.release"])

    async def test_unauthorized_session_stops_before_get_me_or_entities(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            source = source_record(1, "@probe_one")
            repository = FakeRepository((source,), {}, events)
            database = FakeDatabase(events)
            client = FakeClient({}, events, authorized=False)

            async def no_sleep(_delay):
                self.fail("unauthorized probe must not sleep")

            service = build_service(
                root,
                database,
                repository,
                client,
                events,
                no_sleep,
            )
            with self.assertRaisesRegex(Exception, "не авторизована"):
                await service.run()

        self.assertNotIn("client.get_me", events)
        self.assertFalse(
            any(event.startswith("client.get_entity") for event in events)
        )
        self.assertEqual(events[-2:], ["client.disconnect", "lock.release"])


class SourceProbeCliTest(unittest.TestCase):
    def test_success_is_json_and_does_not_expose_entity_secrets(self):
        report = SourceProbeReport(
            candidate_count=1,
            available_count=1,
            unavailable_count=0,
            results=(
                CandidateSourceProbeResult(
                    source_id=1,
                    configured_title="Source",
                    configured_handle="@source",
                    configured_url="https://t.me/example_channel_019",
                    seed_enabled=True,
                    tags=("freelance",),
                    reason="Reason",
                    available=True,
                    entity_kind="Channel",
                    entity_title="Live source",
                    entity_username="source",
                    participants_count=25,
                ),
            ),
        )
        stdout = io.StringIO()
        with (
            patch(
                "ai_sales_radar.source_probe._run_command",
                new=AsyncMock(return_value=report),
            ),
            contextlib.redirect_stdout(stdout),
        ):
            exit_code = main([])

        payload = json.loads(stdout.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["results"][0]["source_id"], 1)
        self.assertNotIn("id", payload["results"][0])
        self.assertNotIn("access_hash", stdout.getvalue())

    def test_unexpected_error_json_omits_exception_text(self):
        stderr = io.StringIO()
        with (
            patch(
                "ai_sales_radar.source_probe._run_command",
                new=AsyncMock(
                    side_effect=RuntimeError("DATABASE PASSWORD CANARY")
                ),
            ),
            contextlib.redirect_stderr(stderr),
        ):
            exit_code = main([])

        payload = json.loads(stderr.getvalue())
        self.assertEqual(exit_code, 1)
        self.assertEqual(payload["error_type"], "RuntimeError")
        self.assertNotIn("DATABASE PASSWORD CANARY", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()

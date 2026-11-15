import contextlib
from datetime import UTC, datetime
import io
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import sqlalchemy as sa

from ai_sales_radar.source_catalog import (
    DEFAULT_SOURCES_PATH,
    SourceCatalogError,
    SourceCatalogService,
    SourceDetails,
    SourceTransitionResult,
    _emit,
    build_parser,
    main,
)
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.schema import sources
from freelancer_bot.persistence.source_repository import (
    SourceLifecycleEvent,
    SourceLineageRecord,
    SourceRecord,
    SourceRepository,
    SourceStatus,
)
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


NOW = datetime(2026, 8, 26, tzinfo=UTC)


def source_record(
    *,
    source_id: int = 1,
    status: SourceStatus = SourceStatus.CANDIDATE,
) -> SourceRecord:
    return SourceRecord(
        id=source_id,
        platform="telegram",
        external_id="username:test_source",
        access_type="public",
        lifecycle_status=status,
        display_name="Test source",
        handle="@test_source",
        canonical_url="https://t.me/example_channel_016",
        created_at=NOW,
        updated_at=NOW,
    )


class AsyncContext:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class FakeConnection:
    def __init__(self):
        self.executions = []

    async def execute(self, statement, parameters=None):
        self.executions.append((statement, parameters))


class FakeDatabase:
    def __init__(self):
        self.connection = FakeConnection()
        self.connect_count = 0
        self.transaction_count = 0

    def connect(self):
        self.connect_count += 1
        return AsyncContext(self.connection)

    def transaction(self):
        self.transaction_count += 1
        return AsyncContext(self.connection)


class FakeSeedRepository:
    def __init__(self):
        self.seeds = []
        self.identities = set()

    async def upsert_seed(self, _connection, seed):
        self.seeds.append(seed)
        identity = (seed.platform, seed.external_id)
        created = identity not in self.identities
        self.identities.add(identity)
        return SimpleNamespace(
            created=created,
            updated=False,
            lineage_created=created,
        )


class SourceCatalogSeedUnitTest(unittest.IsolatedAsyncioTestCase):
    async def test_repository_seed_imports_every_entry_as_candidate(self):
        database = FakeDatabase()
        repository = FakeSeedRepository()
        service = SourceCatalogService(database, repository)

        first = await service.import_seed(DEFAULT_SOURCES_PATH)
        second = await service.import_seed(DEFAULT_SOURCES_PATH)

        self.assertEqual((first.total, first.created, first.candidate_entries), (15, 15, 15))
        self.assertEqual(first.approved_entries, 0)
        self.assertEqual((first.updated, first.unchanged, first.lineage_created), (0, 0, 15))
        self.assertEqual((second.created, second.updated, second.unchanged), (0, 0, 15))
        self.assertEqual(second.lineage_created, 0)
        self.assertEqual(first.snapshot_sha256, second.snapshot_sha256)
        self.assertTrue(repository.seeds)
        self.assertEqual(
            {seed.initial_status for seed in repository.seeds},
            {SourceStatus.CANDIDATE},
        )
        self.assertEqual(
            {seed.context["enabled"] for seed in repository.seeds},
            {False, True},
        )
        self.assertEqual(database.transaction_count, 2)
        self.assertEqual(len(database.connection.executions), 2)

    async def test_seed_path_cannot_escape_project(self):
        service = SourceCatalogService(FakeDatabase(), FakeSeedRepository())

        with self.assertRaisesRegex(SourceCatalogError, "внутри проекта"):
            await service.import_seed(DEFAULT_SOURCES_PATH.parent.parent.parent / "README.md")


class FakeCatalogRepository:
    def __init__(self, record=None):
        self.record = record or source_record()
        self.override_calls = []
        self.list_arguments = None

    async def list_sources(self, _connection, **kwargs):
        self.list_arguments = kwargs
        return (self.record,)

    async def get(self, _connection, source_id):
        if source_id != self.record.id:
            raise AssertionError("unexpected source id")
        return self.record

    async def list_lineage(self, _connection, source_id):
        return [
            SourceLineageRecord(
                id=2,
                source_id=source_id,
                provider="ai_sales_radar_seed",
                lineage_key="config-sources-json:test_source",
                provider_run_id="snapshot",
                discovery_run_id=None,
                seed_source_id=None,
                seed_reference="@test_source",
                discovered_at=NOW,
                context={"enabled": True},
            )
        ]

    async def list_lifecycle_events(self, _connection, source_id):
        return [
            SourceLifecycleEvent(
                id=3,
                source_id=source_id,
                from_status=None,
                to_status=self.record.lifecycle_status,
                actor_kind="seed",
                actor_id=None,
                reason="repository source seed import",
                is_override=False,
                source_audit_id=None,
                changed_at=NOW,
            )
        ]

    async def override(
        self,
        _connection,
        source_id,
        target,
        *,
        operator_id,
        reason,
    ):
        self.override_calls.append((source_id, target, operator_id, reason))
        self.record = source_record(source_id=source_id, status=target)
        return self.record


class SourceCatalogServiceUnitTest(unittest.IsolatedAsyncioTestCase):
    async def test_list_and_show_are_read_only_and_include_history(self):
        database = FakeDatabase()
        repository = FakeCatalogRepository()
        service = SourceCatalogService(database, repository)

        listing = await service.list_sources(
            status="candidate",
            platform="telegram",
            limit=25,
        )
        details = await service.show_source(1)

        self.assertEqual(listing.count, 1)
        self.assertEqual(listing.sources[0].id, 1)
        self.assertEqual(
            repository.list_arguments,
            {"status": "candidate", "platform": "telegram", "limit": 25},
        )
        self.assertIsInstance(details, SourceDetails)
        self.assertEqual(details.lineage[0].provider, "ai_sales_radar_seed")
        self.assertEqual(details.lifecycle_events[0].actor_kind, "seed")
        self.assertEqual(database.connect_count, 2)
        self.assertEqual(database.transaction_count, 0)

    async def test_transition_is_explicit_operator_override(self):
        database = FakeDatabase()
        repository = FakeCatalogRepository()
        service = SourceCatalogService(database, repository)

        result = await service.transition_source(
            1,
            "approved",
            actor="local-operator",
            reason="manual review passed",
        )

        self.assertIsInstance(result, SourceTransitionResult)
        self.assertTrue(result.changed)
        self.assertEqual(result.previous_status, SourceStatus.CANDIDATE)
        self.assertEqual(result.source.lifecycle_status, SourceStatus.APPROVED)
        self.assertEqual(
            repository.override_calls,
            [
                (
                    1,
                    SourceStatus.APPROVED,
                    "local-operator",
                    "manual review passed",
                )
            ],
        )

    async def test_same_status_transition_is_idempotent(self):
        repository = FakeCatalogRepository(
            source_record(status=SourceStatus.PAUSED),
        )
        service = SourceCatalogService(FakeDatabase(), repository)

        result = await service.transition_source(
            1,
            SourceStatus.PAUSED,
            actor="local-operator",
            reason="keep paused",
        )

        self.assertFalse(result.changed)
        self.assertEqual(repository.override_calls, [])

    async def test_transition_rejects_non_operator_target_and_blank_evidence(self):
        database = FakeDatabase()
        service = SourceCatalogService(database, FakeCatalogRepository())

        with self.assertRaisesRegex(SourceCatalogError, "Целевой статус"):
            await service.transition_source(
                1,
                SourceStatus.ACTIVE,
                actor="local-operator",
                reason="collector connected",
            )
        with self.assertRaisesRegex(SourceCatalogError, "actor"):
            await service.transition_source(
                1,
                SourceStatus.APPROVED,
                actor=" ",
                reason="reviewed",
            )
        with self.assertRaisesRegex(SourceCatalogError, "reason"):
            await service.transition_source(
                1,
                SourceStatus.APPROVED,
                actor="local-operator",
                reason=" ",
            )

        self.assertEqual(database.transaction_count, 0)


class SourceCatalogCommandUnitTest(unittest.TestCase):
    def test_parser_exposes_only_three_operator_targets(self):
        parser = build_parser()

        parsed = parser.parse_args(
            [
                "transition",
                "17",
                "approved",
                "--actor",
                "operator-1",
                "--reason",
                "reviewed",
            ]
        )
        self.assertEqual(parsed.source_id, 17)
        self.assertEqual(parsed.target, "approved")

        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args(
                    [
                        "transition",
                        "17",
                        "active",
                        "--actor",
                        "operator-1",
                        "--reason",
                        "reviewed",
                    ]
                )

    def test_missing_command_never_reads_database_configuration(self):
        with (
            patch("ai_sales_radar.source_catalog.DatabaseSettings.from_env") as settings,
            contextlib.redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit),
        ):
            main([])

        settings.assert_not_called()

    def test_unexpected_failure_never_prints_database_url(self):
        secret_url = (
            "postgresql+psycopg://operator:" + "top-secret@localhost/radar"
        )
        settings = SimpleNamespace(require_url=lambda: secret_url)
        failure = AsyncMock(side_effect=RuntimeError(secret_url))
        stderr = io.StringIO()

        with (
            patch(
                "ai_sales_radar.source_catalog.DatabaseSettings.from_env",
                return_value=settings,
            ),
            patch("ai_sales_radar.source_catalog._execute", failure),
            contextlib.redirect_stderr(stderr),
        ):
            exit_code = main(["list"])

        self.assertEqual(exit_code, 1)
        self.assertNotIn("top-secret", stderr.getvalue())
        self.assertNotIn(secret_url, stderr.getvalue())
        self.assertIn("RuntimeError", stderr.getvalue())

    def test_output_is_stable_json_for_future_gui_adapter(self):
        output = io.StringIO()
        value = SourceTransitionResult(
            changed=True,
            previous_status=SourceStatus.CANDIDATE,
            source=source_record(status=SourceStatus.APPROVED),
        )

        with contextlib.redirect_stdout(output):
            _emit(value)

        payload = json.loads(output.getvalue())
        self.assertTrue(payload["changed"])
        self.assertEqual(payload["previous_status"], "candidate")
        self.assertEqual(payload["source"]["lifecycle_status"], "approved")
        self.assertEqual(payload["source"]["created_at"], NOW.isoformat())


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class SourceCatalogPostgresIntegrationTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=2, max_overflow=0)
        self.repository = SourceRepository()
        self.service = SourceCatalogService(self.database, self.repository)

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_all_seed_entries_are_candidate_and_manual_approval_survives_rerun(self):
        first = await self.service.import_seed(DEFAULT_SOURCES_PATH)

        async with self.database.connect() as connection:
            statuses = (
                await connection.execute(sa.select(sources.c.lifecycle_status))
            ).scalars().all()
        self.assertEqual(first.created, 15)
        self.assertEqual(set(statuses), {SourceStatus.CANDIDATE.value})

        listing = await self.service.list_sources(status=SourceStatus.CANDIDATE)
        approved = await self.service.transition_source(
            listing.sources[0].id,
            SourceStatus.APPROVED,
            actor="integration-test",
            reason="manual test approval",
        )
        second = await self.service.import_seed(DEFAULT_SOURCES_PATH)

        self.assertTrue(approved.changed)
        self.assertEqual((second.created, second.updated, second.unchanged), (0, 0, 15))
        details = await self.service.show_source(approved.source.id)
        self.assertEqual(details.source.lifecycle_status, SourceStatus.APPROVED)
        self.assertEqual(details.lifecycle_events[-1].actor_id, "integration-test")


if __name__ == "__main__":
    unittest.main()

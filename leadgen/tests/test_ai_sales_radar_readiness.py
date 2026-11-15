from __future__ import annotations

from contextlib import redirect_stderr
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import io
import json
import unittest
from uuid import UUID, uuid4

import sqlalchemy as sa

from ai_sales_radar.config import DatabaseSettings, OpportunityAISettings
from ai_sales_radar.profiles import LocalProfilesService
from ai_sales_radar.readiness import (
    AISpendCounts,
    ConfigurationReadiness,
    DatabaseReadiness,
    JobStateCounts,
    PipelineJobCounts,
    ProfileCounts,
    RecordCounts,
    assess_readiness,
    build_parser,
    inspect_configuration,
    inspect_readiness_database,
)
from freelancer_bot.matching_jobs import MATCHING_INBOX_JOB_TYPE
from freelancer_bot.message_prefilter import OPPORTUNITY_ANALYSIS_JOB_TYPE
from freelancer_bot.persistence.collector_accounts import CollectorAccountRepository
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.raw_messages import (
    RAW_MESSAGE_JOB_TYPE,
    RawMessageIngestor,
    RawMessageInput,
    RawMessageOrigin,
)
from freelancer_bot.persistence.schema import (
    ai_call_telemetry,
    durable_jobs,
    search_profiles,
)
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


def _database_snapshot(
    *,
    backlog: int = 0,
    primary_profiles: int = 1,
) -> DatabaseReadiness:
    return DatabaseReadiness(
        reachable=True,
        migration_current=True,
        aggregates_readable=True,
        current_heads=("20260826_0037",),
        expected_heads=("20260826_0037",),
        jobs=PipelineJobCounts(
            active_backlog_count=backlog,
            by_type={
                RAW_MESSAGE_JOB_TYPE: JobStateCounts(queued=backlog),
                OPPORTUNITY_ANALYSIS_JOB_TYPE: JobStateCounts(),
                MATCHING_INBOX_JOB_TYPE: JobStateCounts(),
            },
        ),
        records=RecordCounts(raw_messages=0, opportunities=0, inbox_items=0),
        ai_spend=AISpendCounts(
            window_timezone="UTC",
            today_call_count=0,
            today_estimated_usd="0.000000000",
            month_call_count=0,
            month_estimated_usd="0.000000000",
        ),
        profiles=ProfileCounts(
            active_confirmed=primary_profiles,
            active_primary_confirmed=primary_profiles,
        ),
    )


class ReadinessPolicyTest(unittest.TestCase):
    def test_backlog_equal_to_explicit_limit_is_ready(self):
        report = assess_readiness(
            ConfigurationReadiness(
                database_configured=True,
                database_driver_valid=True,
                provider_configured=True,
                external_ai_data_processing_approved=True,
            ),
            _database_snapshot(backlog=5),
            max_active_backlog=5,
        )

        self.assertTrue(report.ready)
        self.assertEqual(report.blockers, ())

    def test_missing_key_profile_and_excess_backlog_fail_closed(self):
        report = assess_readiness(
            ConfigurationReadiness(
                database_configured=True,
                database_driver_valid=True,
                provider_configured=False,
                external_ai_data_processing_approved=True,
                provider="groq",
            ),
            _database_snapshot(backlog=2, primary_profiles=0),
            max_active_backlog=1,
        )

        self.assertFalse(report.ready)
        self.assertEqual(
            report.blockers,
            (
                "ai_provider_key_missing",
                "active_primary_profile_count_invalid",
                "active_backlog_exceeds_canary_limit",
            ),
        )

    def test_external_ai_consent_is_an_independent_readiness_blocker(self):
        report = assess_readiness(
            ConfigurationReadiness(
                database_configured=True,
                database_driver_valid=True,
                provider_configured=True,
                external_ai_data_processing_approved=False,
                provider="groq",
            ),
            _database_snapshot(),
            max_active_backlog=0,
        )

        self.assertFalse(report.ready)
        self.assertEqual(
            report.blockers,
            ("external_ai_data_processing_not_approved",),
        )

    def test_invalid_database_and_migration_fail_closed(self):
        configuration = ConfigurationReadiness(
            database_configured=True,
            database_driver_valid=True,
            provider_configured=True,
            external_ai_data_processing_approved=True,
        )
        unreachable = DatabaseReadiness(
            reachable=False,
            migration_current=False,
            aggregates_readable=False,
            current_heads=(),
            expected_heads=("20260826_0037",),
        )
        outdated = DatabaseReadiness(
            reachable=True,
            migration_current=False,
            aggregates_readable=False,
            current_heads=("old",),
            expected_heads=("20260826_0037",),
        )

        self.assertEqual(
            assess_readiness(
                configuration,
                unreachable,
                max_active_backlog=0,
            ).blockers,
            ("database_unreachable",),
        )
        self.assertEqual(
            assess_readiness(
                configuration,
                outdated,
                max_active_backlog=0,
            ).blockers,
            ("migration_not_current",),
        )

    def test_configuration_and_parser_do_not_require_telegram(self):
        configuration = inspect_configuration(
            DatabaseSettings(
                database_url="postgresql+psycopg://local/readiness",
                _env_file=None,
            ),
            OpportunityAISettings(
                openai_api_key="local-test-key",
                external_ai_data_processing_approved=True,
                _env_file=None,
            ),
        )
        args = build_parser().parse_args(["--max-active-backlog", "5"])

        self.assertTrue(configuration.database_driver_valid)
        self.assertTrue(configuration.provider_configured)
        self.assertTrue(configuration.external_ai_data_processing_approved)
        self.assertEqual(configuration.provider, "openai")
        self.assertEqual(args.max_active_backlog, 5)
        with redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                build_parser().parse_args([])
            with self.assertRaises(SystemExit):
                build_parser().parse_args(["--max-active-backlog", "1001"])


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class ReadinessPostgresTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=2, max_overflow=0)

    async def asyncTearDown(self) -> None:
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_read_only_aggregates_and_canary_threshold(self):
        now = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)
        profiles = LocalProfilesService(self.database)
        created = await profiles.create_profile(
            semantic_text="AI automation and Telegram bot projects",
            roles=("AI automation developer",),
            skills=("Python",),
            categories=("business_process_automation",),
        )
        confirmed = await profiles.confirm_profile(
            created.profile.id,
            expected_revision=created.profile.revision,
        )
        activated = await profiles.activate_profile(
            confirmed.id,
            expected_revision=confirmed.revision,
        )

        sources = SourceRepository()
        async with self.database.transaction() as connection:
            candidate = await sources.create_candidate(
                connection,
                platform="telegram",
                external_id="username:readiness_fixture",
                access_type="public",
                display_name="Readiness fixture",
                handle="@readiness_fixture",
                canonical_url="https://t.me/example_channel_014",
                provider="readiness_fixture",
                lineage_key="readiness:source",
            )
            approved = await sources.override(
                connection,
                candidate.id,
                SourceStatus.APPROVED,
                operator_id="readiness-test",
                reason="read-only readiness fixture",
            )
            account = await CollectorAccountRepository().ensure(
                connection,
                platform="telegram",
                external_account_id="readiness-test-account",
                display_name="Readiness test account",
            )

        secret_content = "SECRET_MESSAGE_CONTENT_MUST_NOT_APPEAR"
        raw = await RawMessageIngestor(self.database).ingest(
            RawMessageInput(
                source_id=approved.id,
                collector_account_id=account.id,
                external_message_id=1,
                message_date=now,
                observed_at=now,
                message_url="https://t.me/example_channel_014/1",
                content=secret_content,
                transport_metadata={"post": True},
                ingestion_origin=RawMessageOrigin.CATCH_UP,
                correlation_id=UUID("a9000000-0000-4000-8000-000000000001"),
            )
        )
        await self._insert_ai_call(
            raw.message.id,
            started_at=now - timedelta(hours=1),
            estimated_cost="0.125",
        )
        await self._insert_ai_call(
            raw.message.id,
            started_at=now.replace(day=2),
            estimated_cost="0.050",
        )
        await self._insert_ai_call(
            raw.message.id,
            started_at=now.replace(month=7, day=31),
            estimated_cost="0.900",
        )

        before = await self._mutation_sentinel(activated.profile.id)
        snapshot = await inspect_readiness_database(self.database, now=now)
        after = await self._mutation_sentinel(activated.profile.id)

        self.assertEqual(before, after)
        self.assertTrue(snapshot.reachable)
        self.assertTrue(snapshot.migration_current)
        self.assertTrue(snapshot.aggregates_readable)
        self.assertEqual(snapshot.jobs.active_backlog_count, 1)
        self.assertEqual(
            snapshot.jobs.by_type[RAW_MESSAGE_JOB_TYPE],
            JobStateCounts(queued=1),
        )
        self.assertEqual(snapshot.records, RecordCounts(1, 0, 0))
        self.assertEqual(snapshot.profiles, ProfileCounts(1, 1))
        self.assertEqual(snapshot.ai_spend.today_call_count, 1)
        self.assertEqual(snapshot.ai_spend.today_estimated_usd, "0.125000000")
        self.assertEqual(snapshot.ai_spend.month_call_count, 2)
        self.assertEqual(snapshot.ai_spend.month_estimated_usd, "0.175000000")

        configuration = ConfigurationReadiness(True, True, True, True)
        ready = assess_readiness(
            configuration,
            snapshot,
            max_active_backlog=1,
        )
        blocked = assess_readiness(
            configuration,
            snapshot,
            max_active_backlog=0,
        )
        payload = json.dumps(asdict(ready), ensure_ascii=False, sort_keys=True)
        self.assertTrue(ready.ready)
        self.assertEqual(
            blocked.blockers,
            ("active_backlog_exceeds_canary_limit",),
        )
        self.assertNotIn(secret_content, payload)
        self.assertNotIn(self.database_url, payload)

    async def _insert_ai_call(
        self,
        raw_message_id,
        *,
        started_at: datetime,
        estimated_cost: str,
    ) -> None:
        async with self.database.transaction() as connection:
            await connection.execute(
                ai_call_telemetry.insert().values(
                    id=uuid4(),
                    raw_message_id=raw_message_id,
                    stage="opportunity.analysis",
                    provider="openai",
                    requested_model="fixture-model",
                    response_model="fixture-model",
                    analyzer_version="fixture.v1",
                    prompt_version="fixture-prompt.v1",
                    schema_version="fixture.v1",
                    routing_version="fixture-route.v1",
                    route_reason="fixture",
                    provider_attempt=1,
                    status="succeeded",
                    pricing_version="fixture-price.v1",
                    input_usd_per_million="1",
                    output_usd_per_million="1",
                    input_tokens=1,
                    output_tokens=1,
                    total_tokens=2,
                    latency_ms=1,
                    estimated_cost_usd=estimated_cost,
                    started_at=started_at,
                    finished_at=started_at + timedelta(seconds=1),
                )
            )

    async def _mutation_sentinel(self, profile_id):
        async with self.database.connect() as connection:
            job_rows = tuple(
                (
                    str(row.id),
                    str(row.job_type),
                    str(row.state),
                    int(row.attempt_count),
                )
                for row in (
                    await connection.execute(
                        sa.select(
                            durable_jobs.c.id,
                            durable_jobs.c.job_type,
                            durable_jobs.c.state,
                            durable_jobs.c.attempt_count,
                        ).order_by(durable_jobs.c.id)
                    )
                )
            )
            profile = (
                await connection.execute(
                    sa.select(
                        search_profiles.c.revision,
                        search_profiles.c.updated_at,
                    ).where(search_profiles.c.id == profile_id)
                )
            ).one()
            telemetry_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(ai_call_telemetry)
            )
        return job_rows, tuple(profile), int(telemetry_count)


if __name__ == "__main__":
    unittest.main()

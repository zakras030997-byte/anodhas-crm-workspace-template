from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import unittest
from uuid import UUID

import sqlalchemy as sa

from ai_sales_radar.config import ReplyDraftSettings
from ai_sales_radar.inbox import LeadInboxService
from ai_sales_radar.matching_worker import MatchingInboxJobProcessor
from ai_sales_radar.persistence.inbox import InboxAvailability
from ai_sales_radar.profiles import LocalProfilesService
from ai_sales_radar.reply_drafts import (
    ReplyDraftOutput,
    ReplyDraftService,
)
from ai_sales_radar.source_catalog import SourceCatalogService
from freelancer_bot.matching_jobs import (
    MATCHING_DELIVERY_JOB_TYPE,
    MATCHING_INBOX_JOB_TYPE,
    enqueue_matching_inbox,
)
from freelancer_bot.message_prefilter import (
    OPPORTUNITY_ANALYSIS_JOB_TYPE,
    RawMessagePrefilterProcessor,
)
from freelancer_bot.opportunity_analysis import (
    OPPORTUNITY_ANALYSIS_PROMPT_VERSION,
    OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
    OPPORTUNITY_ANALYZER_VERSION,
    OpportunityAnalysis,
    OpportunityAnalysisCall,
    OpportunityAnalysisUsage,
    opportunity_analysis_cache_version,
)
from freelancer_bot.opportunity_classifier import OpportunityAnalysisJobProcessor
from freelancer_bot.persistence.collector_accounts import CollectorAccountRepository
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.jobs import DurableJobRepository, JobClaim
from freelancer_bot.persistence.raw_messages import (
    RAW_MESSAGE_JOB_TYPE,
    RawMessageIngestor,
    RawMessageInput,
    RawMessageOrigin,
)
from freelancer_bot.persistence.schema import (
    durable_jobs,
    payment_provider_events,
    personalized_deliveries,
    subscription_periods,
)
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


CORRELATION_ID = UUID("a1000000-0000-4000-8000-000000000001")


class DeterministicOpportunityAnalyzer:
    """Network-free analyzer fixture with the production output contract."""

    provider = "deterministic_fixture"
    model = "deterministic-opportunity-v1"
    analyzer_version = OPPORTUNITY_ANALYZER_VERSION
    prompt_version = OPPORTUNITY_ANALYSIS_PROMPT_VERSION
    schema_version = OPPORTUNITY_ANALYSIS_SCHEMA_VERSION

    def __init__(self) -> None:
        self.calls = 0
        self.analysis = OpportunityAnalysis.model_validate_json(
            json.dumps(
                {
                    "schema_version": OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
                    "is_opportunity": True,
                    "confidence": 0.96,
                    "market_direction": "buyer_to_specialist",
                    "intent_stage": "active",
                    "opportunity_type": "project",
                    "category": "business_process_automation",
                    "role_title": "n8n automation architect",
                    "skills": ["n8n", "Python", "OpenAI API"],
                    "task_summary": (
                        "Build an n8n, Python and OpenAI API automation"
                    ),
                    "budget": {
                        "known": True,
                        "min": 2000,
                        "max": 3000,
                        "currency": "EUR",
                        "period": "project",
                        "explicit": True,
                    },
                    "work": {
                        "remote": True,
                        "location": None,
                        "full_time": False,
                        "part_time": False,
                    },
                    "language": "ru",
                    "contact": {
                        "telegram": "@ai_studio_client",
                        "email": None,
                        "url": None,
                    },
                    "quality": {
                        "actionability": 0.95,
                        "commercial_plausibility": 0.95,
                        "specificity": 0.95,
                        "credibility": 0.9,
                    },
                    "red_flags": [],
                }
            ),
            strict=True,
        )

    async def analyze(self, candidate) -> OpportunityAnalysisCall:
        self.calls += 1
        return OpportunityAnalysisCall(
            analysis=self.analysis,
            provider=self.provider,
            requested_model=self.model,
            response_model=self.model,
            analyzer_version=self.analyzer_version,
            prompt_version=self.prompt_version,
            schema_version=self.schema_version,
            attempt_count=1,
            usage=OpportunityAnalysisUsage(
                input_tokens=50,
                output_tokens=30,
                total_tokens=80,
            ),
        )


class DeterministicReplyGenerator:
    provider = "deterministic_fixture"
    model = "deterministic-reply-v1"
    prompt_version = "reply-draft-prompt.v2"
    schema_version = "reply-draft.v2"

    def __init__(self) -> None:
        self.calls = 0

    @property
    def cache_identity(self):
        return {"fixture": "ai-sales-radar-scripts-e2e-v1"}

    async def generate(self, context, persona) -> ReplyDraftOutput:
        self.calls += 1
        return ReplyDraftOutput(
            proposal_draft=(
                "Могу спроектировать автоматизацию на n8n и Python, подключить "
                "OpenAI API и сначала проверить интеграционный сценарий на тестовых "
                "данных."
            ),
            short_reply=(
                "Могу собрать автоматизацию на n8n, Python и OpenAI API."
            ),
            questions_to_client=(
                "Какие системы должны обмениваться данными?",
            ),
            risks=(),
        )


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class AISalesRadarScriptsPostgresE2ETest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=0)
        self.jobs = DurableJobRepository()

    async def asyncTearDown(self) -> None:
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_scripts_only_pipeline_materializes_inbox_and_one_manual_draft(self):
        now = datetime.now(timezone.utc)

        profiles = LocalProfilesService(self.database)
        created_profile = await profiles.create_profile(
            semantic_text=(
                "n8n automation architect, Python and OpenAI API projects"
            ),
            roles=("n8n automation architect",),
            skills=("n8n", "Python", "OpenAI API"),
            categories=("business_process_automation",),
        )
        confirmed_profile = await profiles.confirm_profile(
            created_profile.profile.id,
            expected_revision=created_profile.profile.revision,
        )
        activated_profile = await profiles.activate_profile(
            confirmed_profile.id,
            expected_revision=confirmed_profile.revision,
        )
        self.assertTrue(activated_profile.profile.is_active)
        self.assertTrue(activated_profile.profile.is_primary)
        self.assertFalse(activated_profile.trial_started)

        sources = SourceRepository()
        async with self.database.transaction() as connection:
            candidate_source = await sources.create_candidate(
                connection,
                platform="telegram",
                external_id="username:ai_sales_radar_e2e",
                access_type="public",
                display_name="AI Sales Radar E2E",
                handle="@ai_sales_radar_e2e",
                canonical_url="https://t.me/example_channel_015",
                provider="scripts_e2e_fixture",
                lineage_key="scripts-e2e:source",
            )
        self.assertIs(candidate_source.lifecycle_status, SourceStatus.CANDIDATE)

        approved_source = await SourceCatalogService(
            self.database,
            sources,
        ).transition_source(
            candidate_source.id,
            SourceStatus.APPROVED,
            actor="operator:scripts-e2e",
            reason="explicit integration-test approval",
        )
        self.assertTrue(approved_source.changed)
        self.assertIs(
            approved_source.source.lifecycle_status,
            SourceStatus.APPROVED,
        )

        async with self.database.transaction() as connection:
            collector_account = await CollectorAccountRepository().ensure(
                connection,
                platform="telegram",
                external_account_id="ai-sales-radar-e2e-account",
                display_name="AI Sales Radar local collector",
            )

        raw = await RawMessageIngestor(self.database).ingest(
            RawMessageInput(
                source_id=approved_source.source.id,
                collector_account_id=collector_account.id,
                external_message_id=101,
                message_date=now,
                observed_at=now,
                message_url="https://t.me/example_channel_015/101",
                content=(
                    "Ищем n8n automation architect на проект: нужна автоматизация "
                    "на n8n, Python и OpenAI API. Бюджет 2000-3000 EUR, удаленно. "
                    "Пишите @ai_studio_client."
                ),
                transport_metadata={"post": True, "media_type": None},
                ingestion_origin=RawMessageOrigin.LIVE,
                correlation_id=CORRELATION_ID,
            )
        )
        self.assertTrue(raw.created)

        analyzer = DeterministicOpportunityAnalyzer()
        raw_claim = await self._claim(RAW_MESSAGE_JOB_TYPE, "prefilter-e2e")
        prefilter_result = await RawMessagePrefilterProcessor(
            self.database,
            analyzer_version=opportunity_analysis_cache_version(analyzer),
        ).process(raw_claim)
        self.assertIsNotNone(prefilter_result.analysis_job_id)
        await self._complete(raw_claim)

        analysis_claim = await self._claim(
            OPPORTUNITY_ANALYSIS_JOB_TYPE,
            "opportunity-e2e",
        )
        classification = await OpportunityAnalysisJobProcessor(
            self.database,
            analyzer,
            matching_enqueuer=enqueue_matching_inbox,
        ).process(analysis_claim)
        self.assertTrue(classification.analyzed)
        self.assertIsNotNone(classification.opportunity)
        self.assertEqual(analyzer.calls, 1)
        await self._complete(analysis_claim)

        matching_claim = await self._claim(
            MATCHING_INBOX_JOB_TYPE,
            "matching-inbox-e2e",
        )
        matching_result = await MatchingInboxJobProcessor(
            self.database,
            jobs=self.jobs,
        ).process(matching_claim)
        self.assertEqual(matching_result.matching.report.eligible_match_count, 1)
        self.assertEqual(matching_result.inbox.created_count, 1)
        self.assertEqual(matching_result.inbox.available_count, 1)
        await self._complete(matching_claim)

        inbox = LeadInboxService(self.database)
        lead_items = await inbox.list_leads(
            profile_id=activated_profile.profile.id,
            availability=InboxAvailability.AVAILABLE,
        )
        self.assertEqual(len(lead_items), 1)
        lead = await inbox.show_lead(lead_items[0].id)
        self.assertEqual(
            lead.opportunity.id,
            classification.opportunity.id,
        )
        self.assertTrue(lead.match.trace.eligible)

        reply_generator = DeterministicReplyGenerator()
        reply_service = ReplyDraftService(
            self.database,
            ReplyDraftSettings(
                openai_api_key=None,
                external_ai_data_processing_approved=True,
                persona_path="missing-scripts-e2e-persona.json",
                _env_file=None,
            ),
            generator=reply_generator,
        )
        first_draft = await reply_service.create(lead.item.id)
        repeated_draft = await reply_service.create(lead.item.id)
        persisted_drafts = await reply_service.list_for_lead(lead.item.id)

        self.assertEqual(first_draft.id, repeated_draft.id)
        self.assertEqual(reply_generator.calls, 1)
        self.assertEqual(len(persisted_drafts), 1)
        self.assertEqual(first_draft.provider, "deterministic_fixture")

        async with self.database.connect() as connection:
            job_rows = (
                await connection.execute(
                    sa.select(durable_jobs.c.job_type, durable_jobs.c.state)
                )
            ).all()
            delivery_job_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(durable_jobs.c.job_type == MATCHING_DELIVERY_JOB_TYPE)
            )
            delivery_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(personalized_deliveries)
            )
            payment_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(payment_provider_events)
            )
            subscription_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(subscription_periods)
            )

        self.assertEqual(len(job_rows), 3)
        self.assertEqual(
            dict(job_rows),
            {
                RAW_MESSAGE_JOB_TYPE: "completed",
                OPPORTUNITY_ANALYSIS_JOB_TYPE: "completed",
                MATCHING_INBOX_JOB_TYPE: "completed",
            },
        )
        self.assertEqual(
            (
                delivery_job_count,
                delivery_count,
                payment_count,
                subscription_count,
            ),
            (0, 0, 0, 0),
        )

    async def _claim(self, job_type: str, worker_id: str) -> JobClaim:
        async with self.database.transaction() as connection:
            claim = await self.jobs.claim_next(
                connection,
                worker_id=worker_id,
                lease_duration=timedelta(minutes=5),
                job_types=(job_type,),
            )
        self.assertIsNotNone(claim)
        return claim

    async def _complete(self, claim: JobClaim) -> None:
        async with self.database.transaction() as connection:
            completed = await self.jobs.complete(connection, claim)
        self.assertTrue(completed)


if __name__ == "__main__":
    unittest.main()

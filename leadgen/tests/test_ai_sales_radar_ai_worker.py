from __future__ import annotations

import asyncio
from decimal import Decimal
import io
import json
import unittest
import urllib.error
from unittest.mock import Mock, patch

from ai_sales_radar.ai_worker import (
    AIWorkerService,
    build_ai_worker,
    build_opportunity_analyzer,
    inspect_ai_configuration,
    main,
    opportunity_analyzer_compatibility,
    run_ai_worker,
)
from ai_sales_radar.config import (
    ConfigurationError,
    DatabaseSettings,
    OpportunityAISettings,
)
from freelancer_bot.ai_telemetry import AIBudgetExceeded
from freelancer_bot.matching_jobs import enqueue_matching_inbox
from freelancer_bot.message_prefilter import OPPORTUNITY_ANALYSIS_JOB_TYPE
from freelancer_bot.opportunity_analysis import (
    OPPORTUNITY_ANALYZER_VERSION,
    OpenAICompatibleOpportunityAnalyzer,
    OpportunityAnalysisError,
    opportunity_analysis_cache_version,
)
from freelancer_bot.opportunity_classifier import IncompatibleOpportunityAnalysisJob


class FakeWorker:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.stop_requested = asyncio.Event()
        self.install_signal_handlers = None

    async def run(self, *, install_signal_handlers=True):
        self.install_signal_handlers = install_signal_handlers
        self.started.set()
        await self.stop_requested.wait()

    def request_stop(self):
        self.stop_requested.set()


def configured_ai(**overrides):
    values = {
        "openai_api_key": "sk-" + "project-test-secret-value-123456789",
        "external_ai_data_processing_approved": True,
        "_env_file": None,
    }
    values.update(overrides)
    return OpportunityAISettings(**values)


class AIWorkerCompositionTest(unittest.TestCase):
    def test_worker_build_fails_before_factory_without_external_ai_consent(self):
        worker_factory = Mock()

        with self.assertRaises(ConfigurationError):
            build_ai_worker(
                object(),
                configured_ai(external_ai_data_processing_approved=False),
                logger=object(),
                repository=object(),
                processor=object(),
                worker_factory=worker_factory,
            )

        worker_factory.assert_not_called()

    def test_factory_registers_only_analysis_handler(self):
        captured = {}
        sentinel = object()

        def worker_factory(database, **kwargs):
            captured["database"] = database
            captured.update(kwargs)
            return sentinel

        result = build_ai_worker(
            object(),
            configured_ai(),
            logger=object(),
            repository="jobs",
            processor="analysis-processor",
            worker_id="ai-test",
            worker_factory=worker_factory,
        )

        self.assertIs(result, sentinel)
        self.assertEqual(
            captured["handlers"],
            {OPPORTUNITY_ANALYSIS_JOB_TYPE: "analysis-processor"},
        )
        self.assertFalse(captured["close_database_on_exit"])

    def test_factory_routes_new_opportunities_to_inbox_matching(self):
        captured = {}
        processor = object()

        def processor_factory(*args, **kwargs):
            captured["processor_args"] = args
            captured["processor_kwargs"] = kwargs
            return processor

        def worker_factory(database, **kwargs):
            captured["worker_kwargs"] = kwargs
            return object()

        with patch(
            "ai_sales_radar.ai_worker.OpportunityAnalysisJobProcessor",
            side_effect=processor_factory,
        ):
            build_ai_worker(
                object(),
                configured_ai(),
                logger="logger",
                repository="jobs",
                analyzer="analyzer",
                worker_id="ai-test",
                worker_factory=worker_factory,
            )

        self.assertIs(
            captured["processor_kwargs"]["matching_enqueuer"],
            enqueue_matching_inbox,
        )
        self.assertEqual(
            captured["worker_kwargs"]["handlers"],
            {OPPORTUNITY_ANALYSIS_JOB_TYPE: processor},
        )

    def test_analyzer_uses_spend_guard_price_and_completion_cap(self):
        captured = {}

        def recorder_factory(database, **kwargs):
            captured["database"] = database
            captured["spend_guard"] = kwargs["spend_guard"]
            return "recorder"

        def analyzer_factory(**kwargs):
            captured["analyzer"] = kwargs
            return "analyzer"

        result = build_opportunity_analyzer(
            "database",
            configured_ai(
                reserve_input_tokens=123,
                reserve_output_tokens=45,
                daily_spend_limit_usd=Decimal("0.50"),
                monthly_spend_limit_usd=Decimal("2.00"),
            ),
            recorder_factory=recorder_factory,
            analyzer_factory=analyzer_factory,
        )

        self.assertEqual(result, "analyzer")
        self.assertEqual(captured["spend_guard"].reserve_input_tokens, 123)
        self.assertEqual(captured["spend_guard"].reserve_output_tokens, 45)
        self.assertEqual(captured["analyzer"]["max_completion_tokens"], 45)
        self.assertEqual(captured["analyzer"]["provider"], "openai")
        self.assertEqual(
            captured["analyzer"]["price"].input_usd_per_million,
            Decimal("0.05"),
        )

    def test_analyzer_build_fails_before_factories_without_external_ai_consent(self):
        recorder_factory = Mock()
        analyzer_factory = Mock()

        with self.assertRaises(ConfigurationError):
            build_opportunity_analyzer(
                "database",
                configured_ai(external_ai_data_processing_approved=False),
                recorder_factory=recorder_factory,
                analyzer_factory=analyzer_factory,
            )

        recorder_factory.assert_not_called()
        analyzer_factory.assert_not_called()

    def test_pure_route_compatibility_matches_groq_analyzer_without_network(self):
        settings = configured_ai(
            provider="groq",
            openai_api_key=None,
            groq_api_key="gsk_fixture_safe_value",
        )
        route_only_settings = OpportunityAISettings(
            provider="groq",
            openai_api_key=None,
            groq_api_key=None,
            external_ai_data_processing_approved=False,
            _env_file=None,
        )
        compatibility = opportunity_analyzer_compatibility(route_only_settings)
        analyzer = build_opportunity_analyzer(
            "database",
            settings,
            recorder_factory=lambda *args, **kwargs: None,
        )

        self.assertEqual(
            compatibility.analyzer_version,
            opportunity_analysis_cache_version(analyzer),
        )
        self.assertNotEqual(
            compatibility.analyzer_version,
            OPPORTUNITY_ANALYZER_VERSION,
        )
        self.assertEqual(
            compatibility.analysis_schema_version,
            analyzer.schema_version,
        )

    def test_incompatible_analysis_job_is_not_retryable(self):
        error = IncompatibleOpportunityAnalysisJob("fixture mismatch")

        self.assertFalse(error.retryable)

    def test_check_is_read_only_and_does_not_construct_runtime(self):
        database = DatabaseSettings(
            database_url="postgresql+psycopg://local/radar",
            _env_file=None,
        )
        ai = configured_ai()
        status = inspect_ai_configuration(database, ai)
        self.assertTrue(status.database_configured)
        self.assertTrue(status.provider_configured)
        self.assertTrue(status.external_ai_data_processing_approved)
        self.assertEqual(status.handled_job_types, (OPPORTUNITY_ANALYSIS_JOB_TYPE,))
        self.assertFalse(status.fallback_enabled)
        self.assertFalse(status.automatic_sending_enabled)

        output = io.StringIO()
        with (
            patch(
                "ai_sales_radar.ai_worker.DatabaseSettings.from_env",
                return_value=database,
            ),
            patch(
                "ai_sales_radar.ai_worker.OpportunityAISettings.from_env",
                return_value=ai,
            ),
            patch("ai_sales_radar.ai_worker.Database") as database_adapter,
            patch("ai_sales_radar.ai_worker.build_opportunity_analyzer") as analyzer,
            patch("sys.stdout", output),
        ):
            self.assertEqual(main(["check"]), 0)
        database_adapter.assert_not_called()
        analyzer.assert_not_called()
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["provider"], "openai")
        self.assertTrue(payload["provider_configured"])
        self.assertTrue(payload["external_ai_data_processing_approved"])
        self.assertNotIn("openai_configured", payload)

    def test_budget_exhaustion_is_terminal_for_durable_worker(self):
        error = AIBudgetExceeded(
            window="daily",
            limit_usd=Decimal("1"),
            used_usd=Decimal("1"),
            requested_usd=Decimal("0.01"),
        )
        self.assertFalse(error.retryable)

    def test_http_408_and_409_are_retryable(self):
        analyzer = OpenAICompatibleOpportunityAnalyzer(
            api_key="sk-" + "project-test-secret-value-123456789",
            model="gpt-5-nano",
        )
        for status in (408, 409):
            with self.subTest(status=status):
                error = urllib.error.HTTPError(
                    "https://api.openai.com/v1/chat/completions",
                    status,
                    "fixture",
                    {},
                    None,
                )
                with (
                    patch("urllib.request.urlopen", side_effect=error),
                    self.assertRaises(OpportunityAnalysisError) as raised,
                ):
                    analyzer._request({"model": "gpt-5-nano"})
                self.assertTrue(raised.exception.retryable)

    def test_completion_cap_is_sent_to_openai(self):
        analyzer = OpenAICompatibleOpportunityAnalyzer(
            api_key="sk-" + "project-test-secret-value-123456789",
            model="gpt-5-nano",
            max_completion_tokens=77,
        )
        from tests.test_opportunity_analysis import _candidate

        self.assertEqual(
            analyzer._payload(_candidate())["max_completion_tokens"],
            77,
        )


class AIWorkerServiceTest(unittest.IsolatedAsyncioTestCase):
    async def test_run_fails_before_database_without_external_ai_consent(self):
        database_factory = Mock()
        with self.assertRaises(ConfigurationError):
            await run_ai_worker(
                DatabaseSettings(
                    database_url="postgresql+psycopg://local/radar",
                    _env_file=None,
                ),
                configured_ai(external_ai_data_processing_approved=False),
                database_factory=database_factory,
            )
        database_factory.assert_not_called()

    async def test_service_lifecycle(self):
        worker = FakeWorker()
        service = AIWorkerService(
            object(),
            configured_ai(),
            logger=object(),
            worker=worker,
        )
        await service.start()
        await asyncio.wait_for(worker.started.wait(), timeout=0.2)
        self.assertTrue(service.running)
        self.assertFalse(worker.install_signal_handlers)
        await asyncio.wait_for(service.stop(), timeout=0.2)
        self.assertFalse(service.running)

    async def test_service_forwards_injected_analyzer_to_worker(self):
        worker = FakeWorker()
        analyzer = Mock()
        with patch(
            "ai_sales_radar.ai_worker.build_ai_worker",
            return_value=worker,
        ) as build_worker:
            AIWorkerService(
                "database",
                configured_ai(),
                logger="logger",
                analyzer=analyzer,
            )

        self.assertIs(build_worker.call_args.kwargs["analyzer"], analyzer)


if __name__ == "__main__":
    unittest.main()

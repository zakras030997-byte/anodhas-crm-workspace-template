from __future__ import annotations

import asyncio
import io
import json
import unittest
from unittest.mock import AsyncMock, patch

from ai_sales_radar.config import DatabaseSettings, OpportunityAISettings
from ai_sales_radar.pipeline import (
    RawPrefilterWorkerService,
    build_prefilter_worker,
    inspect_prefilter_configuration,
    main,
)
from freelancer_bot.persistence.raw_messages import RAW_MESSAGE_JOB_TYPE


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


class PrefilterCompositionTest(unittest.TestCase):
    def test_factory_registers_only_raw_message_handler(self):
        captured = {}
        sentinel_worker = object()

        def worker_factory(database, **kwargs):
            captured["database"] = database
            captured.update(kwargs)
            return sentinel_worker

        database = object()
        repository = object()
        processor = object()
        logger = object()

        result = build_prefilter_worker(
            database,
            logger=logger,
            repository=repository,
            processor=processor,
            worker_id="test-prefilter",
            worker_factory=worker_factory,
        )

        self.assertIs(result, sentinel_worker)
        self.assertEqual(captured["database"], database)
        self.assertEqual(captured["repository"], repository)
        self.assertEqual(captured["handlers"], {RAW_MESSAGE_JOB_TYPE: processor})
        self.assertFalse(captured["close_database_on_exit"])

    def test_factory_forwards_explicit_analyzer_compatibility_route(self):
        captured = {}

        def worker_factory(database, **kwargs):
            captured.update(kwargs)
            return object()

        with patch(
            "ai_sales_radar.pipeline.RawMessagePrefilterProcessor",
            return_value="prefilter-processor",
        ) as processor_factory:
            build_prefilter_worker(
                "database",
                logger="logger",
                repository="jobs",
                analyzer_version="opportunity-analyzer.v1.fixture123",
                analysis_schema_version="opportunity_analysis.v1",
                worker_factory=worker_factory,
            )

        processor_factory.assert_called_once_with(
            "database",
            jobs="jobs",
            analyzer_version="opportunity-analyzer.v1.fixture123",
            analysis_schema_version="opportunity_analysis.v1",
            logger="logger",
        )
        self.assertEqual(
            captured["handlers"],
            {RAW_MESSAGE_JOB_TYPE: "prefilter-processor"},
        )

    def test_factory_fails_closed_without_explicit_route(self):
        with self.assertRaisesRegex(ValueError, "explicit analyzer"):
            build_prefilter_worker(
                object(),
                logger=object(),
                worker_factory=lambda *args, **kwargs: object(),
            )

    def test_cli_run_loads_ai_route_settings_for_standalone_prefilter(self):
        database = DatabaseSettings(
            database_url="postgresql+psycopg://local/radar",
            _env_file=None,
        )
        ai = OpportunityAISettings(
            provider="groq",
            groq_api_key=None,
            _env_file=None,
        )
        run_worker = AsyncMock()
        with (
            patch(
                "ai_sales_radar.pipeline.DatabaseSettings.from_env",
                return_value=database,
            ),
            patch(
                "ai_sales_radar.pipeline.OpportunityAISettings.from_env",
                return_value=ai,
            ),
            patch(
                "ai_sales_radar.pipeline.run_prefilter_worker",
                run_worker,
            ),
        ):
            self.assertEqual(main(["run"]), 0)

        run_worker.assert_awaited_once_with(database, ai)

    def test_check_has_no_ai_or_bot_dependency(self):
        settings = DatabaseSettings(
            database_url="postgresql+psycopg://local/radar",
            _env_file=None,
        )
        check = inspect_prefilter_configuration(settings)
        self.assertTrue(check.database_configured)
        self.assertTrue(check.database_driver_valid)
        self.assertEqual(check.handled_job_types, (RAW_MESSAGE_JOB_TYPE,))
        self.assertFalse(check.openai_required)
        self.assertFalse(check.telegram_bot_required)

    def test_cli_check_does_not_construct_database_or_call_network(self):
        settings = DatabaseSettings(
            database_url="postgresql+psycopg://local/radar",
            _env_file=None,
        )
        output = io.StringIO()
        with (
            patch(
                "ai_sales_radar.pipeline.DatabaseSettings.from_env",
                return_value=settings,
            ),
            patch("ai_sales_radar.pipeline.Database") as database,
            patch("sys.stdout", output),
        ):
            self.assertEqual(main(["check"]), 0)
        database.assert_not_called()
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["handled_job_types"], [RAW_MESSAGE_JOB_TYPE])

    def test_invalid_driver_error_does_not_echo_dsn(self):
        marker = "TOP_SECRET_DSN"
        settings = DatabaseSettings(
            database_url=f"sqlite:///{marker}",
            _env_file=None,
        )
        error = io.StringIO()
        with (
            patch(
                "ai_sales_radar.pipeline.DatabaseSettings.from_env",
                return_value=settings,
            ),
            patch("sys.stderr", error),
        ):
            self.assertEqual(main(["check"]), 2)
        self.assertNotIn(marker, error.getvalue())


class PrefilterServiceTest(unittest.IsolatedAsyncioTestCase):
    async def test_service_starts_without_signal_handlers_and_stops(self):
        worker = FakeWorker()
        service = RawPrefilterWorkerService(
            object(),
            logger=object(),
            worker=worker,
        )

        await service.start()
        await asyncio.wait_for(worker.started.wait(), timeout=0.2)
        self.assertTrue(service.running)
        self.assertFalse(worker.install_signal_handlers)

        await asyncio.wait_for(service.stop(), timeout=0.2)
        self.assertFalse(service.running)

    async def test_service_cannot_be_started_twice(self):
        worker = FakeWorker()
        service = RawPrefilterWorkerService(
            object(),
            logger=object(),
            worker=worker,
        )
        await service.start()
        try:
            with self.assertRaisesRegex(RuntimeError, "started twice"):
                await service.start()
        finally:
            await service.stop()

    async def test_wait_requires_start(self):
        service = RawPrefilterWorkerService(
            object(),
            logger=object(),
            worker=FakeWorker(),
        )
        with self.assertRaisesRegex(RuntimeError, "not started"):
            await service.wait()


if __name__ == "__main__":
    unittest.main()

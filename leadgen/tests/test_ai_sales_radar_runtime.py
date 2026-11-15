from __future__ import annotations

import asyncio
import io
import json
import unittest
from unittest.mock import ANY, AsyncMock, Mock, patch

from ai_sales_radar.config import (
    ConfigurationError,
    DatabaseSettings,
    OpportunityAISettings,
    TelegramAuthSettings,
)
from ai_sales_radar.runtime import AISalesRadarRuntime, main, run_local_runtime


class FakeDatabase:
    def __init__(self, events):
        self.events = events
        self.close_count = 0

    async def close(self):
        self.close_count += 1
        self.events.append("database.stop")


class FakeWorkerService:
    def __init__(self, name, events, *, exit_immediately=False):
        self.name = name
        self.events = events
        self.exit_immediately = exit_immediately
        self.stop_event = asyncio.Event()

    async def start(self):
        self.events.append(f"{self.name}.start")

    async def wait(self):
        if self.exit_immediately:
            return
        await self.stop_event.wait()

    async def stop(self):
        self.events.append(f"{self.name}.stop")
        self.stop_event.set()


class FakeCollector:
    def __init__(self, events):
        self.events = events
        self.started = asyncio.Event()
        self.cancelled = False
        self.stopped = False
        self.stop_requested = asyncio.Event()

    async def run(self, *, options):
        self.events.append("collector.start")
        self.started.set()
        try:
            await self.stop_requested.wait()
        except asyncio.CancelledError:
            self.cancelled = True
            self.events.append("collector.stop")
            raise
        self.stopped = True
        self.events.append("collector.stop")

    async def stop(self):
        self.stop_requested.set()


class PartialStartWorker(FakeWorkerService):
    def __init__(self, name, events):
        super().__init__(name, events)
        self.start_entered = asyncio.Event()
        self.inner_task = None
        self.stop_called = False

    async def start(self):
        self.events.append(f"{self.name}.start")
        self.inner_task = asyncio.create_task(asyncio.Event().wait())
        self.start_entered.set()
        await asyncio.Event().wait()

    async def stop(self):
        self.stop_called = True
        self.events.append(f"{self.name}.stop")
        if self.inner_task is not None:
            self.inner_task.cancel()
            try:
                await self.inner_task
            except asyncio.CancelledError:
                pass


class IgnoringCollector(FakeCollector):
    async def run(self, *, options):
        self.events.append("collector.start")
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            self.events.append("collector.force_cancel")
            raise

    async def stop(self):
        self.events.append("collector.stop_requested")


class FailedCollector(FakeCollector):
    async def run(self, *, options):
        raise RuntimeError("collector failed before stop")


class RuntimeTest(unittest.IsolatedAsyncioTestCase):
    async def test_local_runtime_fails_before_database_without_external_ai_consent(self):
        database_factory = Mock()
        with self.assertRaises(ConfigurationError):
            await run_local_runtime(
                DatabaseSettings(
                    database_url="postgresql+psycopg://local/radar",
                    _env_file=None,
                ),
                TelegramAuthSettings(
                    api_id=123,
                    api_hash="telegram-secret",
                    _env_file=None,
                ),
                OpportunityAISettings(
                    openai_api_key="openai-test-secret",
                    external_ai_data_processing_approved=False,
                    _env_file=None,
                ),
                database_factory=database_factory,
            )
        database_factory.assert_not_called()

    async def test_local_runtime_shares_one_analyzer_and_route_with_prefilter(self):
        database_settings = DatabaseSettings(
            database_url="postgresql+psycopg://local/radar",
            _env_file=None,
        )
        telegram_settings = TelegramAuthSettings(
            api_id=123,
            api_hash="telegram-secret",
            _env_file=None,
        )
        ai_settings = OpportunityAISettings(
            provider="groq",
            groq_api_key="gsk_fixture_safe_value",
            external_ai_data_processing_approved=True,
            _env_file=None,
        )
        database = object()
        analyzer = Mock(schema_version="opportunity_analysis.v1")
        runtime = Mock()
        runtime.run = AsyncMock()

        with (
            patch(
                "ai_sales_radar.runtime.build_opportunity_analyzer",
                return_value=analyzer,
            ) as build_analyzer,
            patch(
                "ai_sales_radar.runtime.opportunity_analysis_cache_version",
                return_value="opportunity-analyzer.v1.fixture123",
            ),
            patch("ai_sales_radar.runtime.TelegramCollectorService"),
            patch("ai_sales_radar.runtime.RawPrefilterWorkerService") as prefilter,
            patch("ai_sales_radar.runtime.AIWorkerService") as ai_worker,
            patch("ai_sales_radar.runtime.MatchingWorkerService"),
            patch(
                "ai_sales_radar.runtime.AISalesRadarRuntime",
                return_value=runtime,
            ),
        ):
            await run_local_runtime(
                database_settings,
                telegram_settings,
                ai_settings,
                database_factory=Mock(return_value=database),
            )

        build_analyzer.assert_called_once_with(database, ai_settings)
        prefilter.assert_called_once_with(
            database,
            logger=ANY,
            analyzer_version="opportunity-analyzer.v1.fixture123",
            analysis_schema_version="opportunity_analysis.v1",
        )
        ai_worker.assert_called_once_with(
            database,
            ai_settings,
            logger=ANY,
            analyzer=analyzer,
        )
        runtime.run.assert_awaited_once_with()

    async def test_runtime_starts_downstream_first_and_closes_database_once(self):
        events = []
        database = FakeDatabase(events)
        collector = FakeCollector(events)
        matching = FakeWorkerService("matching", events)
        ai = FakeWorkerService("ai", events)
        prefilter = FakeWorkerService("prefilter", events)
        runtime = AISalesRadarRuntime(
            database,
            collector=collector,
            matching=matching,
            ai=ai,
            prefilter=prefilter,
        )
        stop = asyncio.Event()
        task = asyncio.create_task(
            runtime.run(stop_event=stop, install_signal_handlers=False)
        )
        await asyncio.wait_for(collector.started.wait(), timeout=0.2)
        stop.set()
        await asyncio.wait_for(task, timeout=0.5)

        self.assertEqual(
            events[:4],
            [
                "matching.start",
                "ai.start",
                "prefilter.start",
                "collector.start",
            ],
        )
        self.assertTrue(collector.stopped)
        self.assertFalse(collector.cancelled)
        self.assertEqual(
            events[-5:-1],
            ["collector.stop", "prefilter.stop", "ai.stop", "matching.stop"],
        )
        self.assertEqual(events[-1], "database.stop")
        self.assertEqual(database.close_count, 1)
        self.assertIn("database.stop", events)

    async def test_unexpected_worker_exit_stops_collector(self):
        events = []
        database = FakeDatabase(events)
        collector = FakeCollector(events)
        runtime = AISalesRadarRuntime(
            database,
            collector=collector,
            matching=FakeWorkerService(
                "matching",
                events,
                exit_immediately=True,
            ),
            ai=FakeWorkerService("ai", events),
            prefilter=FakeWorkerService("prefilter", events),
        )
        with self.assertRaisesRegex(RuntimeError, "worker stopped"):
            await asyncio.wait_for(
                runtime.run(install_signal_handlers=False),
                timeout=0.5,
            )
        self.assertTrue(collector.stopped)
        self.assertFalse(collector.cancelled)
        self.assertEqual(database.close_count, 1)

    async def test_cancel_during_worker_start_stops_partially_owned_service(self):
        events = []
        database = FakeDatabase(events)
        matching = PartialStartWorker("matching", events)
        runtime = AISalesRadarRuntime(
            database,
            collector=FakeCollector(events),
            matching=matching,
            ai=FakeWorkerService("ai", events),
            prefilter=FakeWorkerService("prefilter", events),
        )
        task = asyncio.create_task(
            runtime.run(install_signal_handlers=False)
        )
        await asyncio.wait_for(matching.start_entered.wait(), timeout=0.2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertTrue(matching.stop_called)
        self.assertTrue(matching.inner_task.done())
        self.assertEqual(database.close_count, 1)

    async def test_completed_collector_failure_is_retrieved_during_stop(self):
        events = []
        database = FakeDatabase(events)
        runtime = AISalesRadarRuntime(
            database,
            collector=FailedCollector(events),
            matching=FakeWorkerService("matching", events),
            ai=FakeWorkerService("ai", events),
            prefilter=FakeWorkerService("prefilter", events),
        )
        runtime._collector_task = asyncio.create_task(
            runtime._collector.run(options=runtime._collector_options)
        )
        await asyncio.sleep(0)

        with self.assertRaisesRegex(RuntimeError, "shutdown failed"):
            await runtime.stop()

        self.assertEqual(database.close_count, 1)

    async def test_runtime_preserves_observed_collector_failure(self):
        events = []
        database = FakeDatabase(events)
        runtime = AISalesRadarRuntime(
            database,
            collector=FailedCollector(events),
            matching=FakeWorkerService("matching", events),
            ai=FakeWorkerService("ai", events),
            prefilter=FakeWorkerService("prefilter", events),
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "collector failed before stop",
        ):
            await asyncio.wait_for(
                runtime.run(install_signal_handlers=False),
                timeout=0.5,
            )

        self.assertEqual(database.close_count, 1)

    async def test_collector_force_cancel_is_bounded_fallback(self):
        events = []
        database = FakeDatabase(events)
        collector = IgnoringCollector(events)
        runtime = AISalesRadarRuntime(
            database,
            collector=collector,
            matching=FakeWorkerService("matching", events),
            ai=FakeWorkerService("ai", events),
            prefilter=FakeWorkerService("prefilter", events),
            collector_stop_timeout_seconds=0.01,
        )
        runtime._collector_task = asyncio.create_task(
            collector.run(options=runtime._collector_options)
        )
        await asyncio.wait_for(collector.started.wait(), timeout=0.2)

        with self.assertRaisesRegex(RuntimeError, "shutdown failed"):
            await asyncio.wait_for(runtime.stop(), timeout=0.3)

        self.assertTrue(collector.cancelled)
        self.assertIn("collector.stop_requested", events)
        self.assertIn("collector.force_cancel", events)

    def test_check_is_read_only_and_declares_no_bot_sqlite_or_send(self):
        database = DatabaseSettings(
            database_url="postgresql+psycopg://local/radar",
            _env_file=None,
        )
        telegram = TelegramAuthSettings(
            api_id=123,
            api_hash="telegram-secret",
            _env_file=None,
        )
        ai = OpportunityAISettings(
            openai_api_key="sk-" + "project-fixture-secret-value-123456789",
            external_ai_data_processing_approved=True,
            _env_file=None,
        )
        output = io.StringIO()
        with (
            patch(
                "ai_sales_radar.runtime.DatabaseSettings.from_env",
                return_value=database,
            ),
            patch(
                "ai_sales_radar.runtime.TelegramAuthSettings.from_env",
                return_value=telegram,
            ),
            patch(
                "ai_sales_radar.runtime.OpportunityAISettings.from_env",
                return_value=ai,
            ),
            patch("ai_sales_radar.runtime.Database") as database_adapter,
            patch("urllib.request.urlopen") as network,
            patch("sys.stdout", output),
        ):
            self.assertEqual(main(["check"]), 0)
        database_adapter.assert_not_called()
        network.assert_not_called()
        payload = json.loads(output.getvalue())
        self.assertFalse(payload["telegram_bot_required"])
        self.assertFalse(payload["legacy_sqlite_required"])
        self.assertFalse(payload["automatic_sending_enabled"])
        self.assertEqual(payload["catch_up_default"], 0)
        self.assertEqual(payload["provider"], "openai")
        self.assertTrue(payload["provider_configured"])
        self.assertTrue(payload["external_ai_data_processing_approved"])
        self.assertNotIn("openai_configured", payload)


if __name__ == "__main__":
    unittest.main()

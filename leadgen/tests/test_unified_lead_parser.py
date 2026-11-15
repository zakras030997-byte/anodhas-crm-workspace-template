import asyncio
import unittest
from ai_sales_radar.unified import supervise


class UnifiedLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_stop_waits_for_both_components_to_flush(self):
        stop = asyncio.Event()
        started = []
        flushed = []
        async def worker(event):
            started.append(True)
            await event.wait()
            await asyncio.sleep(0)
            flushed.append(True)
        task = asyncio.create_task(supervise(
            {"collector": worker, "bot": worker}, stop_event=stop))
        while len(started) != 2:
            await asyncio.sleep(0)
        stop.set()
        await task
        self.assertEqual(len(flushed), 2)

    async def test_crash_stops_other_component_and_propagates(self):
        flushed = []
        async def failed(event):
            await asyncio.sleep(0)
            raise RuntimeError("simulated transport failure")
        async def peer(event):
            await event.wait()
            flushed.append(True)
        with self.assertRaisesRegex(RuntimeError, "simulated"):
            await supervise({"collector": failed, "bot": peer},
                            stop_event=asyncio.Event())
        self.assertEqual(flushed, [True])

    async def test_unexpected_clean_exit_is_failure(self):
        async def exits(event):
            return
        async def peer(event):
            await event.wait()
        with self.assertRaisesRegex(RuntimeError, "Component stopped"):
            await supervise({"collector": exits, "bot": peer},
                            stop_event=asyncio.Event())

    async def test_shutdown_failure_not_reported_as_success(self):
        event = asyncio.Event()
        async def worker(stop):
            await stop.wait()
            raise OSError("flush failed")
        task = asyncio.create_task(supervise(
            {"worker": worker}, stop_event=event))
        await asyncio.sleep(0)
        event.set()
        with self.assertRaises(OSError):
            await task

    async def test_stuck_component_is_cancelled_and_reported(self):
        event = asyncio.Event()
        cancelled = []
        async def stuck(stop):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.append(True)
        task = asyncio.create_task(supervise(
            {"stuck": stuck}, stop_event=event, shutdown_timeout=0.01))
        await asyncio.sleep(0)
        event.set()
        with self.assertRaises(TimeoutError):
            await task
        self.assertEqual(cancelled, [True])


class CollectorStartupStopTests(unittest.IsolatedAsyncioTestCase):
    async def test_stop_interrupts_rate_limited_initialization(self):
        from ai_sales_radar.collector import TelegramCollectorService
        from unittest.mock import AsyncMock
        service = object.__new__(TelegramCollectorService)
        service._stop_requested = asyncio.Event()
        started = asyncio.Event()
        cancelled = []
        async def resolving(*args):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.append(True)
        service._load_and_resolve = AsyncMock(side_effect=resolving)
        task = asyncio.create_task(service._prepare_live(None, None))
        await started.wait()
        await service.stop()
        result = await asyncio.wait_for(task, 0.5)
        self.assertIsNone(result)
        self.assertEqual(cancelled, [True])

    async def test_initialization_error_propagates(self):
        from ai_sales_radar.collector import TelegramCollectorService
        from unittest.mock import AsyncMock
        service = object.__new__(TelegramCollectorService)
        service._stop_requested = asyncio.Event()
        service._load_and_resolve = AsyncMock(side_effect=ValueError("identity"))
        with self.assertRaisesRegex(ValueError, "identity"):
            await service._prepare_live(None, None)

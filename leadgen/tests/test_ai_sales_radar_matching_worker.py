from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
import io
import json
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from ai_sales_radar.config import DatabaseSettings
from ai_sales_radar.matching_worker import (
    MatchingInboxJobProcessor,
    build_matching_worker,
    main,
)
from freelancer_bot.matching_jobs import (
    MATCHING_INBOX_JOB_TYPE,
    matching_job_idempotency_key,
)


class FakeDatabase:
    @asynccontextmanager
    async def connect(self):
        yield object()


class MatchingWorkerCompositionTest(unittest.TestCase):
    def test_factory_registers_only_existing_matching_job_type(self):
        captured = {}
        sentinel = object()

        def worker_factory(database, **kwargs):
            captured["database"] = database
            captured.update(kwargs)
            return sentinel

        result = build_matching_worker(
            object(),
            logger=object(),
            repository="jobs",
            processor="inbox-only",
            worker_id="matching-test",
            worker_factory=worker_factory,
        )

        self.assertIs(result, sentinel)
        self.assertEqual(
            captured["handlers"],
            {MATCHING_INBOX_JOB_TYPE: "inbox-only"},
        )
        self.assertFalse(captured["close_database_on_exit"])

    def test_check_never_builds_database_matching_or_delivery(self):
        settings = DatabaseSettings(
            database_url="postgresql+psycopg://local/radar",
            _env_file=None,
        )
        output = io.StringIO()
        with (
            patch(
                "ai_sales_radar.matching_worker.DatabaseSettings.from_env",
                return_value=settings,
            ),
            patch("ai_sales_radar.matching_worker.Database") as database,
            patch(
                "ai_sales_radar.matching_worker.MatchingInboxJobProcessor"
            ) as processor,
            patch("sys.stdout", output),
        ):
            self.assertEqual(main(["check"]), 0)
        database.assert_not_called()
        processor.assert_not_called()
        payload = json.loads(output.getvalue())
        self.assertFalse(payload["telegram_delivery_enabled"])
        self.assertEqual(payload["ai_calls"], 0)


class MatchingInboxProcessorTest(unittest.IsolatedAsyncioTestCase):
    async def test_processor_runs_matching_then_inbox_without_delivery(self):
        opportunity_id = uuid4()
        job_id = uuid4()
        created_at = datetime(2026, 8, 26, tzinfo=timezone.utc)
        matching_outcome = SimpleNamespace(
            persistence=SimpleNamespace(
                run=SimpleNamespace(id=uuid4()),
                traces=(),
            ),
            report=SimpleNamespace(
                eligible_match_count=0,
                user_specific_llm_calls=0,
            ),
        )
        matching = SimpleNamespace(
            generate_matches=AsyncMock(return_value=matching_outcome)
        )
        materialization = SimpleNamespace(
            created_count=0,
            updated_count=0,
        )
        inbox = SimpleNamespace(
            materialize=AsyncMock(return_value=materialization)
        )
        jobs = SimpleNamespace(
            get=AsyncMock(return_value={"created_at": created_at})
        )
        processor = MatchingInboxJobProcessor(
            FakeDatabase(),
            matching=matching,
            inbox=inbox,
            jobs=jobs,
            logger=SimpleNamespace(log=lambda *_args, **_kwargs: None),
        )
        claim = SimpleNamespace(
            id=job_id,
            job_type=MATCHING_INBOX_JOB_TYPE,
            idempotency_key=matching_job_idempotency_key(opportunity_id),
        )

        result = await processor.process(claim)

        self.assertEqual(result.opportunity_id, opportunity_id)
        matching.generate_matches.assert_awaited_once_with(
            (opportunity_id,),
            evaluated_at=created_at,
        )
        inbox.materialize.assert_awaited_once_with(matching_outcome)

    async def test_processor_rejects_other_job_type_before_database_access(self):
        jobs = SimpleNamespace(get=AsyncMock())
        processor = MatchingInboxJobProcessor(
            FakeDatabase(),
            matching=SimpleNamespace(),
            inbox=SimpleNamespace(),
            jobs=jobs,
        )
        claim = SimpleNamespace(
            id=uuid4(),
            job_type="personalized.delivery.v1",
            idempotency_key="not-used",
        )
        with self.assertRaisesRegex(ValueError, "unsupported"):
            await processor.process(claim)
        jobs.get.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()

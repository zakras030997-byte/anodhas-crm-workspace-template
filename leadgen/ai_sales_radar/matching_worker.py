"""Durable matching worker that materializes the local inbox without delivery."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass
from datetime import datetime
import json
import logging
import os
import signal
import socket
import sys
from collections.abc import Callable
from uuid import UUID, uuid4

from freelancer_bot.matching_jobs import (
    MATCHING_INBOX_JOB_TYPE,
    matching_job_idempotency_key,
    opportunity_id_from_matching_job_key,
)
from freelancer_bot.matching_service import (
    CandidateMatchingService,
    MatchGenerationOutcome,
)
from freelancer_bot.observability import Redactor, configure_structured_logger, log_event
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.jobs import DurableJobRepository, JobClaim
from freelancer_bot.worker import DurableWorker, WorkerOptions

from .config import ConfigurationError, DatabaseSettings
from .inbox import InboxMaterializationReport, LeadInboxService


@dataclass(frozen=True)
class MatchingInboxOutcome:
    opportunity_id: UUID
    matching: MatchGenerationOutcome
    inbox: InboxMaterializationReport


@dataclass(frozen=True)
class MatchingWorkerCheck:
    database_configured: bool
    database_driver_valid: bool
    handled_job_types: tuple[str, ...]
    semantic_provider: str = "deterministic-local"
    telegram_delivery_enabled: bool = False
    ai_calls: int = 0


class MatchingInboxJobProcessor:
    def __init__(
        self,
        database: Database,
        *,
        matching: CandidateMatchingService | None = None,
        inbox: LeadInboxService | None = None,
        jobs: DurableJobRepository | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._database = database
        self._matching = matching or CandidateMatchingService(database)
        self._inbox = inbox or LeadInboxService(
            database,
            matching=self._matching,
        )
        self._jobs = jobs or DurableJobRepository()
        self._logger = logger or logging.getLogger(__name__)

    async def __call__(self, claim: JobClaim) -> MatchingInboxOutcome:
        return await self.process(claim)

    async def process(self, claim: JobClaim) -> MatchingInboxOutcome:
        if claim.job_type != MATCHING_INBOX_JOB_TYPE:
            raise ValueError("matching inbox processor received an unsupported job type")
        opportunity_id = opportunity_id_from_matching_job_key(
            claim.idempotency_key
        )
        if claim.idempotency_key != matching_job_idempotency_key(opportunity_id):
            raise ValueError("matching job idempotency key is not canonical")

        async with self._database.connect() as connection:
            job = await self._jobs.get(connection, claim.id)
        if job is None:
            raise LookupError(f"matching job {claim.id} does not exist")
        evaluated_at = _aware(job["created_at"])
        matching = await self._matching.generate_matches(
            (opportunity_id,),
            evaluated_at=evaluated_at,
        )
        materialized = await self._inbox.materialize(matching)
        result = MatchingInboxOutcome(
            opportunity_id=opportunity_id,
            matching=matching,
            inbox=materialized,
        )
        log_event(
            self._logger,
            logging.INFO,
            "matching.inbox_materialized",
            opportunity_id=opportunity_id,
            match_run_id=matching.persistence.run.id,
            trace_count=len(matching.persistence.traces),
            eligible_match_count=matching.report.eligible_match_count,
            inbox_created_count=materialized.created_count,
            inbox_updated_count=materialized.updated_count,
            telegram_delivery_enabled=False,
            user_specific_llm_calls=matching.report.user_specific_llm_calls,
        )
        return result


def build_matching_worker(
    database: Database,
    *,
    logger: logging.Logger,
    repository: DurableJobRepository | None = None,
    processor: MatchingInboxJobProcessor | None = None,
    worker_options: WorkerOptions | None = None,
    worker_id: str | None = None,
    worker_factory: Callable[..., DurableWorker] = DurableWorker,
) -> DurableWorker:
    jobs = repository or DurableJobRepository()
    handler = processor or MatchingInboxJobProcessor(
        database,
        jobs=jobs,
        logger=logger,
    )
    return worker_factory(
        database,
        repository=jobs,
        worker_id=worker_id or _worker_id(),
        handlers={MATCHING_INBOX_JOB_TYPE: handler},
        logger=logger,
        options=worker_options or WorkerOptions(),
        close_database_on_exit=False,
    )


class MatchingWorkerService:
    def __init__(
        self,
        database: Database,
        *,
        logger: logging.Logger,
        worker: DurableWorker | None = None,
    ) -> None:
        self._worker = worker or build_matching_worker(database, logger=logger)
        self._task: asyncio.Task[None] | None = None
        self._started = False

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        if self._started:
            raise RuntimeError("Matching worker service cannot be started twice")
        self._started = True
        self._task = asyncio.create_task(
            self._worker.run(install_signal_handlers=False),
            name="ai-sales-radar-matching-worker",
        )
        await asyncio.sleep(0)
        if self._task.done():
            await self._task

    async def wait(self) -> None:
        if self._task is None:
            raise RuntimeError("Matching worker service is not started")
        await self._task

    async def stop(self) -> None:
        task = self._task
        if task is None:
            return
        self._worker.request_stop()
        try:
            await task
        finally:
            self._task = None


def inspect_matching_configuration(
    settings: DatabaseSettings,
) -> MatchingWorkerCheck:
    valid = False
    if settings.configured():
        settings.require_url()
        valid = True
    return MatchingWorkerCheck(
        database_configured=settings.configured(),
        database_driver_valid=valid,
        handled_job_types=(MATCHING_INBOX_JOB_TYPE,),
    )


async def run_matching_worker(
    settings: DatabaseSettings,
    *,
    database_factory: Callable[..., Database] = Database,
) -> None:
    database_url = settings.require_url()
    database = database_factory(database_url)
    logger = configure_structured_logger(
        "ai_sales_radar.matching_worker",
        redactor=Redactor({database_url}),
    )
    service = MatchingWorkerService(database, logger=logger)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []
    worker_wait: asyncio.Task[None] | None = None
    stop_wait: asyncio.Task[bool] | None = None
    for selected_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(selected_signal, stop_event.set)
        except (NotImplementedError, RuntimeError):
            continue
        installed.append(selected_signal)
    try:
        await service.start()
        worker_wait = asyncio.create_task(service.wait())
        stop_wait = asyncio.create_task(stop_event.wait())
        done, _ = await asyncio.wait(
            {worker_wait, stop_wait},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if worker_wait in done:
            stop_wait.cancel()
            await _consume_cancelled(stop_wait)
            await worker_wait
            raise RuntimeError("Matching worker stopped unexpectedly")
    finally:
        for selected_signal in installed:
            loop.remove_signal_handler(selected_signal)
        if stop_wait is not None and not stop_wait.done():
            stop_wait.cancel()
            await _consume_cancelled(stop_wait)
        try:
            await service.stop()
        finally:
            if worker_wait is not None:
                await _consume_cancelled(worker_wait)
            await database.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="AI Sales Radar: matching и локальный Lead Inbox",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "check",
        help="проверить настройки без БД, Telegram и AI-вызовов",
    )
    commands.add_parser("run", help="запустить matching worker")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        settings = DatabaseSettings.from_env()
        if args.command == "check":
            status = inspect_matching_configuration(settings)
            print(json.dumps(asdict(status), ensure_ascii=False, sort_keys=True))
            return 0 if status.database_configured else 1
        if args.command == "run":
            asyncio.run(run_matching_worker(settings))
            return 0
    except ConfigurationError as exc:
        print(f"Ошибка конфигурации: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(
            "Matching worker остановлен из-за ошибки. Подробности скрыты "
            f"для защиты секретов ({type(exc).__name__}).",
            file=sys.stderr,
        )
        return 1
    return 2


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("durable matching job created_at must include a timezone")
    return value


def _worker_id() -> str:
    return f"ai-sales-radar-match-{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:8]}"


async def _consume_cancelled(task: asyncio.Task[object]) -> None:
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass


__all__ = [
    "MatchingInboxJobProcessor",
    "MatchingInboxOutcome",
    "MatchingWorkerCheck",
    "MatchingWorkerService",
    "build_matching_worker",
    "build_parser",
    "inspect_matching_configuration",
    "main",
    "run_matching_worker",
]

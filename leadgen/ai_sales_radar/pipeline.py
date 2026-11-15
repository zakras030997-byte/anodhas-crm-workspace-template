"""Raw-message prefilter worker for the scripts-only AI Sales Radar runtime.

This boundary never calls an AI provider.  It receives only the deterministic
route compatibility key needed by durable dedup and analysis jobs, and knows
nothing about matching, delivery, Telegram bots, discovery, billing, or the
legacy SQLite lead store.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass
import json
import logging
import os
import signal
import socket
import sys
from collections.abc import Callable
from uuid import uuid4

from freelancer_bot.message_prefilter import RawMessagePrefilterProcessor
from freelancer_bot.observability import Redactor, configure_structured_logger
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.jobs import DurableJobRepository
from freelancer_bot.persistence.raw_messages import RAW_MESSAGE_JOB_TYPE
from freelancer_bot.worker import DurableWorker, WorkerOptions

from .ai_worker import opportunity_analyzer_compatibility
from .config import ConfigurationError, DatabaseSettings, OpportunityAISettings


WorkerFactory = Callable[..., DurableWorker]


@dataclass(frozen=True)
class PrefilterCheck:
    database_configured: bool
    database_driver_valid: bool
    handled_job_types: tuple[str, ...]
    openai_required: bool = False
    telegram_bot_required: bool = False


def build_prefilter_worker(
    database: Database,
    *,
    logger: logging.Logger,
    analyzer_version: str | None = None,
    analysis_schema_version: str | None = None,
    repository: DurableJobRepository | None = None,
    processor: RawMessagePrefilterProcessor | None = None,
    worker_options: WorkerOptions | None = None,
    worker_id: str | None = None,
    worker_factory: WorkerFactory = DurableWorker,
) -> DurableWorker:
    """Compose the deliberately narrow raw-message worker."""
    jobs = repository or DurableJobRepository()
    handler = processor
    if handler is None:
        if analyzer_version is None or analysis_schema_version is None:
            raise ValueError(
                "Prefilter worker requires an explicit analyzer compatibility route"
            )
        handler = RawMessagePrefilterProcessor(
            database,
            jobs=jobs,
            analyzer_version=analyzer_version,
            analysis_schema_version=analysis_schema_version,
            logger=logger,
        )
    return worker_factory(
        database,
        repository=jobs,
        worker_id=worker_id or _worker_id(),
        handlers={RAW_MESSAGE_JOB_TYPE: handler},
        logger=logger,
        options=worker_options or WorkerOptions(),
        close_database_on_exit=False,
    )


class RawPrefilterWorkerService:
    """Own one prefilter-only durable worker task over a shared Database."""

    def __init__(
        self,
        database: Database,
        *,
        logger: logging.Logger,
        worker: DurableWorker | None = None,
        analyzer_version: str | None = None,
        analysis_schema_version: str | None = None,
        repository: DurableJobRepository | None = None,
        worker_options: WorkerOptions | None = None,
        worker_id: str | None = None,
    ) -> None:
        self._worker = worker or build_prefilter_worker(
            database,
            logger=logger,
            analyzer_version=analyzer_version,
            analysis_schema_version=analysis_schema_version,
            repository=repository,
            worker_options=worker_options,
            worker_id=worker_id,
        )
        self._task: asyncio.Task[None] | None = None
        self._started = False

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        if self._started:
            raise RuntimeError("Prefilter worker service cannot be started twice")
        self._started = True
        self._task = asyncio.create_task(
            self._worker.run(install_signal_handlers=False),
            name="ai-sales-radar-prefilter-worker",
        )
        await asyncio.sleep(0)
        if self._task.done():
            await self._task

    async def wait(self) -> None:
        if self._task is None:
            raise RuntimeError("Prefilter worker service is not started")
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


def inspect_prefilter_configuration(
    settings: DatabaseSettings,
) -> PrefilterCheck:
    configured = settings.configured()
    valid = False
    if configured:
        settings.require_url()
        valid = True
    return PrefilterCheck(
        database_configured=configured,
        database_driver_valid=valid,
        handled_job_types=(RAW_MESSAGE_JOB_TYPE,),
    )


async def run_prefilter_worker(
    settings: DatabaseSettings,
    ai_settings: OpportunityAISettings,
    *,
    database_factory: Callable[..., Database] = Database,
) -> None:
    compatibility = opportunity_analyzer_compatibility(ai_settings)
    database_url = settings.require_url()
    database = database_factory(database_url)
    logger = configure_structured_logger(
        "ai_sales_radar.prefilter",
        redactor=Redactor({database_url}),
    )
    service = RawPrefilterWorkerService(
        database,
        logger=logger,
        analyzer_version=compatibility.analyzer_version,
        analysis_schema_version=compatibility.analysis_schema_version,
    )
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
            raise RuntimeError("Prefilter worker stopped unexpectedly")
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
        description="AI Sales Radar: дешёвая фильтрация и exact dedup",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "check",
        help="проверить настройки без подключения к БД и внешним сервисам",
    )
    commands.add_parser("run", help="запустить prefilter worker")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        settings = DatabaseSettings.from_env()
        if args.command == "check":
            print(
                json.dumps(
                    asdict(inspect_prefilter_configuration(settings)),
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 0 if settings.configured() else 1
        if args.command == "run":
            ai_settings = OpportunityAISettings.from_env()
            asyncio.run(run_prefilter_worker(settings, ai_settings))
            return 0
    except ConfigurationError as exc:
        print(f"Ошибка конфигурации: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(
            "Prefilter worker остановлен из-за ошибки. Подробности скрыты "
            f"для защиты секретов ({type(exc).__name__}).",
            file=sys.stderr,
        )
        return 1
    return 2


def _worker_id() -> str:
    return f"ai-sales-radar-prefilter-{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:8]}"


async def _consume_cancelled(task: asyncio.Task[object]) -> None:
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass


__all__ = [
    "PrefilterCheck",
    "RawPrefilterWorkerService",
    "build_prefilter_worker",
    "build_parser",
    "inspect_prefilter_configuration",
    "main",
    "run_prefilter_worker",
]

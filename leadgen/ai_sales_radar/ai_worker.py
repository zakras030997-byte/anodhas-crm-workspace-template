"""Provider-neutral opportunity-extraction worker for the scripts-only runtime."""

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

from freelancer_bot.ai_telemetry import AIModelPrice, AISpendGuardPolicy
from freelancer_bot.message_prefilter import OPPORTUNITY_ANALYSIS_JOB_TYPE
from freelancer_bot.matching_jobs import enqueue_matching_inbox
from freelancer_bot.observability import Redactor, configure_structured_logger
from freelancer_bot.opportunity_analysis import (
    OPPORTUNITY_ANALYSIS_PROMPT_VERSION,
    OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
    OPPORTUNITY_ANALYZER_VERSION,
    OpenAICompatibleOpportunityAnalyzer,
    OpportunityAnalyzer,
    openai_compatible_opportunity_cache_identity,
    opportunity_analysis_cache_version_for_identity,
)
from freelancer_bot.opportunity_classifier import OpportunityAnalysisJobProcessor
from freelancer_bot.persistence.ai_telemetry import PostgreSQLAICallRecorder
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.jobs import DurableJobRepository
from freelancer_bot.worker import DurableWorker, WorkerOptions

from .config import (
    ConfigurationError,
    DatabaseSettings,
    OpportunityAISettings,
)


@dataclass(frozen=True)
class AIWorkerCheck:
    database_configured: bool
    database_driver_valid: bool
    provider_configured: bool
    external_ai_data_processing_approved: bool
    provider: str
    model: str
    handled_job_types: tuple[str, ...]
    daily_spend_limit_usd: str | None
    monthly_spend_limit_usd: str | None
    maximum_completion_tokens: int
    fallback_enabled: bool = False
    automatic_sending_enabled: bool = False


@dataclass(frozen=True)
class OpportunityAnalyzerCompatibility:
    analyzer_version: str
    analysis_schema_version: str


def opportunity_analyzer_compatibility(
    settings: OpportunityAISettings,
) -> OpportunityAnalyzerCompatibility:
    """Resolve the durable route key without a DB connection or network call."""
    identity = openai_compatible_opportunity_cache_identity(
        provider=settings.provider,
        model=settings.model,
        analyzer_version=OPPORTUNITY_ANALYZER_VERSION,
        prompt_version=OPPORTUNITY_ANALYSIS_PROMPT_VERSION,
        schema_version=OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
        endpoint=settings.chat_completions_url(),
        temperature=settings.temperature,
        max_completion_tokens=settings.reserve_output_tokens,
    )
    return OpportunityAnalyzerCompatibility(
        analyzer_version=opportunity_analysis_cache_version_for_identity(
            analyzer_version=OPPORTUNITY_ANALYZER_VERSION,
            cache_identity=identity,
        ),
        analysis_schema_version=OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
    )


def build_opportunity_analyzer(
    database: Database,
    settings: OpportunityAISettings,
    *,
    recorder_factory: Callable[..., PostgreSQLAICallRecorder] = (
        PostgreSQLAICallRecorder
    ),
    analyzer_factory: Callable[..., OpportunityAnalyzer] = (
        OpenAICompatibleOpportunityAnalyzer
    ),
) -> OpportunityAnalyzer:
    settings.require_external_ai_data_processing_approval()
    spend_guard = None
    if (
        settings.daily_spend_limit_usd is not None
        or settings.monthly_spend_limit_usd is not None
    ):
        spend_guard = AISpendGuardPolicy(
            daily_limit_usd=settings.daily_spend_limit_usd,
            monthly_limit_usd=settings.monthly_spend_limit_usd,
            reserve_input_tokens=settings.reserve_input_tokens,
            reserve_output_tokens=settings.reserve_output_tokens,
        )
    recorder = recorder_factory(database, spend_guard=spend_guard)
    return analyzer_factory(
        api_key=settings.require_api_key(),
        api_key_name=settings.api_key_name(),
        model=settings.model,
        temperature=settings.temperature,
        timeout_seconds=settings.timeout_seconds,
        max_output_attempts=settings.max_output_attempts,
        max_completion_tokens=settings.reserve_output_tokens,
        base_url=settings.chat_completions_url(),
        provider=settings.provider,
        recorder=recorder,
        price=AIModelPrice(
            pricing_version=settings.pricing_version,
            input_usd_per_million=settings.input_usd_per_million,
            output_usd_per_million=settings.output_usd_per_million,
        ),
    )


def build_ai_worker(
    database: Database,
    settings: OpportunityAISettings,
    *,
    logger: logging.Logger,
    repository: DurableJobRepository | None = None,
    analyzer: OpportunityAnalyzer | None = None,
    processor: OpportunityAnalysisJobProcessor | None = None,
    worker_options: WorkerOptions | None = None,
    worker_id: str | None = None,
    worker_factory: Callable[..., DurableWorker] = DurableWorker,
) -> DurableWorker:
    settings.require_external_ai_data_processing_approval()
    jobs = repository or DurableJobRepository()
    selected_analyzer = analyzer
    if processor is None:
        selected_analyzer = selected_analyzer or build_opportunity_analyzer(
            database,
            settings,
        )
        processor = OpportunityAnalysisJobProcessor(
            database,
            selected_analyzer,
            jobs=jobs,
            matching_enqueuer=enqueue_matching_inbox,
            logger=logger,
        )
    return worker_factory(
        database,
        repository=jobs,
        worker_id=worker_id or _worker_id(),
        handlers={OPPORTUNITY_ANALYSIS_JOB_TYPE: processor},
        logger=logger,
        options=worker_options or WorkerOptions(),
        close_database_on_exit=False,
    )


class AIWorkerService:
    def __init__(
        self,
        database: Database,
        settings: OpportunityAISettings,
        *,
        logger: logging.Logger,
        worker: DurableWorker | None = None,
        analyzer: OpportunityAnalyzer | None = None,
    ) -> None:
        self._worker = worker or build_ai_worker(
            database,
            settings,
            logger=logger,
            analyzer=analyzer,
        )
        self._task: asyncio.Task[None] | None = None
        self._started = False

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        if self._started:
            raise RuntimeError("AI worker service cannot be started twice")
        self._started = True
        self._task = asyncio.create_task(
            self._worker.run(install_signal_handlers=False),
            name="ai-sales-radar-opportunity-worker",
        )
        await asyncio.sleep(0)
        if self._task.done():
            await self._task

    async def wait(self) -> None:
        if self._task is None:
            raise RuntimeError("AI worker service is not started")
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


def inspect_ai_configuration(
    database: DatabaseSettings,
    ai: OpportunityAISettings,
) -> AIWorkerCheck:
    database_valid = False
    if database.configured():
        database.require_url()
        database_valid = True
    return AIWorkerCheck(
        database_configured=database.configured(),
        database_driver_valid=database_valid,
        provider_configured=ai.configured(),
        external_ai_data_processing_approved=(
            ai.external_ai_data_processing_approved
        ),
        provider=ai.provider,
        model=ai.model,
        handled_job_types=(OPPORTUNITY_ANALYSIS_JOB_TYPE,),
        daily_spend_limit_usd=(
            None
            if ai.daily_spend_limit_usd is None
            else str(ai.daily_spend_limit_usd)
        ),
        monthly_spend_limit_usd=(
            None
            if ai.monthly_spend_limit_usd is None
            else str(ai.monthly_spend_limit_usd)
        ),
        maximum_completion_tokens=ai.reserve_output_tokens,
    )


async def run_ai_worker(
    database_settings: DatabaseSettings,
    ai_settings: OpportunityAISettings,
    *,
    database_factory: Callable[..., Database] = Database,
) -> None:
    ai_settings.require_external_ai_data_processing_approval()
    database_url = database_settings.require_url()
    api_key = ai_settings.require_api_key()
    database = database_factory(database_url)
    logger = configure_structured_logger(
        "ai_sales_radar.opportunity_worker",
        redactor=Redactor({database_url, api_key}),
    )
    service = AIWorkerService(database, ai_settings, logger=logger)
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
            raise RuntimeError("AI worker stopped unexpectedly")
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
        description="AI Sales Radar: извлечение Opportunities через AI provider",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "check",
        help="проверить настройки без подключения к БД и без AI-запроса",
    )
    commands.add_parser("run", help="запустить Opportunity Analysis worker")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        database = DatabaseSettings.from_env()
        ai = OpportunityAISettings.from_env()
        if args.command == "check":
            status = inspect_ai_configuration(database, ai)
            print(json.dumps(asdict(status), ensure_ascii=False, sort_keys=True))
            ready = (
                status.database_configured
                and status.provider_configured
                and status.external_ai_data_processing_approved
            )
            return 0 if ready else 1
        if args.command == "run":
            asyncio.run(run_ai_worker(database, ai))
            return 0
    except ConfigurationError as exc:
        print(f"Ошибка конфигурации: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(
            "AI worker остановлен из-за ошибки. Подробности скрыты "
            f"для защиты секретов ({type(exc).__name__}).",
            file=sys.stderr,
        )
        return 1
    return 2


def _worker_id() -> str:
    return f"ai-sales-radar-ai-{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:8]}"


async def _consume_cancelled(task: asyncio.Task[object]) -> None:
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass


__all__ = [
    "AIWorkerCheck",
    "AIWorkerService",
    "OpportunityAnalyzerCompatibility",
    "build_ai_worker",
    "build_opportunity_analyzer",
    "build_parser",
    "inspect_ai_configuration",
    "main",
    "opportunity_analyzer_compatibility",
    "run_ai_worker",
]

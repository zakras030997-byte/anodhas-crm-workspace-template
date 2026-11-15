"""One local process for Telegram → Inbox, without GUI or auto-send."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass
import json
import logging
import math
import signal
import sys
from collections.abc import Callable

from freelancer_bot.observability import Redactor, configure_structured_logger
from freelancer_bot.matching_jobs import MATCHING_INBOX_JOB_TYPE
from freelancer_bot.message_prefilter import OPPORTUNITY_ANALYSIS_JOB_TYPE
from freelancer_bot.opportunity_analysis import opportunity_analysis_cache_version
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.raw_messages import RAW_MESSAGE_JOB_TYPE

from .ai_worker import AIWorkerService, build_opportunity_analyzer
from .collector import (
    CollectorOptions,
    TelegramCollectorError,
    TelegramCollectorService,
)
from .config import (
    ConfigurationError,
    DatabaseSettings,
    OperatorSourceDiscoverySettings,
    OpportunityAISettings,
    TelegramAuthSettings,
)
from .matching_worker import MatchingWorkerService
from .pipeline import RawPrefilterWorkerService
from .source_discovery import (
    OPERATOR_SOURCE_DISCOVERY_JOB_TYPE,
    OperatorSourceDiscoveryWorkerService,
)


DEFAULT_COLLECTOR_STOP_TIMEOUT_SECONDS = 25.0


@dataclass(frozen=True)
class RuntimeCheck:
    database_configured: bool
    database_driver_valid: bool
    telegram_configured: bool
    provider_configured: bool
    external_ai_data_processing_approved: bool
    provider: str
    catch_up_default: int
    continuous_job_types: tuple[str, ...]
    telegram_bot_required: bool = False
    legacy_sqlite_required: bool = False
    automatic_sending_enabled: bool = False


class AISalesRadarRuntime:
    """Own shared resources and stop ingestion before durable workers."""

    def __init__(
        self,
        database: Database,
        *,
        collector: TelegramCollectorService,
        prefilter: RawPrefilterWorkerService,
        ai: AIWorkerService,
        matching: MatchingWorkerService,
        collector_options: CollectorOptions | None = None,
        collector_stop_timeout_seconds: float = (
            DEFAULT_COLLECTOR_STOP_TIMEOUT_SECONDS
        ),
    ) -> None:
        if (
            not math.isfinite(collector_stop_timeout_seconds)
            or collector_stop_timeout_seconds <= 0
        ):
            raise ValueError(
                "collector_stop_timeout_seconds must be finite and positive"
            )
        self._database = database
        self._collector = collector
        self._prefilter = prefilter
        self._ai = ai
        self._matching = matching
        self._collector_options = collector_options or CollectorOptions()
        self._collector_stop_timeout_seconds = collector_stop_timeout_seconds
        self._collector_task: asyncio.Task[object] | None = None
        self._collector_result_observed = False
        self._started_services: list[object] = []
        self._closed = False

    async def run(
        self,
        *,
        stop_event: asyncio.Event | None = None,
        install_signal_handlers: bool = True,
    ) -> None:
        selected_stop = stop_event or asyncio.Event()
        loop = asyncio.get_running_loop()
        installed: list[signal.Signals] = []
        if install_signal_handlers:
            for selected_signal in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(selected_signal, selected_stop.set)
                except (NotImplementedError, RuntimeError):
                    continue
                installed.append(selected_signal)

        waiters: list[asyncio.Task[object]] = []
        stop_wait: asyncio.Task[bool] | None = None
        try:
            # Downstream consumers are ready before Telegram can enqueue work.
            for service in (self._matching, self._ai, self._prefilter):
                self._started_services.append(service)
                await service.start()
            self._collector_task = asyncio.create_task(
                self._collector.run(options=self._collector_options),
                name="ai-sales-radar-telegram-collector",
            )
            self._collector_result_observed = False
            waiters = [
                asyncio.create_task(
                    service.wait(),
                    name=f"ai-sales-radar-wait-{name}",
                )
                for service, name in (
                    (self._matching, "matching"),
                    (self._ai, "ai"),
                    (self._prefilter, "prefilter"),
                )
            ]
            waiters.append(self._collector_task)
            stop_wait = asyncio.create_task(
                selected_stop.wait(),
                name="ai-sales-radar-stop-signal",
            )
            done, _ = await asyncio.wait(
                {*waiters, stop_wait},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if self._collector_task in done:
                try:
                    await self._collector_task
                finally:
                    self._collector_result_observed = True
                raise RuntimeError("Telegram collector stopped unexpectedly")
            if stop_wait in done:
                return
            completed = next(task for task in waiters if task in done)
            await completed
            raise RuntimeError("A durable pipeline worker stopped unexpectedly")
        finally:
            for selected_signal in installed:
                loop.remove_signal_handler(selected_signal)
            if stop_wait is not None and not stop_wait.done():
                stop_wait.cancel()
                await _consume_cancelled(stop_wait)
            # A waiter awaits the worker's real task.  Cancelling the wrapper
            # first would propagate cancellation into DurableWorker and bypass
            # its cooperative request_stop()/claim-release path.  Stop the
            # owned services first; only then clean up any leftover wrappers.
            try:
                await self.stop()
            finally:
                for waiter in waiters:
                    if not waiter.done():
                        waiter.cancel()
                    await _consume_cancelled(waiter)

    async def stop(self) -> None:
        if self._closed:
            return
        self._closed = True
        failures: list[BaseException] = []

        # Stop accepting Telegram events first.
        collector_task = self._collector_task
        if collector_task is not None and not self._collector_result_observed:
            already_stopped = collector_task.done()
            forced_cancel = False
            if not already_stopped:
                try:
                    await self._collector.stop()
                except BaseException as exc:
                    failures.append(exc)
                done, _ = await asyncio.wait(
                    {collector_task},
                    timeout=self._collector_stop_timeout_seconds,
                )
                if collector_task not in done:
                    forced_cancel = True
                    failures.append(
                        TimeoutError("Telegram collector cooperative stop timed out")
                    )
                    collector_task.cancel()
                    done, _ = await asyncio.wait(
                        {collector_task},
                        timeout=min(
                            1.0,
                            self._collector_stop_timeout_seconds,
                        ),
                    )
                    if collector_task not in done:
                        collector_task.add_done_callback(_consume_task_result)
            if collector_task.done():
                try:
                    await collector_task
                except asyncio.CancelledError as exc:
                    if not forced_cancel:
                        failures.append(exc)
                except BaseException as exc:
                    failures.append(exc)
                else:
                    if already_stopped:
                        failures.append(
                            RuntimeError(
                                "Telegram collector stopped before runtime shutdown"
                            )
                        )
                finally:
                    self._collector_result_observed = True
        self._collector_task = None

        # Each durable worker drains or safely releases its current claim.
        for service in reversed(self._started_services):
            try:
                await service.stop()
            except BaseException as exc:
                failures.append(exc)
        self._started_services.clear()
        try:
            await self._database.close()
        except BaseException as exc:
            failures.append(exc)
        if failures:
            raise RuntimeError(
                "AI Sales Radar shutdown failed "
                f"({type(failures[0]).__name__})"
            ) from None


def inspect_runtime_configuration(
    database: DatabaseSettings,
    telegram: TelegramAuthSettings,
    ai: OpportunityAISettings,
) -> RuntimeCheck:
    valid_database = False
    if database.configured():
        database.require_url()
        valid_database = True
    return RuntimeCheck(
        database_configured=database.configured(),
        database_driver_valid=valid_database,
        telegram_configured=telegram.configured(),
        provider_configured=ai.configured(),
        external_ai_data_processing_approved=(
            ai.external_ai_data_processing_approved
        ),
        provider=ai.provider,
        catch_up_default=0,
        continuous_job_types=(
            RAW_MESSAGE_JOB_TYPE,
            OPPORTUNITY_ANALYSIS_JOB_TYPE,
            MATCHING_INBOX_JOB_TYPE,
            OPERATOR_SOURCE_DISCOVERY_JOB_TYPE,
        ),
    )


async def run_local_runtime(
    database_settings: DatabaseSettings,
    telegram_settings: TelegramAuthSettings,
    ai_settings: OpportunityAISettings,
    *,
    collector_options: CollectorOptions | None = None,
    source_discovery_settings: OperatorSourceDiscoverySettings | None = None,
    database_factory: Callable[..., Database] = Database,
    stop_event: asyncio.Event | None = None,
    research_reader=None,
) -> None:
    ai_settings.require_external_ai_data_processing_approval()
    database_url = database_settings.require_url()
    telegram = telegram_settings.require_credentials()
    ai_api_key = ai_settings.require_api_key()
    database = database_factory(database_url)
    redactor = Redactor(
        {
            database_url,
            telegram.api_hash,
            str(telegram.session_path),
            ai_api_key,
        }
    )
    logger = configure_structured_logger(
        "ai_sales_radar.runtime",
        redactor=redactor,
    )
    configure_structured_logger("ai_sales_radar", redactor=redactor)
    configure_structured_logger("freelancer_bot", redactor=redactor)
    analyzer = build_opportunity_analyzer(database, ai_settings)
    analyzer_version = opportunity_analysis_cache_version(analyzer)
    discovery = source_discovery_settings or OperatorSourceDiscoverySettings.from_env()
    runtime = AISalesRadarRuntime(
        database,
        collector=TelegramCollectorService(
            database,
            telegram,
            research_reader=research_reader,
            connected_service_factory=(
                None
                if not discovery.enabled
                else lambda client, _collector_account_id, governor: (
                    OperatorSourceDiscoveryWorkerService(
                        database,
                        client=client,
                        governor=governor,
                        settings=discovery,
                        logger=logger,
                    )
                )
            ),
        ),
        prefilter=RawPrefilterWorkerService(
            database,
            logger=logger,
            analyzer_version=analyzer_version,
            analysis_schema_version=analyzer.schema_version,
        ),
        ai=AIWorkerService(
            database,
            ai_settings,
            logger=logger,
            analyzer=analyzer,
        ),
        matching=MatchingWorkerService(database, logger=logger),
        collector_options=collector_options,
    )
    if stop_event is None:
        await runtime.run()
    else:
        await runtime.run(stop_event=stop_event, install_signal_handlers=False)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="AI Sales Radar scripts-only local runtime",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "check",
        help="проверить конфигурацию без БД, Telegram и AI-запросов",
    )
    run = commands.add_parser(
        "run",
        help="запустить collector, prefilter, AI и matching в одном процессе",
    )
    run.add_argument(
        "--catch-up-limit",
        type=_catch_up_limit,
        default=0,
    )
    run.add_argument("--source-limit", type=_source_limit)
    return parser


def _bounded_int(
    value: str,
    *,
    minimum: int,
    maximum: int,
    label: str,
) -> int:
    try:
        selected = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{label}: ожидается целое число") from None
    if not minimum <= selected <= maximum:
        raise argparse.ArgumentTypeError(
            f"{label}: значение должно быть от {minimum} до {maximum}"
        )
    return selected


def _catch_up_limit(value: str) -> int:
    return _bounded_int(
        value,
        minimum=0,
        maximum=500,
        label="catch-up-limit",
    )


def _source_limit(value: str) -> int:
    return _bounded_int(
        value,
        minimum=1,
        maximum=1_000,
        label="source-limit",
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        database = DatabaseSettings.from_env()
        telegram = TelegramAuthSettings.from_env()
        ai = OpportunityAISettings.from_env()
        if args.command == "check":
            status = inspect_runtime_configuration(database, telegram, ai)
            print(json.dumps(asdict(status), ensure_ascii=False, sort_keys=True))
            ready = (
                status.database_configured
                and status.database_driver_valid
                and status.telegram_configured
                and status.provider_configured
                and status.external_ai_data_processing_approved
            )
            return 0 if ready else 1
        if args.command == "run":
            asyncio.run(
                run_local_runtime(
                    database,
                    telegram,
                    ai,
                    source_discovery_settings=(
                        OperatorSourceDiscoverySettings.from_env()
                    ),
                    collector_options=CollectorOptions(
                        catch_up_limit=args.catch_up_limit,
                        source_limit=args.source_limit,
                    ),
                )
            )
            return 0
    except (ConfigurationError, TelegramCollectorError, ValueError) as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(
            "AI Sales Radar остановлен из-за ошибки. Подробности скрыты "
            f"для защиты секретов ({type(exc).__name__}).",
            file=sys.stderr,
        )
        return 1
    return 2


async def _consume_cancelled(task: asyncio.Task[object]) -> None:
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass


def _consume_task_result(task: asyncio.Task[object]) -> None:
    try:
        task.result()
    except (asyncio.CancelledError, Exception):
        pass


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "AISalesRadarRuntime",
    "RuntimeCheck",
    "build_parser",
    "inspect_runtime_configuration",
    "main",
    "run_local_runtime",
]

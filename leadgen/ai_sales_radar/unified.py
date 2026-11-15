"""Single owner bot and one ingestion pipeline, sharing existing durable storage."""
from __future__ import annotations

import argparse
import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import asdict
import json
import os
from pathlib import Path
import signal
import sys
import time

from freelancer_bot.telegram_session import TelegramSessionFileLock
from .config import (
    DatabaseSettings, TelegramAuthSettings, OpportunityAISettings,
    OperatorBotSettings, OperatorSourceDiscoverySettings, ReplyDraftSettings,
    LOCAL_RUNTIME_ROOT,
)
from .operator_bot.cli import inspect_operator_bot_configuration
from .operator_bot.telethon_runtime import run_operator_bot
from .readiness import (
    assess_readiness, inspect_configuration, inspect_readiness_from_settings,
)
from .runtime import inspect_runtime_configuration, run_local_runtime

Component = Callable[[asyncio.Event], Awaitable[None]]


async def supervise(
    components: dict[str, Component], *, stop_event: asyncio.Event,
    shutdown_timeout: float = 40.0,
) -> None:
    """Any component failure shuts down its peers; no silent partial service."""
    tasks = {asyncio.create_task(fn(stop_event), name=name): name
             for name, fn in components.items()}
    if not tasks:
        raise ValueError("At least one component is required")
    stopping = asyncio.create_task(stop_event.wait(), name="unified-stop")
    failed = False
    try:
        done, _ = await asyncio.wait(
            {*tasks, stopping}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in done:
            if task is stopping:
                continue
            await task
            if not stop_event.is_set():
                raise RuntimeError(f"Component stopped: {tasks[task]}")
    except BaseException:
        failed = True
        raise
    finally:
        stop_event.set()
        stopping.cancel()
        await asyncio.gather(stopping, return_exceptions=True)
        _, pending = await asyncio.wait(tasks, timeout=shutdown_timeout)
        for task in pending:
            task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        if not failed:
            if pending:
                raise TimeoutError("Component graceful shutdown timed out")
            for result in results:
                if isinstance(result, BaseException):
                    raise result


def configuration() -> dict:
    db = DatabaseSettings.from_env()
    ai = OpportunityAISettings.from_env()
    pipeline = inspect_runtime_configuration(db, TelegramAuthSettings.from_env(), ai)
    operator = inspect_operator_bot_configuration(db, OperatorBotSettings.from_env())
    return {
        "service": "ANODHAS Lead Parser",
        "ready": operator.ready and pipeline.provider_configured
                 and pipeline.telegram_configured
                 and pipeline.external_ai_data_processing_approved,
        "pipeline": asdict(pipeline),
        "operator": asdict(operator),
        "outreach_enabled": False,
        "source_mode": "registered_telegram_channels",
    }


async def preflight(max_backlog: int) -> dict:
    db = DatabaseSettings.from_env()
    ai = OpportunityAISettings.from_env()
    report = assess_readiness(
        inspect_configuration(db, ai),
        await inspect_readiness_from_settings(db),
        max_active_backlog=max_backlog,
    )
    result = configuration()
    result["database_readiness"] = asdict(report)
    result["ready"] = result["ready"] and report.ready
    return result


def write_status(path: Path, phase: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps({
        "pid": os.getpid(), "phase": phase, "updated_at": time.time(),
        "components": ["telegram_pipeline", "owner_bot"],
    }))
    temp.chmod(0o600)
    temp.replace(path)


async def serve(*, duration: float | None = None, max_backlog: int = 100) -> None:
    check = await preflight(max_backlog)
    if not check["ready"]:
        print(json.dumps(check, ensure_ascii=False), flush=True)
        raise RuntimeError("Unified parser preflight failed")
    db = DatabaseSettings.from_env()
    tg = TelegramAuthSettings.from_env()
    ai = OpportunityAISettings.from_env()
    bot = OperatorBotSettings.from_env()
    replies = ReplyDraftSettings.from_env()
    discovery = OperatorSourceDiscoverySettings.from_env()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
            installed.append(sig)
        except (NotImplementedError, RuntimeError):
            pass
    timer = loop.call_later(duration, stop.set) if duration is not None else None
    status_path = LOCAL_RUNTIME_ROOT / "unified-status.json"

    async def heartbeat():
        while not stop.is_set():
            write_status(status_path, "supervising")
            try:
                await asyncio.wait_for(stop.wait(), timeout=5)
            except TimeoutError:
                pass

    from .research.telegram import TelegramResearchReader
    research_reader = TelegramResearchReader()
    heart = asyncio.create_task(heartbeat())
    failed = False
    try:
        await supervise({
            "telegram_pipeline": lambda event: run_local_runtime(
                db, tg, ai, source_discovery_settings=discovery, stop_event=event, research_reader=research_reader),
            "owner_bot": lambda event: run_operator_bot(
                db, bot, replies, source_discovery_settings=discovery, stop_event=event, research_reader=research_reader),
        }, stop_event=stop)
    except BaseException:
        failed = True
        raise
    finally:
        stop.set()
        await heart
        write_status(status_path, "failed" if failed else "stopped")
        if timer is not None:
            timer.cancel()
        for sig in installed:
            loop.remove_signal_handler(sig)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Единый ANODHAS Lead Parser")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check", help="Настройки без сетевых запросов")
    ready = sub.add_parser("readiness", help="Настройки и чтение состояния PostgreSQL")
    ready.add_argument("--max-backlog", type=int, default=100)
    run = sub.add_parser("run", help="Один сборщик и один управляющий бот")
    run.add_argument("--duration", type=float, help="Ограничить запуск секундами")
    run.add_argument("--max-backlog", type=int, default=100)
    args = parser.parse_args(argv)
    try:
        if args.command == "check":
            result = configuration()
        elif args.command == "readiness":
            result = asyncio.run(preflight(args.max_backlog))
        else:
            if args.duration is not None and not 1 <= args.duration <= 86400:
                parser.error("duration must be 1..86400 seconds")
            if not 0 <= args.max_backlog <= 1000:
                parser.error("max-backlog must be 0..1000")
            lock = TelegramSessionFileLock(
                LOCAL_RUNTIME_ROOT / "unified-runtime", role="unified_lead_parser"
            )
            with lock:
                asyncio.run(serve(duration=args.duration, max_backlog=args.max_backlog))
            return 0
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result["ready"] else 1
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        # Exception strings can contain provider URLs or credentials.
        def origin(error: BaseException) -> str:
            frame = error.__traceback__
            locations = []
            while frame is not None:
                locations.append(
                    f"{Path(frame.tb_frame.f_code.co_filename).name}:{frame.tb_lineno}"
                )
                frame = frame.tb_next
            return " > ".join(locations[-12:]) or "unknown"

        chain = []
        current: BaseException | None = exc
        for _ in range(4):
            if current is None:
                break
            chain.append(f"{type(current).__name__}@{origin(current)}")
            current = current.__cause__ or current.__context__
        print(
            f"Parser stopped safely ({' | '.join(chain)})",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

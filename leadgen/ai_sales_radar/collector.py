"""Telegram user-session collector for the scripts-only AI Sales Radar runtime.

The collector deliberately stops at durable raw-message ingestion.  It never
constructs the Telegram Bot UI, sends messages, or invokes the legacy lead
pipeline.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import math
import json
import os
from pathlib import Path
import sys
import time
from typing import Any, Protocol
from uuid import UUID

from .codex_bridge import configured_bridge

from telethon import TelegramClient, events
from telethon.errors import (
    ChannelInvalidError,
    ChannelPrivateError,
    FloodWaitError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)

from freelancer_bot.observability import (
    Redactor,
    configure_structured_logger,
    log_event,
    new_correlation_id,
    trace_context,
)
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.raw_messages import (
    IneligibleRawMessageSource,
    RawMessageIngestor,
    RawMessageInput,
    RawMessageOrigin,
)
from freelancer_bot.persistence.telegram_operation_state import (
    TelegramCollectorFloodWaitActive,
    TelegramCollectorPaused,
)
from freelancer_bot.telegram_collector import (
    ApprovedTelegramSourceAdapter,
    TelegramCollectorSource,
    TelegramCollectorSourceSnapshot,
)
from freelancer_bot.telegram_request_governor import (
    TelegramRequestCategory,
    TelegramRequestGovernor,
)
from freelancer_bot.telegram_session import (
    TelegramSessionFileLock,
    TelegramSessionInUseError,
)

from .collector_folder_gate import (
    CollectorFolderGate,
    ExactCanaryTelegramFolderGate,
    TelegramCollectorFolderGateError,
)
from .config import (
    ConfigurationError,
    DatabaseSettings,
    LOCAL_RUNTIME_ROOT,
    TelegramAuthSettings,
    TelegramCredentials,
)
from .telegram_auth import (
    prepare_private_session_storage,
    private_umask,
    protect_session_files,
)
from .telegram_identity import (
    PostgresTelegramIdentityBindingLoader,
    StrictTelegramChannelIdentityVerifier,
    TelegramEntityIdentityVerifier,
    TelegramIdentityBindingLoader,
    is_trusted_binding,
)


LOGGER = logging.getLogger("ai_sales_radar")
MAX_CATCH_UP_PER_SOURCE = 500
MAX_SELECTED_SOURCES = 1_000
DEFAULT_LIVE_DRAIN_TIMEOUT_SECONDS = 10.0
DEFAULT_DISCONNECT_TIMEOUT_SECONDS = 10.0

SOURCE_SPECIFIC_TELEGRAM_ERRORS = (
    ChannelInvalidError,
    ChannelPrivateError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
    ValueError,
)

ACCOUNT_WIDE_TELEGRAM_ERRORS = (
    FloodWaitError,
    TelegramCollectorFloodWaitActive,
    TelegramCollectorPaused,
)


class TelegramCollectorError(RuntimeError):
    """A collector failure whose message is safe to show to an operator."""


class _SourceTelegramRequestError(RuntimeError):
    """Mark a known per-source failure raised inside a governed request."""

    def __init__(self, error: BaseException) -> None:
        self.error_type = type(error).__name__
        super().__init__(self.error_type)


class TelegramCollectorClient(Protocol):
    async def connect(self) -> Any: ...

    async def disconnect(self) -> Any: ...

    def is_connected(self) -> bool: ...

    async def is_user_authorized(self) -> bool: ...

    async def get_me(self) -> Any: ...

    async def get_entity(self, entity: Any) -> Any: ...

    def __call__(self, request: Any) -> Awaitable[Any]: ...

    def iter_messages(self, entity: Any, *, limit: int) -> AsyncIterator[Any]: ...

    def add_event_handler(self, callback: Any, event: Any = None) -> None: ...

    def remove_event_handler(self, callback: Any, event: Any = None) -> int: ...

    @property
    def disconnected(self) -> Awaitable[Any]: ...

    async def run_until_disconnected(self) -> Any: ...


class SourceAdapter(Protocol):
    async def list_for_session(
        self,
        client: TelegramCollectorClient,
    ) -> TelegramCollectorSourceSnapshot: ...


class MessageIngestor(Protocol):
    async def ingest(self, message: RawMessageInput) -> Any: ...


class RequestGovernor(Protocol):
    async def run(
        self,
        category: str,
        operation: Callable[[], Awaitable[Any]],
    ) -> Any: ...


class ConnectedCollectorService(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...


ClientFactory = Callable[[TelegramCredentials], TelegramCollectorClient]
LockFactory = Callable[..., TelegramSessionFileLock]
ConnectedServiceFactory = Callable[
    [TelegramCollectorClient, int, RequestGovernor],
    ConnectedCollectorService,
]


@dataclass(frozen=True)
class CollectorOptions:
    """Bounded collector behavior shared by one-shot and live modes."""

    catch_up_limit: int = 0
    source_limit: int | None = None
    telegram_crawl_min_delay_seconds: float = 5.0
    telegram_crawl_max_delay_seconds: float = 10.0
    telegram_source_cooldown_min_seconds: float = 15.0
    telegram_source_cooldown_max_seconds: float = 30.0
    telegram_governor_lease_seconds: float = 900.0

    def __post_init__(self) -> None:
        if isinstance(self.catch_up_limit, bool) or not (
            0 <= self.catch_up_limit <= MAX_CATCH_UP_PER_SOURCE
        ):
            raise ValueError(
                f"catch_up_limit must be between 0 and {MAX_CATCH_UP_PER_SOURCE}"
            )
        if self.source_limit is not None and (
            isinstance(self.source_limit, bool)
            or not 1 <= self.source_limit <= MAX_SELECTED_SOURCES
        ):
            raise ValueError(
                f"source_limit must be between 1 and {MAX_SELECTED_SOURCES}"
            )
        if (
            self.telegram_crawl_min_delay_seconds < 0
            or self.telegram_crawl_max_delay_seconds
            < self.telegram_crawl_min_delay_seconds
        ):
            raise ValueError("Telegram crawl delay range is invalid")
        if (
            self.telegram_source_cooldown_min_seconds < 0
            or self.telegram_source_cooldown_max_seconds
            < self.telegram_source_cooldown_min_seconds
        ):
            raise ValueError("Telegram source cooldown range is invalid")
        if self.telegram_governor_lease_seconds <= 0:
            raise ValueError("Telegram governor lease must be positive")


@dataclass(frozen=True)
class CollectorReport:
    collector_account_id: int
    catalog_source_count: int
    selected_source_count: int
    resolved_source_count: int
    source_resolution_failure_count: int
    created_message_count: int
    duplicate_message_count: int
    ingestion_failure_count: int


@dataclass
class _Counters:
    source_resolution_failures: int = 0
    created_messages: int = 0
    duplicate_messages: int = 0
    ingestion_failures: int = 0


class TelegramCollectorService:
    """Compose Telethon, the approved source catalog, and raw ingestion only."""

    def __init__(
        self,
        database: Database,
        credentials: TelegramCredentials,
        *,
        client_factory: ClientFactory | None = None,
        source_adapter: SourceAdapter | None = None,
        ingestor: MessageIngestor | None = None,
        lock_factory: LockFactory = TelegramSessionFileLock,
        governor_factory: Callable[..., RequestGovernor] | None = None,
        identity_binding_loader: TelegramIdentityBindingLoader | None = None,
        entity_identity_verifier: TelegramEntityIdentityVerifier | None = None,
        folder_gate: CollectorFolderGate | None = None,
        runtime_root: Path = LOCAL_RUNTIME_ROOT,
        clock: Callable[[], datetime] | None = None,
        correlation_id_factory: Callable[[], UUID] = new_correlation_id,
        live_drain_timeout_seconds: float = DEFAULT_LIVE_DRAIN_TIMEOUT_SECONDS,
        disconnect_timeout_seconds: float = DEFAULT_DISCONNECT_TIMEOUT_SECONDS,
        connected_service_factory: ConnectedServiceFactory | None = None,
        research_reader=None,
    ) -> None:
        if (
            not math.isfinite(live_drain_timeout_seconds)
            or live_drain_timeout_seconds <= 0
        ):
            raise ValueError("live_drain_timeout_seconds must be finite and positive")
        if (
            not math.isfinite(disconnect_timeout_seconds)
            or disconnect_timeout_seconds <= 0
        ):
            raise ValueError("disconnect_timeout_seconds must be finite and positive")
        self._database = database
        self._credentials = credentials
        self._client_factory = client_factory or _default_client_factory
        self._source_adapter = source_adapter or ApprovedTelegramSourceAdapter(database)
        self._ingestor = ingestor or RawMessageIngestor(database)
        self._lock_factory = lock_factory
        self._governor_factory = (
            governor_factory or _default_governor_factory
        )
        self._identity_binding_loader = (
            identity_binding_loader
            or PostgresTelegramIdentityBindingLoader(database)
        )
        self._entity_identity_verifier = (
            entity_identity_verifier
            or StrictTelegramChannelIdentityVerifier()
        )
        self._folder_gate = folder_gate or ExactCanaryTelegramFolderGate()
        self._runtime_root = runtime_root
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._correlation_id_factory = correlation_id_factory
        self._live_drain_timeout_seconds = live_drain_timeout_seconds
        self._disconnect_timeout_seconds = disconnect_timeout_seconds
        self._connected_service_factory = connected_service_factory
        self._research_reader = research_reader
        self._registered_handlers: list[tuple[Any, Any]] = []
        self._inflight_live_tasks: set[asyncio.Task[Any]] = set()
        self._stop_requested = asyncio.Event()
        self._accepting_live = False
        self._active = False
        self._counters = _Counters()
        self._current_collector_account_id: int | None = None

    async def check(self, *, options: CollectorOptions | None = None) -> CollectorReport:
        """Verify authorization, catalog binding, and source resolution."""
        selected = options or CollectorOptions()
        self._reset_counters()
        async with self._connected_client() as client:
            snapshot, sources, _ = await self._load_and_resolve(client, selected)
            return self._report(snapshot, selected, sources)

    async def collect_once(
        self,
        *,
        options: CollectorOptions | None = None,
    ) -> CollectorReport:
        """Read at most ``catch_up_limit`` historical messages per source."""
        selected = options or CollectorOptions()
        self._reset_counters()
        async with self._connected_client() as client:
            snapshot, sources, governor = await self._load_and_resolve(
                client,
                selected,
            )
            await self._catch_up(
                client,
                sources,
                governor=governor,
                limit=selected.catch_up_limit,
            )
            return self._report(snapshot, selected, sources)

    async def run(
        self,
        *,
        options: CollectorOptions | None = None,
    ) -> CollectorReport | None:
        """Collect live events until Telethon disconnects, with optional catch-up."""
        selected = options or CollectorOptions()
        self._reset_counters()
        disconnected_wait: asyncio.Future[Any] | None = None
        stop_wait: asyncio.Task[bool] | None = None
        live_heartbeat: asyncio.Task[None] | None = None
        live_marker = self._runtime_root / "collector-live.json"
        try:
            async with self._connected_client() as client:
                prepared = await self._prepare_live(
                    client,
                    selected,
                )
                if prepared is None:
                    return None
                snapshot, sources, governor = prepared
                connected_service = (
                    None
                    if self._connected_service_factory is None
                    else self._connected_service_factory(
                        client,
                        snapshot.collector_account.id,
                        governor,
                    )
                )
                codex_bridge = configured_bridge(client, governor)
                try:
                    if connected_service is not None:
                        await connected_service.start()
                    if self._research_reader is not None:
                        self._research_reader.bind(client, sources, governor)
                    if codex_bridge is not None:
                        await codex_bridge.start()
                    self._accepting_live = True
                    self._register_live_handlers(client, sources)
                    await self._catch_up(
                        client,
                        sources,
                        governor=governor,
                        limit=selected.catch_up_limit,
                    )
                    log_event(
                        LOGGER,
                        logging.INFO,
                        "collector.live_started",
                        collector_account_id=snapshot.collector_account.id,
                        source_count=len(sources),
                        catch_up_limit=selected.catch_up_limit,
                    )
                    live_heartbeat = asyncio.create_task(
                        self._mark_live_while_connected(client, live_marker),
                        name="ai-sales-radar-collector-health",
                    )
                    # Waiting on Telethon's disconnected future avoids the
                    # run_until_disconnected() helper, whose cancellation performs
                    # an immediate disconnect and cancels live handlers.
                    disconnected_wait = asyncio.ensure_future(
                        asyncio.shield(client.disconnected),
                    )
                    if isinstance(disconnected_wait, asyncio.Task):
                        disconnected_wait.set_name(
                            "ai-sales-radar-telethon-disconnected"
                        )
                    stop_wait = asyncio.create_task(
                        self._stop_requested.wait(),
                        name="ai-sales-radar-collector-stop",
                    )
                    done, _ = await asyncio.wait(
                        {disconnected_wait, stop_wait},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if disconnected_wait in done:
                        await disconnected_wait
                    return self._report(snapshot, selected, sources)
                finally:
                    self._accepting_live = False
                    if live_heartbeat is not None:
                        live_heartbeat.cancel()
                        await _consume_task(live_heartbeat)
                    live_marker.unlink(missing_ok=True)
                    if self._research_reader is not None:
                        self._research_reader.unbind()
                    if codex_bridge is not None:
                        await codex_bridge.close()
                    if connected_service is not None:
                        await connected_service.stop()
        except TelegramCollectorError as exc:
            frame = exc.__traceback__
            origin = "unknown"
            while frame is not None:
                origin = f"{Path(frame.tb_frame.f_code.co_filename).name}:{frame.tb_lineno}"
                frame = frame.tb_next
            print(f"Collector stopped safely ({origin})", file=sys.stderr)
            raise
        finally:
            if stop_wait is not None and not stop_wait.done():
                stop_wait.cancel()
            if stop_wait is not None:
                await _consume_task(stop_wait)
            if disconnected_wait is not None and not disconnected_wait.done():
                done, _ = await asyncio.wait(
                    {disconnected_wait},
                    timeout=self._disconnect_timeout_seconds,
                )
                if disconnected_wait not in done:
                    disconnected_wait.cancel()
                    done, _ = await asyncio.wait(
                        {disconnected_wait},
                        timeout=min(1.0, self._disconnect_timeout_seconds),
                    )
                    if disconnected_wait not in done:
                        disconnected_wait.add_done_callback(_consume_task_result)
            if disconnected_wait is not None and disconnected_wait.done():
                await _consume_task(disconnected_wait)
            self._accepting_live = False
            self._stop_requested.clear()

    async def _mark_live_while_connected(
        self, client: TelegramCollectorClient, marker: Path
    ) -> None:
        marker.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        while self._accepting_live and not self._stop_requested.is_set():
            is_connected = getattr(client, "is_connected", None)
            if callable(is_connected) and is_connected():
                temporary = marker.with_suffix(".tmp")
                temporary.write_text(json.dumps({
                    "pid": os.getpid(), "updated_at": time.time(), "phase": "live"
                }))
                temporary.chmod(0o600)
                temporary.replace(marker)
            else:
                marker.unlink(missing_ok=True)
            await asyncio.sleep(5)

    async def _prepare_live(self, client, options):
        """Stop a rate-limited startup without cancelling live ingestion."""
        preparing = asyncio.create_task(self._load_and_resolve(client, options))
        stopping = asyncio.create_task(self._stop_requested.wait())
        try:
            done, _ = await asyncio.wait(
                {preparing, stopping}, return_when=asyncio.FIRST_COMPLETED
            )
            if preparing in done:
                return await preparing
            return None
        finally:
            for task in (preparing, stopping):
                if not task.done():
                    task.cancel()
            await asyncio.gather(preparing, stopping, return_exceptions=True)

    async def stop(self) -> None:
        """Request cooperative live shutdown without cancelling ingestion."""
        self._stop_requested.set()

    @asynccontextmanager
    async def _connected_client(self):
        if self._active:
            raise TelegramCollectorError(
                "Collector уже запущен в этом процессе"
            )

        session_path = self._credentials.session_path
        prepare_private_session_storage(
            session_path,
            runtime_root=self._runtime_root,
        )
        # Refuse pre-existing SQLite companions or lock sidecars that were
        # replaced with symlinks before Telethon or the lock opens them.
        protect_session_files(session_path, runtime_root=self._runtime_root)
        lock = self._lock_factory(session_path, role="ai_sales_radar_collector")
        lock.acquire()
        self._active = True
        client: TelegramCollectorClient | None = None
        operation_error: BaseException | None = None
        try:
            # The client must not open its SQLite session before the process lock.
            with private_umask():
                client = self._client_factory(self._credentials)
            await client.connect()
            if not await client.is_user_authorized():
                raise TelegramCollectorError(
                    "Telegram-сессия не авторизована; "
                    "сначала выполните "
                    "telegram_auth.py login"
                )
            yield client
        except BaseException as exc:
            operation_error = exc
            raise
        finally:
            cleanup_errors = await self._cleanup(client, session_path, lock)
            self._active = False
            self._current_collector_account_id = None
            if cleanup_errors and operation_error is None:
                stage, error = cleanup_errors[0]
                raise TelegramCollectorError(
                    "Не удалось безопасно завершить Telegram collector "
                    f"на этапе {stage} ({type(error).__name__})"
                ) from None

    async def _cleanup(
        self,
        client: TelegramCollectorClient | None,
        session_path: Path,
        lock: TelegramSessionFileLock,
    ) -> list[tuple[str, BaseException]]:
        failures: list[tuple[str, BaseException]] = []
        self._accepting_live = False

        if client is not None:
            for callback, event_builder in reversed(self._registered_handlers):
                try:
                    client.remove_event_handler(callback, event_builder)
                except BaseException as exc:
                    failures.append(("remove_handler", exc))
            self._registered_handlers.clear()
            try:
                drain_failure = await self._drain_live_handlers()
                if drain_failure is not None:
                    failures.append(("drain_live", drain_failure))
            except BaseException as exc:
                failures.append(("drain_live", exc))
            try:
                disconnect_failure = await self._disconnect_client(client)
                if disconnect_failure is not None:
                    failures.append(("disconnect", disconnect_failure))
            except BaseException as exc:
                failures.append(("disconnect", exc))
        else:
            self._registered_handlers.clear()
            self._inflight_live_tasks.clear()

        try:
            protect_session_files(session_path, runtime_root=self._runtime_root)
        except BaseException as exc:
            failures.append(("protect_session", exc))
        try:
            lock.release()
        except BaseException as exc:
            failures.append(("release_lock", exc))

        for stage, error in failures:
            log_event(
                LOGGER,
                logging.ERROR,
                "collector.cleanup_failed",
                stage=stage,
                error_type=type(error).__name__,
            )
        return failures

    async def _drain_live_handlers(self) -> BaseException | None:
        current = asyncio.current_task()
        pending = {
            task
            for task in self._inflight_live_tasks
            if task is not current and not task.done()
        }
        if not pending:
            return None
        _, unfinished = await asyncio.wait(
            pending,
            timeout=self._live_drain_timeout_seconds,
        )
        if not unfinished:
            return None
        for task in unfinished:
            task.cancel()
        # Do not let a cancellation-resistant handler make shutdown unbounded.
        await asyncio.wait(
            unfinished,
            timeout=min(1.0, self._live_drain_timeout_seconds),
        )
        return TimeoutError("live ingestion drain timed out")

    async def _disconnect_client(
        self,
        client: TelegramCollectorClient,
    ) -> BaseException | None:
        # Telethon returns a shielded Future here rather than a bare coroutine;
        # ensure_future supports both that production API and async test fakes.
        task = asyncio.ensure_future(client.disconnect())
        if isinstance(task, asyncio.Task):
            task.set_name("ai-sales-radar-telethon-disconnect")
        done, _ = await asyncio.wait(
            {task},
            timeout=self._disconnect_timeout_seconds,
        )
        if task in done:
            try:
                await task
            except BaseException as exc:
                return exc
            return None

        task.cancel()
        done, _ = await asyncio.wait(
            {task},
            timeout=min(1.0, self._disconnect_timeout_seconds),
        )
        if task in done:
            await _consume_task(task)
        else:
            task.add_done_callback(_consume_task_result)
        return TimeoutError("Telegram disconnect timed out")

    async def _load_and_resolve(
        self,
        client: TelegramCollectorClient,
        options: CollectorOptions,
    ) -> tuple[
        TelegramCollectorSourceSnapshot,
        list[tuple[TelegramCollectorSource, Any]],
        RequestGovernor,
    ]:
        snapshot = await self._source_adapter.list_for_session(client)
        if not snapshot.collector_account.is_active:
            raise TelegramCollectorError(
                "Telegram collector account отключён "
                "в локальном каталоге"
            )
        self._current_collector_account_id = snapshot.collector_account.id
        try:
            self._folder_gate.validate_catalog(snapshot.sources)
        except TelegramCollectorFolderGateError as exc:
            raise TelegramCollectorError(str(exc)) from None
        selected_sources = snapshot.sources
        if options.source_limit is not None:
            selected_sources = selected_sources[: options.source_limit]
        try:
            self._folder_gate.validate_scope(selected_sources)
        except TelegramCollectorFolderGateError as exc:
            raise TelegramCollectorError(str(exc)) from None
        governor = self._governor_factory(
            self._database,
            snapshot.collector_account.id,
            options,
        )
        resolved = await self._resolve_sources(
            client,
            selected_sources,
            collector_account_id=snapshot.collector_account.id,
            governor=governor,
        )
        try:
            await self._folder_gate.verify(
                client,
                selected_sources=selected_sources,
                resolved_sources=resolved,
                governor=governor,
            )
        except TelegramCollectorFolderGateError as exc:
            raise TelegramCollectorError(str(exc)) from None
        return snapshot, resolved, governor

    async def _resolve_sources(
        self,
        client: TelegramCollectorClient,
        sources: tuple[TelegramCollectorSource, ...],
        *,
        collector_account_id: int,
        governor: RequestGovernor,
    ) -> list[tuple[TelegramCollectorSource, Any]]:
        source_ids = tuple(source.record.id for source in sources)
        bindings = await self._identity_binding_loader.load(
            source_ids=source_ids,
            collector_account_id=collector_account_id,
        )
        resolved: list[tuple[TelegramCollectorSource, Any]] = []
        for source in sources:
            binding = bindings.get(source.record.id)
            if not is_trusted_binding(
                binding,
                source_id=source.record.id,
                collector_account_id=collector_account_id,
            ):
                self._record_source_resolution_failure(
                    source,
                    failure_code="identity_binding_unavailable",
                )
                continue

            async def resolve_entity() -> Any:
                try:
                    return await client.get_entity(source.lookup)
                except asyncio.CancelledError:
                    raise
                except SOURCE_SPECIFIC_TELEGRAM_ERRORS as exc:
                    raise _SourceTelegramRequestError(exc) from exc

            try:
                entity = await governor.run(
                    TelegramRequestCategory.ENTITY_ACCESS,
                    resolve_entity,
                )
            except asyncio.CancelledError:
                raise
            except ACCOUNT_WIDE_TELEGRAM_ERRORS:
                raise
            except _SourceTelegramRequestError as exc:
                self._record_source_resolution_failure(
                    source,
                    failure_code="telegram_entity_unavailable",
                    error_type=exc.error_type,
                )
                continue
            verified_entity = self._entity_identity_verifier.verify(
                lookup=source.lookup,
                entity=entity,
                binding=binding,
            )
            if verified_entity is None:
                self._record_source_resolution_failure(
                    source,
                    failure_code="identity_mismatch",
                )
                continue

            # Pin history and event subscriptions to InputChannel(id, hash),
            # never to the mutable username.  PostgreSQL and Telegram cannot
            # share one transaction, so a validation-row revocation that races
            # this snapshot takes effect on the next collector operation; raw
            # ingestion still independently rechecks source eligibility.
            resolved.append((source, verified_entity))
        return resolved

    def _record_source_resolution_failure(
        self,
        source: TelegramCollectorSource,
        *,
        failure_code: str,
        error_type: str | None = None,
    ) -> None:
        self._counters.source_resolution_failures += 1
        fields: dict[str, object] = {
            "source_id": source.record.id,
            "failure_code": failure_code,
        }
        if error_type is not None:
            fields["error_type"] = error_type
        log_event(
            LOGGER,
            logging.WARNING,
            "collector.source_resolution_failed",
            **fields,
        )

    def _register_live_handlers(
        self,
        client: TelegramCollectorClient,
        sources: list[tuple[TelegramCollectorSource, Any]],
    ) -> None:
        for source, entity in sources:
            event_builder = events.NewMessage(chats=entity)

            async def on_message(
                event: Any,
                bound_source: TelegramCollectorSource = source,
            ) -> None:
                if not self._accepting_live:
                    return
                handler_task = asyncio.current_task()
                if handler_task is not None:
                    self._inflight_live_tasks.add(handler_task)
                message = getattr(event, "message", None)
                try:
                    if message is None:
                        log_event(
                            LOGGER,
                            logging.WARNING,
                            "collector.live_event_invalid",
                            source_id=bound_source.record.id,
                        )
                        return
                    await self._dispatch_safely(
                        bound_source,
                        message,
                        origin=RawMessageOrigin.LIVE,
                    )
                finally:
                    if handler_task is not None:
                        self._inflight_live_tasks.discard(handler_task)

            client.add_event_handler(on_message, event_builder)
            self._registered_handlers.append((on_message, event_builder))

    async def _catch_up(
        self,
        client: TelegramCollectorClient,
        sources: list[tuple[TelegramCollectorSource, Any]],
        *,
        governor: RequestGovernor,
        limit: int,
    ) -> None:
        if limit == 0:
            return
        buffered: list[tuple[datetime, int, int, TelegramCollectorSource, Any]] = []
        for source, entity in sources:
            if self._stop_requested.is_set():
                return
            try:
                async def read_history() -> list[Any]:
                    try:
                        return [
                            message
                            async for message in client.iter_messages(
                                entity,
                                limit=limit,
                            )
                        ]
                    except asyncio.CancelledError:
                        raise
                    except SOURCE_SPECIFIC_TELEGRAM_ERRORS as exc:
                        raise _SourceTelegramRequestError(exc) from exc

                messages = await governor.run(
                    TelegramRequestCategory.HISTORY,
                    read_history,
                )
                for message in messages:
                    buffered.append(
                        (
                            _message_date(message),
                            source.record.id,
                            _message_id(message),
                            source,
                            message,
                        )
                    )
            except asyncio.CancelledError:
                raise
            except ACCOUNT_WIDE_TELEGRAM_ERRORS:
                raise
            except _SourceTelegramRequestError as exc:
                log_event(
                    LOGGER,
                    logging.WARNING,
                    "collector.catch_up_source_failed",
                    source_id=source.record.id,
                    error_type=exc.error_type,
                )

        for _, _, _, source, message in sorted(buffered, key=lambda item: item[:3]):
            if self._stop_requested.is_set():
                return
            await self._dispatch_safely(
                source,
                message,
                origin=RawMessageOrigin.CATCH_UP,
            )

    async def _dispatch_safely(
        self,
        source: TelegramCollectorSource,
        message: Any,
        *,
        origin: RawMessageOrigin,
    ) -> None:
        try:
            await self._dispatch(source, message, origin=origin)
        except asyncio.CancelledError:
            raise
        except IneligibleRawMessageSource:
            self._counters.ingestion_failures += 1
            log_event(
                LOGGER,
                logging.WARNING,
                "collector.message_refused",
                source_id=source.record.id,
                telegram_message_id=_safe_message_id(message),
                reason_code="source_ineligible",
            )
        except Exception as exc:
            self._counters.ingestion_failures += 1
            # Never interpolate the exception: provider errors can echo content.
            log_event(
                LOGGER,
                logging.ERROR,
                "collector.message_ingestion_failed",
                source_id=source.record.id,
                telegram_message_id=_safe_message_id(message),
                origin=origin.value,
                error_type=type(exc).__name__,
            )

    async def _dispatch(
        self,
        source: TelegramCollectorSource,
        message: Any,
        *,
        origin: RawMessageOrigin,
    ) -> None:
        message_id = _message_id(message)
        correlation_id = self._correlation_id_factory()
        if not isinstance(correlation_id, UUID):
            raise TypeError("correlation_id_factory must return UUID")
        observed_at = self._clock()
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("collector clock must return an aware datetime")
        raw_content = getattr(message, "message", None)
        content = raw_content if isinstance(raw_content, str) else ""

        with trace_context(correlation_id):
            log_event(
                LOGGER,
                logging.INFO,
                "collector.message_dispatched",
                correlation_id=correlation_id,
                source_id=source.record.id,
                telegram_message_id=message_id,
                origin=origin.value,
            )
            outcome = await self._ingestor.ingest(
                RawMessageInput(
                    source_id=source.record.id,
                    collector_account_id=self._collector_account_id(source),
                    external_message_id=message_id,
                    message_date=_message_date(message),
                    observed_at=observed_at.astimezone(timezone.utc),
                    message_url=source.message_url(message_id),
                    content=content,
                    transport_metadata=_transport_metadata(message),
                    ingestion_origin=origin,
                    correlation_id=correlation_id,
                )
            )
            if bool(getattr(outcome, "created", False)):
                self._counters.created_messages += 1
            else:
                self._counters.duplicate_messages += 1

    def _collector_account_id(self, _source: TelegramCollectorSource) -> int:
        # The adapter binds every selected source to exactly one session.  The
        # current account is stored only for the duration of one operation.
        account_id = getattr(self, "_current_collector_account_id", None)
        if (
            isinstance(account_id, bool)
            or not isinstance(account_id, int)
            or account_id <= 0
        ):
            raise TelegramCollectorError("Collector account не инициализирован")
        return account_id

    def _report(
        self,
        snapshot: TelegramCollectorSourceSnapshot,
        options: CollectorOptions,
        sources: list[tuple[TelegramCollectorSource, Any]],
    ) -> CollectorReport:
        selected_count = min(
            len(snapshot.sources),
            (
                options.source_limit
                if options.source_limit is not None
                else len(snapshot.sources)
            ),
        )
        return CollectorReport(
            collector_account_id=snapshot.collector_account.id,
            catalog_source_count=len(snapshot.sources),
            selected_source_count=selected_count,
            resolved_source_count=len(sources),
            source_resolution_failure_count=self._counters.source_resolution_failures,
            created_message_count=self._counters.created_messages,
            duplicate_message_count=self._counters.duplicate_messages,
            ingestion_failure_count=self._counters.ingestion_failures,
        )

    def _reset_counters(self) -> None:
        self._counters = _Counters()
        self._current_collector_account_id = None


def _default_client_factory(credentials: TelegramCredentials) -> TelegramCollectorClient:
    return TelegramClient(
        str(credentials.session_path),
        credentials.api_id,
        credentials.api_hash,
        flood_sleep_threshold=0,
    )


def _default_governor_factory(
    database: Database,
    collector_account_id: int,
    options: CollectorOptions,
) -> RequestGovernor:
    return TelegramRequestGovernor(
        database,
        collector_account_id,
        options,  # CollectorOptions intentionally exposes the narrow config shape.
    )


async def _consume_task(task: asyncio.Future[Any]) -> None:
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass


def _consume_task_result(task: asyncio.Future[Any]) -> None:
    try:
        task.result()
    except (asyncio.CancelledError, Exception):
        pass


def _message_id(message: Any) -> int:
    value = getattr(message, "id", None)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("Telegram message has no positive identifier")
    return value


def _safe_message_id(message: Any) -> int | None:
    try:
        return _message_id(message)
    except (TypeError, ValueError):
        return None


def _message_date(message: Any) -> datetime:
    value = getattr(message, "date", None)
    if value is None:
        return datetime.now(timezone.utc)
    if not isinstance(value, datetime):
        raise TypeError("Telegram message date must be datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _transport_metadata(message: Any) -> dict[str, object]:
    metadata: dict[str, object] = {}
    for field in (
        "chat_id",
        "sender_id",
        "reply_to_msg_id",
        "grouped_id",
        "via_bot_id",
        "post",
        "mentioned",
        "out",
        "silent",
    ):
        value = getattr(message, field, None)
        if isinstance(value, (bool, int, str)):
            metadata[field] = value
    edit_date = getattr(message, "edit_date", None)
    if isinstance(edit_date, datetime):
        if edit_date.tzinfo is None or edit_date.utcoffset() is None:
            edit_date = edit_date.replace(tzinfo=timezone.utc)
        metadata["edit_date"] = edit_date.astimezone(timezone.utc).isoformat()
    media = getattr(message, "media", None)
    if media is not None:
        metadata["media_type"] = type(media).__name__
    action = getattr(message, "action", None)
    if action is not None:
        metadata["service_action_type"] = type(action).__name__
    return metadata


def _positive_bounded_int(value: str) -> int:
    try:
        selected = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("ожидается целое число") from None
    if not 1 <= selected <= MAX_SELECTED_SOURCES:
        raise argparse.ArgumentTypeError(
            f"значение должно быть от 1 до {MAX_SELECTED_SOURCES}"
        )
    return selected


def _catch_up_int(value: str) -> int:
    try:
        selected = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("ожидается целое число") from None
    if not 0 <= selected <= MAX_CATCH_UP_PER_SOURCE:
        raise argparse.ArgumentTypeError(
            f"значение должно быть от 0 до {MAX_CATCH_UP_PER_SOURCE}"
        )
    return selected


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Telegram collector AI Sales Radar (без Telegram Bot UI)",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)
    for name, help_text in (
        (
            "check",
            "проверить сессию, каталог "
            "и доступ к источникам",
        ),
        ("once", "однократно собрать ограниченную историю"),
        ("run", "собирать новые сообщения до остановки"),
    ):
        command = subcommands.add_parser(name, help=help_text)
        command.add_argument(
            "--source-limit",
            type=_positive_bounded_int,
            default=None,
            help=(
                "временно ограничить число источников "
                "(по умолчанию все)"
            ),
        )
        if name in {"once", "run"}:
            command.add_argument(
                "--catch-up-limit",
                type=_catch_up_int,
                default=0,
                help=(
                    "прочитать до N прошлых сообщений "
                    "на источник; "
                    "по умолчанию 0"
                ),
            )
    return parser


async def _run_command(args: argparse.Namespace) -> CollectorReport:
    database = Database(DatabaseSettings.from_env().require_url())
    try:
        credentials = TelegramAuthSettings.from_env().require_credentials()
        service = TelegramCollectorService(database, credentials)
        options = CollectorOptions(
            catch_up_limit=getattr(args, "catch_up_limit", 0),
            source_limit=args.source_limit,
        )
        if args.command == "check":
            return await service.check(options=options)
        if args.command == "once":
            return await service.collect_once(options=options)
        if args.command == "run":
            return await service.run(options=options)
        raise TelegramCollectorError("Неизвестная команда collector")
    finally:
        await database.close()


def _print_report(command: str, report: CollectorReport) -> None:
    if command == "check":
        print(
            "Collector готов: "
            f"аккаунт #{report.collector_account_id}, "
            f"доступно источников {report.resolved_source_count}/"
            f"{report.selected_source_count}."
        )
        return
    print(
        "Collector завершён: "
        f"новых {report.created_message_count}, "
        f"дубликатов {report.duplicate_message_count}, "
        f"ошибок ingestion {report.ingestion_failure_count}."
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_structured_logger("ai_sales_radar", redactor=Redactor())
    configure_structured_logger("freelancer_bot", redactor=Redactor())
    try:
        report = asyncio.run(_run_command(args))
        _print_report(args.command, report)
        return 0
    except (ConfigurationError, TelegramCollectorError) as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 2
    except TelegramSessionInUseError:
        print(
            "Ошибка: Telegram-сессия уже используется "
            "другим процессом.",
            file=sys.stderr,
        )
        return 3
    except KeyboardInterrupt:
        print("Collector остановлен.")
        return 130
    except Exception as exc:
        print(
            "Collector не выполнен. Подробности скрыты "
            "для защиты данных "
            f"({type(exc).__name__}).",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

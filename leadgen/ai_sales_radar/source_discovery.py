"""Owner-triggered, bounded Telegram source discovery.

The operator bot only enqueues a durable request.  The live collector process
executes global Telegram search with its already-authorized user session.  New
communities are materialized as candidates only; this module never joins,
approves, audits, reads arbitrary history, or sends messages.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import logging
import os
import socket
from uuid import UUID, uuid4

from freelancer_bot.observability import log_event
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.jobs import DurableJobRepository, JobClaim
from freelancer_bot.persistence.search_profiles import SearchProfileRepository
from freelancer_bot.profile_discovery import (
    ProfileDiscoveryService,
    ProfileDiscoveryIntent,
    build_profile_discovery_intent,
)
from freelancer_bot.telegram_profile_discovery import (
    QUERY_TIER_BROAD,
    TelegramProfileSearchQuery,
    build_telegram_profile_search_queries,
)
from freelancer_bot.worker import DurableWorker, WorkerOptions

from .config import OperatorSourceDiscoverySettings


OPERATOR_SOURCE_DISCOVERY_JOB_TYPE = "operator.source_discovery.v1"
OPERATOR_SOURCE_DISCOVERY_KEY_PREFIX = "operator-source-discovery"
OPERATOR_SOURCE_DISCOVERY_PROVIDER = "telegram_global_profile"


class OperatorSourceDiscoveryBlocked(RuntimeError):
    retryable = False


def source_discovery_job_key(
    profile_id: UUID,
    profile_revision: int,
    *,
    requested_at: datetime,
    cooldown_seconds: int,
) -> str:
    if profile_revision < 1:
        raise ValueError("profile_revision must be positive")
    if requested_at.tzinfo is None or requested_at.utcoffset() is None:
        raise ValueError("requested_at must include a timezone")
    if cooldown_seconds < 1:
        raise ValueError("cooldown_seconds must be positive")
    bucket = int(requested_at.astimezone(timezone.utc).timestamp()) // cooldown_seconds
    return (
        f"{OPERATOR_SOURCE_DISCOVERY_KEY_PREFIX}:"
        f"{profile_id}:r{profile_revision}:b{bucket}"
    )


def parse_source_discovery_job_key(value: str) -> tuple[UUID, int, int]:
    prefix = f"{OPERATOR_SOURCE_DISCOVERY_KEY_PREFIX}:"
    if not value.startswith(prefix):
        raise ValueError("invalid operator source discovery job key")
    profile_text, revision_and_bucket = value.removeprefix(prefix).split(":r", 1)
    revision_text, bucket_text = revision_and_bucket.split(":b", 1)
    profile_id = UUID(profile_text)
    revision = int(revision_text)
    bucket = int(bucket_text)
    if revision < 1 or bucket < 0:
        raise ValueError("invalid operator source discovery job key")
    canonical = (
        f"{OPERATOR_SOURCE_DISCOVERY_KEY_PREFIX}:"
        f"{profile_id}:r{revision}:b{bucket}"
    )
    if value != canonical:
        raise ValueError("operator source discovery job key is not canonical")
    return profile_id, revision, bucket


async def enqueue_source_discovery(
    database: Database,
    *,
    profile_id: UUID,
    profile_revision: int,
    requested_at: datetime,
    settings: OperatorSourceDiscoverySettings,
    jobs: DurableJobRepository | None = None,
) -> UUID:
    if not settings.enabled:
        raise OperatorSourceDiscoveryBlocked("Поиск новых источников выключен.")
    repository = jobs or DurableJobRepository()
    key = source_discovery_job_key(
        profile_id,
        profile_revision,
        requested_at=requested_at,
        cooldown_seconds=settings.cooldown_seconds,
    )
    async with database.transaction() as connection:
        return await repository.enqueue(
            connection,
            job_type=OPERATOR_SOURCE_DISCOVERY_JOB_TYPE,
            idempotency_key=key,
            max_attempts=2,
        )


def build_operator_source_queries(
    intent: ProfileDiscoveryIntent,
    *,
    max_queries: int,
) -> tuple[TelegramProfileSearchQuery, ...]:
    """Mix direct buyer demand with buyer/seller marketplace signals."""

    if not 4 <= max_queries <= 20:
        raise ValueError("max_queries must be between 4 and 20")
    base_limit = max(2, max_queries // 2)
    values = list(
        build_telegram_profile_search_queries(intent, max_queries=base_limit)
    )
    seen = {item.text.casefold() for item in values}
    terms = _intent_terms(intent)
    templates = (
        ("ru", "seller_need", "ищу заказчика {term}"),
        ("ru", "marketplace", "{term} фриланс заказы"),
        ("ru", "buyer_need", "ищу исполнителя {term}"),
        ("en", "seller_need", "{term} looking for clients"),
        ("en", "marketplace", "{term} freelance jobs"),
        ("en", "buyer_need", "looking for {term} specialist"),
    )
    for term in terms:
        for language, query_kind, template in templates:
            text = " ".join(template.format(term=term).split())
            identity = text.casefold()
            if identity in seen:
                continue
            seen.add(identity)
            values.append(
                TelegramProfileSearchQuery(
                    text=text,
                    family="OPERATOR_MARKETPLACE",
                    language=language,
                    angle="buyer_habitat",
                    query_kind=query_kind,
                    tier=QUERY_TIER_BROAD,
                )
            )
            if len(values) >= max_queries:
                return tuple(values)
    return tuple(values)


class OperatorSourceDiscoveryJobProcessor:
    def __init__(
        self,
        database: Database,
        *,
        client: object,
        governor: object,
        settings: OperatorSourceDiscoverySettings,
        logger: logging.Logger,
        profiles: SearchProfileRepository | None = None,
        discovery: ProfileDiscoveryService | None = None,
    ) -> None:
        self._database = database
        self._client = client
        self._governor = governor
        self._settings = settings
        self._logger = logger
        self._profiles = profiles or SearchProfileRepository()
        self._discovery = discovery or ProfileDiscoveryService(database)

    async def __call__(self, claim: JobClaim) -> None:
        if claim.job_type != OPERATOR_SOURCE_DISCOVERY_JOB_TYPE:
            raise ValueError("unsupported operator source discovery job type")
        if not self._settings.enabled:
            raise OperatorSourceDiscoveryBlocked("Manual source discovery is disabled")
        profile_id, revision, _bucket = parse_source_discovery_job_key(
            claim.idempotency_key
        )
        async with self._database.connect() as connection:
            profile = await self._profiles.get(connection, profile_id)
        if (
            not profile.is_active
            or not profile.is_primary
            or profile.revision != revision
        ):
            raise OperatorSourceDiscoveryBlocked(
                "Search profile is no longer the active primary revision"
            )
        intent = build_profile_discovery_intent(profile)
        queries = build_operator_source_queries(
            intent,
            max_queries=self._settings.max_queries,
        )
        execution = await self._discovery.discover_telegram_profile(
            profile,
            requested_at=datetime.now(timezone.utc),
            run_key=claim.idempotency_key,
            client=self._client,
            governor=self._governor,
            queries=queries,
            results_per_query=self._settings.results_per_query,
            max_candidates=self._settings.max_candidates,
            minimum_participants=self._settings.minimum_participants,
            participant_lookup_limit=self._settings.participant_lookup_limit,
            public_only=True,
        )
        log_event(
            self._logger,
            logging.INFO,
            "operator_source_discovery.completed",
            profile_id=profile_id,
            profile_revision=revision,
            query_count=len(queries),
            candidate_count=execution.unique_candidates,
            new_candidate_count=execution.new_candidates,
            minimum_participants=self._settings.minimum_participants,
            automatic_approval_enabled=False,
            automatic_join_enabled=False,
        )


class OperatorSourceDiscoveryWorkerService:
    def __init__(
        self,
        database: Database,
        *,
        client: object,
        governor: object,
        settings: OperatorSourceDiscoverySettings,
        logger: logging.Logger,
        worker: DurableWorker | None = None,
    ) -> None:
        processor = OperatorSourceDiscoveryJobProcessor(
            database,
            client=client,
            governor=governor,
            settings=settings,
            logger=logger,
        )
        self._worker = worker or DurableWorker(
            database,
            repository=DurableJobRepository(),
            worker_id=_worker_id(),
            handlers={OPERATOR_SOURCE_DISCOVERY_JOB_TYPE: processor},
            logger=logger,
            options=WorkerOptions(),
            close_database_on_exit=False,
        )
        self._task: asyncio.Task[None] | None = None
        self._started = False

    async def start(self) -> None:
        if self._started:
            raise RuntimeError("Source discovery worker cannot be started twice")
        self._started = True
        self._task = asyncio.create_task(
            self._worker.run(install_signal_handlers=False),
            name="ai-sales-radar-source-discovery-worker",
        )
        await asyncio.sleep(0)
        if self._task.done():
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


def _intent_terms(intent: ProfileDiscoveryIntent) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for value in (*intent.services, *intent.skills, *intent.roles):
        selected = " ".join(str(value).split()).strip(" ,;")
        identity = selected.casefold()
        if not selected or identity in seen:
            continue
        seen.add(identity)
        result.append(selected)
        if len(result) >= 4:
            break
    return tuple(result)


def _worker_id() -> str:
    return (
        f"ai-sales-radar-source-discovery-{socket.gethostname()}-"
        f"{os.getpid()}-{uuid4().hex[:8]}"
    )


__all__ = [
    "OPERATOR_SOURCE_DISCOVERY_JOB_TYPE",
    "OPERATOR_SOURCE_DISCOVERY_KEY_PREFIX",
    "OPERATOR_SOURCE_DISCOVERY_PROVIDER",
    "OperatorSourceDiscoveryBlocked",
    "OperatorSourceDiscoveryJobProcessor",
    "OperatorSourceDiscoveryWorkerService",
    "build_operator_source_queries",
    "enqueue_source_discovery",
    "parse_source_discovery_job_key",
    "source_discovery_job_key",
]

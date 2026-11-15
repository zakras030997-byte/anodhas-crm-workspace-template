"""Narrow recovery for the first five incompatible Opportunity jobs.

The command in this module is intentionally not a general-purpose job retry
tool.  It repairs only the bounded canary failure caused by prefilter rows that
were persisted with the legacy base analyzer version before the provider route
fingerprint was wired into the scripts-only pipeline.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from hashlib import sha256
import json
import re
from secrets import compare_digest
import sys
from typing import Any, Protocol
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection

from freelancer_bot.message_prefilter import (
    OPPORTUNITY_ANALYSIS_JOB_TYPE,
    PREFILTER_SCHEMA_VERSION,
)
from freelancer_bot.opportunity_analysis import OPPORTUNITY_ANALYZER_VERSION
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.raw_messages import RAW_MESSAGE_JOB_TYPE
from freelancer_bot.persistence.schema import (
    ai_call_telemetry,
    durable_jobs,
    message_prefilter_results,
    opportunity_analysis_cache,
    opportunity_source_messages,
    raw_messages,
)

from .config import ConfigurationError, DatabaseSettings, OpportunityAISettings


EXPECTED_REPAIR_JOB_COUNT = 5
EXPECTED_REPAIR_MAX_ATTEMPTS = 3
REPAIR_CANARY_SOURCE_IDS = (3, 6, 7, 12, 13)
INCOMPATIBLE_FAILURE_CODE = "IncompatibleOpportunityAnalysisJob"
REPAIR_MANIFEST_VERSION = "analysis-version-repair.v1"
_SAFE_VERSION = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_SAFE_MANIFEST = re.compile(
    rf"^{re.escape(REPAIR_MANIFEST_VERSION)}:[0-9a-f]{{64}}$"
)


class AnalysisRepairConflict(RuntimeError):
    """The locked database state changed or is outside the repair contract."""


@dataclass(frozen=True)
class AnalysisRepairTarget:
    provider: str
    model: str
    analyzer_version: str
    analysis_schema_version: str

    def __post_init__(self) -> None:
        if not self.provider.strip() or not self.model.strip():
            raise ValueError("repair target provider and model must not be blank")
        for value, label in (
            (self.analyzer_version, "analyzer_version"),
            (self.analysis_schema_version, "analysis_schema_version"),
        ):
            if not _SAFE_VERSION.fullmatch(value):
                raise ValueError(f"repair target {label} is invalid")
        if self.analyzer_version == OPPORTUNITY_ANALYZER_VERSION:
            raise ValueError("repair target must use a route-specific analyzer version")


@dataclass(frozen=True)
class RepairPrefilterRow:
    id: UUID
    raw_message_id: UUID
    analysis_job_id: UUID
    canonical_prefilter_result_id: UUID | None
    schema_version: str
    decision: str
    analyzer_version: str | None
    analysis_schema_version: str | None
    dedup_relation: str | None
    normalized_content: str | None
    normalized_content_sha256: str | None
    analysis_input_sha256: str | None
    source_id: int
    ingestion_origin: str
    raw_processing_job_id: UUID
    raw_processing_job_type: str | None
    raw_processing_job_state: str | None
    raw_correlation_id: UUID
    raw_processing_job_correlation_id: UUID | None


@dataclass(frozen=True)
class RepairJobInspection:
    job_id: UUID
    job_type: str | None
    idempotency_key: str | None
    state: str | None
    attempt_count: int | None
    max_attempts: int | None
    failure_code: str | None
    correlation_id: UUID | None = None
    claimed_at: Any | None = None
    lease_owner: str | None = None
    lease_expires_at: Any | None = None
    completed_at: Any | None = None
    failed_at: Any | None = None
    prefilters: tuple[RepairPrefilterRow, ...] = ()
    ai_call_count: int = 0
    analysis_cache_count: int = 0
    opportunity_source_count: int = 0
    target_canonical_conflict_count: int = 0


@dataclass(frozen=True)
class RepairCanaryBinding:
    raw_message_id: UUID
    source_id: int
    ingestion_origin: str
    processing_job_id: UUID
    prefilter_result_count: int
    analysis_job_ids: tuple[UUID, ...]


@dataclass(frozen=True)
class RepairInspectionBatch:
    inspections: tuple[RepairJobInspection, ...]
    total_raw_message_count: int
    canary_bindings: tuple[RepairCanaryBinding, ...]


@dataclass(frozen=True)
class RepairBlock:
    job_id: str
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class AnalysisRepairReport:
    mode: str
    provider: str
    model: str
    target_analyzer_version: str
    target_analysis_schema_version: str
    requested_job_ids: tuple[str, ...]
    repair_manifest: str | None
    eligible_job_ids: tuple[str, ...]
    already_reconciled_job_ids: tuple[str, ...]
    blocked_jobs: tuple[RepairBlock, ...]
    global_blockers: tuple[str, ...]
    ready: bool
    applied: bool = False
    updated_prefilter_rows: int = 0
    requeued_jobs: int = 0


@dataclass(frozen=True)
class RepairWriteCounts:
    updated_prefilter_rows: int
    requeued_jobs: int


class RepairRepository(Protocol):
    async def inspect(
        self,
        connection: AsyncConnection,
        *,
        job_ids: tuple[UUID, ...],
        target: AnalysisRepairTarget,
        lock: bool,
    ) -> RepairInspectionBatch: ...

    async def apply_repair(
        self,
        connection: AsyncConnection,
        *,
        job_ids: tuple[UUID, ...],
        prefilter_ids: tuple[UUID, ...],
        raw_message_ids: tuple[UUID, ...],
        target: AnalysisRepairTarget,
    ) -> RepairWriteCounts: ...


class PostgreSQLAnalysisRepairRepository:
    """PostgreSQL adapter with exact predicates and no delete operations."""

    async def inspect(
        self,
        connection: AsyncConnection,
        *,
        job_ids: tuple[UUID, ...],
        target: AnalysisRepairTarget,
        lock: bool,
    ) -> RepairInspectionBatch:
        raw_processing_job = durable_jobs.alias("repair_raw_processing_job")
        jobs_query = (
            sa.select(
                durable_jobs.c.id,
                durable_jobs.c.job_type,
                durable_jobs.c.idempotency_key,
                durable_jobs.c.state,
                durable_jobs.c.attempt_count,
                durable_jobs.c.max_attempts,
                durable_jobs.c.failure_code,
                durable_jobs.c.correlation_id,
                durable_jobs.c.claimed_at,
                durable_jobs.c.lease_owner,
                durable_jobs.c.lease_expires_at,
                durable_jobs.c.completed_at,
                durable_jobs.c.failed_at,
            )
            .where(durable_jobs.c.id.in_(job_ids))
            .order_by(durable_jobs.c.id)
        )
        prefilters_query = (
            sa.select(
                message_prefilter_results.c.id,
                message_prefilter_results.c.raw_message_id,
                message_prefilter_results.c.analysis_job_id,
                message_prefilter_results.c.canonical_prefilter_result_id,
                message_prefilter_results.c.schema_version,
                message_prefilter_results.c.decision,
                message_prefilter_results.c.analyzer_version,
                message_prefilter_results.c.analysis_schema_version,
                message_prefilter_results.c.dedup_relation,
                message_prefilter_results.c.normalized_content,
                message_prefilter_results.c.normalized_content_sha256,
                message_prefilter_results.c.analysis_input_sha256,
                raw_messages.c.source_id,
                raw_messages.c.ingestion_origin,
                raw_messages.c.processing_job_id.label("raw_processing_job_id"),
                raw_messages.c.correlation_id.label("raw_correlation_id"),
                raw_processing_job.c.job_type.label("raw_processing_job_type"),
                raw_processing_job.c.state.label("raw_processing_job_state"),
                raw_processing_job.c.correlation_id.label(
                    "raw_processing_job_correlation_id"
                ),
            )
            .select_from(
                message_prefilter_results.join(
                    raw_messages,
                    raw_messages.c.id
                    == message_prefilter_results.c.raw_message_id,
                ).join(
                    raw_processing_job,
                    raw_processing_job.c.id == raw_messages.c.processing_job_id,
                )
            )
            .where(message_prefilter_results.c.analysis_job_id.in_(job_ids))
            .order_by(
                message_prefilter_results.c.analysis_job_id,
                message_prefilter_results.c.id,
            )
        )
        if lock:
            jobs_query = jobs_query.with_for_update()
            prefilters_query = prefilters_query.with_for_update()

        job_rows = (await connection.execute(jobs_query)).mappings().all()
        prefilter_rows = (
            await connection.execute(prefilters_query)
        ).mappings().all()

        scope_raw_query = (
            sa.select(
                raw_messages.c.id,
                raw_messages.c.source_id,
                raw_messages.c.ingestion_origin,
                raw_messages.c.processing_job_id,
            )
            .where(raw_messages.c.source_id.in_(REPAIR_CANARY_SOURCE_IDS))
            .order_by(raw_messages.c.source_id, raw_messages.c.id)
        )
        if lock:
            scope_raw_query = scope_raw_query.with_for_update()
        scope_raw_rows = (
            await connection.execute(scope_raw_query)
        ).mappings().all()
        scope_raw_ids = tuple(row["id"] for row in scope_raw_rows)
        scope_prefilter_query = (
            sa.select(
                message_prefilter_results.c.raw_message_id,
                message_prefilter_results.c.analysis_job_id,
            )
            .where(message_prefilter_results.c.raw_message_id.in_(scope_raw_ids))
            .order_by(
                message_prefilter_results.c.raw_message_id,
                message_prefilter_results.c.id,
            )
        )
        if lock:
            scope_prefilter_query = scope_prefilter_query.with_for_update()
        scope_prefilter_rows = (
            await connection.execute(scope_prefilter_query)
        ).mappings().all()
        total_raw_message_count = int(
            await connection.scalar(
                sa.select(sa.func.count()).select_from(raw_messages)
            )
            or 0
        )

        prefilter_alias = message_prefilter_results.alias("repair_prefilter")
        cache_counts = await _counts_by_job(
            connection,
            sa.select(
                prefilter_alias.c.analysis_job_id.label("analysis_job_id"),
                sa.func.count(sa.distinct(opportunity_analysis_cache.c.id)).label(
                    "count"
                ),
            )
            .select_from(
                prefilter_alias.join(
                    opportunity_analysis_cache,
                    sa.and_(
                        opportunity_analysis_cache.c.normalized_content_sha256
                        == prefilter_alias.c.normalized_content_sha256,
                        opportunity_analysis_cache.c.analysis_input_sha256
                        == prefilter_alias.c.analysis_input_sha256,
                        opportunity_analysis_cache.c.analysis_schema_version
                        == prefilter_alias.c.analysis_schema_version,
                    ),
                )
            )
            .where(prefilter_alias.c.analysis_job_id.in_(job_ids))
            .group_by(prefilter_alias.c.analysis_job_id),
        )
        telemetry_counts = await _counts_by_job(
            connection,
            sa.select(
                prefilter_alias.c.analysis_job_id.label("analysis_job_id"),
                sa.func.count(sa.distinct(ai_call_telemetry.c.id)).label("count"),
            )
            .select_from(
                prefilter_alias.join(
                    ai_call_telemetry,
                    ai_call_telemetry.c.raw_message_id
                    == prefilter_alias.c.raw_message_id,
                )
            )
            .where(prefilter_alias.c.analysis_job_id.in_(job_ids))
            .group_by(prefilter_alias.c.analysis_job_id),
        )
        opportunity_counts = await _counts_by_job(
            connection,
            sa.select(
                prefilter_alias.c.analysis_job_id.label("analysis_job_id"),
                sa.func.count(
                    sa.distinct(opportunity_source_messages.c.raw_message_id)
                ).label("count"),
            )
            .select_from(
                prefilter_alias.join(
                    opportunity_source_messages,
                    opportunity_source_messages.c.raw_message_id
                    == prefilter_alias.c.raw_message_id,
                )
            )
            .where(prefilter_alias.c.analysis_job_id.in_(job_ids))
            .group_by(prefilter_alias.c.analysis_job_id),
        )

        current = message_prefilter_results.alias("repair_current")
        other = message_prefilter_results.alias("repair_target_other")
        target_conflicts = await _counts_by_job(
            connection,
            sa.select(
                current.c.analysis_job_id.label("analysis_job_id"),
                sa.func.count(sa.distinct(other.c.id)).label("count"),
            )
            .select_from(
                current.join(
                    other,
                    sa.and_(
                        other.c.id != current.c.id,
                        other.c.decision == "passed",
                        other.c.analyzer_version == target.analyzer_version,
                        other.c.analysis_schema_version
                        == target.analysis_schema_version,
                        other.c.normalized_content
                        == current.c.normalized_content,
                        other.c.normalized_content_sha256
                        == current.c.normalized_content_sha256,
                        other.c.analysis_input_sha256
                        == current.c.analysis_input_sha256,
                    ),
                )
            )
            .where(
                current.c.analysis_job_id.in_(job_ids),
                current.c.dedup_relation == "canonical",
            )
            .group_by(current.c.analysis_job_id),
        )

        jobs = {row["id"]: row for row in job_rows}
        prefilters: dict[UUID, list[RepairPrefilterRow]] = defaultdict(list)
        for row in prefilter_rows:
            job_id = row["analysis_job_id"]
            prefilters[job_id].append(_prefilter_record(row))

        scope_job_ids: dict[UUID, list[UUID]] = defaultdict(list)
        scope_prefilter_counts: dict[UUID, int] = defaultdict(int)
        for row in scope_prefilter_rows:
            scope_prefilter_counts[row["raw_message_id"]] += 1
            analysis_job_id = row["analysis_job_id"]
            if analysis_job_id is not None:
                scope_job_ids[row["raw_message_id"]].append(analysis_job_id)

        return RepairInspectionBatch(
            inspections=tuple(
                _inspection_record(
                    job_id,
                    jobs.get(job_id),
                    tuple(prefilters[job_id]),
                    ai_call_count=telemetry_counts.get(job_id, 0),
                    analysis_cache_count=cache_counts.get(job_id, 0),
                    opportunity_source_count=opportunity_counts.get(job_id, 0),
                    target_canonical_conflict_count=target_conflicts.get(job_id, 0),
                )
                for job_id in job_ids
            ),
            total_raw_message_count=total_raw_message_count,
            canary_bindings=tuple(
                RepairCanaryBinding(
                    raw_message_id=row["id"],
                    source_id=int(row["source_id"]),
                    ingestion_origin=str(row["ingestion_origin"]),
                    processing_job_id=row["processing_job_id"],
                    prefilter_result_count=scope_prefilter_counts[row["id"]],
                    analysis_job_ids=tuple(
                        sorted(scope_job_ids[row["id"]], key=str)
                    ),
                )
                for row in scope_raw_rows
            ),
        )

    async def apply_repair(
        self,
        connection: AsyncConnection,
        *,
        job_ids: tuple[UUID, ...],
        prefilter_ids: tuple[UUID, ...],
        raw_message_ids: tuple[UUID, ...],
        target: AnalysisRepairTarget,
    ) -> RepairWriteCounts:
        prefilter_result = await connection.execute(
            sa.update(message_prefilter_results)
            .where(
                message_prefilter_results.c.id.in_(prefilter_ids),
                message_prefilter_results.c.analysis_job_id.in_(job_ids),
                message_prefilter_results.c.raw_message_id.in_(raw_message_ids),
                message_prefilter_results.c.schema_version
                == PREFILTER_SCHEMA_VERSION,
                message_prefilter_results.c.decision == "passed",
                message_prefilter_results.c.dedup_relation == "canonical",
                message_prefilter_results.c.canonical_prefilter_result_id.is_(None),
                message_prefilter_results.c.analyzer_version
                == OPPORTUNITY_ANALYZER_VERSION,
                message_prefilter_results.c.analysis_schema_version
                == target.analysis_schema_version,
            )
            .values(analyzer_version=target.analyzer_version)
        )
        if prefilter_result.rowcount != EXPECTED_REPAIR_JOB_COUNT:
            raise AnalysisRepairConflict(
                "prefilter repair row count changed while the transaction was locked"
            )

        job_result = await connection.execute(
            sa.update(durable_jobs)
            .where(
                durable_jobs.c.id.in_(job_ids),
                durable_jobs.c.job_type == OPPORTUNITY_ANALYSIS_JOB_TYPE,
                durable_jobs.c.state == "queued",
                durable_jobs.c.attempt_count == 1,
                durable_jobs.c.max_attempts == EXPECTED_REPAIR_MAX_ATTEMPTS,
                durable_jobs.c.failure_code == INCOMPATIBLE_FAILURE_CODE,
                durable_jobs.c.claimed_at.is_(None),
                durable_jobs.c.lease_owner.is_(None),
                durable_jobs.c.lease_expires_at.is_(None),
                durable_jobs.c.completed_at.is_(None),
                durable_jobs.c.failed_at.is_(None),
            )
            .values(
                state="queued",
                attempt_count=0,
                available_at=sa.func.now(),
                claimed_at=None,
                lease_owner=None,
                lease_expires_at=None,
                failure_code=None,
                completed_at=None,
                failed_at=None,
                updated_at=sa.func.now(),
            )
        )
        if job_result.rowcount != len(job_ids):
            raise AnalysisRepairConflict(
                "durable job repair row count changed while the transaction was locked"
            )
        return RepairWriteCounts(
            updated_prefilter_rows=prefilter_result.rowcount,
            requeued_jobs=job_result.rowcount,
        )


class AnalysisRepairService:
    def __init__(
        self,
        database: Database,
        *,
        repository: RepairRepository | None = None,
    ) -> None:
        self._database = database
        self._repository = repository or PostgreSQLAnalysisRepairRepository()

    async def dry_run(
        self,
        job_ids: Sequence[UUID],
        target: AnalysisRepairTarget,
    ) -> AnalysisRepairReport:
        selected = normalize_job_ids(job_ids)
        async with self._database.connect() as connection:
            await connection.execute(
                sa.text(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
                )
            )
            batch = await self._repository.inspect(
                connection,
                job_ids=selected,
                target=target,
                lock=False,
            )
            _require_exact_inspection_set(batch.inspections, selected)
        return build_repair_report(
            batch,
            target=target,
            mode="dry-run",
        )

    async def apply(
        self,
        job_ids: Sequence[UUID],
        target: AnalysisRepairTarget,
        *,
        confirmation_manifest: str,
    ) -> AnalysisRepairReport:
        selected = normalize_job_ids(job_ids)
        confirmation = normalize_repair_manifest(confirmation_manifest)
        async with self._database.transaction() as connection:
            await connection.execute(
                sa.text("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            )
            await connection.execute(
                sa.text(
                    "LOCK TABLE raw_messages, message_prefilter_results, "
                    "durable_jobs, ai_call_telemetry, opportunity_analysis_cache, "
                    "opportunity_source_messages IN SHARE ROW EXCLUSIVE MODE"
                )
            )
            batch = await self._repository.inspect(
                connection,
                job_ids=selected,
                target=target,
                lock=True,
            )
            _require_exact_inspection_set(batch.inspections, selected)
            report = build_repair_report(
                batch,
                target=target,
                mode="apply",
            )
            if not report.ready:
                return report
            if report.repair_manifest is None or not compare_digest(
                report.repair_manifest,
                confirmation,
            ):
                return replace(
                    report,
                    global_blockers=(*report.global_blockers, "manifest_mismatch"),
                    ready=False,
                )

            prefilters = tuple(
                inspection.prefilters[0] for inspection in batch.inspections
            )
            counts = await self._repository.apply_repair(
                connection,
                job_ids=selected,
                prefilter_ids=tuple(row.id for row in prefilters),
                raw_message_ids=tuple(row.raw_message_id for row in prefilters),
                target=target,
            )
            verified = await self._repository.inspect(
                connection,
                job_ids=selected,
                target=target,
                lock=True,
            )
            _require_exact_inspection_set(verified.inspections, selected)
            verification = build_repair_report(
                verified,
                target=target,
                mode="apply",
                allow_post_repair=True,
            )
            if (
                not verification.ready
                or len(verification.already_reconciled_job_ids)
                != EXPECTED_REPAIR_JOB_COUNT
                or verification.repair_manifest is None
                or not compare_digest(
                    verification.repair_manifest,
                    confirmation,
                )
            ):
                raise AnalysisRepairConflict(
                    "post-write repair verification did not match the exact canary set"
                )
            return replace(
                verification,
                applied=True,
                updated_prefilter_rows=counts.updated_prefilter_rows,
                requeued_jobs=counts.requeued_jobs,
            )


def build_repair_report(
    batch: RepairInspectionBatch,
    *,
    target: AnalysisRepairTarget,
    mode: str,
    allow_post_repair: bool = False,
) -> AnalysisRepairReport:
    if mode not in {"dry-run", "apply"}:
        raise ValueError("repair report mode must be dry-run or apply")
    inspections = batch.inspections
    if len(inspections) != EXPECTED_REPAIR_JOB_COUNT:
        raise ValueError("repair inspection must contain exactly five jobs")
    requested = normalize_job_ids(item.job_id for item in inspections)
    eligible: list[str] = []
    already: list[str] = []
    blocked: list[RepairBlock] = []
    for inspection in inspections:
        status, reasons = _classify_inspection(
            inspection,
            target,
            allow_post_repair=allow_post_repair,
        )
        job_id = str(inspection.job_id)
        if status == "eligible":
            eligible.append(job_id)
        elif status == "post_repaired":
            already.append(job_id)
        else:
            blocked.append(RepairBlock(job_id=job_id, reasons=reasons))

    scope_blockers = _scope_blockers(batch, requested)
    global_blockers = list(scope_blockers)
    if not blocked and eligible and already:
        global_blockers.append("mixed_repair_state")
    ready = not blocked and not global_blockers and (
        len(eligible) == EXPECTED_REPAIR_JOB_COUNT
        or (
            allow_post_repair
            and len(already) == EXPECTED_REPAIR_JOB_COUNT
        )
    )
    repair_manifest = (
        _repair_manifest(inspections, target)
        if not scope_blockers
        else None
    )
    return AnalysisRepairReport(
        mode=mode,
        provider=target.provider,
        model=target.model,
        target_analyzer_version=target.analyzer_version,
        target_analysis_schema_version=target.analysis_schema_version,
        requested_job_ids=tuple(str(value) for value in requested),
        repair_manifest=repair_manifest,
        eligible_job_ids=tuple(sorted(eligible)),
        already_reconciled_job_ids=tuple(sorted(already)),
        blocked_jobs=tuple(sorted(blocked, key=lambda item: item.job_id)),
        global_blockers=tuple(global_blockers),
        ready=ready,
    )


def normalize_job_ids(values: Iterable[UUID]) -> tuple[UUID, ...]:
    selected = tuple(values)
    if len(selected) != EXPECTED_REPAIR_JOB_COUNT:
        raise ValueError("repair requires exactly five job IDs")
    if any(not isinstance(value, UUID) for value in selected):
        raise TypeError("repair job IDs must be UUID values")
    if len(set(selected)) != EXPECTED_REPAIR_JOB_COUNT:
        raise ValueError("repair job IDs must be five distinct UUID values")
    return tuple(sorted(selected, key=str))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="AI Sales Radar: узкий repair пяти incompatible AI jobs",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    for name, help_text in (
        (
            "dry-run",
            "проверить ровно пять jobs без изменений",
        ),
        (
            "apply",
            "атомарно исправить и повторно поставить ровно пять jobs",
        ),
    ):
        command = commands.add_parser(name, help=help_text)
        command.add_argument(
            "--job-id",
            action="append",
            type=UUID,
            required=True,
            help=(
                "UUID Opportunity Analysis job; повторить ровно пять раз"
            ),
        )
        if name == "apply":
            command.add_argument(
                "--confirm-manifest",
                required=True,
                help="repair_manifest из успешного dry-run",
            )
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
        job_ids = normalize_job_ids(args.job_id)
        target = repair_target_from_env()
        database_url = DatabaseSettings.from_env().require_url()
        report = asyncio.run(
            _execute(
                command=args.command,
                database_url=database_url,
                job_ids=job_ids,
                target=target,
                confirmation_manifest=(
                    args.confirm_manifest if args.command == "apply" else None
                ),
            )
        )
        print(json.dumps(asdict(report), ensure_ascii=False, indent=2, sort_keys=True))
        return 0 if report.ready else 1
    except (AnalysisRepairConflict, ConfigurationError, TypeError, ValueError) as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(
            "Repair не выполнен. Подробности скрыты для защиты "
            "секретов "
            f"({type(exc).__name__}).",
            file=sys.stderr,
        )
        return 1


def repair_target_from_env(
    *,
    settings_factory: Callable[[], OpportunityAISettings] = (
        OpportunityAISettings.from_env
    ),
) -> AnalysisRepairTarget:
    settings = settings_factory()
    settings.require_external_ai_data_processing_approval()
    settings.require_api_key()
    # Lazy import lets the compatibility wiring land independently while
    # keeping this recovery command coupled to the same public helper.
    from .ai_worker import opportunity_analyzer_compatibility

    compatibility = opportunity_analyzer_compatibility(settings)
    return AnalysisRepairTarget(
        provider=settings.provider,
        model=settings.model,
        analyzer_version=compatibility.analyzer_version,
        analysis_schema_version=compatibility.analysis_schema_version,
    )


async def _execute(
    *,
    command: str,
    database_url: str,
    job_ids: tuple[UUID, ...],
    target: AnalysisRepairTarget,
    confirmation_manifest: str | None = None,
    database_factory: Callable[..., Database] = Database,
) -> AnalysisRepairReport:
    database = database_factory(database_url, pool_size=1, max_overflow=0)
    try:
        service = AnalysisRepairService(database)
        if command == "dry-run":
            return await service.dry_run(job_ids, target)
        if command == "apply":
            if confirmation_manifest is None:
                raise ValueError("apply requires the dry-run repair manifest")
            return await service.apply(
                job_ids,
                target,
                confirmation_manifest=confirmation_manifest,
            )
        raise ValueError("unsupported repair command")
    finally:
        await database.close()


def _classify_inspection(
    inspection: RepairJobInspection,
    target: AnalysisRepairTarget,
    *,
    allow_post_repair: bool,
) -> tuple[str, tuple[str, ...]]:
    reasons: list[str] = []
    if inspection.job_type is None:
        return "blocked", ("job_not_found",)
    if inspection.job_type != OPPORTUNITY_ANALYSIS_JOB_TYPE:
        reasons.append("job_type_mismatch")

    rows = inspection.prefilters
    if len(rows) != 1:
        reasons.append("prefilter_row_count_invalid")
    else:
        row = rows[0]
        if (
            row.schema_version != PREFILTER_SCHEMA_VERSION
            or row.analysis_job_id != inspection.job_id
            or row.decision != "passed"
            or row.dedup_relation != "canonical"
            or row.canonical_prefilter_result_id is not None
            or row.analysis_schema_version != target.analysis_schema_version
            or not row.normalized_content
            or not _is_sha256(row.normalized_content_sha256)
            or not _is_sha256(row.analysis_input_sha256)
        ):
            reasons.append("prefilter_contract_mismatch")
        if inspection.idempotency_key != (
            f"{PREFILTER_SCHEMA_VERSION}:{row.raw_message_id}"
        ):
            reasons.append("job_idempotency_key_mismatch")
        if row.source_id not in REPAIR_CANARY_SOURCE_IDS:
            reasons.append("source_not_in_canary")
        if row.ingestion_origin != "catch_up":
            reasons.append("ingestion_origin_mismatch")
        if row.raw_processing_job_type != RAW_MESSAGE_JOB_TYPE:
            reasons.append("raw_processing_job_type_mismatch")
        if row.raw_processing_job_state != "completed":
            reasons.append("raw_processing_job_state_mismatch")
        if (
            inspection.correlation_id != row.raw_correlation_id
            or row.raw_processing_job_correlation_id != row.raw_correlation_id
        ):
            reasons.append("correlation_chain_mismatch")

    if inspection.ai_call_count:
        reasons.append("ai_telemetry_present")
    if inspection.analysis_cache_count:
        reasons.append("analysis_cache_present")
    if inspection.opportunity_source_count:
        reasons.append("opportunity_present")
    if inspection.target_canonical_conflict_count:
        reasons.append("target_canonical_conflict")
    if reasons:
        return "blocked", tuple(dict.fromkeys(reasons))

    row = rows[0]
    if row.analyzer_version == OPPORTUNITY_ANALYZER_VERSION:
        old_state_reasons: list[str] = []
        if inspection.state != "queued":
            old_state_reasons.append("old_job_state_mismatch")
        if inspection.attempt_count != 1:
            old_state_reasons.append("old_attempt_count_mismatch")
        if inspection.max_attempts != EXPECTED_REPAIR_MAX_ATTEMPTS:
            old_state_reasons.append("max_attempts_mismatch")
        if inspection.failure_code != INCOMPATIBLE_FAILURE_CODE:
            old_state_reasons.append("failure_code_mismatch")
        if not _inactive_job_fields_clear(inspection):
            old_state_reasons.append("old_job_lease_or_terminal_fields_present")
        if old_state_reasons:
            return "blocked", tuple(old_state_reasons)
        return "eligible", ()
    if row.analyzer_version == target.analyzer_version:
        if not allow_post_repair:
            return "blocked", ("already_applied",)
        if not (
            inspection.state == "queued"
            and inspection.attempt_count == 0
            and inspection.max_attempts == EXPECTED_REPAIR_MAX_ATTEMPTS
            and inspection.failure_code is None
            and _inactive_job_fields_clear(inspection)
        ):
            return "blocked", ("post_repair_state_invalid",)
        return "post_repaired", ()
    return "blocked", ("analyzer_version_mismatch",)


def normalize_repair_manifest(value: str) -> str:
    if not isinstance(value, str) or not _SAFE_MANIFEST.fullmatch(value):
        raise ValueError("confirm-manifest is not a valid repair manifest")
    return value


def _scope_blockers(
    batch: RepairInspectionBatch,
    requested: tuple[UUID, ...],
) -> tuple[str, ...]:
    reasons: list[str] = []
    bindings = batch.canary_bindings
    if batch.total_raw_message_count != EXPECTED_REPAIR_JOB_COUNT:
        reasons.append("total_raw_message_count_not_exact_five")
    if len(bindings) != EXPECTED_REPAIR_JOB_COUNT:
        reasons.append("canary_raw_message_count_not_exact_five")
    if tuple(sorted(binding.source_id for binding in bindings)) != tuple(
        sorted(REPAIR_CANARY_SOURCE_IDS)
    ):
        reasons.append("canary_source_set_mismatch")
    if any(binding.ingestion_origin != "catch_up" for binding in bindings):
        reasons.append("canary_ingestion_origin_mismatch")
    if len({binding.raw_message_id for binding in bindings}) != len(bindings):
        reasons.append("canary_raw_message_ids_not_distinct")
    if len({binding.processing_job_id for binding in bindings}) != len(bindings):
        reasons.append("canary_processing_job_ids_not_distinct")
    if any(len(binding.analysis_job_ids) != 1 for binding in bindings):
        reasons.append("canary_prefilter_binding_count_invalid")
    if any(binding.prefilter_result_count != 1 for binding in bindings):
        reasons.append("canary_prefilter_row_count_invalid")
    scoped_jobs = tuple(
        job_id
        for binding in bindings
        for job_id in binding.analysis_job_ids
    )
    if len(scoped_jobs) != EXPECTED_REPAIR_JOB_COUNT or set(scoped_jobs) != set(
        requested
    ):
        reasons.append("canary_analysis_job_manifest_mismatch")

    rows = tuple(row for item in batch.inspections for row in item.prefilters)
    if len(rows) != EXPECTED_REPAIR_JOB_COUNT:
        reasons.append("selected_prefilter_row_count_not_exact_five")
    if {row.raw_message_id for row in rows} != {
        binding.raw_message_id for binding in bindings
    }:
        reasons.append("selected_raw_message_manifest_mismatch")
    if {row.raw_processing_job_id for row in rows} != {
        binding.processing_job_id for binding in bindings
    }:
        reasons.append("selected_processing_job_manifest_mismatch")
    return tuple(dict.fromkeys(reasons))


def _repair_manifest(
    inspections: Sequence[RepairJobInspection],
    target: AnalysisRepairTarget,
) -> str:
    bindings = []
    for inspection in sorted(inspections, key=lambda item: str(item.job_id)):
        if len(inspection.prefilters) != 1:
            raise ValueError("repair manifest requires one prefilter row per job")
        row = inspection.prefilters[0]
        bindings.append(
            {
                "analysis_job_id": str(inspection.job_id),
                "prefilter_result_id": str(row.id),
                "raw_message_id": str(row.raw_message_id),
                "raw_processing_job_id": str(row.raw_processing_job_id),
                "source_id": row.source_id,
            }
        )
    payload = {
        "version": REPAIR_MANIFEST_VERSION,
        "provider": target.provider,
        "model": target.model,
        "analyzer_version": target.analyzer_version,
        "analysis_schema_version": target.analysis_schema_version,
        "bindings": bindings,
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"{REPAIR_MANIFEST_VERSION}:{sha256(encoded).hexdigest()}"


def _inactive_job_fields_clear(inspection: RepairJobInspection) -> bool:
    return all(
        value is None
        for value in (
            inspection.claimed_at,
            inspection.lease_owner,
            inspection.lease_expires_at,
            inspection.completed_at,
            inspection.failed_at,
        )
    )


def _is_sha256(value: str | None) -> bool:
    return bool(value and re.fullmatch(r"[0-9a-f]{64}", value))


def _require_exact_inspection_set(
    inspections: Sequence[RepairJobInspection],
    expected: tuple[UUID, ...],
) -> None:
    try:
        actual = normalize_job_ids(item.job_id for item in inspections)
    except (TypeError, ValueError):
        raise AnalysisRepairConflict(
            "repair inspection did not return the exact requested canary set"
        ) from None
    if actual != expected:
        raise AnalysisRepairConflict(
            "repair inspection did not return the exact requested canary set"
        )


async def _counts_by_job(
    connection: AsyncConnection,
    statement: sa.Select[Any],
) -> dict[UUID, int]:
    rows = (await connection.execute(statement)).mappings().all()
    return {row["analysis_job_id"]: int(row["count"]) for row in rows}


def _prefilter_record(row: Mapping[str, Any]) -> RepairPrefilterRow:
    return RepairPrefilterRow(
        id=row["id"],
        raw_message_id=row["raw_message_id"],
        analysis_job_id=row["analysis_job_id"],
        canonical_prefilter_result_id=row["canonical_prefilter_result_id"],
        schema_version=str(row["schema_version"]),
        decision=str(row["decision"]),
        analyzer_version=row["analyzer_version"],
        analysis_schema_version=row["analysis_schema_version"],
        dedup_relation=row["dedup_relation"],
        normalized_content=row["normalized_content"],
        normalized_content_sha256=row["normalized_content_sha256"],
        analysis_input_sha256=row["analysis_input_sha256"],
        source_id=int(row["source_id"]),
        ingestion_origin=str(row["ingestion_origin"]),
        raw_processing_job_id=row["raw_processing_job_id"],
        raw_processing_job_type=row["raw_processing_job_type"],
        raw_processing_job_state=row["raw_processing_job_state"],
        raw_correlation_id=row["raw_correlation_id"],
        raw_processing_job_correlation_id=row[
            "raw_processing_job_correlation_id"
        ],
    )


def _inspection_record(
    job_id: UUID,
    row: Mapping[str, Any] | None,
    prefilters: tuple[RepairPrefilterRow, ...],
    **hazards: int,
) -> RepairJobInspection:
    if row is None:
        return RepairJobInspection(
            job_id=job_id,
            job_type=None,
            idempotency_key=None,
            state=None,
            attempt_count=None,
            max_attempts=None,
            failure_code=None,
            **hazards,
        )
    return RepairJobInspection(
        job_id=job_id,
        job_type=str(row["job_type"]),
        idempotency_key=str(row["idempotency_key"]),
        state=str(row["state"]),
        attempt_count=int(row["attempt_count"]),
        max_attempts=int(row["max_attempts"]),
        failure_code=row["failure_code"],
        correlation_id=row["correlation_id"],
        claimed_at=row["claimed_at"],
        lease_owner=row["lease_owner"],
        lease_expires_at=row["lease_expires_at"],
        completed_at=row["completed_at"],
        failed_at=row["failed_at"],
        prefilters=prefilters,
        **hazards,
    )


__all__ = [
    "AnalysisRepairConflict",
    "AnalysisRepairReport",
    "AnalysisRepairService",
    "AnalysisRepairTarget",
    "EXPECTED_REPAIR_JOB_COUNT",
    "EXPECTED_REPAIR_MAX_ATTEMPTS",
    "REPAIR_CANARY_SOURCE_IDS",
    "INCOMPATIBLE_FAILURE_CODE",
    "REPAIR_MANIFEST_VERSION",
    "PostgreSQLAnalysisRepairRepository",
    "RepairBlock",
    "RepairCanaryBinding",
    "RepairInspectionBatch",
    "RepairJobInspection",
    "RepairPrefilterRow",
    "RepairWriteCounts",
    "build_parser",
    "build_repair_report",
    "main",
    "normalize_job_ids",
    "normalize_repair_manifest",
    "repair_target_from_env",
]

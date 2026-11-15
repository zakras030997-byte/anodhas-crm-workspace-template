"""Read-only readiness gate for the scripts-only AI pipeline.

The command reports aggregates only.  It never opens Telegram, starts a worker,
reads message content, or calls an AI provider.  PostgreSQL inspection is forced
into a read-only transaction before any schema or aggregate query is issued.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
import json
import sys

from alembic.runtime.migration import MigrationContext
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection

from freelancer_bot.matching_jobs import MATCHING_INBOX_JOB_TYPE
from freelancer_bot.message_prefilter import OPPORTUNITY_ANALYSIS_JOB_TYPE
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.raw_messages import RAW_MESSAGE_JOB_TYPE
from freelancer_bot.persistence.schema import (
    ai_call_telemetry,
    durable_jobs,
    opportunities,
    raw_messages,
    search_profiles,
)

from .bootstrap import expected_migration_heads
from .config import (
    ConfigurationError,
    DatabaseSettings,
    OpportunityAISettings,
)
from .persistence.schema import lead_inbox_items


PIPELINE_JOB_TYPES = (
    RAW_MESSAGE_JOB_TYPE,
    OPPORTUNITY_ANALYSIS_JOB_TYPE,
    MATCHING_INBOX_JOB_TYPE,
)
JOB_STATES = ("queued", "running", "completed", "failed")
ACTIVE_JOB_STATES = frozenset({"queued", "running"})
MAX_CANARY_BACKLOG_LIMIT = 1_000
MONEY_QUANTUM = Decimal("0.000000001")


@dataclass(frozen=True)
class ConfigurationReadiness:
    database_configured: bool
    database_driver_valid: bool
    provider_configured: bool
    external_ai_data_processing_approved: bool = False
    provider: str = "openai"


@dataclass(frozen=True)
class JobStateCounts:
    queued: int = 0
    running: int = 0
    completed: int = 0
    failed: int = 0


@dataclass(frozen=True)
class PipelineJobCounts:
    active_backlog_count: int
    by_type: Mapping[str, JobStateCounts]


@dataclass(frozen=True)
class RecordCounts:
    raw_messages: int
    opportunities: int
    inbox_items: int


@dataclass(frozen=True)
class AISpendCounts:
    window_timezone: str
    today_call_count: int
    today_estimated_usd: str
    month_call_count: int
    month_estimated_usd: str


@dataclass(frozen=True)
class ProfileCounts:
    active_confirmed: int
    active_primary_confirmed: int


@dataclass(frozen=True)
class DatabaseReadiness:
    reachable: bool
    migration_current: bool
    aggregates_readable: bool
    current_heads: tuple[str, ...]
    expected_heads: tuple[str, ...]
    jobs: PipelineJobCounts | None = None
    records: RecordCounts | None = None
    ai_spend: AISpendCounts | None = None
    profiles: ProfileCounts | None = None


@dataclass(frozen=True)
class RuntimeReadinessReport:
    ready: bool
    max_active_backlog: int
    blockers: tuple[str, ...]
    configuration: ConfigurationReadiness
    database: DatabaseReadiness


def inspect_configuration(
    database: DatabaseSettings,
    ai: OpportunityAISettings,
) -> ConfigurationReadiness:
    database_driver_valid = False
    if database.configured():
        try:
            database.require_url()
        except ConfigurationError:
            pass
        else:
            database_driver_valid = True
    return ConfigurationReadiness(
        database_configured=database.configured(),
        database_driver_valid=database_driver_valid,
        provider_configured=ai.configured(),
        external_ai_data_processing_approved=(
            ai.external_ai_data_processing_approved
        ),
        provider=ai.provider,
    )


async def inspect_readiness_database(
    database: Database,
    *,
    expected_heads: Sequence[str] | None = None,
    now: datetime | None = None,
) -> DatabaseReadiness:
    """Inspect one database connection without permitting persistent writes."""
    expected = tuple(
        sorted(expected_migration_heads() if expected_heads is None else expected_heads)
    )
    selected_now = now or datetime.now(timezone.utc)
    if selected_now.tzinfo is None or selected_now.utcoffset() is None:
        raise ValueError("readiness clock must return an aware datetime")
    selected_now = selected_now.astimezone(timezone.utc)
    day_start = selected_now.replace(hour=0, minute=0, second=0, microsecond=0)
    month_start = day_start.replace(day=1)

    reachable = False
    migration_current = False
    current: tuple[str, ...] = ()
    try:
        async with database.connect() as connection:
            # This must be the first statement in the implicit transaction.
            await connection.execute(sa.text("SET TRANSACTION READ ONLY"))
            await connection.execute(sa.text("SELECT 1"))
            reachable = True
            current = await connection.run_sync(
                lambda sync_connection: tuple(
                    sorted(
                        MigrationContext.configure(
                            sync_connection
                        ).get_current_heads()
                    )
                )
            )
            migration_current = current == expected
            if not migration_current:
                return DatabaseReadiness(
                    reachable=True,
                    migration_current=False,
                    aggregates_readable=False,
                    current_heads=current,
                    expected_heads=expected,
                )

            jobs = await _job_counts(connection)
            records = await _record_counts(connection)
            spend = await _spend_counts(
                connection,
                day_start=day_start,
                month_start=month_start,
            )
            profiles = await _profile_counts(connection)
            return DatabaseReadiness(
                reachable=True,
                migration_current=True,
                aggregates_readable=True,
                current_heads=current,
                expected_heads=expected,
                jobs=jobs,
                records=records,
                ai_spend=spend,
                profiles=profiles,
            )
    except asyncio.CancelledError:
        raise
    except Exception:
        # Provider/driver errors can contain credentials.  The report exposes
        # only the safe readiness state and never interpolates the exception.
        return DatabaseReadiness(
            reachable=reachable,
            migration_current=migration_current,
            aggregates_readable=False,
            current_heads=current,
            expected_heads=expected,
        )


async def inspect_readiness_from_settings(
    settings: DatabaseSettings,
    *,
    expected_heads: Sequence[str] | None = None,
    now: datetime | None = None,
    database_factory: Callable[..., Database] = Database,
) -> DatabaseReadiness:
    expected = tuple(
        sorted(expected_migration_heads() if expected_heads is None else expected_heads)
    )
    try:
        database_url = settings.require_url()
    except ConfigurationError:
        return DatabaseReadiness(
            reachable=False,
            migration_current=False,
            aggregates_readable=False,
            current_heads=(),
            expected_heads=expected,
        )

    database: Database | None = None
    try:
        database = database_factory(database_url, pool_size=1, max_overflow=0)
        return await inspect_readiness_database(
            database,
            expected_heads=expected,
            now=now,
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        return DatabaseReadiness(
            reachable=False,
            migration_current=False,
            aggregates_readable=False,
            current_heads=(),
            expected_heads=expected,
        )
    finally:
        if database is not None:
            try:
                await database.close()
            except Exception:
                pass


def assess_readiness(
    configuration: ConfigurationReadiness,
    database: DatabaseReadiness,
    *,
    max_active_backlog: int,
) -> RuntimeReadinessReport:
    _validate_backlog_limit(max_active_backlog)
    blockers: list[str] = []
    if not configuration.database_configured:
        blockers.append("database_not_configured")
    elif not configuration.database_driver_valid:
        blockers.append("database_driver_invalid")
    if not configuration.provider_configured:
        blockers.append("ai_provider_key_missing")
    if not configuration.external_ai_data_processing_approved:
        blockers.append("external_ai_data_processing_not_approved")

    if not database.reachable:
        blockers.append("database_unreachable")
    elif not database.migration_current:
        blockers.append("migration_not_current")
    elif not database.aggregates_readable:
        blockers.append("readiness_aggregates_unavailable")
    else:
        if database.profiles is None or database.jobs is None:
            blockers.append("readiness_aggregates_unavailable")
        else:
            if (
                database.profiles.active_confirmed != 1
                or database.profiles.active_primary_confirmed != 1
            ):
                blockers.append("active_primary_profile_count_invalid")
            if database.jobs.active_backlog_count > max_active_backlog:
                blockers.append("active_backlog_exceeds_canary_limit")

    return RuntimeReadinessReport(
        ready=not blockers,
        max_active_backlog=max_active_backlog,
        blockers=tuple(blockers),
        configuration=configuration,
        database=database,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only readiness AI Sales Radar без Telegram, workers и AI-вызовов"
        ),
    )
    parser.add_argument(
        "--max-active-backlog",
        required=True,
        type=_backlog_limit,
        help=(
            "явный максимум queued+running jobs трёх pipeline-стадий; "
            f"от 0 до {MAX_CANARY_BACKLOG_LIMIT}"
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        database_settings = DatabaseSettings.from_env()
        ai_settings = OpportunityAISettings.from_env()
        configuration = inspect_configuration(database_settings, ai_settings)
        database = asyncio.run(
            inspect_readiness_from_settings(database_settings)
        )
        report = assess_readiness(
            configuration,
            database,
            max_active_backlog=args.max_active_backlog,
        )
    except ConfigurationError:
        payload = {
            "ready": False,
            "max_active_backlog": args.max_active_backlog,
            "blockers": ["configuration_invalid"],
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 1
    except KeyboardInterrupt:
        return 130
    except Exception:
        payload = {
            "ready": False,
            "max_active_backlog": args.max_active_backlog,
            "blockers": ["readiness_check_failed"],
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 1

    print(
        json.dumps(
            asdict(report),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report.ready else 1


async def _job_counts(connection: AsyncConnection) -> PipelineJobCounts:
    rows = (
        await connection.execute(
            sa.select(
                durable_jobs.c.job_type,
                durable_jobs.c.state,
                sa.func.count().label("count"),
            )
            .where(durable_jobs.c.job_type.in_(PIPELINE_JOB_TYPES))
            .group_by(durable_jobs.c.job_type, durable_jobs.c.state)
        )
    ).mappings().all()
    mutable = {
        job_type: {state: 0 for state in JOB_STATES}
        for job_type in PIPELINE_JOB_TYPES
    }
    for row in rows:
        job_type = str(row["job_type"])
        state = str(row["state"])
        if job_type in mutable and state in mutable[job_type]:
            mutable[job_type][state] = int(row["count"])
    by_type = {
        job_type: JobStateCounts(**mutable[job_type])
        for job_type in PIPELINE_JOB_TYPES
    }
    active = sum(
        getattr(counts, state)
        for counts in by_type.values()
        for state in ACTIVE_JOB_STATES
    )
    return PipelineJobCounts(active_backlog_count=active, by_type=by_type)


async def _record_counts(connection: AsyncConnection) -> RecordCounts:
    row = (
        await connection.execute(
            sa.select(
                sa.select(sa.func.count())
                .select_from(raw_messages)
                .scalar_subquery()
                .label("raw_messages"),
                sa.select(sa.func.count())
                .select_from(opportunities)
                .scalar_subquery()
                .label("opportunities"),
                sa.select(sa.func.count())
                .select_from(lead_inbox_items)
                .scalar_subquery()
                .label("inbox_items"),
            )
        )
    ).mappings().one()
    return RecordCounts(
        raw_messages=int(row["raw_messages"]),
        opportunities=int(row["opportunities"]),
        inbox_items=int(row["inbox_items"]),
    )


async def _spend_counts(
    connection: AsyncConnection,
    *,
    day_start: datetime,
    month_start: datetime,
) -> AISpendCounts:
    today = ai_call_telemetry.c.started_at >= day_start
    month = ai_call_telemetry.c.started_at >= month_start
    row = (
        await connection.execute(
            sa.select(
                sa.func.count().filter(today).label("today_call_count"),
                sa.func.coalesce(
                    sa.func.sum(ai_call_telemetry.c.estimated_cost_usd).filter(today),
                    0,
                ).label("today_estimated_usd"),
                sa.func.count().filter(month).label("month_call_count"),
                sa.func.coalesce(
                    sa.func.sum(ai_call_telemetry.c.estimated_cost_usd).filter(month),
                    0,
                ).label("month_estimated_usd"),
            ).select_from(ai_call_telemetry)
        )
    ).mappings().one()
    return AISpendCounts(
        window_timezone="UTC",
        today_call_count=int(row["today_call_count"]),
        today_estimated_usd=_money(row["today_estimated_usd"]),
        month_call_count=int(row["month_call_count"]),
        month_estimated_usd=_money(row["month_estimated_usd"]),
    )


async def _profile_counts(connection: AsyncConnection) -> ProfileCounts:
    confirmed = search_profiles.c.confirmation_status == "confirmed"
    active = search_profiles.c.is_active.is_(True)
    primary = search_profiles.c.is_primary.is_(True)
    row = (
        await connection.execute(
            sa.select(
                sa.func.count()
                .filter(confirmed, active)
                .label("active_confirmed"),
                sa.func.count()
                .filter(confirmed, active, primary)
                .label("active_primary_confirmed"),
            ).select_from(search_profiles)
        )
    ).mappings().one()
    return ProfileCounts(
        active_confirmed=int(row["active_confirmed"]),
        active_primary_confirmed=int(row["active_primary_confirmed"]),
    )


def _money(value: object) -> str:
    selected = Decimal(str(value or 0))
    if not selected.is_finite() or selected < 0:
        raise ValueError("AI spend aggregate must be finite and nonnegative")
    return format(selected.quantize(MONEY_QUANTUM), "f")


def _validate_backlog_limit(value: int) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= MAX_CANARY_BACKLOG_LIMIT
    ):
        raise ValueError(
            f"max_active_backlog must be between 0 and {MAX_CANARY_BACKLOG_LIMIT}"
        )


def _backlog_limit(value: str) -> int:
    try:
        selected = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("ожидается целое число") from None
    try:
        _validate_backlog_limit(selected)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None
    return selected


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "AISpendCounts",
    "ConfigurationReadiness",
    "DatabaseReadiness",
    "JobStateCounts",
    "PipelineJobCounts",
    "ProfileCounts",
    "RecordCounts",
    "RuntimeReadinessReport",
    "assess_readiness",
    "build_parser",
    "inspect_configuration",
    "inspect_readiness_database",
    "inspect_readiness_from_settings",
    "main",
]

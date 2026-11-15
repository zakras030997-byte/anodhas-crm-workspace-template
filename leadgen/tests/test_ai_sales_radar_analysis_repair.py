from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock
from uuid import UUID

from sqlalchemy.dialects import postgresql

from ai_sales_radar.ai_worker import opportunity_analyzer_compatibility
from ai_sales_radar.analysis_repair import (
    AnalysisRepairConflict,
    AnalysisRepairService,
    AnalysisRepairTarget,
    EXPECTED_REPAIR_MAX_ATTEMPTS,
    INCOMPATIBLE_FAILURE_CODE,
    PostgreSQLAnalysisRepairRepository,
    REPAIR_CANARY_SOURCE_IDS,
    RepairCanaryBinding,
    RepairInspectionBatch,
    RepairJobInspection,
    RepairPrefilterRow,
    RepairWriteCounts,
    build_parser,
    build_repair_report,
    normalize_job_ids,
    repair_target_from_env,
)
from ai_sales_radar.config import OpportunityAISettings
from freelancer_bot.message_prefilter import (
    OPPORTUNITY_ANALYSIS_JOB_TYPE,
    PREFILTER_SCHEMA_VERSION,
)
from freelancer_bot.opportunity_analysis import OPPORTUNITY_ANALYZER_VERSION
from freelancer_bot.persistence.raw_messages import RAW_MESSAGE_JOB_TYPE


TARGET = AnalysisRepairTarget(
    provider="groq",
    model="openai/gpt-oss-20b",
    analyzer_version="opportunity-analyzer.v1.812138b6cad3",
    analysis_schema_version="opportunity_analysis.v1",
)
JOB_IDS = tuple(UUID(int=value) for value in range(1, 6))


def inspection(
    job_id: UUID,
    *,
    analyzer_version: str = OPPORTUNITY_ANALYZER_VERSION,
    state: str = "queued",
    attempt_count: int = 1,
    max_attempts: int = EXPECTED_REPAIR_MAX_ATTEMPTS,
    failure_code: str | None = INCOMPATIBLE_FAILURE_CODE,
    source_id: int | None = None,
    ingestion_origin: str = "catch_up",
    raw_processing_job_type: str = RAW_MESSAGE_JOB_TYPE,
    raw_processing_job_state: str = "completed",
    correlation_mismatch: bool = False,
    **hazards: int,
) -> RepairJobInspection:
    position = (job_id.int - 1) % len(REPAIR_CANARY_SOURCE_IDS)
    source = source_id or REPAIR_CANARY_SOURCE_IDS[position]
    raw_id = UUID(int=100 + job_id.int)
    prefilter_id = UUID(int=200 + job_id.int)
    processing_job_id = UUID(int=300 + job_id.int)
    correlation_id = UUID(int=400 + job_id.int)
    row = RepairPrefilterRow(
        id=prefilter_id,
        raw_message_id=raw_id,
        analysis_job_id=job_id,
        canonical_prefilter_result_id=None,
        schema_version=PREFILTER_SCHEMA_VERSION,
        decision="passed",
        analyzer_version=analyzer_version,
        analysis_schema_version=TARGET.analysis_schema_version,
        dedup_relation="canonical",
        normalized_content=f"message {job_id.int}",
        normalized_content_sha256=f"{job_id.int:064x}",
        analysis_input_sha256=f"{job_id.int + 1000:064x}",
        source_id=source,
        ingestion_origin=ingestion_origin,
        raw_processing_job_id=processing_job_id,
        raw_processing_job_type=raw_processing_job_type,
        raw_processing_job_state=raw_processing_job_state,
        raw_correlation_id=correlation_id,
        raw_processing_job_correlation_id=(
            UUID(int=9999) if correlation_mismatch else correlation_id
        ),
    )
    return RepairJobInspection(
        job_id=job_id,
        job_type=OPPORTUNITY_ANALYSIS_JOB_TYPE,
        idempotency_key=f"{PREFILTER_SCHEMA_VERSION}:{raw_id}",
        state=state,
        attempt_count=attempt_count,
        max_attempts=max_attempts,
        failure_code=failure_code,
        correlation_id=correlation_id,
        prefilters=(row,),
        **hazards,
    )


def eligible_inspections() -> tuple[RepairJobInspection, ...]:
    return tuple(inspection(job_id) for job_id in JOB_IDS)


def repaired_inspections() -> tuple[RepairJobInspection, ...]:
    return tuple(
        inspection(
            job_id,
            analyzer_version=TARGET.analyzer_version,
            attempt_count=0,
            failure_code=None,
        )
        for job_id in JOB_IDS
    )


def batch(
    inspections: tuple[RepairJobInspection, ...] | list[RepairJobInspection],
    *,
    total_raw_message_count: int = 5,
    bindings: tuple[RepairCanaryBinding, ...] | None = None,
) -> RepairInspectionBatch:
    selected = tuple(inspections)
    if bindings is None:
        built = []
        for item in selected:
            if not item.prefilters:
                continue
            row = item.prefilters[0]
            built.append(
                RepairCanaryBinding(
                    raw_message_id=row.raw_message_id,
                    source_id=row.source_id,
                    ingestion_origin=row.ingestion_origin,
                    processing_job_id=row.raw_processing_job_id,
                    prefilter_result_count=1,
                    analysis_job_ids=(item.job_id,),
                )
            )
        bindings = tuple(built)
    return RepairInspectionBatch(
        inspections=selected,
        total_raw_message_count=total_raw_message_count,
        canary_bindings=bindings,
    )


def eligible_batch() -> RepairInspectionBatch:
    return batch(eligible_inspections())


def repaired_batch() -> RepairInspectionBatch:
    return batch(repaired_inspections())


def manifest_for(value: RepairInspectionBatch | None = None) -> str:
    report = build_repair_report(
        value or eligible_batch(),
        target=TARGET,
        mode="dry-run",
    )
    assert report.repair_manifest is not None
    return report.repair_manifest


class FakeConnection:
    def __init__(self):
        self.statements = []

    async def execute(self, statement):
        self.statements.append(statement)
        return SimpleNamespace(rowcount=0)


class FakeDatabase:
    def __init__(self):
        self.connection = FakeConnection()
        self.connect_entries = 0
        self.transaction_entries = 0
        self.transaction_commits = 0
        self.transaction_rollbacks = 0

    @asynccontextmanager
    async def connect(self):
        self.connect_entries += 1
        yield self.connection

    @asynccontextmanager
    async def transaction(self):
        self.transaction_entries += 1
        try:
            yield self.connection
        except BaseException:
            self.transaction_rollbacks += 1
            raise
        else:
            self.transaction_commits += 1


class FakeRepository:
    def __init__(self, before: RepairInspectionBatch, after=None):
        self.before = before
        self.after = after or before
        self.applied = False
        self.inspect_locks = []
        self.apply_calls = []

    async def inspect(self, connection, *, job_ids, target, lock):
        self.inspect_locks.append(lock)
        return self.after if self.applied else self.before

    async def apply_repair(
        self,
        connection,
        *,
        job_ids,
        prefilter_ids,
        raw_message_ids,
        target,
    ):
        self.apply_calls.append(
            (job_ids, prefilter_ids, raw_message_ids, target)
        )
        self.applied = True
        return RepairWriteCounts(
            updated_prefilter_rows=len(prefilter_ids),
            requeued_jobs=len(job_ids),
        )


class AnalysisRepairContractTest(unittest.TestCase):
    def test_exact_five_distinct_job_ids_are_required(self):
        self.assertEqual(normalize_job_ids(reversed(JOB_IDS)), JOB_IDS)
        with self.assertRaisesRegex(ValueError, "exactly five"):
            normalize_job_ids(JOB_IDS[:4])
        with self.assertRaisesRegex(ValueError, "distinct"):
            normalize_job_ids((*JOB_IDS[:4], JOB_IDS[0]))

    def test_only_exact_old_state_is_eligible(self):
        report = build_repair_report(
            eligible_batch(), target=TARGET, mode="dry-run"
        )
        self.assertTrue(report.ready)
        self.assertEqual(len(report.eligible_job_ids), 5)
        self.assertIsNotNone(report.repair_manifest)

        variants = {
            "old_job_state_mismatch": {"state": "failed"},
            "old_attempt_count_mismatch": {"attempt_count": 2},
            "max_attempts_mismatch": {"max_attempts": 4},
            "failure_code_mismatch": {"failure_code": "DifferentFailure"},
        }
        for reason, overrides in variants.items():
            with self.subTest(reason=reason):
                rows = list(eligible_inspections())
                rows[0] = inspection(JOB_IDS[0], **overrides)
                result = build_repair_report(
                    batch(rows), target=TARGET, mode="dry-run"
                )
                self.assertFalse(result.ready)
                self.assertIn(reason, result.blocked_jobs[0].reasons)

    def test_repaired_state_is_never_publicly_idempotent(self):
        public = build_repair_report(
            repaired_batch(), target=TARGET, mode="apply"
        )
        self.assertFalse(public.ready)
        self.assertTrue(
            all("already_applied" in item.reasons for item in public.blocked_jobs)
        )

        internal = build_repair_report(
            repaired_batch(),
            target=TARGET,
            mode="apply",
            allow_post_repair=True,
        )
        self.assertTrue(internal.ready)
        self.assertEqual(len(internal.already_reconciled_job_ids), 5)

    def test_processed_or_ambiguous_jobs_are_blocked(self):
        variants = {
            "ai_telemetry_present": {"ai_call_count": 1},
            "analysis_cache_present": {"analysis_cache_count": 1},
            "opportunity_present": {"opportunity_source_count": 1},
            "target_canonical_conflict": {
                "target_canonical_conflict_count": 1
            },
        }
        for reason, overrides in variants.items():
            with self.subTest(reason=reason):
                rows = list(eligible_inspections())
                rows[0] = inspection(JOB_IDS[0], **overrides)
                report = build_repair_report(
                    batch(rows), target=TARGET, mode="dry-run"
                )
                self.assertFalse(report.ready)
                self.assertIn(reason, report.blocked_jobs[0].reasons)

    def test_duplicate_or_noncanonical_prefilter_is_blocked(self):
        rows = list(eligible_inspections())
        duplicate = replace(
            rows[0].prefilters[0],
            id=UUID(int=999),
            dedup_relation="exact_duplicate",
            canonical_prefilter_result_id=rows[0].prefilters[0].id,
        )
        rows[0] = replace(rows[0], prefilters=(*rows[0].prefilters, duplicate))
        report = build_repair_report(batch(rows), target=TARGET, mode="dry-run")
        self.assertFalse(report.ready)
        self.assertIn(
            "selected_prefilter_row_count_not_exact_five",
            report.global_blockers,
        )
        first = next(
            item for item in report.blocked_jobs if item.job_id == str(JOB_IDS[0])
        )
        self.assertIn("prefilter_row_count_invalid", first.reasons)

    def test_source_origin_raw_job_and_correlation_are_enforced(self):
        variants = {
            "source_not_in_canary": {"source_id": 99},
            "ingestion_origin_mismatch": {"ingestion_origin": "live"},
            "raw_processing_job_type_mismatch": {
                "raw_processing_job_type": "wrong.job"
            },
            "raw_processing_job_state_mismatch": {
                "raw_processing_job_state": "queued"
            },
            "correlation_chain_mismatch": {"correlation_mismatch": True},
        }
        for reason, overrides in variants.items():
            with self.subTest(reason=reason):
                rows = list(eligible_inspections())
                rows[0] = inspection(JOB_IDS[0], **overrides)
                report = build_repair_report(
                    batch(rows), target=TARGET, mode="dry-run"
                )
                self.assertFalse(report.ready)
                per_job_reasons = {
                    value
                    for item in report.blocked_jobs
                    for value in item.reasons
                }
                self.assertIn(reason, per_job_reasons)

    def test_scope_requires_only_the_exact_five_canary_raw_messages(self):
        too_many = build_repair_report(
            batch(eligible_inspections(), total_raw_message_count=6),
            target=TARGET,
            mode="dry-run",
        )
        self.assertIn(
            "total_raw_message_count_not_exact_five",
            too_many.global_blockers,
        )

        bindings = list(eligible_batch().canary_bindings)
        bindings[0] = replace(bindings[0], analysis_job_ids=(UUID(int=999),))
        wrong_manifest = build_repair_report(
            batch(eligible_inspections(), bindings=tuple(bindings)),
            target=TARGET,
            mode="dry-run",
        )
        self.assertIn(
            "canary_analysis_job_manifest_mismatch",
            wrong_manifest.global_blockers,
        )
        self.assertIsNone(wrong_manifest.repair_manifest)

        bindings = list(eligible_batch().canary_bindings)
        bindings[0] = replace(bindings[0], prefilter_result_count=2)
        extra_schema_row = build_repair_report(
            batch(eligible_inspections(), bindings=tuple(bindings)),
            target=TARGET,
            mode="dry-run",
        )
        self.assertIn(
            "canary_prefilter_row_count_invalid",
            extra_schema_row.global_blockers,
        )

    def test_manifest_is_stable_and_binds_target_and_rows(self):
        first = manifest_for()
        self.assertEqual(first, manifest_for())

        other_target = replace(TARGET, model="different-model")
        changed = build_repair_report(
            eligible_batch(), target=other_target, mode="dry-run"
        )
        self.assertNotEqual(first, changed.repair_manifest)

        rows = list(eligible_inspections())
        changed_row = replace(rows[0].prefilters[0], id=UUID(int=777))
        rows[0] = replace(rows[0], prefilters=(changed_row,))
        self.assertNotEqual(first, manifest_for(batch(rows)))

    def test_target_is_derived_from_shared_pure_compatibility_helper(self):
        settings = OpportunityAISettings(
            provider="groq",
            groq_api_key="groq-repair-unit-test-key",
            external_ai_data_processing_approved=True,
            _env_file=None,
        )
        expected = opportunity_analyzer_compatibility(settings)

        target = repair_target_from_env(settings_factory=lambda: settings)

        self.assertEqual(target.provider, "groq")
        self.assertEqual(target.model, "openai/gpt-oss-20b")
        self.assertEqual(target.analyzer_version, expected.analyzer_version)
        self.assertEqual(
            target.analysis_schema_version,
            expected.analysis_schema_version,
        )

    def test_apply_parser_requires_dry_run_manifest(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "apply",
                *(part for job_id in JOB_IDS for part in ("--job-id", str(job_id))),
                "--confirm-manifest",
                manifest_for(),
            ]
        )
        self.assertEqual(args.confirm_manifest, manifest_for())


class AnalysisRepairServiceTest(unittest.IsolatedAsyncioTestCase):
    async def test_dry_run_uses_one_repeatable_read_snapshot_and_never_writes(self):
        database = FakeDatabase()
        repository = FakeRepository(eligible_batch())
        service = AnalysisRepairService(database, repository=repository)

        report = await service.dry_run(JOB_IDS, TARGET)

        self.assertTrue(report.ready)
        self.assertFalse(report.applied)
        self.assertEqual(database.connect_entries, 1)
        self.assertEqual(database.transaction_entries, 0)
        self.assertEqual(repository.inspect_locks, [False])
        self.assertEqual(repository.apply_calls, [])
        self.assertIn(
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
            str(database.connection.statements[0]),
        )

    async def test_apply_is_atomic_rechecks_and_verifies_exact_five(self):
        database = FakeDatabase()
        repository = FakeRepository(eligible_batch(), repaired_batch())
        service = AnalysisRepairService(database, repository=repository)

        report = await service.apply(
            JOB_IDS,
            TARGET,
            confirmation_manifest=manifest_for(),
        )

        self.assertTrue(report.ready)
        self.assertTrue(report.applied)
        self.assertEqual(report.updated_prefilter_rows, 5)
        self.assertEqual(report.requeued_jobs, 5)
        self.assertEqual(database.transaction_commits, 1)
        self.assertEqual(repository.inspect_locks, [True, True])
        self.assertEqual(len(repository.apply_calls), 1)
        job_ids, prefilter_ids, raw_ids, target = repository.apply_calls[0]
        self.assertEqual(job_ids, JOB_IDS)
        self.assertEqual(len(prefilter_ids), 5)
        self.assertEqual(len(raw_ids), 5)
        self.assertEqual(target, TARGET)
        self.assertIn(
            "LOCK TABLE raw_messages, message_prefilter_results",
            str(database.connection.statements[1]),
        )

    async def test_manifest_mismatch_blocks_before_writes(self):
        database = FakeDatabase()
        repository = FakeRepository(eligible_batch())
        service = AnalysisRepairService(database, repository=repository)
        good = manifest_for()
        wrong = good[:-1] + ("0" if good[-1] != "0" else "1")

        report = await service.apply(
            JOB_IDS,
            TARGET,
            confirmation_manifest=wrong,
        )

        self.assertFalse(report.ready)
        self.assertIn("manifest_mismatch", report.global_blockers)
        self.assertEqual(repository.apply_calls, [])

    async def test_repeated_apply_is_blocked_and_makes_no_writes(self):
        database = FakeDatabase()
        repository = FakeRepository(repaired_batch())
        service = AnalysisRepairService(database, repository=repository)

        report = await service.apply(
            JOB_IDS,
            TARGET,
            confirmation_manifest=manifest_for(),
        )

        self.assertFalse(report.ready)
        self.assertFalse(report.applied)
        self.assertEqual(repository.inspect_locks, [True])
        self.assertEqual(repository.apply_calls, [])

    async def test_blocked_apply_makes_no_writes(self):
        database = FakeDatabase()
        rows = list(eligible_inspections())
        rows[0] = replace(rows[0], ai_call_count=1)
        repository = FakeRepository(batch(rows))
        service = AnalysisRepairService(database, repository=repository)

        report = await service.apply(
            JOB_IDS,
            TARGET,
            confirmation_manifest=manifest_for(),
        )

        self.assertFalse(report.ready)
        self.assertFalse(report.applied)
        self.assertEqual(repository.apply_calls, [])

    async def test_repository_must_return_the_exact_requested_set(self):
        database = FakeDatabase()
        wrong = list(eligible_inspections())
        wrong[-1] = inspection(
            UUID(int=999), source_id=REPAIR_CANARY_SOURCE_IDS[-1]
        )
        repository = FakeRepository(batch(wrong))
        service = AnalysisRepairService(database, repository=repository)

        with self.assertRaises(AnalysisRepairConflict):
            await service.apply(
                JOB_IDS,
                TARGET,
                confirmation_manifest=manifest_for(),
            )

        self.assertEqual(repository.apply_calls, [])

    async def test_post_write_verification_failure_rolls_back(self):
        database = FakeDatabase()
        after = repaired_batch()
        bad_rows = list(after.inspections)
        bad_rows[0] = replace(bad_rows[0], ai_call_count=1)
        repository = FakeRepository(
            eligible_batch(),
            replace(after, inspections=tuple(bad_rows)),
        )
        service = AnalysisRepairService(database, repository=repository)

        with self.assertRaises(AnalysisRepairConflict):
            await service.apply(
                JOB_IDS,
                TARGET,
                confirmation_manifest=manifest_for(),
            )

        self.assertEqual(database.transaction_rollbacks, 1)
        self.assertEqual(database.transaction_commits, 0)


class PostgreSQLRepairUpdateTest(unittest.IsolatedAsyncioTestCase):
    async def test_updates_use_only_exact_rows_and_exact_old_job_state(self):
        connection = AsyncMock()
        connection.execute.side_effect = [
            SimpleNamespace(rowcount=5),
            SimpleNamespace(rowcount=5),
        ]
        repository = PostgreSQLAnalysisRepairRepository()
        rows = eligible_inspections()

        counts = await repository.apply_repair(
            connection,
            job_ids=JOB_IDS,
            prefilter_ids=tuple(item.prefilters[0].id for item in rows),
            raw_message_ids=tuple(
                item.prefilters[0].raw_message_id for item in rows
            ),
            target=TARGET,
        )

        self.assertEqual(counts, RepairWriteCounts(5, 5))
        prefilter_statement = connection.execute.await_args_list[0].args[0]
        job_statement = connection.execute.await_args_list[1].args[0]
        prefilter_sql = str(
            prefilter_statement.compile(dialect=postgresql.dialect())
        )
        job_sql = str(job_statement.compile(dialect=postgresql.dialect()))
        for predicate in (
            "message_prefilter_results.id IN",
            "message_prefilter_results.analysis_job_id IN",
            "message_prefilter_results.raw_message_id IN",
            "message_prefilter_results.dedup_relation",
            "message_prefilter_results.canonical_prefilter_result_id IS NULL",
            "message_prefilter_results.analyzer_version",
        ):
            self.assertIn(predicate, prefilter_sql)
        for predicate in (
            "durable_jobs.failure_code",
            "durable_jobs.job_type",
            "durable_jobs.id IN",
            "durable_jobs.state =",
            "durable_jobs.attempt_count =",
            "durable_jobs.max_attempts =",
            "durable_jobs.claimed_at IS NULL",
            "durable_jobs.failed_at IS NULL",
        ):
            self.assertIn(predicate, job_sql)
        self.assertIn(
            OPPORTUNITY_ANALYZER_VERSION,
            prefilter_statement.compile().params.values(),
        )
        self.assertIn(
            INCOMPATIBLE_FAILURE_CODE,
            job_statement.compile().params.values(),
        )

    async def test_prefilter_rowcount_mismatch_stops_before_job_update(self):
        connection = AsyncMock()
        connection.execute.return_value = SimpleNamespace(rowcount=4)
        repository = PostgreSQLAnalysisRepairRepository()
        rows = eligible_inspections()

        with self.assertRaises(AnalysisRepairConflict):
            await repository.apply_repair(
                connection,
                job_ids=JOB_IDS,
                prefilter_ids=tuple(item.prefilters[0].id for item in rows),
                raw_message_ids=tuple(
                    item.prefilters[0].raw_message_id for item in rows
                ),
                target=TARGET,
            )

        self.assertEqual(connection.execute.await_count, 1)

    async def test_job_rowcount_mismatch_raises_for_transaction_rollback(self):
        connection = AsyncMock()
        connection.execute.side_effect = [
            SimpleNamespace(rowcount=5),
            SimpleNamespace(rowcount=4),
        ]
        repository = PostgreSQLAnalysisRepairRepository()
        rows = eligible_inspections()

        with self.assertRaises(AnalysisRepairConflict):
            await repository.apply_repair(
                connection,
                job_ids=JOB_IDS,
                prefilter_ids=tuple(item.prefilters[0].id for item in rows),
                raw_message_ids=tuple(
                    item.prefilters[0].raw_message_id for item in rows
                ),
                target=TARGET,
            )

        self.assertEqual(connection.execute.await_count, 2)


if __name__ == "__main__":
    unittest.main()

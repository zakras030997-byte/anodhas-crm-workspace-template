import contextlib
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
import io
import json
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
from uuid import UUID, uuid4

import sqlalchemy as sa

from ai_sales_radar.profiles import (
    LOCAL_USER_EXTERNAL_ID,
    LOCAL_USER_PLATFORM,
    LocalProfileActivationResult,
    LocalProfileStateRepository,
    LocalProfilesService,
    ProfileCreateResult,
    ProfilesError,
    _emit,
    build_parser,
    main,
)
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.schema import (
    durable_jobs,
    profile_discovery_intents,
    search_profiles,
    telegram_chat_discovery_topics,
    users,
)
from freelancer_bot.persistence.search_profiles import (
    SearchProfileActivationError,
    SearchProfileConfirmationStatus,
    SearchProfileRecord,
    SearchProfileWriteOutcome,
    UserNotFound,
    UserRecord,
    UserWriteOutcome,
)
from freelancer_bot.search_profiles import (
    BudgetPolicy,
    OpportunityType,
    WorkMode,
    empty_search_profile_preferences,
    parse_search_profile,
    parse_search_profile_preferences,
)
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


NOW = datetime(2026, 8, 26, tzinfo=UTC)
USER_ID = UUID("00000000-0000-0000-0000-000000000111")
PROFILE_ID = UUID("00000000-0000-0000-0000-000000000222")


def user_record(*, user_id=USER_ID):
    return UserRecord(
        id=user_id,
        platform=LOCAL_USER_PLATFORM,
        external_user_id=LOCAL_USER_EXTERNAL_ID,
        trial_started_at=None,
        trial_expires_at=None,
        trial_policy_version=None,
        created_at=NOW,
        updated_at=NOW,
    )


def profile_record(
    *,
    profile_id=PROFILE_ID,
    user_id=USER_ID,
    revision=1,
    confirmation_status=SearchProfileConfirmationStatus.DRAFT,
    is_active=False,
    is_primary=False,
):
    parsed = parse_search_profile(
        roles=("AI Developer",),
        skills=("Python", "n8n"),
        categories=("AI Automation",),
        semantic_text="Ищу проекты по AI-автоматизации для бизнеса.",
    )
    confirmed = confirmation_status is SearchProfileConfirmationStatus.CONFIRMED
    return SearchProfileRecord(
        id=profile_id,
        user_id=user_id,
        schema_version=parsed.schema_version,
        parser_version=parsed.parser_version,
        analysis_cache_id=None,
        roles=parsed.roles,
        skills=parsed.skills,
        categories=parsed.categories,
        semantic_text_original=parsed.semantic_text_original,
        semantic_text_normalized=parsed.semantic_text_normalized,
        preferences=empty_search_profile_preferences(),
        confirmation_status=confirmation_status,
        revision=revision,
        confirmed_at=NOW if confirmed else None,
        is_active=is_active,
        is_primary=is_primary,
        activated_at=NOW if is_active else None,
        deactivated_at=None,
        created_at=NOW,
        updated_at=NOW,
    )


class AsyncContext:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class FakeDatabase:
    def __init__(self, connection=None):
        self.connection = connection or object()
        self.connect_count = 0
        self.transaction_count = 0

    def connect(self):
        self.connect_count += 1
        return AsyncContext(self.connection)

    def transaction(self):
        self.transaction_count += 1
        return AsyncContext(self.connection)


class FakeUsers:
    def __init__(self, *, exists=True):
        self.exists = exists
        self.ensure_arguments = None

    async def ensure(self, _connection, *, platform, external_user_id):
        self.ensure_arguments = (platform, external_user_id)
        return UserWriteOutcome(user=user_record(), created=True)

    async def get_by_identity(self, _connection, *, platform, external_user_id):
        if not self.exists:
            raise UserNotFound("local user missing")
        if (platform, external_user_id) != (
            LOCAL_USER_PLATFORM,
            LOCAL_USER_EXTERNAL_ID,
        ):
            raise AssertionError("unexpected local identity")
        return user_record()


class FakeProfiles:
    def __init__(self):
        self.profile = profile_record()
        self.created_profile = None
        self.edit_arguments = None
        self.preference_arguments = None
        self.confirm_arguments = None

    async def create(self, _connection, *, user_id, parsed_profile):
        self.created_profile = parsed_profile
        self.profile = replace(
            self.profile,
            user_id=user_id,
            roles=parsed_profile.roles,
            skills=parsed_profile.skills,
            categories=parsed_profile.categories,
            semantic_text_original=parsed_profile.semantic_text_original,
            semantic_text_normalized=parsed_profile.semantic_text_normalized,
        )
        return SearchProfileWriteOutcome(profile=self.profile, created=True)

    async def update_preferences(
        self,
        _connection,
        *,
        profile_id,
        user_id,
        preferences,
        expected_revision,
    ):
        self.preference_arguments = (
            profile_id,
            user_id,
            preferences,
            expected_revision,
        )
        self.profile = replace(
            self.profile,
            preferences=preferences,
            revision=expected_revision + 1,
        )
        return self.profile

    async def list_for_user(self, _connection, *, user_id):
        if user_id != USER_ID:
            raise AssertionError("unexpected user")
        return (self.profile,)

    async def get(self, _connection, profile_id):
        if profile_id != self.profile.id:
            raise AssertionError("unexpected profile")
        return self.profile

    async def edit(
        self,
        _connection,
        *,
        profile_id,
        user_id,
        parsed_profile,
        expected_revision,
    ):
        self.edit_arguments = (
            profile_id,
            user_id,
            parsed_profile,
            expected_revision,
        )
        self.profile = replace(
            self.profile,
            roles=parsed_profile.roles,
            skills=parsed_profile.skills,
            categories=parsed_profile.categories,
            revision=expected_revision + 1,
        )
        return self.profile

    async def confirm(
        self,
        _connection,
        *,
        profile_id,
        user_id,
        expected_revision,
    ):
        self.confirm_arguments = (profile_id, user_id, expected_revision)
        self.profile = replace(
            self.profile,
            confirmation_status=SearchProfileConfirmationStatus.CONFIRMED,
            confirmed_at=NOW,
            revision=expected_revision + 1,
        )
        return self.profile


class FakeState:
    def __init__(self, profile):
        self.profile = profile
        self.arguments = None

    async def activate_primary(
        self,
        _connection,
        *,
        profile_id,
        user_id,
        expected_revision,
    ):
        self.arguments = (profile_id, user_id, expected_revision)
        activated = replace(
            self.profile,
            is_active=True,
            is_primary=True,
            revision=expected_revision + 1,
            activated_at=NOW,
        )
        return LocalProfileActivationResult(changed=True, profile=activated)


class LocalProfilesServiceTest(unittest.IsolatedAsyncioTestCase):
    async def test_create_uses_local_identity_normalizers_and_preferences(self):
        database = FakeDatabase()
        users_repository = FakeUsers()
        profiles_repository = FakeProfiles()
        service = LocalProfilesService(
            database,
            users_repository=users_repository,
            profiles_repository=profiles_repository,
        )
        preferences = parse_search_profile_preferences(
            work_types=(OpportunityType.PROJECT,),
            minimum_budget="2000",
            currency="eur",
            budget_policy=BudgetPolicy.ALLOW_UNKNOWN,
            languages=("Русский",),
            geographies=("Europe",),
            work_modes=(WorkMode.REMOTE,),
            excluded_categories=("Gambling",),
        )

        result = await service.create_profile(
            semantic_text="  AI automation   и n8n  ",
            roles=(" AI Developer ",),
            skills=("Python", "python", "n8n"),
            categories=("Automation",),
            preferences=preferences,
        )

        self.assertIsInstance(result, ProfileCreateResult)
        self.assertEqual(
            users_repository.ensure_arguments,
            (LOCAL_USER_PLATFORM, LOCAL_USER_EXTERNAL_ID),
        )
        self.assertEqual(
            [term.value for term in profiles_repository.created_profile.skills],
            ["Python", "n8n"],
        )
        self.assertEqual(
            profiles_repository.created_profile.semantic_text_normalized,
            "AI automation и n8n",
        )
        self.assertEqual(result.profile.preferences.minimum_budget, Decimal("2000"))
        self.assertEqual(result.profile.preferences.currency, "EUR")
        self.assertEqual(result.profile.revision, 2)
        self.assertEqual(database.transaction_count, 1)

    async def test_create_and_edit_reject_empty_structured_target(self):
        service = LocalProfilesService(
            FakeDatabase(),
            users_repository=FakeUsers(),
            profiles_repository=FakeProfiles(),
        )

        with self.assertRaisesRegex(ProfilesError, "хотя бы одна"):
            await service.create_profile(
                semantic_text="AI automation",
                roles=(),
                skills=(),
                categories=(),
            )
        with self.assertRaisesRegex(ProfilesError, "Нельзя удалить"):
            await service.edit_profile(
                PROFILE_ID,
                expected_revision=1,
                roles=(),
                skills=(),
                categories=(),
            )

    async def test_edit_preserves_semantic_source_and_replaces_terms(self):
        profiles_repository = FakeProfiles()
        service = LocalProfilesService(
            FakeDatabase(),
            users_repository=FakeUsers(),
            profiles_repository=profiles_repository,
        )

        updated = await service.edit_profile(
            PROFILE_ID,
            expected_revision=1,
            roles=("Automation Engineer",),
            skills=("OpenAI API",),
            categories=("Business Automation",),
        )

        parsed = profiles_repository.edit_arguments[2]
        self.assertEqual(parsed.semantic_text_original, profile_record().semantic_text_original)
        self.assertEqual([term.value for term in updated.roles], ["Automation Engineer"])
        self.assertEqual(updated.revision, 2)

    async def test_list_before_first_create_is_empty_and_read_only(self):
        database = FakeDatabase()
        service = LocalProfilesService(
            database,
            users_repository=FakeUsers(exists=False),
            profiles_repository=FakeProfiles(),
        )

        result = await service.list_profiles()

        self.assertEqual((result.count, result.profiles), (0, ()))
        self.assertEqual(database.connect_count, 1)
        self.assertEqual(database.transaction_count, 0)

    async def test_confirm_and_activate_use_only_local_profile_boundaries(self):
        database = FakeDatabase()
        profiles_repository = FakeProfiles()
        state = FakeState(profiles_repository.profile)
        service = LocalProfilesService(
            database,
            users_repository=FakeUsers(),
            profiles_repository=profiles_repository,
            state_repository=state,
        )

        confirmed = await service.confirm_profile(PROFILE_ID, expected_revision=1)
        activated = await service.activate_profile(
            PROFILE_ID,
            expected_revision=confirmed.revision,
        )

        self.assertEqual(profiles_repository.confirm_arguments, (PROFILE_ID, USER_ID, 1))
        self.assertEqual(state.arguments, (PROFILE_ID, USER_ID, 2))
        self.assertTrue(activated.changed)
        self.assertFalse(activated.trial_started)
        self.assertEqual(activated.jobs_enqueued, 0)


class FakeStateConnection:
    def __init__(self, *, user_id=USER_ID, profile_id=PROFILE_ID):
        self.user_id = user_id
        self.profile_id = profile_id
        self.statements = []

    async def scalar(self, statement):
        self.statements.append(statement)
        if getattr(statement, "is_select", False):
            return self.user_id
        if getattr(statement, "is_update", False):
            return self.profile_id
        raise AssertionError("unexpected scalar statement")

    async def execute(self, statement):
        self.statements.append(statement)
        return SimpleNamespace(rowcount=0)


class SequentialProfileReader:
    def __init__(self, records):
        self.records = list(records)

    async def get(self, _connection, _profile_id):
        return self.records.pop(0)


class LocalProfileStateRepositoryTest(unittest.IsolatedAsyncioTestCase):
    async def test_activation_mutates_only_search_profiles(self):
        current = profile_record(
            revision=3,
            confirmation_status=SearchProfileConfirmationStatus.CONFIRMED,
        )
        activated = replace(
            current,
            revision=4,
            is_active=True,
            is_primary=True,
            activated_at=NOW,
        )
        profiles_repository = SequentialProfileReader([current, activated])
        connection = FakeStateConnection()
        repository = LocalProfileStateRepository(profiles_repository)

        result = await repository.activate_primary(
            connection,
            profile_id=PROFILE_ID,
            user_id=USER_ID,
            expected_revision=3,
        )

        mutated_tables = {
            statement.table.name
            for statement in connection.statements
            if getattr(statement, "is_update", False)
        }
        self.assertEqual(mutated_tables, {search_profiles.name})
        self.assertTrue(result.changed)
        self.assertFalse(result.trial_started)
        self.assertEqual(result.jobs_enqueued, 0)

    async def test_draft_cannot_activate(self):
        repository = LocalProfileStateRepository(
            SequentialProfileReader([profile_record()])
        )

        with self.assertRaises(SearchProfileActivationError):
            await repository.activate_primary(
                FakeStateConnection(),
                profile_id=PROFILE_ID,
                user_id=USER_ID,
                expected_revision=1,
            )


class ProfilesCommandTest(unittest.TestCase):
    def test_parser_exposes_manual_profile_lifecycle(self):
        parser = build_parser()

        create = parser.parse_args(
            [
                "create",
                "--semantic-text",
                "AI automation",
                "--role",
                "AI Developer",
                "--skill",
                "n8n",
                "--work-type",
                "project",
                "--minimum-budget",
                "2000",
                "--currency",
                "EUR",
            ]
        )
        self.assertEqual(create.command, "create")
        self.assertEqual(create.role, ["AI Developer"])
        self.assertEqual(create.work_type, ["project"])

        for command in ("show", "confirm", "activate"):
            arguments = [command, str(PROFILE_ID)]
            if command in {"confirm", "activate"}:
                arguments.extend(("--revision", "3"))
            self.assertEqual(parser.parse_args(arguments).command, command)

    def test_missing_command_never_reads_database_configuration(self):
        with (
            patch("ai_sales_radar.profiles.DatabaseSettings.from_env") as settings,
            contextlib.redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit),
        ):
            main([])
        settings.assert_not_called()

    def test_unexpected_failure_does_not_expose_dsn(self):
        database_url = (
            "postgresql+psycopg://operator:" + "top-secret@localhost/radar"
        )
        settings = SimpleNamespace(require_url=lambda: database_url)
        stderr = io.StringIO()
        with (
            patch(
                "ai_sales_radar.profiles.DatabaseSettings.from_env",
                return_value=settings,
            ),
            patch(
                "ai_sales_radar.profiles._execute",
                AsyncMock(side_effect=RuntimeError(database_url)),
            ),
            contextlib.redirect_stderr(stderr),
        ):
            exit_code = main(["list"])

        self.assertEqual(exit_code, 1)
        self.assertNotIn("top-secret", stderr.getvalue())
        self.assertNotIn(database_url, stderr.getvalue())

    def test_json_output_keeps_decimal_uuid_and_enums_stable(self):
        output = io.StringIO()
        preferences = parse_search_profile_preferences(
            minimum_budget="2000",
            currency="EUR",
            work_types=(OpportunityType.PROJECT,),
        )
        record = replace(profile_record(), preferences=preferences)

        with contextlib.redirect_stdout(output):
            _emit(record)

        payload = json.loads(output.getvalue())
        self.assertEqual(payload["id"], str(PROFILE_ID))
        self.assertEqual(payload["preferences"]["minimum_budget"], "2000")
        self.assertEqual(payload["preferences"]["work_types"], ["project"])
        self.assertEqual(payload["confirmation_status"], "draft")


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class LocalProfilesPostgresIntegrationTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=2, max_overflow=0)
        self.service = LocalProfilesService(self.database)

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_manual_lifecycle_has_no_trial_billing_discovery_or_jobs(self):
        preferences = parse_search_profile_preferences(
            work_types=(OpportunityType.PROJECT, OpportunityType.ONE_OFF_ORDER),
            minimum_budget="2000",
            currency="EUR",
            budget_policy=BudgetPolicy.ALLOW_UNKNOWN,
            languages=("Russian", "English"),
            geographies=("Europe",),
            work_modes=(WorkMode.REMOTE,),
            excluded_categories=("Gambling",),
        )
        created = await self.service.create_profile(
            semantic_text="AI automation, n8n, Telegram bots and RAG",
            roles=("AI Developer",),
            skills=("n8n", "Python", "RAG"),
            categories=("AI Automation",),
            preferences=preferences,
        )
        edited = await self.service.edit_profile(
            created.profile.id,
            expected_revision=created.profile.revision,
            roles=("AI Automation Developer",),
            skills=("n8n", "Python", "RAG", "OpenAI API"),
            categories=("AI Automation", "Telegram Bots"),
        )
        confirmed = await self.service.confirm_profile(
            edited.id,
            expected_revision=edited.revision,
        )
        activated = await self.service.activate_profile(
            confirmed.id,
            expected_revision=confirmed.revision,
        )

        self.assertTrue(activated.changed)
        self.assertTrue(activated.profile.is_active)
        self.assertTrue(activated.profile.is_primary)
        self.assertFalse(activated.trial_started)
        self.assertEqual(activated.jobs_enqueued, 0)
        async with self.database.connect() as connection:
            user_row = (
                await connection.execute(
                    sa.select(users).where(
                        users.c.platform == LOCAL_USER_PLATFORM,
                        users.c.external_user_id == LOCAL_USER_EXTERNAL_ID,
                    )
                )
            ).mappings().one()
            jobs_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(durable_jobs)
            )
            profile_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(search_profiles)
            )
            discovery_intent_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(profile_discovery_intents)
            )
            telegram_topic_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(
                    telegram_chat_discovery_topics
                )
            )

        self.assertIsNone(user_row["trial_started_at"])
        self.assertIsNone(user_row["trial_expires_at"])
        self.assertIsNone(user_row["trial_policy_version"])
        self.assertEqual(jobs_count, 0)
        self.assertEqual(profile_count, 1)
        self.assertEqual(discovery_intent_count, 0)
        self.assertEqual(telegram_topic_count, 0)


if __name__ == "__main__":
    unittest.main()

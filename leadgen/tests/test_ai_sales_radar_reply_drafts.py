from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import io
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
import urllib.error
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from pydantic import ValidationError

from ai_sales_radar.config import DatabaseSettings, ReplyDraftSettings
from ai_sales_radar.reply_drafts import (
    GROQ_CHAT_COMPLETIONS_URL,
    OpenAICompatibleReplyDraftGenerator,
    OpenAIReplyDraftGenerator,
    ReplyDraftContext,
    ReplyDraftError,
    ReplyDraftGenerationLock,
    ReplyDraftOutput,
    ReplyDraftService,
    canonical_sha256,
    draft_input_sha256,
    load_private_persona,
    persona_prompt_payload,
    sanitize_reply_draft,
    validate_reply_draft_output,
)
from ai_sales_radar.reply_drafts_cli import (
    inspect_reply_draft_configuration,
    main,
)


class FakeDatabase:
    def connect(self):
        return _context(object())

    def transaction(self):
        return _context(object())


@asynccontextmanager
async def _context(value):
    yield value


def context() -> ReplyDraftContext:
    return ReplyDraftContext(
        inbox_item_id=uuid4(),
        match_trace_id=uuid4(),
        opportunity_id=uuid4(),
        search_profile_id=uuid4(),
        profile_revision=3,
        original_message="Нужен Telegram-бот с интеграцией CRM.",
        message_url="https://t.me/example_channel_012/10",
        opportunity={"summary": "Telegram CRM bot"},
        match={"eligible": True, "combined_relevance_score": "0.8"},
        search_profile={"skills": ["Python", "Telethon"]},
    )


def approved_reply_settings(**values) -> ReplyDraftSettings:
    values.setdefault("external_ai_data_processing_approved", True)
    settings = ReplyDraftSettings(_env_file=None, **values)
    return settings


class FakeGenerator:
    provider = "openai"
    model = "fixture-model"
    prompt_version = "reply-draft-prompt.v2"
    schema_version = "reply-draft.v2"

    def __init__(self, *, delay: float = 0.0):
        self.calls = []
        self.delay = delay

    @property
    def cache_identity(self):
        return {"fixture": "stable"}

    async def generate(self, draft_context, persona):
        self.calls.append((draft_context, persona))
        if self.delay:
            await asyncio.sleep(self.delay)
        return ReplyDraftOutput(
            proposal_draft=(
                "Здравствуйте. Сначала проверю поток данных между Telegram и CRM, "
                "затем соберу небольшой тестовый сценарий."
            ),
            short_reply="Здравствуйте. Начну с проверки интеграции Telegram и CRM.",
            questions_to_client=("Какая CRM используется?",),
            risks=(),
        )


class FakeGenerationLock:
    def __init__(self):
        self._lock = asyncio.Lock()
        self.keys = []

    @asynccontextmanager
    async def hold(self, key):
        self.keys.append(key)
        async with self._lock:
            yield


class MemoryDraftRepository:
    def __init__(self):
        self.record = None
        self.ensure_calls = 0

    async def get_by_idempotency_key(self, connection, key):
        return self.record

    async def ensure(self, connection, **values):
        self.ensure_calls += 1
        self.record = SimpleNamespace(id=uuid4(), **values)
        return self.record


class FakeAdvisoryConnection:
    def __init__(self):
        self.scalar_calls = []
        self.commits = 0
        self.invalidations = 0

    async def scalar(self, statement, parameters):
        self.scalar_calls.append((str(statement), dict(parameters)))
        return True

    async def commit(self):
        self.commits += 1

    async def invalidate(self):
        self.invalidations += 1


class FakeLockDatabase:
    def __init__(self):
        self.connection = FakeAdvisoryConnection()

    def connect(self):
        return _context(self.connection)


class ReplyDraftContractTest(unittest.IsolatedAsyncioTestCase):
    async def test_generation_lock_acquires_and_releases_same_session(self):
        database = FakeLockDatabase()
        lock = ReplyDraftGenerationLock(database, timeout_seconds=1)
        async with lock.hold("a" * 64):
            self.assertEqual(len(database.connection.scalar_calls), 1)
        self.assertEqual(len(database.connection.scalar_calls), 2)
        acquire, release = database.connection.scalar_calls
        self.assertIn("pg_try_advisory_lock", acquire[0])
        self.assertIn("pg_advisory_unlock", release[0])
        self.assertEqual(acquire[1], release[1])
        self.assertEqual(
            acquire[1]["key"],
            "ai-sales-radar-reply-draft:" + "a" * 64,
        )
        self.assertEqual(database.connection.commits, 2)
        self.assertEqual(database.connection.invalidations, 0)

    def test_output_contract_is_strict_and_has_only_four_fields(self):
        schema = ReplyDraftOutput.model_json_schema()
        self.assertFalse(schema["additionalProperties"])
        self.assertEqual(
            set(schema["properties"]),
            {
                "proposal_draft",
                "short_reply",
                "questions_to_client",
                "risks",
            },
        )
        with self.assertRaises(ValidationError):
            ReplyDraftOutput.model_validate(
                {
                    "proposal_draft": "x" * 20,
                    "short_reply": "x" * 10,
                    "questions_to_client": [],
                    "risks": [],
                    "fit_score": 99,
                },
                strict=True,
            )

    def test_input_hash_binds_exact_match_profile_persona_and_generator(self):
        generator = FakeGenerator()
        first = draft_input_sha256(
            context(),
            persona_sha256="a" * 64,
            generator=generator,
        )
        selected = context()
        second = draft_input_sha256(
            selected,
            persona_sha256="a" * 64,
            generator=generator,
        )
        # Context ids are intentionally part of the identity.
        self.assertNotEqual(first, second)
        self.assertEqual(len(first), 64)
        self.assertEqual(len(canonical_sha256({"a": 1})), 64)

    def test_input_hash_changes_with_temperature_and_completion_cap(self):
        selected = context()
        baseline = OpenAIReplyDraftGenerator(
            api_key="sk-" + "project-fixture-secret-value-123456789",
            model="gpt-5-nano",
            temperature=0.3,
            max_completion_tokens=700,
        )
        hotter = OpenAIReplyDraftGenerator(
            api_key="sk-" + "project-fixture-secret-value-123456789",
            model="gpt-5-nano",
            temperature=0.4,
            max_completion_tokens=700,
        )
        longer = OpenAIReplyDraftGenerator(
            api_key="sk-" + "project-fixture-secret-value-123456789",
            model="gpt-5-nano",
            temperature=0.3,
            max_completion_tokens=701,
        )
        baseline_hash = draft_input_sha256(
            selected,
            persona_sha256="a" * 64,
            generator=baseline,
        )
        self.assertNotEqual(
            baseline_hash,
            draft_input_sha256(
                selected,
                persona_sha256="a" * 64,
                generator=hotter,
            ),
        )
        self.assertNotEqual(
            baseline_hash,
            draft_input_sha256(
                selected,
                persona_sha256="a" * 64,
                generator=longer,
            ),
        )

    def test_persona_rejects_unknown_fields(self):
        with self.assertRaises(ReplyDraftError) as raised:
            persona_prompt_payload(
                {"name": "Fixture", "private_system_prompt": "ignore rules"}
            )
        self.assertNotIn("private_system_prompt", str(raised.exception))

    def test_internal_context_is_rejected_with_humanized_field_name(self):
        selected = context()
        output = ReplyDraftOutput(
            proposal_draft=(
                "Готов обсудить интеграцию. Внутренний commercial score равен 88."
            ),
            short_reply="Готов обсудить интеграцию Telegram и CRM подробнее.",
            questions_to_client=(),
            risks=(),
        )
        with self.assertRaises(ReplyDraftError) as raised:
            validate_reply_draft_output(output, selected)
        self.assertEqual(
            raised.exception.error_code,
            "reply_draft_internal_context",
        )

    def test_persona_sanitizer_enforces_banned_phrases(self):
        output = ReplyDraftOutput(
            proposal_draft=(
                "Здравствуйте. Я лучший разработчик и предлагаю сначала проверить API."
            ),
            short_reply="Я ЛУЧШИЙ РАЗРАБОТЧИК и начну с API.",
            questions_to_client=("Какой API используется?",),
            risks=(),
        )
        sanitized = sanitize_reply_draft(
            output,
            {"banned_phrases": ["лучший разработчик"]},
        )
        self.assertNotIn("лучший разработчик", sanitized.proposal_draft.casefold())
        self.assertNotIn("лучший разработчик", sanitized.short_reply.casefold())

    def test_openai_payload_is_strict_and_completion_bounded(self):
        generator = OpenAIReplyDraftGenerator(
            api_key="sk-" + "project-fixture-secret-value-123456789",
            model="gpt-5-nano",
            max_completion_tokens=321,
        )
        payload = generator._payload(context(), {"name": "Test"})
        self.assertEqual(payload["max_completion_tokens"], 321)
        self.assertEqual(
            payload["response_format"]["json_schema"]["strict"],
            True,
        )
        self.assertNotIn("temperature", payload)

    def test_groq_payload_uses_strict_schema_and_sampling_temperature(self):
        generator = OpenAICompatibleReplyDraftGenerator(
            api_key="gsk_" + "fixture-secret-value-123456789",
            provider="groq",
            api_key_name="GROQ_API_KEY",
            base_url=GROQ_CHAT_COMPLETIONS_URL,
            model="openai/gpt-oss-20b",
            max_completion_tokens=900,
        )
        payload = generator._payload(context(), {"name": "Test"})
        self.assertEqual(payload["model"], "openai/gpt-oss-20b")
        self.assertEqual(payload["max_completion_tokens"], 900)
        self.assertEqual(payload["temperature"], 0.3)
        self.assertEqual(payload["reasoning_effort"], "low")
        self.assertIs(payload["include_reasoning"], False)
        self.assertEqual(
            payload["response_format"]["json_schema"]["strict"],
            True,
        )

    def test_groq_request_uses_fixed_endpoint_without_secret_in_metadata(self):
        secret = "gsk_" + "fixture-secret-value-123456789"
        generator = OpenAICompatibleReplyDraftGenerator(
            api_key=secret,
            provider="groq",
            api_key_name="GROQ_API_KEY",
            base_url=GROQ_CHAT_COMPLETIONS_URL,
            model="openai/gpt-oss-20b",
            max_completion_tokens=900,
        )
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b"{}"
        with patch("urllib.request.urlopen", return_value=response) as request:
            self.assertEqual(generator._request({"model": generator.model}), "{}")
        prepared = request.call_args.args[0]
        self.assertEqual(prepared.full_url, GROQ_CHAT_COMPLETIONS_URL)
        self.assertEqual(prepared.get_header("Authorization"), f"Bearer {secret}")
        self.assertEqual(prepared.get_header("User-agent"), "AI-Sales-Radar/0.1")
        self.assertNotIn(secret, json.dumps(generator.cache_identity))
        self.assertEqual(generator.cache_identity["provider"], "groq")
        self.assertEqual(generator.cache_identity["model"], "openai/gpt-oss-20b")
        self.assertEqual(generator.cache_identity["reasoning_effort"], "low")
        self.assertIs(generator.cache_identity["include_reasoning"], False)

    def test_groq_rejects_noncanonical_endpoint_before_request(self):
        with self.assertRaises(ValueError):
            OpenAICompatibleReplyDraftGenerator(
                api_key="gsk_" + "fixture-secret-value-123456789",
                provider="groq",
                api_key_name="GROQ_API_KEY",
                base_url="https://example.invalid/v1/chat/completions",
                model="openai/gpt-oss-20b",
                max_completion_tokens=900,
            )

    def test_groq_rejects_model_without_strict_json_schema_contract(self):
        with self.assertRaises(ValueError):
            OpenAICompatibleReplyDraftGenerator(
                api_key="gsk_" + "fixture-secret-value-123456789",
                provider="groq",
                api_key_name="GROQ_API_KEY",
                base_url=GROQ_CHAT_COMPLETIONS_URL,
                model="llama-3.3-70b-versatile",
                max_completion_tokens=900,
            )

    def test_groq_rejects_completion_cap_too_small_for_reasoned_output(self):
        with self.assertRaises(ValueError):
            OpenAICompatibleReplyDraftGenerator(
                api_key="gsk_" + "fixture-secret-value-123456789",
                provider="groq",
                api_key_name="GROQ_API_KEY",
                base_url=GROQ_CHAT_COMPLETIONS_URL,
                model="openai/gpt-oss-20b",
                max_completion_tokens=799,
            )

    def test_http_error_never_echoes_provider_body(self):
        generator = OpenAIReplyDraftGenerator(
            api_key="sk-" + "project-fixture-secret-value-123456789",
            model="gpt-5-nano",
        )
        error = urllib.error.HTTPError(
            "https://api.openai.com/v1/chat/completions",
            429,
            "secret provider body",
            {},
            None,
        )
        with (
            patch("urllib.request.urlopen", side_effect=error),
            self.assertRaises(ReplyDraftError) as raised,
        ):
            generator._request({"model": "gpt-5-nano"})
        self.assertTrue(raised.exception.retryable)
        self.assertNotIn("secret provider body", str(raised.exception))

    async def test_service_reuses_persisted_draft_without_provider_call(self):
        existing = SimpleNamespace(id=uuid4())
        drafts = SimpleNamespace(
            get_by_idempotency_key=AsyncMock(return_value=existing)
        )
        generator = FakeGenerator()
        settings = approved_reply_settings(
            openai_api_key=None,
        )
        service = ReplyDraftService(
            FakeDatabase(),
            settings,
            generator=generator,
            drafts=drafts,
        )
        fixed_context = context()
        service._load_context = AsyncMock(return_value=fixed_context)
        with patch(
            "ai_sales_radar.reply_drafts.load_private_persona",
            return_value={"name": "Fixture"},
        ):
            result = await service.create(fixed_context.inbox_item_id)
        self.assertIs(result, existing)
        self.assertEqual(generator.calls, [])

    async def test_finish_reason_and_refusal_map_to_safe_error_codes(self):
        generator = OpenAIReplyDraftGenerator(
            api_key="sk-" + "project-fixture-secret-value-123456789",
            model="gpt-5-nano",
        )
        valid_content = ReplyDraftOutput(
            proposal_draft=(
                "Готов проверить интеграцию Telegram и CRM на тестовом сценарии."
            ),
            short_reply="Готов проверить связку Telegram и CRM на тесте.",
            questions_to_client=(),
            risks=(),
        ).model_dump_json()
        responses = (
            (
                {
                    "choices": [
                        {
                            "finish_reason": "length",
                            "message": {"content": valid_content, "refusal": None},
                        }
                    ]
                },
                "provider_output_truncated",
            ),
            (
                {
                    "choices": [
                        {
                            "finish_reason": "content_filter",
                            "message": {"content": valid_content, "refusal": None},
                        }
                    ]
                },
                "provider_content_filtered",
            ),
            (
                {
                    "choices": [
                        {
                            "finish_reason": None,
                            "message": {"content": valid_content, "refusal": None},
                        }
                    ]
                },
                "provider_finish_reason_invalid",
            ),
            (
                {
                    "choices": [
                        {
                            "finish_reason": "stop",
                            "message": {
                                "content": valid_content,
                                "refusal": "Unable to comply",
                            },
                        }
                    ]
                },
                "provider_refusal",
            ),
        )
        for response, expected_code in responses:
            with self.subTest(expected_code=expected_code):
                with (
                    patch.object(
                        generator,
                        "_request",
                        return_value=json.dumps(response),
                    ),
                    self.assertRaises(ReplyDraftError) as raised,
                ):
                    await generator.generate(context(), {"name": "Fixture"})
                self.assertEqual(raised.exception.error_code, expected_code)

    async def test_concurrent_create_calls_provider_once_after_lock_recheck(self):
        drafts = MemoryDraftRepository()
        generator = FakeGenerator(delay=0.02)
        generation_lock = FakeGenerationLock()
        service = ReplyDraftService(
            FakeDatabase(),
            approved_reply_settings(openai_api_key=None),
            generator=generator,
            drafts=drafts,
            generation_lock=generation_lock,
        )
        selected = context()
        service._load_context = AsyncMock(return_value=selected)
        with patch(
            "ai_sales_radar.reply_drafts.load_private_persona",
            return_value={"name": "Fixture"},
        ):
            first, second = await asyncio.gather(
                service.create(selected.inbox_item_id),
                service.create(selected.inbox_item_id),
            )
        self.assertIs(first, second)
        self.assertEqual(len(generator.calls), 1)
        self.assertEqual(drafts.ensure_calls, 1)
        self.assertEqual(len(generation_lock.keys), 2)
        self.assertEqual(generation_lock.keys[0], generation_lock.keys[1])

    async def test_stale_full_context_hash_discards_generated_output(self):
        initial = context()
        changed = ReplyDraftContext(
            inbox_item_id=initial.inbox_item_id,
            match_trace_id=initial.match_trace_id,
            opportunity_id=initial.opportunity_id,
            search_profile_id=initial.search_profile_id,
            profile_revision=initial.profile_revision,
            original_message=initial.original_message,
            message_url=initial.message_url,
            opportunity={"summary": "The persisted summary changed"},
            match=initial.match,
            search_profile=initial.search_profile,
        )
        drafts = SimpleNamespace(
            get_by_idempotency_key=AsyncMock(return_value=None),
            ensure=AsyncMock(),
        )
        generator = FakeGenerator()
        service = ReplyDraftService(
            FakeDatabase(),
            approved_reply_settings(openai_api_key=None),
            generator=generator,
            drafts=drafts,
            generation_lock=FakeGenerationLock(),
        )
        service._load_context = AsyncMock(side_effect=(initial, changed))
        with (
            patch(
                "ai_sales_radar.reply_drafts.load_private_persona",
                return_value={"name": "Fixture"},
            ),
            self.assertRaises(ReplyDraftError) as raised,
        ):
            await service.create(initial.inbox_item_id)
        self.assertIn("changed", str(raised.exception).casefold())
        self.assertEqual(len(generator.calls), 1)
        drafts.ensure.assert_not_awaited()


class ReplyDraftConfigurationTest(unittest.TestCase):
    def test_factory_blocks_before_key_adapter_or_network_without_consent(self):
        secret = "gsk_" + "must-never-be-read-or-reported-123456789"
        require_api_key = MagicMock(side_effect=AssertionError(secret))
        settings = SimpleNamespace(
            external_ai_data_processing_approved=False,
            provider="groq",
            model="openai/gpt-oss-20b",
            temperature=0.3,
            timeout_seconds=45,
            max_completion_tokens=900,
            persona_path=Path("freelancer_profile.json"),
            require_api_key=require_api_key,
            api_key_name=MagicMock(return_value="GROQ_API_KEY"),
            chat_completions_url=MagicMock(return_value=GROQ_CHAT_COMPLETIONS_URL),
        )
        service = ReplyDraftService(
            FakeDatabase(),
            settings,
            generation_lock=FakeGenerationLock(),
        )
        with (
            patch(
                "ai_sales_radar.reply_drafts.OpenAICompatibleReplyDraftGenerator"
            ) as adapter,
            patch("urllib.request.urlopen") as provider,
            self.assertRaises(ReplyDraftError) as raised,
        ):
            service._generator_or_configured()
        self.assertEqual(
            raised.exception.error_code,
            "external_ai_data_processing_consent_required",
        )
        self.assertNotIn(secret, str(raised.exception))
        require_api_key.assert_not_called()
        settings.api_key_name.assert_not_called()
        settings.chat_completions_url.assert_not_called()
        adapter.assert_not_called()
        provider.assert_not_called()

    def test_factory_binds_groq_key_to_groq_endpoint_after_consent(self):
        secret = "gsk_" + "fixture-secret-value-123456789"
        settings = SimpleNamespace(
            external_ai_data_processing_approved=True,
            provider="groq",
            model="openai/gpt-oss-20b",
            temperature=0.3,
            timeout_seconds=45,
            max_completion_tokens=900,
            persona_path=Path("freelancer_profile.json"),
            require_api_key=MagicMock(return_value=secret),
            api_key_name=MagicMock(return_value="GROQ_API_KEY"),
            chat_completions_url=MagicMock(return_value=GROQ_CHAT_COMPLETIONS_URL),
        )
        service = ReplyDraftService(
            FakeDatabase(),
            settings,
            generation_lock=FakeGenerationLock(),
        )
        with patch("urllib.request.urlopen") as provider:
            generator = service._generator_or_configured()
        self.assertEqual(generator.provider, "groq")
        self.assertEqual(
            generator.cache_identity["endpoint"],
            GROQ_CHAT_COMPLETIONS_URL,
        )
        self.assertNotIn(secret, json.dumps(generator.cache_identity))
        settings.require_api_key.assert_called_once_with()
        provider.assert_not_called()

    def test_missing_persona_uses_safe_default_inside_project(self):
        settings = approved_reply_settings(
            persona_path="missing-persona.json",
        )
        persona = load_private_persona(settings.persona_path)
        self.assertIn("do_not_claim", persona)

    def test_check_does_not_construct_database_or_call_ai_provider(self):
        database = DatabaseSettings(
            database_url="postgresql+psycopg://local/radar",
            _env_file=None,
        )
        drafts = approved_reply_settings(
            openai_api_key="sk-" + "project-fixture-secret-value-123456789",
        )
        status = inspect_reply_draft_configuration(database, drafts)
        self.assertTrue(status.ai_provider_configured)
        self.assertTrue(status.external_ai_data_processing_approved)
        self.assertEqual(status.provider, "openai")
        self.assertFalse(status.automatic_sending_enabled)

        output = io.StringIO()
        with (
            patch(
                "ai_sales_radar.reply_drafts_cli.DatabaseSettings.from_env",
                return_value=database,
            ),
            patch(
                "ai_sales_radar.reply_drafts_cli.ReplyDraftSettings.from_env",
                return_value=drafts,
            ),
            patch("ai_sales_radar.reply_drafts_cli.Database") as database_adapter,
            patch("urllib.request.urlopen") as provider,
            patch("sys.stdout", output),
        ):
            self.assertEqual(main(["check"]), 0)
        database_adapter.assert_not_called()
        provider.assert_not_called()
        self.assertFalse(json.loads(output.getvalue())["automatic_sending_enabled"])

    def test_check_is_not_ready_without_external_ai_consent(self):
        database = DatabaseSettings(
            database_url="postgresql+psycopg://local/radar",
            _env_file=None,
        )
        drafts = ReplyDraftSettings(
            provider="groq",
            groq_api_key="gsk_" + "fixture-secret-value-123456789",
            _env_file=None,
        )
        if hasattr(drafts, "external_ai_data_processing_approved"):
            self.assertFalse(drafts.external_ai_data_processing_approved)
        status = inspect_reply_draft_configuration(database, drafts)
        self.assertTrue(status.ai_provider_configured)
        self.assertFalse(status.external_ai_data_processing_approved)
        self.assertEqual(status.provider, "groq")

        output = io.StringIO()
        with (
            patch(
                "ai_sales_radar.reply_drafts_cli.DatabaseSettings.from_env",
                return_value=database,
            ),
            patch(
                "ai_sales_radar.reply_drafts_cli.ReplyDraftSettings.from_env",
                return_value=drafts,
            ),
            patch("ai_sales_radar.reply_drafts_cli.Database") as database_adapter,
            patch("urllib.request.urlopen") as provider,
            patch("sys.stdout", output),
        ):
            self.assertEqual(main(["check"]), 1)
        database_adapter.assert_not_called()
        provider.assert_not_called()
        payload = json.loads(output.getvalue())
        self.assertFalse(payload["external_ai_data_processing_approved"])
        self.assertNotIn("openai_configured", payload)


if __name__ == "__main__":
    unittest.main()

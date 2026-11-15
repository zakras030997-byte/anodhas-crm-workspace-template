from __future__ import annotations

from dataclasses import asdict
from decimal import Decimal
import unittest
from unittest.mock import patch

from pydantic import ValidationError

from ai_sales_radar.ai_worker import (
    build_opportunity_analyzer,
    inspect_ai_configuration,
)
from ai_sales_radar.config import (
    ConfigurationError as SalesRadarConfigurationError,
    DatabaseSettings,
    OpportunityAISettings,
    ReplyDraftSettings,
)
from freelancer_bot.config import (
    ConfigurationError as CoreConfigurationError,
    RuntimeConfig,
    RuntimeMode,
)
from freelancer_bot.opportunity_analysis import (
    GROQ_CHAT_COMPLETIONS_URL,
    OpenAICompatibleOpportunityAnalyzer,
    OpportunityAnalysisProviderConfigurationError,
    OpportunityAnalysisProviderUnavailable,
    opportunity_analysis_cache_version,
    resolve_opportunity_analysis_provider,
)
from tests.test_opportunity_analysis import _candidate


GROQ_KEY = "groq-opportunity-test-secret"


class GroqScriptsConfigurationTest(unittest.TestCase):
    def test_groq_defaults_are_safe_versioned_and_selected_key_only(self):
        settings = OpportunityAISettings(
            provider="groq",
            groq_api_key=GROQ_KEY,
            _env_file=None,
        )

        self.assertTrue(settings.configured())
        self.assertFalse(settings.external_ai_data_processing_approved)
        with self.assertRaises(SalesRadarConfigurationError):
            settings.require_external_ai_data_processing_approval()
        self.assertEqual(settings.api_key_name(), "GROQ_API_KEY")
        self.assertEqual(settings.require_api_key(), GROQ_KEY)
        self.assertEqual(settings.chat_completions_url(), GROQ_CHAT_COMPLETIONS_URL)
        self.assertEqual(settings.model, "openai/gpt-oss-20b")
        self.assertEqual(
            settings.pricing_version,
            "groq-gpt-oss-20b-2026-08-27",
        )
        self.assertEqual(settings.input_usd_per_million, Decimal("0.075"))
        self.assertEqual(settings.output_usd_per_million, Decimal("0.30"))
        self.assertEqual(settings.reserve_output_tokens, 800)
        self.assertNotIn(GROQ_KEY, repr(settings))
        self.assertNotIn(GROQ_KEY, repr(settings.model_dump()))

        wrong_key = OpportunityAISettings(
            provider="groq",
            openai_api_key="openai-key-must-not-fallback",
            _env_file=None,
        )
        self.assertFalse(wrong_key.configured())
        with self.assertRaisesRegex(
            SalesRadarConfigurationError,
            "GROQ_API_KEY",
        ) as raised:
            wrong_key.require_api_key()
        self.assertNotIn("openai-key-must-not-fallback", str(raised.exception))

    def test_groq_model_allowlist_and_120b_explicit_pricing(self):
        with self.assertRaises(ValidationError):
            OpportunityAISettings(
                provider="groq",
                groq_api_key=GROQ_KEY,
                model="gpt-5-nano",
                _env_file=None,
            )
        with self.assertRaises(ValidationError):
            OpportunityAISettings(
                provider="groq",
                groq_api_key=GROQ_KEY,
                model="openai/gpt-oss-120b",
                _env_file=None,
            )
        with self.assertRaises(ValidationError):
            OpportunityAISettings(
                provider="groq",
                groq_api_key=GROQ_KEY,
                reserve_output_tokens=799,
                _env_file=None,
            )
        with self.assertRaises(ValidationError):
            OpportunityAISettings(
                provider="groq",
                groq_api_key=GROQ_KEY,
                input_usd_per_million=Decimal("0.05"),
                _env_file=None,
            )

        settings = OpportunityAISettings(
            provider="groq",
            groq_api_key=GROQ_KEY,
            model="openai/gpt-oss-120b",
            pricing_version="groq-gpt-oss-120b-operator.v1",
            input_usd_per_million=Decimal("1"),
            output_usd_per_million=Decimal("2"),
            _env_file=None,
        )

        self.assertEqual(settings.model, "openai/gpt-oss-120b")
        self.assertEqual(settings.pricing_version, "groq-gpt-oss-120b-operator.v1")

    def test_worker_uses_groq_endpoint_price_and_completion_reserve(self):
        captured: dict[str, object] = {}

        def recorder_factory(database, **kwargs):
            captured["database"] = database
            captured["spend_guard"] = kwargs["spend_guard"]
            return "recorder"

        def analyzer_factory(**kwargs):
            captured["analyzer"] = kwargs
            return "analyzer"

        result = build_opportunity_analyzer(
            "database",
            OpportunityAISettings(
                provider="groq",
                groq_api_key=GROQ_KEY,
                external_ai_data_processing_approved=True,
                _env_file=None,
            ),
            recorder_factory=recorder_factory,
            analyzer_factory=analyzer_factory,
        )

        self.assertEqual(result, "analyzer")
        analyzer = captured["analyzer"]
        self.assertEqual(analyzer["provider"], "groq")
        self.assertEqual(analyzer["api_key_name"], "GROQ_API_KEY")
        self.assertEqual(analyzer["base_url"], GROQ_CHAT_COMPLETIONS_URL)
        self.assertEqual(analyzer["model"], "openai/gpt-oss-20b")
        self.assertEqual(analyzer["max_completion_tokens"], 800)
        self.assertEqual(
            analyzer["price"].pricing_version,
            "groq-gpt-oss-20b-2026-08-27",
        )
        self.assertEqual(
            analyzer["price"].input_usd_per_million,
            Decimal("0.075"),
        )

    def test_worker_check_is_provider_neutral(self):
        status = inspect_ai_configuration(
            DatabaseSettings(
                database_url="postgresql+psycopg://local/radar",
                _env_file=None,
            ),
            OpportunityAISettings(
                provider="groq",
                groq_api_key=GROQ_KEY,
                _env_file=None,
            ),
        )
        payload = asdict(status)

        self.assertEqual(payload["provider"], "groq")
        self.assertTrue(payload["provider_configured"])
        self.assertFalse(payload["external_ai_data_processing_approved"])
        self.assertNotIn("openai_configured", payload)

    def test_reply_settings_select_groq_without_openai_key_fallback(self):
        settings = ReplyDraftSettings(
            provider="groq",
            groq_api_key=GROQ_KEY,
            _env_file=None,
        )

        self.assertEqual(settings.model, "openai/gpt-oss-20b")
        self.assertEqual(settings.max_completion_tokens, 800)
        self.assertEqual(settings.api_key_name(), "GROQ_API_KEY")
        self.assertEqual(settings.chat_completions_url(), GROQ_CHAT_COMPLETIONS_URL)
        self.assertEqual(settings.require_api_key(), GROQ_KEY)
        self.assertFalse(settings.external_ai_data_processing_approved)
        with self.assertRaises(SalesRadarConfigurationError):
            settings.require_external_ai_data_processing_approval()
        with self.assertRaises(ValidationError):
            ReplyDraftSettings(
                provider="groq",
                groq_api_key=GROQ_KEY,
                max_completion_tokens=799,
                _env_file=None,
            )

        wrong_key = ReplyDraftSettings(
            provider="groq",
            openai_api_key="openai-key-must-not-fallback",
            _env_file=None,
        )
        self.assertFalse(wrong_key.configured())


class GroqCoreAdapterTest(unittest.TestCase):
    def test_payload_uses_strict_schema_bounded_reasoning_and_cache_identity(self):
        default = OpenAICompatibleOpportunityAnalyzer(
            api_key=GROQ_KEY,
            api_key_name="GROQ_API_KEY",
            model="openai/gpt-oss-20b",
            provider="groq",
        )
        larger_cap = OpenAICompatibleOpportunityAnalyzer(
            api_key=GROQ_KEY,
            api_key_name="GROQ_API_KEY",
            model="openai/gpt-oss-20b",
            provider="groq",
            max_completion_tokens=900,
        )

        payload = default._payload(_candidate())
        self.assertEqual(payload["response_format"]["type"], "json_schema")
        self.assertTrue(payload["response_format"]["json_schema"]["strict"])
        self.assertEqual(payload["reasoning_effort"], "low")
        self.assertFalse(payload["include_reasoning"])
        self.assertEqual(payload["max_completion_tokens"], 800)
        self.assertEqual(default.cache_identity["response_format"], "json_schema")
        self.assertEqual(default.cache_identity["reasoning_effort"], "low")
        self.assertEqual(default.cache_identity["endpoint"], GROQ_CHAT_COMPLETIONS_URL)
        self.assertEqual(default.cache_identity["temperature"], 0.0)
        self.assertNotEqual(
            opportunity_analysis_cache_version(default),
            opportunity_analysis_cache_version(larger_cap),
        )

    def test_endpoint_is_fixed_and_transport_never_calls_network_in_test(self):
        captured: dict[str, object] = {}

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self):
                return b"{}"

        def fake_urlopen(request, timeout):
            captured["request"] = request
            captured["timeout"] = timeout
            return Response()

        analyzer = OpenAICompatibleOpportunityAnalyzer(
            api_key=GROQ_KEY,
            api_key_name="GROQ_API_KEY",
            model="openai/gpt-oss-20b",
            provider="groq",
        )
        with patch(
            "freelancer_bot.opportunity_analysis.urllib.request.urlopen",
            side_effect=fake_urlopen,
        ) as network:
            self.assertEqual(analyzer._request({"model": analyzer.model}), "{}")

        network.assert_called_once()
        request = captured["request"]
        self.assertEqual(request.full_url, GROQ_CHAT_COMPLETIONS_URL)
        self.assertEqual(request.headers["Authorization"], f"Bearer {GROQ_KEY}")
        self.assertEqual(request.headers["User-agent"], "AI-Sales-Radar/0.1")
        with self.assertRaises(OpportunityAnalysisProviderConfigurationError):
            OpenAICompatibleOpportunityAnalyzer(
                api_key=GROQ_KEY,
                model="openai/gpt-oss-20b",
                provider="groq",
                base_url="https://example.invalid/chat/completions",
            )
        with self.assertRaises(OpportunityAnalysisProviderConfigurationError) as cap:
            OpenAICompatibleOpportunityAnalyzer(
                api_key=GROQ_KEY,
                model="openai/gpt-oss-20b",
                provider="groq",
                max_completion_tokens=799,
            )
        self.assertEqual(cap.exception.error_code, "groq_completion_cap_too_small")

    def test_core_resolver_selects_groq_key_without_openai_fallback(self):
        with patch.dict(
            "os.environ",
            {
                "OPPORTUNITY_ANALYSIS_PROVIDER": "groq",
                "GROQ_API_KEY": GROQ_KEY,
            },
            clear=True,
        ):
            config = RuntimeConfig.from_env(
                mode=RuntimeMode.CHECK_CONFIG,
                env_file=None,
            )
        selected = resolve_opportunity_analysis_provider(config)

        self.assertEqual(selected.name, "groq")
        self.assertEqual(selected.api_key_name, "GROQ_API_KEY")
        self.assertEqual(selected.base_url, GROQ_CHAT_COMPLETIONS_URL)
        self.assertEqual(config.opportunity_analysis_model, "openai/gpt-oss-20b")
        self.assertEqual(config.opportunity_analysis_budget_reserve_output_tokens, 800)

        with patch.dict(
            "os.environ",
            {
                "OPPORTUNITY_ANALYSIS_PROVIDER": "groq",
                "OPENAI_API_KEY": "openai-key-must-not-fallback",
            },
            clear=True,
        ):
            missing = RuntimeConfig.from_env(
                mode=RuntimeMode.CHECK_CONFIG,
                env_file=None,
            )
        with self.assertRaises(OpportunityAnalysisProviderUnavailable):
            resolve_opportunity_analysis_provider(missing)

    def test_core_reply_mode_accepts_groq_enum_and_requires_selected_key(self):
        with patch.dict(
            "os.environ",
            {
                "AI_REPLY_PROVIDER": "groq",
                "GROQ_API_KEY": GROQ_KEY,
            },
            clear=True,
        ):
            configured = RuntimeConfig.from_env(
                mode=RuntimeMode.DRAFT_TEXT,
                env_file=None,
            )

        self.assertEqual(configured.ai_provider.value, "groq")
        self.assertEqual(configured.openai_model, "openai/gpt-oss-20b")
        self.assertEqual(configured.ai_reply_provider_credential()[0], "GROQ_API_KEY")

        with patch.dict(
            "os.environ",
            {"AI_REPLY_PROVIDER": "groq"},
            clear=True,
        ):
            with self.assertRaisesRegex(CoreConfigurationError, "GROQ_API_KEY"):
                RuntimeConfig.from_env(
                    mode=RuntimeMode.DRAFT_TEXT,
                    env_file=None,
                )

    def test_core_analyzer_rejects_non_gpt_oss_groq_model(self):
        with self.assertRaises(OpportunityAnalysisProviderConfigurationError) as raised:
            OpenAICompatibleOpportunityAnalyzer(
                api_key=GROQ_KEY,
                model="gpt-5-nano",
                provider="groq",
            )
        self.assertEqual(raised.exception.error_code, "unsupported_groq_model")

    def test_groq_fallback_is_explicitly_unsupported(self):
        with patch.dict(
            "os.environ",
            {"OPPORTUNITY_ANALYSIS_FALLBACK_PROVIDER": "groq"},
            clear=True,
        ):
            with self.assertRaisesRegex(
                CoreConfigurationError,
                "OPPORTUNITY_ANALYSIS_FALLBACK_PROVIDER",
            ):
                RuntimeConfig.from_env(
                    mode=RuntimeMode.CHECK_CONFIG,
                    env_file=None,
                )

        fallback = RuntimeConfig(
            opportunity_analysis_fallback_provider="openai",
            _env_file=None,
        ).model_copy(
            update={
                "opportunity_analysis_fallback_provider": "groq",
                "groq_api_key": GROQ_KEY,
            }
        )
        with self.assertRaises(
            OpportunityAnalysisProviderConfigurationError,
        ) as raised:
            resolve_opportunity_analysis_provider(fallback, fallback=True)
        self.assertEqual(raised.exception.error_code, "unsupported_groq_fallback")


if __name__ == "__main__":
    unittest.main()

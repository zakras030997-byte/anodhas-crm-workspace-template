"""Feature-scoped settings for the scripts-only AI Sales Radar runtime."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
import re
from typing import Self

from pydantic import (
    AliasChoices,
    Field,
    SecretStr,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

from freelancer_bot.opportunity_analysis import (
    DEEPSEEK_CHAT_COMPLETIONS_URL,
    DEFAULT_GROQ_MAX_COMPLETION_TOKENS,
    DEFAULT_GROQ_OPPORTUNITY_MODEL,
    GROQ_CHAT_COMPLETIONS_URL,
    GROQ_GPT_OSS_20B_INPUT_USD_PER_MILLION,
    GROQ_GPT_OSS_20B_OUTPUT_USD_PER_MILLION,
    GROQ_GPT_OSS_20B_PRICING_VERSION,
    GROQ_OPPORTUNITY_MODELS,
    OPENAI_CHAT_COMPLETIONS_URL,
    SUPPORTED_OPPORTUNITY_AI_PROVIDERS,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = PROJECT_ROOT / ".env"
LOCAL_RUNTIME_ROOT = PROJECT_ROOT / ".local"


class ConfigurationError(RuntimeError):
    """A safe configuration error that never includes secret input values."""


@dataclass(frozen=True)
class TelegramCredentials:
    api_id: int
    api_hash: str = field(repr=False)
    session_path: Path


@dataclass(frozen=True)
class OperatorBotCredentials:
    api_id: int
    api_hash: str = field(repr=False)
    bot_token: str = field(repr=False)
    bot_id: int
    owner_user_id: int
    owner_chat_id: int
    session_path: Path


class _ProjectSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=DEFAULT_ENV_FILE,
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        extra="ignore",
        frozen=True,
        hide_input_in_errors=True,
        populate_by_name=True,
    )

    @classmethod
    def from_env(cls, env_file: str | Path | None = DEFAULT_ENV_FILE) -> Self:
        try:
            return cls(_env_file=env_file, _env_file_encoding="utf-8")
        except ValidationError as exc:
            raise ConfigurationError(_safe_validation_message(exc)) from None


class DatabaseSettings(_ProjectSettings):
    database_url: SecretStr | None = Field(
        default=None,
        validation_alias="DATABASE_URL",
    )

    def configured(self) -> bool:
        return bool(
            self.database_url
            and self.database_url.get_secret_value().strip()
        )

    def require_url(self) -> str:
        if not self.configured():
            raise ConfigurationError("Не задан DATABASE_URL в .env")
        value = self.database_url.get_secret_value().strip()  # type: ignore[union-attr]
        if not value.startswith("postgresql+psycopg://"):
            raise ConfigurationError(
                "DATABASE_URL должен начинаться с postgresql+psycopg://"
            )
        return value


class TelegramAuthSettings(_ProjectSettings):
    api_id: int | None = Field(
        default=None,
        validation_alias=AliasChoices("TELEGRAM_API_ID", "API_ID"),
        gt=0,
    )
    api_hash: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices("TELEGRAM_API_HASH", "API_HASH"),
    )
    session_path: Path = Field(
        default=Path(".local/telegram/collector.session"),
        validation_alias="AI_SALES_RADAR_SESSION_PATH",
    )

    @field_validator("session_path")
    @classmethod
    def validate_session_path(cls, value: Path) -> Path:
        return canonical_project_session_path(value)

    def configured(self) -> bool:
        return bool(
            self.api_id
            and self.api_hash
            and self.api_hash.get_secret_value().strip()
        )

    def require_credentials(self) -> TelegramCredentials:
        missing: list[str] = []
        if self.api_id is None:
            missing.append("TELEGRAM_API_ID")
        if self.api_hash is None or not self.api_hash.get_secret_value().strip():
            missing.append("TELEGRAM_API_HASH")
        if missing:
            raise ConfigurationError(
                "Не заданы обязательные настройки Telegram: " + ", ".join(missing)
            )
        return TelegramCredentials(
            api_id=self.api_id,
            api_hash=self.api_hash.get_secret_value().strip(),
            session_path=self.session_path,
        )


class OperatorBotSettings(_ProjectSettings):
    """Fail-closed settings for the private, bot-only operator UI."""

    enabled: bool = Field(
        default=False,
        validation_alias="AI_SALES_RADAR_OPERATOR_BOT_ENABLED",
    )
    api_id: int | None = Field(
        default=None,
        validation_alias=AliasChoices("TELEGRAM_API_ID", "API_ID"),
        gt=0,
    )
    api_hash: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices("TELEGRAM_API_HASH", "API_HASH"),
    )
    bot_token: SecretStr | None = Field(
        default=None,
        validation_alias="AI_SALES_RADAR_OPERATOR_BOT_TOKEN",
    )
    owner_user_id: int | None = Field(
        default=None,
        validation_alias="AI_SALES_RADAR_OPERATOR_OWNER_USER_ID",
        gt=0,
    )
    owner_chat_id: int | None = Field(
        default=None,
        validation_alias="AI_SALES_RADAR_OPERATOR_OWNER_CHAT_ID",
        gt=0,
    )
    session_path: Path = Field(
        default=Path(".local/telegram/operator_bot.session"),
        validation_alias="AI_SALES_RADAR_OPERATOR_BOT_SESSION_PATH",
    )
    collector_session_path: Path = Field(
        default=Path(".local/telegram/collector.session"),
        validation_alias="AI_SALES_RADAR_SESSION_PATH",
        exclude=True,
    )
    callback_ttl_seconds: int = Field(
        default=900,
        validation_alias="AI_SALES_RADAR_OPERATOR_CALLBACK_TTL_SECONDS",
        ge=60,
        le=3_600,
    )
    max_leads_per_command: int = Field(
        default=5,
        validation_alias="AI_SALES_RADAR_OPERATOR_MAX_LEADS",
        ge=1,
        le=10,
    )
    reads_per_minute: int = Field(
        default=30,
        validation_alias="AI_SALES_RADAR_OPERATOR_READS_PER_MINUTE",
        ge=1,
        le=120,
    )
    mutations_per_minute: int = Field(
        default=10,
        validation_alias="AI_SALES_RADAR_OPERATOR_MUTATIONS_PER_MINUTE",
        ge=1,
        le=60,
    )
    drafts_per_hour: int = Field(
        default=5,
        validation_alias="AI_SALES_RADAR_OPERATOR_DRAFTS_PER_HOUR",
        ge=1,
        le=20,
    )
    notifications_enabled: bool = Field(
        default=False,
        validation_alias="AI_SALES_RADAR_OPERATOR_NOTIFICATIONS_ENABLED",
    )

    @field_validator("bot_token")
    @classmethod
    def validate_bot_token(cls, value: SecretStr | None) -> SecretStr | None:
        if value is None:
            return None
        selected = value.get_secret_value().strip()
        if not selected:
            return None
        if re.fullmatch(
            r"[1-9][0-9]{4,19}:[A-Za-z0-9_-]{20,128}",
            selected,
        ) is None:
            raise ValueError("некорректный token operator bot")
        return SecretStr(selected)

    @field_validator("session_path", "collector_session_path")
    @classmethod
    def validate_session_paths(cls, value: Path) -> Path:
        return canonical_project_session_path(value)

    @model_validator(mode="after")
    def validate_private_binding_and_session_isolation(self) -> Self:
        if self.notifications_enabled:
            raise ValueError(
                "proactive operator bot notifications ещё не поддерживаются"
            )
        if (
            self.owner_user_id is not None
            and self.owner_chat_id is not None
            and self.owner_user_id != self.owner_chat_id
        ):
            raise ValueError(
                "operator bot поддерживает только личный чат владельца"
            )
        if self.session_path == self.collector_session_path:
            raise ValueError(
                "operator bot и Telegram collector должны иметь разные сессии"
            )
        return self

    def telegram_api_configured(self) -> bool:
        return bool(
            self.api_id
            and self.api_hash
            and self.api_hash.get_secret_value().strip()
        )

    def bot_token_configured(self) -> bool:
        return bool(
            self.bot_token
            and self.bot_token.get_secret_value().strip()
        )

    def owner_configured(self) -> bool:
        return bool(
            self.owner_user_id
            and self.owner_chat_id
            and self.owner_user_id == self.owner_chat_id
        )

    def configured(self) -> bool:
        return (
            self.enabled
            and self.telegram_api_configured()
            and self.bot_token_configured()
            and self.owner_configured()
        )

    def require_credentials(self) -> OperatorBotCredentials:
        if not self.enabled:
            raise ConfigurationError(
                "Operator bot выключен: задайте "
                "AI_SALES_RADAR_OPERATOR_BOT_ENABLED=true"
            )
        missing: list[str] = []
        if self.api_id is None:
            missing.append("TELEGRAM_API_ID")
        if self.api_hash is None or not self.api_hash.get_secret_value().strip():
            missing.append("TELEGRAM_API_HASH")
        if self.bot_token is None or not self.bot_token.get_secret_value().strip():
            missing.append("AI_SALES_RADAR_OPERATOR_BOT_TOKEN")
        if self.owner_user_id is None:
            missing.append("AI_SALES_RADAR_OPERATOR_OWNER_USER_ID")
        if self.owner_chat_id is None:
            missing.append("AI_SALES_RADAR_OPERATOR_OWNER_CHAT_ID")
        if missing:
            raise ConfigurationError(
                "Не заданы обязательные настройки operator bot: "
                + ", ".join(missing)
            )

        token = self.bot_token.get_secret_value().strip()  # type: ignore[union-attr]
        match = re.fullmatch(
            r"([1-9][0-9]{4,19}):[A-Za-z0-9_-]{20,128}",
            token,
        )
        if match is None:
            raise ConfigurationError("Некорректный token operator bot")
        if self.owner_user_id != self.owner_chat_id:
            raise ConfigurationError(
                "Operator bot поддерживает только личный чат владельца"
            )
        return OperatorBotCredentials(
            api_id=self.api_id,  # type: ignore[arg-type]
            api_hash=self.api_hash.get_secret_value().strip(),  # type: ignore[union-attr]
            bot_token=token,
            bot_id=int(match.group(1)),
            owner_user_id=self.owner_user_id,  # type: ignore[arg-type]
            owner_chat_id=self.owner_chat_id,  # type: ignore[arg-type]
            session_path=self.session_path,
        )


class OperatorSourceDiscoverySettings(_ProjectSettings):
    """Bounded manual Telegram source discovery started only by the owner."""

    enabled: bool = Field(
        default=True,
        validation_alias="AI_SALES_RADAR_SOURCE_DISCOVERY_ENABLED",
    )
    minimum_participants: int = Field(
        default=500,
        validation_alias="AI_SALES_RADAR_SOURCE_DISCOVERY_MIN_PARTICIPANTS",
        ge=100,
        le=10_000_000,
    )
    max_queries: int = Field(
        default=12,
        validation_alias="AI_SALES_RADAR_SOURCE_DISCOVERY_MAX_QUERIES",
        ge=4,
        le=20,
    )
    results_per_query: int = Field(
        default=15,
        validation_alias="AI_SALES_RADAR_SOURCE_DISCOVERY_RESULTS_PER_QUERY",
        ge=5,
        le=50,
    )
    max_candidates: int = Field(
        default=10,
        validation_alias="AI_SALES_RADAR_SOURCE_DISCOVERY_MAX_CANDIDATES",
        ge=1,
        le=20,
    )
    participant_lookup_limit: int = Field(
        default=30,
        validation_alias="AI_SALES_RADAR_SOURCE_DISCOVERY_LOOKUP_LIMIT",
        ge=1,
        le=50,
    )
    cooldown_seconds: int = Field(
        default=1_800,
        validation_alias="AI_SALES_RADAR_SOURCE_DISCOVERY_COOLDOWN_SECONDS",
        ge=300,
        le=86_400,
    )


class AISettings(_ProjectSettings):
    openai_api_key: SecretStr | None = Field(
        default=None,
        validation_alias="OPENAI_API_KEY",
    )

    def configured(self) -> bool:
        return bool(
            self.openai_api_key
            and self.openai_api_key.get_secret_value().strip()
        )


class OpportunityAISettings(_ProjectSettings):
    """Provider-scoped settings used only by opportunity extraction scripts."""

    provider: str = Field(
        default="openai",
        validation_alias="OPPORTUNITY_ANALYSIS_PROVIDER",
        min_length=1,
        max_length=64,
    )
    external_ai_data_processing_approved: bool = Field(
        default=False,
        validation_alias="EXTERNAL_AI_DATA_PROCESSING_APPROVED",
    )

    openai_api_key: SecretStr | None = Field(
        default=None,
        validation_alias="OPENAI_API_KEY",
    )
    deepseek_api_key: SecretStr | None = Field(
        default=None,
        validation_alias="DEEPSEEK_API_KEY",
    )
    groq_api_key: SecretStr | None = Field(
        default=None,
        validation_alias="GROQ_API_KEY",
    )
    tokenrouter_api_key: SecretStr | None = Field(
        default=None,
        validation_alias="TOKENROUTER_API_KEY",
    )
    tokenrouter_base_url: str = Field(
        default="https://api.tokenrouter.com/v1",
        validation_alias="TOKENROUTER_BASE_URL",
        min_length=1,
        max_length=2048,
    )
    model: str = Field(
        default="gpt-5-nano",
        validation_alias="OPPORTUNITY_ANALYSIS_MODEL",
        min_length=1,
        max_length=128,
    )
    temperature: float = Field(
        default=0.0,
        validation_alias="OPPORTUNITY_ANALYSIS_TEMPERATURE",
        ge=0,
        le=2,
    )
    timeout_seconds: int = Field(
        default=45,
        validation_alias="OPPORTUNITY_ANALYSIS_TIMEOUT_SECONDS",
        gt=0,
    )
    max_output_attempts: int = Field(
        default=2,
        validation_alias="OPPORTUNITY_ANALYSIS_MAX_OUTPUT_ATTEMPTS",
        ge=1,
        le=5,
    )
    pricing_version: str = Field(
        default="openai-gpt5-2025-08-07",
        validation_alias="OPPORTUNITY_ANALYSIS_PRICING_VERSION",
        pattern=r"^[a-z0-9][a-z0-9._-]{0,99}$",
    )
    input_usd_per_million: Decimal = Field(
        default=Decimal("0.05"),
        validation_alias="OPPORTUNITY_ANALYSIS_INPUT_USD_PER_MILLION",
        ge=0,
    )
    output_usd_per_million: Decimal = Field(
        default=Decimal("0.40"),
        validation_alias="OPPORTUNITY_ANALYSIS_OUTPUT_USD_PER_MILLION",
        ge=0,
    )
    daily_spend_limit_usd: Decimal | None = Field(
        default=Decimal("1.00"),
        validation_alias="OPPORTUNITY_ANALYSIS_DAILY_SPEND_LIMIT_USD",
        ge=0,
    )
    monthly_spend_limit_usd: Decimal | None = Field(
        default=Decimal("10.00"),
        validation_alias="OPPORTUNITY_ANALYSIS_MONTHLY_SPEND_LIMIT_USD",
        ge=0,
    )
    reserve_input_tokens: int = Field(
        default=1_000,
        validation_alias="OPPORTUNITY_ANALYSIS_BUDGET_RESERVE_INPUT_TOKENS",
        ge=0,
    )
    reserve_output_tokens: int = Field(
        default=300,
        validation_alias="OPPORTUNITY_ANALYSIS_BUDGET_RESERVE_OUTPUT_TOKENS",
        gt=0,
    )

    @field_validator("provider")
    @classmethod
    def normalize_provider(cls, value: str) -> str:
        selected = value.strip().lower()
        if selected not in SUPPORTED_OPPORTUNITY_AI_PROVIDERS:
            raise ValueError("неподдерживаемый Opportunity Analysis provider")
        return selected

    @field_validator("model")
    @classmethod
    def normalize_model(cls, value: str) -> str:
        selected = value.strip()
        if not selected:
            raise ValueError("модель Opportunity Analysis не должна быть пустой")
        return selected

    @model_validator(mode="after")
    def apply_provider_defaults(self) -> Self:
        if self.provider != "groq":
            return self

        explicitly_configured = self.model_fields_set
        if "model" not in explicitly_configured:
            object.__setattr__(self, "model", DEFAULT_GROQ_OPPORTUNITY_MODEL)
        if self.model not in GROQ_OPPORTUNITY_MODELS:
            raise ValueError(
                "для Groq разрешены только openai/gpt-oss-20b и "
                "openai/gpt-oss-120b"
            )
        if "reserve_output_tokens" not in explicitly_configured:
            object.__setattr__(
                self,
                "reserve_output_tokens",
                DEFAULT_GROQ_MAX_COMPLETION_TOKENS,
            )
        if self.reserve_output_tokens < DEFAULT_GROQ_MAX_COMPLETION_TOKENS:
            raise ValueError(
                "для Groq reserve_output_tokens должен быть не меньше 800"
            )

        if self.model == DEFAULT_GROQ_OPPORTUNITY_MODEL:
            defaults = {
                "pricing_version": GROQ_GPT_OSS_20B_PRICING_VERSION,
                "input_usd_per_million": (
                    GROQ_GPT_OSS_20B_INPUT_USD_PER_MILLION
                ),
                "output_usd_per_million": (
                    GROQ_GPT_OSS_20B_OUTPUT_USD_PER_MILLION
                ),
            }
            for field_name, default in defaults.items():
                if field_name not in explicitly_configured:
                    object.__setattr__(self, field_name, default)
            if (
                self.pricing_version != GROQ_GPT_OSS_20B_PRICING_VERSION
                or self.input_usd_per_million
                != GROQ_GPT_OSS_20B_INPUT_USD_PER_MILLION
                or self.output_usd_per_million
                != GROQ_GPT_OSS_20B_OUTPUT_USD_PER_MILLION
            ):
                raise ValueError(
                    "цены Groq openai/gpt-oss-20b должны соответствовать "
                    "зафиксированной версии"
                )
        else:
            required_pricing = {
                "pricing_version",
                "input_usd_per_million",
                "output_usd_per_million",
            }
            if not required_pricing.issubset(explicitly_configured):
                raise ValueError(
                    "openai/gpt-oss-120b требует явно задать pricing version и цены"
                )
            if (
                self.pricing_version == GROQ_GPT_OSS_20B_PRICING_VERSION
                or self.input_usd_per_million <= 0
                or self.output_usd_per_million <= 0
            ):
                raise ValueError(
                    "openai/gpt-oss-120b требует отдельную положительную цену"
                )
        return self

    def api_key_name(self) -> str:
        return {
            "openai": "OPENAI_API_KEY",
            "deepseek": "DEEPSEEK_API_KEY",
            "groq": "GROQ_API_KEY",
            "tokenrouter": "TOKENROUTER_API_KEY",
        }[self.provider]

    def chat_completions_url(self) -> str:
        if self.provider == "openai":
            return OPENAI_CHAT_COMPLETIONS_URL
        if self.provider == "deepseek":
            return DEEPSEEK_CHAT_COMPLETIONS_URL
        if self.provider == "groq":
            return GROQ_CHAT_COMPLETIONS_URL
        normalized = self.tokenrouter_base_url.strip().rstrip("/")
        if normalized.endswith("/chat/completions"):
            return normalized
        return f"{normalized}/chat/completions"

    def _selected_api_key(self) -> SecretStr | None:
        return {
            "openai": self.openai_api_key,
            "deepseek": self.deepseek_api_key,
            "groq": self.groq_api_key,
            "tokenrouter": self.tokenrouter_api_key,
        }[self.provider]

    def configured(self) -> bool:
        selected = self._selected_api_key()
        return bool(
            selected
            and selected.get_secret_value().strip()
        )

    def require_api_key(self) -> str:
        if not self.configured():
            raise ConfigurationError(
                f"Не задан {self.api_key_name()} для provider={self.provider}"
            )
        selected = self._selected_api_key()
        return selected.get_secret_value().strip()  # type: ignore[union-attr]

    def require_external_ai_data_processing_approval(self) -> None:
        if not self.external_ai_data_processing_approved:
            raise ConfigurationError(
                "Для внешнего AI требуется явно задать "
                "EXTERNAL_AI_DATA_PROCESSING_APPROVED=true"
            )


class ReplyDraftSettings(_ProjectSettings):
    provider: str = Field(
        default="openai",
        validation_alias="AI_REPLY_PROVIDER",
        min_length=1,
        max_length=64,
    )
    external_ai_data_processing_approved: bool = Field(
        default=False,
        validation_alias="EXTERNAL_AI_DATA_PROCESSING_APPROVED",
    )
    openai_api_key: SecretStr | None = Field(
        default=None,
        validation_alias="OPENAI_API_KEY",
    )
    groq_api_key: SecretStr | None = Field(
        default=None,
        validation_alias="GROQ_API_KEY",
    )
    model: str = Field(
        default="gpt-5-nano",
        validation_alias="AI_REPLY_MODEL",
        min_length=1,
        max_length=128,
    )
    temperature: float = Field(
        default=0.3,
        validation_alias="AI_REPLY_TEMPERATURE",
        ge=0,
        le=2,
    )
    timeout_seconds: int = Field(
        default=45,
        validation_alias="AI_REQUEST_TIMEOUT_SECONDS",
        gt=0,
    )
    max_completion_tokens: int = Field(
        default=700,
        validation_alias="AI_REPLY_MAX_COMPLETION_TOKENS",
        gt=0,
        le=4_000,
    )
    persona_path: Path = Field(
        default=Path("freelancer_profile.json"),
        validation_alias="FREELANCER_PROFILE_PATH",
    )

    @field_validator("provider")
    @classmethod
    def normalize_provider(cls, value: str) -> str:
        selected = value.strip().lower()
        if selected not in {"openai", "groq"}:
            raise ValueError("AI Reply поддерживает только openai или groq")
        return selected

    @field_validator("model")
    @classmethod
    def normalize_model(cls, value: str) -> str:
        selected = value.strip()
        if not selected:
            raise ValueError("модель AI Reply не должна быть пустой")
        return selected

    @field_validator("persona_path")
    @classmethod
    def validate_persona_path(cls, value: Path) -> Path:
        candidate = value.expanduser()
        if not candidate.is_absolute():
            candidate = PROJECT_ROOT / candidate
        resolved = candidate.resolve(strict=False)
        if not resolved.is_relative_to(PROJECT_ROOT.resolve(strict=True)):
            raise ValueError("профиль фрилансера должен находиться внутри проекта")
        if resolved.suffix.lower() != ".json":
            raise ValueError("профиль фрилансера должен быть JSON-файлом")
        return resolved

    @model_validator(mode="after")
    def apply_provider_defaults(self) -> Self:
        if self.provider != "groq":
            return self
        explicitly_configured = self.model_fields_set
        if "model" not in explicitly_configured:
            object.__setattr__(self, "model", DEFAULT_GROQ_OPPORTUNITY_MODEL)
        if self.model not in GROQ_OPPORTUNITY_MODELS:
            raise ValueError(
                "для Groq AI Reply разрешены только openai/gpt-oss-20b и "
                "openai/gpt-oss-120b"
            )
        if "max_completion_tokens" not in explicitly_configured:
            object.__setattr__(
                self,
                "max_completion_tokens",
                DEFAULT_GROQ_MAX_COMPLETION_TOKENS,
            )
        if self.max_completion_tokens < DEFAULT_GROQ_MAX_COMPLETION_TOKENS:
            raise ValueError(
                "для Groq AI Reply max_completion_tokens должен быть не меньше 800"
            )
        return self

    def api_key_name(self) -> str:
        return "GROQ_API_KEY" if self.provider == "groq" else "OPENAI_API_KEY"

    def chat_completions_url(self) -> str:
        return (
            GROQ_CHAT_COMPLETIONS_URL
            if self.provider == "groq"
            else OPENAI_CHAT_COMPLETIONS_URL
        )

    def _selected_api_key(self) -> SecretStr | None:
        return self.groq_api_key if self.provider == "groq" else self.openai_api_key

    def configured(self) -> bool:
        selected = self._selected_api_key()
        return bool(
            selected
            and selected.get_secret_value().strip()
        )

    def require_api_key(self) -> str:
        if not self.configured():
            raise ConfigurationError(
                f"Не задан {self.api_key_name()} для AI Reply provider={self.provider}"
            )
        selected = self._selected_api_key()
        return selected.get_secret_value().strip()  # type: ignore[union-attr]

    def require_external_ai_data_processing_approval(self) -> None:
        if not self.external_ai_data_processing_approved:
            raise ConfigurationError(
                "Для внешнего AI требуется явно задать "
                "EXTERNAL_AI_DATA_PROCESSING_APPROVED=true"
            )


def canonical_project_session_path(value: Path) -> Path:
    """Return one canonical `.session` path below the private `.local` tree."""
    project_root = PROJECT_ROOT.resolve(strict=True)
    nominal_local_root = project_root / ".local"
    if nominal_local_root.is_symlink():
        raise ValueError("каталог .local не должен быть символической ссылкой")
    local_root = nominal_local_root.resolve(strict=False)
    if local_root != nominal_local_root:
        raise ValueError("каталог .local выходит за пределы проекта")

    candidate = value.expanduser()
    if candidate.suffix and candidate.suffix != ".session":
        raise ValueError("путь Telegram-сессии должен быть без суффикса или с .session")
    if candidate.suffix != ".session":
        candidate = Path(f"{candidate}.session")
    if not candidate.is_absolute():
        candidate = project_root / candidate
    resolved = candidate.resolve(strict=False)
    if not resolved.is_relative_to(local_root):
        raise ValueError("Telegram-сессия должна находиться внутри .local проекта")
    return resolved


def _safe_validation_message(error: ValidationError) -> str:
    issues: list[str] = []
    for item in error.errors(
        include_url=False,
        include_context=False,
        include_input=False,
    ):
        location = ".".join(str(part) for part in item.get("loc", ()))
        issues.append(f"{location or 'configuration'}: {item.get('msg', 'invalid value')}")
    return "Некорректная конфигурация: " + "; ".join(issues)

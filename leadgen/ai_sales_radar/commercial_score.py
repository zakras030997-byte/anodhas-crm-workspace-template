"""Pure, deterministic Commercial Score v1 calculation.

The module deliberately accepts already-normalized matching signals.  It does
not read configuration, persistence, Telegram state, or the current clock, so
the same semantic input always produces the same score and input hash.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from hashlib import sha256
import json
import math
import unicodedata
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


COMMERCIAL_SCORE_POLICY_VERSION = "commercial-score.v1"

_RELEVANCE_WEIGHT = Decimal("35")
_BUDGET_WEIGHT = Decimal("15")
_ACTIONABILITY_WEIGHT = Decimal("10")
_SOURCE_QUALITY_WEIGHT = Decimal("10")
_UNKNOWN_SOURCE_QUALITY_POINTS = Decimal("5")
_RED_FLAG_POINTS = 5
_MAX_RED_FLAG_PENALTY = 20


class CommercialIntent(str, Enum):
    ACTIVE = "active"
    RECOMMENDATION = "recommendation"
    RESEARCH = "research"
    WEAK = "weak"
    NONE = "none"


class CommercialUrgency(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    NORMAL = "normal"
    UNKNOWN = "unknown"


class Contactability(str, Enum):
    DIRECT_CONTACT = "direct_contact"
    SOURCE_ONLY = "source_only"
    NONE = "none"


class CommercialTier(str, Enum):
    HOT = "HOT"
    WARM = "WARM"
    LOW = "LOW"


_INTENT_POINTS: dict[CommercialIntent, Decimal] = {
    CommercialIntent.ACTIVE: Decimal("15"),
    CommercialIntent.RECOMMENDATION: Decimal("10"),
    CommercialIntent.RESEARCH: Decimal("5"),
    CommercialIntent.WEAK: Decimal("2"),
    CommercialIntent.NONE: Decimal("0"),
}

_URGENCY_POINTS: dict[CommercialUrgency, Decimal] = {
    CommercialUrgency.CRITICAL: Decimal("10"),
    CommercialUrgency.HIGH: Decimal("8"),
    CommercialUrgency.NORMAL: Decimal("3"),
    CommercialUrgency.UNKNOWN: Decimal("0"),
}

_CONTACTABILITY_POINTS: dict[Contactability, Decimal] = {
    Contactability.DIRECT_CONTACT: Decimal("5"),
    Contactability.SOURCE_ONLY: Decimal("2"),
    Contactability.NONE: Decimal("0"),
}


class _StrictModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )


class CommercialScoreInput(_StrictModel):
    """Normalized signals required by Commercial Score v1.

    ``budget_component`` is the existing normalized budget match component.
    ``source_quality`` is the normalized score derived from the latest source
    quality snapshot; ``None`` means that no snapshot is known.
    """

    relevance: float = Field(ge=0, le=1)
    intent: CommercialIntent
    budget_component: float = Field(ge=0, le=1)
    urgency: CommercialUrgency
    actionability: float = Field(ge=0, le=1)
    contactability: Contactability
    source_quality: float | None = Field(default=None, ge=0, le=1)
    red_flags: tuple[str, ...] = ()

    @field_validator(
        "relevance",
        "budget_component",
        "actionability",
        "source_quality",
    )
    @classmethod
    def validate_finite_signal(cls, value: float | None) -> float | None:
        if value is not None and not math.isfinite(value):
            raise ValueError("normalized score signals must be finite")
        return value

    @field_validator("red_flags")
    @classmethod
    def validate_red_flags(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if len(values) > 64:
            raise ValueError("red_flags is bounded to 64 values")
        normalized: list[str] = []
        for value in values:
            label = " ".join(unicodedata.normalize("NFKC", value).split())
            if not label:
                raise ValueError("red flags cannot be empty")
            if len(label) > 200:
                raise ValueError("red flags are bounded to 200 characters")
            normalized.append(label)
        return tuple(normalized)


class CommercialScoreBreakdown(_StrictModel):
    relevance_points: Decimal
    intent_points: Decimal
    budget_points: Decimal
    urgency_points: Decimal
    actionability_points: Decimal
    contactability_points: Decimal
    source_quality_points: Decimal
    red_flag_count: int = Field(ge=0)
    red_flag_penalty: Decimal = Field(ge=0, le=_MAX_RED_FLAG_PENALTY)
    gross_score: Decimal = Field(ge=0, le=100)
    score_before_rounding: Decimal = Field(ge=0, le=100)


class CommercialScore(_StrictModel):
    policy_version: Literal["commercial-score.v1"]
    input_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    score: int = Field(ge=0, le=100)
    tier: CommercialTier
    breakdown: CommercialScoreBreakdown


def calculate_commercial_score(score_input: CommercialScoreInput) -> CommercialScore:
    """Calculate Commercial Score v1 without side effects."""

    relevance_points = _decimal(score_input.relevance) * _RELEVANCE_WEIGHT
    intent_points = _INTENT_POINTS[score_input.intent]
    budget_points = _decimal(score_input.budget_component) * _BUDGET_WEIGHT
    urgency_points = _URGENCY_POINTS[score_input.urgency]
    actionability_points = (
        _decimal(score_input.actionability) * _ACTIONABILITY_WEIGHT
    )
    contactability_points = _CONTACTABILITY_POINTS[score_input.contactability]
    source_quality_points = (
        _UNKNOWN_SOURCE_QUALITY_POINTS
        if score_input.source_quality is None
        else _decimal(score_input.source_quality) * _SOURCE_QUALITY_WEIGHT
    )

    distinct_red_flags = _distinct_red_flags(score_input.red_flags)
    red_flag_penalty = Decimal(
        min(
            _MAX_RED_FLAG_PENALTY,
            len(distinct_red_flags) * _RED_FLAG_POINTS,
        )
    )
    gross_score = sum(
        (
            relevance_points,
            intent_points,
            budget_points,
            urgency_points,
            actionability_points,
            contactability_points,
            source_quality_points,
        ),
        Decimal("0"),
    )
    score_before_rounding = _clamp(gross_score - red_flag_penalty)
    rounded_score = int(
        score_before_rounding.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    )

    return CommercialScore(
        policy_version=COMMERCIAL_SCORE_POLICY_VERSION,
        input_hash=_input_hash(score_input, distinct_red_flags),
        score=rounded_score,
        tier=_tier(rounded_score),
        breakdown=CommercialScoreBreakdown(
            relevance_points=relevance_points,
            intent_points=intent_points,
            budget_points=budget_points,
            urgency_points=urgency_points,
            actionability_points=actionability_points,
            contactability_points=contactability_points,
            source_quality_points=source_quality_points,
            red_flag_count=len(distinct_red_flags),
            red_flag_penalty=red_flag_penalty,
            gross_score=gross_score,
            score_before_rounding=score_before_rounding,
        ),
    )


def _decimal(value: float) -> Decimal:
    return Decimal(str(value))


def _clamp(value: Decimal) -> Decimal:
    return max(Decimal("0"), min(Decimal("100"), value))


def _tier(score: int) -> CommercialTier:
    if score >= 80:
        return CommercialTier.HOT
    if score >= 60:
        return CommercialTier.WARM
    return CommercialTier.LOW


def _distinct_red_flags(red_flags: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(sorted({value.casefold() for value in red_flags}))


def _canonical_decimal(value: float) -> str:
    decimal_value = _decimal(value).normalize()
    if decimal_value == 0:
        return "0"
    return format(decimal_value, "f")


def _input_hash(
    score_input: CommercialScoreInput,
    distinct_red_flags: tuple[str, ...],
) -> str:
    payload = {
        "actionability": _canonical_decimal(score_input.actionability),
        "budget_component": _canonical_decimal(score_input.budget_component),
        "contactability": score_input.contactability.value,
        "intent": score_input.intent.value,
        "policy_version": COMMERCIAL_SCORE_POLICY_VERSION,
        "red_flags": distinct_red_flags,
        "relevance": _canonical_decimal(score_input.relevance),
        "source_quality": (
            None
            if score_input.source_quality is None
            else _canonical_decimal(score_input.source_quality)
        ),
        "urgency": score_input.urgency.value,
    }
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(serialized).hexdigest()


__all__ = [
    "COMMERCIAL_SCORE_POLICY_VERSION",
    "CommercialIntent",
    "CommercialScore",
    "CommercialScoreBreakdown",
    "CommercialScoreInput",
    "CommercialTier",
    "CommercialUrgency",
    "Contactability",
    "calculate_commercial_score",
]

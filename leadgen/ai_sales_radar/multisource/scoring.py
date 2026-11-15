"""Pure, deterministic qualification score for non-Telegram leads."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from hashlib import sha256
import json
import unicodedata
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


MULTISOURCE_SCORE_POLICY_VERSION = "multisource-qualification-score.v2"

_WEIGHTS: tuple[tuple[str, Decimal], ...] = (
    ("company_fit", Decimal("25")),
    ("pain_strength", Decimal("25")),
    ("timing_readiness", Decimal("20")),
    ("decision_maker_access", Decimal("15")),
    ("evidence_quality", Decimal("15")),
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )


class ScoreFactor(_StrictModel):
    """One normalized factor and the evidence supporting it.

    ``None`` is unknown and contributes zero points.  A positive value cannot
    be used without at least one evidence identifier.
    """

    value: Decimal | None = Field(default=None, ge=Decimal("0"), le=Decimal("1"))
    evidence_ids: tuple[UUID, ...] = Field(default=(), max_length=64)
    reason_codes: tuple[str, ...] = Field(default=(), max_length=32)

    @field_validator("value")
    @classmethod
    def validate_finite_value(cls, value: Decimal | None) -> Decimal | None:
        if value is not None and not value.is_finite():
            raise ValueError("score factors must be finite")
        return value

    @field_validator("evidence_ids")
    @classmethod
    def normalize_evidence_ids(
        cls,
        values: tuple[UUID, ...],
    ) -> tuple[UUID, ...]:
        if len(set(values)) != len(values):
            raise ValueError("factor evidence_ids must not contain duplicates")
        return tuple(sorted(values, key=str))

    @field_validator("reason_codes")
    @classmethod
    def normalize_reason_codes(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        normalized: set[str] = set()
        for value in values:
            code = "_".join(
                unicodedata.normalize("NFKC", value).casefold().split()
            )
            if not code or len(code) > 100:
                raise ValueError("reason codes must be non-empty and bounded")
            normalized.add(code)
        return tuple(sorted(normalized))

    @model_validator(mode="after")
    def require_evidence_for_positive_value(self) -> "ScoreFactor":
        if self.value is not None and self.value > 0 and not self.evidence_ids:
            raise ValueError("positive score factors require evidence_ids")
        if (self.value is None or self.value == 0) and self.evidence_ids:
            raise ValueError("zero or unknown score factors cannot cite evidence")
        return self


class QualificationScoreInputV2(_StrictModel):
    company_fit: ScoreFactor
    pain_strength: ScoreFactor
    timing_readiness: ScoreFactor
    decision_maker_access: ScoreFactor
    evidence_quality: ScoreFactor


class QualificationBreakdownV2(_StrictModel):
    company_fit_points: Decimal = Field(ge=0, le=25)
    pain_strength_points: Decimal = Field(ge=0, le=25)
    timing_readiness_points: Decimal = Field(ge=0, le=20)
    decision_maker_access_points: Decimal = Field(ge=0, le=15)
    evidence_quality_points: Decimal = Field(ge=0, le=15)
    score_before_rounding: Decimal = Field(ge=0, le=100)
    unknown_factors: tuple[str, ...]


class QualificationScoreV2(_StrictModel):
    policy_version: Literal["multisource-qualification-score.v2"]
    input_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    total: int = Field(ge=0, le=100)
    breakdown: QualificationBreakdownV2


def calculate_qualification_score_v2(
    score_input: QualificationScoreInputV2,
) -> QualificationScoreV2:
    """Calculate the fixed 25/25/20/15/15 policy without side effects."""

    points: dict[str, Decimal] = {}
    unknown: list[str] = []
    for field_name, weight in _WEIGHTS:
        factor = getattr(score_input, field_name)
        if factor.value is None:
            unknown.append(field_name)
            normalized_value = Decimal("0")
        else:
            normalized_value = factor.value
        points[field_name] = normalized_value * weight

    raw_total = sum(points.values(), Decimal("0"))
    rounded_total = int(raw_total.quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    return QualificationScoreV2(
        policy_version=MULTISOURCE_SCORE_POLICY_VERSION,
        input_hash=_input_hash(score_input),
        total=rounded_total,
        breakdown=QualificationBreakdownV2(
            company_fit_points=points["company_fit"],
            pain_strength_points=points["pain_strength"],
            timing_readiness_points=points["timing_readiness"],
            decision_maker_access_points=points["decision_maker_access"],
            evidence_quality_points=points["evidence_quality"],
            score_before_rounding=raw_total,
            unknown_factors=tuple(unknown),
        ),
    )


def score_factor_evidence_ids(
    score_input: QualificationScoreInputV2,
) -> tuple[UUID, ...]:
    values = {
        evidence_id
        for field_name, _weight in _WEIGHTS
        for evidence_id in getattr(score_input, field_name).evidence_ids
    }
    return tuple(sorted(values, key=str))


def _input_hash(score_input: QualificationScoreInputV2) -> str:
    payload: dict[str, object] = {
        "policy_version": MULTISOURCE_SCORE_POLICY_VERSION,
    }
    for field_name, _weight in _WEIGHTS:
        factor = getattr(score_input, field_name)
        payload[field_name] = {
            "value": None if factor.value is None else _canonical_decimal(factor.value),
            "evidence_ids": tuple(str(item) for item in factor.evidence_ids),
            "reason_codes": factor.reason_codes,
        }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _canonical_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == 0:
        return "0"
    return format(normalized, "f")

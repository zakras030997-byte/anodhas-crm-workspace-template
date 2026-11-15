from __future__ import annotations

from decimal import Decimal
import unittest
from uuid import UUID

from pydantic import ValidationError

from ai_sales_radar.multisource.scoring import (
    MULTISOURCE_SCORE_POLICY_VERSION,
    QualificationScoreInputV2,
    ScoreFactor,
    calculate_qualification_score_v2,
)


EVIDENCE_A = UUID("00000000-0000-0000-0000-000000000001")
EVIDENCE_B = UUID("00000000-0000-0000-0000-000000000002")


class MultisourceScoringTest(unittest.TestCase):
    def test_perfect_score_has_exact_25_25_20_15_15_breakdown(self):
        result = calculate_qualification_score_v2(self._perfect_input())

        self.assertEqual(
            result.policy_version,
            "multisource-qualification-score.v2",
        )
        self.assertEqual(result.policy_version, MULTISOURCE_SCORE_POLICY_VERSION)
        self.assertEqual(result.total, 100)
        self.assertEqual(result.breakdown.company_fit_points, Decimal("25"))
        self.assertEqual(result.breakdown.pain_strength_points, Decimal("25"))
        self.assertEqual(result.breakdown.timing_readiness_points, Decimal("20"))
        self.assertEqual(
            result.breakdown.decision_maker_access_points,
            Decimal("15"),
        )
        self.assertEqual(result.breakdown.evidence_quality_points, Decimal("15"))

    def test_unknown_is_zero_not_a_neutral_bonus(self):
        unknown = ScoreFactor()
        result = calculate_qualification_score_v2(
            QualificationScoreInputV2(
                company_fit=unknown,
                pain_strength=unknown,
                timing_readiness=unknown,
                decision_maker_access=unknown,
                evidence_quality=unknown,
            )
        )

        self.assertEqual(result.total, 0)
        self.assertEqual(
            result.breakdown.unknown_factors,
            (
                "company_fit",
                "pain_strength",
                "timing_readiness",
                "decision_maker_access",
                "evidence_quality",
            ),
        )

    def test_rounding_is_half_up(self):
        zero = ScoreFactor(value=Decimal("0"))
        result = calculate_qualification_score_v2(
            QualificationScoreInputV2(
                company_fit=ScoreFactor(
                    value=Decimal("0.02"),
                    evidence_ids=(EVIDENCE_A,),
                ),
                pain_strength=zero,
                timing_readiness=zero,
                decision_maker_access=zero,
                evidence_quality=zero,
            )
        )

        self.assertEqual(result.breakdown.score_before_rounding, Decimal("0.50"))
        self.assertEqual(result.total, 1)

    def test_hash_is_deterministic_for_semantic_factor_order(self):
        first = calculate_qualification_score_v2(
            self._perfect_input(
                evidence_ids=(EVIDENCE_B, EVIDENCE_A),
                reason_codes=(" Verified Fit ", "demand"),
            )
        )
        second = calculate_qualification_score_v2(
            self._perfect_input(
                evidence_ids=(EVIDENCE_A, EVIDENCE_B),
                reason_codes=("DEMAND", "verified_fit"),
            )
        )

        self.assertEqual(first.input_hash, second.input_hash)
        self.assertEqual(len(first.input_hash), 64)

    def test_positive_factor_requires_evidence_and_bounds_are_strict(self):
        with self.assertRaises(ValidationError):
            ScoreFactor(value=Decimal("0.1"))
        with self.assertRaises(ValidationError):
            ScoreFactor(value=Decimal("1.01"), evidence_ids=(EVIDENCE_A,))
        with self.assertRaises(ValidationError):
            ScoreFactor.model_validate(
                {"value": Decimal("0.5"), "unexpected": True}
            )

    def test_zero_or_unknown_factor_cannot_smuggle_evidence(self):
        with self.assertRaises(ValidationError):
            ScoreFactor(value=Decimal("0"), evidence_ids=(EVIDENCE_A,))
        with self.assertRaises(ValidationError):
            ScoreFactor(value=None, evidence_ids=(EVIDENCE_A,))

    @staticmethod
    def _perfect_input(
        *,
        evidence_ids: tuple[UUID, ...] = (EVIDENCE_A,),
        reason_codes: tuple[str, ...] = ("verified",),
    ) -> QualificationScoreInputV2:
        factor = ScoreFactor(
            value=Decimal("1"),
            evidence_ids=evidence_ids,
            reason_codes=reason_codes,
        )
        return QualificationScoreInputV2(
            company_fit=factor,
            pain_strength=factor,
            timing_readiness=factor,
            decision_maker_access=factor,
            evidence_quality=factor,
        )


if __name__ == "__main__":
    unittest.main()

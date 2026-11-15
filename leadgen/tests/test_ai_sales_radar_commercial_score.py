from __future__ import annotations

from decimal import Decimal
import unittest

from pydantic import ValidationError

from ai_sales_radar.commercial_score import (
    COMMERCIAL_SCORE_POLICY_VERSION,
    CommercialIntent,
    CommercialScoreInput,
    CommercialTier,
    CommercialUrgency,
    Contactability,
    calculate_commercial_score,
)


class CommercialScoreTest(unittest.TestCase):
    def test_perfect_signals_have_fixed_v1_breakdown(self):
        result = calculate_commercial_score(
            self._input(
                relevance=1.0,
                intent=CommercialIntent.ACTIVE,
                budget_component=1.0,
                urgency=CommercialUrgency.CRITICAL,
                actionability=1.0,
                contactability=Contactability.DIRECT_CONTACT,
                source_quality=1.0,
            )
        )

        self.assertEqual(result.policy_version, COMMERCIAL_SCORE_POLICY_VERSION)
        self.assertEqual(result.score, 100)
        self.assertEqual(result.tier, CommercialTier.HOT)
        self.assertEqual(result.breakdown.relevance_points, Decimal("35.0"))
        self.assertEqual(result.breakdown.intent_points, Decimal("15"))
        self.assertEqual(result.breakdown.budget_points, Decimal("15.0"))
        self.assertEqual(result.breakdown.urgency_points, Decimal("10"))
        self.assertEqual(result.breakdown.actionability_points, Decimal("10.0"))
        self.assertEqual(result.breakdown.contactability_points, Decimal("5"))
        self.assertEqual(result.breakdown.source_quality_points, Decimal("10.0"))
        self.assertEqual(result.breakdown.red_flag_penalty, Decimal("0"))

    def test_categorical_points_follow_fixed_policy(self):
        intent_points = {
            CommercialIntent.ACTIVE: Decimal("15"),
            CommercialIntent.RECOMMENDATION: Decimal("10"),
            CommercialIntent.RESEARCH: Decimal("5"),
            CommercialIntent.WEAK: Decimal("2"),
            CommercialIntent.NONE: Decimal("0"),
        }
        for intent, expected in intent_points.items():
            with self.subTest(intent=intent):
                result = calculate_commercial_score(self._input(intent=intent))
                self.assertEqual(result.breakdown.intent_points, expected)

        urgency_points = {
            CommercialUrgency.CRITICAL: Decimal("10"),
            CommercialUrgency.HIGH: Decimal("8"),
            CommercialUrgency.NORMAL: Decimal("3"),
            CommercialUrgency.UNKNOWN: Decimal("0"),
        }
        for urgency, expected in urgency_points.items():
            with self.subTest(urgency=urgency):
                result = calculate_commercial_score(self._input(urgency=urgency))
                self.assertEqual(result.breakdown.urgency_points, expected)

        contact_points = {
            Contactability.DIRECT_CONTACT: Decimal("5"),
            Contactability.SOURCE_ONLY: Decimal("2"),
            Contactability.NONE: Decimal("0"),
        }
        for contactability, expected in contact_points.items():
            with self.subTest(contactability=contactability):
                result = calculate_commercial_score(
                    self._input(contactability=contactability)
                )
                self.assertEqual(result.breakdown.contactability_points, expected)

    def test_unknown_source_quality_is_neutral_not_zero(self):
        unknown = calculate_commercial_score(self._input(source_quality=None))
        known_zero = calculate_commercial_score(self._input(source_quality=0.0))

        self.assertEqual(unknown.breakdown.source_quality_points, Decimal("5"))
        self.assertEqual(known_zero.breakdown.source_quality_points, Decimal("0.0"))

    def test_distinct_red_flags_cost_five_points_and_cap_at_twenty(self):
        result = calculate_commercial_score(
            self._input(
                relevance=1.0,
                intent=CommercialIntent.ACTIVE,
                budget_component=1.0,
                urgency=CommercialUrgency.CRITICAL,
                actionability=1.0,
                contactability=Contactability.DIRECT_CONTACT,
                source_quality=1.0,
                red_flags=(
                    " Spam ",
                    "spam",
                    "SCAM",
                    "unpaid",
                    "reseller",
                    "vague",
                ),
            )
        )

        self.assertEqual(result.breakdown.red_flag_count, 5)
        self.assertEqual(result.breakdown.red_flag_penalty, Decimal("20"))
        self.assertEqual(result.score, 80)
        self.assertEqual(result.tier, CommercialTier.HOT)

    def test_tier_boundaries_use_the_rounded_integer_score(self):
        hot = calculate_commercial_score(
            self._input(
                relevance=0.5,
                intent=CommercialIntent.ACTIVE,
                budget_component=1.0,
                urgency=CommercialUrgency.CRITICAL,
                actionability=1.0,
                contactability=Contactability.DIRECT_CONTACT,
                source_quality=0.75,
            )
        )
        warm = calculate_commercial_score(
            self._input(
                relevance=0.5,
                intent=CommercialIntent.ACTIVE,
                budget_component=1.0,
                urgency=CommercialUrgency.UNKNOWN,
                actionability=0.5,
                contactability=Contactability.NONE,
                source_quality=0.75,
            )
        )
        low = calculate_commercial_score(
            self._input(
                relevance=0.5,
                intent=CommercialIntent.ACTIVE,
                budget_component=1.0,
                urgency=CommercialUrgency.UNKNOWN,
                actionability=0.4,
                contactability=Contactability.NONE,
                source_quality=0.75,
            )
        )

        self.assertEqual((hot.score, hot.tier), (80, CommercialTier.HOT))
        self.assertEqual((warm.score, warm.tier), (60, CommercialTier.WARM))
        self.assertEqual((low.score, low.tier), (59, CommercialTier.LOW))

    def test_rounding_is_half_up_and_score_is_clamped(self):
        rounded = calculate_commercial_score(
            self._input(relevance=0.1, source_quality=None)
        )
        clamped = calculate_commercial_score(
            self._input(red_flags=("one", "two", "three", "four"))
        )

        self.assertEqual(rounded.breakdown.score_before_rounding, Decimal("8.5"))
        self.assertEqual(rounded.score, 9)
        self.assertEqual(clamped.breakdown.score_before_rounding, Decimal("0"))
        self.assertEqual(clamped.score, 0)

    def test_hash_and_breakdown_are_deterministic_for_semantic_input(self):
        first = calculate_commercial_score(
            self._input(red_flags=(" Spam ", "SCAM", "spam"))
        )
        second = calculate_commercial_score(
            self._input(red_flags=("scam", "SPAM"))
        )
        changed = calculate_commercial_score(
            self._input(actionability=0.1, red_flags=("scam", "spam"))
        )

        self.assertEqual(first, second)
        self.assertEqual(len(first.input_hash), 64)
        self.assertNotEqual(first.input_hash, changed.input_hash)

    def test_input_contract_is_strict_and_bounded(self):
        with self.assertRaises(ValidationError):
            CommercialScoreInput.model_validate(
                {
                    **self._input().model_dump(),
                    "relevance": "0.5",
                }
            )
        with self.assertRaises(ValidationError):
            self._input(relevance=1.01)
        with self.assertRaises(ValidationError):
            CommercialScoreInput.model_validate(
                {
                    **self._input().model_dump(),
                    "unexpected": True,
                }
            )
        with self.assertRaises(ValidationError):
            self._input(red_flags=(" ",))

    @staticmethod
    def _input(**overrides) -> CommercialScoreInput:
        values = {
            "relevance": 0.0,
            "intent": CommercialIntent.NONE,
            "budget_component": 0.0,
            "urgency": CommercialUrgency.UNKNOWN,
            "actionability": 0.0,
            "contactability": Contactability.NONE,
            "source_quality": None,
            "red_flags": (),
        }
        values.update(overrides)
        return CommercialScoreInput(**values)


if __name__ == "__main__":
    unittest.main()

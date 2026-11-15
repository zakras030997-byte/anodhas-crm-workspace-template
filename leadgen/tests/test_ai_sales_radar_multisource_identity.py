from __future__ import annotations

import unittest

from ai_sales_radar.multisource.identity import (
    IdentifierKind,
    is_valid_inn,
    is_valid_ogrn,
    normalize_domain,
    normalize_identifier,
    provisional_candidate_key,
    select_dedupe_key,
)


class MultisourceIdentityTest(unittest.TestCase):
    def test_russian_registry_identifiers_validate_checksums(self):
        self.assertTrue(is_valid_inn("7707083893"))
        self.assertTrue(is_valid_ogrn("1027700132195"))
        self.assertEqual(
            normalize_identifier(IdentifierKind.INN, "7707 083-893"),
            "7707083893",
        )

        self.assertFalse(is_valid_inn("7707083894"))
        self.assertFalse(is_valid_ogrn("1027700132194"))
        with self.assertRaises(ValueError):
            normalize_identifier(IdentifierKind.INN, "7707083894")

    def test_domain_is_cross_source_normalized(self):
        self.assertEqual(
            normalize_domain("https://WWW.пример.рф/services?utm=1"),
            "xn--e1afmkfd.xn--p1ai",
        )
        with self.assertRaises(ValueError):
            normalize_domain("https://user:contact011@example.invalid")

    def test_dedupe_prefers_inn_and_marks_provider_ids_provisional(self):
        durable_key, durable = select_dedupe_key(
            (
                (IdentifierKind.DOMAIN, "example.ru"),
                (IdentifierKind.INN, "7707083893"),
                (IdentifierKind.TWO_GIS_ID, "branch-42"),
            )
        )
        provider_key, provider_durable = select_dedupe_key(
            ((IdentifierKind.TWO_GIS_ID, "branch-42"),)
        )

        self.assertEqual(durable_key, "inn:7707083893")
        self.assertTrue(durable)
        self.assertEqual(provider_key, "two_gis_id:branch-42")
        self.assertFalse(provider_durable)

    def test_name_address_key_is_only_a_stable_comparison_hash(self):
        first = provisional_candidate_key(" Кофейня № 1 ", "Невский 1")
        second = provisional_candidate_key("кофейня № 1", "  Невский   1 ")

        self.assertEqual(first, second)
        self.assertTrue(first.startswith("candidate:"))

    def test_conflicting_identifiers_never_use_last_value_wins(self):
        with self.assertRaises(ValueError):
            select_dedupe_key(
                (
                    (IdentifierKind.INN, "7707083893"),
                    (IdentifierKind.INN, "7736050003"),
                )
            )

    def test_branch_provider_ids_do_not_override_a_single_legal_identity(self):
        key, durable = select_dedupe_key(
            (
                (IdentifierKind.INN, "7707083893"),
                (IdentifierKind.TWO_GIS_ID, "branch-1"),
                (IdentifierKind.TWO_GIS_ID, "branch-2"),
            )
        )

        self.assertEqual(key, "inn:7707083893")
        self.assertTrue(durable)


if __name__ == "__main__":
    unittest.main()

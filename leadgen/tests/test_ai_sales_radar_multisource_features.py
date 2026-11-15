from __future__ import annotations

import unittest

from pydantic import ValidationError

from ai_sales_radar.multisource.features import MultisourceFeatureFlags


class MultisourceFeatureFlagsTest(unittest.TestCase):
    def test_every_active_capability_is_off_by_default(self):
        flags = MultisourceFeatureFlags()
        values = flags.model_dump()

        self.assertTrue(flags.dry_run_only)
        for field_name, value in values.items():
            if field_name.endswith("enabled") or field_name == "persistence_enabled":
                self.assertFalse(value, field_name)

    def test_child_flags_require_explicit_master_switch(self):
        with self.assertRaises(ValidationError):
            MultisourceFeatureFlags(two_gis_discovery_enabled=True)

        flags = MultisourceFeatureFlags(
            enabled=True,
            two_gis_discovery_enabled=True,
        )
        effective = flags.effective()
        self.assertTrue(effective.enabled)
        self.assertTrue(effective.two_gis_discovery_enabled)

    def test_foundation_cannot_enable_writes_or_outreach(self):
        for field_name in (
            "persistence_enabled",
            "sheets_write_enabled",
            "automatic_outreach_enabled",
        ):
            with self.subTest(field_name=field_name):
                with self.assertRaises(ValidationError):
                    MultisourceFeatureFlags.model_validate({field_name: True})

        with self.assertRaises(ValidationError):
            MultisourceFeatureFlags.model_validate({"dry_run_only": False})


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""Inspect a multisource feature plan; this command never executes adapters."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pydantic import ValidationError  # noqa: E402

from ai_sales_radar.multisource.dry_run import build_dry_run_plan  # noqa: E402
from ai_sales_radar.multisource.features import MultisourceFeatureFlags  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect a default-off multisource dry-run plan. "
            "No network call or write sink is available in this command."
        )
    )
    parser.add_argument("--enable", action="store_true", help="enable master flag in plan")
    parser.add_argument("--yandex", action="store_true", help="plan Yandex API discovery")
    parser.add_argument("--two-gis", action="store_true", help="plan 2GIS API discovery")
    parser.add_argument("--fns", action="store_true", help="plan FNS record verification")
    parser.add_argument(
        "--website",
        action="store_true",
        help="plan an explicit-domain official website read",
    )
    parser.add_argument(
        "--qualification",
        action="store_true",
        help="plan local conservative qualification preview",
    )
    parser.add_argument(
        "--registry-preview",
        action="store_true",
        help="plan a local registry operation preview without a write",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        flags = MultisourceFeatureFlags(
            enabled=args.enable,
            yandex_maps_discovery_enabled=args.yandex,
            two_gis_discovery_enabled=args.two_gis,
            fns_verification_enabled=args.fns,
            company_website_research_enabled=args.website,
            qualification_enabled=args.qualification,
            registry_preview_enabled=args.registry_preview,
        )
    except ValidationError as error:
        parser.error(str(error))
    plan = build_dry_run_plan(flags)
    output = {
        "schema_version": 1,
        "mode": "inspection_only",
        "network_executed": False,
        "writes_executed": False,
        "flags": flags.model_dump(mode="json"),
        "plan": plan.model_dump(mode="json"),
    }
    print(json.dumps(output, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import ast
from datetime import datetime, timezone
from hashlib import sha256
import inspect
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest

from ai_sales_radar.multisource.adapters import (
    AdapterReceipt,
    DirectoryAdapterResult,
    DirectorySearchQuery,
    ProviderName,
    TransientBusinessObservation,
    TransientObservationPage,
    TransientWebsitePage,
    WebsiteReadRequest,
    WebsiteReadResult,
)
from ai_sales_radar.multisource.dry_run import (
    DryRunRequest,
    MultisourceDryRun,
    build_dry_run_plan,
)
from ai_sales_radar.multisource.features import MultisourceFeatureFlags


NOW = datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DRY_RUN_PATH = PROJECT_ROOT / "ai_sales_radar" / "multisource" / "dry_run.py"
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "multisource_preview.py"


class FakeDirectoryAdapter:
    def __init__(self) -> None:
        self.calls = 0

    async def discover(self, query, *, budget, collected_at):
        self.calls += 1
        observation = TransientBusinessObservation(
            provider=ProviderName.YANDEX_MAPS,
            name="Кофейня Тест",
            category="Кофейня",
            address="Санкт-Петербург, Невский проспект, 1",
            website_url="https://coffee.example.org/",
            public_phones=("+70000000002",),
            source_url="https://search-maps.yandex.ru/v1/",
            collected_at=collected_at,
            content_sha256="a" * 64,
        )
        return DirectoryAdapterResult(
            pages=(
                TransientObservationPage(
                    provider=ProviderName.YANDEX_MAPS,
                    observations=(observation,),
                ),
            ),
            receipt=AdapterReceipt(
                provider=ProviderName.YANDEX_MAPS,
                endpoint="https://search-maps.yandex.ru/v1/",
                collected_at=collected_at,
                request_count=1,
                status_codes=(200,),
                content_sha256=("a" * 64,),
                completed=True,
                outcome="completed",
            ),
        )


class FakeWebsiteAdapter:
    def __init__(self) -> None:
        self.calls = 0

    async def read(self, request, *, budget, collected_at):
        self.calls += 1
        body = "ephemeral page body that must not serialize"
        digest = sha256(body.encode()).hexdigest()
        return WebsiteReadResult(
            page=TransientWebsitePage(
                url=request.url,
                content_type="text/html",
                collected_at=collected_at,
                content_sha256=digest,
                content_length=len(body.encode()),
                text=body,
            ),
            receipt=AdapterReceipt(
                provider=ProviderName.OFFICIAL_WEBSITE,
                endpoint=request.url,
                collected_at=collected_at,
                request_count=1,
                status_codes=(200,),
                content_sha256=(digest,),
                completed=True,
                outcome="completed",
            ),
        )


class FailIfCalledAdapter:
    def __init__(self) -> None:
        self.calls = 0

    def __getattr__(self, name):
        async def fail(*args, **kwargs):
            self.calls += 1
            raise AssertionError(f"disabled adapter method {name} was called")

        return fail


class MultisourceDryRunTest(unittest.IsolatedAsyncioTestCase):
    async def test_default_flags_return_before_every_adapter_call(self):
        adapters = [FailIfCalledAdapter() for _ in range(4)]
        runner = MultisourceDryRun(
            yandex_maps=adapters[0],
            two_gis=adapters[1],
            fns=adapters[2],
            official_website=adapters[3],
        )
        preview = await runner.run(
            DryRunRequest(
                directory_query=_query(),
                website_requests=(
                    WebsiteReadRequest(
                        url="https://example.org/",
                        official_domain="example.org",
                    ),
                ),
            ),
            collected_at=NOW,
        )

        self.assertFalse(preview.enabled)
        self.assertEqual([item.calls for item in adapters], [0, 0, 0, 0])
        self.assertEqual(preview.candidates, ())
        self.assertEqual(preview.receipts, ())
        self.assertFalse(preview.persistence_performed)
        self.assertFalse(preview.sheets_write_performed)
        self.assertFalse(preview.outreach_performed)

    async def test_enabled_dry_run_returns_ephemeral_previews_only(self):
        directory = FakeDirectoryAdapter()
        website = FakeWebsiteAdapter()
        flags = MultisourceFeatureFlags(
            enabled=True,
            yandex_maps_discovery_enabled=True,
            company_website_research_enabled=True,
            qualification_enabled=True,
            registry_preview_enabled=True,
        )
        preview = await MultisourceDryRun(
            yandex_maps=directory,
            official_website=website,
        ).run(
            DryRunRequest(
                flags=flags,
                directory_query=_query(),
                website_requests=(
                    WebsiteReadRequest(
                        url="https://example.org/",
                        official_domain="example.org",
                    ),
                ),
            ),
            collected_at=NOW,
        )

        self.assertTrue(preview.enabled)
        self.assertEqual(directory.calls, 1)
        self.assertEqual(website.calls, 1)
        self.assertEqual(len(preview.candidates), 1)
        self.assertEqual(len(preview.evidence), 2)
        self.assertEqual(len(preview.qualification), 1)
        self.assertEqual(len(preview.registry_preview), 1)
        self.assertFalse(preview.registry_preview[0].write_performed)
        serialized = preview.model_dump_json()
        self.assertNotIn("ephemeral page body", serialized)
        self.assertNotIn("provider_place_id", serialized)
        self.assertFalse(preview.persistence_performed)
        self.assertFalse(preview.sheets_write_performed)
        self.assertFalse(preview.outreach_performed)

    async def test_master_on_with_all_children_off_still_calls_nothing(self):
        adapter = FailIfCalledAdapter()
        preview = await MultisourceDryRun(yandex_maps=adapter).run(
            DryRunRequest(flags=MultisourceFeatureFlags(enabled=True)),
            collected_at=NOW,
        )
        self.assertTrue(preview.enabled)
        self.assertEqual(adapter.calls, 0)

    def test_plan_is_inspection_only_and_never_offers_write_sink(self):
        plan = build_dry_run_plan(MultisourceFeatureFlags())
        self.assertFalse(plan.master_enabled)
        self.assertEqual(plan.enabled_sources, ())
        self.assertEqual(plan.default_execution, "inspect_only")
        self.assertFalse(plan.write_sinks_available)


class DryRunIsolationTest(unittest.TestCase):
    def test_constructor_cannot_accept_database_sheets_or_outreach_sink(self):
        parameters = set(inspect.signature(MultisourceDryRun).parameters)
        self.assertEqual(
            parameters,
            {"yandex_maps", "two_gis", "fns", "official_website"},
        )

    def test_dry_run_module_imports_no_runtime_or_write_side_library(self):
        tree = ast.parse(DRY_RUN_PATH.read_text(encoding="utf-8"))
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.add(node.module or "")
        forbidden = {
            "ai_sales_radar.runtime",
            "freelancer_bot",
            "googleapiclient",
            "gspread",
            "psycopg",
            "sqlalchemy",
            "telethon",
        }
        self.assertFalse(
            any(
                item == root or item.startswith(f"{root}.")
                for item in imported
                for root in forbidden
            ),
            imported,
        )

    def test_preview_script_default_is_off_and_executes_no_network_or_write(self):
        completed = subprocess.run(
            [sys.executable, str(SCRIPT_PATH)],
            cwd=PROJECT_ROOT,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        payload = json.loads(completed.stdout)
        self.assertFalse(payload["flags"]["enabled"])
        self.assertEqual(payload["mode"], "inspection_only")
        self.assertFalse(payload["network_executed"])
        self.assertFalse(payload["writes_executed"])


def _query() -> DirectorySearchQuery:
    return DirectorySearchQuery(
        text="кофейня",
        city="Санкт-Петербург",
        limit=1,
        page_size=1,
    )


if __name__ == "__main__":
    unittest.main()

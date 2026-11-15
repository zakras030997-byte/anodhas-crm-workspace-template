from __future__ import annotations

import ast
from hashlib import sha256
import os
from pathlib import Path
import subprocess
import sys
import unittest

from ai_sales_radar.bootstrap import expected_migration_heads
from ai_sales_radar.commercial_score import COMMERCIAL_SCORE_POLICY_VERSION
from ai_sales_radar.config import (
    DatabaseSettings,
    OpportunityAISettings,
    TelegramAuthSettings,
)
from ai_sales_radar.runtime import inspect_runtime_configuration


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MULTISOURCE_ROOT = PROJECT_ROOT / "ai_sales_radar" / "multisource"
TELEGRAM_ENTRYPOINTS = (
    PROJECT_ROOT / "ai_sales_radar" / "collector.py",
    PROJECT_ROOT / "ai_sales_radar" / "runtime.py",
    PROJECT_ROOT / "freelancer_bot" / "collector_only.py",
    PROJECT_ROOT / "freelancer_bot" / "app.py",
    PROJECT_ROOT / "scripts" / "collector.py",
)
DEDICATED_MULTISOURCE_ENTRYPOINTS = (
    PROJECT_ROOT / "scripts" / "multisource_preview.py",
)


class MultisourceCompatibilityTest(unittest.TestCase):
    def test_existing_telegram_entrypoints_do_not_import_multisource(self):
        for path in TELEGRAM_ENTRYPOINTS:
            with self.subTest(path=path.relative_to(PROJECT_ROOT)):
                imports = _imports(path)
                self.assertFalse(
                    any(_is_multisource_import(item) for item in imports),
                    imports,
                )

    def test_no_existing_python_module_is_wired_to_multisource(self):
        violations: list[str] = []
        for path in PROJECT_ROOT.rglob("*.py"):
            relative = path.relative_to(PROJECT_ROOT)
            if (
                path in DEDICATED_MULTISOURCE_ENTRYPOINTS
                or
                "multisource" in relative.parts
                or "tests" in relative.parts
                or any(part.startswith(".") for part in relative.parts)
                or "__pycache__" in relative.parts
            ):
                continue
            if any(_is_multisource_import(item) for item in _imports(path)):
                violations.append(str(relative))

        self.assertEqual(violations, [])

    def test_existing_runtime_import_graph_does_not_load_multisource(self):
        modules = (
            "ai_sales_radar.collector",
            "ai_sales_radar.runtime",
            "freelancer_bot.collector_only",
            "freelancer_bot.app",
        )
        script = (
            "import importlib,sys; "
            "importlib.import_module(sys.argv[1]); "
            "bad=sorted(k for k in sys.modules if "
            "k == 'ai_sales_radar.multisource' or "
            "k.startswith('ai_sales_radar.multisource.')); "
            "raise SystemExit('loaded: '+','.join(bad) if bad else 0)"
        )
        environment = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
        for module in modules:
            with self.subTest(module=module):
                completed = subprocess.run(
                    [sys.executable, "-c", script, module],
                    cwd=PROJECT_ROOT,
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=30,
                    check=False,
                )
                self.assertEqual(
                    completed.returncode,
                    0,
                    completed.stderr or completed.stdout,
                )

    def test_multisource_package_has_no_runtime_or_transport_imports(self):
        forbidden_roots = {
            "ai_sales_radar.runtime",
            "aiohttp",
            "freelancer_bot",
            "googleapiclient",
            "gspread",
            "httpx",
            "psycopg",
            "requests",
            "socket",
            "sqlalchemy",
            "telethon",
            "urllib.request",
        }
        for path in MULTISOURCE_ROOT.rglob("*.py"):
            with self.subTest(path=path.name):
                imports = _imports(path)
                violations = sorted(
                    item
                    for item in imports
                    if any(
                        item == root or item.startswith(f"{root}.")
                        for root in forbidden_roots
                    )
                )
                self.assertEqual(violations, [])

    def test_importing_package_alone_loads_no_transports(self):
        script = (
            "import sys; import ai_sales_radar.multisource; "
            "bad=sorted(k for k in sys.modules if "
            "k == 'telethon' or k.startswith('telethon.') or "
            "k == 'sqlalchemy' or k.startswith('sqlalchemy.') or "
            "k == 'freelancer_bot' or k.startswith('freelancer_bot.')); "
            "raise SystemExit('loaded: '+','.join(bad) if bad else 0)"
        )
        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=PROJECT_ROOT,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )

        self.assertEqual(
            completed.returncode,
            0,
            completed.stderr or completed.stdout,
        )

    def test_telegram_commercial_score_v1_is_frozen(self):
        score_path = PROJECT_ROOT / "ai_sales_radar" / "commercial_score.py"

        self.assertEqual(COMMERCIAL_SCORE_POLICY_VERSION, "commercial-score.v1")
        self.assertEqual(
            sha256(score_path.read_bytes()).hexdigest(),
            "a4fc0d16ee7aa74f8d02d24dc6942f65a5d366f47f6ffc34829ea05c4f2440ba",
        )

    def test_foundation_adds_no_database_migration(self):
        migration_files = tuple(
            (PROJECT_ROOT / "migrations" / "versions").glob("*.py")
        )

        self.assertEqual(expected_migration_heads(), ("20260826_0037",))
        self.assertEqual(len(migration_files), 37)

    def test_existing_runtime_job_set_is_unchanged(self):
        status = inspect_runtime_configuration(
            DatabaseSettings(
                database_url="postgresql+psycopg://local/radar",
                _env_file=None,
            ),
            TelegramAuthSettings(
                api_id=123,
                api_hash="fixture-only-value",
                _env_file=None,
            ),
            OpportunityAISettings(
                openai_api_key="fixture-only-value-long-enough-for-validation",
                external_ai_data_processing_approved=True,
                _env_file=None,
            ),
        )

        self.assertEqual(
            status.continuous_job_types,
            (
                "telegram.raw_message.v1",
                "opportunity.analysis.v1",
                "opportunity.matching_inbox.v1",
                "operator.source_discovery.v1",
            ),
        )
        self.assertFalse(status.automatic_sending_enabled)


def _imports(path: Path) -> tuple[str, ...]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            imports.append(module)
            imports.extend(
                f"{module}.{alias.name}" if module else alias.name
                for alias in node.names
            )
    return tuple(imports)


def _is_multisource_import(module: str) -> bool:
    return (
        module == "multisource"
        or module.startswith("multisource.")
        or module == "ai_sales_radar.multisource"
        or module.startswith("ai_sales_radar.multisource.")
    )


if __name__ == "__main__":
    unittest.main()

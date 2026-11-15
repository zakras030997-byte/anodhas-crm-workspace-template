from __future__ import annotations

from contextlib import asynccontextmanager
import io
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch

from ai_sales_radar.bootstrap import (
    ENV_TEMPLATE,
    LocalLayout,
    alembic_config,
    build_parser,
    expected_migration_heads,
    initialize_local_layout,
    inspect_bootstrap,
    inspect_database,
)
from ai_sales_radar.config import ConfigurationError, DatabaseSettings


class LocalBootstrapTest(unittest.TestCase):
    def test_init_creates_private_layout_without_overwriting_env(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".python-version").write_text("3.14.7\n", encoding="utf-8")
            layout = LocalLayout(root)

            self.assertTrue(initialize_local_layout(layout))
            layout.env_file.write_text("KEEP_ME=1\n", encoding="utf-8")
            self.assertFalse(initialize_local_layout(layout))

            self.assertEqual(layout.env_file.read_text(encoding="utf-8"), "KEEP_ME=1\n")
            self.assertEqual(stat.S_IMODE(layout.env_file.stat().st_mode), 0o600)
            for path in layout.runtime_directories:
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o700)

    def test_database_settings_reject_sqlite_without_leaking_secret(self):
        marker = "DO_NOT_PRINT_THIS_SECRET"
        settings = DatabaseSettings(
            database_url=f"sqlite:///{marker}",
            _env_file=None,
        )
        with self.assertRaises(ConfigurationError) as raised:
            settings.require_url()
        self.assertNotIn(marker, str(raised.exception))
        self.assertNotIn(marker, repr(settings))

    def test_init_rejects_env_symlink_without_changing_target_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            outside = root.parent / f"{root.name}-outside-env"
            outside.write_text("KEEP=1\n", encoding="utf-8")
            outside.chmod(0o644)
            (root / ".env").symlink_to(outside)
            try:
                with self.assertRaisesRegex(ValueError, "символической ссылкой"):
                    initialize_local_layout(LocalLayout(root))
                self.assertEqual(stat.S_IMODE(outside.stat().st_mode), 0o644)
            finally:
                outside.unlink()

    def test_alembic_percent_escape_and_parser_commands(self):
        config = alembic_config("postgresql+psycopg://local/p%25")
        self.assertEqual(
            config.get_main_option("sqlalchemy.url"),
            "postgresql+psycopg://local/p%25",
        )
        for command in ("init", "check", "db-status", "migrate"):
            with self.subTest(command=command):
                self.assertEqual(build_parser().parse_args([command]).command, command)

    def test_bootstrap_reports_selected_provider_without_exposing_key(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".python-version").write_text("3.14.7\n", encoding="utf-8")
            marker = "groq-bootstrap-secret"
            env_file = root / ".env"
            env_file.write_text(
                "OPPORTUNITY_ANALYSIS_PROVIDER=groq\n"
                f"GROQ_API_KEY={marker}\n",
                encoding="utf-8",
            )

            status = inspect_bootstrap(
                layout=LocalLayout(root),
                env_file=env_file,
            )

        self.assertEqual(status.provider, "groq")
        self.assertTrue(status.provider_configured)
        self.assertFalse(status.external_ai_data_processing_approved)
        self.assertNotIn(marker, repr(status))
        self.assertIn("GROQ_API_KEY=", ENV_TEMPLATE)
        self.assertIn("OPPORTUNITY_ANALYSIS_PROVIDER=groq", ENV_TEMPLATE)
        self.assertIn("OPPORTUNITY_ANALYSIS_MODEL=openai/gpt-oss-20b", ENV_TEMPLATE)
        self.assertIn("EXTERNAL_AI_DATA_PROCESSING_APPROVED=false", ENV_TEMPLATE)


class FakeConnection:
    def __init__(self, heads):
        self.heads = heads

    async def execute(self, statement):
        self.statement = str(statement)

    async def run_sync(self, callback):
        class FakeMigrationContext:
            def get_current_heads(inner_self):
                return self.heads

        with patch(
            "ai_sales_radar.bootstrap.MigrationContext.configure",
            return_value=FakeMigrationContext(),
        ):
            return callback(object())


class FakeDatabase:
    instances = []

    def __init__(self, _url, **_kwargs):
        self.closed = False
        self.__class__.instances.append(self)

    @asynccontextmanager
    async def connect(self):
        yield FakeConnection(expected_migration_heads())

    async def close(self):
        self.closed = True


class DatabaseStatusTest(unittest.IsolatedAsyncioTestCase):
    async def test_status_uses_existing_database_adapter_and_always_closes(self):
        FakeDatabase.instances.clear()
        settings = DatabaseSettings(
            database_url="postgresql+psycopg://local/ai_sales_radar",
            _env_file=None,
        )
        status = await inspect_database(settings, database_factory=FakeDatabase)

        self.assertTrue(status.reachable)
        self.assertTrue(status.migration_current)
        self.assertTrue(FakeDatabase.instances[-1].closed)


if __name__ == "__main__":
    unittest.main()

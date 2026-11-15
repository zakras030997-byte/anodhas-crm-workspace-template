import ast
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
ALEMBIC_ENV = ROOT / "migrations" / "env.py"
AI_SALES_RADAR_TABLES = {
    "commercial_score_snapshots",
    "lead_inbox_items",
    "lead_inbox_status_events",
    "reply_drafts",
}


class AlembicMetadataRegistrationTest(unittest.TestCase):
    def test_env_imports_desktop_schema_into_target_metadata(self):
        """Execute env.py imports without starting an Alembic migration run."""
        tree = ast.parse(ALEMBIC_ENV.read_text(encoding="utf-8"), ALEMBIC_ENV.name)
        import_only = ast.Module(
            body=[
                node
                for node in tree.body
                if isinstance(node, (ast.Import, ast.ImportFrom))
            ],
            type_ignores=[],
        )
        namespace = {}

        exec(
            compile(import_only, str(ALEMBIC_ENV), "exec"),
            namespace,
            namespace,
        )

        metadata = namespace["metadata"]
        self.assertTrue(AI_SALES_RADAR_TABLES.issubset(metadata.tables))
        self.assertIs(namespace["_ai_sales_radar_schema"].metadata, metadata)


if __name__ == "__main__":
    unittest.main()

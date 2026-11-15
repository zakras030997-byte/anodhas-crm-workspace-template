from __future__ import annotations

import contextlib
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
import io
import json
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from ai_sales_radar.inbox_cli import _emit, build_parser, main


class FixtureStatus(str, Enum):
    READY = "ready"


@dataclass(frozen=True)
class FixtureOutput:
    identifier: object
    score: Decimal
    status: FixtureStatus
    created_at: datetime


class InboxCommandTest(unittest.TestCase):
    def test_parser_exposes_sync_list_show_and_status(self):
        parser = build_parser()
        profile_id = uuid4()
        lead_id = uuid4()
        self.assertEqual(
            parser.parse_args(
                ["sync", "--profile-id", str(profile_id)]
            ).command,
            "sync",
        )
        self.assertEqual(
            parser.parse_args(
                ["list", "--profile-id", str(profile_id), "--tier", "hot"]
            ).tier,
            "hot",
        )
        self.assertEqual(
            parser.parse_args(
                ["show", "--lead-id", str(lead_id)]
            ).lead_id,
            lead_id,
        )
        status = parser.parse_args(
            [
                "status",
                "--lead-id",
                str(lead_id),
                "--to",
                "shortlisted",
                "--expected-revision",
                "2",
                "--reason",
                "good fit",
                "--request-id",
                "request-1",
            ]
        )
        self.assertEqual(status.to, "shortlisted")

    def test_help_never_reads_database_configuration(self):
        with (
            patch("ai_sales_radar.inbox_cli.DatabaseSettings.from_env") as settings,
            contextlib.redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit),
        ):
            main([])
        settings.assert_not_called()

    def test_unexpected_error_does_not_print_database_url(self):
        secret_url = "postgresql+psycopg://operator:secret@localhost/radar"
        settings = SimpleNamespace(require_url=lambda: secret_url)
        stderr = io.StringIO()
        with (
            patch(
                "ai_sales_radar.inbox_cli.DatabaseSettings.from_env",
                return_value=settings,
            ),
            patch(
                "ai_sales_radar.inbox_cli._execute",
                AsyncMock(side_effect=RuntimeError(secret_url)),
            ),
            patch("sys.stderr", stderr),
        ):
            self.assertEqual(
                main(["list", "--profile-id", str(uuid4())]),
                1,
            )
        self.assertNotIn("secret", stderr.getvalue())
        self.assertIn("RuntimeError", stderr.getvalue())

    def test_json_output_handles_domain_types(self):
        value = FixtureOutput(
            identifier=uuid4(),
            score=Decimal("81.5"),
            status=FixtureStatus.READY,
            created_at=datetime(2026, 8, 26, tzinfo=timezone.utc),
        )
        output = io.StringIO()
        with patch("sys.stdout", output):
            _emit(value)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["score"], "81.5")
        self.assertEqual(payload["status"], "ready")


if __name__ == "__main__":
    unittest.main()

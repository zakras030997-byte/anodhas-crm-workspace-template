#!/usr/bin/env python3
"""Thin project-root wrapper for the AI Sales Radar Telegram auth CLI."""

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ai_sales_radar.telegram_auth import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())

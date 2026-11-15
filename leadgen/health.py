"""Container readiness requires a fresh supervisor and live Telegram collector."""

import json
import sys
import time
from pathlib import Path


def ready(
    root: Path = Path("/app/.local"),
    proc_root: Path = Path("/proc"),
    now: float | None = None,
) -> bool:
    now = time.time() if now is None else now
    try:
        supervisor = json.loads((root / "unified-status.json").read_text())
        collector = json.loads((root / "collector-live.json").read_text())
        pid = supervisor["pid"]
        return (
            type(pid) is int
            and pid > 0
            and supervisor["phase"] == "supervising"
            and collector["phase"] == "live"
            and collector["pid"] == pid
            and 0 <= now - supervisor["updated_at"] < 30
            and 0 <= now - collector["updated_at"] < 20
            and (proc_root / str(pid)).exists()
        )
    except (OSError, ValueError, KeyError, TypeError):
        return False


if __name__ == "__main__":
    sys.exit(0 if ready() else 1)

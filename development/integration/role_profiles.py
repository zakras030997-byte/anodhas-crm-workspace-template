"""Authenticated, read-only display of the five project role prompts and profiles."""
from __future__ import annotations

import os
from pathlib import Path

from fastapi import APIRouter, Depends, FastAPI, HTTPException
from mounted_sources import SourceError, _open_root, _read_file
from raytsystem.webapp.app import require_session
from role_policy import MODELS, REASONING

ROLE_DIRECTORY = Path(__file__).resolve().parent / "roles"
# Deployment-owned allowlist; no request value is used as a filesystem path.
ROLE_PROFILES = {
    "agent_orchestrator": ("orchestrator.md", REASONING["orchestrator"]),
    "agent_builder": ("builder.md", REASONING["builder"]),
    "agent_reviewer": ("reviewer.md", REASONING["reviewer"]),
    "agent_librarian": ("librarian.md", REASONING["librarian"]),
    "agent_researcher": ("researcher.md", REASONING["researcher"]),
}


def read_role_profile(agent_id: str) -> dict:
    profile = ROLE_PROFILES.get(agent_id)
    if profile is None:
        raise HTTPException(404, detail={"code": "role_profile_not_found"})
    filename, reasoning = profile
    try:
        root = _open_root(ROLE_DIRECTORY)
        try:
            prompt, digest, _ = _read_file(root, filename, limit=64 * 1024)
        finally:
            os.close(root)
    except (OSError, UnicodeError, SourceError):
        raise HTTPException(503, detail={"code": "role_prompt_unavailable"}) from None
    return {
        "agent_id": agent_id,
        "system_prompt": prompt,
        "prompt_sha256": digest,
        "profile": {"model": MODELS[agent_id.removeprefix("agent_")], "reasoning_effort": reasoning},
        "profile_source": "configured_project_profile",
        "automatic_upgrade": False,
    }


def install_role_profiles(app: FastAPI) -> None:
    router = APIRouter(
        prefix="/api/v1/anodhas/role-profiles", dependencies=[Depends(require_session)]
    )

    @router.get("/{agent_id}")
    def role_profile(agent_id: str) -> dict:
        return read_role_profile(agent_id)

    # Keep this route ahead of the upstream SPA fallback and inside its middleware.
    app.router.routes[0:0] = router.routes

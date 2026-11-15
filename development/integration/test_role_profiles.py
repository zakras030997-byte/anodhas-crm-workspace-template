from __future__ import annotations

import hashlib
import os

import pytest
import role_profiles as profiles
from fastapi import HTTPException
from fastapi.testclient import TestClient
from server import CRM_COOKIE, Settings, build_app
from test_server import CRM, ORIGIN, VALID_COOKIE


@pytest.mark.parametrize("agent_id", profiles.ROLE_PROFILES)
def test_role_text_and_fixed_economical_profile(agent_id):
    filename, reasoning = profiles.ROLE_PROFILES[agent_id]
    original = (profiles.ROLE_DIRECTORY / filename).read_bytes()
    result = profiles.read_role_profile(agent_id)
    assert result["system_prompt"] == original.decode()
    assert result["prompt_sha256"] == hashlib.sha256(original).hexdigest()
    assert result["profile"] == {"model": profiles.MODELS[agent_id.removeprefix("agent_")], "reasoning_effort": reasoning}
    assert result["automatic_upgrade"] is False


@pytest.mark.parametrize("agent_id", ["README", "builder", "../roles/builder", "/etc/passwd"])
def test_unknown_ids_are_not_paths(agent_id):
    with pytest.raises(HTTPException) as error:
        profiles.read_role_profile(agent_id)
    assert error.value.status_code == 404


def test_symlinks_hardlinks_oversized_and_missing_prompts_fail_closed(tmp_path, monkeypatch):
    directory = tmp_path / "roles"
    directory.mkdir()
    monkeypatch.setattr(profiles, "ROLE_DIRECTORY", directory)
    prompt = directory / "builder.md"
    outside = tmp_path / "private.md"
    outside.write_text("PRIVATE")
    for mode in ("missing", "symlink", "hardlink", "oversized"):
        if mode == "symlink":
            prompt.symlink_to(outside)
        elif mode == "hardlink":
            os.link(outside, prompt)
        elif mode == "oversized":
            prompt.write_bytes(b"a" * (64 * 1024 + 1))
        with pytest.raises(HTTPException) as error:
            profiles.read_role_profile("agent_builder")
        assert error.value.status_code == 503
        if prompt.exists() or prompt.is_symlink():
            prompt.unlink()


def test_endpoint_requires_both_sessions_and_has_no_write_route(tmp_path):
    static = tmp_path / "static"
    static.mkdir()
    (static / "index.html").write_text(
        '<meta name="raytsystem-csp-nonce" content="__RAYTSYSTEM_CSP_NONCE__">'
    )
    app = build_app(Settings(123, workspace=tmp_path, static_dir=static), fetch_session=CRM())
    with TestClient(app, base_url=ORIGIN) as client:
        endpoint = "/development/api/v1/anodhas/role-profiles/agent_builder"
        assert client.get(endpoint).status_code == 401
        client.cookies.set(CRM_COOKIE, VALID_COOKIE)
        assert client.get(endpoint).status_code == 401
        assert client.get("/development/").status_code == 200
        response = client.get(endpoint)
        assert response.status_code == 200
        assert response.headers["cache-control"] == "no-store"
        assert response.json()["system_prompt"].startswith("#")
        assert client.get(endpoint.replace("agent_builder", "README")).status_code == 404
        csrf = client.get("/development/api/v1/session").json()["csrf_token"]
        assert client.post(endpoint, json={}, headers={
            "Origin": ORIGIN, "X-CSRF-Token": csrf, "Idempotency-Key": "no-write"
        }).status_code == 405

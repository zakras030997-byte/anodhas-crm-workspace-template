from __future__ import annotations

import json
import os

import mounted_sources as module
import pytest
from fastapi.testclient import TestClient
from mounted_sources import MountedSources, SourceError, scrub_text
from server import CRM_COOKIE, Settings, build_app
from test_server import CRM, ORIGIN, VALID_COOKIE


@pytest.fixture
def sources(tmp_path):
    notes, skills = tmp_path / "notes", tmp_path / "skills"
    notes.mkdir()
    (notes / "Projects").mkdir()
    (notes / "Projects" / "Studio.md").write_text("# Studio\n\n[[Review]] [[Missing]]\n")
    (notes / "Projects" / "Review.md").write_text("# Review\n\n[Studio](Studio.md)\n")
    (skills / "review" / "references").mkdir(parents=True)
    (skills / "review" / "SKILL.md").write_text(
        "---\nname: review\ndescription: Read and review\n---\n# Review skill\n"
    )
    (skills / "review" / "references" / "guide.md").write_text("# Not a skill\n")
    return MountedSources(notes, skills), notes, skills


def test_inventory_has_metadata_links_and_only_skill_definitions(sources):
    service, notes, skills = sources
    before = {path: path.read_bytes() for root in (notes, skills) for path in root.rglob("*.md")}
    snapshot = service.snapshot()
    assert [source["state"] for source in snapshot["sources"]] == ["current", "current"]
    assert [source["file_count"] for source in snapshot["sources"]] == [2, 1]
    assert snapshot["checked_at"]
    assert len(snapshot["items"]) == 3
    assert all(item["mode"] == "read_only" for item in snapshot["items"])
    assert all(len(item["source_sha256"]) == 64 for item in snapshot["items"])
    payload = json.dumps(snapshot)
    assert "# Studio" not in payload and "# Review skill" not in payload
    assert str(notes) not in payload and str(skills) not in payload
    assert "Missing" not in payload and "guide.md" not in payload
    assert len([edge for edge in snapshot["graph"]["edges"] if edge["kind"] == "references"]) == 2
    node_ids = {node["node_id"] for node in snapshot["graph"]["nodes"]}
    assert all((node["x"], node["y"]) != (0, 0) for node in snapshot["graph"]["nodes"])
    assert all(
        edge["source"] in node_ids and edge["target"] in node_ids
        for edge in snapshot["graph"]["edges"]
    )
    assert before == {
        path: path.read_bytes() for root in (notes, skills) for path in root.rglob("*.md")
    }


def test_hidden_credentials_symlinks_and_hardlinks_are_excluded(sources, tmp_path):
    service, notes, _ = sources
    (notes / ".obsidian").mkdir()
    (notes / ".obsidian" / "hidden.md").write_text("Hidden")
    (notes / "Passwords.md").write_text("password: this-is-private")
    external = tmp_path / "external.md"
    external.write_text("Outside mounted source")
    (notes / "Linked.md").symlink_to(external)
    (notes / "LinkedDir").symlink_to(tmp_path, target_is_directory=True)
    os.link(external, notes / "Hardlinked.md")
    items = service.snapshot()["items"]
    assert {item["relative_path"] for item in items if item["source_id"] == "obsidian"} == {
        "Projects/Studio.md",
        "Projects/Review.md",
    }


def test_symlink_root_or_ancestor_is_not_opened(tmp_path):
    real = tmp_path / "real"
    real.mkdir()
    (real / "Public.md").write_text("Source")
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)
    for path in (alias, alias / "nested"):
        (real / "nested").mkdir(exist_ok=True)
        snapshot = MountedSources(path, tmp_path / "missing").snapshot()
        assert snapshot["sources"][0]["state"] == "unavailable"
        assert snapshot["items"] == []


def test_missing_sources_are_explicit_and_have_no_fixture_data(tmp_path):
    snapshot = MountedSources(tmp_path / "a", tmp_path / "b").snapshot()
    assert [source["state"] for source in snapshot["sources"]] == ["missing", "missing"]
    assert snapshot["items"] == []
    assert all(node["kind"] == "mounted_source" for node in snapshot["graph"]["nodes"])


def test_server_manifest_time_distinct_from_file_mtime_and_check_time(sources):
    service, notes, _ = sources
    # Manifest records can exceed the note limit, but are never returned to UI.
    snapshot_utc = "2026-10-14T10:30:00Z"
    (notes / ".source-manifest.json").write_text(
        json.dumps(
            {
                "snapshot_utc": snapshot_utc,
                "source": "obsidian",
                "mode": "readonly_snapshot",
                "records": [{"path": "hidden-" + "x" * (300 * 1024), "sha256": "a" * 64}],
            }
        )
    )
    snapshot = service.snapshot()
    assert snapshot["sources"][0]["snapshot_utc"] == snapshot_utc
    assert snapshot["sources"][0]["snapshot_manifest_state"] == "current"
    assert snapshot["sources"][0]["mode"] == "readonly_snapshot"
    assert snapshot["checked_at"] != snapshot_utc
    assert "hidden-" not in json.dumps(snapshot)
    assert all(
        item["snapshot_utc"] == snapshot_utc
        for item in snapshot["items"]
        if item["source_id"] == "obsidian"
    )


def test_invalid_or_symlink_snapshot_manifest_never_becomes_verified(sources, tmp_path):
    service, notes, _ = sources
    manifest = notes / ".source-manifest.json"
    manifest.write_text(
        json.dumps(
            {"snapshot_utc": "2026-10-14", "source": "obsidian", "mode": "readonly_snapshot"}
        )
    )
    assert service.snapshot()["sources"][0]["snapshot_manifest_state"] == "unavailable"
    manifest.unlink()
    external = tmp_path / "manifest.json"
    external.write_text(
        json.dumps(
            {
                "snapshot_utc": "2026-10-14T10:30:00Z",
                "source": "obsidian",
                "mode": "readonly_snapshot",
            }
        )
    )
    manifest.symlink_to(external)
    assert service.snapshot()["sources"][0]["snapshot_utc"] is None


def test_selected_text_is_hash_and_nanosecond_mtime_bound(sources):
    service, notes, _ = sources
    item = next(item for item in service.snapshot()["items"] if item["label"] == "Studio")
    response = service.read(item["item_id"], item["source_sha256"], item["updated_at"])
    assert response["content"].startswith("# Studio") and response["format"] == "text"
    assert response["redacted"] is False
    path = notes / "Projects" / "Studio.md"
    stamp = path.stat().st_mtime_ns
    os.utime(path, ns=(stamp, stamp + 1))
    with pytest.raises(SourceError) as error:
        service.read(item["item_id"], item["source_sha256"], item["updated_at"])
    assert error.value.status == 409
    fresh = next(item for item in service.snapshot()["items"] if item["label"] == "Studio")
    assert fresh["source_sha256"] == item["source_sha256"]
    assert fresh["updated_at"] != item["updated_at"]
    path.write_text("# Studio updated\n")
    with pytest.raises(SourceError) as error:
        service.read(fresh["item_id"], fresh["source_sha256"], fresh["updated_at"])
    assert error.value.code == "source_changed"


def test_source_symlink_swap_never_returns_target_body(sources, tmp_path):
    service, notes, _ = sources
    item = next(item for item in service.snapshot()["items"] if item["label"] == "Studio")
    path = notes / "Projects" / "Studio.md"
    path.unlink()
    external = tmp_path / "outside.md"
    external.write_text("DO NOT LEAK")
    path.symlink_to(external)
    with pytest.raises(SourceError) as error:
        service.read(item["item_id"], item["source_sha256"], item["updated_at"])
    assert error.value.status == 404


def test_known_secrets_masked_and_html_remains_inert_text(sources):
    service, notes, _ = sources
    (notes / "Display.md").write_text(
        "# Display\npassword: short-private-value\n"
        "api_key = sk-" + "X" * 60 + "\n"
        "vless://sensitive-subscription@host\n"
        "postgres://user:secret@host/db\n"
        "https://service/path?token=private-value\n"
        '<script>window.pwned=true</script>\n<img onerror="alert(1)">\n'
    )
    item = next(item for item in service.snapshot()["items"] if item["label"] == "Display")
    response = service.read(item["item_id"], item["source_sha256"], item["updated_at"])
    assert response["redacted"] is True and response["redaction_count"] >= 5
    assert all(
        secret not in response["content"]
        for secret in (
            "short-private-value",
            "X" * 60,
            "sensitive-subscription",
            "user:secret",
            "private-value",
        )
    )
    assert "<script>" in response["content"]  # Data, never an HTML response.


@pytest.mark.parametrize(
    "body",
    [
        "-----BEGIN OPENSSH PRIVATE KEY-----\nraw-secret\n-----END OPENSSH PRIVATE KEY-----",
        "password: |\n  multiline-secret",
        "seed_phrase: >\n  private words",
    ],
)
def test_multiline_private_blocks_refused(sources, body):
    service, notes, _ = sources
    (notes / "Display.md").write_text(body)
    assert not any(item["label"] == "Display" for item in service.snapshot()["items"])
    with pytest.raises(SourceError) as error:
        scrub_text(body)
    assert error.value.code == "source_content_restricted"


def test_size_encoding_and_inventory_budgets_are_bounded(sources, monkeypatch):
    service, notes, _ = sources
    (notes / "Oversized.md").write_bytes(b"x" * (module.MAX_FILE_BYTES + 1))
    (notes / "Broken.md").write_bytes(b"\xff\xfe")
    assert not any(item["label"] in {"Oversized", "Broken"} for item in service.snapshot()["items"])
    monkeypatch.setattr(module, "MAX_FILES", 1)
    snapshot = service.snapshot()
    assert snapshot["sources"][0]["truncated"] is True
    assert snapshot["sources"][0]["file_count"] == 1


def test_ambiguous_wikilink_does_not_invent_cross_note_connection(sources):
    service, notes, _ = sources
    (notes / "Other").mkdir()
    (notes / "Other" / "Review.md").write_text("# Another Review")
    (notes / "Root.md").write_text("[[Review]]")
    snapshot = service.snapshot()
    root = next(item for item in snapshot["items"] if item["label"] == "Root")
    assert not any(edge["source"] == root["item_id"] for edge in snapshot["graph"]["edges"])


def test_api_requires_both_crm_and_inner_session_and_uses_no_write_route(sources, tmp_path):
    _, notes, skills = sources
    static = tmp_path / "static"
    static.mkdir()
    (static / "index.html").write_text(
        '<meta name="raytsystem-csp-nonce" content="__RAYTSYSTEM_CSP_NONCE__">'
    )
    (tmp_path / "workspace").mkdir()
    app = build_app(
        Settings(
            123,
            workspace=tmp_path / "workspace",
            static_dir=static,
            obsidian_dir=notes,
            hermes_skills_dir=skills,
        ),
        fetch_session=CRM(),
    )
    with TestClient(app, base_url=ORIGIN) as client:
        endpoint = "/development/api/v1/mounted-sources"
        assert client.get(endpoint).status_code == 401
        client.cookies.set(CRM_COOKIE, VALID_COOKIE)
        assert client.get(endpoint).status_code == 401
        assert client.get("/development/").status_code == 200
        snapshot = client.get(endpoint)
        assert snapshot.status_code == 200 and snapshot.headers["cache-control"] == "no-store"
        item = snapshot.json()["items"][0]
        detail = client.get(
            endpoint + "/" + item["item_id"],
            params={"expected": item["source_sha256"], "updated_at": item["updated_at"]},
        )
        assert detail.status_code == 200 and detail.headers["content-type"].startswith(
            "application/json"
        )
        assert detail.json()["format"] == "text"
        assert (
            client.get(
                endpoint + "/not-a-path",
                params={"expected": item["source_sha256"], "updated_at": item["updated_at"]},
            ).status_code
            == 404
        )
        assert (
            client.get(
                endpoint + "/" + item["item_id"],
                params={"expected": "0" * 64, "updated_at": item["updated_at"]},
            ).status_code
            == 409
        )
        csrf = client.get("/development/api/v1/session").json()["csrf_token"]
        assert (
            client.post(
                endpoint,
                json={},
                headers={
                    "Origin": ORIGIN,
                    "X-CSRF-Token": csrf,
                    "Idempotency-Key": "must-not-write",
                },
            ).status_code
            == 405
        )

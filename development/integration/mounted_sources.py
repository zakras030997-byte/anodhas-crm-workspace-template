"""Bounded read-only projections of separately mounted owner knowledge sources.

Nothing is ingested into the canonical ledger, sent to a model, or executed.
Inventory contains metadata and links only. Selected text is read again through
no-follow descriptors and bound to both the source hash and modification time.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import stat
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlsplit

import yaml
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query
from raytsystem.webapp.app import require_session

MAX_FILE_BYTES = 256 * 1024
MAX_MANIFEST_BYTES = 1024 * 1024
MAX_FILES = 1500  # Per source, with a separate directory-entry budget.
MAX_ENTRIES = 20000
MAX_DEPTH = 16
MAX_LINKS = 4000
REDACTION = "[СКРЫТО]"
SENSITIVE_PATH = re.compile(
    r"(?i)(?:password|passwd|credential|secret|\.env|token|private.?key|"
    r"парол|секрет|токен|плат[её]ж|реквизит|паспорт|seed|mnemonic)"
)
ASSIGNMENT = re.compile(
    r"(?im)((?:[\"']?)(?:password|passwd|api[_ -]?key|client[_ -]?secret|"
    r"session[_ -]?secret|access[_ -]?token|refresh[_ -]?token|bot[_ -]?token|"
    r"authorization|пароль|токен|секрет|seed(?:[_ -]?phrase)?|recovery[_ -]?phrase|"
    r"private[_ -]?key|mnemonic)(?:[\"']?)"
    r"\s*[:=]\s*)([^\r\n]+)"
)
LONG_TOKEN = re.compile(r"(?<![\w])[A-Za-z0-9_+/=-]{40,}(?![\w])")
TELEGRAM_TOKEN = re.compile(r"\b[0-9]{5,12}:[A-Za-z0-9_-]{20,}\b")
JWT_TOKEN = re.compile(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b")
VPN_URL = re.compile(r"(?i)\b(?:vless|vmess|ss|hy|hysteria2?|trojan)://\S+")
AUTH_URL = re.compile(r"\b[a-zA-Z][a-zA-Z0-9+.-]*://[^\s/@]+:[^\s/@]+@\S+")
QUERY_SECRET = re.compile(r"(?i)([?&](?:token|key|secret|auth|password)=)[^\s&#]+")
PRIVATE_BLOCK = re.compile(r"(?i)-----BEGIN [^-\r\n]*PRIVATE KEY-----")
MULTILINE_SECRET = re.compile(
    r"(?im)(?:password|secret|token|пароль|секрет|seed(?:[_ -]?phrase)?|mnemonic)"
    r"\s*[:=]\s*(?:[|>]|\"\"\"|'\'\')\s*$"
)


class SourceError(Exception):
    def __init__(self, code: str, status: int) -> None:
        self.code, self.status = code, status


def scrub_text(text: str) -> tuple[str, int]:
    """Mask obvious credential values; refuse unbounded multiline secret blocks."""
    if PRIVATE_BLOCK.search(text) or MULTILINE_SECRET.search(text):
        raise SourceError("source_content_restricted", 403)
    count = 0
    for pattern, replacement in (
        (TELEGRAM_TOKEN, REDACTION),
        (JWT_TOKEN, REDACTION),
        (VPN_URL, REDACTION),
        (AUTH_URL, REDACTION),
        (QUERY_SECRET, lambda m: m[1] + REDACTION),
        (ASSIGNMENT, lambda m: m[1] + REDACTION),
        (LONG_TOKEN, REDACTION),
    ):
        text, changed = pattern.subn(replacement, text)
        count += changed
    return text, count


def _label(text: str, limit: int = 240) -> str:
    try:
        text, _ = scrub_text(text)
    except SourceError:
        return REDACTION
    text = re.sub(r"<[^>]*>", " ", text)
    text = re.sub(r"https?://\S+|[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", REDACTION, text)
    return " ".join(text.split())[:limit]


def _stamp(value: int) -> str:
    seconds, nanoseconds = divmod(value, 1_000_000_000)
    return (
        datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%S")
        + f".{nanoseconds:09d}+00:00"
    )


def _open_directory(base: int, parts: tuple[str, ...]) -> int:
    descriptor = os.dup(base)
    try:
        for part in parts:
            if part in {"", ".", ".."} or "/" in part or "\\" in part:
                raise OSError("Invalid source component")
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _open_root(path: Path) -> int:
    if not path.is_absolute():
        raise OSError("Source root must be absolute")
    descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        return _open_directory(descriptor, tuple(path.parts[1:]))
    finally:
        os.close(descriptor)


def _read_file(root: int, relative: str, *, limit: int = MAX_FILE_BYTES) -> tuple[str, str, str]:
    parts = PurePosixPath(relative).parts
    parent = _open_directory(root, parts[:-1])
    try:
        descriptor = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
    finally:
        os.close(parent)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise SourceError("source_file_restricted", 403)
        if before.st_size > limit:
            raise SourceError("source_file_too_large", 413)
        chunks, remaining = [], limit + 1
        while remaining:
            chunk = os.read(descriptor, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        after = os.fstat(descriptor)
        if len(data) > limit:
            raise SourceError("source_file_too_large", 413)
        if (before.st_size, before.st_mtime_ns, before.st_ino) != (
            after.st_size,
            after.st_mtime_ns,
            after.st_ino,
        ):
            raise SourceError("source_changed", 409)
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            raise SourceError("source_encoding_unsupported", 415) from None
        if "\x00" in text:
            raise SourceError("source_encoding_unsupported", 415)
        return text, hashlib.sha256(data).hexdigest(), _stamp(after.st_mtime_ns)
    finally:
        os.close(descriptor)


@dataclass(frozen=True)
class Source:
    source_id: str
    label: str
    root: Path
    kind: str


class MountedSources:
    def __init__(
        self, obsidian: Path = Path("/data/obsidian"), skills: Path = Path("/data/hermes-skills")
    ) -> None:
        self.sources = (
            Source("obsidian", "Obsidian сервера", obsidian, "mounted_note"),
            Source("hermes-skills", "Навыки Hermes сервера", skills, "mounted_skill"),
        )

    def _scan(self, source: Source) -> tuple[dict, list[dict], dict[str, str]]:
        status = {
            "source_id": source.source_id,
            "label": source.label,
            "mode": "read_only",
            "state": "current",
            "file_count": 0,
            "skipped_count": 0,
            "truncated": False,
            "updated_at": None,
            "snapshot_utc": None,
            "snapshot_manifest_state": "absent",
        }
        records, bodies = [], {}
        try:
            root = _open_root(source.root)
        except FileNotFoundError:
            status["state"] = "missing"
            return status, records, bodies
        except OSError:
            status["state"] = "unavailable"
            return status, records, bodies
        queue, visited = deque([()]), 0
        try:
            try:
                manifest_text, _, _ = _read_file(
                    root, ".source-manifest.json", limit=MAX_MANIFEST_BYTES
                )
                manifest = json.loads(manifest_text)
                stamp = manifest.get("snapshot_utc") if isinstance(manifest, dict) else None
                if (
                    not isinstance(stamp, str)
                    or datetime.fromisoformat(stamp).tzinfo is None
                    or manifest.get("source") != source.source_id
                    or manifest.get("mode") != "readonly_snapshot"
                ):
                    raise ValueError("Invalid snapshot metadata")
                status["snapshot_utc"] = stamp
                status["snapshot_manifest_state"] = "current"
                status["mode"] = "readonly_snapshot"
            except FileNotFoundError:
                pass
            except (OSError, SourceError, ValueError, TypeError):
                status["snapshot_manifest_state"] = "unavailable"
            while queue:
                parts = queue.popleft()
                try:
                    directory = _open_directory(root, parts)
                except OSError:
                    status["skipped_count"] += 1
                    continue
                try:
                    names = []
                    with os.scandir(directory) as entries:
                        for entry in entries:
                            visited += 1
                            if visited > MAX_ENTRIES:
                                status["truncated"] = True
                                break
                            names.append(entry.name)
                    for name in sorted(names):
                        relative = "/".join((*parts, name))
                        if (
                            name.startswith(".")
                            or SENSITIVE_PATH.search(relative)
                            or "\\" in name
                            or any(ord(c) < 32 for c in name)
                        ):
                            status["skipped_count"] += 1
                            continue
                        try:
                            metadata = os.stat(name, dir_fd=directory, follow_symlinks=False)
                        except OSError:
                            status["skipped_count"] += 1
                            continue
                        if stat.S_ISDIR(metadata.st_mode):
                            if len(parts) < MAX_DEPTH:
                                queue.append((*parts, name))
                            else:
                                status["truncated"] = True
                            continue
                        if (
                            not stat.S_ISREG(metadata.st_mode)
                            or (source.kind == "mounted_skill" and name != "SKILL.md")
                            or (source.kind == "mounted_note" and not name.lower().endswith(".md"))
                        ):
                            status["skipped_count"] += 1
                            continue
                        if len(records) >= MAX_FILES:
                            status["truncated"] = True
                            break
                        try:
                            text, digest, updated_at = _read_file(root, relative)
                            # A private key/seed block is excluded, including its metadata.
                            scrub_text(text)
                        except (OSError, SourceError):
                            status["skipped_count"] += 1
                            continue
                        label = PurePosixPath(relative).stem
                        description = ""
                        if source.kind == "mounted_skill":
                            label = PurePosixPath(relative).parent.name or "SKILL"
                            match = re.match(r"\A---\s*\n(.{0,16384}?)\n---(?:\s|$)", text, re.S)
                            if match:
                                try:
                                    frontmatter = yaml.safe_load(match[1])
                                    if isinstance(frontmatter, dict):
                                        if isinstance(frontmatter.get("name"), str):
                                            label = frontmatter["name"]
                                        if isinstance(frontmatter.get("description"), str):
                                            description = frontmatter["description"]
                                except (yaml.YAMLError, ValueError, RecursionError):
                                    pass
                        item_id = (
                            "mounted_"
                            + hashlib.sha256(f"{source.source_id}:{relative}".encode()).hexdigest()[
                                :24
                            ]
                        )
                        records.append(
                            {
                                "item_id": item_id,
                                "source_id": source.source_id,
                                "kind": source.kind,
                                "label": _label(label),
                                "description": _label(description),
                                "relative_path": _label(relative, 1024),
                                "source_ref": f"{source.source_id}:{_label(relative, 1024)}",
                                "source_sha256": digest,
                                "updated_at": updated_at,
                                "mode": "read_only",
                                "snapshot_utc": status["snapshot_utc"],
                                "_relative": relative,
                            }
                        )
                        bodies[item_id] = text
                    if status["truncated"] and (visited > MAX_ENTRIES or len(records) >= MAX_FILES):
                        break
                finally:
                    os.close(directory)
        except OSError:
            status["state"] = "unavailable"
            records, bodies = [], {}
        finally:
            os.close(root)
        status["file_count"] = len(records)
        status["updated_at"] = max((r["updated_at"] for r in records), default=None)
        if status["truncated"]:
            status["state"] = "truncated"
        return status, records, bodies

    def snapshot(self) -> dict:
        statuses, records, bodies = [], [], {}
        for source in self.sources:
            status, items, text = self._scan(source)
            statuses.append(status)
            records.extend(items)
            bodies.update(text)
        records.sort(key=lambda item: (item["source_id"], item["_relative"]))
        lookup = defaultdict(list)
        for record in records:
            if record["source_id"] == "obsidian":
                relative = record["_relative"]
                for key in {
                    relative.casefold(),
                    relative[:-3].casefold(),
                    PurePosixPath(relative).stem.casefold(),
                }:
                    lookup[key].append(record["item_id"])
        edges = []
        for record in records:
            edges.append(
                self._edge("mounted_source_" + record["source_id"], record["item_id"], "contains")
            )
            if record["source_id"] != "obsidian":
                continue
            text = bodies[record["item_id"]]
            targets = re.findall(r"\[\[([^\]\r\n]{1,500})\]\]", text)
            targets.extend(
                match
                for match in re.findall(r"\[[^\]\r\n]*\]\(([^)\r\n]{1,500})\)", text)
                if not urlsplit(match).scheme and not match.startswith("/")
            )
            for target in targets[:100]:
                target = unquote(target.split("|", 1)[0].split("#", 1)[0].strip())
                if not target or ".." in PurePosixPath(target).parts:
                    continue
                siblings = str(PurePosixPath(record["_relative"]).parent / target).casefold()
                candidates = lookup.get(siblings) or lookup.get(target.casefold(), [])
                if len(candidates) == 1 and candidates[0] != record["item_id"]:
                    edges.append(self._edge(record["item_id"], candidates[0], "references"))
                if len(edges) >= MAX_LINKS:
                    break
        # Deduplicate literal references without exposing unresolved targets.
        edges = list({edge["edge_id"]: edge for edge in edges}.values())[:MAX_LINKS]
        public = [
            {key: value for key, value in record.items() if not key.startswith("_")}
            for record in records
        ]
        digest = hashlib.sha256(
            json.dumps([public, statuses, edges], ensure_ascii=False, sort_keys=True).encode()
        ).hexdigest()
        checked_at = datetime.now(UTC).isoformat()
        nodes = [
            self._node(
                "mounted_source_" + status["source_id"],
                "mounted_source",
                status["label"],
                status["state"],
                status["updated_at"],
                {
                    "source_id": status["source_id"],
                    "mode": "read_only",
                    "source_state": status["state"],
                },
            )
            for status in statuses
        ]
        for item in public:
            nodes.append(
                self._node(
                    item["item_id"],
                    item["kind"],
                    item["label"],
                    "read_only",
                    item["updated_at"],
                    {
                        key: str(item[key])
                        for key in ("source_ref", "source_sha256", "updated_at", "mode")
                    },
                )
            )
        return {
            "snapshot_id": digest,
            "checked_at": checked_at,
            "sources": statuses,
            "items": public,
            "graph": {"nodes": nodes, "edges": edges},
            "projection": "read_only_metadata_and_explicit_links",
        }

    @staticmethod
    def _node(
        item_id: str, kind: str, label: str, state: str, updated_at: str | None, metadata: dict
    ) -> dict:
        seed = hashlib.sha256(item_id.encode()).hexdigest()
        angle = int(seed[:8], 16) / 0xFFFFFFFF * math.tau
        base_radius = 1200 if kind == "mounted_note" else 950 if kind == "mounted_skill" else 700
        radius = base_radius + int(seed[8:12], 16) / 0xFFFF * 180
        return {
            "node_id": item_id,
            "kind": kind,
            "label": label,
            "subtitle": metadata.get("source_ref", "Серверный источник · только чтение"),
            "status": state,
            "ring": "knowledge",
            "importance": 0.4,
            "x": round(radius * math.cos(angle)),
            "y": round(radius * math.sin(angle)),
            "recorded_at": updated_at,
            "source_ref": metadata.get("source_ref"),
            "metadata": metadata,
        }

    @staticmethod
    def _edge(source: str, target: str, kind: str) -> dict:
        digest = hashlib.sha256(f"{source}:{kind}:{target}".encode()).hexdigest()[:24]
        return {
            "edge_id": "mounted_edge_" + digest,
            "source": source,
            "target": target,
            "kind": kind,
            "status": "read_only",
            "directed": True,
            "metadata": {"provenance": "explicit_source_structure"},
        }

    def read(self, item_id: str, expected: str, updated_at: str) -> dict:
        snapshot = self.snapshot()
        item = next((item for item in snapshot["items"] if item["item_id"] == item_id), None)
        if item is None:
            raise SourceError("source_item_unavailable", 404)
        if item["source_sha256"] != expected or item["updated_at"] != updated_at:
            raise SourceError("source_changed", 409)
        source = next(source for source in self.sources if source.source_id == item["source_id"])
        # Resolve opaque ID from a fresh inventory; never treat client ID as a path.
        _, private_items, _ = self._scan(source)
        private = next((r for r in private_items if r["item_id"] == item_id), None)
        if private is None:
            raise SourceError("source_item_unavailable", 404)
        try:
            root = _open_root(source.root)
            try:
                text, digest, stamp = _read_file(root, private["_relative"])
            finally:
                os.close(root)
        except OSError:
            raise SourceError("source_item_unavailable", 404) from None
        if digest != expected or stamp != updated_at:
            raise SourceError("source_changed", 409)
        content, count = scrub_text(text)
        return {
            "item": item,
            "content": content,
            "format": "text",
            "redacted": count > 0,
            "redaction_count": count,
            "displayed_sha256": hashlib.sha256(content.encode()).hexdigest(),
            "checked_at": datetime.now(UTC).isoformat(),
        }


def install_mounted_sources(app: FastAPI, sources: MountedSources) -> None:
    router = APIRouter(prefix="/api/v1/mounted-sources", dependencies=[Depends(require_session)])

    @router.get("")
    def inventory() -> dict:
        return sources.snapshot()

    @router.get("/{item_id}")
    def selected_text(
        item_id: str,
        expected: str = Query(pattern=r"^[0-9a-f]{64}$"),
        updated_at: str = Query(min_length=20, max_length=64),
    ) -> dict:
        try:
            return sources.read(item_id, expected, updated_at)
        except SourceError as error:
            raise HTTPException(error.status, detail={"code": error.code}) from None

    # These GET endpoints must precede the native final SPA catch-all.
    app.router.routes[0:0] = router.routes

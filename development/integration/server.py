"""Owner-only ASGI boundary for the pinned raytsystem application.

CRM remains the authority: an opaque owner cookie is verified over loopback on
EVERY request. The raytsystem session and CSRF checks remain a separate inner
boundary. Run this module on a UNIX socket, never a public TCP listener.
"""

from __future__ import annotations

import asyncio
import http.client
import json
import os
import re
import socket
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from http.cookies import CookieError, SimpleCookie
from pathlib import Path
from urllib.parse import unquote_to_bytes, urlsplit

from starlette.types import ASGIApp, Message, Receive, Scope, Send

CRM_COOKIE = "__Host-anodhas_session"
INNER_COOKIE = "raytsystem_session"
EXTERNAL_COOKIE = "anodhas_development_session"
PREFIX = "/development/"
SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
METHODS = SAFE_METHODS | {"POST", "PUT", "PATCH", "DELETE"}
TOKEN = re.compile(r"[A-Za-z0-9_-]{32,256}\Z")
FORWARDED_HEADERS = frozenset(
    {
        b"host",
        b"origin",
        b"accept",
        b"accept-encoding",
        b"accept-language",
        b"content-type",
        b"content-length",
        b"x-csrf-token",
        b"idempotency-key",
        b"range",
        b"if-range",
        b"if-none-match",
        b"if-modified-since",
    }
)


@dataclass(frozen=True)
class Settings:
    owner_id: int
    public_origin: str = "https://example.invalid"
    workspace: Path = Path("/data/workspace")
    socket: Path = Path("/run/development/raytsystem.sock")
    static_dir: Path | None = None
    crm_auth_socket: Path | None = None
    obsidian_dir: Path = Path("/data/obsidian")
    hermes_skills_dir: Path = Path("/data/hermes-skills")

    def __post_init__(self) -> None:
        if type(self.owner_id) is not int or self.owner_id <= 0:
            raise ValueError("A positive numeric ANODHAS_OWNER_ID is mandatory")
        parsed = urlsplit(self.public_origin)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.path
            or parsed.query
            or parsed.fragment
            or parsed.netloc != parsed.netloc.lower()
        ):
            raise ValueError("DEVELOPMENT_PUBLIC_ORIGIN must be an exact HTTPS origin")
        # Validate malformed ports at startup, rather than during a request.
        _ = parsed.port
        if self.crm_auth_socket is not None and not self.crm_auth_socket.is_absolute():
            raise ValueError("DEVELOPMENT_CRM_AUTH_SOCKET must be an absolute UNIX socket path")

    @property
    def host(self) -> str:
        return urlsplit(self.public_origin).netloc

    @classmethod
    def from_env(cls) -> Settings:
        raw = os.environ.get("ANODHAS_OWNER_ID", "")
        if not re.fullmatch(r"[1-9][0-9]*", raw):
            raise ValueError("ANODHAS_OWNER_ID is mandatory")
        static = os.environ.get("DEVELOPMENT_STATIC_DIR")
        auth_socket = os.environ.get("DEVELOPMENT_CRM_AUTH_SOCKET")
        return cls(
            owner_id=int(raw),
            public_origin=os.environ.get("DEVELOPMENT_PUBLIC_ORIGIN", "https://example.invalid"),
            workspace=Path(os.environ.get("DEVELOPMENT_WORKSPACE", "/data/workspace")),
            socket=Path(os.environ.get("DEVELOPMENT_SOCKET", "/run/development/raytsystem.sock")),
            static_dir=Path(static) if static else None,
            crm_auth_socket=Path(auth_socket) if auth_socket else None,
            obsidian_dir=Path(os.environ.get("DEVELOPMENT_OBSIDIAN_DIR", "/data/obsidian")),
            hermes_skills_dir=Path(
                os.environ.get("DEVELOPMENT_HERMES_SKILLS_DIR", "/data/hermes-skills")
            ),
        )


class CRMUnavailable(Exception):
    """A bounded, non-sensitive CRM availability failure."""


SessionResponse = tuple[int, object]
SessionFetcher = Callable[[str], Awaitable[SessionResponse]]


class LoopbackCRM:
    """Fixed loopback HTTP target; no redirects, proxies or client-controlled URL."""

    def __init__(self, host: str, *, timeout: float = 3.0, auth_socket: Path | None = None) -> None:
        self.host = host
        self.timeout = timeout
        self.auth_socket = auth_socket

    async def __call__(self, token: str) -> SessionResponse:
        return await asyncio.to_thread(self._fetch, token)

    def _fetch(self, token: str) -> SessionResponse:
        connection = (
            UnixHTTPConnection(self.auth_socket, timeout=self.timeout)
            if self.auth_socket
            else http.client.HTTPConnection("127.0.0.1", 18081, timeout=self.timeout)
        )
        try:
            connection.request(
                "GET",
                "/api/session",
                headers={
                    "Host": self.host,
                    "Cookie": f"{CRM_COOKIE}={token}",
                    "Accept": "application/json",
                    "Connection": "close",
                },
            )
            response = connection.getresponse()
            if response.status in {401, 403}:
                return response.status, None
            if response.status != 200:
                raise CRMUnavailable()
            if (
                response.getheader("Content-Type", "").split(";", 1)[0].strip()
                != "application/json"
            ):
                raise CRMUnavailable()
            body = response.read(16 * 1024 + 1)
            if len(body) > 16 * 1024:
                raise CRMUnavailable()
            return 200, json.loads(body)
        except (OSError, http.client.HTTPException, ValueError, UnicodeError):
            raise CRMUnavailable() from None
        finally:
            connection.close()


class UnixHTTPConnection(http.client.HTTPConnection):
    """Use a private auth relay without giving the data container any network."""

    def __init__(self, path: Path, *, timeout: float) -> None:
        super().__init__("localhost", timeout=timeout)
        self.path = path

    def connect(self) -> None:
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(self.timeout)
        try:
            connection.connect(str(self.path))
        except OSError:
            connection.close()
            raise
        self.sock = connection


def _cookie_values(headers: list[tuple[bytes, bytes]], name: str) -> list[str]:
    values: list[str] = []
    for key, value in headers:
        if key.lower() != b"cookie":
            continue
        for part in value.decode("latin-1").split(";"):
            raw_name, separator, raw_value = part.strip().partition("=")
            if raw_name == name and separator:
                values.append(raw_value)
    return values


def _cookie(headers: list[tuple[bytes, bytes]], name: str) -> str | None:
    """Reject duplicate/ambiguous cookies instead of accepting a parser's winner."""
    values = _cookie_values(headers, name)
    return values[0] if len(values) == 1 and TOKEN.fullmatch(values[0]) else None


def _inner_path(scope: Scope) -> str | None:
    path = scope.get("path", "")
    raw = scope.get("raw_path", path.encode("utf-8"))
    try:
        encoded = raw.decode("ascii")
        if re.search(r"%(?![a-fA-F0-9]{2})", encoded):
            return None
        # A proxy and ASGI server must not disagree on delimiters or traversal.
        if re.search(r"%(?:2e|2f|5c|25)", encoded, re.I):
            return None
        if unquote_to_bytes(encoded).decode("utf-8") != path:
            return None
    except (UnicodeError, AttributeError):
        return None
    if (
        not path.startswith(PREFIX)
        or "\\" in path
        or "//" in path
        or any(ord(c) < 32 or ord(c) == 127 for c in path)
        or any(part in {".", ".."} for part in path.split("/"))
    ):
        return None
    return "/" + path[len(PREFIX) :]


class OwnerGateway:
    def __init__(self, app: ASGIApp, settings: Settings, fetch_session: SessionFetcher) -> None:
        self.app, self.settings, self.fetch_session = app, settings, fetch_session

    def _valid_owner(self, payload: object) -> bool:
        if not isinstance(payload, dict) or set(payload) != {
            "owner_id",
            "csrf",
            "expires_at",
        }:
            return False
        if type(payload["owner_id"]) is not int or payload["owner_id"] != self.settings.owner_id:
            return False
        if not isinstance(payload["csrf"], str) or not re.fullmatch(
            r"[0-9a-f]{64}", payload["csrf"]
        ):
            return False
        if not isinstance(payload["expires_at"], str):
            return False
        try:
            expiry = datetime.fromisoformat(payload["expires_at"])
            return expiry.tzinfo is not None and expiry > datetime.now(UTC)
        except ValueError:
            return False

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            await self.app(scope, receive, send)
            return
        if scope["type"] not in {"http", "websocket"}:
            return
        path = _inner_path(scope)
        headers = list(scope.get("headers", []))
        hosts = [v.decode("latin-1").lower() for k, v in headers if k.lower() == b"host"]
        origins = [v.decode("latin-1") for k, v in headers if k.lower() == b"origin"]
        status, code = 0, ""
        if path is None:
            status, code = 404, "not_found"
        elif hosts != [self.settings.host]:
            status, code = 421, "host_rejected"
        elif len(origins) > 1 or (origins and origins != [self.settings.public_origin]):
            status, code = 403, "origin_rejected"
        else:
            token = _cookie(headers, CRM_COOKIE)
            if token is None:
                status, code = 401, "login_required"
            else:
                try:
                    crm_status, payload = await self.fetch_session(token)
                    if crm_status in {401, 403}:
                        status, code = crm_status, "login_required"
                    elif crm_status != 200:
                        status, code = 503, "session_unavailable"
                    elif not self._valid_owner(payload):
                        status, code = 401, "login_required"
                except (CRMUnavailable, OSError, TimeoutError):
                    status, code = 503, "session_unavailable"
        if scope["type"] == "websocket":
            # The upstream has no supported WebSocket API. Never pass upgrades
            # through its HTTP-only security middleware, even after owner auth.
            await send({"type": "websocket.close", "code": 1008 if status else 1003})
            return
        if status:
            await self._reject(send, status, code)
            return
        if len(_cookie_values(headers, EXTERNAL_COOKIE)) > 1:
            await self._reject(send, 401, "development_session_invalid")
            return
        method = scope.get("method", "GET").upper()
        if method not in METHODS:
            await self._reject(send, 405, "method_rejected")
            return
        if method not in SAFE_METHODS and origins != [self.settings.public_origin]:
            await self._reject(send, 403, "origin_rejected")
            return
        inner_headers = [(k.lower(), v) for k, v in headers if k.lower() in FORWARDED_HEADERS]
        # The original local raytsystem cookie may coexist on the same host.
        # Only our separate browser namespace can become an upstream session.
        inner_cookie = _cookie(headers, EXTERNAL_COOKIE)
        if inner_cookie is not None:
            inner_headers.append((b"cookie", f"{INNER_COOKIE}={inner_cookie}".encode("ascii")))
        inner_scope = dict(
            scope,
            path=path,
            raw_path=path.encode("utf-8"),
            root_path="",
            headers=inner_headers,
            state={},
        )

        async def secure_send(message: Message) -> None:
            if message["type"] == "http.response.start":
                message = dict(message, headers=self._response_headers(message.get("headers", [])))
            await send(message)

        await self.app(inner_scope, receive, secure_send)

    def _response_headers(self, headers: list[tuple[bytes, bytes]]) -> list[tuple[bytes, bytes]]:
        result: list[tuple[bytes, bytes]] = []
        for key, value in headers:
            key = key.lower()
            if key in {b"cache-control", b"x-frame-options"} or key.startswith(b"access-control-"):
                continue
            if key == b"set-cookie":
                cookie = SimpleCookie()
                try:
                    cookie.load(value.decode("latin-1"))
                except CookieError:
                    continue
                if set(cookie) != {INNER_COOKIE}:
                    continue
                morsel = cookie[INNER_COOKIE]
                if not TOKEN.fullmatch(morsel.value):
                    continue
                rewritten = SimpleCookie()
                rewritten[EXTERNAL_COOKIE] = morsel.value
                for attribute in ("max-age", "expires"):
                    if morsel[attribute]:
                        rewritten[EXTERNAL_COOKIE][attribute] = morsel[attribute]
                rewritten[EXTERNAL_COOKIE]["path"] = PREFIX
                rewritten[EXTERNAL_COOKIE]["secure"] = True
                rewritten[EXTERNAL_COOKIE]["httponly"] = True
                rewritten[EXTERNAL_COOKIE]["samesite"] = "strict"
                value = rewritten[EXTERNAL_COOKIE].OutputString().encode("latin-1")
            elif key == b"content-security-policy":
                value = re.sub(
                    rb"(?i)(^|;\s*)frame-ancestors\s+'none'(?=\s*(?:;|$))",
                    rb"\1frame-ancestors 'self'",
                    value,
                )
            elif key == b"location":
                location = value.decode("latin-1")
                if location.startswith("/") and not location.startswith("//"):
                    value = (PREFIX.rstrip("/") + location).encode("latin-1")
                else:
                    # No external redirects or redirects into the CRM surface.
                    continue
            result.append((key, value))
        result.extend([(b"cache-control", b"no-store"), (b"x-frame-options", b"SAMEORIGIN")])
        return result

    @staticmethod
    async def _reject(send: Send, status: int, code: str) -> None:
        body = json.dumps({"error": {"code": code}}, separators=(",", ":")).encode()
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode()),
                    (b"cache-control", b"no-store"),
                    (b"x-content-type-options", b"nosniff"),
                    (
                        b"content-security-policy",
                        b"default-src 'none'; frame-ancestors 'self'",
                    ),
                    (b"x-frame-options", b"SAMEORIGIN"),
                    (b"referrer-policy", b"no-referrer"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})


def build_app(
    settings: Settings | None = None, *, fetch_session: SessionFetcher | None = None
) -> OwnerGateway:
    from raytsystem.webapp import create_app

    settings = settings or Settings.from_env()
    inner = create_app(
        settings.workspace,
        allowed_hosts=frozenset({settings.host}),
        allowed_origins=frozenset({settings.public_origin}),
        static_dir=settings.static_dir,
    )
    from mounted_sources import MountedSources, install_mounted_sources

    install_mounted_sources(
        inner, MountedSources(settings.obsidian_dir, settings.hermes_skills_dir)
    )
    from role_profiles import install_role_profiles

    install_role_profiles(inner)
    return OwnerGateway(
        inner,
        settings,
        fetch_session or LoopbackCRM(settings.host, auth_socket=settings.crm_auth_socket),
    )


def main() -> None:
    import uvicorn

    settings = Settings.from_env()
    # The service manager supplies a private runtime directory (0700). A
    # restrictive socket umask keeps the transport private to the proxy group.
    os.umask(0o007)

    class PrivateSocketServer(uvicorn.Server):
        async def startup(self, sockets=None):
            await super().startup(sockets=sockets)
            # Uvicorn explicitly chmods UNIX sockets to 0666. Keep transport
            # writable only by the workspace owner and the existing CRM proxy.
            os.chmod(settings.socket, 0o660)

    config = uvicorn.Config(
        build_app(settings),
        uds=str(settings.socket),
        proxy_headers=False,
        access_log=False,
        server_header=False,
    )
    PrivateSocketServer(config).run()


if __name__ == "__main__":
    main()

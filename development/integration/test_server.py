from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi import Request
from fastapi.testclient import TestClient
from server import (
    CRM_COOKIE,
    EXTERNAL_COOKIE,
    INNER_COOKIE,
    CRMUnavailable,
    LoopbackCRM,
    OwnerGateway,
    Settings,
    build_app,
)

ORIGIN = "https://example.invalid"
VALID_COOKIE = "a" * 64
RAYT_COOKIE = "b" * 43


class CRM:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.status = 200
        self.payload = {
            "owner_id": 123,
            "csrf": "c" * 64,
            "expires_at": (datetime.now(UTC) + timedelta(minutes=30)).isoformat(),
        }
        self.unavailable = False

    async def __call__(self, token: str):
        self.calls.append(token)
        if self.unavailable:
            raise CRMUnavailable()
        return self.status, self.payload


@pytest.fixture
def protected(tmp_path: Path):
    static = tmp_path / "static"
    (static / "assets").mkdir(parents=True)
    (static / "index.html").write_text(
        '<html><head><meta name="raytsystem-csp-nonce" '
        'content="__RAYTSYSTEM_CSP_NONCE__"></head><body>Private UI</body></html>'
    )
    (static / "assets" / "private.js").write_text("window.privateOwnerUI=true;")
    crm = CRM()
    gateway = build_app(
        Settings(123, workspace=tmp_path, static_dir=static), fetch_session=crm
    )
    seen = []

    @gateway.app.post("/api/v1/security-probe")
    async def mutation(request: Request):
        seen.append(dict(request.headers))
        return {"accepted": True}

    @gateway.app.get("/api/v1/headers-probe")
    async def headers_probe(request: Request):
        return {
            "headers": dict(request.headers),
            "path": request.scope["path"],
            "query": request.scope["query_string"].decode(),
        }

    # Place the diagnostic read before upstream's final GET SPA catch-all.
    gateway.app.router.routes.insert(0, gateway.app.router.routes.pop())

    with TestClient(gateway, base_url=ORIGIN) as client:
        yield client, crm, gateway, seen


def authorize(client: TestClient) -> str:
    client.cookies.set(CRM_COOKIE, VALID_COOKIE)
    assert client.get("/development/").status_code == 200
    session = client.get("/development/api/v1/session")
    assert session.status_code == 200
    return session.json()["csrf_token"]


@pytest.mark.parametrize(
    "path",
    [
        "/development/",
        "/development/assets/private.js",
        "/development/api/v1/session",
        "/development/handbook",
    ],
)
def test_all_paths_require_crm(protected, path):
    client, crm, _, _ = protected
    response = client.get(path)
    assert response.status_code == 401
    assert response.headers["cache-control"] == "no-store"
    assert "privateOwnerUI" not in response.text
    assert crm.calls == []


@pytest.mark.parametrize(
    "headers",
    [
        {"X-Authenticated": "true", "X-Owner-Id": "123"},
        {"Authorization": "Bearer " + VALID_COOKIE},
        {"X-Forwarded-User": "123", "X-Auth-Request-User": "123"},
        {"Cookie": f"{INNER_COOKIE}={RAYT_COOKIE}"},
        {"Cookie": f"{CRM_COOKIE}=raw"},
        {"Cookie": f"{CRM_COOKIE}={VALID_COOKIE}; {CRM_COOKIE}={VALID_COOKIE}"},
        {"Cookie": f'{CRM_COOKIE}="{VALID_COOKIE}"'},
    ],
)
def test_raw_cookies_and_bypass_headers_deny(protected, headers):
    client, crm, _, _ = protected
    assert client.get("/development/", headers=headers).status_code == 401
    assert crm.calls == []


@pytest.mark.parametrize("status", [401, 403])
def test_forged_owner_cookie_is_verified_and_denied(protected, status):
    client, crm, _, _ = protected
    crm.status = status
    response = client.get(
        "/development/", headers={"Cookie": f"{CRM_COOKIE}={VALID_COOKIE}"}
    )
    assert response.status_code == status
    assert crm.calls == [VALID_COOKIE]
    assert "set-cookie" not in response.headers


def test_logout_and_expiry_recheck_every_asset_and_api_request(protected):
    client, crm, _, _ = protected
    authorize(client)
    assert len(crm.calls) == 2
    assert client.get("/development/assets/private.js").status_code == 200
    assert len(crm.calls) == 3
    crm.payload["expires_at"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    assert client.get("/development/assets/private.js").status_code == 401
    assert len(crm.calls) == 4
    crm.status = 401
    assert client.get("/development/api/v1/session").status_code == 401
    assert len(crm.calls) == 5


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"authenticated": True},
        {"owner_id": "123", "csrf": "c" * 64, "expires_at": "2099-01-01T00:00:00Z"},
        {"owner_id": True, "csrf": "c" * 64, "expires_at": "2099-01-01T00:00:00Z"},
        {"owner_id": 999, "csrf": "c" * 64, "expires_at": "2099-01-01T00:00:00Z"},
        {"owner_id": 123, "csrf": "bad", "expires_at": "2099-01-01T00:00:00Z"},
        {"owner_id": 123, "csrf": "c" * 64, "expires_at": "2099-01-01T00:00:00"},
        {"owner_id": 123, "csrf": "c" * 64, "expires_at": "broken"},
        {"owner_id": 123, "csrf": "c" * 64, "expires_at": 123456},
    ],
)
def test_only_exact_positive_owner_response_passes(protected, payload):
    client, crm, _, _ = protected
    client.cookies.set(CRM_COOKIE, VALID_COOKIE)
    crm.payload = payload
    assert client.get("/development/").status_code == 401


@pytest.mark.parametrize("status", [302, 404, 500, 503])
def test_crm_non_auth_failure_is_unavailable(protected, status):
    client, crm, _, _ = protected
    client.cookies.set(CRM_COOKIE, VALID_COOKIE)
    crm.status = status
    response = client.get("/development/")
    assert response.status_code == 503
    assert response.headers["cache-control"] == "no-store"


def test_crm_outage_never_uses_previous_positive_result(protected):
    client, crm, _, _ = protected
    authorize(client)
    crm.unavailable = True
    response = client.get("/development/assets/private.js")
    assert response.status_code == 503
    assert len(crm.calls) == 3
    assert VALID_COOKIE not in response.text


@pytest.mark.parametrize(
    "headers, status",
    [
        ({"Host": "evil.example"}, 421),
        ({"Host": "127.0.0.1:18081"}, 421),
        ({"Origin": "http://example.invalid"}, 403),
        ({"Origin": "https://evil.example"}, 403),
        ({"Origin": "null"}, 403),
    ],
)
def test_wrong_host_and_origin_are_rejected(protected, headers, status):
    client, crm, _, _ = protected
    client.cookies.set(CRM_COOKIE, VALID_COOKIE)
    assert client.get("/development/", headers=headers).status_code == status
    assert crm.calls == []


@pytest.mark.parametrize(
    "path",
    [
        "/",
        "/api/session",
        "/admin/",
        "/development-other/",
        "/development",
        "/development/%2e%2e/admin",
        "/development/%252e%252e/admin",
        "/development/%2fapi/v1/session",
        "/development/%5cadmin",
        "/development//api/v1/session",
        "/development/%00private",
        "/development/%ZZ",
    ],
)
def test_no_prefix_escape_or_encoded_traversal(protected, path):
    client, crm, _, _ = protected
    client.cookies.set(CRM_COOKIE, VALID_COOKIE)
    assert client.get(path, follow_redirects=False).status_code == 404
    assert crm.calls == []


def test_headers_and_cookie_are_isolated(protected):
    client, _, _, _ = protected
    authorize(client)
    response = client.get(
        "/development/api/v1/headers-probe?query=kept",
        headers={
            "Authorization": "Bearer private-crm-token",
            "X-Owner-Id": "123",
            "X-Forwarded-Host": "evil.example",
            "X-Forwarded-Proto": "http",
        },
    )
    data = response.json()
    assert data["path"] == "/api/v1/headers-probe"
    assert data["query"] == "query=kept"
    assert data["headers"]["cookie"].startswith(INNER_COOKIE + "=")
    assert CRM_COOKIE not in response.text
    assert VALID_COOKIE not in response.text
    for key in ("authorization", "x-owner-id", "x-forwarded-host", "x-forwarded-proto"):
        assert key not in data["headers"]


def test_secure_cookie_and_narrow_same_origin_framing(protected):
    client, _, _, _ = protected
    client.cookies.set(CRM_COOKIE, VALID_COOKIE)
    response = client.get("/development/")
    cookie = response.headers["set-cookie"]
    assert cookie.startswith(EXTERNAL_COOKIE + "=")
    assert INNER_COOKIE + "=" not in cookie
    assert "Secure" in cookie and "HttpOnly" in cookie and "SameSite=strict" in cookie
    assert "Path=/development/" in cookie
    assert "Domain=" not in cookie and CRM_COOKIE not in cookie
    csp = response.headers["content-security-policy"]
    assert "frame-ancestors 'self'" in csp and "frame-src 'none'" in csp
    assert "script-src 'self'" in csp and "object-src 'none'" in csp
    assert response.headers["x-frame-options"] == "SAMEORIGIN"
    assert response.headers["cache-control"] == "no-store"
    assert (
        client.get("/development/assets/private.js").headers["cache-control"]
        == "no-store"
    )


def test_inner_session_csrf_json_and_idempotency_remain_required(protected):
    client, _, _, seen = protected
    client.cookies.set(CRM_COOKIE, VALID_COOKIE)
    assert client.get("/development/api/v1/session").status_code == 401
    csrf = authorize(client)
    endpoint = "/development/api/v1/security-probe"
    assert client.post(endpoint, json={}).status_code == 403  # HTTPS Origin required
    assert client.post(endpoint, json={}, headers={"Origin": ORIGIN}).status_code == 403
    headers = {
        "Origin": ORIGIN,
        "X-CSRF-Token": "c" * 64,
        "Idempotency-Key": "probe-key-001",
    }
    assert (
        client.post(endpoint, json={}, headers=headers).status_code == 403
    )  # CRM CSRF != rayt CSRF
    headers["X-CSRF-Token"] = csrf
    headers.pop("Idempotency-Key")
    assert client.post(endpoint, json={}, headers=headers).status_code == 400
    headers["Idempotency-Key"] = "short"
    assert client.post(endpoint, json={}, headers=headers).status_code == 400
    headers["Idempotency-Key"] = "probe-key-001"
    assert client.post(endpoint, content="not-json", headers=headers).status_code == 415
    assert seen == []
    assert client.post(endpoint, json={}, headers=headers).json() == {"accepted": True}
    assert len(seen) == 1
    assert seen[0]["idempotency-key"] == "probe-key-001"


def test_only_rayt_response_cookie_survives_and_redirect_stays_under_prefix():
    gateway = OwnerGateway(lambda *args: None, Settings(123), CRM())
    headers = gateway._response_headers(
        [
            (b"set-cookie", f"{CRM_COOKIE}={VALID_COOKIE}; Path=/".encode()),
            (
                b"set-cookie",
                f"{INNER_COOKIE}={RAYT_COOKIE}; Domain=evil.example; Path=/".encode(),
            ),
            (b"location", b"/handbook"),
            (b"access-control-allow-origin", b"*"),
        ]
    )
    assert [value for key, value in headers if key == b"location"] == [
        b"/development/handbook"
    ]
    cookies = [value for key, value in headers if key == b"set-cookie"]
    assert len(cookies) == 1 and b"Domain=" not in cookies[0]
    assert cookies[0].startswith(EXTERNAL_COOKIE.encode() + b"=")
    assert not any(key.startswith(b"access-control-") for key, _ in headers)
    assert not any(
        key == b"location"
        for key, _ in gateway._response_headers(
            [(b"location", b"https://evil.example/")]
        )
    )


def test_local_cookie_collision_does_not_replace_development_session(protected):
    client, _, _, _ = protected
    authorize(client)
    session = client.cookies.get(EXTERNAL_COOKIE)
    response = client.get(
        "/development/api/v1/headers-probe",
        headers={
            "Cookie": f"{CRM_COOKIE}={VALID_COOKIE}; {EXTERNAL_COOKIE}={session}; "
            f"{INNER_COOKIE}={RAYT_COOKIE}; {INNER_COOKIE}={'d' * 43}"
        },
    )
    assert response.status_code == 200
    assert response.json()["headers"]["cookie"] == f"{INNER_COOKIE}={session}"
    assert EXTERNAL_COOKIE not in response.text
    assert RAYT_COOKIE not in response.text


def test_original_local_cookie_is_never_forwarded_as_development_session(protected):
    client, _, _, _ = protected
    authorize(client)
    # Even a known valid inner token must be presented in our browser namespace.
    session = client.cookies.get(EXTERNAL_COOKIE)
    response = client.get(
        "/development/api/v1/session",
        headers={"Cookie": f"{CRM_COOKIE}={VALID_COOKIE}; {INNER_COOKIE}={session}"},
    )
    assert response.status_code == 401


@pytest.mark.parametrize("path", ["/development/", "/development/api/v1/session"])
def test_duplicate_development_cookie_rejected_without_picking_a_winner(
    protected, path
):
    client, _, _, _ = protected
    authorize(client)
    session = client.cookies.get(EXTERNAL_COOKIE)
    response = client.get(
        path,
        headers={
            "Cookie": f"{CRM_COOKIE}={VALID_COOKIE}; "
            f"{EXTERNAL_COOKIE}={session}; {EXTERNAL_COOKIE}={RAYT_COOKIE}"
        },
    )
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "development_session_invalid"
    assert "set-cookie" not in response.headers


def test_websocket_auth_is_checked_but_upgrade_is_unsupported():
    crm = CRM()
    gateway = OwnerGateway(lambda *args: None, Settings(123), crm)

    async def check(cookie: bool):
        messages = []

        async def send(message):
            messages.append(message)

        headers = [(b"host", b"example.invalid")]
        if cookie:
            headers.append((b"cookie", f"{CRM_COOKIE}={VALID_COOKIE}".encode()))
        await gateway(
            {"type": "websocket", "path": "/development/ws", "headers": headers},
            lambda: None,
            send,
        )
        return messages

    assert asyncio.run(check(False)) == [{"type": "websocket.close", "code": 1008}]
    assert crm.calls == []
    assert asyncio.run(check(True)) == [{"type": "websocket.close", "code": 1003}]
    assert crm.calls == [VALID_COOKIE]


def test_settings_require_numeric_owner_and_https_origin(monkeypatch):
    monkeypatch.delenv("ANODHAS_OWNER_ID", raising=False)
    with pytest.raises(ValueError):
        Settings.from_env()
    for value in ("", "0", "-123", "owner", " 123"):
        monkeypatch.setenv("ANODHAS_OWNER_ID", value)
        with pytest.raises(ValueError):
            Settings.from_env()
    for origin in (
        "http://example.invalid",
        "https://example.invalid/",
        "https://example.invalid/path",
        "https://user:contact006@example.invalid",
        "https://example.invalid?x=1",
    ):
        with pytest.raises(ValueError):
            Settings(123, public_origin=origin)
    with pytest.raises(ValueError):
        Settings(123, crm_auth_socket=Path("relative.sock"))


def test_unix_auth_relay_has_fixed_request_and_real_response():
    import json
    import socketserver
    import tempfile
    import threading

    calls = []
    payload = CRM().payload
    # macOS limits AF_UNIX paths to 104 bytes; pytest's temp root is longer.
    temporary = tempfile.TemporaryDirectory(dir="/tmp", prefix="da-")
    path = Path(temporary.name) / "crm-auth.sock"

    class Handler(socketserver.StreamRequestHandler):
        def handle(self):
            request = []
            while True:
                line = self.rfile.readline()
                request.append(line)
                if line == b"\r\n":
                    break
            calls.append(b"".join(request))
            body = json.dumps(payload).encode()
            self.wfile.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
                + body
            )

    relay = socketserver.UnixStreamServer(str(path), Handler)
    thread = threading.Thread(target=relay.handle_request, daemon=True)
    thread.start()
    try:
        result = asyncio.run(LoopbackCRM("example.invalid", auth_socket=path)(VALID_COOKIE))
        assert result == (200, payload)
        thread.join(timeout=3)
        assert not thread.is_alive()
        assert calls[0].startswith(b"GET /api/session HTTP/1.1\r\n")
        assert b"Host: example.invalid\r\n" in calls[0]
        assert f"Cookie: {CRM_COOKIE}={VALID_COOKIE}\r\n".encode() in calls[0]
        assert b"Authorization:" not in calls[0] and b"X-Owner-Id:" not in calls[0]
    finally:
        relay.server_close()
        temporary.cleanup()


def test_loopback_transport_fixed_endpoint_bounded_and_no_client_headers(monkeypatch):
    calls = []

    class Connection:
        def __init__(self, host, port, timeout):
            calls.append((host, port, timeout))

        def request(self, method, path, headers):
            calls.append((method, path, headers))

        def getresponse(self):
            return self

        status = 200

        def getheader(self, name, default):
            return "application/json; charset=utf-8"

        def read(self, size):
            calls.append(size)
            return b'{"owner_id":123}'

        def close(self):
            calls.append("closed")

    monkeypatch.setattr("server.http.client.HTTPConnection", Connection)
    response = asyncio.run(LoopbackCRM("example.invalid")(VALID_COOKIE))
    assert response == (200, {"owner_id": 123})
    assert calls[0] == ("127.0.0.1", 18081, 3.0)
    assert calls[1][0:2] == ("GET", "/api/session")
    assert calls[1][2] == {
        "Host": "example.invalid",
        "Cookie": f"{CRM_COOKIE}={VALID_COOKIE}",
        "Accept": "application/json",
        "Connection": "close",
    }
    assert calls[2:] == [16385, "closed"]


@pytest.mark.parametrize(
    "status, content_type, body",
    [
        (302, "application/json", b"{}"),
        (500, "application/json", b"{}"),
        (200, "text/html", b"{}"),
        (200, "application/json", b"not-json"),
        (200, "application/json", b"a" * 16385),
    ],
)
def test_loopback_malformed_and_redirect_responses_fail_closed(
    monkeypatch, status, content_type, body
):
    class Connection:
        def __init__(self, *args, **kwargs):
            self.status = status

        def request(self, *args, **kwargs):
            pass

        def getresponse(self):
            return self

        def getheader(self, *args):
            return content_type

        def read(self, size):
            return body

        def close(self):
            pass

    monkeypatch.setattr("server.http.client.HTTPConnection", Connection)
    with pytest.raises(CRMUnavailable):
        asyncio.run(LoopbackCRM("example.invalid")(VALID_COOKIE))

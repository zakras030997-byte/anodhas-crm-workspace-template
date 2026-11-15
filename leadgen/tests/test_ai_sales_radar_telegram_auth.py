from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
import stat
import tempfile
import unittest

from telethon.errors import SessionPasswordNeededError

from ai_sales_radar.config import TelegramCredentials
from ai_sales_radar.telegram_auth import (
    TelegramAuthService,
    disconnect_client_fully,
    protect_session_files,
)


class FakeLock:
    def __init__(self, _path, *, role, events):
        self.events = events
        self.role = role

    def acquire(self):
        self.events.append("lock.acquire")

    def release(self):
        self.events.append("lock.release")


class FakeClient:
    def __init__(
        self,
        *,
        authorized=False,
        require_2fa=False,
        disconnect_error=None,
        logout_result=True,
        events=None,
    ):
        self.authorized = authorized
        self.require_2fa = require_2fa
        self.disconnect_error = disconnect_error
        self.logout_result = logout_result
        self.events = events if events is not None else []
        self.send_code_calls = 0
        self.sign_in_calls = []

    async def connect(self):
        self.events.append("client.connect")

    async def disconnect(self):
        self.events.append("client.disconnect")
        if self.disconnect_error is not None:
            raise self.disconnect_error

    async def is_user_authorized(self):
        return self.authorized

    async def get_me(self):
        return SimpleNamespace(id=70001, username="sales_radar")

    async def send_code_request(self, phone):
        self.send_code_calls += 1
        self.events.append("send_code")
        return SimpleNamespace(phone_code_hash="private-hash")

    async def sign_in(self, *args, **kwargs):
        self.sign_in_calls.append(kwargs)
        if "code" in kwargs and self.require_2fa:
            raise SessionPasswordNeededError(request=None)
        self.authorized = True

    async def log_out(self):
        self.authorized = False
        self.events.append("log_out")
        return self.logout_result


def service_for(client, root, events):
    credentials = TelegramCredentials(
        api_id=12345,
        api_hash="private-api-hash",
        session_path=root / "collector.session",
    )

    def lock_factory(path, *, role):
        return FakeLock(path, role=role, events=events)

    def client_factory(_credentials):
        events.append("client.factory")
        return client

    return TelegramAuthService(
        credentials,
        client_factory=client_factory,
        lock_factory=lock_factory,
        runtime_root=root,
    )


class TelegramAuthServiceTest(unittest.IsolatedAsyncioTestCase):
    async def test_disconnect_cancellation_waits_for_session_flush(self):
        started = asyncio.Event()
        release = asyncio.Event()
        finished = asyncio.Event()

        class BlockingClient:
            async def disconnect(self):
                started.set()
                await release.wait()
                finished.set()

        task = asyncio.create_task(disconnect_client_fully(BlockingClient()))
        await started.wait()
        task.cancel()
        await asyncio.sleep(0)

        self.assertFalse(task.done())
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(finished.is_set())

    async def test_existing_session_skips_code_and_factory_runs_after_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            events = []
            client = FakeClient(authorized=True, events=events)
            service = service_for(client, Path(directory), events)

            status = await service.login(
                phone_provider=lambda: self.fail("phone must not be requested"),
                code_provider=lambda: self.fail("code must not be requested"),
                password_provider=lambda: self.fail("password must not be requested"),
            )

        self.assertTrue(status.authorized)
        self.assertEqual(status.account.user_id, 70001)
        self.assertEqual(client.send_code_calls, 0)
        self.assertLess(events.index("lock.acquire"), events.index("client.factory"))
        self.assertEqual(events[-2:], ["client.disconnect", "lock.release"])

    async def test_login_uses_explicit_code_flow(self):
        with tempfile.TemporaryDirectory() as directory:
            events = []
            client = FakeClient(events=events)
            service = service_for(client, Path(directory), events)

            status = await service.login(
                phone_provider=lambda: "+10000000000",
                code_provider=lambda: "12345",
                password_provider=lambda: self.fail("2FA must not be requested"),
            )

        self.assertTrue(status.authorized)
        self.assertEqual(client.send_code_calls, 1)
        self.assertEqual(client.sign_in_calls[0]["code"], "12345")
        self.assertEqual(client.sign_in_calls[0]["phone_code_hash"], "private-hash")

    async def test_login_supports_two_factor_password(self):
        with tempfile.TemporaryDirectory() as directory:
            events = []
            client = FakeClient(require_2fa=True, events=events)
            service = service_for(client, Path(directory), events)

            status = await service.login(
                phone_provider=lambda: "+10000000000",
                code_provider=lambda: "12345",
                password_provider=lambda: "second-factor",
            )

        self.assertTrue(status.authorized)
        self.assertEqual(client.sign_in_calls[-1], {"password": "second-factor"})

    async def test_logout_is_explicit_and_disconnects(self):
        with tempfile.TemporaryDirectory() as directory:
            events = []
            client = FakeClient(authorized=True, events=events)
            service = service_for(client, Path(directory), events)

            self.assertTrue(await service.logout())

        self.assertIn("log_out", events)
        self.assertEqual(events[-2:], ["client.disconnect", "lock.release"])

    async def test_lock_is_released_when_disconnect_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            events = []
            client = FakeClient(
                authorized=True,
                disconnect_error=RuntimeError("disconnect failed"),
                events=events,
            )
            service = service_for(client, Path(directory), events)

            with self.assertRaisesRegex(RuntimeError, "disconnect failed"):
                await service.status()

        self.assertEqual(events[-1], "lock.release")

    async def test_logout_fails_closed_when_telegram_returns_false(self):
        with tempfile.TemporaryDirectory() as directory:
            events = []
            client = FakeClient(
                authorized=True,
                logout_result=False,
                events=events,
            )
            service = service_for(client, Path(directory), events)

            with self.assertRaisesRegex(Exception, "не подтвердил отзыв"):
                await service.logout()

        self.assertEqual(events[-2:], ["client.disconnect", "lock.release"])

    def test_session_and_companion_files_are_private(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            session = root / "collector.session"
            companions = [session, root / "collector.session-wal", root / "collector.session.lock"]
            for path in companions:
                path.write_text("fixture", encoding="utf-8")
                path.chmod(0o644)

            protect_session_files(session, runtime_root=root)

            for path in companions:
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

    def test_session_companion_symlink_is_rejected_without_chmod_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            session = root / "collector.session"
            outside = root.parent / f"{root.name}-outside"
            outside.write_text("outside", encoding="utf-8")
            outside.chmod(0o644)
            companion = root / "collector.session-wal"
            companion.symlink_to(outside)
            try:
                with self.assertRaisesRegex(Exception, "символической ссылкой"):
                    protect_session_files(session, runtime_root=root)
                self.assertEqual(stat.S_IMODE(outside.stat().st_mode), 0o644)
            finally:
                outside.unlink()


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import io
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch
from uuid import UUID

from telethon import types

from freelancer_bot.observability import Redactor, configure_structured_logger
from freelancer_bot.persistence.raw_messages import RawMessageOrigin
from freelancer_bot.persistence.telegram_operation_state import (
    TelegramCollectorPaused,
)

from ai_sales_radar.collector import (
    CollectorOptions,
    TelegramCollectorError,
    TelegramCollectorService,
    build_parser,
)
from ai_sales_radar.collector_folder_gate import (
    CANARY_SOURCE_IDS,
    ExactCanaryTelegramFolderGate,
    TelegramCollectorFolderGateError,
)
from ai_sales_radar.config import TelegramCredentials
from ai_sales_radar.telegram_identity import (
    StrictTelegramChannelIdentityVerifier,
    TelegramSourceIdentityBinding,
)


NOW = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
TRACE_ID = UUID("45454545-4545-4545-4545-454545454545")


class FakeLock:
    def __init__(self, _path, *, role, events, release_error=None):
        self.events = events
        self.role = role
        self.release_error = release_error

    def acquire(self):
        self.events.append("lock.acquire")

    def release(self):
        self.events.append("lock.release")
        if self.release_error is not None:
            raise self.release_error


class FakeSource:
    def __init__(self, source_id, lookup):
        self.record = SimpleNamespace(id=source_id)
        self.lookup = lookup

    def message_url(self, message_id):
        return f"https://t.me/{self.lookup.removeprefix('@')}/{message_id}"


class FakeAdapter:
    def __init__(self, sources, events, *, active=True):
        self.snapshot = SimpleNamespace(
            collector_account=SimpleNamespace(id=77, is_active=active),
            sources=tuple(sources),
        )
        self.events = events
        self.calls = 0

    async def list_for_session(self, _client):
        self.calls += 1
        self.events.append("adapter.list")
        return self.snapshot


class FakeIngestor:
    def __init__(self, *, duplicate_ids=(), error=None, events=None):
        self.duplicate_ids = set(duplicate_ids)
        self.error = error
        self.messages = []
        self.events = events if events is not None else []

    async def ingest(self, message):
        self.messages.append(message)
        self.events.append(f"ingest:{message.external_message_id}")
        if self.error is not None:
            raise self.error
        return SimpleNamespace(created=message.external_message_id not in self.duplicate_ids)


class FakeGovernor:
    def __init__(self):
        self.categories = []

    async def run(self, category, operation):
        self.categories.append(category)
        return await operation()


class FakeIdentityBindingLoader:
    def __init__(self, bindings=None):
        self.bindings = None if bindings is None else dict(bindings)
        self.calls = []

    async def load(self, *, source_ids, collector_account_id):
        self.calls.append((tuple(source_ids), collector_account_id))
        if self.bindings is None:
            return {
                source_id: TelegramSourceIdentityBinding(
                    source_id=source_id,
                    collector_account_id=collector_account_id,
                    state="accessible",
                    canonical_peer_id=source_id,
                )
                for source_id in source_ids
            }
        return {
            source_id: self.bindings[source_id]
            for source_id in source_ids
            if source_id in self.bindings
        }


class PassThroughIdentityVerifier:
    def verify(self, *, lookup, entity, binding):
        return entity


class FakeFolderGate:
    def __init__(
        self,
        events=None,
        *,
        catalog_error=None,
        scope_error=None,
        verify_error=None,
    ):
        self.events = events
        self.catalog_error = catalog_error
        self.scope_error = scope_error
        self.verify_error = verify_error
        self.catalog_calls = []
        self.scope_calls = []
        self.verify_calls = []

    def validate_catalog(self, eligible_sources):
        source_ids = tuple(source.record.id for source in eligible_sources)
        self.catalog_calls.append(source_ids)
        if self.events is not None:
            self.events.append("folder.catalog")
        if self.catalog_error is not None:
            raise self.catalog_error

    def validate_scope(self, selected_sources):
        source_ids = tuple(source.record.id for source in selected_sources)
        self.scope_calls.append(source_ids)
        if self.events is not None:
            self.events.append("folder.scope")
        if self.scope_error is not None:
            raise self.scope_error

    async def verify(
        self,
        _client,
        *,
        selected_sources,
        resolved_sources,
        governor,
    ):
        self.verify_calls.append(
            (
                tuple(source.record.id for source in selected_sources),
                tuple(source.record.id for source, _entity in resolved_sources),
                governor,
            )
        )
        if self.events is not None:
            self.events.append("folder.verify")
        if self.verify_error is not None:
            raise self.verify_error


class BlockingFolderGate(FakeFolderGate):
    def __init__(self, events):
        super().__init__(events)
        self.started = asyncio.Event()

    async def verify(self, *args, **kwargs):
        await super().verify(*args, **kwargs)
        self.started.set()
        await asyncio.Event().wait()


class FailingGovernor:
    def __init__(self, error):
        self.error = error
        self.calls = 0

    async def run(self, _category, _operation):
        self.calls += 1
        raise self.error


class HistoryFailingGovernor(FakeGovernor):
    async def run(self, category, operation):
        self.categories.append(category)
        if category == "history":
            raise ValueError("history governor database failure")
        return await operation()


class FakeClient:
    def __init__(
        self,
        events,
        *,
        authorized=True,
        resolution_failures=(),
        entities=None,
        histories=None,
        live_messages=(),
        remove_error=None,
        disconnect_error=None,
        disconnect_hangs=False,
        hold_open=False,
    ):
        self.events = events
        self.authorized = authorized
        self.resolution_failures = set(resolution_failures)
        self.entities = dict(entities or {})
        self.histories = histories or {}
        self.live_messages = tuple(live_messages)
        self.remove_error = remove_error
        self.disconnect_error = disconnect_error
        self.disconnect_hangs = disconnect_hangs
        self.hold_open = hold_open
        self.handlers = []
        self.iter_calls = []
        self.disconnect_event = asyncio.Event()
        self.live_tasks = []

    async def connect(self):
        self.events.append("client.connect")

    async def disconnect(self):
        self.events.append("client.disconnect")
        if self.disconnect_hangs:
            await asyncio.Event().wait()
        if self.disconnect_error is not None:
            raise self.disconnect_error
        self.disconnect_event.set()

    async def is_user_authorized(self):
        self.events.append("client.authorized")
        return self.authorized

    async def get_me(self):
        return SimpleNamespace(id=70001)

    async def get_entity(self, lookup):
        self.events.append(f"resolve:{lookup}")
        if lookup in self.resolution_failures:
            raise ValueError("private lookup detail")
        if lookup in self.entities:
            return self.entities[lookup]
        return f"entity:{lookup}"

    def iter_messages(self, entity, *, limit):
        self.events.append("history.iter")
        self.iter_calls.append((entity, limit))

        async def iterate():
            try:
                selected = self.histories.get(entity, ())
            except TypeError:
                selected = ()
            for message in selected:
                yield message

        return iterate()

    def add_event_handler(self, callback, event=None):
        self.events.append("handler.add")
        self.handlers.append((callback, event))

    def remove_event_handler(self, callback, event=None):
        self.events.append("handler.remove")
        if self.remove_error is not None:
            raise self.remove_error
        self.handlers.remove((callback, event))
        return 1

    @property
    def disconnected(self):
        return self._wait_until_disconnected()

    async def _wait_until_disconnected(self):
        self.events.append("client.run")
        if self.hold_open:
            self.live_tasks = [
                asyncio.create_task(callback(SimpleNamespace(message=selected)))
                for (callback, _), selected in zip(
                    tuple(self.handlers),
                    self.live_messages,
                )
            ]
            await self.disconnect_event.wait()
            return
        for (callback, _), message in zip(tuple(self.handlers), self.live_messages):
            await callback(SimpleNamespace(message=message))

    async def run_until_disconnected(self):
        await self._wait_until_disconnected()


class BlockingIngestor(FakeIngestor):
    def __init__(self, events):
        super().__init__(events=events)
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def ingest(self, message):
        self.messages.append(message)
        self.events.append("ingest.started")
        self.started.set()
        await self.release.wait()
        self.events.append("ingest.finished")
        return SimpleNamespace(created=True)


def message(message_id, text, date, **metadata):
    return SimpleNamespace(id=message_id, message=text, date=date, **metadata)


def public_channel(
    channel_id,
    username,
    *,
    access_hash=919191,
    broadcast=True,
    megagroup=False,
    left=False,
    scam=False,
    fake=False,
    restricted=False,
    usernames=None,
):
    return types.Channel(
        id=channel_id,
        title="Test source",
        photo=types.ChatPhotoEmpty(),
        date=NOW,
        access_hash=access_hash,
        username=username,
        usernames=usernames,
        broadcast=broadcast,
        megagroup=megagroup,
        left=left,
        scam=scam,
        fake=fake,
        restricted=restricted,
    )


def build_service(
    root,
    client,
    adapter,
    ingestor,
    events,
    *,
    release_error=None,
    governor=None,
    identity_binding_loader=None,
    entity_identity_verifier=None,
    folder_gate=None,
    live_drain_timeout_seconds=10.0,
    disconnect_timeout_seconds=10.0,
    connected_service_factory=None,
):
    credentials = TelegramCredentials(
        api_id=12345,
        api_hash="private-api-hash",
        session_path=root / "collector.session",
    )

    def client_factory(_credentials):
        events.append("client.factory")
        return client

    def lock_factory(path, *, role):
        return FakeLock(
            path,
            role=role,
            events=events,
            release_error=release_error,
        )

    selected_governor = governor or FakeGovernor()
    return TelegramCollectorService(
        object(),
        credentials,
        client_factory=client_factory,
        source_adapter=adapter,
        ingestor=ingestor,
        lock_factory=lock_factory,
        governor_factory=lambda *_args: selected_governor,
        identity_binding_loader=(
            identity_binding_loader or FakeIdentityBindingLoader()
        ),
        entity_identity_verifier=(
            entity_identity_verifier or PassThroughIdentityVerifier()
        ),
        folder_gate=folder_gate or FakeFolderGate(),
        runtime_root=root,
        clock=lambda: NOW,
        correlation_id_factory=lambda: TRACE_ID,
        live_drain_timeout_seconds=live_drain_timeout_seconds,
        disconnect_timeout_seconds=disconnect_timeout_seconds,
        connected_service_factory=connected_service_factory,
    )


class CollectorOptionsTest(unittest.TestCase):
    def test_limits_are_bounded_and_catch_up_defaults_to_zero(self):
        self.assertEqual(CollectorOptions().catch_up_limit, 0)
        with self.assertRaises(ValueError):
            CollectorOptions(catch_up_limit=501)
        with self.assertRaises(ValueError):
            CollectorOptions(source_limit=0)

    def test_cli_exposes_check_once_and_run_with_safe_defaults(self):
        parser = build_parser()
        self.assertEqual(parser.parse_args(["check"]).source_limit, None)
        once = parser.parse_args(["once"])
        self.assertEqual(once.catch_up_limit, 0)
        run = parser.parse_args(["run", "--catch-up-limit", "25"])
        self.assertEqual(run.catch_up_limit, 25)


class TelegramCollectorServiceTest(unittest.IsolatedAsyncioTestCase):
    async def test_connected_sidecar_runs_only_in_live_mode_and_stops_before_disconnect(self):
        class Sidecar:
            async def start(self):
                events.append("sidecar.start")

            async def stop(self):
                events.append("sidecar.stop")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            client = FakeClient(events)

            def factory(selected_client, account_id, governor):
                self.assertIs(selected_client, client)
                self.assertEqual(account_id, 77)
                self.assertTrue(hasattr(governor, "run"))
                events.append("sidecar.factory")
                return Sidecar()

            service = build_service(
                root,
                client,
                FakeAdapter([FakeSource(11, "@alpha")], events),
                FakeIngestor(events=events),
                events,
                connected_service_factory=factory,
            )
            await service.run()

        self.assertLess(events.index("sidecar.start"), events.index("handler.add"))
        self.assertLess(events.index("sidecar.stop"), events.index("client.disconnect"))

    async def test_folder_gate_covers_check_once_and_run_before_collection(self):
        for mode in ("check", "once", "run"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                events = []
                client = FakeClient(events)
                gate = FakeFolderGate(events)
                service = build_service(
                    root,
                    client,
                    FakeAdapter([FakeSource(11, "@alpha")], events),
                    FakeIngestor(events=events),
                    events,
                    folder_gate=gate,
                )

                if mode == "check":
                    await service.check()
                elif mode == "once":
                    await service.collect_once(
                        options=CollectorOptions(catch_up_limit=1),
                    )
                else:
                    await service.run(
                        options=CollectorOptions(catch_up_limit=1),
                    )

            self.assertEqual(gate.scope_calls, [(11,)])
            self.assertEqual(gate.catalog_calls, [(11,)])
            self.assertEqual(len(gate.verify_calls), 1)
            self.assertLess(
                events.index("lock.acquire"),
                events.index("folder.catalog"),
            )
            self.assertLess(
                events.index("folder.catalog"),
                events.index("folder.scope"),
            )
            self.assertLess(
                events.index("folder.scope"),
                events.index("resolve:@alpha"),
            )
            self.assertLess(
                events.index("resolve:@alpha"),
                events.index("folder.verify"),
            )
            if mode in {"once", "run"}:
                self.assertLess(
                    events.index("folder.verify"),
                    events.index("history.iter"),
                )
            else:
                self.assertNotIn("history.iter", events)
            if mode == "run":
                self.assertLess(
                    events.index("folder.verify"),
                    events.index("handler.add"),
                )
            else:
                self.assertNotIn("handler.add", events)

    async def test_folder_scope_failure_prevents_any_entity_request(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            gate = FakeFolderGate(
                events,
                scope_error=TelegramCollectorFolderGateError(
                    "Collector canary scope invalid"
                ),
            )
            service = build_service(
                root,
                FakeClient(events),
                FakeAdapter([FakeSource(11, "@unselected")], events),
                FakeIngestor(events=events),
                events,
                folder_gate=gate,
            )

            with self.assertRaisesRegex(
                TelegramCollectorError,
                "Collector canary scope invalid",
            ):
                await service.check()

        self.assertNotIn("resolve:@unselected", events)
        self.assertEqual(gate.verify_calls, [])
        self.assertEqual(events[-2:], ["client.disconnect", "lock.release"])

    async def test_extra_eligible_source_cannot_be_hidden_by_source_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            loader = FakeIdentityBindingLoader()
            client = FakeClient(events)
            source_ids = (*CANARY_SOURCE_IDS, 99)
            service = build_service(
                root,
                client,
                FakeAdapter(
                    [
                        FakeSource(source_id, f"@source_{source_id}")
                        for source_id in source_ids
                    ],
                    events,
                ),
                FakeIngestor(events=events),
                events,
                identity_binding_loader=loader,
                folder_gate=ExactCanaryTelegramFolderGate(),
            )

            with self.assertRaisesRegex(
                TelegramCollectorError,
                "Eligible Telegram-каталог",
            ):
                await service.run(
                    options=CollectorOptions(
                        source_limit=len(CANARY_SOURCE_IDS),
                        catch_up_limit=1,
                    ),
                )

        self.assertEqual(loader.calls, [])
        self.assertFalse(any(value.startswith("resolve:") for value in events))
        self.assertEqual(client.iter_calls, [])
        self.assertNotIn("history.iter", events)
        self.assertNotIn("handler.add", events)
        self.assertNotIn("client.run", events)
        self.assertEqual(events[-2:], ["client.disconnect", "lock.release"])

    async def test_folder_verification_failure_blocks_all_collection_modes(self):
        for mode in ("check", "once", "run"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                events = []
                client = FakeClient(events)
                gate = FakeFolderGate(
                    events,
                    verify_error=TelegramCollectorFolderGateError(
                        "Папка ЛидГениратор не прошла "
                        "проверку"
                    ),
                )
                service = build_service(
                    root,
                    client,
                    FakeAdapter([FakeSource(11, "@alpha")], events),
                    FakeIngestor(events=events),
                    events,
                    folder_gate=gate,
                )

                with self.assertRaisesRegex(
                    TelegramCollectorError,
                    "ЛидГениратор",
                ):
                    if mode == "check":
                        await service.check()
                    elif mode == "once":
                        await service.collect_once(
                            options=CollectorOptions(catch_up_limit=1),
                        )
                    else:
                        await service.run(
                            options=CollectorOptions(catch_up_limit=1),
                        )

            self.assertEqual(client.iter_calls, [])
            self.assertNotIn("history.iter", events)
            self.assertNotIn("handler.add", events)
            self.assertEqual(events[-2:], ["client.disconnect", "lock.release"])

    async def test_folder_gate_cancellation_disconnects_before_lock_release(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            gate = BlockingFolderGate(events)
            client = FakeClient(events)
            service = build_service(
                root,
                client,
                FakeAdapter([FakeSource(11, "@alpha")], events),
                FakeIngestor(events=events),
                events,
                folder_gate=gate,
            )

            task = asyncio.create_task(service.check())
            await asyncio.wait_for(gate.started.wait(), timeout=0.2)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        self.assertEqual(events[-2:], ["client.disconnect", "lock.release"])

    async def test_check_locks_before_client_factory_and_isolates_resolution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            sources = [FakeSource(11, "@working"), FakeSource(12, "@unavailable")]
            client = FakeClient(
                events,
                resolution_failures={"@unavailable"},
            )
            adapter = FakeAdapter(sources, events)
            service = build_service(
                root,
                client,
                adapter,
                FakeIngestor(events=events),
                events,
            )

            report = await service.check()

        self.assertEqual(report.collector_account_id, 77)
        self.assertEqual(report.catalog_source_count, 2)
        self.assertEqual(report.resolved_source_count, 1)
        self.assertEqual(report.source_resolution_failure_count, 1)
        self.assertLess(events.index("lock.acquire"), events.index("client.factory"))
        self.assertEqual(events[-2:], ["client.disconnect", "lock.release"])

    async def test_missing_identity_binding_skips_resolution_history_and_ingestion(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            loader = FakeIdentityBindingLoader({})
            client = FakeClient(events)
            ingestor = FakeIngestor(events=events)
            service = build_service(
                root,
                client,
                FakeAdapter([FakeSource(11, "@unbound")], events),
                ingestor,
                events,
                identity_binding_loader=loader,
                entity_identity_verifier=StrictTelegramChannelIdentityVerifier(),
            )

            report = await service.collect_once(
                options=CollectorOptions(catch_up_limit=5),
            )

        self.assertEqual(loader.calls, [((11,), 77)])
        self.assertEqual(report.resolved_source_count, 0)
        self.assertEqual(report.source_resolution_failure_count, 1)
        self.assertNotIn("resolve:@unbound", events)
        self.assertEqual(client.iter_calls, [])
        self.assertEqual(ingestor.messages, [])

    async def test_untrusted_state_or_wrong_collector_binding_is_rejected(self):
        cases = (
            TelegramSourceIdentityBinding(11, 77, "rejected", 501),
            TelegramSourceIdentityBinding(11, 999, "accessible", 501),
            TelegramSourceIdentityBinding(12, 77, "accessible", 501),
            TelegramSourceIdentityBinding(11, 77, "accessible", None),
        )
        for binding in cases:
            with self.subTest(binding=binding), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                events = []
                client = FakeClient(events)
                ingestor = FakeIngestor(events=events)
                service = build_service(
                    root,
                    client,
                    FakeAdapter([FakeSource(11, "@untrusted")], events),
                    ingestor,
                    events,
                    identity_binding_loader=FakeIdentityBindingLoader({11: binding}),
                    entity_identity_verifier=StrictTelegramChannelIdentityVerifier(),
                )

                report = await service.collect_once(
                    options=CollectorOptions(catch_up_limit=1),
                )

            self.assertEqual(report.source_resolution_failure_count, 1)
            self.assertNotIn("resolve:@untrusted", events)
            self.assertEqual(client.iter_calls, [])
            self.assertEqual(ingestor.messages, [])

    async def test_transferred_username_or_changed_username_fails_identity_check(self):
        cases = (
            (
                public_channel(502, "transferred_name"),
                TelegramSourceIdentityBinding(11, 77, "accessible", 501),
            ),
            (
                public_channel(501, "different_name"),
                TelegramSourceIdentityBinding(11, 77, "approved", 501),
            ),
        )
        for entity, binding in cases:
            with self.subTest(entity_id=entity.id), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                events = []
                client = FakeClient(
                    events,
                    entities={"@transferred_name": entity},
                )
                ingestor = FakeIngestor(events=events)
                service = build_service(
                    root,
                    client,
                    FakeAdapter([FakeSource(11, "@transferred_name")], events),
                    ingestor,
                    events,
                    identity_binding_loader=FakeIdentityBindingLoader({11: binding}),
                    entity_identity_verifier=StrictTelegramChannelIdentityVerifier(),
                )

                report = await service.collect_once(
                    options=CollectorOptions(catch_up_limit=3),
                )

            self.assertEqual(report.resolved_source_count, 0)
            self.assertEqual(report.source_resolution_failure_count, 1)
            self.assertEqual(client.iter_calls, [])
            self.assertEqual(ingestor.messages, [])

    async def test_user_or_basic_chat_numeric_collision_is_rejected(self):
        for entity in (
            types.User(id=601, access_hash=616161, username="collision"),
            types.ChatEmpty(id=601),
        ):
            with self.subTest(entity=type(entity).__name__), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                events = []
                client = FakeClient(events, entities={"@collision": entity})
                ingestor = FakeIngestor(events=events)
                service = build_service(
                    root,
                    client,
                    FakeAdapter([FakeSource(11, "@collision")], events),
                    ingestor,
                    events,
                    identity_binding_loader=FakeIdentityBindingLoader(
                        {
                            11: TelegramSourceIdentityBinding(
                                11,
                                77,
                                "accessible",
                                601,
                            )
                        }
                    ),
                    entity_identity_verifier=StrictTelegramChannelIdentityVerifier(),
                )

                report = await service.collect_once(
                    options=CollectorOptions(catch_up_limit=1),
                )

            self.assertEqual(report.source_resolution_failure_count, 1)
            self.assertEqual(client.iter_calls, [])
            self.assertEqual(ingestor.messages, [])

    async def test_invalid_or_unsafe_channel_flags_are_rejected(self):
        cases = {
            "not_channel_kind": public_channel(
                701,
                "unsafe",
                broadcast=False,
                megagroup=False,
            ),
            "left": public_channel(701, "unsafe", left=True),
            "scam": public_channel(701, "unsafe", scam=True),
            "fake": public_channel(701, "unsafe", fake=True),
            "restricted": public_channel(701, "unsafe", restricted=True),
            "missing_access_hash": public_channel(
                701,
                "unsafe",
                access_hash=None,
            ),
            "zero_access_hash": public_channel(
                701,
                "unsafe",
                access_hash=0,
            ),
        }
        for label, entity in cases.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                events = []
                client = FakeClient(events, entities={"@unsafe": entity})
                ingestor = FakeIngestor(events=events)
                service = build_service(
                    root,
                    client,
                    FakeAdapter([FakeSource(11, "@unsafe")], events),
                    ingestor,
                    events,
                    identity_binding_loader=FakeIdentityBindingLoader(
                        {
                            11: TelegramSourceIdentityBinding(
                                11,
                                77,
                                "accessible",
                                701,
                            )
                        }
                    ),
                    entity_identity_verifier=StrictTelegramChannelIdentityVerifier(),
                )

                report = await service.collect_once(
                    options=CollectorOptions(catch_up_limit=1),
                )

            self.assertEqual(report.source_resolution_failure_count, 1)
            self.assertEqual(client.iter_calls, [])
            self.assertEqual(ingestor.messages, [])

    async def test_valid_public_channel_is_pinned_to_input_channel(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            entity = public_channel(
                801,
                "primary_alias",
                broadcast=False,
                megagroup=True,
                usernames=[types.Username("wanted_alias", active=True)],
            )
            client = FakeClient(events, entities={"@wanted_alias": entity})
            service = build_service(
                root,
                client,
                FakeAdapter([FakeSource(11, "@wanted_alias")], events),
                FakeIngestor(events=events),
                events,
                identity_binding_loader=FakeIdentityBindingLoader(
                    {
                        11: TelegramSourceIdentityBinding(
                            11,
                            77,
                            "approved",
                            801,
                        )
                    }
                ),
                entity_identity_verifier=StrictTelegramChannelIdentityVerifier(),
            )

            report = await service.collect_once(
                options=CollectorOptions(catch_up_limit=1),
            )

        self.assertEqual(report.resolved_source_count, 1)
        self.assertEqual(report.source_resolution_failure_count, 0)
        self.assertEqual(len(client.iter_calls), 1)
        pinned, limit = client.iter_calls[0]
        self.assertIsInstance(pinned, types.InputChannel)
        self.assertEqual(pinned.channel_id, 801)
        self.assertEqual(pinned.access_hash, 919191)
        self.assertEqual(limit, 1)

    async def test_identity_failure_log_contains_no_peer_hash_username_or_content(self):
        secret_username = "private_target_314159"
        secret_peer_id = 987654321
        secret_access_hash = 887766554433
        secret_content = "LEAD CONTENT MUST NOT APPEAR"
        output = io.StringIO()
        logger = configure_structured_logger(
            "ai_sales_radar",
            redactor=Redactor(),
            stream=output,
        )
        self.addCleanup(logger.handlers.clear)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            entity = public_channel(
                secret_peer_id,
                secret_username,
                access_hash=secret_access_hash,
                scam=True,
            )
            client = FakeClient(
                events,
                entities={f"@{secret_username}": entity},
            )
            ingestor = FakeIngestor(events=events)
            service = build_service(
                root,
                client,
                FakeAdapter([FakeSource(11, f"@{secret_username}")], events),
                ingestor,
                events,
                identity_binding_loader=FakeIdentityBindingLoader(
                    {
                        11: TelegramSourceIdentityBinding(
                            11,
                            77,
                            "accessible",
                            secret_peer_id,
                        )
                    }
                ),
                entity_identity_verifier=StrictTelegramChannelIdentityVerifier(),
            )

            report = await service.collect_once(
                options=CollectorOptions(catch_up_limit=1),
            )

        logged = output.getvalue()
        self.assertEqual(report.source_resolution_failure_count, 1)
        self.assertEqual(client.iter_calls, [])
        self.assertEqual(ingestor.messages, [])
        self.assertNotIn(secret_username, logged)
        self.assertNotIn(str(secret_peer_id), logged)
        self.assertNotIn(str(secret_access_hash), logged)
        self.assertNotIn(secret_content, logged)

    async def test_unexpected_governor_error_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            client = FakeClient(events)
            governor = FailingGovernor(RuntimeError("database unavailable"))
            service = build_service(
                root,
                client,
                FakeAdapter(
                    [FakeSource(11, "@alpha"), FakeSource(12, "@beta")],
                    events,
                ),
                FakeIngestor(events=events),
                events,
                governor=governor,
            )

            with self.assertRaisesRegex(RuntimeError, "database unavailable"):
                await service.check()

        self.assertEqual(governor.calls, 1)
        self.assertNotIn("resolve:@beta", events)

    async def test_governor_value_error_is_not_treated_as_source_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            governor = FailingGovernor(ValueError("invalid database state"))
            service = build_service(
                root,
                FakeClient(events),
                FakeAdapter([FakeSource(11, "@alpha")], events),
                FakeIngestor(events=events),
                events,
                governor=governor,
            )

            with self.assertRaisesRegex(ValueError, "invalid database state"):
                await service.check()

        self.assertEqual(governor.calls, 1)

    async def test_catch_up_governor_error_is_not_treated_as_source_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            governor = HistoryFailingGovernor()
            service = build_service(
                root,
                FakeClient(events),
                FakeAdapter([FakeSource(11, "@alpha")], events),
                FakeIngestor(events=events),
                events,
                governor=governor,
            )

            with self.assertRaisesRegex(
                ValueError,
                "history governor database failure",
            ):
                await service.collect_once(
                    options=CollectorOptions(catch_up_limit=1),
                )

        self.assertEqual(governor.categories, ["entity_access", "history"])

    async def test_account_wide_governor_pause_is_not_masked(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            service = build_service(
                root,
                FakeClient(events),
                FakeAdapter([FakeSource(11, "@alpha")], events),
                FakeIngestor(events=events),
                events,
                governor=FailingGovernor(TelegramCollectorPaused("paused")),
            )

            with self.assertRaises(TelegramCollectorPaused):
                await service.check()

    async def test_collect_once_orders_bounded_history_and_uses_raw_ingestor(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            source_a = FakeSource(11, "@alpha")
            source_b = FakeSource(12, "@beta")
            later = message(
                102,
                "later lead",
                datetime(2026, 8, 26, 12, 2, tzinfo=timezone.utc),
                sender_id=400,
                post=True,
            )
            earlier = message(
                101,
                "earlier lead",
                datetime(2026, 8, 26, 12, 1),
                chat_id=-100500,
            )
            client = FakeClient(
                events,
                histories={
                    "entity:@alpha": (later,),
                    "entity:@beta": (earlier,),
                },
            )
            ingestor = FakeIngestor(duplicate_ids={102}, events=events)
            service = build_service(
                root,
                client,
                FakeAdapter([source_a, source_b], events),
                ingestor,
                events,
            )

            report = await service.collect_once(
                options=CollectorOptions(catch_up_limit=2),
            )

        self.assertEqual(client.iter_calls, [("entity:@alpha", 2), ("entity:@beta", 2)])
        self.assertEqual(
            [item.external_message_id for item in ingestor.messages],
            [101, 102],
        )
        self.assertEqual(
            [item.ingestion_origin for item in ingestor.messages],
            [RawMessageOrigin.CATCH_UP, RawMessageOrigin.CATCH_UP],
        )
        self.assertEqual(ingestor.messages[0].collector_account_id, 77)
        self.assertEqual(ingestor.messages[0].message_url, "https://t.me/example_channel_011/101")
        self.assertEqual(ingestor.messages[0].transport_metadata["chat_id"], -100500)
        self.assertEqual(ingestor.messages[0].message_date.tzinfo, timezone.utc)
        self.assertEqual(report.created_message_count, 1)
        self.assertEqual(report.duplicate_message_count, 1)

    async def test_default_once_does_not_read_history(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            client = FakeClient(events)
            ingestor = FakeIngestor(events=events)
            service = build_service(
                root,
                client,
                FakeAdapter([FakeSource(11, "@alpha")], events),
                ingestor,
                events,
            )

            report = await service.collect_once()

        self.assertEqual(client.iter_calls, [])
        self.assertEqual(ingestor.messages, [])
        self.assertEqual(report.created_message_count, 0)

    async def test_run_registers_live_handler_and_removes_it_before_disconnect(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            live = message(201, "live lead", NOW, sender_id=701)
            client = FakeClient(events, live_messages=(live,))
            ingestor = FakeIngestor(events=events)
            service = build_service(
                root,
                client,
                FakeAdapter([FakeSource(11, "@alpha")], events),
                ingestor,
                events,
            )

            report = await service.run()

        self.assertEqual(len(ingestor.messages), 1)
        self.assertIs(ingestor.messages[0].ingestion_origin, RawMessageOrigin.LIVE)
        self.assertEqual(client.iter_calls, [])
        self.assertLess(events.index("handler.add"), events.index("client.run"))
        self.assertLess(events.index("handler.remove"), events.index("client.disconnect"))
        self.assertEqual(report.created_message_count, 1)

    async def test_cooperative_stop_drains_live_ingestion_before_disconnect(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            client = FakeClient(
                events,
                live_messages=(message(202, "in flight", NOW),),
                hold_open=True,
            )
            ingestor = BlockingIngestor(events)
            service = build_service(
                root,
                client,
                FakeAdapter([FakeSource(11, "@alpha")], events),
                ingestor,
                events,
                live_drain_timeout_seconds=0.2,
                disconnect_timeout_seconds=0.2,
            )

            run_task = asyncio.create_task(service.run())
            await asyncio.wait_for(ingestor.started.wait(), timeout=0.2)
            await service.stop()
            await asyncio.sleep(0)
            self.assertNotIn("client.disconnect", events)
            ingestor.release.set()
            report = await asyncio.wait_for(run_task, timeout=0.5)

        self.assertEqual(report.created_message_count, 1)
        self.assertLess(events.index("ingest.finished"), events.index("client.disconnect"))

    async def test_live_drain_timeout_is_bounded_and_disconnects(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            client = FakeClient(
                events,
                live_messages=(message(203, "blocked", NOW),),
                hold_open=True,
            )
            ingestor = BlockingIngestor(events)
            service = build_service(
                root,
                client,
                FakeAdapter([FakeSource(11, "@alpha")], events),
                ingestor,
                events,
                live_drain_timeout_seconds=0.01,
                disconnect_timeout_seconds=0.05,
            )

            run_task = asyncio.create_task(service.run())
            await asyncio.wait_for(ingestor.started.wait(), timeout=0.2)
            await service.stop()
            with self.assertRaisesRegex(TelegramCollectorError, "drain_live"):
                await asyncio.wait_for(run_task, timeout=0.3)

        self.assertIn("client.disconnect", events)
        self.assertIn("lock.release", events)

    async def test_unauthorized_session_fails_before_catalog_and_still_cleans_up(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            client = FakeClient(events, authorized=False)
            adapter = FakeAdapter([FakeSource(11, "@alpha")], events)
            service = build_service(
                root,
                client,
                adapter,
                FakeIngestor(events=events),
                events,
            )

            with self.assertRaisesRegex(TelegramCollectorError, "не авторизована"):
                await service.check()

        self.assertEqual(adapter.calls, 0)
        self.assertEqual(events[-2:], ["client.disconnect", "lock.release"])

    async def test_client_factory_failure_still_protects_and_releases_session(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            client = FakeClient(events)
            service = build_service(
                root,
                client,
                FakeAdapter([], events),
                FakeIngestor(events=events),
                events,
            )

            def failed_factory(_credentials):
                events.append("client.factory.failed")
                raise RuntimeError("factory detail")

            service._client_factory = failed_factory
            with self.assertRaisesRegex(RuntimeError, "factory detail"):
                await service.check()

        self.assertLess(events.index("lock.acquire"), events.index("client.factory.failed"))
        self.assertEqual(events[-1], "lock.release")

    async def test_inactive_catalog_account_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            client = FakeClient(events)
            service = build_service(
                root,
                client,
                FakeAdapter([FakeSource(11, "@alpha")], events, active=False),
                FakeIngestor(events=events),
                events,
            )

            with self.assertRaisesRegex(TelegramCollectorError, "отключён"):
                await service.check()

        self.assertNotIn("resolve:@alpha", events)

    async def test_cleanup_attempts_protection_and_lock_after_other_failures(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            client = FakeClient(
                events,
                remove_error=RuntimeError("remove detail"),
                disconnect_error=RuntimeError("disconnect detail"),
            )
            service = build_service(
                root,
                client,
                FakeAdapter([FakeSource(11, "@alpha")], events),
                FakeIngestor(events=events),
                events,
            )

            def protect(_path, *, runtime_root):
                self.assertEqual(runtime_root, root)
                events.append("session.protect")

            with patch("ai_sales_radar.collector.protect_session_files", protect):
                with self.assertRaisesRegex(TelegramCollectorError, "remove_handler"):
                    await service.run()

        self.assertIn("handler.remove", events)
        self.assertIn("client.disconnect", events)
        self.assertIn("session.protect", events)
        self.assertEqual(events[-1], "lock.release")

    async def test_session_protection_failure_does_not_prevent_lock_release(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            client = FakeClient(events)
            service = build_service(
                root,
                client,
                FakeAdapter([], events),
                FakeIngestor(events=events),
                events,
            )

            def failed_protect(_path, *, runtime_root):
                self.assertEqual(runtime_root, root)
                events.append("session.protect.failed")
                if events.count("session.protect.failed") == 2:
                    raise RuntimeError("protection detail")

            with patch(
                "ai_sales_radar.collector.protect_session_files",
                failed_protect,
            ):
                with self.assertRaisesRegex(
                    TelegramCollectorError,
                    "protect_session",
                ):
                    await service.check()

        self.assertEqual(events[-1], "lock.release")

    async def test_disconnect_timeout_is_bounded_and_releases_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            client = FakeClient(events, disconnect_hangs=True)
            service = build_service(
                root,
                client,
                FakeAdapter([], events),
                FakeIngestor(events=events),
                events,
                disconnect_timeout_seconds=0.01,
            )

            with self.assertRaisesRegex(TelegramCollectorError, "disconnect"):
                await asyncio.wait_for(service.check(), timeout=0.3)

        self.assertEqual(events[-1], "lock.release")

    async def test_ingestion_error_does_not_log_message_body_or_exception_text(self):
        secret_body = "PRIVATE LEAD BODY 314159"
        output = io.StringIO()
        logger = configure_structured_logger(
            "ai_sales_radar",
            redactor=Redactor(),
            stream=output,
        )
        self.addCleanup(logger.handlers.clear)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            client = FakeClient(events, live_messages=(message(301, secret_body, NOW),))
            service = build_service(
                root,
                client,
                FakeAdapter([FakeSource(11, "@alpha")], events),
                FakeIngestor(error=RuntimeError(secret_body), events=events),
                events,
            )

            report = await service.run()

        self.assertEqual(report.ingestion_failure_count, 1)
        self.assertNotIn(secret_body, output.getvalue())
        self.assertNotIn("314159", output.getvalue())


if __name__ == "__main__":
    unittest.main()

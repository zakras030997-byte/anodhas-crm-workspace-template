from __future__ import annotations

import asyncio
from dataclasses import asdict
from datetime import UTC, datetime
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

from telethon import functions, types

from ai_sales_radar.config import TelegramCredentials
from ai_sales_radar.telegram_folder import (
    MAX_FOLDER_PEERS,
    PostgresReadOnlyApprovedSourceLoader,
    TelegramFolderError,
    TelegramFolderReconciler,
    TelegramSourceIdentityBinding,
    build_parser,
)
from freelancer_bot.persistence.source_repository import SourceRecord, SourceStatus
from freelancer_bot.telegram_collector import (
    TelegramCollectorSource,
    TelegramCollectorSourceSnapshot,
)


NOW = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)


def source(
    source_id: int,
    username: str,
    *,
    status: SourceStatus = SourceStatus.APPROVED,
) -> TelegramCollectorSource:
    record = SourceRecord(
        id=source_id,
        platform="telegram",
        external_id=f"username:{username}",
        access_type="public",
        lifecycle_status=status,
        display_name=f"Source {source_id}",
        handle=f"@{username}",
        canonical_url=f"https://t.me/{username}",
        created_at=NOW,
        updated_at=NOW,
    )
    return TelegramCollectorSource(record=record, lookup=f"@{username}")


def dialog(username: str, peer):
    if isinstance(peer, types.InputPeerChannel):
        entity = types.Channel(
            id=peer.channel_id,
            title=f"Channel {peer.channel_id}",
            photo=types.ChatPhotoEmpty(),
            date=None,
            broadcast=True,
            megagroup=False,
            access_hash=peer.access_hash,
            username=username,
            usernames=[],
        )
    elif isinstance(peer, types.InputPeerUser):
        entity = types.User(
            id=peer.user_id,
            access_hash=peer.access_hash,
            first_name="User",
            username=username,
            usernames=[],
        )
    elif isinstance(peer, types.InputPeerChat):
        entity = types.Chat(
            id=peer.chat_id,
            title=f"Chat {peer.chat_id}",
            photo=types.ChatPhotoEmpty(),
            participants_count=2,
            date=None,
            version=1,
        )
        entity.username = username
        entity.usernames = []
    else:
        raise AssertionError(f"unsupported fixture peer: {type(peer).__name__}")
    return SimpleNamespace(
        entity=entity,
        input_entity=peer,
    )


def folder(
    *,
    title: str = "Ист_Лид_АИ",
    folder_id: int = 7,
    pinned=(),
    included=(),
    excluded=(),
    contacts=False,
    non_contacts=True,
    groups=True,
    broadcasts=True,
    bots=False,
    exclude_muted=True,
    exclude_read=False,
    exclude_archived=True,
    title_noanimate=True,
    emoticon="📂",
    color=5,
):
    return types.DialogFilter(
        id=folder_id,
        title=types.TextWithEntities(
            text=title,
            entities=[types.MessageEntityBold(offset=0, length=3)],
        ),
        pinned_peers=list(pinned),
        include_peers=list(included),
        exclude_peers=list(excluded),
        contacts=contacts,
        non_contacts=non_contacts,
        groups=groups,
        broadcasts=broadcasts,
        bots=bots,
        exclude_muted=exclude_muted,
        exclude_read=exclude_read,
        exclude_archived=exclude_archived,
        title_noanimate=title_noanimate,
        emoticon=emoticon,
        color=color,
    )


class FakeLock:
    def __init__(self, _path, *, role, events):
        self.role = role
        self.events = events

    def acquire(self):
        self.events.append("lock.acquire")

    def release(self):
        self.events.append("lock.release")


class FakeApprovedSourceAdapter:
    def __init__(self, sources, events, *, active=True):
        self.snapshot = TelegramCollectorSourceSnapshot(
            collector_account=SimpleNamespace(id=41, is_active=active),
            sources=tuple(sources),
        )
        self.events = events
        self.calls = 0

    async def list_for_session(self, _client):
        self.calls += 1
        self.events.append("adapter.list_for_session")
        return self.snapshot


class FakeReadOnlySourceLoader:
    def __init__(self, sources, events, *, active=True, missing=False):
        self.snapshot = TelegramCollectorSourceSnapshot(
            collector_account=SimpleNamespace(id=41, is_active=active),
            sources=tuple(sources),
        )
        self.events = events
        self.missing = missing
        self.calls = 0

    async def list_for_session(self, _client):
        self.calls += 1
        self.events.append("readonly_sources.list_for_session")
        if self.missing:
            raise TelegramFolderError(
                "Telegram collector account отсутствует в локальном каталоге"
            )
        return self.snapshot


class ChangingApprovedSourceAdapter:
    def __init__(self, source_batches, events):
        self.source_batches = [tuple(batch) for batch in source_batches]
        self.events = events
        self.calls = 0

    async def list_for_session(self, _client):
        index = min(self.calls, len(self.source_batches) - 1)
        self.calls += 1
        self.events.append("adapter.list_for_session")
        return TelegramCollectorSourceSnapshot(
            collector_account=SimpleNamespace(id=41, is_active=True),
            sources=self.source_batches[index],
        )


class FakeIdentityBindingLoader:
    def __init__(self, bindings, events):
        self.bindings = dict(bindings)
        self.events = events
        self.calls = []

    async def load(self, *, source_ids, collector_account_id):
        self.events.append("identity_bindings.load")
        self.calls.append((tuple(source_ids), collector_account_id))
        return {
            source_id: self.bindings[source_id]
            for source_id in source_ids
            if source_id in self.bindings
        }


class AsyncContext:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class FakeSelectResult:
    def __init__(self, row):
        self.row = row

    def mappings(self):
        return self

    def one_or_none(self):
        return self.row


class ReadOnlyConnection:
    def __init__(self, account_row):
        self.account_row = account_row
        self.statements = []

    async def execute(self, statement):
        if not statement.is_select:
            raise AssertionError("read-only loader emitted a write statement")
        self.statements.append(statement)
        return FakeSelectResult(self.account_row)


class ReadOnlyDatabase:
    def __init__(self, account_row):
        self.connection = ReadOnlyConnection(account_row)
        self.connect_calls = 0
        self.transaction_calls = 0

    def connect(self):
        self.connect_calls += 1
        return AsyncContext(self.connection)

    def transaction(self):
        self.transaction_calls += 1
        raise AssertionError("read-only loader must not open a write transaction")


class FakeSourceRepository:
    def __init__(self, records):
        self.records = list(records)
        self.calls = []

    async def list_for_collector(
        self,
        connection,
        *,
        collector_account_id,
        platform,
    ):
        self.calls.append((connection, collector_account_id, platform))
        return list(self.records)


class FakeClient:
    def __init__(
        self,
        filters,
        dialogs,
        events,
        *,
        authorized=True,
        reject_update=False,
        corrupt_post_verify=False,
        disconnect_gate=None,
    ):
        self.filters = list(filters)
        self.dialogs = list(dialogs)
        self.events = events
        self.authorized = authorized
        self.reject_update = reject_update
        self.corrupt_post_verify = corrupt_post_verify
        self.disconnect_gate = disconnect_gate
        self.get_filter_requests = 0
        self.update_requests = []
        self._updated = False

    async def connect(self):
        self.events.append("client.connect")

    async def disconnect(self):
        if self.disconnect_gate is None:
            self.events.append("client.disconnect")
            return
        self.events.append("client.disconnect.started")
        await self.disconnect_gate.wait()
        self.events.append("client.disconnect.completed")

    async def is_user_authorized(self):
        self.events.append("client.is_user_authorized")
        return self.authorized

    async def get_me(self):
        self.events.append("client.get_me")
        return SimpleNamespace(id=70001)

    async def get_dialogs(self, limit=None):
        if limit is not None:
            raise AssertionError("all existing dialogs must be considered")
        self.events.append("client.get_dialogs")
        return list(self.dialogs)

    async def __call__(self, request):
        if isinstance(request, functions.messages.GetDialogFiltersRequest):
            self.get_filter_requests += 1
            self.events.append("request.get_dialog_filters")
            if self._updated and self.corrupt_post_verify:
                current = self.filters[0]
                return types.messages.DialogFilters(
                    filters=[
                        folder(
                            folder_id=current.id,
                            pinned=current.pinned_peers,
                            included=current.include_peers,
                            excluded=current.exclude_peers,
                            emoticon="❌",
                        )
                    ],
                    tags_enabled=True,
                )
            return types.messages.DialogFilters(
                filters=list(self.filters),
                tags_enabled=True,
            )
        if isinstance(request, functions.messages.UpdateDialogFilterRequest):
            self.events.append("request.update_dialog_filter")
            self.update_requests.append(request)
            if self.reject_update:
                return False
            self.filters = [
                request.filter if item.id == request.id else item
                for item in self.filters
            ]
            self._updated = True
            return True
        raise AssertionError(f"unexpected Telegram request: {type(request).__name__}")

    def iter_messages(self, *_args, **_kwargs):
        raise AssertionError("folder reconciler must not read messages")

    async def join(self, *_args, **_kwargs):
        raise AssertionError("folder reconciler must not join chats")


def reconciler_for(
    root,
    *,
    sources,
    client,
    events,
    active=True,
    source_adapter=None,
    plan_source_loader=None,
    identity_binding_loader=None,
):
    credentials = TelegramCredentials(
        api_id=12345,
        api_hash="PRIVATE_API_HASH",
        session_path=root / "collector.session",
    )
    adapter = source_adapter or FakeApprovedSourceAdapter(
        sources,
        events,
        active=active,
    )
    readonly_loader = plan_source_loader or FakeReadOnlySourceLoader(
        sources,
        events,
        active=active,
    )
    if identity_binding_loader is None:
        known_sources = list(sources)
        snapshot = getattr(adapter, "snapshot", None)
        if snapshot is not None:
            known_sources.extend(snapshot.sources)
        for batch in getattr(adapter, "source_batches", ()):
            known_sources.extend(batch)
        bindings = {}
        for target_source in known_sources:
            expected_username = target_source.lookup.removeprefix("@").casefold()
            canonical_id = target_source.record.id
            for existing_dialog in client.dialogs:
                entity = existing_dialog.entity
                usernames = [getattr(entity, "username", None)]
                usernames.extend(
                    item.username
                    for item in getattr(entity, "usernames", ())
                    if item.active is True
                )
                if expected_username in {
                    value.casefold()
                    for value in usernames
                    if isinstance(value, str)
                }:
                    peer = existing_dialog.input_entity
                    canonical_id = getattr(
                        peer,
                        "user_id",
                        getattr(peer, "chat_id", getattr(peer, "channel_id", 0)),
                    )
                    break
            bindings[target_source.record.id] = TelegramSourceIdentityBinding(
                source_id=target_source.record.id,
                state="accessible",
                canonical_peer_id=canonical_id,
            )
        identity_binding_loader = FakeIdentityBindingLoader(bindings, events)

    def client_factory(_credentials):
        events.append("client.factory")
        return client

    def lock_factory(path, *, role):
        return FakeLock(path, role=role, events=events)

    return (
        TelegramFolderReconciler(
            object(),
            credentials,
            source_adapter=adapter,
            plan_source_loader=readonly_loader,
            identity_binding_loader=identity_binding_loader,
            client_factory=client_factory,
            lock_factory=lock_factory,
            runtime_root=root,
        ),
        adapter,
    )


class TelegramFolderParserTest(unittest.TestCase):
    def test_only_explicit_plan_and_sync_commands_exist(self):
        parser = build_parser()
        self.assertEqual(parser.parse_args(["plan"]).command, "plan")
        sync = parser.parse_args(["sync"])
        self.assertEqual(sync.command, "sync")
        self.assertIn("не поддерживает CAS", sync.concurrent_edit_warning)
        self.assertIn("Telegram Desktop/mobile", parser.format_help())
        with self.assertRaises(SystemExit):
            parser.parse_args([])


class PostgresReadOnlyApprovedSourceLoaderTest(unittest.IsolatedAsyncioTestCase):
    async def test_loader_uses_only_connect_and_select_contract(self):
        account_row = {
            "id": 41,
            "platform": "telegram",
            "external_account_id": "70001",
            "display_name": "Existing collector",
            "is_active": True,
            "created_at": NOW,
            "updated_at": NOW,
        }
        database = ReadOnlyDatabase(account_row)
        record = source(1, "readonly_source").record
        repository = FakeSourceRepository([record])
        events = []
        client = FakeClient([folder()], [], events)
        loader = PostgresReadOnlyApprovedSourceLoader(
            database,
            repository=repository,
        )

        snapshot = await loader.list_for_session(client)

        self.assertEqual(snapshot.collector_account.id, 41)
        self.assertEqual([item.record.id for item in snapshot.sources], [1])
        self.assertEqual(database.connect_calls, 1)
        self.assertEqual(database.transaction_calls, 0)
        self.assertEqual(len(database.connection.statements), 1)
        self.assertEqual(repository.calls[0][1:], (41, "telegram"))

    async def test_missing_collector_account_fails_closed_without_repository_read(self):
        database = ReadOnlyDatabase(None)
        repository = FakeSourceRepository([])
        loader = PostgresReadOnlyApprovedSourceLoader(
            database,
            repository=repository,
        )
        client = FakeClient([folder()], [], [])

        with self.assertRaisesRegex(TelegramFolderError, "отсутствует"):
            await loader.list_for_session(client)

        self.assertEqual(database.transaction_calls, 0)
        self.assertEqual(repository.calls, [])


class TelegramFolderReconcilerTest(unittest.IsolatedAsyncioTestCase):
    async def test_plan_is_read_only_and_json_contains_no_access_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            target = types.InputPeerChannel(channel_id=101, access_hash=987654321)
            client = FakeClient(
                [folder(included=[types.InputPeerChat(chat_id=9)])],
                [dialog("approved_one", target)],
                events,
            )
            service, adapter = reconciler_for(
                root,
                sources=[source(1, "approved_one")],
                client=client,
                events=events,
            )

            report = await service.plan()

        self.assertEqual(report.planned_add_count, 1)
        self.assertTrue(report.would_update)
        self.assertFalse(report.updated)
        self.assertFalse(report.verified)
        self.assertEqual(client.get_filter_requests, 1)
        self.assertEqual(client.update_requests, [])
        self.assertEqual(adapter.calls, 0)
        self.assertIn("readonly_sources.list_for_session", events)
        self.assertLess(events.index("lock.acquire"), events.index("client.factory"))
        self.assertEqual(events[-2:], ["client.disconnect", "lock.release"])
        payload = json.dumps(asdict(report), ensure_ascii=False)
        self.assertNotIn("access_hash", payload)
        self.assertNotIn("987654321", payload)
        self.assertNotIn("PRIVATE_API_HASH", payload)

    async def test_sync_rereads_then_adds_and_post_verifies_preserving_filter(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            pinned = types.InputPeerUser(user_id=11, access_hash=111)
            included = types.InputPeerChat(chat_id=12)
            excluded = types.InputPeerChannel(channel_id=13, access_hash=1313)
            target = types.InputPeerChannel(channel_id=14, access_hash=1414)
            original = folder(
                pinned=[pinned],
                included=[included],
                excluded=[excluded],
            )
            client = FakeClient(
                [original],
                [dialog("approved_target", target)],
                events,
            )
            service, _adapter = reconciler_for(
                root,
                sources=[source(1, "approved_target", status=SourceStatus.ACTIVE)],
                client=client,
                events=events,
            )

            report = await service.sync()

        self.assertTrue(report.updated)
        self.assertTrue(report.verified)
        self.assertEqual(report.planned_add_count, 1)
        self.assertEqual(client.get_filter_requests, 3)
        self.assertEqual(len(client.update_requests), 1)
        request = client.update_requests[0]
        updated = request.filter
        self.assertIsInstance(request, functions.messages.UpdateDialogFilterRequest)
        self.assertEqual(updated.pinned_peers, original.pinned_peers)
        self.assertEqual(updated.include_peers, [included, target])
        self.assertEqual(updated.exclude_peers, original.exclude_peers)
        self.assertEqual(bytes(updated.title), bytes(original.title))
        for field in (
            "contacts",
            "non_contacts",
            "groups",
            "broadcasts",
            "bots",
            "exclude_muted",
            "exclude_read",
            "exclude_archived",
            "title_noanimate",
            "emoticon",
            "color",
        ):
            self.assertEqual(getattr(updated, field), getattr(original, field))
        self.assertLess(
            events.index("request.get_dialog_filters"),
            events.index("request.update_dialog_filter"),
        )
        second_dialog_read = [
            index
            for index, event in enumerate(events)
            if event == "client.get_dialogs"
        ][1]
        second_adapter_read = [
            index
            for index, event in enumerate(events)
            if event == "adapter.list_for_session"
        ][1]
        self.assertLess(second_dialog_read, second_adapter_read)
        self.assertLess(
            second_adapter_read,
            events.index("request.update_dialog_filter"),
        )

    async def test_exact_source_folder_is_supported(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            client = FakeClient([folder(title="Ист_Лид_АИ")], [], events)
            service, _adapter = reconciler_for(
                root,
                sources=[],
                client=client,
                events=events,
            )

            report = await service.plan()

        self.assertEqual(report.folder_title, "Ист_Лид_АИ")

    async def test_folder_alias_matching_is_trimmed_unicode_and_case_insensitive(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            client = FakeClient([folder(title="  ист_лид_аи  ")], [], events)
            service, _adapter = reconciler_for(
                root,
                sources=[],
                client=client,
                events=events,
            )

            report = await service.plan()

        self.assertEqual(report.folder_title, "  ист_лид_аи  ")

    async def test_missing_ambiguous_and_shared_filters_fail_closed(self):
        cases = (
            ([], "не найдена"),
            (
                [folder(title="Ист_Лид_АИ"), folder(title=" ист_лид_аи ", folder_id=8)],
                "несколько папок",
            ),
            (
                [
                    types.DialogFilterChatlist(
                        id=9,
                        title=types.TextWithEntities("Ист_Лид_АИ", []),
                        pinned_peers=[],
                        include_peers=[],
                    )
                ],
                "chatlist",
            ),
        )
        for filters, message in cases:
            with self.subTest(message=message), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                events = []
                client = FakeClient(filters, [], events)
                service, _adapter = reconciler_for(
                    root,
                    sources=[],
                    client=client,
                    events=events,
                )
                with self.assertRaisesRegex(TelegramFolderError, message):
                    await service.plan()
                self.assertEqual(client.update_requests, [])

    async def test_excluded_target_is_reported_and_sync_refuses_write(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            target = types.InputPeerChannel(channel_id=31, access_hash=3131)
            client = FakeClient(
                [folder(excluded=[target])],
                [dialog("excluded_source", target)],
                events,
            )
            service, _adapter = reconciler_for(
                root,
                sources=[source(1, "excluded_source")],
                client=client,
                events=events,
            )

            plan = await service.plan()
            with self.assertRaisesRegex(TelegramFolderError, "exclude_peers"):
                await service.sync()

        self.assertEqual(plan.blocked_reasons, ("target_in_exclude",))
        self.assertEqual(plan.exclude_conflict_count, 1)
        self.assertEqual(client.update_requests, [])

    async def test_capacity_is_a_conservative_union_of_pinned_and_included(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            existing = [
                types.InputPeerChannel(channel_id=value, access_hash=value * 10)
                for value in range(1, MAX_FOLDER_PEERS + 1)
            ]
            target = types.InputPeerChannel(channel_id=501, access_hash=5010)
            client = FakeClient(
                [folder(included=existing)],
                [dialog("capacity_target", target)],
                events,
            )
            service, _adapter = reconciler_for(
                root,
                sources=[source(1, "capacity_target")],
                client=client,
                events=events,
            )

            report = await service.plan()

        self.assertEqual(report.existing_peer_count, MAX_FOLDER_PEERS)
        self.assertEqual(report.resulting_peer_count, MAX_FOLDER_PEERS + 1)
        self.assertEqual(report.blocked_reasons, ("capacity_exceeded",))
        self.assertFalse(report.would_update)

    async def test_only_adapter_sources_with_allowed_lifecycle_are_accepted(self):
        for status in (SourceStatus.CANDIDATE, SourceStatus.PAUSED, SourceStatus.REJECTED):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                events = []
                client = FakeClient([folder()], [], events)
                service, _adapter = reconciler_for(
                    root,
                    sources=[source(1, "forbidden", status=status)],
                    client=client,
                    events=events,
                )

                with self.assertRaisesRegex(TelegramFolderError, "недопустимый статус"):
                    await service.plan()

                self.assertEqual(client.get_filter_requests, 0)
                self.assertEqual(client.update_requests, [])

    async def test_source_without_existing_dialog_is_never_resolved_or_added(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            client = FakeClient([folder()], [], events)
            service, _adapter = reconciler_for(
                root,
                sources=[source(1, "not_open")],
                client=client,
                events=events,
            )

            report = await service.sync()

        self.assertEqual(report.missing_dialog_count, 1)
        self.assertEqual(report.planned_add_count, 0)
        self.assertFalse(report.updated)
        self.assertTrue(report.verified)
        self.assertEqual(client.update_requests, [])

    async def test_inactive_secondary_username_is_not_treated_as_dialog_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            peer = types.InputPeerChannel(channel_id=51, access_hash=5151)
            existing_dialog = SimpleNamespace(
                entity=SimpleNamespace(
                    username="primary_name",
                    usernames=[types.Username("inactive_alias", active=None)],
                ),
                input_entity=peer,
            )
            client = FakeClient([folder()], [existing_dialog], events)
            service, _adapter = reconciler_for(
                root,
                sources=[source(1, "inactive_alias")],
                client=client,
                events=events,
            )

            report = await service.sync()

        self.assertEqual(report.missing_dialog_count, 1)
        self.assertEqual(report.planned_add_count, 0)
        self.assertEqual(client.update_requests, [])

    async def test_source_without_validation_binding_is_never_added(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            target = types.InputPeerChannel(channel_id=52, access_hash=5252)
            client = FakeClient(
                [folder()],
                [dialog("unvalidated_target", target)],
                events,
            )
            binding_loader = FakeIdentityBindingLoader({}, events)
            service, _adapter = reconciler_for(
                root,
                sources=[source(1, "unvalidated_target")],
                client=client,
                events=events,
                identity_binding_loader=binding_loader,
            )

            report = await service.sync()

        self.assertEqual(report.unvalidated_identity_count, 1)
        self.assertEqual(report.identity_mismatch_count, 0)
        self.assertEqual(report.sources[0].result, "unvalidated_identity")
        self.assertEqual(client.update_requests, [])

    async def test_username_match_with_different_canonical_peer_is_never_added(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            target_source = source(1, "transferred_username")
            target = types.InputPeerChannel(channel_id=53, access_hash=5353)
            client = FakeClient(
                [folder()],
                [dialog("transferred_username", target)],
                events,
            )
            binding_loader = FakeIdentityBindingLoader(
                {
                    1: TelegramSourceIdentityBinding(
                        source_id=1,
                        state="accessible",
                        canonical_peer_id=999,
                    )
                },
                events,
            )
            service, _adapter = reconciler_for(
                root,
                sources=[target_source],
                client=client,
                events=events,
                identity_binding_loader=binding_loader,
            )

            report = await service.sync()

        self.assertEqual(report.unvalidated_identity_count, 0)
        self.assertEqual(report.identity_mismatch_count, 1)
        self.assertEqual(report.sources[0].result, "identity_mismatch")
        self.assertEqual(client.update_requests, [])

    async def test_user_or_basic_chat_numeric_collision_is_never_added(self):
        cases = (
            types.InputPeerUser(user_id=54, access_hash=5454),
            types.InputPeerChat(chat_id=55),
        )
        for peer in cases:
            with self.subTest(peer=type(peer).__name__), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                events = []
                username = f"collision_{type(peer).__name__.lower()}"
                client = FakeClient([folder()], [dialog(username, peer)], events)
                service, _adapter = reconciler_for(
                    root,
                    sources=[source(1, username)],
                    client=client,
                    events=events,
                )

                report = await service.sync()

                self.assertEqual(report.identity_mismatch_count, 1)
                self.assertEqual(report.sources[0].result, "identity_mismatch")
                self.assertEqual(client.update_requests, [])

    async def test_public_supergroup_channel_is_allowed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            peer = types.InputPeerChannel(channel_id=56, access_hash=5656)
            existing_dialog = dialog("public_supergroup", peer)
            existing_dialog.entity.broadcast = False
            existing_dialog.entity.megagroup = True
            client = FakeClient([folder()], [existing_dialog], events)
            service, _adapter = reconciler_for(
                root,
                sources=[source(1, "public_supergroup")],
                client=client,
                events=events,
            )

            report = await service.sync()

        self.assertTrue(report.updated)
        self.assertEqual(report.identity_mismatch_count, 0)
        self.assertEqual(len(client.update_requests), 1)

    async def test_sync_reloads_approved_sources_before_write(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            target_source = source(1, "race_target")
            target = types.InputPeerChannel(channel_id=61, access_hash=6161)
            client = FakeClient(
                [folder()],
                [dialog("race_target", target)],
                events,
            )
            adapter = ChangingApprovedSourceAdapter(
                [[target_source], []],
                events,
            )
            service, _adapter = reconciler_for(
                root,
                sources=[],
                client=client,
                events=events,
                source_adapter=adapter,
            )

            report = await service.sync()

        self.assertEqual(adapter.calls, 2)
        self.assertEqual(report.approved_source_count, 0)
        self.assertEqual(report.planned_add_count, 0)
        self.assertFalse(report.updated)
        self.assertTrue(report.verified)
        self.assertEqual(client.get_filter_requests, 2)
        self.assertEqual(client.update_requests, [])

    async def test_inactive_collector_account_fails_before_telegram_folder_reads(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            client = FakeClient([folder()], [], events)
            service, _adapter = reconciler_for(
                root,
                sources=[],
                client=client,
                events=events,
                active=False,
            )

            with self.assertRaisesRegex(TelegramFolderError, "отключён"):
                await service.plan()

        self.assertEqual(client.get_filter_requests, 0)
        self.assertEqual(client.update_requests, [])

    async def test_failed_post_verification_is_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            target = types.InputPeerChannel(channel_id=71, access_hash=7171)
            client = FakeClient(
                [folder()],
                [dialog("verify_target", target)],
                events,
                corrupt_post_verify=True,
            )
            service, _adapter = reconciler_for(
                root,
                sources=[source(1, "verify_target")],
                client=client,
                events=events,
            )

            with self.assertRaisesRegex(TelegramFolderError, "Флаги"):
                await service.sync()

        self.assertEqual(len(client.update_requests), 1)
        self.assertEqual(client.get_filter_requests, 3)

    async def test_cancellation_waits_for_disconnect_before_releasing_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = []
            disconnect_gate = asyncio.Event()
            client = FakeClient(
                [folder()],
                [],
                events,
                disconnect_gate=disconnect_gate,
            )
            service, _adapter = reconciler_for(
                root,
                sources=[],
                client=client,
                events=events,
            )

            task = asyncio.create_task(service.plan())
            while "client.disconnect.started" not in events:
                await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
            self.assertNotIn("lock.release", events)

            disconnect_gate.set()
            with self.assertRaises(asyncio.CancelledError):
                await task

        self.assertLess(
            events.index("client.disconnect.completed"),
            events.index("lock.release"),
        )


if __name__ == "__main__":
    unittest.main()

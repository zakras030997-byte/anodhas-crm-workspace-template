from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

from telethon import functions, types

from ai_sales_radar.config import OperatorBotCredentials, TelegramCredentials
from ai_sales_radar.operator_bot.folder import (
    OperatorBotFolderError,
    OperatorBotFolderReconciler,
)
from ai_sales_radar.operator_bot.folder_cli import build_parser
from ai_sales_radar.telegram_folder import MAX_FOLDER_PEERS, TelegramFolderError


OWNER_ID = 70_001
BOT_ID = 123_456_789
BOT_TOKEN = f"{BOT_ID}:" + "A" * 35
ACCESS_HASH = 9_876_543_210


def project_folder(
    *,
    title: str = "Ист_Лид_АИ",
    pinned=(),
    included=(),
    excluded=(),
    emoticon: str = "📂",
) -> types.DialogFilter:
    return types.DialogFilter(
        id=167,
        title=types.TextWithEntities(
            text=title,
            entities=[types.MessageEntityBold(offset=0, length=3)],
        ),
        pinned_peers=list(pinned),
        include_peers=list(included),
        exclude_peers=list(excluded),
        contacts=False,
        non_contacts=True,
        groups=True,
        broadcasts=True,
        bots=False,
        exclude_muted=True,
        exclude_read=False,
        exclude_archived=True,
        title_noanimate=True,
        emoticon=emoticon,
        color=5,
    )


def bot_dialog(
    *,
    bot_id: int = BOT_ID,
    peer_id: int | None = None,
    entity_hash: int = ACCESS_HASH,
    peer_hash: int | None = None,
    is_bot: bool = True,
    deleted: bool = False,
    peer=None,
):
    selected_peer_id = bot_id if peer_id is None else peer_id
    selected_peer_hash = entity_hash if peer_hash is None else peer_hash
    selected_peer = peer or types.InputPeerUser(
        user_id=selected_peer_id,
        access_hash=selected_peer_hash,
    )
    entity = types.User(
        id=bot_id,
        access_hash=entity_hash,
        first_name="AI Sales Radar",
        username="AI_Sales_Radar_Bot",
        bot=is_bot,
        deleted=deleted,
    )
    return SimpleNamespace(entity=entity, input_entity=selected_peer)


def clone_folder(value: types.DialogFilter, *, emoticon: str | None = None):
    return types.DialogFilter(
        id=value.id,
        title=value.title,
        pinned_peers=list(value.pinned_peers),
        include_peers=list(value.include_peers),
        exclude_peers=list(value.exclude_peers),
        contacts=value.contacts,
        non_contacts=value.non_contacts,
        groups=value.groups,
        broadcasts=value.broadcasts,
        bots=value.bots,
        exclude_muted=value.exclude_muted,
        exclude_read=value.exclude_read,
        exclude_archived=value.exclude_archived,
        title_noanimate=value.title_noanimate,
        emoticon=value.emoticon if emoticon is None else emoticon,
        color=value.color,
    )


class FakeLock:
    def __init__(self, _path, *, role: str, events: list[str]) -> None:
        self.role = role
        self.events = events

    def acquire(self) -> None:
        self.events.append("lock.acquire")

    def release(self) -> None:
        self.events.append("lock.release")


class FakeClient:
    def __init__(
        self,
        current_filter: types.DialogFilter,
        dialogs,
        events: list[str],
        *,
        owner_id: int = OWNER_ID,
        authorized: bool = True,
        reject_update: bool = False,
        corrupt_post_verify: bool = False,
        second_filter: types.DialogFilter | None = None,
    ) -> None:
        self.current_filter = current_filter
        self.dialogs = list(dialogs)
        self.events = events
        self.owner_id = owner_id
        self.authorized = authorized
        self.reject_update = reject_update
        self.corrupt_post_verify = corrupt_post_verify
        self.second_filter = second_filter
        self.get_filter_requests = 0
        self.update_requests = []
        self.updated = False

    async def connect(self):
        self.events.append("client.connect")

    async def disconnect(self):
        self.events.append("client.disconnect")

    async def is_user_authorized(self):
        self.events.append("client.is_user_authorized")
        return self.authorized

    async def get_me(self):
        self.events.append("client.get_me")
        return SimpleNamespace(id=self.owner_id, bot=False)

    async def get_dialogs(self, limit=None):
        if limit is not None:
            raise AssertionError("all dialogs must be read")
        self.events.append("client.get_dialogs")
        return list(self.dialogs)

    async def __call__(self, request):
        if isinstance(request, functions.messages.GetDialogFiltersRequest):
            self.get_filter_requests += 1
            self.events.append("request.get_dialog_filters")
            if self.second_filter is not None and self.get_filter_requests >= 2:
                self.current_filter = self.second_filter
                self.second_filter = None
            selected = self.current_filter
            if self.updated and self.corrupt_post_verify:
                selected = clone_folder(selected, emoticon="❌")
            return types.messages.DialogFilters(
                filters=[selected],
                tags_enabled=True,
            )
        if isinstance(request, functions.messages.UpdateDialogFilterRequest):
            self.events.append("request.update_dialog_filter")
            self.update_requests.append(request)
            if self.reject_update:
                return False
            self.current_filter = request.filter
            self.updated = True
            return True
        raise AssertionError(f"unexpected request: {type(request).__name__}")


def reconciler_for(root: Path, client: FakeClient, events: list[str]):
    user_credentials = TelegramCredentials(
        api_id=33_441_450,
        api_hash="PRIVATE_API_HASH",
        session_path=root / "collector.session",
    )
    bot_credentials = OperatorBotCredentials(
        api_id=33_441_450,
        api_hash="PRIVATE_API_HASH",
        bot_token=BOT_TOKEN,
        bot_id=BOT_ID,
        owner_user_id=OWNER_ID,
        owner_chat_id=OWNER_ID,
        session_path=root / "operator_bot.session",
    )

    def client_factory(credentials):
        events.append("client.factory")
        if credentials.session_path != user_credentials.session_path:
            raise AssertionError("folder must use collector session")
        return client

    def lock_factory(path, *, role):
        if path != user_credentials.session_path:
            raise AssertionError("lock must protect collector session")
        return FakeLock(path, role=role, events=events)

    return OperatorBotFolderReconciler(
        user_credentials,
        bot_credentials,
        client_factory=client_factory,
        lock_factory=lock_factory,
        runtime_root=root,
    )


class OperatorBotFolderParserTest(unittest.TestCase):
    def test_only_explicit_plan_and_sync_exist(self):
        parser = build_parser()
        self.assertEqual(parser.parse_args(["plan"]).command, "plan")
        sync = parser.parse_args(["sync"])
        self.assertEqual(sync.command, "sync")
        self.assertIn("не поддерживает CAS", sync.concurrent_edit_warning)
        with self.assertRaises(SystemExit):
            parser.parse_args([])


class OperatorBotFolderReconcilerTest(unittest.IsolatedAsyncioTestCase):
    async def test_plan_is_read_only_and_report_contains_no_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events: list[str] = []
            client = FakeClient(project_folder(), [bot_dialog()], events)
            report = await reconciler_for(root, client, events).plan()

        self.assertEqual(report.result, "would_add")
        self.assertTrue(report.would_update)
        self.assertEqual(report.bot_username, "@AI_Sales_Radar_Bot")
        self.assertEqual(client.update_requests, [])
        self.assertLess(events.index("lock.acquire"), events.index("client.factory"))
        self.assertEqual(events[-2:], ["client.disconnect", "lock.release"])
        payload = json.dumps(asdict(report), ensure_ascii=False)
        for secret in (
            BOT_TOKEN,
            "PRIVATE_API_HASH",
            str(ACCESS_HASH),
            str(OWNER_ID),
            "collector.session",
            "operator_bot.session",
        ):
            self.assertNotIn(secret, payload)

    async def test_sync_appends_only_bot_and_preserves_complete_filter(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events: list[str] = []
            pinned = types.InputPeerUser(user_id=11, access_hash=111)
            included = types.InputPeerChat(chat_id=12)
            excluded = types.InputPeerChannel(channel_id=13, access_hash=1313)
            original = project_folder(
                pinned=[pinned],
                included=[included],
                excluded=[excluded],
            )
            target_dialog = bot_dialog()
            client = FakeClient(original, [target_dialog], events)
            report = await reconciler_for(root, client, events).sync()

        self.assertTrue(report.updated)
        self.assertTrue(report.verified)
        self.assertEqual(len(client.update_requests), 1)
        updated = client.update_requests[0].filter
        self.assertEqual(updated.pinned_peers, [pinned])
        self.assertEqual(updated.include_peers, [included, target_dialog.input_entity])
        self.assertEqual(updated.exclude_peers, [excluded])
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
        self.assertEqual(client.get_filter_requests, 3)

    async def test_already_included_or_pinned_is_an_idempotent_verified_noop(self):
        target = bot_dialog()
        for location in ("included", "pinned"):
            with self.subTest(location=location), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                events: list[str] = []
                kwargs = {location: [target.input_entity]}
                client = FakeClient(project_folder(**kwargs), [target], events)
                report = await reconciler_for(root, client, events).sync()

                self.assertEqual(report.result, "already_present")
                self.assertFalse(report.updated)
                self.assertTrue(report.verified)
                self.assertEqual(client.update_requests, [])

    async def test_identity_failures_never_write(self):
        wrong_peer = types.InputPeerChannel(channel_id=BOT_ID, access_hash=ACCESS_HASH)
        cases = (
            ([], "не найден"),
            ([bot_dialog(is_bot=False)], "личности"),
            ([bot_dialog(deleted=True)], "личности"),
            ([bot_dialog(peer_hash=ACCESS_HASH + 1)], "личности"),
            ([bot_dialog(peer=wrong_peer)], "личности"),
            ([bot_dialog(), bot_dialog()], "несколько"),
        )
        for dialogs, message in cases:
            with self.subTest(message=message), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                events: list[str] = []
                client = FakeClient(project_folder(), dialogs, events)
                with self.assertRaisesRegex(OperatorBotFolderError, message):
                    await reconciler_for(root, client, events).sync()
                self.assertEqual(client.update_requests, [])

    async def test_exact_folder_owner_exclude_and_capacity_fail_closed(self):
        target = bot_dialog()
        full = [
            types.InputPeerChannel(channel_id=value, access_hash=value * 10)
            for value in range(1, MAX_FOLDER_PEERS + 1)
        ]
        cases = (
            (FakeClient(project_folder(title="ЛидГениратор"), [target], []), "не найдена"),
            (FakeClient(project_folder(), [target], [], owner_id=OWNER_ID + 1), "владельцу"),
            (FakeClient(project_folder(excluded=[target.input_entity]), [target], []), "exclude_peers"),
            (FakeClient(project_folder(included=full), [target], []), "Лимит"),
        )
        for client, message in cases:
            with self.subTest(message=message), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                events: list[str] = []
                client.events = events
                with self.assertRaisesRegex((OperatorBotFolderError, TelegramFolderError), message):
                    await reconciler_for(root, client, events).sync()
                self.assertEqual(client.update_requests, [])

    async def test_sync_rereads_remote_state_before_write(self):
        target = bot_dialog()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events: list[str] = []
            appeared = project_folder(included=[target.input_entity])
            client = FakeClient(
                project_folder(),
                [target],
                events,
                second_filter=appeared,
            )
            report = await reconciler_for(root, client, events).sync()

        self.assertEqual(report.result, "already_present")
        self.assertTrue(report.verified)
        self.assertFalse(report.updated)
        self.assertEqual(client.update_requests, [])

    async def test_rejected_update_and_post_verify_mismatch_are_not_retried(self):
        for options, message in (
            ({"reject_update": True}, "не подтвердил"),
            ({"corrupt_post_verify": True}, "Флаги"),
        ):
            with self.subTest(message=message), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                events: list[str] = []
                client = FakeClient(project_folder(), [bot_dialog()], events, **options)
                with self.assertRaisesRegex(TelegramFolderError, message):
                    await reconciler_for(root, client, events).sync()
                self.assertEqual(len(client.update_requests), 1)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

from types import SimpleNamespace
import unittest

from telethon import functions, types

from ai_sales_radar.collector_folder_gate import (
    CANARY_SOURCE_IDS,
    EXACT_COLLECTOR_FOLDER_TITLE,
    ExactCanaryTelegramFolderGate,
    TelegramCollectorFolderGateError,
)


def source(source_id: int):
    return SimpleNamespace(record=SimpleNamespace(id=source_id))


def sources(source_ids=CANARY_SOURCE_IDS):
    return tuple(source(source_id) for source_id in source_ids)


def resolved(selected, *, peer_ids=None, access_hash=919191):
    selected_peer_ids = peer_ids or {
        item.record.id: 100_000 + item.record.id for item in selected
    }
    return tuple(
        (
            item,
            types.InputChannel(
                channel_id=selected_peer_ids[item.record.id],
                access_hash=access_hash,
            ),
        )
        for item in selected
    )


def peer(channel_id: int, access_hash: int = 818181):
    return types.InputPeerChannel(
        channel_id=channel_id,
        access_hash=access_hash,
    )


def folder(
    *,
    title=EXACT_COLLECTOR_FOLDER_TITLE,
    folder_id=7,
    pinned=(),
    included=(),
    excluded=(),
):
    return types.DialogFilter(
        id=folder_id,
        title=types.TextWithEntities(text=title, entities=[]),
        pinned_peers=list(pinned),
        include_peers=list(included),
        exclude_peers=list(excluded),
    )


class FakeClient:
    def __init__(self, filters):
        self.filters = list(filters)
        self.requests = []

    async def __call__(self, request):
        self.requests.append(request)
        if not isinstance(request, functions.messages.GetDialogFiltersRequest):
            raise AssertionError("collector folder gate must be read-only")
        return types.messages.DialogFilters(
            filters=self.filters,
            tags_enabled=True,
        )


class FakeGovernor:
    def __init__(self, *, error=None):
        self.error = error
        self.categories = []

    async def run(self, category, operation):
        self.categories.append(category)
        if self.error is not None:
            raise self.error
        return await operation()


class ExactCanaryTelegramFolderGateTest(unittest.IsolatedAsyncioTestCase):
    def test_eligible_catalog_scope_is_exact(self):
        gate = ExactCanaryTelegramFolderGate()
        gate.validate_catalog(sources())

        with self.assertRaisesRegex(
            TelegramCollectorFolderGateError,
            "Eligible Telegram-каталог",
        ):
            gate.validate_catalog(sources((*CANARY_SOURCE_IDS, 99)))

    async def test_accepts_exact_folder_by_immutable_channel_ids_read_only(self):
        selected = sources()
        resolved_sources = resolved(selected)
        peers = [peer(entity.channel_id) for _source, entity in resolved_sources]
        client = FakeClient(
            [folder(pinned=peers[:2], included=peers[2:])]
        )
        governor = FakeGovernor()

        await ExactCanaryTelegramFolderGate().verify(
            client,
            selected_sources=selected,
            resolved_sources=resolved_sources,
            governor=governor,
        )

        self.assertEqual(governor.categories, ["entity_access"])
        self.assertEqual(len(client.requests), 1)
        self.assertIsInstance(
            client.requests[0],
            functions.messages.GetDialogFiltersRequest,
        )

    async def test_only_exact_source_folder_name_is_accepted(self):
        selected = sources()
        resolved_sources = resolved(selected)
        included = [peer(entity.channel_id) for _source, entity in resolved_sources]
        cases = (
            "Ист_Лид_AI",
            " Ист_Лид_АИ ",
            "ист_лид_аи",
        )
        for title in cases:
            with self.subTest(title=title):
                client = FakeClient([folder(title=title, included=included)])
                with self.assertRaisesRegex(
                    TelegramCollectorFolderGateError,
                    "точная папка",
                ):
                    await ExactCanaryTelegramFolderGate().verify(
                        client,
                        selected_sources=selected,
                        resolved_sources=resolved_sources,
                        governor=FakeGovernor(),
                    )

    async def test_missing_ambiguous_or_shared_exact_folder_fails_closed(self):
        selected = sources()
        resolved_sources = resolved(selected)
        included = [peer(entity.channel_id) for _source, entity in resolved_sources]
        shared = types.DialogFilterChatlist(
            id=9,
            title=types.TextWithEntities(EXACT_COLLECTOR_FOLDER_TITLE, []),
            pinned_peers=[],
            include_peers=included,
        )
        cases = (
            ([], "точная папка"),
            (
                [
                    folder(included=included),
                    folder(folder_id=8, included=included),
                ],
                "несколько папок",
            ),
            ([shared], "приватной"),
        )
        for filters, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(
                    TelegramCollectorFolderGateError,
                    message,
                ):
                    await ExactCanaryTelegramFolderGate().verify(
                        FakeClient(filters),
                        selected_sources=selected,
                        resolved_sources=resolved_sources,
                        governor=FakeGovernor(),
                    )

    async def test_missing_or_excluded_binding_fails_without_peer_secrets(self):
        selected = sources()
        peer_ids = {
            item.record.id: 987_654_000 + item.record.id for item in selected
        }
        resolved_sources = resolved(
            selected,
            peer_ids=peer_ids,
            access_hash=777_666_555_444,
        )
        included = [
            peer(entity.channel_id)
            for source_item, entity in resolved_sources
            if source_item.record.id != 3
        ]
        cases = (
            (folder(included=included), "отсутствуют source ID 3"),
            (
                folder(
                    included=[
                        peer(entity.channel_id)
                        for _, entity in resolved_sources
                    ],
                    excluded=[peer(peer_ids[3])],
                ),
                "исключает source ID 3",
            ),
        )
        for selected_folder, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(
                    TelegramCollectorFolderGateError,
                    message,
                ) as raised:
                    await ExactCanaryTelegramFolderGate().verify(
                        FakeClient([selected_folder]),
                        selected_sources=selected,
                        resolved_sources=resolved_sources,
                        governor=FakeGovernor(),
                    )
                output = str(raised.exception)
                self.assertNotIn(str(peer_ids[3]), output)
                self.assertNotIn("777666555444", output)

    async def test_wrong_or_duplicate_resolved_identity_fails_before_folder_read(self):
        selected = sources()
        valid = list(resolved(selected))
        cases = []

        wrong_type = valid.copy()
        wrong_type[0] = (
            selected[0],
            types.InputPeerChannel(channel_id=100_003, access_hash=919191),
        )
        cases.append(wrong_type)

        duplicate = valid.copy()
        duplicate[1] = (
            selected[1],
            types.InputChannel(
                channel_id=valid[0][1].channel_id,
                access_hash=919191,
            ),
        )
        cases.append(duplicate)

        for resolved_case in cases:
            with self.subTest(entity=type(resolved_case[0][1]).__name__):
                client = FakeClient([])
                with self.assertRaises(TelegramCollectorFolderGateError):
                    await ExactCanaryTelegramFolderGate().verify(
                        client,
                        selected_sources=selected,
                        resolved_sources=resolved_case,
                        governor=FakeGovernor(),
                    )
                self.assertEqual(client.requests, [])

    async def test_scope_is_exact_and_rejected_before_telegram_request(self):
        expected_ids = ", ".join(str(value) for value in CANARY_SOURCE_IDS)
        cases = (
            CANARY_SOURCE_IDS[:-1],
            (*CANARY_SOURCE_IDS, 99),
            (*CANARY_SOURCE_IDS[:-1], CANARY_SOURCE_IDS[-2]),
        )
        gate = ExactCanaryTelegramFolderGate()
        for source_ids in cases:
            with self.subTest(source_ids=source_ids):
                selected = sources(source_ids)
                client = FakeClient([])
                with self.assertRaisesRegex(
                    TelegramCollectorFolderGateError,
                    f"ровно source ID {expected_ids}",
                ):
                    await gate.verify(
                        client,
                        selected_sources=selected,
                        resolved_sources=(),
                        governor=FakeGovernor(),
                    )
                self.assertEqual(client.requests, [])

    async def test_governor_failure_is_not_bypassed_or_masked(self):
        selected = sources()
        resolved_sources = resolved(selected)
        included = [peer(entity.channel_id) for _source, entity in resolved_sources]
        client = FakeClient([folder(included=included)])
        failure = RuntimeError("governor unavailable")
        governor = FakeGovernor(error=failure)

        with self.assertRaisesRegex(RuntimeError, "governor unavailable"):
            await ExactCanaryTelegramFolderGate().verify(
                client,
                selected_sources=selected,
                resolved_sources=resolved_sources,
                governor=governor,
            )

        self.assertEqual(governor.categories, ["entity_access"])
        self.assertEqual(client.requests, [])

    async def test_unknown_or_malformed_unrelated_folder_peer_fails_closed(self):
        selected = sources()
        resolved_sources = resolved(selected)
        included = [peer(entity.channel_id) for _, entity in resolved_sources]
        malformed_peers = (
            object(),
            types.InputPeerChannel(channel_id=777, access_hash=0),
            types.InputPeerUser(user_id=778, access_hash=0),
            types.InputPeerChat(chat_id=0),
        )
        for malformed in malformed_peers:
            with self.subTest(peer_type=type(malformed).__name__):
                with self.assertRaisesRegex(
                    TelegramCollectorFolderGateError,
                    "peer",
                ):
                    await ExactCanaryTelegramFolderGate().verify(
                        FakeClient(
                            [folder(included=[*included, malformed])]
                        ),
                        selected_sources=selected,
                        resolved_sources=resolved_sources,
                        governor=FakeGovernor(),
                    )

    async def test_valid_extra_user_and_basic_chat_peers_are_ignored(self):
        selected = sources()
        resolved_sources = resolved(selected)
        included = [peer(entity.channel_id) for _, entity in resolved_sources]
        included.extend(
            (
                types.InputPeerUser(user_id=778, access_hash=900000000),
                types.InputPeerChat(chat_id=779),
            )
        )

        await ExactCanaryTelegramFolderGate().verify(
            FakeClient([folder(included=included)]),
            selected_sources=selected,
            resolved_sources=resolved_sources,
            governor=FakeGovernor(),
        )


if __name__ == "__main__":
    unittest.main()

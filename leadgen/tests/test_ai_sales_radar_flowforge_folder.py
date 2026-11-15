from __future__ import annotations

import unittest

from telethon import types

from ai_sales_radar.flowforge_folder import (
    FlowforgeFolderError,
    verify_flowforge_dialog_filter_post_update,
)


def folder(peers):
    return types.DialogFilter(
        id=13,
        title=types.TextWithEntities(text="🤖Flowforge", entities=[]),
        pinned_peers=[],
        include_peers=list(peers),
        exclude_peers=[],
        contacts=False,
        non_contacts=False,
        groups=False,
        broadcasts=False,
        bots=False,
        exclude_muted=False,
        exclude_read=False,
        exclude_archived=False,
        title_noanimate=False,
        emoticon="🤖",
        color=6,
    )


class FlowforgeFolderVerificationTest(unittest.TestCase):
    def test_telegram_include_reordering_is_accepted(self):
        first = types.InputPeerUser(user_id=1, access_hash=11)
        second = types.InputPeerChannel(channel_id=2, access_hash=22)

        verify_flowforge_dialog_filter_post_update(
            folder([first, second]),
            folder([second, first]),
        )

    def test_changed_membership_is_rejected(self):
        first = types.InputPeerUser(user_id=1, access_hash=11)
        second = types.InputPeerChannel(channel_id=2, access_hash=22)
        replacement = types.InputPeerChannel(channel_id=3, access_hash=33)

        with self.assertRaises(FlowforgeFolderError):
            verify_flowforge_dialog_filter_post_update(
                folder([first, second]),
                folder([first, replacement]),
            )


if __name__ == "__main__":
    unittest.main()

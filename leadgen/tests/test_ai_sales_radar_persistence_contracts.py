from __future__ import annotations

import unittest
from uuid import uuid4

from ai_sales_radar.inbox import status_request_key
from ai_sales_radar.persistence.inbox import CommercialScoreSnapshotRepository
from ai_sales_radar.persistence.schema import (
    commercial_score_snapshots,
    lead_inbox_items,
    reply_drafts,
)
from freelancer_bot.persistence.schema import match_traces
from ai_sales_radar.persistence.inbox import InboxStatus


def _constraint_names(table) -> set[str]:
    return {str(item.name) for item in table.constraints}


class PersistenceGraphContractTest(unittest.TestCase):
    def test_schema_rejects_cross_context_links(self):
        self.assertIn(
            "uq_match_traces_identity_context",
            _constraint_names(match_traces),
        )
        self.assertIn(
            "uq_commercial_score_snapshots_id_opportunity",
            _constraint_names(commercial_score_snapshots),
        )
        self.assertIn(
            "fk_lead_inbox_items_latest_match_context_valid",
            _constraint_names(lead_inbox_items),
        )
        self.assertIn(
            "fk_lead_inbox_items_commercial_score_opportunity_valid",
            _constraint_names(lead_inbox_items),
        )
        self.assertIn(
            "fk_reply_drafts_inbox_context_valid",
            _constraint_names(reply_drafts),
        )
        self.assertIn(
            "fk_reply_drafts_match_context_valid",
            _constraint_names(reply_drafts),
        )
        self.assertTrue(
            {"opportunity_id", "search_profile_id"}.issubset(
                reply_drafts.c.keys()
            )
        )

    def test_status_request_identity_is_target_independent(self):
        item_id = uuid4()
        opened = status_request_key(
            item_id=item_id,
            target=InboxStatus.OPENED,
            request_id="operator-request-1",
        )
        ignored = status_request_key(
            item_id=item_id,
            target=InboxStatus.IGNORED,
            request_id="operator-request-1",
        )
        another = status_request_key(
            item_id=item_id,
            target=InboxStatus.OPENED,
            request_id="operator-request-2",
        )
        self.assertEqual(opened, ignored)
        self.assertNotEqual(opened, another)


class CommercialScorePersistenceValidationTest(unittest.IsolatedAsyncioTestCase):
    async def test_band_must_match_score_before_database_io(self):
        repository = CommercialScoreSnapshotRepository()
        with self.assertRaisesRegex(ValueError, "band does not match"):
            await repository.ensure(
                object(),
                opportunity_id=uuid4(),
                policy_version="commercial-score.v1",
                input_sha256="a" * 64,
                total_score=90,
                band="low",
                breakdown={},
            )

    async def test_score_must_be_an_integer(self):
        repository = CommercialScoreSnapshotRepository()
        with self.assertRaisesRegex(ValueError, "integer"):
            await repository.ensure(
                object(),
                opportunity_id=uuid4(),
                policy_version="commercial-score.v1",
                input_sha256="a" * 64,
                total_score=80.5,
                band="hot",
                breakdown={},
            )


if __name__ == "__main__":
    unittest.main()

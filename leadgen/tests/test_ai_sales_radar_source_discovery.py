from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from uuid import UUID

from telethon.tl.types import Channel

from ai_sales_radar.source_discovery import (
    build_operator_source_queries,
    parse_source_discovery_job_key,
    source_discovery_job_key,
)
from freelancer_bot.discovery import DiscoveryRequest
from freelancer_bot.telegram_profile_discovery import (
    TelegramGlobalSearchProvider,
    TelegramProfileSearchQuery,
)


NOW = datetime(2026, 8, 27, 16, 0, tzinfo=timezone.utc)
PROFILE_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


class _Governor:
    def __init__(self) -> None:
        self.categories: list[str] = []

    async def run(self, category, operation):
        self.categories.append(category)
        return await operation()


class _SearchClient:
    async def get_messages(self, _entity, *, search, limit, offset_id=0):
        self.last_request = (search, limit, offset_id)
        large_group = Channel(
            id=1001,
            title="AI заказчики и исполнители",
            photo=None,
            date=NOW,
            megagroup=True,
            username="ai_marketplace_large",
            participants_count=12_500,
        )
        small_channel = Channel(
            id=1002,
            title="Маленький AI канал",
            photo=None,
            date=NOW,
            broadcast=True,
            username="ai_marketplace_small",
            participants_count=120,
        )
        return (
            SimpleNamespace(
                id=1,
                chat=large_group,
                date=NOW,
                message="ищу исполнителя для AI автоматизации",
            ),
            SimpleNamespace(
                id=2,
                chat=small_channel,
                date=NOW,
                message="AI фриланс заказы",
            ),
        )


class OperatorSourceDiscoveryTest(unittest.IsolatedAsyncioTestCase):
    def test_job_key_is_canonical_and_bucketed_for_cooldown(self):
        key = source_discovery_job_key(
            PROFILE_ID,
            4,
            requested_at=NOW,
            cooldown_seconds=1800,
        )
        self.assertEqual(parse_source_discovery_job_key(key)[:2], (PROFILE_ID, 4))
        same_bucket = source_discovery_job_key(
            PROFILE_ID,
            4,
            requested_at=NOW.replace(minute=10),
            cooldown_seconds=1800,
        )
        self.assertEqual(key, same_bucket)

    def test_query_plan_covers_buyer_seller_and_marketplace_language(self):
        intent = SimpleNamespace(
            id=PROFILE_ID,
            profile_revision=4,
            languages=("ru", "en"),
            roles=("AI Automation Developer",),
            services=("AI Automation",),
            skills=("n8n", "RAG"),
            industries=(),
        )
        queries = build_operator_source_queries(intent, max_queries=12)
        texts = {item.text.casefold() for item in queries}
        self.assertEqual(len(queries), 12)
        self.assertTrue(any("ищу исполнителя" in item for item in texts))
        self.assertTrue(any("ищу заказчика" in item for item in texts))
        self.assertTrue(any("фриланс заказы" in item for item in texts))

    async def test_provider_keeps_only_public_large_communities(self):
        query = TelegramProfileSearchQuery(
            text="AI автоматизация фриланс",
            family="OPERATOR_MARKETPLACE",
            language="ru",
            angle="buyer_habitat",
            query_kind="marketplace",
            tier="broad",
        )
        provider = TelegramGlobalSearchProvider(
            _SearchClient(),
            governor=_Governor(),
            intent=SimpleNamespace(id=PROFILE_ID, profile_revision=4),
            queries=(query,),
            max_candidates=2,
            minimum_participants=500,
            participant_lookup_limit=2,
            public_only=True,
        )
        candidates = await provider.discover(
            DiscoveryRequest(parameters={}, requested_at=NOW)
        )
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].handle, "@ai_marketplace_large")
        self.assertEqual(candidates[0].context["participants_count"], 12_500)
        self.assertEqual(candidates[0].context["source_kind"], "supergroup")
        self.assertEqual(provider.observability["small_candidates_removed"], 1)


if __name__ == "__main__":
    unittest.main()

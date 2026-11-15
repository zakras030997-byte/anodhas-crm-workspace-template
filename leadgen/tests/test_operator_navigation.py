from datetime import timedelta
import unittest
from unittest.mock import AsyncMock
from types import SimpleNamespace
import logging
from uuid import UUID

from test_ai_sales_radar_operator_bot import FakeCore, controller_for, NOW, OWNER_ID, BOT_TOKEN
from ai_sales_radar.operator_bot.callbacks import CallbackAction, SignedCallbackCodec
from ai_sales_radar.operator_bot.navigation import ROUTES, button, menu_response
from ai_sales_radar.operator_bot.telethon_runtime import TelethonOperatorTransport

IDENTITY = dict(is_private=True, sender_id=OWNER_ID, chat_id=OWNER_ID)


class NavigationTests(unittest.IsolatedAsyncioTestCase):
    async def test_every_menu_route_reaches_existing_handler(self):
        codec = SignedCallbackCodec(BOT_TOKEN)
        for route in ROUTES:
            with self.subTest(route=route):
                core = FakeCore()
                controller = controller_for(core)
                outcome = await controller.handle_callback(
                    **IDENTITY, data=button('Go', route, codec, now=NOW).callback_data)
                self.assertTrue(outcome.responses)
                self.assertTrue(outcome.responses[-1].buttons)
                self.assertNotIn('Кнопка устарела', outcome.responses[0].text)
                if route in {11, 12, 13}:
                    self.assertIn(('leads', {11: 'hot', 12: 'warm', 13: 'low'}[route], False, 5), core.calls)
                if route == 14:
                    self.assertIn(('leads', None, True, 5), core.calls)
                if route == 30:
                    self.assertIn(('source_discovery_start',), core.calls)

    async def test_navigation_is_owner_only(self):
        core = FakeCore()
        controller = controller_for(core)
        codec = SignedCallbackCodec(BOT_TOKEN)
        result = await controller.handle_callback(is_private=True, sender_id=1, chat_id=1,
            data=button('Go', 30, codec, now=NOW).callback_data)
        self.assertIsNone(result)
        self.assertEqual(core.calls, [])

    async def test_expired_and_invalid_routes_do_not_mutate(self):
        for route, when in [(30, NOW - timedelta(hours=2)), (999, NOW)]:
            core = FakeCore()
            controller = controller_for(core)
            outcome = await controller.handle_callback(**IDENTITY,
                data=button('Go', route, SignedCallbackCodec(BOT_TOKEN), now=when).callback_data)
            self.assertEqual(core.calls, [])
            self.assertTrue(outcome.responses[0].buttons)

    async def test_empty_leads_have_return_path(self):
        core = FakeCore()
        core.list_leads = AsyncMock(return_value=())
        result = await controller_for(core).handle_message(**IDENTITY, text='/leads')
        self.assertIn('пока нет', result[0].text)
        self.assertIn('🏠 Главное меню', [b.label for r in result[0].buttons for b in r])

    async def test_navigation_edits_existing_message(self):
        controller = controller_for(FakeCore())
        client = SimpleNamespace(send_message=AsyncMock())
        transport = TelethonOperatorTransport(client, controller, owner_chat_id=OWNER_ID, logger=logging.getLogger('test'))
        event = SimpleNamespace(**IDENTITY, data=button('Go', 3, SignedCallbackCodec(BOT_TOKEN), now=NOW).callback_data,
                                answer=AsyncMock(), edit=AsyncMock())
        await transport._on_callback(event)
        event.edit.assert_awaited_once()
        client.send_message.assert_not_awaited()

    async def test_all_visible_navigation_buttons_are_bounded_and_resolve(self):
        codec = SignedCallbackCodec(BOT_TOKEN)
        for section in ['home', 'leads', 'sources', 'system', 'help']:
            response = menu_response(section, codec, now=NOW)
            self.assertLessEqual(len(response.text), 4096)
            for row in response.buttons:
                self.assertLessEqual(len(row), 2)
                for item in row:
                    decoded = codec.decode(item.callback_data, now=NOW)
                    self.assertEqual(decoded.action, CallbackAction.NAVIGATE)
                    self.assertIn(decoded.lead_id.int, ROUTES)

    async def test_navigation_obeys_read_rate_limit(self):
        core = FakeCore()
        controller = controller_for(core)
        data = button('Go', 40, SignedCallbackCodec(BOT_TOKEN), now=NOW).callback_data
        for _ in range(30):
            await controller.handle_callback(**IDENTITY, data=data)
        result = await controller.handle_callback(**IDENTITY, data=data)
        self.assertIn('Слишком много', result.responses[0].text)
        self.assertEqual(len(core.calls), 30)

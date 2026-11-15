import asyncio
from datetime import datetime, timezone
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from ai_sales_radar.research.catalog import DIRECTIONS, SOURCES
from ai_sales_radar.research.cards import make_card, render_card
from ai_sales_radar.research.service import ResearchService, ResearchBlocked
from ai_sales_radar.research.telegram import TelegramResearchReader
from ai_sales_radar.research.pages import _read, PageUnavailable
from ai_sales_radar.operator_bot.models import OperatorResponse
from ai_sales_radar.operator_bot.controller import OperatorBotController
from ai_sales_radar.operator_bot.auth import OwnerGate
from ai_sales_radar.operator_bot.callbacks import SignedCallbackCodec
from ai_sales_radar.operator_bot.navigation import button
from test_ai_sales_radar_operator_bot import FakeCore, BOT_TOKEN, OWNER_ID, NOW

IDENTITY = dict(is_private=True, sender_id=OWNER_ID, chat_id=OWNER_ID)

class ResearchTests(unittest.IsolatedAsyncioTestCase):
    def test_all_directions_have_relevant_bounded_source_plans(self):
        self.assertEqual(len(DIRECTIONS), 11)
        for direction in DIRECTIONS.values():
            self.assertTrue(5 <= len(direction.sources) <= 10)
            self.assertEqual(len(set(direction.sources)), len(direction.sources))
            self.assertTrue(set(direction.sources) <= SOURCES.keys())

    def test_only_demand_is_selected(self):
        args = dict(url='https://t.me/example_channel_022/1', observed_at=NOW.isoformat(), platform='Telegram')
        self.assertIsNone(make_card(DIRECTIONS[2], text='Предлагаю услуги: лендинг', **args))
        self.assertIsNone(make_card(DIRECTIONS[2], text='Ищу заказчика на лендинг', **args))
        self.assertIsNone(make_card(DIRECTIONS[2], text='Нужен эскиз', **args))
        self.assertIsNone(make_card(DIRECTIONS[2], text='Нужен менеджер ' + '.' * 400 + 'Лендинг', **args))
        self.assertIsNone(make_card(DIRECTIONS[2], text='Не нужен лендинг', **args))
        self.assertIsNotNone(make_card(DIRECTIONS[2], text='Нужен лендинг', **args))

    def test_snippets_do_not_prove_contacts(self):
        card = make_card(DIRECTIONS[2], text='Нужен лендинг. Директор Иванов Иван Иванович +70000000006 contact020@example.invalid',
            url='https://example.org/task', observed_at=None, platform='Kwork', evidence='snippet')
        self.assertEqual(card['phone'], [])
        self.assertEqual(card['email'], [])
        self.assertIsNone(card['decision_maker'])
        self.assertIsNone(card['telegram'])
        self.assertEqual(card['qualification'], 'кандидат на проверку')

    def test_original_contacts_keep_evidence_and_unknowns(self):
        card = make_card(DIRECTIONS[2], text='Нужен лендинг. Контакт: +70000000006 contact021@example.invalid',
            url='https://t.me/example_channel_022/1', observed_at=NOW.isoformat(), platform='Telegram')
        self.assertEqual(len(card['phone']), 1)
        self.assertEqual(card['email'], ['contact021@example.invalid'])
        self.assertIn('не проверена', card['email_status'])
        self.assertIsNone(card['decision_maker'])
        self.assertIsNone(card['telegram'])

    def test_card_is_safe_and_fits_telegram(self):
        card = make_card(DIRECTIONS[2], text='Нужен лендинг <script>' + '&<>' * 500,
            url='https://example.org/task', observed_at=None, platform='Kwork')
        rendered = render_card(card, 1)
        self.assertNotIn('<script>', rendered)
        OperatorResponse(rendered, parse_mode='html')

    async def test_start_is_idempotent_and_only_one_run_is_active(self):
        with tempfile.TemporaryDirectory() as root:
            service = ResearchService(None, root=root, env={})
            first, created = await service.start(2, 'tg', 'same-click')
            second, duplicated = await service.start(2, 'tg', 'same-click')
            third, parallel = await service.start(3, 'tg', 'another-click')
            self.assertTrue(created)
            self.assertFalse(duplicated)
            self.assertFalse(parallel)
            self.assertEqual(first['id'], second['id'])
            self.assertEqual(first['id'], third['id'])

    async def test_start_waits_for_connected_collector(self):
        with tempfile.TemporaryDirectory() as root:
            service = ResearchService(None, root=root, env={}, telegram_reader=TelegramResearchReader())
            with self.assertRaises(ResearchBlocked):
                await service.start(1, 'tg', 'one')
            self.assertIsNone(service.latest())

    async def test_global_without_key_is_blocked_before_job_creation(self):
        with tempfile.TemporaryDirectory() as root:
            service = ResearchService(None, root=root, env={})
            with self.assertRaises(ResearchBlocked):
                await service.start(1, 'global', 'one')
            self.assertIsNone(service.latest())

    async def test_telegram_run_uses_fresh_reader_and_saves_results(self):
        reader = SimpleNamespace(read=AsyncMock(return_value=([
            {'content': 'Нужен лендинг. +70000000006', 'message_url': 'https://t.me/example_channel_022/1', 'message_date': NOW},
            {'content': 'Предлагаю услуги лендинг', 'message_url': 'https://t.me/example_channel_022/2', 'message_date': NOW}], 16)))
        with tempfile.TemporaryDirectory() as root:
            service = ResearchService(None, root=root, env={}, telegram_reader=reader)
            run, _ = await service.start(2, 'tg', 'one')
            finished = await service.execute(run['id'])
            self.assertEqual(finished['status'], 'completed')
            self.assertEqual(finished['scanned'], 2)
            self.assertEqual(len(finished['cards']), 1)
            self.assertEqual(service.latest()['id'], run['id'])
            await service.execute(run['id'])
            reader.read.assert_awaited_once()

    async def test_global_queries_plan_and_reads_original_pages(self):
        backend = SimpleNamespace(search=AsyncMock(return_value=[SimpleNamespace(
            url='https://kwork.ru/projects/job', title='Нужен лендинг', snippet='Заявка')]))
        pages = AsyncMock(return_value=('Нужен лендинг. Почта: contact021@example.invalid', NOW.isoformat()))
        with tempfile.TemporaryDirectory() as root:
            service = ResearchService(None, root=root, env={}, backend=backend, page_reader=pages)
            run, _ = await service.start(2, 'global', 'one')
            with patch('ai_sales_radar.research.service.asyncio.sleep', new=AsyncMock()):
                finished = await service.execute(run['id'])
            self.assertEqual(backend.search.await_count, len(DIRECTIONS[2].sources))
            self.assertEqual(len(finished['cards']), 1)
            self.assertEqual(finished['cards'][0]['email'], ['contact021@example.invalid'])
            pages.assert_awaited_once()

    async def test_restart_marks_unfinished_run_interrupted(self):
        with tempfile.TemporaryDirectory() as root:
            one = ResearchService(None, root=root, env={})
            run, _ = await one.start(2, 'tg', 'one')
            two = ResearchService(None, root=root, env={})
            self.assertEqual(two.get(run['id'])['status'], 'interrupted')

    async def test_full_wizard_selects_direction_and_starts_background_job(self):
        with tempfile.TemporaryDirectory() as root:
            reader = SimpleNamespace(read=AsyncMock(return_value=([], 16)))
            service = ResearchService(None, root=root, env={}, telegram_reader=reader)
            codec = SignedCallbackCodec(BOT_TOKEN)
            controller = OperatorBotController(FakeCore(), gate=OwnerGate(OWNER_ID, OWNER_ID),
                callback_codec=codec, clock=lambda: NOW, research=service)
            direction = await controller.handle_callback(**IDENTITY, data=button('AI', 101, codec, now=NOW).callback_data)
            self.assertIn('Где ищем', direction.responses[0].text)
            start = await controller.handle_callback(**IDENTITY, data=button('TG', 1001, codec, now=NOW).callback_data)
            self.assertIsNotNone(start.background_job)
            result = await controller.complete_research(start.background_job)
            self.assertIn('не найдено', result[0].text)

    async def test_cancel_preserves_progress_and_stops_run(self):
        entered = asyncio.Event()
        class Reader:
            async def read(self, *, on_source):
                await on_source([{'content': 'Нужен лендинг', 'message_url': 'https://t.me/example_channel_022/1', 'message_date': NOW}], 1)
                entered.set()
                await asyncio.Event().wait()
        with tempfile.TemporaryDirectory() as root:
            service = ResearchService(None, root=root, env={}, telegram_reader=Reader())
            run, _ = await service.start(2, 'tg', 'one')
            task = asyncio.create_task(service.execute(run['id']))
            await entered.wait()
            self.assertEqual(service.latest()['scanned'], 1)
            result = await service.cancel()
            self.assertEqual(result['status'], 'cancelled')
            self.assertEqual(len(result['cards']), 1)
            self.assertTrue(task.cancelled())

    async def test_source_failure_preserves_partial_results(self):
        class Reader:
            async def read(self, *, on_source):
                await on_source([{'content': 'Нужен лендинг', 'message_url': 'https://t.me/example_channel_022/1', 'message_date': NOW}], 1)
                raise TimeoutError()
        with tempfile.TemporaryDirectory() as root:
            service = ResearchService(None, root=root, env={}, telegram_reader=Reader())
            run, _ = await service.start(2, 'tg', 'one')
            result = await service.execute(run['id'])
            self.assertEqual(result['status'], 'partial')
            self.assertEqual(len(result['cards']), 1)
            self.assertEqual(result['errors'], ['TimeoutError'])

    async def test_transport_delivers_background_result_and_closes_cleanly(self):
        from ai_sales_radar.operator_bot.telethon_runtime import TelethonOperatorTransport
        import logging
        with tempfile.TemporaryDirectory() as root:
            reader = SimpleNamespace(read=AsyncMock(return_value=([], 16)))
            service = ResearchService(None, root=root, env={}, telegram_reader=reader)
            codec = SignedCallbackCodec(BOT_TOKEN)
            controller = OperatorBotController(FakeCore(), gate=OwnerGate(OWNER_ID, OWNER_ID),
                callback_codec=codec, clock=lambda: NOW, research=service)
            client = SimpleNamespace(send_message=AsyncMock())
            transport = TelethonOperatorTransport(client, controller, owner_chat_id=OWNER_ID, logger=logging.getLogger('test'))
            event = SimpleNamespace(**IDENTITY, answer=AsyncMock(), edit=AsyncMock(),
                data=button('TG', 1001, codec, now=NOW).callback_data)
            await transport._on_callback(event)
            await asyncio.gather(*transport._research_tasks)
            self.assertEqual(client.send_message.await_count, 2)
            self.assertIn('не найдено', client.send_message.await_args_list[-1].args[1])
            await transport.close()

    def test_page_reader_rejects_private_and_non_https_urls(self):
        for url in ['http://example.org', 'https://user:contact022@example.invalid', 'https://example.org:8443']:
            with self.assertRaises(PageUnavailable):
                _read(url)
        with patch('socket.getaddrinfo', return_value=[(2, 1, 6, '', ('127.0.0.1', 443))]):
            with self.assertRaises(PageUnavailable):
                _read('https://example.org')

    async def test_telegram_reader_uses_verified_sources_and_governor(self):
        reader = TelegramResearchReader()
        now = datetime.now(timezone.utc)
        class Client:
            async def iter_messages(self, entity, *, limit):
                self_entity = entity
                self_limit = limit
                yield SimpleNamespace(id=1, date=now, message='Нужен лендинг')
        class Governor:
            async def run(self, category, operation):
                return await operation()
        reader.bind(Client(), [(SimpleNamespace(message_url=lambda mid: f'https://t.me/example_channel_022/{mid}'), 'verified')], Governor())
        rows, count = await reader.read()
        self.assertEqual(count, 1)
        self.assertEqual(len(rows), 1)
        reader.unbind()
        with self.assertRaises(ResearchBlocked):
            await reader.read()

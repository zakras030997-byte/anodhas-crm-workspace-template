import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
import urllib.error
import urllib.request

from ai_sales_radar.research.threads import (
    MAX_BYTES, PER_QUERY, QUERIES, ThreadsPost, ThreadsResearchReader,
    ThreadsUnavailable, _fetch, _ThreadsRedirect, parse_search_html, candidate,
)
from ai_sales_radar.research.catalog import DIRECTIONS
from ai_sales_radar.research.service import ResearchService, ResearchBlocked
from ai_sales_radar.research.presentation import mode_menu, run_responses
from ai_sales_radar.operator_bot.controller import OperatorBotController
from ai_sales_radar.operator_bot.auth import OwnerGate
from ai_sales_radar.operator_bot.callbacks import SignedCallbackCodec
from ai_sales_radar.operator_bot.navigation import button
from ai_sales_radar.operator_bot.models import OperatorResponse
from test_ai_sales_radar_operator_bot import FakeCore, BOT_TOKEN, OWNER_ID, NOW

NOW_LIVE = datetime.now(timezone.utc)


def raw(post_id='123_456', text='Ищу разработчика чат-бота для сайта. Бюджет 20000 руб.'):
    return {'pk': post_id, 'code': 'TEST0001', 'caption': {'text': text},
            'user': {'username': 'synthetic_buyer'}, 'taken_at': NOW_LIVE.timestamp()}


def page(posts, *, recommendation=None):
    data = {'queryName': 'BarcelonaSearchResultsQuery', 'data': {'searchResults': {'edges': [
        {'node': {'thread': {'thread_items': [{'post': p}]}}} for p in posts]}}}
    if recommendation:
        data['recommended'] = {'thread_items': [{'post': recommendation}]}
    return '<script data-sjs type="application/json">' + json.dumps(data, ensure_ascii=False) + '</script>'


def post(**kw):
    p = ThreadsPost('123_456', 'https://www.threads.com/@synthetic_buyer/post/TEST0001',
                    raw()['caption']['text'], NOW_LIVE, 'synthetic_buyer')
    return replace(p, **kw)


class ParserTests(unittest.TestCase):
    def test_parse_original_preserve_id_text_date_and_url(self):
        p, = parse_search_html(page([raw()]))
        self.assertEqual(p.id, '123_456')
        self.assertEqual(p.text, raw()['caption']['text'])
        self.assertEqual(p.published_at, NOW_LIVE)
        self.assertEqual(p.url, post().url)

    def test_empty_is_valid_but_shell_and_login_are_not(self):
        self.assertEqual(parse_search_html(page([])), ())
        for html in ['<html>Log in to see</html>', '<script>{}</script>',
                     '<script type="application/json">{"queryName":"BarcelonaSearchResultsQuery","searchResults":null}</script>']:
            with self.subTest(html=html), self.assertRaises(ThreadsUnavailable):
                parse_search_html(html)

    def test_recommendations_are_not_search_hits_and_ids_deduped(self):
        result = parse_search_html(page([raw(), raw()], recommendation=raw('555')))
        self.assertEqual([p.id for p in result], ['123_456'])

    def test_cap_and_optional_metrics(self):
        self.assertEqual(len(parse_search_html(page([raw(str(i+1)) for i in range(80)]))), PER_QUERY)
        self.assertEqual(len(parse_search_html(page([raw()]))), 1)

    def test_malformed_and_unsafe_records_are_skipped(self):
        entries = [None, {}, {'pk': []}]
        for key, value in [('taken_at', None), ('taken_at', False), ('taken_at', float('inf')),
                           ('pk', '../bad'), ('code', '../../'), ('caption', None),
                           ('user', {'username': 'contact023@example.invalid'}), ('text_post_app_info', []),
                           ('caption', {'text': 'x' * 16001})]:
            p = raw(); p[key] = value; entries.append(p)
        # An empty optional object is accepted; malformed non-empty types are not.
        entries[-2]['text_post_app_info'] = ['bad']
        self.assertEqual(parse_search_html(page(entries)), ())

    def test_oversized_page_rejected(self):
        with self.assertRaises(ThreadsUnavailable):
            parse_search_html('x' * (MAX_BYTES + 1))

    def test_demand_is_selected_not_sellers_or_educational_posts(self):
        self.assertIsNotNone(candidate(DIRECTIONS[1], post()))
        self.assertIsNotNone(candidate(DIRECTIONS[1], post(text='Нужен бот для поддержки клиентов в Telegram')))
        for text in ['Ищу клиентов, делаю чат-ботов', 'Не нужен чат-бот', 'Как сделать чат-бота? Нужен совет',
                     'Нужен ли чат-бот бизнесу?', 'Предлагаю услуги разработки, нужен чат-бот? Пишите',
                     'Ищу работу разработчика чат-ботов', 'Нужен дизайнер логотипа', 'Нужна работа в офисе']:
            with self.subTest(text=text):
                self.assertIsNone(candidate(DIRECTIONS[1], post(text=text)))

    def test_freshness_and_reply_provenance(self):
        for change in [dict(published_at=NOW_LIVE-timedelta(days=8)),
                       dict(published_at=NOW_LIVE+timedelta(days=1)),
                       dict(is_reply=True), dict(is_quote=True)]:
            self.assertIsNone(candidate(DIRECTIONS[1], post(**change), now=NOW_LIVE))

    def test_keeps_original_and_unknowns_without_private_contact_enrichment(self):
        c = candidate(DIRECTIONS[1], post(text='Нужен чат-бот. contact021@example.invalid +70000000006'))
        self.assertEqual(c['email'], [])
        self.assertEqual(c['phone'], [])
        self.assertEqual(c['platform'], 'Threads')
        self.assertIsNone(c['telegram'])
        self.assertEqual(c['qualification'], 'кандидат на проверку')
        self.assertIn('contact021@example.invalid', c['text'])

    def test_redirects_reject_other_hosts_and_insecure_target(self):
        redirect = _ThreadsRedirect()
        req = urllib.request.Request('https://www.threads.com/search?q=test')
        for target in ['http://www.threads.com/', 'https://evil.com/', 'https://127.0.0.1/',
                       'https://contact024@example.invalid/', 'https://www.threads.com:8443/']:
            with self.subTest(target=target), self.assertRaises(ThreadsUnavailable):
                redirect.redirect_request(req, None, 302, '', {}, target)

    def test_http_denial_stops_without_retry_or_credentials(self):
        for status in [401, 403, 429, 503]:
            opener = SimpleNamespace(open=unittest.mock.Mock(side_effect=urllib.error.HTTPError(
                'https://www.threads.com', status, 'secret response', {}, None)))
            with patch('urllib.request.build_opener', return_value=opener):
                with self.assertRaises(ThreadsUnavailable) as error:
                    _fetch('test')
            self.assertNotIn('secret response', str(error.exception))
            self.assertEqual(opener.open.call_count, 1)
            req = opener.open.call_args.args[0]
            self.assertNotIn('Cookie', req.headers)
            self.assertNotIn('Authorization', req.headers)
            self.assertIn('ANODHAS', req.headers['User-agent'])


class WorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def test_reader_rejects_unknown_query_before_io(self):
        fetch = AsyncMock()
        with self.assertRaises(ThreadsUnavailable):
            await ThreadsResearchReader(fetch=fetch).search('unknown')
        fetch.assert_not_awaited()

    async def test_real_parser_to_existing_research_results_without_brave_or_db(self):
        fetch = AsyncMock(return_value=page([raw(), raw('999', 'Предлагаю услуги разработки чат-ботов')]))
        with tempfile.TemporaryDirectory() as root, patch('ai_sales_radar.research.service.asyncio.sleep', new=AsyncMock()):
            service = ResearchService(None, root=root, env={}, threads_reader=ThreadsResearchReader(fetch=fetch))
            r, _ = await service.start(1, 'threads', 'click')
            result = await service.execute(r['id'])
            self.assertEqual(result['status'], 'completed')
            self.assertEqual(fetch.await_count, 3)
            self.assertEqual(result['scanned'], 2)
            self.assertEqual(len(result['cards']), 1)
            self.assertEqual(service.get(r['id'])['cards'], result['cards'])
            self.assertIn('страна автора не подтверждена', run_responses(result)[0].text)
            await service.execute(r['id'])
            self.assertEqual(fetch.await_count, 3)

    async def test_text_dedup_across_posts(self):
        fetch = AsyncMock(return_value=page([raw(), raw('999')]))
        with tempfile.TemporaryDirectory() as root, patch('ai_sales_radar.research.service.asyncio.sleep', new=AsyncMock()):
            service = ResearchService(None, root=root, env={}, threads_reader=ThreadsResearchReader(fetch=fetch))
            r, _ = await service.start(1, 'threads', 'click')
            self.assertEqual(len((await service.execute(r['id']))['cards']), 1)

    async def test_source_failure_preserves_results_and_stops_remaining_queries(self):
        fetch = AsyncMock(side_effect=[page([raw()]), ThreadsUnavailable('Threads: HTTP 429')])
        with tempfile.TemporaryDirectory() as root, patch('ai_sales_radar.research.service.asyncio.sleep', new=AsyncMock()):
            service = ResearchService(None, root=root, env={}, threads_reader=ThreadsResearchReader(fetch=fetch))
            r, _ = await service.start(1, 'threads', 'click')
            result = await service.execute(r['id'])
            self.assertEqual(result['status'], 'partial')
            self.assertEqual(len(result['cards']), 1)
            self.assertEqual(fetch.await_count, 2)
            self.assertEqual(result['errors'], ['Threads: HTTP 429'])

    async def test_first_failure_is_not_successful_zero_results(self):
        with tempfile.TemporaryDirectory() as root:
            service = ResearchService(None, root=root, env={}, threads_reader=SimpleNamespace(
                search=AsyncMock(side_effect=ThreadsUnavailable('Unavailable'))))
            r, _ = await service.start(1, 'threads', 'click')
            self.assertEqual((await service.execute(r['id']))['status'], 'failed')

    async def test_cancel_stops_future_requests(self):
        entered = asyncio.Event()
        async def fetch(query):
            entered.set()
            await asyncio.Event().wait()
        reader = ThreadsResearchReader(fetch=fetch)
        with tempfile.TemporaryDirectory() as root:
            service = ResearchService(None, root=root, env={}, threads_reader=reader)
            r, _ = await service.start(1, 'threads', 'click')
            task = asyncio.create_task(service.execute(r['id']))
            await entered.wait()
            result = await service.cancel()
            self.assertEqual(result['status'], 'cancelled')
            self.assertTrue(task.cancelled())

    async def test_supported_menu_and_full_controller_route(self):
        codec = SignedCallbackCodec(BOT_TOKEN)
        for direction_id in DIRECTIONS:
            response = mode_menu(direction_id, codec, NOW)
            buttons = [b.label for row in response.buttons for b in row]
            self.assertEqual('🧵 Threads' in buttons, direction_id in QUERIES)
            OperatorResponse(response.text, parse_mode='html')
        with tempfile.TemporaryDirectory() as root, patch('ai_sales_radar.research.service.asyncio.sleep', new=AsyncMock()):
            service = ResearchService(None, root=root, env={}, threads_reader=ThreadsResearchReader(fetch=AsyncMock(return_value=page([raw()]))))
            controller = OperatorBotController(FakeCore(), gate=OwnerGate(OWNER_ID, OWNER_ID),
                callback_codec=codec, clock=lambda: NOW, research=service)
            outcome = await controller.handle_callback(is_private=True, sender_id=OWNER_ID, chat_id=OWNER_ID,
                data=button('Threads', 3001, codec, now=NOW).callback_data)
            self.assertIsNotNone(outcome.background_job)
            self.assertIn('Режим: Threads', outcome.responses[0].text)
            results = await controller.complete_research(outcome.background_job)
            self.assertEqual(len(results), 2)

    async def test_unsupported_direction_is_not_queued(self):
        with tempfile.TemporaryDirectory() as root:
            service = ResearchService(None, root=root, env={})
            with self.assertRaises(ResearchBlocked):
                await service.start(2, 'threads', 'click')
            self.assertIsNone(service.latest())

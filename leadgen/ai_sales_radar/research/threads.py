"""Bounded anonymous Threads research. No login, LLM, posting or background loop.

The search-payload traversal is adapted from Egor01KKK/threads-content-research-agent
(Apache-2.0), revision 981cccd2eb53d44f46cc5675b2388c8c7a6ad758.
See THIRD_PARTY_NOTICES.md. The transport and demand selection are ANODHAS code.
"""
from __future__ import annotations

import asyncio
import errno
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from html.parser import HTMLParser
import http.client
import json
import logging
import re
import socket
import ssl
import urllib.error
import urllib.parse
import urllib.request

from .cards import make_card

MAX_BYTES = 4_000_000
PER_QUERY = 20
MAX_AGE_DAYS = 7
QUERY_DELAY = 3
RETRY_DELAY = 2
logger = logging.getLogger(__name__)
QUERIES = {
    1: ('чат-бот', 'телеграм бот', 'AI агент'),
    2: ('лендинг', 'landing', 'одностраничный сайт'),
    3: ('CRM', 'Битрикс24', 'amoCRM'),
    4: ('n8n', 'автоматизация', 'интеграция CRM'),
    5: ('нейросети', 'GPT', 'AI услуги'),
    6: ('инфографика', 'карточки товаров', 'дизайн карточек'),
    7: ('эскиз', 'иллюстрация', 'скетч'),
    8: ('аудит сайта', 'аудит CRM', 'аудит автоматизации'),
    9: ('разработка программы', 'нужен скрипт', 'нужен парсер'),
    10: ('разработка приложения', 'мобильное приложение', 'разработчик Flutter'),
    11: ('Telegram Mini App', 'мини приложение Telegram', 'miniapp'),
}
TERMS = {
    1: ('телеграм бот', 'telegram бот', 'telegram-бот', 'бота', 'ai агент', 'ai-агент', 'ии агент', 'ии-агент'),
    3: ('амо срм', 'амо crm', 'amo crm'),
    4: ('автоматизировать', 'автоматизацию', 'запиер', 'zapier'),
    5: ('искусственн', 'ai агент', 'ии-агент', 'gpt', 'нейросет'),
    6: ('карточек товаров', 'карточек товара', 'карточки товаров', 'дизайн карточек'),
    11: ('мини приложен', 'мини-приложен', 'тма для telegram', 'тма для телеграм'),
}
BOT_SERVICE = re.compile(r'(?<!\w)(?:чат[ -]?бот\w*|(?:telegram|телеграм)[ -]бот\w*|бот(?:а|ов|ы|у|ом|ами)?\b|(?:ai|ии)[ -]агент\w*)', re.I)
SELLER = re.compile(
    r'(?:ищу|ищем)\s+(?:клиент\w*|заказ\w*|работ\w*)|'
    r'(?:предлагаю|оказываю)\s+услуг|'
    r'\b(?:делаю|разрабатываю|помогу|создаю)\b|'
    r'\bкак\s+(?:создать|сделать|настроить|разработать)\b|'
    r'\b(?:нужен|нужна|нужно)\s+ли\b', re.I)
SHORTCODE = re.compile(r'[A-Za-z0-9_-]{5,64}\Z')
HANDLE = re.compile(r'[A-Za-z0-9._]{1,64}\Z')


class ThreadsUnavailable(RuntimeError):
    """Sanitized error suitable for the owner's search report."""


class ThreadsTransientUnavailable(ThreadsUnavailable):
    """A temporary connection failure eligible for one bounded retry."""


def _transport_error(error, phase):
    reason = error.reason if isinstance(error, urllib.error.URLError) else error
    temporary = False
    if isinstance(reason, ssl.SSLCertVerificationError):
        code, message = 'tls_certificate', 'сертификат TLS не прошёл проверку'
    elif isinstance(reason, (TimeoutError, socket.timeout)):
        code, message, temporary = 'timeout', 'превышено время ожидания ответа', True
    elif isinstance(reason, socket.gaierror):
        code, message = 'dns', 'не удалось определить адрес Threads'
        temporary = reason.errno == socket.EAI_AGAIN
    elif isinstance(reason, (ssl.SSLEOFError, http.client.IncompleteRead, http.client.RemoteDisconnected)):
        code, message, temporary = 'connection_closed', 'соединение прервалось до получения полного ответа', True
    elif isinstance(reason, ssl.SSLError):
        code, message = 'tls', 'ошибка защищённого соединения TLS'
    elif isinstance(reason, UnicodeError):
        code, message = 'encoding', 'не удалось прочитать кодировку ответа'
    elif isinstance(reason, OSError) and reason.errno in {
            errno.ECONNRESET, errno.ECONNABORTED, errno.ETIMEDOUT, errno.EPIPE}:
        code, message, temporary = 'connection_closed', 'соединение прервалось до получения полного ответа', True
    else:
        code, message = 'connection', 'не удалось загрузить ответ Threads'
    # Never log response bodies, queries, exception messages, URLs or credentials.
    logger.warning('threads_transport_failure code=%s phase=%s temporary=%s', code, phase, temporary)
    cls = ThreadsTransientUnavailable if temporary else ThreadsUnavailable
    return cls(f'Threads: {message}.')


@dataclass(frozen=True)
class ThreadsPost:
    id: str
    url: str
    text: str
    published_at: datetime
    username: str
    is_reply: bool = False
    is_quote: bool = False


class _JSONScripts(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=False)
        self.blocks = []
        self.active = False
        self.parts = []

    def handle_starttag(self, tag, attrs):
        if tag == 'script':
            self.active = dict(attrs).get('type') == 'application/json'
            self.parts = []

    def handle_data(self, data):
        if self.active:
            self.parts.append(data)

    def handle_endtag(self, tag):
        if tag == 'script' and self.active:
            self.blocks.append(''.join(self.parts))
            self.active = False
            self.parts = []


def _search_objects(data):
    pending = [(data, 0)]
    visited = 0
    while pending:
        value, depth = pending.pop()
        visited += 1
        if visited > 100_000:
            raise ThreadsUnavailable('Threads: структура ответа слишком велика.')
        if depth > 30:
            continue
        if isinstance(value, dict):
            results = value.get('searchResults')
            if isinstance(results, dict):
                yield results
            pending.extend((v, depth + 1) for v in reversed(list(value.values())))
        elif isinstance(value, list):
            pending.extend((v, depth + 1) for v in reversed(value))


def parse_search_html(body: str) -> tuple[ThreadsPost, ...]:
    """Read only searchResults, never recommendations or profile biographies.

    A recognized empty result differs from a login wall or changed schema.
    """
    if len(body.encode('utf-8')) > MAX_BYTES:
        raise ThreadsUnavailable('Threads: превышен допустимый размер ответа.')
    parser = _JSONScripts()
    parser.feed(body)
    posts, seen = [], set()
    valid = False
    for block in parser.blocks:
        if 'BarcelonaSearchResultsQuery' not in block or '"searchResults"' not in block:
            continue
        try:
            data = json.loads(block)
        except (ValueError, RecursionError):
            continue
        for results in _search_objects(data):
            edges = results.get('edges')
            if not isinstance(edges, list):
                continue
            valid = True
            for edge in edges:
                try:
                    items = edge['node']['thread']['thread_items']
                except (KeyError, TypeError):
                    continue
                if not isinstance(items, list):
                    continue
                for item in items:
                    try:
                        raw = item['post']
                        post_id = str(raw['pk'])
                        code = raw['code']
                        username = raw['user']['username']
                        text = raw['caption']['text']
                        stamp = raw['taken_at']
                        if (not re.fullmatch(r'[0-9]{1,30}(?:_[0-9]{1,30})?', post_id) or
                                not isinstance(code, str) or not SHORTCODE.fullmatch(code) or
                                not isinstance(username, str) or not HANDLE.fullmatch(username) or
                                not isinstance(text, str) or not text.strip() or len(text) > 16_000 or
                                isinstance(stamp, bool) or not isinstance(stamp, (int, float))):
                            continue
                        published = datetime.fromtimestamp(stamp, timezone.utc)
                        info = raw.get('text_post_app_info') or {}
                        if not isinstance(info, dict):
                            continue
                        post = ThreadsPost(post_id,
                            f'https://www.threads.com/@{username}/post/{code}', text,
                            published, username, info.get('reply_to_author') is not None,
                            bool(info.get('is_quote_post')))
                    except (KeyError, TypeError, ValueError, OSError, OverflowError):
                        continue
                    if post.id not in seen:
                        seen.add(post.id)
                        posts.append(post)
                        if len(posts) >= PER_QUERY:
                            return tuple(posts)
    if not valid:
        raise ThreadsUnavailable('Threads: публичная выдача недоступна или её формат изменился. Поиск не подтверждён.')
    return tuple(posts)


class _ThreadsRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        url = urllib.parse.urlsplit(newurl)
        if (url.scheme != 'https' or url.hostname not in {'threads.com', 'www.threads.com'}
                or url.username or url.password or url.port not in {None, 443}):
            raise ThreadsUnavailable('Threads: неожиданное перенаправление остановлено.')
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _fetch(query: str) -> str:
    request = urllib.request.Request(
        'https://www.threads.com/search?' + urllib.parse.urlencode({'q': query}),
        headers={'User-Agent': 'ANODHAS-Research/1.0 (public research; https://example.invalid)',
                 'Accept': 'text/html', 'Accept-Language': 'ru-RU,ru;q=0.9'})
    phase = 'headers'
    try:
        with urllib.request.build_opener(_ThreadsRedirect()).open(request, timeout=15) as response:
            if response.status != 200:
                raise ThreadsUnavailable(f'Threads: HTTP {response.status}.')
            phase = 'body'
            body = response.read(MAX_BYTES + 1)
            if len(body) > MAX_BYTES:
                raise ThreadsUnavailable('Threads: превышен допустимый размер ответа.')
            # Bounded read(n) can silently return a truncated Content-Length body.
            remaining = getattr(response, 'length', None)
            if isinstance(remaining, int) and remaining > 0:
                raise http.client.IncompleteRead(b'', remaining)
            phase = 'decode'
            return body.decode('utf-8')
    except urllib.error.HTTPError as exc:
        status = exc.code
        exc.close()
        raise ThreadsUnavailable(f'Threads: HTTP {status}; повторные запросы остановлены.') from None
    except (urllib.error.URLError, TimeoutError, UnicodeError, OSError, http.client.HTTPException) as error:
        raise _transport_error(error, phase) from None


class ThreadsResearchReader:
    def __init__(self, *, fetch=None):
        self._fetch = fetch

    async def search(self, query):
        if query not in {q for queries in QUERIES.values() for q in queries}:
            raise ThreadsUnavailable('Threads: запрос не входит в выбранный план.')
        for attempt in range(2):
            try:
                body = await self._fetch(query) if self._fetch else await asyncio.to_thread(_fetch, query)
                return parse_search_html(body)
            except ThreadsTransientUnavailable as error:
                if attempt:
                    raise ThreadsUnavailable(f'{error} Однократный повтор не помог; поиск остановлен.') from None
                logger.info('threads_transport_retry attempt=2')
                await asyncio.sleep(RETRY_DELAY)


def candidate(direction, post, *, now=None, price=None):
    now = now or datetime.now(timezone.utc)
    if not now - timedelta(days=MAX_AGE_DAYS) <= post.published_at <= now + timedelta(minutes=5):
        return None
    if post.is_reply or post.is_quote or SELLER.search(post.text):
        return None
    if direction.id == 1 and not BOT_SERVICE.search(post.text):
        return None
    bot_terms = (BOT_SERVICE.search(post.text).group().casefold(),) if direction.id == 1 else ()
    expanded = replace(direction, terms=direction.terms + TERMS.get(direction.id, ()) + bot_terms)
    card = make_card(expanded, text=post.text, url=post.url,
                     observed_at=post.published_at.isoformat(), platform='Threads', price=price)
    if card:
        card['source_post_id'] = post.id
        card['author_username'] = post.username
        card['source_kind'] = 'threads_public_search'
        card['content_sha256'] = sha256(re.sub(r'\s+', ' ', post.text.casefold()).strip().encode()).hexdigest()
        card['request_status'] = 'текст публичного поста из выдачи Threads; актуальность заказа требует проверки'
        card['phone'], card['email'], card['decision_maker'] = [], [], None
        card['decision_maker_status'] = 'не подтверждён'
        card['website'] = None
    return card

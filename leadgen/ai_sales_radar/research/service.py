"""Bounded owner-requested research runs, isolated from the ingestion pipeline."""
from __future__ import annotations
import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
from urllib.parse import urlsplit

from dotenv import dotenv_values
import sqlalchemy as sa
from freelancer_bot.persistence.schema import raw_messages
from freelancer_bot.web_provider_chain import BraveSearchBackend
from ai_sales_radar.config import DEFAULT_ENV_FILE, LOCAL_RUNTIME_ROOT
from .catalog import DIRECTIONS, SOURCES
from .cards import make_card
from .pages import read_public_page
from .threads import ThreadsResearchReader, ThreadsUnavailable, QUERIES, QUERY_DELAY, candidate


class ResearchBlocked(RuntimeError):
    pass


class ResearchService:
    def __init__(self, database, *, root=None, backend=None, env=None, page_reader=read_public_page, telegram_reader=None, threads_reader=None):
        self.database = database
        self.page_reader = page_reader
        self.telegram_reader = telegram_reader
        self.threads_reader = threads_reader or ThreadsResearchReader()
        self.root = Path(root or LOCAL_RUNTIME_ROOT / 'research')
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        values = dict(dotenv_values(DEFAULT_ENV_FILE)) if env is None else dict(env)
        if env is None:
            values.update(os.environ)
        key = (values.get('BRAVE_SEARCH_API_KEY') or '').strip()
        self.backend = backend or (BraveSearchBackend(key) if key else None)
        self.geography = values.get('LEAD_RESEARCH_GEOGRAPHY') or 'Россия'
        self.local_geography = values.get('LEAD_RESEARCH_LOCAL_GEOGRAPHY') or 'Санкт-Петербург'
        self.prices = {}
        prices = Path(__file__).resolve().parents[2] / 'config/research-prices.json'
        if prices.exists():
            self.prices = json.loads(prices.read_text())
        self._lock = asyncio.Lock()
        self._tasks = {}
        self._cancelled = set()
        # A prior process may have stopped mid-run. Never silently repeat requests.
        for path in self.root.glob('*.json'):
            run = json.loads(path.read_text())
            if run.get('status') in {'queued', 'running'}:
                run['status'] = 'interrupted'
                self._save(run)

    def _save(self, run):
        path = self.root / (run['id'] + '.json')
        temporary = path.with_suffix('.tmp')
        temporary.write_text(json.dumps(run, ensure_ascii=False, indent=2))
        temporary.chmod(0o600)
        temporary.replace(path)

    def get(self, run_id):
        if len(run_id) != 32 or any(c not in '0123456789abcdef' for c in run_id):
            raise ResearchBlocked('Неверный номер поиска.')
        return json.loads((self.root / (run_id + '.json')).read_text())

    def latest(self):
        paths = sorted(self.root.glob('*.json'), key=lambda p: p.stat().st_mtime, reverse=True)
        return json.loads(paths[0].read_text()) if paths else None

    async def start(self, direction_id, mode, request_id):
        if direction_id not in DIRECTIONS or mode not in {'tg', 'global', 'threads'}:
            raise ResearchBlocked('Неизвестное направление или режим.')
        if mode == 'threads' and direction_id not in QUERIES:
            raise ResearchBlocked('Для этого направления ещё не настроен поиск Threads.')
        async with self._lock:
            run_id = hashlib.sha256(request_id.encode()).hexdigest()[:32]
            if (self.root / (run_id + '.json')).exists():
                return self.get(run_id), False
            current = self.latest()
            if current and current['status'] in {'queued', 'running'}:
                return current, False
            if mode == 'tg' and self.telegram_reader is not None and getattr(self.telegram_reader, 'client', True) is None:
                raise ResearchBlocked('Сборщик ещё подключает каналы. Повторите поиск через несколько минут.')
            if mode == 'global' and self.backend is None:
                raise ResearchBlocked('Глобальный поиск ещё не подключён: нужен BRAVE_SEARCH_API_KEY в настройках сервера. Ключ не присылайте боту. Telegram-поиск доступен. Яндекс/2ГИС/ФНС не проверялись.')
            direction = DIRECTIONS[direction_id]
            run = {'id': run_id, 'direction_id': direction_id, 'direction': direction.label,
                   'mode': mode, 'geography': self.geography, 'local_geography': self.local_geography, 'status': 'queued',
                   'created_at': datetime.now(timezone.utc).isoformat(),
                   'sources': list(direction.sources) if mode == 'global' else ['threads' if mode == 'threads' else 'telegram'],
                   'coverage': [], 'cards': [], 'scanned': 0, 'errors': []}
            self._save(run)
            return run, True

    async def cancel(self):
        run = self.latest()
        if run is None or run['status'] not in {'queued', 'running'}:
            return run
        self._cancelled.add(run['id'])
        task = self._tasks.get(run['id'])
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            return self.get(run['id'])
        run['status'] = 'cancelled'
        self._save(run)
        return run

    async def execute(self, run_id):
        run = self.get(run_id)
        if run['status'] != 'queued':
            return run
        self._tasks[run_id] = asyncio.current_task()
        run['status'] = 'running'
        self._save(run)
        try:
            async with asyncio.timeout(900 if run["mode"] == "tg" else 240):
                if run['mode'] == 'tg':
                    await self._telegram(run)
                elif run['mode'] == 'threads':
                    await self._threads(run)
                else:
                    await self._global(run)
            run['cards'] = sorted(run['cards'], key=lambda c: (-c['relevance'], c['url']))[:10]
            run['status'] = 'partial' if run['errors'] else 'completed'
        except asyncio.CancelledError:
            run['status'] = 'cancelled' if run_id in self._cancelled else 'interrupted'
            raise
        except Exception as exc:
            run['status'] = 'partial' if run['scanned'] else 'failed'
            run['errors'].append(str(exc) if isinstance(exc, (ResearchBlocked, ThreadsUnavailable)) else type(exc).__name__)
        finally:
            self._tasks.pop(run_id, None)
            self._cancelled.discard(run_id)
            run['cards'] = sorted(run['cards'], key=lambda c: (-c['relevance'], c['url']))[:10]
            run['finished_at'] = datetime.now(timezone.utc).isoformat()
            self._save(run)
        return run

    async def _threads(self, run):
        direction = DIRECTIONS[run['direction_id']]
        seen_ids, seen_text = set(), set()
        run['coverage'].append('Threads: ограниченная публичная выборка, до 3 запросов по 20 постов; отбор за 7 дней. Покрытие площадки и география автора не гарантированы.')
        run['queries'] = list(QUERIES[direction.id])
        for index, query in enumerate(run['queries']):
            if index:
                await asyncio.sleep(QUERY_DELAY)
            posts = await self.threads_reader.search(query)
            run['coverage'].append(f'Threads · {query}: получено {len(posts)} постов.')
            for post in posts:
                if post.id in seen_ids:
                    continue
                seen_ids.add(post.id)
                run['scanned'] += 1
                card = candidate(direction, post, price=self.prices.get(str(direction.id)))
                if card and card['content_sha256'] not in seen_text:
                    seen_text.add(card['content_sha256'])
                    run['cards'].append(card)
            self._save(run)

    async def _telegram(self, run):
        direction = DIRECTIONS[run['direction_id']]
        seen = set()
        async def consume(rows, source_count):
            for row in rows:
                run['scanned'] += 1
                card = make_card(direction, text=row['content'], url=row['message_url'],
                    observed_at=row['message_date'].isoformat(), platform='Telegram',
                    price=self.prices.get(str(direction.id)))
                if card and card['url'] not in seen and len(run['cards']) < 100:
                    seen.add(card['url'])
                    run['cards'].append(card)
            run['coverage'] = [f'Telegram: проверено каналов {source_count}; до 30 последних сообщений на канал, отбор за 7 дней.']
            self._save(run)
        if self.telegram_reader is not None:
            rows, source_count = await self.telegram_reader.read(on_source=consume)
            if not run['coverage']:
                await consume(rows, source_count)
        else:
            since = datetime.now(timezone.utc) - timedelta(days=7)
            rows = await self._stored_telegram(since)
            await consume(rows, 0)
            run['coverage'] = [f'Telegram: {len(rows)} уже собранных сообщений за 7 дней; максимум 500. История каналов отдельно не перечитывалась.']

    async def _stored_telegram(self, since):
        async with self.database.connect() as connection:
            result = await connection.execute(sa.select(
                raw_messages.c.content, raw_messages.c.message_url, raw_messages.c.message_date,
            ).where(raw_messages.c.platform == 'telegram', raw_messages.c.message_date >= since)
              .order_by(raw_messages.c.message_date.desc(), raw_messages.c.id).limit(500))
            return result.mappings().all()

    async def _global(self, run):
        direction = DIRECTIONS[run['direction_id']]
        seen = set()
        for source_id in run['sources']:
            label, domain, role = SOURCES[source_id]
            geography = run.get('local_geography', self.local_geography) if source_id in {'yandex', '2gis'} else run['geography']
            query = f'{direction.label} ищу исполнителя {geography}'
            if domain:
                query += ' site:' + domain
            else:
                query += ' запрос предложений контакты'
            try:
                results = await self.backend.search(query, language='ru', limit=5)
            except Exception as exc:
                run['errors'].append(label + ': ' + type(exc).__name__)
                run['coverage'].append(label + ': поиск недоступен')
                self._save(run)
                continue
            run['coverage'].append(f'{label}: {len(results)} веб-результатов. {role}. Прямой API площадки не использован.')
            for item in results:
                run['scanned'] += 1
                parsed = urlsplit(item.url)
                if domain:
                    expected = urlsplit('https://' + domain)
                    if parsed.hostname != expected.hostname and not (parsed.hostname or '').endswith('.' + expected.hostname):
                        continue
                    if expected.path and not parsed.path.startswith(expected.path):
                        continue
                if item.url in seen:
                    continue
                seen.add(item.url)
                snippet = item.title + '\n' + item.snippet
                preliminary = make_card(direction, text=snippet, url=item.url,
                    observed_at=None, platform=label, evidence='snippet')
                if preliminary is None or len(run['cards']) >= 10:
                    continue
                text, published, evidence = snippet, None, 'snippet'
                try:
                    text, published = await self.page_reader(item.url)
                    evidence = 'original'
                except Exception:
                    run['coverage'].append(label + ': первоисточник кандидата недоступен; сохранён только фрагмент')
                card = make_card(direction, text=text,
                    url=item.url, observed_at=published, platform=label if domain else 'Веб-страница; принадлежность компании не подтверждена', evidence=evidence,
                    price=self.prices.get(str(direction.id)))
                if card and len(run['cards']) < 100:
                    run['cards'].append(card)
            self._save(run)
            await asyncio.sleep(1)

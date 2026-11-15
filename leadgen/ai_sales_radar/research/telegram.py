"""Read only the collector's already verified sources through its governor."""
from datetime import datetime, timedelta, timezone
from freelancer_bot.telegram_request_governor import TelegramRequestCategory
from .service import ResearchBlocked


class TelegramResearchReader:
    def __init__(self):
        self.client = None
        self.sources = ()
        self.governor = None

    def bind(self, client, sources, governor):
        self.client, self.sources, self.governor = client, tuple(sources), governor

    def unbind(self):
        self.client, self.sources, self.governor = None, (), None

    async def read(self, *, on_source=None):
        if self.client is None:
            raise ResearchBlocked('Сборщик ещё подключает каналы. Повторите поиск через несколько минут.')
        client, sources, governor = self.client, self.sources[:20], self.governor
        since = datetime.now(timezone.utc) - timedelta(days=7)
        rows = []
        for index, (source, entity) in enumerate(sources, 1):
            batch = []
            if self.client is not client:
                raise ResearchBlocked('Сборщик остановлен. Поиск прерван.')
            async def history():
                return [m async for m in client.iter_messages(entity, limit=30)]
            messages = await governor.run(TelegramRequestCategory.HISTORY, history)
            for message in messages:
                date = getattr(message, 'date', None)
                text = getattr(message, 'message', None)
                if not date or date.tzinfo is None or date < since or not text:
                    continue
                batch.append({'content': text, 'message_url': source.message_url(message.id), 'message_date': date})
            rows.extend(batch)
            if on_source is not None:
                await on_source(batch, index)
        return rows, len(sources)

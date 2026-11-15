"""Evidence-bound cards. No guessed identities or inferred Telegram availability."""
from datetime import datetime, timezone
import html
import re
from urllib.parse import urlsplit

DEMAND = re.compile(r'\b(ищу|ищем|нужен|нужна|нужно|нужны|требуется|требуются|закажу|тендер|ищется)\b', re.I)
SELLER = re.compile(r'(ищу|ищем)\s+(заказчик|клиент|заказов)|предлагаю\s+(услуг|разработ)|оказываю\s+услуг', re.I)
PHONE = re.compile(r'(?<!\d)(?:\+7|8)[\s(.-]*\d{3}[\s).-]*\d{3}[\s.-]*\d{2}[\s.-]*\d{2}(?!\d)')
EMAIL = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b')
BUDGET = re.compile(r'\b\d[\d\s.,]{0,16}\s*(?:₽|руб(?:лей|ля|ль)?\.?|USD|EUR|долларов)(?=\s|$|[,.;])', re.I)
WEBSITE = re.compile(r'https://[^\s<>\"\']+')
NAME = re.compile(r'(?:директор|руководитель|основатель|владелец)\s*[:—-]?\s*([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+)?)')


def safe_url(value):
    try:
        p = urlsplit(value)
        return len(value) <= 512 and len(html.escape(value)) <= 700 and p.scheme == 'https' and bool(p.hostname) and not p.username and not p.password
    except ValueError:
        return False


def make_card(direction, *, text, url, observed_at, platform, evidence='original', price=None):
    text = text[:16000]
    normalized = text.casefold().replace('ё', 'е')
    matches = [term for term in direction.terms if term in normalized]
    if not matches or not DEMAND.search(text) or SELLER.search(text) or not safe_url(url):
        return None
    demand_positions = [m.start() for m in DEMAND.finditer(text) if not re.search(r'не\s*$', text[max(0, m.start()-5):m.start()], re.I)]
    term_positions = [normalized.find(term) for term in matches]
    if not any(abs(a-b) <= 250 for a in demand_positions for b in term_positions):
        return None
    # Contact strings are used only from original source text, never search snippets.
    original = evidence == 'original'
    phones = list(dict.fromkeys(PHONE.findall(text)))[:2] if original else []
    emails = [e for e in dict.fromkeys(EMAIL.findall(text)) if len(e) <= 254][:2] if original else []
    name = NAME.search(text) if original else None
    budget = BUDGET.search(text) if original else None
    websites = [u.rstrip('.,;)') for u in WEBSITE.findall(text) if safe_url(u.rstrip('.,;)')) and urlsplit(u).hostname not in {'t.me', 'telegram.me'}] if original else []
    return {
        'direction': direction.label, 'url': url, 'platform': platform,
        'checked_at': datetime.now(timezone.utc).isoformat(),
        'published_at': observed_at, 'text': text[:1800], 'offer': direction.offer,
        'decision_maker': name.group(1)[:120] if name else None,
        'decision_maker_status': 'роль указана в источнике; полномочия по этой покупке не подтверждены' if name else 'не найден',
        'phone': phones, 'email': emails, 'email_status': 'доставляемость не проверена',
        'website': websites[0] if websites else None,
        'telegram': None, 'telegram_status': 'активность и доступность сообщений не подтверждены; контакт не включён',
        'request_status': 'текст из первоисточника' if original else 'поисковый фрагмент; первоисточник не проверен',
        'budget': budget.group(0) if budget else None,
        'price': price or 'не задана; нужна оценка объёма работ',
        'relevance': min(65, 35 + 10 * len(matches) + (10 if original else 0)),
        'reason': 'Совпадения: ' + ', '.join(matches) + '; есть формулировка спроса. Требуется проверка контекста и актуальности.',
        'qualification': 'кандидат на проверку',
    }


def render_card(card, number):
    def esc(v):
        value = str(v)
        while len(html.escape(value)) > 1300:
            value = value[:max(1, len(value) * 3 // 4)]
        return html.escape(value)
    fields = [
        ('Статус', card['qualification']), ('Направление', card['direction']),
        ('ФИО / ЛПР', (card['decision_maker'] or 'не найден') + ' · ' + card['decision_maker_status']),
        ('Телефон', ', '.join(card['phone']) or 'не найден'),
        ('Почта', (', '.join(card['email']) or 'не найдена') + ' · ' + card['email_status']),
        ('Сайт из запроса', card['website'] or 'не подтверждён'),
        ('Telegram', card['telegram'] or card['telegram_status']),
        ('Запрос' if len(card['text']) <= 1100 else 'Запрос (фрагмент, полный текст по ссылке)', card['text'][:1100]), ('Подтверждение запроса', card['request_status']),
        ('Предложение', card['offer']), ('Бюджет заказчика', card['budget'] or 'не указан'),
        ('Наша стоимость', card['price']), ('Релевантность', f"{card['relevance']}/100 — предварительный отбор"),
        ('Почему', card['reason']), ('Дата публикации', card['published_at'] or 'не подтверждена'),
        ('Проверено', card['checked_at']),
    ]
    body = f'<b>Кандидат №{number}</b>\n\n' + '\n\n'.join(f'<b>{esc(k)}:</b> {esc(v)}' for k, v in fields)
    return body + '\n\n<a href="' + html.escape(card['url'], quote=True) + '">Источник</a>'

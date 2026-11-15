from datetime import datetime
import html
from ai_sales_radar.operator_bot.models import OperatorResponse
from ai_sales_radar.operator_bot.navigation import button
from .catalog import DIRECTIONS, SOURCES
from .cards import render_card
from .threads import QUERIES


def direction_menu(codec, now):
    entries = list(DIRECTIONS.values())
    rows = [tuple(button(d.label, 100+d.id, codec, now=now) for d in entries[i:i+2])
            for i in range(0, len(entries), 2)]
    rows += [(button('📋 Результаты поиска', 60, codec, now=now),),
             (button('⏹ Остановить поиск', 61, codec, now=now),),
             (button('⚙️ Каталог каналов', 50, codec, now=now),)]
    return OperatorResponse('<b>📡 Источники · Шаг 1 из 2</b>\n\nКакие лиды нужны? Выберите направление. Затем укажите, где искать.',
                            parse_mode='html', buttons=tuple(rows))


def mode_menu(direction_id, codec, now):
    d = DIRECTIONS[direction_id]
    threads_text = ('<b>Threads</b>: публичные посты за 7 дней, до 3 запросов по 20 записей. '
                    'Отбор потребности в услуге; реклама исполнителей исключается.\n' if d.id in QUERIES else '')
    plan = '\n'.join('• ' + SOURCES[s][0] + ' — ' + SOURCES[s][2] for s in d.sources)
    text = (f'<b>{html.escape(d.label)} · Шаг 2 из 2</b>\n\nГде ищем?\n\n'
            '<b>Telegram</b> — до 30 последних сообщений из каждого подключённого канала; отбор за 7 дней.\n'
            '<b>Глобально</b> — веб-поиск по выбранным площадкам, до 5 результатов на площадку. '
            'Требуется подключённый веб-поиск.\n\n'
            f'{threads_text}<b>План глобального поиска · {len(d.sources)} площадок</b>\n{html.escape(plan)}\n\n'
            'Нажатие запускает поиск. Telegram-проверка может занять 5–15 минут из-за пауз между каналами. Результат придёт сюда. '
            'Контакты и ЛПР могут отсутствовать; неподтверждённые сведения будут отмечены.')
    return OperatorResponse(text, parse_mode='html', buttons=(
        (button('✈️ Только Telegram', 1000+d.id, codec, now=now),),
        (button('🌍 Глобальный поиск', 2000+d.id, codec, now=now),),
        *(((button('🧵 Threads', 3000+d.id, codec, now=now),),) if d.id in QUERIES else ()),
        (button('← Направления', 3, codec, now=now),),
    ))


def run_responses(run):
    if run is None:
        return (OperatorResponse('Поиск по направлениям ещё не запускался. Откройте «Источники» и выберите направление.'),)
    labels = {'queued':'в очереди', 'running':'выполняется', 'completed':'завершён',
              'partial':'завершён частично', 'cancelled':'остановлен по вашей команде', 'failed':'ошибка', 'interrupted':'прерван'}
    coverage = '\n'.join('• ' + s for s in run['coverage'])
    geography = ('Threads: язык запроса русский; страна автора не подтверждена' if run.get('mode') == 'threads'
                 else f"География: {run['geography']}; локальные компании — {run.get('local_geography', 'Санкт-Петербург')}")
    header = (f"Поиск: {run['direction']}\nСтатус: {labels[run['status']]}\n"
              f"{geography}\n"
              f"Просмотрено записей: {run['scanned']}\nКандидатов: {len(run['cards'])}\n\n{coverage}")
    if run['status'] in {'completed', 'partial'} and not run['cards']:
        header += '\n\nВ проверенном объёме подходящих запросов не найдено. Это не означает, что их нет на всей площадке.'
    if run['errors']:
        header += '\n\nНе удалось проверить: ' + '; '.join(run['errors'])
    return (OperatorResponse(header[:4000]),) + tuple(
        OperatorResponse(render_card(card, i), parse_mode='html') for i, card in enumerate(run['cards'][:10], 1))

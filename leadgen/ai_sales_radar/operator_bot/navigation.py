"""Owner navigation: signed, bounded routes to existing application actions."""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from uuid import UUID

from .callbacks import CallbackAction, SignedCallbackCodec, DecodedCallback
from .models import OperatorButton, OperatorResponse

# IDs are stable so already displayed keyboards keep their meaning after restart.
ROUTES = {
    1: '/menu', 2: '/leads_menu', 3: '/sources_menu', 4: '/system_menu', 5: '/help',
    10: '/leads', 11: '/leads hot', 12: '/leads warm', 13: '/leads low', 14: '/leads all',
    20: '/sources approved', 21: '/sources active', 22: '/sources candidate',
    23: '/sources paused', 24: '/sources all', 25: '/sources needs_review',
    26: '/sources degraded', 27: '/sources review_required',
    28: '/sources rejected', 29: '/sources retired',
    30: '/discover_sources', 31: '/discover_results',
    40: '/status', 41: '/stats', 50: '/source_catalog', 60: '/research_results', 61: '/research_cancel',
}

ROUTES.update({100+i: f'/research_direction {i}' for i in range(1, 12)})
ROUTES.update({1000+i: f'/research_run {i}:tg' for i in range(1, 12)})
ROUTES.update({2000+i: f'/research_run {i}:global' for i in range(1, 12)})
ROUTES.update({3000+i: f'/research_run {i}:threads' for i in (1, 4, 5)})

def navigation_target(callback: DecodedCallback) -> str | None:
    if callback.expected_revision != 1:
        return None
    return ROUTES.get(callback.lead_id.int)


def button(label: str, route: int, codec: SignedCallbackCodec, *, now: datetime) -> OperatorButton:
    return OperatorButton(label, callback_data=codec.encode(
        CallbackAction.NAVIGATE, UUID(int=route), 1, now=now,
    ))


def menu_response(section: str, codec: SignedCallbackCodec, *, now: datetime) -> OperatorResponse:
    if section == 'sources':
        from ai_sales_radar.research.presentation import direction_menu
        return with_navigation(direction_menu(codec, now), codec, now=now)
    menus = {
        'home': (
            '<b>AI Sales Radar · Главное меню</b>\n\n'
            '🎯 Лиды — отбор объявлений и работа с карточками.\n'
            '📡 Источники — каналы и поиск новых сообществ.\n'
            '📊 Состояние — проверка базы, очереди и расходы AI.\n\n'
            'Выберите раздел. Доступ открыт только владельцу.',
            [[('🎯 Лиды', 2), ('📡 Источники', 3)], [('📊 Состояние', 4), ('❓ Помощь', 5)]],
        ),
        'leads': (
            '<b>🎯 Лиды</b>\n\nВыберите подборку по оценке соответствия. '
            'В карточке можно открыть оригинал, взять лид в работу, пропустить '
            'или подготовить AI-черновик ответа.\n\n'
            'Показывается ограниченная подборка лучших лидов. '
            '«Все статусы» включает ранее закрытые карточки.',
            [[('📥 Актуальные', 10), ('🔥 Горячие', 11)],
             [('🟡 Тёплые', 12), ('⚪ Низкая оценка', 13)], [('🗂 Все статусы', 14)]],
        ),
        'source_catalog': (
            '<b>📡 Источники</b>\n\nПросмотрите каналы по статусу или запустите '
            'поиск новых сообществ по профилю. Поиск выполняется сборщиком: '
            'результат появится после обработки очереди.\n\n'
            'Найденные сообщества сохраняются кандидатами. '
            'Просмотр списка не меняет их статус и не подключает их к сбору. '
            'Список показывает до 20 источников.',
            [[('✅ Одобренные', 20), ('🟢 Активные', 21)],
             [('🔎 Найти источники', 30), ('📋 Результаты', 31)],
             [('🆕 Кандидаты', 22), ('⏸ На паузе', 23)],
             [('🔍 Нужна проверка', 25), ('🔁 Перепроверить', 27)],
             [('📉 Снижение качества', 26), ('🚫 Отклонённые', 28)],
             [('🗃 Архив', 29), ('📚 Все источники', 24)]],
        ),
        'system': (
            '<b>📊 Состояние и расходы</b>\n\n'
            '«Проверить состояние» читает доступность базы, профиль и очередь задач.\n'
            '«Статистика и AI» показывает объём обработки и учтённые расходы '
            'за сегодня и месяц. Эти экраны обновляют данные при каждом открытии.',
            [[('🩺 Проверить состояние', 40)], [('📈 Статистика и AI', 41)]],
        ),
        'help': (
            '<b>❓ Как работать</b>\n\n'
            '1. Откройте «Лиды» и выберите подборку.\n'
            '2. Изучите карточку и оригинал объявления.\n'
            '3. Нажмите «В работу» или «Не подходит».\n'
            '4. «Черновик» подготовит текст после подтверждения AI-запроса. '
            'Отправку клиенту выполняете вы.\n\n'
            'В «Источниках» доступны каталог, поиск и его результаты. '
            'Пустая подборка означает, что подходящих записей пока нет.\n\n'
            '/menu или /start — восстановить главное меню.\n'
            'Кнопки действуют ограниченное время; при истечении откройте меню заново.\n\n'
            'Бот работает, пока запущен его процесс на компьютере или сервере. '
            'Редактирование API-ключей, профиля и запуск выключенного процесса '
            'через это меню не поддерживаются.',
            [],
        ),
    }
    text, rows = menus[section]
    keyboard = tuple(tuple(button(label, route, codec, now=now) for label, route in row) for row in rows)
    response = OperatorResponse(text, parse_mode='html', buttons=keyboard)
    return response if section == 'home' else with_navigation(response, codec, now=now)


def with_navigation(response: OperatorResponse, codec: SignedCallbackCodec, *, now: datetime) -> OperatorResponse:
    for row in response.buttons:
        for item in row:
            if item.label == '🏠 Главное меню' or item.label == '🎯 Лиды':
                return response
    return replace(response, buttons=response.buttons + (
        (button('🎯 Лиды', 2, codec, now=now), button('📡 Источники', 3, codec, now=now)),
        (button('🏠 Главное меню', 1, codec, now=now),),
    ))

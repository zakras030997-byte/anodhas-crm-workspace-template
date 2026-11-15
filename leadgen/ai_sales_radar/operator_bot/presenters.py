"""Bounded Telegram presentation with no raw-message logging or sending."""

from __future__ import annotations

from datetime import datetime
import html

from .callbacks import CallbackAction, SYSTEM_CALLBACK_ID, SignedCallbackCodec
from .models import (
    OperatorButton,
    OperatorDraft,
    OperatorLead,
    OperatorResponse,
    OperatorSource,
    OperatorSourceDiscoveryReport,
    OperatorSourceDiscoveryStart,
    OperatorStatusSnapshot,
    TELEGRAM_TEXT_LIMIT,
)


_STATUS_LABELS = {
    "new": "новый",
    "opened": "открыт",
    "shortlisted": "в работе",
    "ignored": "пропущен",
    "replied": "ответ подготовлен",
}
_SOURCE_STATUS_LABELS = {
    "candidate": "кандидат",
    "needs_review": "нужна проверка",
    "approved": "одобрен",
    "active": "активен",
    "degraded": "качество снизилось",
    "paused": "пауза",
    "review_required": "перепроверить",
    "rejected": "отклонён",
    "retired": "архив",
}


def help_response(
    codec: SignedCallbackCodec | None = None,
    *,
    now: datetime | None = None,
) -> OperatorResponse:
    buttons: tuple[tuple[OperatorButton, ...], ...] = ()
    if codec is not None and now is not None:
        buttons = _source_discovery_buttons(codec, now=now, include_start=True)
    return OperatorResponse(
        text=(
            "<b>AI Sales Radar</b>\n\n"
            "/status — состояние ядра и очередей\n"
            "/leads — свежие подходящие лиды\n"
            "/leads hot — только горячие\n"
            "/sources — одобренные источники\n"
            "/discover_sources — найти новые крупные сообщества\n"
            "/discover_results — результаты последнего поиска\n"
            "/stats — статистика обработки\n\n"
            "Бот показывает данные только здесь. Клиентам он ничего "
            "не отправляет."
        ),
        parse_mode="html",
        buttons=buttons,
    )


def source_discovery_started_response(
    result: OperatorSourceDiscoveryStart,
    codec: SignedCallbackCodec,
    *,
    now: datetime,
) -> OperatorResponse:
    return OperatorResponse(
        text=(
            "<b>Поиск источников поставлен в очередь</b>\n\n"
            "Collector проверит публичные каналы и чаты, связанные с вашим "
            "профилем, и оставит только сообщества от "
            f"{_format_count(result.minimum_participants)} участников.\n\n"
            "Ничего не будет автоматически добавлено, одобрено или открыто. "
            "Повторный запрос в период ограничения использует ту же задачу."
        ),
        parse_mode="html",
        buttons=_source_discovery_buttons(codec, now=now, include_start=False),
    )


def source_discovery_report_response(
    report: OperatorSourceDiscoveryReport,
    codec: SignedCallbackCodec,
    *,
    now: datetime,
) -> OperatorResponse:
    status_labels = {
        "queued": "в очереди",
        "running": "выполняется",
        "completed": "завершён",
        "failed": "завершён с ошибкой",
    }
    status = status_labels.get(report.status, report.status)
    text = (
        "<b>Последний поиск источников</b>\n\n"
        f"Статус: {html.escape(status)}\n"
        f"Минимум участников: {_format_count(report.minimum_participants)}"
    )
    if report.failure_code:
        text += "\nКод ошибки: " + html.escape(report.failure_code)
    if not report.candidates:
        if report.status == "completed":
            text += "\n\nНовых подходящих крупных сообществ не найдено."
        else:
            text += "\n\nРезультаты ещё не готовы."
    else:
        text += "\n\n<b>Кандидаты:</b>"
        for index, source in enumerate(report.candidates, start=1):
            identity = source.handle or source.display_name
            label = html.escape(identity)
            if source.canonical_url:
                label = (
                    f'<a href="{html.escape(source.canonical_url, quote=True)}">'
                    f"{label}</a>"
                )
            line = (
                f"\n{index}. {label} · {_peer_type_label(source.peer_type)}"
                f"\n   {_format_count(source.participants_count)} участников · "
                f"релевантность {html.escape(source.relevance_score)} · "
                f"совпадений {source.query_hit_count}"
            )
            if len(text) + len(line) > TELEGRAM_TEXT_LIMIT - 80:
                text += "\n…список сокращён"
                break
            text += line
        text += "\n\nВсе результаты сохранены только как кандидаты."
    return OperatorResponse(
        text=text,
        parse_mode="html",
        buttons=_source_discovery_buttons(codec, now=now, include_start=True),
    )


def source_discovery_missing_response(
    codec: SignedCallbackCodec,
    *,
    now: datetime,
) -> OperatorResponse:
    return OperatorResponse(
        text="Поиск источников ещё не запускался.",
        buttons=_source_discovery_buttons(codec, now=now, include_start=True),
    )


def status_response(snapshot: OperatorStatusSnapshot) -> OperatorResponse:
    state = "✅ готово" if snapshot.ready else "⚠️ нужна проверка"
    database = "доступна" if snapshot.database_reachable else "недоступна"
    migrations = "актуальна" if snapshot.migration_current else "не актуальна"
    text = (
        f"<b>AI Sales Radar: {state}</b>\n\n"
        f"PostgreSQL: {database}\n"
        f"Схема БД: {migrations}\n"
        f"Активных основных профилей: {snapshot.active_primary_profiles}\n"
        f"Очередь: {snapshot.queued_jobs} · выполняется: "
        f"{snapshot.running_jobs}\n"
        f"Исторических ошибок jobs: {snapshot.failed_jobs}\n\n"
        "Статус показывает состояние БД и очередей, но не доказывает, "
        "что collector-процесс сейчас запущен."
    )
    return OperatorResponse(text=text, parse_mode="html")


def stats_response(snapshot: OperatorStatusSnapshot) -> OperatorResponse:
    text = (
        "<b>Статистика ядра</b>\n\n"
        f"Сообщений: {snapshot.raw_messages}\n"
        f"Возможностей: {snapshot.opportunities}\n"
        f"Лидов в Inbox: {snapshot.inbox_items}\n\n"
        f"AI сегодня: {snapshot.today_ai_calls} вызовов · "
        f"${html.escape(snapshot.today_ai_spend_usd)}\n"
        f"AI за месяц: {snapshot.month_ai_calls} вызовов · "
        f"${html.escape(snapshot.month_ai_spend_usd)}"
    )
    return OperatorResponse(text=text, parse_mode="html")


def sources_response(
    sources: tuple[OperatorSource, ...],
    *,
    requested_status: str | None,
) -> OperatorResponse:
    label = "все" if requested_status is None else _SOURCE_STATUS_LABELS.get(
        requested_status,
        requested_status,
    )
    text = f"<b>Telegram-источники: {html.escape(label)}</b>"
    if not sources:
        text += "\nИсточники с таким статусом не найдены."
    for source in sources:
        identity = source.handle or source.display_name
        status = _SOURCE_STATUS_LABELS.get(source.status, source.status)
        line = (
            f"\n{source.source_id}. {html.escape(identity)} · "
            f"{html.escape(status)}"
        )
        suffix = "\n…список сокращён"
        if len(text) + len(line) + len(suffix) > TELEGRAM_TEXT_LIMIT:
            text += suffix
            break
        text += line
    return OperatorResponse(
        text=text,
        parse_mode="html",
    )


def leads_empty_response() -> OperatorResponse:
    return OperatorResponse(
        text=(
            "Подходящих лидов в Inbox пока нет. Это не ошибка: сообщения "
            "могли не пройти профиль поиска."
        )
    )


def lead_response(
    lead: OperatorLead,
    codec: SignedCallbackCodec,
    *,
    now: datetime,
) -> OperatorResponse:
    rows: list[tuple[OperatorButton, ...]] = []
    if lead.source_url is not None:
        rows.append((OperatorButton("Оригинал", url=lead.source_url),))
    rows.append(
        (
            OperatorButton(
                "В работу",
                callback_data=codec.encode(
                    CallbackAction.SHORTLIST,
                    lead.item_id,
                    lead.revision,
                    now=now,
                ),
            ),
            OperatorButton(
                "Пропустить",
                callback_data=codec.encode(
                    CallbackAction.SKIP,
                    lead.item_id,
                    lead.revision,
                    now=now,
                ),
            ),
        )
    )
    rows.append(
        (
            OperatorButton(
                "Черновик",
                callback_data=codec.encode(
                    CallbackAction.DRAFT_PROMPT,
                    lead.item_id,
                    lead.revision,
                    now=now,
                ),
            ),
            OperatorButton(
                "Не подходит",
                callback_data=codec.encode(
                    CallbackAction.NOT_FIT,
                    lead.item_id,
                    lead.revision,
                    now=now,
                ),
            ),
        )
    )
    return OperatorResponse(
        text=lead.body_html,
        parse_mode="html",
        buttons=tuple(rows),
    )


def status_changed_responses(
    lead: OperatorLead,
    codec: SignedCallbackCodec,
    *,
    now: datetime,
) -> tuple[OperatorResponse, ...]:
    label = _STATUS_LABELS.get(lead.workflow_status, lead.workflow_status)
    return (
        OperatorResponse(text=f"Статус обновлён: {label}."),
        lead_response(lead, codec, now=now),
    )


def draft_confirmation_response(
    lead_id,
    revision: int,
    codec: SignedCallbackCodec,
    *,
    now: datetime,
) -> OperatorResponse:
    return OperatorResponse(
        text=(
            "Создать AI-черновик? Текст исходного объявления и профиль "
            "исполнителя будут отправлены выбранному AI-провайдеру. "
            "Клиенту сообщение отправлено не будет."
        ),
        buttons=(
            (
                OperatorButton(
                    "Подтвердить AI-черновик",
                    callback_data=codec.encode(
                        CallbackAction.DRAFT_CONFIRM,
                        lead_id,
                        revision,
                        now=now,
                    ),
                ),
            ),
        ),
    )


def draft_responses(draft: OperatorDraft) -> tuple[OperatorResponse, ...]:
    sections = [
        "AI-черновик ответа\n\n" + draft.proposal_draft,
        "Короткий вариант\n\n" + draft.short_reply,
    ]
    if draft.questions_to_client:
        sections.append(
            "Вопросы клиенту\n\n"
            + "\n".join(f"• {item}" for item in draft.questions_to_client)
        )
    if draft.risks:
        sections.append(
            "Риски для проверки\n\n"
            + "\n".join(f"• {item}" for item in draft.risks)
        )
    chunks: list[str] = []
    for section in sections:
        chunks.extend(_split_plain(section))
    chunks.append("Это только черновик. Автоматическая отправка отключена.")
    return tuple(OperatorResponse(text=chunk) for chunk in chunks)


def _split_plain(value: str) -> tuple[str, ...]:
    selected = value.strip()
    if not selected:
        return ()
    limit = TELEGRAM_TEXT_LIMIT
    return tuple(
        selected[offset : offset + limit]
        for offset in range(0, len(selected), limit)
    )


def _source_discovery_buttons(
    codec: SignedCallbackCodec,
    *,
    now: datetime,
    include_start: bool,
) -> tuple[tuple[OperatorButton, ...], ...]:
    values: list[OperatorButton] = []
    if include_start:
        values.append(
            OperatorButton(
                "🔎 Найти источники",
                callback_data=codec.encode(
                    CallbackAction.SOURCE_DISCOVERY_START,
                    SYSTEM_CALLBACK_ID,
                    1,
                    now=now,
                ),
            )
        )
    values.append(
        OperatorButton(
            "📋 Результаты",
            callback_data=codec.encode(
                CallbackAction.SOURCE_DISCOVERY_RESULTS,
                SYSTEM_CALLBACK_ID,
                1,
                now=now,
            ),
        )
    )
    return (tuple(values),)


def _peer_type_label(value: str) -> str:
    return {
        "broadcast": "канал",
        "channel": "канал",
        "group": "чат",
        "supergroup": "чат",
    }.get(value, "сообщество")


def _format_count(value: int) -> str:
    return f"{value:,}".replace(",", " ")

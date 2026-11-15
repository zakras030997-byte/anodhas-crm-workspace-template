"""Private owner-only command and callback controller."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
import re
from uuid import UUID

from ai_sales_radar.persistence.inbox import InboxStatus

from .auth import OwnerGate
from .callbacks import (
    CallbackAction,
    CallbackDecodeError,
    SYSTEM_CALLBACK_ID,
    SignedCallbackCodec,
    callback_request_id,
)
from .models import (
    OperatorBotActionConflict,
    OperatorBotError,
    OperatorCallbackOutcome,
    OperatorResponse,
)
from .presenters import (
    draft_confirmation_response,
    draft_responses,
    lead_response,
    leads_empty_response,
    sources_response,
    source_discovery_missing_response,
    source_discovery_report_response,
    source_discovery_started_response,
    stats_response,
    status_changed_responses,
    status_response,
)
from .navigation import menu_response, navigation_target, with_navigation
from .rate_limit import WindowRateLimiter
from .service import OperatorCore


_COMMAND = re.compile(
    r"^/(?P<name>[a-z_]+)(?:@[A-Za-z0-9_]{5,32})?(?:\s+(?P<arg>\S+))?\s*$",
    re.IGNORECASE,
)
_SOURCE_STATUSES = frozenset(
    {
        "candidate",
        "needs_review",
        "approved",
        "active",
        "degraded",
        "paused",
        "review_required",
        "rejected",
        "retired",
    }
)


class OperatorBotController:
    def __init__(
        self,
        core: OperatorCore,
        *,
        gate: OwnerGate,
        callback_codec: SignedCallbackCodec,
        max_leads: int = 5,
        reads_per_minute: int = 30,
        mutations_per_minute: int = 10,
        drafts_per_hour: int = 5,
        limiter: WindowRateLimiter | None = None,
        clock: Callable[[], datetime] | None = None,
        research=None,
    ) -> None:
        if not 1 <= max_leads <= 10:
            raise ValueError("max leads must be between 1 and 10")
        self._core = core
        self._research = research
        self.gate = gate
        self._codec = callback_codec
        self._max_leads = max_leads
        self._reads_per_minute = reads_per_minute
        self._mutations_per_minute = mutations_per_minute
        self._drafts_per_hour = drafts_per_hour
        self._limiter = limiter or WindowRateLimiter()
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    async def handle_message(self, **identity) -> tuple[OperatorResponse, ...]:
        responses = await self._handle_message(**identity)
        return tuple(with_navigation(r, self._codec, now=self._aware_now()) for r in responses)

    async def handle_callback(self, **identity) -> OperatorCallbackOutcome | None:
        outcome = await self._handle_callback(**identity)
        if outcome is None:
            return None
        return OperatorCallbackOutcome(tuple(
            with_navigation(r, self._codec, now=self._aware_now())
            for r in outcome.responses
        ), replace_message=outcome.replace_message, background_job=outcome.background_job)

    async def _handle_message(
        self,
        *,
        is_private: bool,
        sender_id: int | None,
        chat_id: int | None,
        text: str,
    ) -> tuple[OperatorResponse, ...]:
        if not self.gate.allows(
            is_private=is_private,
            sender_id=sender_id,
            chat_id=chat_id,
        ):
            return ()
        match = _COMMAND.fullmatch(text.strip())
        if match is None:
            return (menu_response("home", self._codec, now=self._aware_now()),)
        name = match.group("name").casefold()
        argument = match.group("arg")
        if name in {"start", "menu"} and argument is None:
            return (menu_response("home", self._codec, now=self._aware_now()),)
        if name in {"help", "leads_menu", "sources_menu", "system_menu"} and argument is None:
            section = {"help": "help", "leads_menu": "leads", "sources_menu": "sources", "system_menu": "system"}[name]
            return (menu_response(section, self._codec, now=self._aware_now()),)
        if name == "source_catalog" and argument is None:
            return (menu_response("source_catalog", self._codec, now=self._aware_now()),)
        if name == "research_direction" and argument is not None:
            from ai_sales_radar.research.catalog import DIRECTIONS
            from ai_sales_radar.research.presentation import mode_menu
            if argument.isdigit() and int(argument) in DIRECTIONS:
                return (mode_menu(int(argument), self._codec, self._aware_now()),)
        if name == "research_cancel" and argument is None:
            from ai_sales_radar.research.presentation import run_responses
            if not await self._limiter.allow("mutation", limit=self._mutations_per_minute, window_seconds=60):
                return (_rate_limited_response(),)
            return run_responses(await self._research.cancel() if self._research else None)
        if name == "research_results" and argument is None:
            from ai_sales_radar.research.presentation import run_responses
            if not await self._allow_read():
                return (_rate_limited_response(),)
            return run_responses(self._research.latest() if self._research else None)
        if name == "research_run":
            return (OperatorResponse("Для запуска откройте «Источники» и выберите направление и режим кнопками."),)
        if name == "status" and argument is None:
            if not await self._allow_read():
                return (_rate_limited_response(),)
            return (status_response(await self._core.status()),)
        if name == "stats" and argument is None:
            if not await self._allow_read():
                return (_rate_limited_response(),)
            return (stats_response(await self._core.status()),)
        if name == "sources":
            selected_status = "approved" if argument is None else argument.casefold()
            if selected_status == "all":
                selected_status = None
            elif selected_status not in _SOURCE_STATUSES:
                return (OperatorResponse("Неизвестный статус источника."),)
            if not await self._allow_read():
                return (_rate_limited_response(),)
            sources = await self._core.list_sources(
                status=selected_status,
                limit=20,
            )
            return (
                sources_response(sources, requested_status=selected_status),
            )
        if name == "discover_sources" and argument is None:
            if not await self._limiter.allow(
                "mutation",
                limit=self._mutations_per_minute,
                window_seconds=60,
            ):
                return (_rate_limited_response(),)
            try:
                started = await self._core.start_source_discovery()
            except OperatorBotError as exc:
                return (OperatorResponse(str(exc)),)
            return (
                source_discovery_started_response(
                    started,
                    self._codec,
                    now=self._aware_now(),
                ),
            )
        if name == "discover_results" and argument is None:
            if not await self._allow_read():
                return (_rate_limited_response(),)
            report = await self._core.latest_source_discovery()
            if report is None:
                return (
                    source_discovery_missing_response(
                        self._codec,
                        now=self._aware_now(),
                    ),
                )
            return (
                source_discovery_report_response(
                    report,
                    self._codec,
                    now=self._aware_now(),
                ),
            )
        if name == "leads":
            selected = "active" if argument is None else argument.casefold()
            if selected not in {"active", "hot", "warm", "low", "all"}:
                return (OperatorResponse("Фильтр: hot, warm, low или all."),)
            if not await self._allow_read():
                return (_rate_limited_response(),)
            leads = await self._core.list_leads(
                tier=None if selected in {"active", "all"} else selected,
                include_closed=selected == "all",
                limit=self._max_leads,
            )
            if not leads:
                return (leads_empty_response(),)
            now = self._aware_now()
            return tuple(lead_response(lead, self._codec, now=now) for lead in leads)
        if name == "lead" and argument is not None:
            try:
                lead_id = UUID(argument)
            except ValueError:
                return (OperatorResponse("Некорректный ID лида."),)
            if not await self._allow_read():
                return (_rate_limited_response(),)
            try:
                lead = await self._core.show_lead(lead_id)
            except OperatorBotError as exc:
                return (OperatorResponse(str(exc)),)
            return (lead_response(lead, self._codec, now=self._aware_now()),)
        return (menu_response("home", self._codec, now=self._aware_now()),)

    async def _handle_callback(
        self,
        *,
        is_private: bool,
        sender_id: int | None,
        chat_id: int | None,
        data: bytes,
    ) -> OperatorCallbackOutcome | None:
        if not self.gate.allows(
            is_private=is_private,
            sender_id=sender_id,
            chat_id=chat_id,
        ):
            return None
        try:
            callback = self._codec.decode(data, now=self._aware_now())
        except CallbackDecodeError:
            return OperatorCallbackOutcome(
                (OperatorResponse("Кнопка устарела. Откройте главное меню и выберите действие заново."),)
            )
        if callback.action is CallbackAction.NAVIGATE:
            target = navigation_target(callback)
            if target is None:
                return OperatorCallbackOutcome((menu_response("home", self._codec, now=self._aware_now()),))
            if target.startswith("/research_run "):
                return await self._start_research(target, data)
            responses = await self.handle_message(
                is_private=is_private, sender_id=sender_id, chat_id=chat_id, text=target,
            )
            return OperatorCallbackOutcome(responses, replace_message=len(responses) == 1)
        if callback.action in {
            CallbackAction.SOURCE_DISCOVERY_START,
            CallbackAction.SOURCE_DISCOVERY_RESULTS,
        }:
            if callback.lead_id != SYSTEM_CALLBACK_ID or callback.expected_revision != 1:
                return OperatorCallbackOutcome(
                    (OperatorResponse("Кнопка устарела. Откройте /start."),)
                )
            if callback.action is CallbackAction.SOURCE_DISCOVERY_START:
                if not await self._limiter.allow(
                    "mutation",
                    limit=self._mutations_per_minute,
                    window_seconds=60,
                ):
                    return OperatorCallbackOutcome((_rate_limited_response(),))
                try:
                    started = await self._core.start_source_discovery()
                except OperatorBotError as exc:
                    return OperatorCallbackOutcome((OperatorResponse(str(exc)),))
                return OperatorCallbackOutcome(
                    (
                        source_discovery_started_response(
                            started,
                            self._codec,
                            now=self._aware_now(),
                        ),
                    )
                )
            if not await self._allow_read():
                return OperatorCallbackOutcome((_rate_limited_response(),))
            report = await self._core.latest_source_discovery()
            response = (
                source_discovery_missing_response(
                    self._codec,
                    now=self._aware_now(),
                )
                if report is None
                else source_discovery_report_response(
                    report,
                    self._codec,
                    now=self._aware_now(),
                )
            )
            return OperatorCallbackOutcome((response,))
        if callback.action is CallbackAction.DRAFT_PROMPT:
            return OperatorCallbackOutcome(
                (
                    draft_confirmation_response(
                        callback.lead_id,
                        callback.expected_revision,
                        self._codec,
                        now=self._aware_now(),
                    ),
                )
            )
        if callback.action is CallbackAction.DRAFT_CONFIRM:
            if not await self._limiter.allow(
                "draft",
                limit=self._drafts_per_hour,
                window_seconds=3_600,
            ):
                return OperatorCallbackOutcome((_rate_limited_response(),))
            try:
                draft = await self._core.create_reply_draft(
                    callback.lead_id,
                    expected_revision=callback.expected_revision,
                )
            except OperatorBotError as exc:
                return OperatorCallbackOutcome((OperatorResponse(str(exc)),))
            return OperatorCallbackOutcome(draft_responses(draft))

        if not await self._limiter.allow(
            "mutation",
            limit=self._mutations_per_minute,
            window_seconds=60,
        ):
            return OperatorCallbackOutcome((_rate_limited_response(),))
        action = {
            CallbackAction.SHORTLIST: (
                InboxStatus.SHORTLISTED,
                "operator_shortlisted",
            ),
            CallbackAction.SKIP: (InboxStatus.IGNORED, "operator_skipped"),
            CallbackAction.NOT_FIT: (InboxStatus.IGNORED, "operator_not_fit"),
        }.get(callback.action)
        if action is None:
            return OperatorCallbackOutcome(
                (OperatorResponse("Кнопка устарела. Откройте главное меню и выберите действие заново."),)
            )
        target, reason = action
        try:
            lead = await self._core.update_lead_status(
                callback.lead_id,
                target,
                expected_revision=callback.expected_revision,
                actor_id="telegram-operator",
                reason=reason,
                request_id=callback_request_id(data),
            )
        except (OperatorBotActionConflict, OperatorBotError) as exc:
            return OperatorCallbackOutcome((OperatorResponse(str(exc)),))
        return OperatorCallbackOutcome(
            status_changed_responses(
                lead,
                self._codec,
                now=self._aware_now(),
            )
        )

    async def _start_research(self, target, data):
        from ai_sales_radar.research.service import ResearchBlocked
        from ai_sales_radar.research.presentation import run_responses
        if self._research is None:
            return OperatorCallbackOutcome((OperatorResponse("Поисковый сервис не подключён."),))
        if not await self._limiter.allow("mutation", limit=self._mutations_per_minute, window_seconds=60):
            return OperatorCallbackOutcome((_rate_limited_response(),))
        direction, mode = target.split()[1].split(":")
        try:
            run, created = await self._research.start(int(direction), mode, callback_request_id(data))
        except ResearchBlocked as exc:
            return OperatorCallbackOutcome((OperatorResponse(str(exc)),))
        if not created:
            return OperatorCallbackOutcome(run_responses(run))
        mode_label = {'tg': 'Telegram', 'threads': 'Threads', 'global': 'глобальный'}[mode]
        text = (f"Поиск запущен: {run['direction']}.\n"
                f"Режим: {mode_label}.\n"
                "Результат придёт в этот чат. Его также можно открыть в «Источники → Результаты поиска».")
        return OperatorCallbackOutcome((OperatorResponse(text),), background_job=run['id'])

    async def complete_research(self, run_id):
        from ai_sales_radar.research.presentation import run_responses
        run = await self._research.execute(run_id)
        return tuple(with_navigation(r, self._codec, now=self._aware_now()) for r in run_responses(run))

    async def _allow_read(self) -> bool:
        return await self._limiter.allow(
            "read",
            limit=self._reads_per_minute,
            window_seconds=60,
        )

    def _aware_now(self) -> datetime:
        selected = self._clock()
        if selected.tzinfo is None or selected.utcoffset() is None:
            raise ValueError("operator bot clock must include a timezone")
        return selected.astimezone(timezone.utc)


def _rate_limited_response() -> OperatorResponse:
    return OperatorResponse("Слишком много действий. Попробуйте чуть позже.")

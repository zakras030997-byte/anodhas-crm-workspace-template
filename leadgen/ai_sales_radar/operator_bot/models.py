"""Channel-neutral records exchanged by the operator bot layers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Mapping
from uuid import UUID


TELEGRAM_TEXT_LIMIT = 4096
TELEGRAM_CALLBACK_LIMIT = 64


class OperatorBotError(RuntimeError):
    """A bounded error whose message is safe to show only to the owner."""


class OperatorBotActionConflict(OperatorBotError):
    """The lead or callback changed and must be refreshed."""


@dataclass(frozen=True)
class OperatorButton:
    label: str
    callback_data: bytes | None = field(default=None, repr=False)
    url: str | None = None

    def __post_init__(self) -> None:
        if not self.label.strip() or len(self.label) > 64:
            raise ValueError("button label is invalid")
        if (self.callback_data is None) == (self.url is None):
            raise ValueError("button requires exactly one action")
        if (
            self.callback_data is not None
            and len(self.callback_data) > TELEGRAM_CALLBACK_LIMIT
        ):
            raise ValueError("Telegram callback exceeds 64 bytes")


@dataclass(frozen=True)
class OperatorResponse:
    text: str
    parse_mode: str | None = None
    buttons: tuple[tuple[OperatorButton, ...], ...] = ()

    def __post_init__(self) -> None:
        if not self.text.strip():
            raise ValueError("operator response must not be blank")
        if len(self.text) > TELEGRAM_TEXT_LIMIT:
            raise ValueError("operator response exceeds Telegram text limit")
        if self.parse_mode not in {None, "html"}:
            raise ValueError("unsupported operator response parse mode")


@dataclass(frozen=True)
class OperatorCallbackOutcome:
    responses: tuple[OperatorResponse, ...]
    replace_message: bool = False
    background_job: str | None = None


@dataclass(frozen=True)
class OperatorStatusSnapshot:
    database_reachable: bool
    migration_current: bool
    aggregates_readable: bool
    active_primary_profiles: int
    queued_jobs: int
    running_jobs: int
    failed_jobs: int
    raw_messages: int
    opportunities: int
    inbox_items: int
    today_ai_calls: int
    today_ai_spend_usd: str
    month_ai_calls: int
    month_ai_spend_usd: str
    jobs_by_type: Mapping[str, Mapping[str, int]]

    @property
    def ready(self) -> bool:
        return (
            self.database_reachable
            and self.migration_current
            and self.aggregates_readable
            and self.active_primary_profiles == 1
        )


@dataclass(frozen=True)
class OperatorSource:
    source_id: int
    display_name: str
    handle: str | None
    status: str


@dataclass(frozen=True)
class OperatorLead:
    item_id: UUID
    revision: int
    workflow_status: str
    commercial_score: int
    commercial_band: str
    body_html: str
    source_url: str | None


@dataclass(frozen=True)
class OperatorDraft:
    lead_id: UUID
    proposal_draft: str
    short_reply: str
    questions_to_client: tuple[str, ...]
    risks: tuple[str, ...]


@dataclass(frozen=True)
class OperatorSourceDiscoveryStart:
    job_id: UUID
    minimum_participants: int
    cooldown_seconds: int


@dataclass(frozen=True)
class OperatorDiscoveredSource:
    source_id: int
    display_name: str
    handle: str | None
    canonical_url: str | None
    status: str
    peer_type: str
    participants_count: int
    relevance_score: str
    relevance_class: str
    query_hit_count: int
    message_hit_count: int


@dataclass(frozen=True)
class OperatorSourceDiscoveryReport:
    status: str
    requested_at: datetime
    finished_at: datetime | None
    minimum_participants: int
    candidates: tuple[OperatorDiscoveredSource, ...]
    failure_code: str | None = None

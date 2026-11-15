"""Fail-closed Telegram folder membership gate for the approved collector set.

The gate is intentionally read-only.  It verifies the immutable channel IDs
already trusted by the collector against one exact private Telegram folder and
never joins channels, edits filters, reads history, or exposes Telegram peer
identifiers in operator-facing errors.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Protocol

from telethon import functions, types

from freelancer_bot.telegram_collector import TelegramCollectorSource
from freelancer_bot.telegram_request_governor import TelegramRequestCategory


CANARY_SOURCE_IDS = (
    1,
    3,
    4,
    5,
    6,
    7,
    9,
    10,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19,
)
EXACT_COLLECTOR_FOLDER_TITLE = "Ист_Лид_АИ"


class TelegramCollectorFolderGateError(RuntimeError):
    """A fail-closed folder error whose text contains no Telegram secrets."""


class TelegramCollectorFolderClient(Protocol):
    def __call__(self, request: Any) -> Awaitable[Any]: ...


class RequestGovernor(Protocol):
    async def run(
        self,
        category: str,
        operation: Callable[[], Awaitable[Any]],
    ) -> Any: ...


class CollectorFolderGate(Protocol):
    def validate_catalog(
        self,
        eligible_sources: Sequence[TelegramCollectorSource],
    ) -> None: ...

    def validate_scope(
        self,
        selected_sources: Sequence[TelegramCollectorSource],
    ) -> None: ...

    async def verify(
        self,
        client: TelegramCollectorFolderClient,
        *,
        selected_sources: Sequence[TelegramCollectorSource],
        resolved_sources: Sequence[tuple[TelegramCollectorSource, Any]],
        governor: RequestGovernor,
    ) -> None: ...


class ExactCanaryTelegramFolderGate:
    """Require the exact approved source set in folder ``Ист_Лид_АИ``."""

    def validate_catalog(
        self,
        eligible_sources: Sequence[TelegramCollectorSource],
    ) -> None:
        _require_exact_canary_ids(
            eligible_sources,
            error_prefix="Eligible Telegram-каталог должен содержать ровно",
        )

    def validate_scope(
        self,
        selected_sources: Sequence[TelegramCollectorSource],
    ) -> None:
        _require_exact_canary_ids(
            selected_sources,
            error_prefix="Collector canary должен содержать ровно",
        )

    async def verify(
        self,
        client: TelegramCollectorFolderClient,
        *,
        selected_sources: Sequence[TelegramCollectorSource],
        resolved_sources: Sequence[tuple[TelegramCollectorSource, Any]],
        governor: RequestGovernor,
    ) -> None:
        self.validate_scope(selected_sources)

        expected_peer_by_source = _resolved_peer_ids(resolved_sources)
        if set(expected_peer_by_source) != set(CANARY_SOURCE_IDS):
            missing = sorted(set(CANARY_SOURCE_IDS) - set(expected_peer_by_source))
            suffix = (
                ": " + ", ".join(str(value) for value in missing)
                if missing
                else ""
            )
            raise TelegramCollectorFolderGateError(
                "Не все canary-источники прошли immutable "
                "identity-проверку"
                + suffix
            )
        if len(set(expected_peer_by_source.values())) != len(CANARY_SOURCE_IDS):
            raise TelegramCollectorFolderGateError(
                "Immutable identity-привязки canary-источников "
                "конфликтуют"
            )

        async def read_filters() -> Any:
            return await client(functions.messages.GetDialogFiltersRequest())

        response = await governor.run(
            TelegramRequestCategory.ENTITY_ACCESS,
            read_filters,
        )
        folder = _exact_private_folder(response)
        included_ids = _channel_ids(
            (*folder.pinned_peers, *folder.include_peers),
        )
        excluded_ids = _channel_ids(folder.exclude_peers)

        excluded_sources = tuple(
            source_id
            for source_id in sorted(expected_peer_by_source)
            if expected_peer_by_source[source_id] in excluded_ids
        )
        missing_sources = tuple(
            source_id
            for source_id in sorted(expected_peer_by_source)
            if expected_peer_by_source[source_id] not in included_ids
        )
        if excluded_sources:
            raise TelegramCollectorFolderGateError(
                f"Папка {EXACT_COLLECTOR_FOLDER_TITLE} исключает "
                "source ID "
                + ", ".join(str(value) for value in excluded_sources)
            )
        if missing_sources:
            raise TelegramCollectorFolderGateError(
                f"В папке {EXACT_COLLECTOR_FOLDER_TITLE} отсутствуют "
                "source ID "
                + ", ".join(str(value) for value in missing_sources)
            )


def _source_ids(
    sources: Sequence[TelegramCollectorSource],
) -> tuple[int, ...]:
    values: list[int] = []
    for source in sources:
        value = getattr(getattr(source, "record", None), "id", None)
        if not _is_positive_int(value):
            raise TelegramCollectorFolderGateError(
                "Canary-каталог содержит некорректный "
                "source ID"
            )
        values.append(value)
    return tuple(values)


def _require_exact_canary_ids(
    sources: Sequence[TelegramCollectorSource],
    *,
    error_prefix: str,
) -> None:
    selected_ids = _source_ids(sources)
    if (
        len(selected_ids) != len(CANARY_SOURCE_IDS)
        or len(set(selected_ids)) != len(selected_ids)
        or tuple(sorted(selected_ids)) != CANARY_SOURCE_IDS
    ):
        raise TelegramCollectorFolderGateError(
            f"{error_prefix} source ID "
            + ", ".join(str(value) for value in CANARY_SOURCE_IDS)
        )


def _resolved_peer_ids(
    resolved_sources: Sequence[tuple[TelegramCollectorSource, Any]],
) -> dict[int, int]:
    values: dict[int, int] = {}
    for item in resolved_sources:
        if not isinstance(item, tuple) or len(item) != 2:
            raise TelegramCollectorFolderGateError(
                "Некорректный результат immutable "
                "identity-проверки"
            )
        source, entity = item
        source_ids = _source_ids((source,))
        source_id = source_ids[0]
        channel_id = getattr(entity, "channel_id", None)
        access_hash = getattr(entity, "access_hash", None)
        if (
            source_id in values
            or not isinstance(entity, types.InputChannel)
            or not _is_positive_int(channel_id)
            or isinstance(access_hash, bool)
            or not isinstance(access_hash, int)
            or access_hash == 0
        ):
            raise TelegramCollectorFolderGateError(
                "Некорректный результат immutable "
                "identity-проверки "
                f"для source ID {source_id}"
            )
        values[source_id] = channel_id
    return values


def _exact_private_folder(response: Any) -> types.DialogFilter:
    filters = getattr(response, "filters", None)
    if not isinstance(filters, Sequence) or isinstance(
        filters,
        (str, bytes, bytearray),
    ):
        raise TelegramCollectorFolderGateError(
            "Telegram вернул некорректный список папок"
        )

    matches = [
        item
        for item in filters
        if isinstance(getattr(item, "title", None), types.TextWithEntities)
        and item.title.text == EXACT_COLLECTOR_FOLDER_TITLE
    ]
    if not matches:
        raise TelegramCollectorFolderGateError(
            "Не найдена точная папка Telegram: "
            f"{EXACT_COLLECTOR_FOLDER_TITLE}"
        )
    if len(matches) != 1:
        raise TelegramCollectorFolderGateError(
            "Найдено несколько папок Telegram: "
            f"{EXACT_COLLECTOR_FOLDER_TITLE}"
        )
    selected = matches[0]
    if type(selected) is not types.DialogFilter:
        raise TelegramCollectorFolderGateError(
            "Точная папка Telegram должна быть "
            "приватной обычной папкой"
        )
    for field in ("pinned_peers", "include_peers", "exclude_peers"):
        value = getattr(selected, field, None)
        if not isinstance(value, Sequence) or isinstance(
            value,
            (str, bytes, bytearray),
        ):
            raise TelegramCollectorFolderGateError(
                "Telegram вернул некорректный состав папки"
            )
    return selected


def _channel_ids(peers: Sequence[Any]) -> frozenset[int]:
    values: set[int] = set()
    for peer in peers:
        if isinstance(peer, types.InputPeerChannel):
            channel_id = getattr(peer, "channel_id", None)
            access_hash = getattr(peer, "access_hash", None)
            if not _is_positive_int(channel_id) or not _is_nonzero_int(
                access_hash
            ):
                raise TelegramCollectorFolderGateError(
                    "Telegram вернул некорректный peer папки"
                )
            values.add(channel_id)
            continue
        if isinstance(peer, types.InputPeerUser):
            if not _is_positive_int(
                getattr(peer, "user_id", None)
            ) or not _is_nonzero_int(getattr(peer, "access_hash", None)):
                raise TelegramCollectorFolderGateError(
                    "Telegram вернул некорректный peer папки"
                )
            continue
        if isinstance(peer, types.InputPeerChat):
            if _is_positive_int(getattr(peer, "chat_id", None)):
                continue
            raise TelegramCollectorFolderGateError(
                "Telegram вернул некорректный peer папки"
            )
        raise TelegramCollectorFolderGateError(
            "Telegram-папка содержит неподдерживаемый peer"
        )
    return frozenset(values)


def _is_positive_int(value: Any) -> bool:
    return not isinstance(value, bool) and isinstance(value, int) and value > 0


def _is_nonzero_int(value: Any) -> bool:
    return not isinstance(value, bool) and isinstance(value, int) and value != 0


__all__ = [
    "CANARY_SOURCE_IDS",
    "EXACT_COLLECTOR_FOLDER_TITLE",
    "CollectorFolderGate",
    "ExactCanaryTelegramFolderGate",
    "TelegramCollectorFolderGateError",
]

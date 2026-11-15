"""Pure organization-identity normalization and deduplication helpers."""

from __future__ import annotations

from enum import Enum
from hashlib import sha256
import re
import unicodedata
from urllib.parse import urlsplit


class IdentifierKind(str, Enum):
    INN = "inn"
    OGRN = "ogrn"
    KPP = "kpp"
    DOMAIN = "domain"
    TWO_GIS_ID = "two_gis_id"
    YANDEX_MAPS_ID = "yandex_maps_id"


_DURABLE_PRIORITY = (
    IdentifierKind.INN,
    IdentifierKind.OGRN,
    IdentifierKind.DOMAIN,
)
_PROVIDER_PRIORITY = (
    IdentifierKind.TWO_GIS_ID,
    IdentifierKind.YANDEX_MAPS_ID,
)


def normalize_identifier(kind: IdentifierKind, raw_value: str) -> str:
    """Normalize and validate a supported organization identifier."""

    value = unicodedata.normalize("NFKC", raw_value).strip()
    if not value:
        raise ValueError("organization identifier cannot be blank")

    if kind in {IdentifierKind.INN, IdentifierKind.OGRN, IdentifierKind.KPP}:
        digits = re.sub(r"[\s-]+", "", value)
        if not digits.isascii() or not digits.isdigit():
            raise ValueError(f"{kind.value} must contain digits only")
        if kind is IdentifierKind.INN and not is_valid_inn(digits):
            raise ValueError("invalid INN length or checksum")
        if kind is IdentifierKind.OGRN and not is_valid_ogrn(digits):
            raise ValueError("invalid OGRN length or checksum")
        if kind is IdentifierKind.KPP and len(digits) != 9:
            raise ValueError("KPP must contain exactly 9 digits")
        return digits

    if kind is IdentifierKind.DOMAIN:
        return normalize_domain(value)

    normalized = " ".join(value.split())
    if len(normalized) > 200:
        raise ValueError("provider identifier is bounded to 200 characters")
    return normalized


def is_valid_inn(value: str) -> bool:
    if not value.isascii() or not value.isdigit():
        return False
    digits = tuple(int(item) for item in value)
    if len(digits) == 10:
        return _weighted_digit(digits[:9], (2, 4, 10, 3, 5, 9, 4, 6, 8)) == digits[9]
    if len(digits) == 12:
        check_11 = _weighted_digit(
            digits[:10],
            (7, 2, 4, 10, 3, 5, 9, 4, 6, 8),
        )
        check_12 = _weighted_digit(
            digits[:11],
            (3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8),
        )
        return (check_11, check_12) == digits[10:]
    return False


def is_valid_ogrn(value: str) -> bool:
    if not value.isascii() or not value.isdigit():
        return False
    if len(value) == 13:
        return int(value[:12]) % 11 % 10 == int(value[12])
    if len(value) == 15:
        return int(value[:14]) % 13 % 10 == int(value[14])
    return False


def normalize_domain(raw_value: str) -> str:
    candidate = raw_value.strip()
    parsed = urlsplit(
        candidate if "://" in candidate else f"//{candidate}",
        scheme="https",
    )
    if parsed.username or parsed.password:
        raise ValueError("credential-bearing domains are not allowed")
    host = parsed.hostname
    if not host:
        raise ValueError("domain must contain a hostname")
    try:
        domain = host.rstrip(".").casefold().encode("idna").decode("ascii")
    except UnicodeError as error:
        raise ValueError("domain cannot be IDNA-normalized") from error
    if domain.startswith("www."):
        domain = domain[4:]
    if len(domain) > 253:
        raise ValueError("domain exceeds 253 characters")
    labels = domain.split(".")
    if len(labels) < 2 or any(not _valid_domain_label(label) for label in labels):
        raise ValueError("domain is not a valid public hostname")
    if labels[-1].isdigit():
        raise ValueError("domain suffix cannot be numeric")
    return domain


def select_dedupe_key(
    identifiers: tuple[tuple[IdentifierKind, str], ...],
    *,
    allow_provider_fallback: bool = True,
) -> tuple[str | None, bool]:
    """Return ``(key, durable)`` using a cross-source-safe priority."""

    normalized: dict[IdentifierKind, set[str]] = {}
    for kind, value in identifiers:
        normalized.setdefault(kind, set()).add(normalize_identifier(kind, value))
    conflicts = tuple(
        kind.value
        for kind, values in normalized.items()
        if kind in {IdentifierKind.INN, IdentifierKind.OGRN}
        and len(values) > 1
    )
    if conflicts:
        raise ValueError(
            "conflicting organization identifiers: " + ", ".join(sorted(conflicts))
        )
    for kind in _DURABLE_PRIORITY:
        if kind in normalized and len(normalized[kind]) == 1:
            return f"{kind.value}:{next(iter(normalized[kind]))}", True
    if allow_provider_fallback:
        for kind in _PROVIDER_PRIORITY:
            if kind in normalized and len(normalized[kind]) == 1:
                return f"{kind.value}:{next(iter(normalized[kind]))}", False
    return None, False


def provisional_candidate_key(name: str, address: str) -> str:
    """Build a comparison key that must never qualify a lead by itself."""

    normalized_name = _identity_text(name)
    normalized_address = _identity_text(address)
    if not normalized_name or not normalized_address:
        raise ValueError("candidate key requires both name and address")
    payload = f"{normalized_name}\n{normalized_address}".encode("utf-8")
    return f"candidate:{sha256(payload).hexdigest()}"


def _weighted_digit(digits: tuple[int, ...], weights: tuple[int, ...]) -> int:
    return sum(digit * weight for digit, weight in zip(digits, weights, strict=True)) % 11 % 10


def _valid_domain_label(label: str) -> bool:
    return bool(
        1 <= len(label) <= 63
        and re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
    )


def _identity_text(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())

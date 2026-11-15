"""Fail-closed read-only adapters for multisource dry-run research.

All I/O is dependency-injected. Importing this module performs no network,
filesystem, database, Google Sheets, or Telegram action.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from enum import Enum
from hashlib import sha256
from ipaddress import IPv6Address, IPv6Network, ip_address
import json
import re
from typing import Any, Literal, Mapping, Protocol, runtime_checkable
from urllib.parse import urljoin, urlsplit

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)

from .identity import IdentifierKind, normalize_identifier


class AdapterError(RuntimeError):
    """A deliberately sanitized adapter failure."""


class RequestBudgetExceeded(AdapterError):
    """Raised before an adapter can exceed its explicit request budget."""


class AdapterConfigurationError(AdapterError):
    pass


class AdapterAuthorizationError(AdapterError):
    pass


class AdapterRateLimitError(AdapterError):
    pass


class AdapterTransientError(AdapterError):
    pass


class AdapterTimeoutError(AdapterError):
    pass


class AdapterResponseError(AdapterError):
    pass


class AdapterPayloadError(AdapterError):
    pass


class AdapterRedirectError(AdapterError):
    pass


class ProviderName(str, Enum):
    YANDEX_MAPS = "yandex_maps"
    TWO_GIS = "two_gis"
    FNS = "fns"
    OFFICIAL_WEBSITE = "official_website"


class _StrictModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )


class ProviderEntitlement(_StrictModel):
    """Explicit provider grant; construction without values always denies use."""

    provider: ProviderName
    allowed: bool = False
    license_confirmed: bool = False
    permitted_fields: tuple[str, ...] = ()

    @classmethod
    def from_mapping(
        cls,
        provider: ProviderName,
        values: Mapping[str, Any] | None = None,
    ) -> "ProviderEntitlement":
        raw = dict(values or {})
        return cls(provider=provider, **raw)

    @field_validator("permitted_fields")
    @classmethod
    def normalize_permitted_fields(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(sorted(set(values)))
        if any(not re.fullmatch(r"[a-z][a-z0-9_.-]{0,63}", item) for item in normalized):
            raise ValueError("permitted_fields contains an invalid field name")
        return normalized

    def require(self, provider: ProviderName, *, licensed: bool = False) -> None:
        if self.provider is not provider or not self.allowed:
            raise AdapterError(f"{provider.value} access is not entitled")
        if licensed and not self.license_confirmed:
            raise AdapterError(f"{provider.value} licensed access is not confirmed")


class RequestBudget(_StrictModel):
    max_requests: int = Field(default=1, ge=1, le=100)
    used_requests: int = Field(default=0, ge=0, le=100)

    @model_validator(mode="after")
    def validate_usage(self) -> "RequestBudget":
        if self.used_requests > self.max_requests:
            raise ValueError("used_requests cannot exceed max_requests")
        return self

    @property
    def remaining(self) -> int:
        return self.max_requests - self.used_requests

    def consume(self) -> "RequestBudget":
        if self.remaining < 1:
            raise RequestBudgetExceeded("request budget exhausted before network call")
        return self.model_copy(update={"used_requests": self.used_requests + 1})


class QueryParameter(_StrictModel):
    name: str = Field(min_length=1, max_length=100, pattern=r"^[A-Za-z0-9_.-]+$")
    value: str = Field(max_length=2_000)


class SecretQueryParameter(_StrictModel):
    name: str = Field(min_length=1, max_length=100, pattern=r"^[A-Za-z0-9_.-]+$")
    value: SecretStr = Field(repr=False)


class HttpHeader(_StrictModel):
    name: str = Field(min_length=1, max_length=100)
    value: str = Field(max_length=4_000)

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        if not re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", value):
            raise ValueError("invalid HTTP header name")
        return value.casefold()


class HttpRequest(_StrictModel):
    method: Literal["GET"] = "GET"
    url: str = Field(min_length=8, max_length=2_000)
    query: tuple[QueryParameter, ...] = Field(default=(), max_length=64)
    secret_query: tuple[SecretQueryParameter, ...] = Field(
        default=(),
        max_length=8,
        repr=False,
        exclude=True,
    )
    headers: tuple[HttpHeader, ...] = Field(default=(), max_length=32)
    resolved_ips: tuple[str, ...] = Field(default=(), max_length=32)
    timeout_seconds: float = Field(default=10.0, gt=0, le=30)
    max_response_bytes: int = Field(default=1_000_000, ge=1, le=5_000_000)
    follow_redirects: Literal[False] = False

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("HTTP request URL must be absolute HTTPS")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("HTTP request URL cannot contain credentials, query, or fragment")
        return value

    @model_validator(mode="after")
    def reject_duplicate_parameters(self) -> "HttpRequest":
        names = [item.name for item in (*self.query, *self.secret_query)]
        if len(set(names)) != len(names):
            raise ValueError("HTTP query parameter names must be unique")
        for value in self.resolved_ips:
            try:
                address = ip_address(value)
            except ValueError as error:
                raise ValueError("resolved_ips must contain IP addresses") from error
            if not _is_public_ip_address(address):
                raise ValueError("resolved_ips must be globally routable")
        return self

    def transport_query(self) -> tuple[tuple[str, str], ...]:
        """Reveal credentials only to the injected transport boundary."""

        public = tuple((item.name, item.value) for item in self.query)
        secret = tuple(
            (item.name, item.value.get_secret_value()) for item in self.secret_query
        )
        return (*public, *secret)


class HttpResponse(_StrictModel):
    status_code: int = Field(ge=100, le=599)
    headers: tuple[HttpHeader, ...] = Field(default=(), max_length=128)
    body: bytes = Field(repr=False, max_length=5_000_000)

    def header(self, name: str) -> str | None:
        wanted = name.casefold()
        values = tuple(item.value for item in self.headers if item.name == wanted)
        return values[-1] if values else None


@runtime_checkable
class AsyncHttpTransport(Protocol):
    async def request(self, request: HttpRequest) -> HttpResponse: ...


@runtime_checkable
class AsyncValidatedIpHttpTransport(Protocol):
    """Connect directly to connect_ip while preserving URL host for TLS and HTTP."""

    async def request_to_validated_ip(
        self,
        request: HttpRequest,
        *,
        connect_ip: str,
    ) -> HttpResponse: ...


class TransientBusinessObservation(_StrictModel):
    """Allowlisted public facts only; provider-specific IDs are absent by design."""

    provider: ProviderName
    name: str = Field(min_length=1, max_length=300)
    category: str | None = Field(default=None, max_length=200)
    address: str | None = Field(default=None, max_length=500)
    website_url: str | None = Field(default=None, max_length=2_000)
    public_phones: tuple[str, ...] = Field(default=(), max_length=16)
    inn: str | None = Field(default=None, pattern=r"^(?:[0-9]{10}|[0-9]{12})$")
    ogrn: str | None = Field(default=None, pattern=r"^(?:[0-9]{13}|[0-9]{15})$")
    legal_status: str | None = Field(default=None, max_length=100)
    source_url: str = Field(min_length=8, max_length=2_000)
    collected_at: datetime
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("collected_at")
    @classmethod
    def require_aware_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("collected_at must be timezone-aware")
        return value


class TransientObservationPage(_StrictModel):
    provider: ProviderName
    observations: tuple[TransientBusinessObservation, ...] = Field(
        default=(),
        max_length=200,
    )
    continuation_token: str | None = Field(
        default=None,
        max_length=500,
        repr=False,
        exclude=True,
    )


class AdapterReceipt(_StrictModel):
    provider: ProviderName
    endpoint: str = Field(min_length=8, max_length=2_000)
    collected_at: datetime
    request_count: int = Field(ge=0, le=100)
    status_codes: tuple[int, ...] = Field(default=(), max_length=100)
    content_sha256: tuple[str, ...] = Field(default=(), max_length=100)
    completed: bool
    outcome: str = Field(min_length=1, max_length=100)

    @field_validator("collected_at")
    @classmethod
    def require_aware_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("collected_at must be timezone-aware")
        return value


class TransientWebsitePage(_StrictModel):
    url: str = Field(min_length=8, max_length=2_000)
    content_type: str = Field(min_length=1, max_length=200)
    collected_at: datetime
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    content_length: int = Field(ge=0, le=5_000_000)
    text: str = Field(repr=False, exclude=True, max_length=2_000_000)

    @field_validator("collected_at")
    @classmethod
    def require_aware_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("collected_at must be timezone-aware")
        return value


YANDEX_MAPS_SEARCH_ENDPOINT = "https://search-maps.yandex.ru/v1/"
TWO_GIS_PLACES_ENDPOINT = "https://catalog.api.2gis.com/3.0/items"

_YANDEX_FIELDS = frozenset(
    {"name", "category", "address", "website_url", "public_phones"}
)
_TWO_GIS_FIELDS = frozenset(
    {"name", "category", "address", "website_url", "public_phones", "inn"}
)
_TWO_GIS_SAFE_DEFAULT_FIELDS = frozenset({"name", "category", "address"})
_TWO_GIS_PROVIDER_FIELD_ORDER = (
    ("name", ("items.name",)),
    ("address", ("items.address_name", "items.full_name")),
    ("category", ("items.rubrics",)),
    ("inn", ("items.itin",)),
)
_TWO_GIS_CONTACT_FIELDS = frozenset({"website_url", "public_phones"})
_IPV6_TRANSITION_NETWORKS = (
    IPv6Network("64:ff9b::/96"),
    IPv6Network("64:ff9b:1::/48"),
)


class DirectorySearchQuery(_StrictModel):
    text: str = Field(min_length=1, max_length=200)
    city: str = Field(min_length=1, max_length=200)
    limit: int = Field(default=20, ge=1, le=200)
    page_size: int = Field(default=20, ge=1, le=50)

    @field_validator("text", "city")
    @classmethod
    def normalize_text(cls, value: str) -> str:
        normalized = " ".join(value.split())
        if not normalized:
            raise ValueError("search text cannot be blank")
        return normalized


class DirectoryAdapterResult(_StrictModel):
    pages: tuple[TransientObservationPage, ...] = Field(default=(), max_length=100)
    receipt: AdapterReceipt

    @property
    def observations(self) -> tuple[TransientBusinessObservation, ...]:
        return tuple(item for page in self.pages for item in page.observations)


class YandexMapsConfig(_StrictModel):
    api_key: SecretStr | None = Field(default=None, repr=False)
    timeout_seconds: float = Field(default=10.0, gt=0, le=30)
    max_response_bytes: int = Field(default=1_000_000, ge=1, le=2_000_000)


class TwoGisConfig(_StrictModel):
    api_key: SecretStr | None = Field(default=None, repr=False)
    timeout_seconds: float = Field(default=10.0, gt=0, le=30)
    max_response_bytes: int = Field(default=1_000_000, ge=1, le=2_000_000)


class FNSAccessMode(str, Enum):
    MANUAL = "manual"
    OPEN_DATA_SNAPSHOT = "open_data_snapshot"
    LICENSED_EGRUL_FTP = "licensed_egrul_ftp"
    LICENSED_PROVIDER = "licensed_provider"


class FNSRecordRequest(_StrictModel):
    mode: FNSAccessMode
    inn: str | None = None
    ogrn: str | None = None

    @field_validator("inn")
    @classmethod
    def normalize_inn(cls, value: str | None) -> str | None:
        return None if value is None else normalize_identifier(IdentifierKind.INN, value)

    @field_validator("ogrn")
    @classmethod
    def normalize_ogrn(cls, value: str | None) -> str | None:
        return None if value is None else normalize_identifier(IdentifierKind.OGRN, value)

    @model_validator(mode="after")
    def require_identifier(self) -> "FNSRecordRequest":
        if self.inn is None and self.ogrn is None:
            raise ValueError("FNS verification requires INN or OGRN")
        return self


class OfficialFNSRecord(_StrictModel):
    mode: FNSAccessMode
    source_url: str = Field(min_length=8, max_length=2_000)
    observed_at: datetime
    legal_name: str = Field(min_length=1, max_length=500)
    legal_status: str = Field(min_length=1, max_length=100)
    inn: str | None = None
    ogrn: str | None = None
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    provider_name: str = Field(default="fns", min_length=1, max_length=200)

    @field_validator("inn")
    @classmethod
    def normalize_inn(cls, value: str | None) -> str | None:
        return None if value is None else normalize_identifier(IdentifierKind.INN, value)

    @field_validator("ogrn")
    @classmethod
    def normalize_ogrn(cls, value: str | None) -> str | None:
        return None if value is None else normalize_identifier(IdentifierKind.OGRN, value)

    @field_validator("source_url")
    @classmethod
    def validate_source_url(cls, value: str) -> str:
        _validate_canonical_public_https(value)
        return value

    @field_validator("observed_at")
    @classmethod
    def require_aware_time(cls, value: datetime) -> datetime:
        _require_aware(value)
        return value

    @model_validator(mode="after")
    def validate_record(self) -> "OfficialFNSRecord":
        if self.inn is None and self.ogrn is None:
            raise ValueError("official FNS record requires INN or OGRN")
        host = urlsplit(self.source_url).hostname or ""
        if self.mode is not FNSAccessMode.LICENSED_PROVIDER:
            official_host = (
                host == "nalog.ru"
                or host.endswith(".nalog.ru")
                or host == "nalog.gov.ru"
                or host.endswith(".nalog.gov.ru")
            )
            if not official_host:
                raise ValueError("official FNS modes require an official FNS source")
        return self


class FNSConfig(_StrictModel):
    timeout_seconds: float = Field(default=10.0, gt=0, le=30)


@runtime_checkable
class OfficialFNSRecordTransport(Protocol):
    """Injected structured official-record source; never a website scraper."""

    async def fetch(self, request: FNSRecordRequest) -> OfficialFNSRecord | None: ...


class FNSVerificationResult(_StrictModel):
    observation: TransientBusinessObservation | None = None
    receipt: AdapterReceipt


class WebsiteRedirectPolicy(str, Enum):
    DENY = "deny"
    SAME_ORIGIN = "same_origin"


class WebsiteReadRequest(_StrictModel):
    url: str = Field(min_length=8, max_length=2_000)
    official_domain: str = Field(min_length=3, max_length=253)

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        _validate_canonical_public_https(value)
        return value

    @field_validator("official_domain")
    @classmethod
    def normalize_official_domain(cls, value: str) -> str:
        return _normalize_public_domain(value)

    @model_validator(mode="after")
    def bind_url_to_explicit_domain(self) -> "WebsiteReadRequest":
        if (urlsplit(self.url).hostname or "").rstrip(".").casefold() != self.official_domain:
            raise ValueError("website URL host must equal the explicit official domain")
        return self


class WebsiteReaderConfig(_StrictModel):
    timeout_seconds: float = Field(default=10.0, gt=0, le=30)
    max_response_bytes: int = Field(default=1_000_000, ge=1, le=2_000_000)
    redirect_policy: WebsiteRedirectPolicy = WebsiteRedirectPolicy.DENY
    max_redirects: int = Field(default=0, ge=0, le=3)

    @model_validator(mode="after")
    def validate_redirect_limit(self) -> "WebsiteReaderConfig":
        if self.redirect_policy is WebsiteRedirectPolicy.DENY and self.max_redirects != 0:
            raise ValueError("deny redirect policy requires max_redirects=0")
        if self.redirect_policy is WebsiteRedirectPolicy.SAME_ORIGIN and self.max_redirects < 1:
            raise ValueError("same_origin redirect policy requires max_redirects>=1")
        return self


@runtime_checkable
class AsyncHostResolver(Protocol):
    async def resolve(self, host: str) -> tuple[str, ...]: ...


class WebsiteReadResult(_StrictModel):
    page: TransientWebsitePage
    receipt: AdapterReceipt


class OfficialWebsiteReader:
    """Fetch one explicit public HTTPS origin through validated-IP pinning."""

    source_family = ProviderName.OFFICIAL_WEBSITE.value

    def __init__(
        self,
        *,
        transport: AsyncValidatedIpHttpTransport,
        resolver: AsyncHostResolver,
        entitlement: ProviderEntitlement,
        config: WebsiteReaderConfig | None = None,
    ) -> None:
        self._transport = transport
        self._resolver = resolver
        self._entitlement = entitlement
        self._config = config or WebsiteReaderConfig()

    async def read(
        self,
        request: WebsiteReadRequest,
        *,
        budget: RequestBudget,
        collected_at: datetime,
    ) -> WebsiteReadResult:
        _require_aware(collected_at)
        self._entitlement.require(ProviderName.OFFICIAL_WEBSITE)
        if not isinstance(self._transport, AsyncValidatedIpHttpTransport):
            raise AdapterConfigurationError(
                "official_website transport lacks validated-IP pinning"
            )
        current_url = request.url
        allowed_origin = _https_origin(request.url)
        current_budget = budget
        statuses: list[int] = []
        visited: set[str] = set()
        redirect_count = 0
        while True:
            if current_url in visited:
                raise AdapterRedirectError("official_website redirect loop rejected")
            visited.add(current_url)
            resolved_ips = await _resolve_public_addresses(
                self._resolver,
                request.official_domain,
                timeout_seconds=self._config.timeout_seconds,
            )
            current_budget = current_budget.consume()
            http_request = HttpRequest(
                url=current_url,
                headers=(
                    HttpHeader(name="accept", value="text/html,application/xhtml+xml,text/plain"),
                ),
                resolved_ips=resolved_ips,
                timeout_seconds=self._config.timeout_seconds,
                max_response_bytes=self._config.max_response_bytes,
            )
            transport_failure: str | None = None
            try:
                async with asyncio.timeout(http_request.timeout_seconds):
                    response = await self._transport.request_to_validated_ip(
                        http_request,
                        connect_ip=resolved_ips[0],
                    )
            except TimeoutError:
                transport_failure = "timeout"
            except Exception:
                transport_failure = "error"
            if transport_failure == "timeout":
                raise AdapterTimeoutError("official_website request timed out") from None
            if transport_failure is not None:
                raise AdapterTransientError(
                    "official_website transport failed safely"
                ) from None
            statuses.append(response.status_code)
            if 300 <= response.status_code <= 399:
                if self._config.redirect_policy is WebsiteRedirectPolicy.DENY:
                    raise AdapterRedirectError("official_website redirect rejected by policy")
                if redirect_count >= self._config.max_redirects:
                    raise AdapterRedirectError("official_website redirect limit reached")
                location = response.header("location")
                if not location:
                    raise AdapterRedirectError("official_website redirect has no location")
                redirected = urljoin(current_url, location)
                try:
                    _validate_canonical_public_https(redirected)
                    redirected_origin = _https_origin(redirected)
                except ValueError as error:
                    raise AdapterRedirectError(
                        "official_website redirect target is unsafe"
                    ) from error
                if redirected_origin != allowed_origin:
                    raise AdapterRedirectError(
                        "official_website cross-origin redirect rejected"
                    )
                current_url = redirected
                redirect_count += 1
                continue
            _validate_response_status(response, ProviderName.OFFICIAL_WEBSITE)
            _validate_response_size(
                response,
                self._config.max_response_bytes,
                ProviderName.OFFICIAL_WEBSITE,
            )
            content_type = _media_type(response.header("content-type"))
            if content_type not in {"text/html", "application/xhtml+xml", "text/plain"}:
                raise AdapterResponseError(
                    "official_website returned unsupported content type"
                )
            charset = _charset(response.header("content-type"))
            if charset not in {None, "utf-8", "utf8"}:
                raise AdapterResponseError("official_website charset is not allowed")
            try:
                text = response.body.decode("utf-8")
            except UnicodeDecodeError as error:
                raise AdapterPayloadError(
                    "official_website body is not valid UTF-8"
                ) from error
            content_hash = sha256(response.body).hexdigest()
            return WebsiteReadResult(
                page=TransientWebsitePage(
                    url=current_url,
                    content_type=content_type,
                    collected_at=collected_at,
                    content_sha256=content_hash,
                    content_length=len(response.body),
                    text=text,
                ),
                receipt=AdapterReceipt(
                    provider=ProviderName.OFFICIAL_WEBSITE,
                    endpoint=current_url,
                    collected_at=collected_at,
                    request_count=current_budget.used_requests - budget.used_requests,
                    status_codes=tuple(statuses),
                    content_sha256=(content_hash,),
                    completed=True,
                    outcome="completed",
                ),
            )


class FNSVerifierAdapter:
    source_family = ProviderName.FNS.value

    def __init__(
        self,
        *,
        transport: OfficialFNSRecordTransport,
        entitlement: ProviderEntitlement,
        config: FNSConfig | None = None,
    ) -> None:
        self._transport = transport
        self._entitlement = entitlement
        self._config = config or FNSConfig()

    async def verify(
        self,
        request: FNSRecordRequest,
        *,
        budget: RequestBudget,
        collected_at: datetime,
    ) -> FNSVerificationResult:
        _require_aware(collected_at)
        licensed = request.mode in {
            FNSAccessMode.LICENSED_EGRUL_FTP,
            FNSAccessMode.LICENSED_PROVIDER,
        }
        self._entitlement.require(ProviderName.FNS, licensed=licensed)
        consumed = budget.consume()
        transport_failure: str | None = None
        try:
            async with asyncio.timeout(self._config.timeout_seconds):
                record = await self._transport.fetch(request)
        except TimeoutError:
            transport_failure = "timeout"
        except Exception:
            transport_failure = "error"
        if transport_failure == "timeout":
            raise AdapterTimeoutError("fns record transport timed out") from None
        if transport_failure is not None:
            raise AdapterTransientError("fns record transport failed safely") from None
        if record is None:
            return FNSVerificationResult(
                receipt=AdapterReceipt(
                    provider=ProviderName.FNS,
                    endpoint="https://www.nalog.gov.ru/",
                    collected_at=collected_at,
                    request_count=consumed.used_requests - budget.used_requests,
                    completed=True,
                    outcome="not_found",
                )
            )
        _validate_fns_record_matches_request(record, request, collected_at=collected_at)
        observation = TransientBusinessObservation(
            provider=ProviderName.FNS,
            name=record.legal_name,
            inn=record.inn,
            ogrn=record.ogrn,
            legal_status=record.legal_status,
            source_url=record.source_url,
            collected_at=collected_at,
            content_sha256=record.content_sha256,
        )
        return FNSVerificationResult(
            observation=observation,
            receipt=AdapterReceipt(
                provider=ProviderName.FNS,
                endpoint=record.source_url,
                collected_at=collected_at,
                request_count=consumed.used_requests - budget.used_requests,
                content_sha256=(record.content_sha256,),
                completed=True,
                outcome="verified_record",
            ),
        )


class YandexMapsAdapter:
    """Read-only client for the documented Yandex Organization Search API."""

    source_family = ProviderName.YANDEX_MAPS.value

    def __init__(
        self,
        *,
        transport: AsyncHttpTransport,
        entitlement: ProviderEntitlement,
        config: YandexMapsConfig,
    ) -> None:
        self._transport = transport
        self._entitlement = entitlement
        self._config = config

    async def discover(
        self,
        query: DirectorySearchQuery,
        *,
        budget: RequestBudget,
        collected_at: datetime,
    ) -> DirectoryAdapterResult:
        _require_aware(collected_at)
        self._entitlement.require(ProviderName.YANDEX_MAPS, licensed=True)
        key = _required_key(self._config.api_key, ProviderName.YANDEX_MAPS)
        fields = _permitted_fields(self._entitlement, _YANDEX_FIELDS)
        pages: list[TransientObservationPage] = []
        statuses: list[int] = []
        hashes: list[str] = []
        current_budget = budget
        offset = 0
        provider_page_size = query.page_size
        while offset < query.limit:
            current_budget = current_budget.consume()
            page_size = provider_page_size
            remaining = query.limit - offset
            request = HttpRequest(
                url=YANDEX_MAPS_SEARCH_ENDPOINT,
                query=(
                    QueryParameter(name="text", value=f"{query.text}, {query.city}"),
                    QueryParameter(name="type", value="biz"),
                    QueryParameter(name="lang", value="ru_RU"),
                    QueryParameter(name="results", value=str(page_size)),
                    QueryParameter(name="skip", value=str(offset)),
                ),
                secret_query=(
                    SecretQueryParameter(name="apikey", value=SecretStr(key)),
                ),
                timeout_seconds=self._config.timeout_seconds,
                max_response_bytes=self._config.max_response_bytes,
            )
            payload, response, content_hash = await _request_json(
                self._transport,
                request,
                provider=ProviderName.YANDEX_MAPS,
            )
            statuses.append(response.status_code)
            hashes.append(content_hash)
            observations, found = _parse_yandex_page(
                payload,
                fields=fields,
                collected_at=collected_at,
                content_hash=content_hash,
            )
            observations = observations[: min(page_size, remaining)]
            offset += page_size
            has_more = found is not None and offset < min(found, query.limit)
            pages.append(
                TransientObservationPage(
                    provider=ProviderName.YANDEX_MAPS,
                    observations=observations,
                    continuation_token=str(offset) if has_more else None,
                )
            )
            if not has_more:
                break
        return DirectoryAdapterResult(
            pages=tuple(pages),
            receipt=AdapterReceipt(
                provider=ProviderName.YANDEX_MAPS,
                endpoint=YANDEX_MAPS_SEARCH_ENDPOINT,
                collected_at=collected_at,
                request_count=current_budget.used_requests - budget.used_requests,
                status_codes=tuple(statuses),
                content_sha256=tuple(hashes),
                completed=True,
                outcome="completed",
            ),
        )


class TwoGisPlacesAdapter:
    """Read-only client for the documented 2GIS Places API endpoint."""

    source_family = ProviderName.TWO_GIS.value

    def __init__(
        self,
        *,
        transport: AsyncHttpTransport,
        entitlement: ProviderEntitlement,
        config: TwoGisConfig,
    ) -> None:
        self._transport = transport
        self._entitlement = entitlement
        self._config = config

    async def discover(
        self,
        query: DirectorySearchQuery,
        *,
        budget: RequestBudget,
        collected_at: datetime,
    ) -> DirectoryAdapterResult:
        _require_aware(collected_at)
        self._entitlement.require(ProviderName.TWO_GIS, licensed=True)
        key = _required_key(self._config.api_key, ProviderName.TWO_GIS)
        fields = _permitted_fields(
            self._entitlement,
            _TWO_GIS_FIELDS,
            default_fields=_TWO_GIS_SAFE_DEFAULT_FIELDS,
        )
        provider_fields = _two_gis_request_fields(fields)
        pages: list[TransientObservationPage] = []
        statuses: list[int] = []
        hashes: list[str] = []
        current_budget = budget
        page_number = 1
        seen = 0
        while seen < query.limit:
            current_budget = current_budget.consume()
            page_size = min(query.page_size, query.limit - seen)
            request = HttpRequest(
                url=TWO_GIS_PLACES_ENDPOINT,
                query=(
                    QueryParameter(name="q", value=f"{query.text}, {query.city}"),
                    QueryParameter(name="type", value="branch"),
                    QueryParameter(name="page", value=str(page_number)),
                    QueryParameter(name="page_size", value=str(page_size)),
                    QueryParameter(
                        name="fields",
                        value=",".join(provider_fields),
                    ),
                ),
                secret_query=(
                    SecretQueryParameter(name="key", value=SecretStr(key)),
                ),
                timeout_seconds=self._config.timeout_seconds,
                max_response_bytes=self._config.max_response_bytes,
            )
            payload, response, content_hash = await _request_json(
                self._transport,
                request,
                provider=ProviderName.TWO_GIS,
            )
            statuses.append(response.status_code)
            hashes.append(content_hash)
            observations, total, raw_count = _parse_two_gis_page(
                payload,
                fields=fields,
                collected_at=collected_at,
                content_hash=content_hash,
            )
            seen += raw_count
            has_more = (
                raw_count > 0
                and seen < query.limit
                and (
                    (total is not None and seen < total)
                    or (total is None and raw_count == page_size)
                )
            )
            pages.append(
                TransientObservationPage(
                    provider=ProviderName.TWO_GIS,
                    observations=observations,
                    continuation_token=str(page_number + 1) if has_more else None,
                )
            )
            if not has_more:
                break
            page_number += 1
        return DirectoryAdapterResult(
            pages=tuple(pages),
            receipt=AdapterReceipt(
                provider=ProviderName.TWO_GIS,
                endpoint=TWO_GIS_PLACES_ENDPOINT,
                collected_at=collected_at,
                request_count=current_budget.used_requests - budget.used_requests,
                status_codes=tuple(statuses),
                content_sha256=tuple(hashes),
                completed=True,
                outcome="completed",
            ),
        )


async def _request_json(
    transport: AsyncHttpTransport,
    request: HttpRequest,
    *,
    provider: ProviderName,
) -> tuple[Mapping[str, Any], HttpResponse, str]:
    transport_failure: str | None = None
    try:
        async with asyncio.timeout(request.timeout_seconds):
            response = await transport.request(request)
    except TimeoutError:
        transport_failure = "timeout"
    except Exception:
        transport_failure = "error"
    if transport_failure == "timeout":
        raise AdapterTimeoutError(f"{provider.value} request timed out") from None
    if transport_failure is not None:
        raise AdapterTransientError(
            f"{provider.value} transport failed safely"
        ) from None
    _validate_response_status(response, provider)
    _validate_response_size(response, request.max_response_bytes, provider)
    media_type = _media_type(response.header("content-type"))
    if media_type not in {"application/json", "application/geo+json"}:
        raise AdapterResponseError(f"{provider.value} returned unsupported content type")
    try:
        payload = json.loads(response.body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AdapterPayloadError(f"{provider.value} returned malformed JSON") from error
    if not isinstance(payload, Mapping):
        raise AdapterPayloadError(f"{provider.value} JSON root must be an object")
    return payload, response, sha256(response.body).hexdigest()


def _validate_response_status(response: HttpResponse, provider: ProviderName) -> None:
    status = response.status_code
    if status in {401, 403}:
        raise AdapterAuthorizationError(f"{provider.value} authorization rejected")
    if status == 429:
        raise AdapterRateLimitError(f"{provider.value} rate limit reached")
    if 500 <= status <= 599:
        raise AdapterTransientError(f"{provider.value} service unavailable")
    if 300 <= status <= 399:
        raise AdapterRedirectError(f"{provider.value} unexpected redirect")
    if not 200 <= status <= 299:
        raise AdapterResponseError(f"{provider.value} unexpected HTTP status {status}")


def _validate_response_size(
    response: HttpResponse,
    maximum: int,
    provider: ProviderName,
) -> None:
    content_length = response.header("content-length")
    if content_length is not None:
        if not content_length.isascii() or not content_length.isdigit():
            raise AdapterResponseError(f"{provider.value} invalid content length")
        if int(content_length) > maximum:
            raise AdapterResponseError(f"{provider.value} response exceeds size limit")
    if len(response.body) > maximum:
        raise AdapterResponseError(f"{provider.value} response exceeds size limit")


def _parse_yandex_page(
    payload: Mapping[str, Any],
    *,
    fields: frozenset[str],
    collected_at: datetime,
    content_hash: str,
) -> tuple[tuple[TransientBusinessObservation, ...], int | None]:
    raw_features = payload.get("features")
    if not isinstance(raw_features, list):
        raise AdapterPayloadError("yandex_maps payload has no features array")
    observations: list[TransientBusinessObservation] = []
    for raw_feature in raw_features:
        if not isinstance(raw_feature, Mapping):
            raise AdapterPayloadError("yandex_maps feature must be an object")
        properties = raw_feature.get("properties")
        if not isinstance(properties, Mapping):
            raise AdapterPayloadError("yandex_maps feature properties are invalid")
        company = properties.get("CompanyMetaData")
        if not isinstance(company, Mapping):
            continue
        name = _bounded_string(company.get("name"), maximum=300)
        if not name:
            continue
        category = _first_named(company.get("Categories")) if "category" in fields else None
        address = (
            _bounded_string(company.get("address"), maximum=500)
            if "address" in fields
            else None
        )
        website = (
            _public_https_or_none(company.get("url"))
            if "website_url" in fields
            else None
        )
        phones = (
            _public_phone_values(company.get("Phones"))
            if "public_phones" in fields
            else ()
        )
        observations.append(
            TransientBusinessObservation(
                provider=ProviderName.YANDEX_MAPS,
                name=name,
                category=category,
                address=address,
                website_url=website,
                public_phones=phones,
                source_url=YANDEX_MAPS_SEARCH_ENDPOINT,
                collected_at=collected_at,
                content_sha256=content_hash,
            )
        )
    found = _nested_nonnegative_int(
        payload,
        "properties",
        "ResponseMetaData",
        "SearchResponse",
        "found",
    )
    return tuple(observations), found


def _parse_two_gis_page(
    payload: Mapping[str, Any],
    *,
    fields: frozenset[str],
    collected_at: datetime,
    content_hash: str,
) -> tuple[tuple[TransientBusinessObservation, ...], int | None, int]:
    raw_result = payload.get("result")
    if not isinstance(raw_result, Mapping):
        raise AdapterPayloadError("two_gis payload has no result object")
    raw_items = raw_result.get("items")
    if not isinstance(raw_items, list):
        raise AdapterPayloadError("two_gis payload has no items array")
    observations: list[TransientBusinessObservation] = []
    for raw_item in raw_items:
        if not isinstance(raw_item, Mapping):
            raise AdapterPayloadError("two_gis item must be an object")
        name = _bounded_string(raw_item.get("name"), maximum=300)
        if not name:
            continue
        category = _first_named(raw_item.get("rubrics")) if "category" in fields else None
        address = None
        if "address" in fields:
            address = _bounded_string(
                raw_item.get("full_name") or raw_item.get("address_name"),
                maximum=500,
            )
        website: str | None = None
        phones: tuple[str, ...] = ()
        if fields & _TWO_GIS_CONTACT_FIELDS:
            website, phones = _two_gis_contacts(
                raw_item.get("contact_groups"),
                allow_website="website_url" in fields,
                allow_phones="public_phones" in fields,
            )
        inn = None
        if "inn" in fields:
            inn_candidate = raw_item.get("itin")
            if isinstance(inn_candidate, str) and re.fullmatch(
                r"(?:[0-9]{10}|[0-9]{12})",
                inn_candidate,
            ):
                inn = inn_candidate
        observations.append(
            TransientBusinessObservation(
                provider=ProviderName.TWO_GIS,
                name=name,
                category=category,
                address=address,
                website_url=website if "website_url" in fields else None,
                public_phones=phones if "public_phones" in fields else (),
                inn=inn,
                source_url=TWO_GIS_PLACES_ENDPOINT,
                collected_at=collected_at,
                content_sha256=content_hash,
            )
        )
    total = raw_result.get("total")
    if isinstance(total, bool) or not isinstance(total, int) or total < 0:
        total = None
    return tuple(observations), total, len(raw_items)


def _two_gis_contacts(
    value: object,
    *,
    allow_website: bool,
    allow_phones: bool,
) -> tuple[str | None, tuple[str, ...]]:
    if not isinstance(value, list):
        return None, ()
    website: str | None = None
    phones: list[str] = []
    for group in value[:16]:
        if not isinstance(group, Mapping):
            continue
        contacts = group.get("contacts")
        if not isinstance(contacts, list):
            continue
        for contact in contacts[:32]:
            if not isinstance(contact, Mapping):
                continue
            kind = contact.get("type")
            raw_value = contact.get("value") or contact.get("text") or contact.get("url")
            if allow_website and kind == "website" and website is None:
                website = _public_https_or_none(raw_value)
            elif allow_phones and kind == "phone":
                phone = _bounded_string(raw_value, maximum=100)
                if phone and re.fullmatch(r"\+?[0-9() .-]{5,100}", phone):
                    phones.append(phone)
    return website, tuple(dict.fromkeys(phones[:16]))


def _required_key(value: SecretStr | None, provider: ProviderName) -> str:
    if value is None or not value.get_secret_value().strip():
        raise AdapterConfigurationError(f"{provider.value} API key is not configured")
    return value.get_secret_value()


def _validate_fns_record_matches_request(
    record: OfficialFNSRecord,
    request: FNSRecordRequest,
    *,
    collected_at: datetime,
) -> None:
    if record.mode is not request.mode:
        raise AdapterPayloadError("fns record mode does not match request")
    if request.inn is not None and record.inn != request.inn:
        raise AdapterPayloadError("fns record INN does not match request")
    if request.ogrn is not None and record.ogrn != request.ogrn:
        raise AdapterPayloadError("fns record OGRN does not match request")
    if record.observed_at > collected_at:
        raise AdapterPayloadError("fns record observation cannot be from the future")


def _permitted_fields(
    entitlement: ProviderEntitlement,
    allowlist: frozenset[str],
    *,
    default_fields: frozenset[str] | None = None,
) -> frozenset[str]:
    if not entitlement.permitted_fields:
        return allowlist if default_fields is None else default_fields
    requested = frozenset(entitlement.permitted_fields)
    if not requested <= allowlist:
        raise AdapterConfigurationError(
            f"{entitlement.provider.value} entitlement contains unsupported fields"
        )
    return requested


def _two_gis_request_fields(fields: frozenset[str]) -> tuple[str, ...]:
    provider_fields: list[str] = []
    for public_field, requested_fields in _TWO_GIS_PROVIDER_FIELD_ORDER:
        if public_field in fields:
            provider_fields.extend(requested_fields)
    if fields & _TWO_GIS_CONTACT_FIELDS:
        provider_fields.append("items.contact_groups")
    return tuple(provider_fields)


def _bounded_string(value: object, *, maximum: int) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.split())
    if not normalized or len(normalized) > maximum:
        return None
    return normalized


def _first_named(value: object) -> str | None:
    if not isinstance(value, list):
        return None
    for item in value[:32]:
        if isinstance(item, Mapping):
            name = _bounded_string(item.get("name"), maximum=200)
            if name:
                return name
    return None


def _public_phone_values(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    result: list[str] = []
    for item in value[:16]:
        if not isinstance(item, Mapping):
            continue
        phone = _bounded_string(item.get("formatted") or item.get("value"), maximum=100)
        if phone and re.fullmatch(r"\+?[0-9() .-]{5,100}", phone):
            result.append(phone)
    return tuple(dict.fromkeys(result))


def _public_https_or_none(value: object) -> str | None:
    candidate = _bounded_string(value, maximum=2_000)
    if candidate is None:
        return None
    try:
        parsed = urlsplit(candidate)
        if parsed.scheme != "https" or not parsed.hostname:
            return None
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            return None
        _validate_public_host_literal(parsed.hostname)
    except ValueError:
        return None
    return candidate


def _validate_canonical_public_https(value: str) -> None:
    parsed = urlsplit(value)
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError("URL must be absolute HTTPS")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("URL cannot contain credentials, query, or fragment")
    _https_origin(value)
    _validate_public_host_literal(parsed.hostname)


def _https_origin(value: str) -> tuple[str, str, int]:
    parsed = urlsplit(value)
    if parsed.scheme.casefold() != "https" or not parsed.hostname:
        raise ValueError("URL must be absolute HTTPS")
    try:
        host = parsed.hostname.rstrip(".").casefold().encode("idna").decode("ascii")
        port = parsed.port or 443
    except (UnicodeError, ValueError) as error:
        raise ValueError("URL origin is invalid") from error
    return parsed.scheme.casefold(), host, port


def _nested_nonnegative_int(payload: Mapping[str, Any], *path: str) -> int | None:
    value: object = payload
    for part in path:
        if not isinstance(value, Mapping):
            return None
        value = value.get(part)
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _media_type(value: str | None) -> str:
    return "" if value is None else value.split(";", 1)[0].strip().casefold()


def _charset(value: str | None) -> str | None:
    if value is None:
        return None
    for part in value.split(";")[1:]:
        name, separator, raw = part.partition("=")
        if separator and name.strip().casefold() == "charset":
            return raw.strip().strip('"').casefold()
    return None


async def _resolve_public_addresses(
    resolver: AsyncHostResolver,
    host: str,
    *,
    timeout_seconds: float,
) -> tuple[str, ...]:
    resolver_failure: str | None = None
    try:
        async with asyncio.timeout(timeout_seconds):
            values = await resolver.resolve(host)
    except TimeoutError:
        resolver_failure = "timeout"
    except Exception:
        resolver_failure = "error"
    if resolver_failure == "timeout":
        raise AdapterTimeoutError("official_website DNS resolution timed out") from None
    if resolver_failure is not None:
        raise AdapterTransientError("official_website DNS resolution failed safely") from None
    if not isinstance(values, tuple) or not values:
        raise AdapterResponseError("official_website DNS resolution returned no addresses")
    normalized: list[str] = []
    for value in values:
        if not isinstance(value, str):
            raise AdapterResponseError("official_website DNS response is invalid")
        try:
            address = ip_address(value)
        except ValueError as error:
            raise AdapterResponseError("official_website DNS response is invalid") from error
        if not _is_public_ip_address(address):
            raise AdapterResponseError("official_website resolved to a non-public address")
        normalized.append(address.compressed)
    return tuple(dict.fromkeys(normalized))


def _normalize_public_domain(value: str) -> str:
    if "://" in value or "/" in value or "@" in value or ":" in value:
        raise ValueError("official_domain must be a hostname only")
    normalized = value.rstrip(".").casefold().encode("idna").decode("ascii")
    _validate_public_host_literal(normalized)
    try:
        ip_address(normalized)
    except ValueError:
        return normalized
    raise ValueError("official_domain must be a DNS hostname, not an IP address")


def _require_aware(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")


def _is_public_ip_address(address: object) -> bool:
    if not getattr(address, "is_global", False) or getattr(address, "is_reserved", True):
        return False
    if isinstance(address, IPv6Address):
        if (
            address.ipv4_mapped is not None
            or address.sixtofour is not None
            or address.teredo is not None
        ):
            return False
        if any(address in network for network in _IPV6_TRANSITION_NETWORKS):
            return False
    return True


def _validate_public_host_literal(host: str) -> None:
    normalized = host.rstrip(".").casefold()
    if normalized == "localhost" or normalized.endswith(
        (".local", ".internal", ".lan", ".home", ".test", ".invalid", ".example", ".onion")
    ):
        raise ValueError("host is not public")
    try:
        address = ip_address(normalized)
    except ValueError:
        labels = normalized.encode("idna").decode("ascii").split(".")
        if len(labels) < 2 or any(
            not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
            for label in labels
        ) or labels[-1].isdigit():
            raise ValueError("host is invalid")
    else:
        if not _is_public_ip_address(address):
            raise ValueError("IP host is not globally routable")

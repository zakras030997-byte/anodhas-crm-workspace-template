"""Ephemeral orchestration for explicitly enabled multisource research.

This module has no persistence, Sheets, database, Telegram, outreach, runtime,
or scheduler port.  Disabled flags return before any injected adapter is used.
"""

from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field

from .adapters import (
    AdapterReceipt,
    DirectorySearchQuery,
    FNSRecordRequest,
    FNSVerifierAdapter,
    OfficialWebsiteReader,
    ProviderName,
    RequestBudget,
    TransientBusinessObservation,
    TransientWebsitePage,
    TwoGisPlacesAdapter,
    WebsiteReadRequest,
    YandexMapsAdapter,
)
from .features import MultisourceFeatureFlags


class DryRunError(RuntimeError):
    pass


class DryRunConfigurationError(DryRunError):
    pass


class _StrictModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )


class DryRunRequest(_StrictModel):
    flags: MultisourceFeatureFlags = Field(default_factory=MultisourceFeatureFlags)
    directory_query: DirectorySearchQuery | None = None
    fns_requests: tuple[FNSRecordRequest, ...] = Field(default=(), max_length=50)
    website_requests: tuple[WebsiteReadRequest, ...] = Field(default=(), max_length=50)
    max_requests_per_adapter: int = Field(default=5, ge=1, le=50)


class DryRunCandidatePreview(_StrictModel):
    candidate_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    provider: ProviderName
    name: str = Field(min_length=1, max_length=300)
    category: str | None = Field(default=None, max_length=200)
    address: str | None = Field(default=None, max_length=500)
    website_url: str | None = Field(default=None, max_length=2_000)
    public_phones: tuple[str, ...] = Field(default=(), max_length=16)
    inn: str | None = None
    ogrn: str | None = None
    legal_status: str | None = Field(default=None, max_length=100)
    source_url: str = Field(min_length=8, max_length=2_000)
    collected_at: datetime
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class DryRunEvidencePreview(_StrictModel):
    provider: ProviderName
    source_url: str = Field(min_length=8, max_length=2_000)
    collected_at: datetime
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    public_facts: tuple[tuple[str, str], ...] = Field(default=(), max_length=32)


class DryRunQualificationPreview(_StrictModel):
    candidate_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    disposition: Literal["research_only", "manual_review_candidate"]
    reason_codes: tuple[str, ...] = Field(min_length=1, max_length=16)
    persistence_allowed: Literal[False] = False
    outreach_allowed: Literal[False] = False


class DryRunRegistryPreview(_StrictModel):
    candidate_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    operation: Literal["manual_review_only"] = "manual_review_only"
    write_performed: Literal[False] = False


class DryRunPlan(_StrictModel):
    master_enabled: bool
    enabled_sources: tuple[str, ...]
    planned_operations: tuple[str, ...]
    default_execution: Literal["inspect_only"] = "inspect_only"
    write_sinks_available: Literal[False] = False


class DryRunPreview(_StrictModel):
    enabled: bool
    candidates: tuple[DryRunCandidatePreview, ...] = Field(default=(), max_length=1_000)
    evidence: tuple[DryRunEvidencePreview, ...] = Field(default=(), max_length=1_000)
    qualification: tuple[DryRunQualificationPreview, ...] = Field(
        default=(),
        max_length=1_000,
    )
    registry_preview: tuple[DryRunRegistryPreview, ...] = Field(
        default=(),
        max_length=1_000,
    )
    website_pages: tuple[TransientWebsitePage, ...] = Field(default=(), max_length=50)
    receipts: tuple[AdapterReceipt, ...] = Field(default=(), max_length=200)
    persistence_performed: Literal[False] = False
    sheets_write_performed: Literal[False] = False
    outreach_performed: Literal[False] = False


def build_dry_run_plan(flags: MultisourceFeatureFlags) -> DryRunPlan:
    effective = flags.effective()
    enabled_sources: list[str] = []
    planned: list[str] = []
    flag_map = (
        (effective.yandex_maps_discovery_enabled, "yandex_maps", "directory discovery"),
        (effective.two_gis_discovery_enabled, "two_gis", "directory discovery"),
        (effective.fns_verification_enabled, "fns", "official record verification"),
        (
            effective.company_website_research_enabled,
            "official_website",
            "explicit-domain page read",
        ),
    )
    for is_enabled, source, operation in flag_map:
        if is_enabled:
            enabled_sources.append(source)
            planned.append(f"{source}: {operation}")
    if effective.qualification_enabled:
        planned.append("local conservative qualification preview")
    if effective.registry_preview_enabled:
        planned.append("local registry operation preview; no write")
    return DryRunPlan(
        master_enabled=effective.enabled,
        enabled_sources=tuple(enabled_sources),
        planned_operations=tuple(planned),
    )


class MultisourceDryRun:
    """Compose read-only adapters without accepting any write-side dependency."""

    def __init__(
        self,
        *,
        yandex_maps: YandexMapsAdapter | None = None,
        two_gis: TwoGisPlacesAdapter | None = None,
        fns: FNSVerifierAdapter | None = None,
        official_website: OfficialWebsiteReader | None = None,
    ) -> None:
        self._yandex_maps = yandex_maps
        self._two_gis = two_gis
        self._fns = fns
        self._official_website = official_website

    async def run(
        self,
        request: DryRunRequest,
        *,
        collected_at: datetime,
    ) -> DryRunPreview:
        if collected_at.tzinfo is None or collected_at.utcoffset() is None:
            raise ValueError("collected_at must be timezone-aware")
        effective = request.flags.effective()
        if not effective.enabled:
            return DryRunPreview(enabled=False)
        unsupported = tuple(
            name
            for name, enabled in (
                ("demand_signal_research", effective.demand_signal_research_enabled),
                ("decision_maker_research", effective.decision_maker_research_enabled),
                (
                    "public_contact_verification",
                    effective.public_contact_verification_enabled,
                ),
            )
            if enabled
        )
        if unsupported:
            raise DryRunConfigurationError(
                "unsupported dry-run capabilities: " + ", ".join(unsupported)
            )

        observations: list[TransientBusinessObservation] = []
        pages: list[TransientWebsitePage] = []
        receipts: list[AdapterReceipt] = []
        if effective.yandex_maps_discovery_enabled:
            adapter = _required_adapter(self._yandex_maps, "yandex_maps")
            query = _required_query(request.directory_query, "yandex_maps")
            result = await adapter.discover(
                query,
                budget=RequestBudget(max_requests=request.max_requests_per_adapter),
                collected_at=collected_at,
            )
            observations.extend(result.observations)
            receipts.append(result.receipt)
        if effective.two_gis_discovery_enabled:
            adapter = _required_adapter(self._two_gis, "two_gis")
            query = _required_query(request.directory_query, "two_gis")
            result = await adapter.discover(
                query,
                budget=RequestBudget(max_requests=request.max_requests_per_adapter),
                collected_at=collected_at,
            )
            observations.extend(result.observations)
            receipts.append(result.receipt)
        if effective.fns_verification_enabled:
            adapter = _required_adapter(self._fns, "fns")
            fns_budget = RequestBudget(max_requests=request.max_requests_per_adapter)
            for fns_request in request.fns_requests:
                result = await adapter.verify(
                    fns_request,
                    budget=fns_budget,
                    collected_at=collected_at,
                )
                fns_budget = fns_budget.model_copy(
                    update={
                        "used_requests": fns_budget.used_requests
                        + result.receipt.request_count
                    }
                )
                if result.observation is not None:
                    observations.append(result.observation)
                receipts.append(result.receipt)
        if effective.company_website_research_enabled:
            adapter = _required_adapter(self._official_website, "official_website")
            website_budget = RequestBudget(max_requests=request.max_requests_per_adapter)
            for website_request in request.website_requests:
                result = await adapter.read(
                    website_request,
                    budget=website_budget,
                    collected_at=collected_at,
                )
                website_budget = website_budget.model_copy(
                    update={
                        "used_requests": website_budget.used_requests
                        + result.receipt.request_count
                    }
                )
                pages.append(result.page)
                receipts.append(result.receipt)

        candidates = tuple(_candidate_preview(item) for item in observations)
        evidence = tuple(_observation_evidence(item) for item in observations) + tuple(
            _website_evidence(item) for item in pages
        )
        qualification = ()
        if effective.qualification_enabled:
            qualification = tuple(_qualification_preview(item) for item in candidates)
        registry_preview = ()
        if effective.registry_preview_enabled:
            registry_preview = tuple(
                DryRunRegistryPreview(candidate_sha256=item.candidate_sha256)
                for item in candidates
            )
        return DryRunPreview(
            enabled=True,
            candidates=candidates,
            evidence=evidence,
            qualification=qualification,
            registry_preview=registry_preview,
            website_pages=tuple(pages),
            receipts=tuple(receipts),
        )


def _required_adapter(adapter: object | None, name: str):
    if adapter is None:
        raise DryRunConfigurationError(f"{name} adapter is enabled but not configured")
    return adapter


def _required_query(
    query: DirectorySearchQuery | None,
    name: str,
) -> DirectorySearchQuery:
    if query is None:
        raise DryRunConfigurationError(f"{name} requires directory_query")
    return query


def _candidate_preview(
    observation: TransientBusinessObservation,
) -> DryRunCandidatePreview:
    payload = {
        "name": observation.name.casefold(),
        "address": observation.address.casefold() if observation.address else None,
        "website_url": observation.website_url,
        "inn": observation.inn,
        "ogrn": observation.ogrn,
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return DryRunCandidatePreview(
        candidate_sha256=sha256(encoded).hexdigest(),
        provider=observation.provider,
        name=observation.name,
        category=observation.category,
        address=observation.address,
        website_url=observation.website_url,
        public_phones=observation.public_phones,
        inn=observation.inn,
        ogrn=observation.ogrn,
        legal_status=observation.legal_status,
        source_url=observation.source_url,
        collected_at=observation.collected_at,
        content_sha256=observation.content_sha256,
    )


def _observation_evidence(
    observation: TransientBusinessObservation,
) -> DryRunEvidencePreview:
    facts = [("business.name", observation.name)]
    for name, value in (
        ("business.category", observation.category),
        ("business.address", observation.address),
        ("business.website_url", observation.website_url),
        ("organization.inn", observation.inn),
        ("organization.ogrn", observation.ogrn),
        ("legal.status", observation.legal_status),
    ):
        if value is not None:
            facts.append((name, value))
    facts.extend(("contact.business_phone", value) for value in observation.public_phones)
    return DryRunEvidencePreview(
        provider=observation.provider,
        source_url=observation.source_url,
        collected_at=observation.collected_at,
        content_sha256=observation.content_sha256,
        public_facts=tuple(facts),
    )


def _website_evidence(page: TransientWebsitePage) -> DryRunEvidencePreview:
    host = urlsplit(page.url).hostname or ""
    return DryRunEvidencePreview(
        provider=ProviderName.OFFICIAL_WEBSITE,
        source_url=page.url,
        collected_at=page.collected_at,
        content_sha256=page.content_sha256,
        public_facts=(("business.domain", host),),
    )


def _qualification_preview(
    candidate: DryRunCandidatePreview,
) -> DryRunQualificationPreview:
    reasons = ["manual_review_required"]
    if candidate.provider is not ProviderName.FNS:
        reasons.append("fns_verification_required")
    if not candidate.website_url:
        reasons.append("official_website_evidence_required")
    reasons.extend(
        (
            "demand_signal_required",
            "decision_maker_required",
            "public_contact_verification_required",
        )
    )
    disposition: Literal["research_only", "manual_review_candidate"] = (
        "manual_review_candidate"
        if candidate.inn is not None and candidate.legal_status == "active"
        else "research_only"
    )
    return DryRunQualificationPreview(
        candidate_sha256=candidate.candidate_sha256,
        disposition=disposition,
        reason_codes=tuple(reasons),
    )

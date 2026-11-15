"""Strict domain contracts for evidence-backed local business leads."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from hashlib import sha256
from ipaddress import ip_address
import json
import re
import unicodedata
from typing import Literal
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from .identity import IdentifierKind, normalize_identifier


_NON_PUBLIC_HOST_SUFFIXES = (
    ".home",
    ".example",
    ".internal",
    ".invalid",
    ".lan",
    ".local",
    ".localhost",
    ".onion",
    ".test",
)


class VerificationState(str, Enum):
    UNVERIFIED = "unverified"
    VERIFIED = "verified"
    REJECTED = "rejected"


class OrganizationLifecycle(str, Enum):
    CANDIDATE = "candidate"
    VERIFIED = "verified"
    REJECTED = "rejected"
    ARCHIVED = "archived"


class OrganizationLegalStatus(str, Enum):
    UNKNOWN = "unknown"
    ACTIVE = "active"
    LIQUIDATING = "liquidating"
    INACTIVE = "inactive"


class BusinessVertical(str, Enum):
    AUTO_SERVICE = "auto_service"
    COFFEE_SHOP = "coffee_shop"
    LEGAL_BUREAU = "legal_bureau"
    LAW_OFFICE = "law_office"


class EvidenceSourceKind(str, Enum):
    BUSINESS_DIRECTORY = "business_directory"
    OFFICIAL_REGISTRY = "official_registry"
    OFFICIAL_WEBSITE = "official_website"
    PUBLIC_BUSINESS_PROFILE = "public_business_profile"
    PUBLIC_JOB_POSTING = "public_job_posting"
    MANUAL_RESEARCH = "manual_research"


class CollectionBasis(str, Enum):
    OFFICIAL_API = "official_api"
    PUBLIC_WEB = "public_web"
    LICENSED = "licensed"
    MANUAL = "manual"


class EvidenceStoragePolicy(str, Enum):
    LINK_ONLY = "link_only"
    FACTS_ONLY = "facts_only"
    LICENSED_RAW = "licensed_raw"


class DemandSignalKind(str, Enum):
    EXPLICIT_PROJECT_REQUEST = "explicit_project_request"
    LEAD_HANDLING_GAP = "lead_handling_gap"
    BOOKING_GAP = "booking_gap"
    CUSTOMER_SERVICE_GAP = "customer_service_gap"
    DOCUMENT_WORKFLOW_GAP = "document_workflow_gap"
    REPUTATION_GAP = "reputation_gap"
    DIGITAL_PRESENCE_GAP = "digital_presence_gap"
    HIRING_FOR_AUTOMATION = "hiring_for_automation"


class DecisionMakerSeniority(str, Enum):
    OWNER = "owner"
    EXECUTIVE = "executive"
    HEAD = "head"
    MANAGER = "manager"
    UNKNOWN = "unknown"


class ContactChannel(str, Enum):
    BUSINESS_EMAIL = "business_email"
    BUSINESS_PHONE = "business_phone"
    TELEGRAM = "telegram"
    WEBSITE_FORM = "website_form"
    OFFICIAL_PROFILE = "official_profile"


class ContactScope(str, Enum):
    ORGANIZATION = "organization"
    ROLE = "role"
    NAMED_BUSINESS_PERSON = "named_business_person"


class _StrictModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )


class EvidenceFact(_StrictModel):
    name: str = Field(min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9_.-]*$")
    value: str = Field(min_length=1, max_length=2_000)

    @field_validator("name")
    @classmethod
    def reject_sensitive_person_fields(cls, value: str) -> str:
        allowed = {
            "business.address",
            "business.category",
            "business.domain",
            "business.name",
            "business.vertical",
            "contact.channel",
            "contact.decision_maker_id",
            "contact.decision_maker_name_sha256",
            "contact.scope",
            "contact.value_sha256",
            "demand.kind",
            "demand.strength",
            "demand.summary_sha256",
            "demand.timing_readiness",
            "legal.status",
            "organization.inn",
            "organization.kpp",
            "organization.legal_name",
            "organization.ogrn",
            "role.name",
            "role.responsibility",
            "role.seniority",
            "role.title",
            "source.record_id",
            "verified.claim",
        }
        if value not in allowed:
            raise ValueError("evidence fact name is outside the business allowlist")
        return value

    @field_validator("value")
    @classmethod
    def normalize_value(cls, value: str, info: ValidationInfo) -> str:
        normalized = _bounded_text(
            value,
            field_name="evidence fact",
            maximum=2_000,
        )
        _reject_sensitive_text(normalized)
        fact_name = info.data.get("name")
        hash_fact_names = {
            "contact.decision_maker_name_sha256",
            "contact.value_sha256",
            "demand.summary_sha256",
        }
        if fact_name in hash_fact_names:
            if not re.fullmatch(r"[0-9a-f]{64}", normalized):
                raise ValueError(f"{fact_name} must be a lowercase SHA-256")
            return normalized
        if fact_name == "contact.decision_maker_id":
            try:
                return str(UUID(normalized))
            except ValueError as error:
                raise ValueError(
                    "contact.decision_maker_id must be a UUID"
                ) from error
        legal_identifier_kinds = {
            "organization.inn": IdentifierKind.INN,
            "organization.kpp": IdentifierKind.KPP,
            "organization.ogrn": IdentifierKind.OGRN,
        }
        if fact_name in legal_identifier_kinds:
            return normalize_identifier(
                legal_identifier_kinds[fact_name],
                normalized,
            )
        if fact_name == "business.domain":
            return normalize_identifier(IdentifierKind.DOMAIN, normalized)
        if fact_name == "source.record_id":
            if not re.fullmatch(r"[A-Za-z0-9._:-]{1,256}", normalized):
                raise ValueError("source.record_id must be an opaque token")
            return normalized
        enum_fact_values = {
            "business.vertical": {item.value for item in BusinessVertical},
            "contact.channel": {item.value for item in ContactChannel},
            "contact.scope": {item.value for item in ContactScope},
            "demand.kind": {item.value for item in DemandSignalKind},
            "legal.status": {item.value for item in OrganizationLegalStatus},
            "role.seniority": {item.value for item in DecisionMakerSeniority},
        }
        if fact_name in enum_fact_values:
            if normalized not in enum_fact_values[fact_name]:
                raise ValueError(f"{fact_name} has an unsupported value")
            return normalized
        if fact_name in {"demand.strength", "demand.timing_readiness"}:
            try:
                decimal_value = Decimal(normalized)
            except InvalidOperation as error:
                raise ValueError(f"{fact_name} must be a decimal") from error
            if not decimal_value.is_finite() or not Decimal("0") <= decimal_value <= Decimal("1"):
                raise ValueError(f"{fact_name} must be between 0 and 1")
            decimal_value = decimal_value.normalize()
            return "0" if decimal_value == 0 else format(decimal_value, "f")
        if re.search(r"[^\s@]+@[^\s@]+\.[^\s@]+", normalized):
            raise ValueError("raw contact values are not allowed in evidence facts")
        digits = re.sub(r"\D", "", normalized)
        if len(digits) >= 10:
            raise ValueError("phone-like values are not allowed in evidence facts")
        return normalized


class EvidenceEnvelope(_StrictModel):
    """Provenance without a provider payload or private-person data."""

    evidence_id: UUID = Field(default_factory=uuid4)
    subject_type: str = Field(min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9_.-]*$")
    subject_id: UUID
    source_kind: EvidenceSourceKind
    source_family: str = Field(min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9_.-]*$")
    source_name: str = Field(min_length=1, max_length=200)
    source_url: str = Field(min_length=8, max_length=2_000)
    external_record_id: str | None = Field(default=None, max_length=256)
    claim_type: str = Field(min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9_.-]*$")
    claim_summary: str = Field(min_length=1, max_length=1_000)
    facts: tuple[EvidenceFact, ...] = Field(default=(), max_length=64)
    observed_at: datetime
    collected_at: datetime
    valid_until: datetime
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    facts_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    collector_version: str = Field(min_length=1, max_length=100)
    collection_basis: CollectionBasis
    storage_policy: EvidenceStoragePolicy
    retention_basis: str | None = Field(default=None, max_length=500)
    confidence: Decimal = Field(ge=Decimal("0"), le=Decimal("1"))
    public_business_data: Literal[True] = True
    verification_state: VerificationState = VerificationState.UNVERIFIED
    verified_at: datetime | None = None

    @field_validator("source_name", "claim_summary")
    @classmethod
    def normalize_text_fields(cls, value: str, info: ValidationInfo) -> str:
        maximum = 200 if info.field_name == "source_name" else 1_000
        normalized = _bounded_text(
            value,
            field_name=info.field_name,
            maximum=maximum,
        )
        if info.field_name == "claim_summary":
            _reject_sensitive_text(normalized)
        return normalized

    @field_validator("external_record_id")
    @classmethod
    def validate_external_record_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = unicodedata.normalize("NFKC", value).strip()
        if not re.fullmatch(r"[A-Za-z0-9._:-]{1,256}", normalized):
            raise ValueError("external_record_id must be an opaque provider token")
        return normalized

    @field_validator("source_url")
    @classmethod
    def validate_source_url(cls, value: str) -> str:
        return normalize_https_url(value)

    @field_validator("observed_at", "collected_at", "valid_until", "verified_at")
    @classmethod
    def validate_datetimes(cls, value: datetime | None) -> datetime | None:
        if value is not None:
            _require_aware(value)
        return value

    @model_validator(mode="after")
    def validate_evidence(self) -> "EvidenceEnvelope":
        if self.observed_at > self.collected_at:
            raise ValueError("observed_at cannot be later than collected_at")
        if self.valid_until < self.collected_at:
            raise ValueError("valid_until cannot be earlier than collected_at")
        if self.verified_at is not None and self.valid_until < self.verified_at:
            raise ValueError("valid_until cannot be earlier than verified_at")
        if self.verified_at is not None and self.verified_at < self.collected_at:
            raise ValueError("verified_at cannot be earlier than collected_at")
        if calculate_facts_sha256(self.facts) != self.facts_sha256:
            raise ValueError("facts_sha256 does not match normalized facts")
        fact_names = tuple(fact.name for fact in self.facts)
        if len(set(fact_names)) != len(fact_names):
            raise ValueError(
                "an evidence envelope cannot contain duplicate fact names"
            )
        if self.storage_policy is EvidenceStoragePolicy.LINK_ONLY:
            if self.facts:
                raise ValueError("link_only evidence cannot retain normalized facts")
        elif not self.facts:
            raise ValueError("facts_only and licensed_raw evidence require facts")
        if self.storage_policy is EvidenceStoragePolicy.LICENSED_RAW:
            if self.collection_basis is not CollectionBasis.LICENSED:
                raise ValueError("licensed_raw requires a licensed collection basis")
            if not self.retention_basis:
                raise ValueError("licensed_raw requires a documented retention basis")
        elif self.retention_basis is not None:
            raise ValueError("retention_basis is only valid for licensed_raw")
        _validate_verification_timestamp(
            self.verification_state,
            self.verified_at,
        )
        return self

    def is_current(self, as_of: datetime) -> bool:
        _require_aware(as_of)
        return self.observed_at <= as_of <= self.valid_until


class OrganizationIdentifier(_StrictModel):
    kind: IdentifierKind
    value: str = Field(min_length=1, max_length=253)
    verification_state: VerificationState = VerificationState.UNVERIFIED
    evidence_ids: tuple[UUID, ...] = Field(min_length=1, max_length=16)
    verified_at: datetime | None = None

    @field_validator("value")
    @classmethod
    def normalize_value(cls, value: str, info: ValidationInfo) -> str:
        kind = info.data.get("kind")
        if not isinstance(kind, IdentifierKind):
            return value
        return normalize_identifier(kind, value)

    @field_validator("evidence_ids")
    @classmethod
    def unique_evidence_ids(cls, value: tuple[UUID, ...]) -> tuple[UUID, ...]:
        return _unique_uuids(value, field_name="identifier evidence_ids")

    @field_validator("verified_at")
    @classmethod
    def validate_verified_at(cls, value: datetime | None) -> datetime | None:
        if value is not None:
            _require_aware(value)
        return value

    @model_validator(mode="after")
    def validate_verification(self) -> "OrganizationIdentifier":
        _validate_verification_timestamp(
            self.verification_state,
            self.verified_at,
        )
        return self


class Organization(_StrictModel):
    organization_id: UUID = Field(default_factory=uuid4)
    display_name: str = Field(min_length=1, max_length=300)
    legal_name: str | None = Field(default=None, max_length=500)
    vertical: BusinessVertical
    category: str = Field(min_length=1, max_length=200)
    country_code: Literal["RU"] = "RU"
    region: str | None = Field(default=None, max_length=200)
    city: str = Field(min_length=1, max_length=200)
    address: str | None = Field(default=None, max_length=500)
    website_url: str | None = Field(default=None, max_length=2_000)
    identifiers: tuple[OrganizationIdentifier, ...] = Field(max_length=32)
    legal_status: OrganizationLegalStatus = OrganizationLegalStatus.UNKNOWN
    lifecycle: OrganizationLifecycle = OrganizationLifecycle.CANDIDATE
    evidence_ids: tuple[UUID, ...] = Field(min_length=1, max_length=128)

    @field_validator(
        "display_name",
        "legal_name",
        "category",
        "region",
        "city",
        "address",
    )
    @classmethod
    def normalize_organization_text(
        cls,
        value: str | None,
        info: ValidationInfo,
    ) -> str | None:
        if value is None:
            return None
        maximum = {
            "display_name": 300,
            "legal_name": 500,
            "category": 200,
            "region": 200,
            "city": 200,
            "address": 500,
        }[info.field_name]
        return _bounded_text(value, field_name=info.field_name, maximum=maximum)

    @field_validator("website_url")
    @classmethod
    def validate_website_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return normalize_https_url(value)

    @field_validator("evidence_ids")
    @classmethod
    def unique_evidence_ids(cls, value: tuple[UUID, ...]) -> tuple[UUID, ...]:
        return _unique_uuids(value, field_name="organization evidence_ids")

    @model_validator(mode="after")
    def validate_identifiers(self) -> "Organization":
        identities = tuple((item.kind, item.value) for item in self.identifiers)
        if len(set(identities)) != len(identities):
            raise ValueError("organization identifiers must be unique")
        singleton_legal_kinds = tuple(
            item.kind
            for item in self.identifiers
            if item.kind in {IdentifierKind.INN, IdentifierKind.OGRN}
        )
        if len(set(singleton_legal_kinds)) != len(singleton_legal_kinds):
            raise ValueError("organization legal identifiers of one kind conflict")
        if self.lifecycle is OrganizationLifecycle.VERIFIED and not any(
            item.verification_state is VerificationState.VERIFIED
            for item in self.identifiers
        ):
            raise ValueError("verified organization requires a verified identifier")
        return self


class DemandSignal(_StrictModel):
    signal_id: UUID = Field(default_factory=uuid4)
    organization_id: UUID
    kind: DemandSignalKind
    summary: str = Field(min_length=1, max_length=1_000)
    strength: Decimal = Field(ge=Decimal("0"), le=Decimal("1"))
    timing_readiness: Decimal = Field(ge=Decimal("0"), le=Decimal("1"))
    occurred_at: datetime
    valid_until: datetime
    evidence_ids: tuple[UUID, ...] = Field(min_length=1, max_length=32)
    verification_state: VerificationState = VerificationState.UNVERIFIED
    verified_at: datetime | None = None

    @field_validator("summary")
    @classmethod
    def normalize_summary(cls, value: str) -> str:
        normalized = _bounded_text(
            value,
            field_name="demand summary",
            maximum=1_000,
        )
        _reject_sensitive_text(normalized)
        return normalized

    @field_validator("occurred_at", "valid_until", "verified_at")
    @classmethod
    def validate_datetimes(cls, value: datetime | None) -> datetime | None:
        if value is not None:
            _require_aware(value)
        return value

    @field_validator("evidence_ids")
    @classmethod
    def unique_evidence_ids(cls, value: tuple[UUID, ...]) -> tuple[UUID, ...]:
        return _unique_uuids(value, field_name="demand evidence_ids")

    @model_validator(mode="after")
    def validate_signal(self) -> "DemandSignal":
        if self.valid_until < self.occurred_at:
            raise ValueError("valid_until cannot be earlier than occurred_at")
        _validate_verification_timestamp(
            self.verification_state,
            self.verified_at,
        )
        if self.verified_at is not None and not (
            self.occurred_at <= self.verified_at <= self.valid_until
        ):
            raise ValueError("verified_at must be inside the demand validity window")
        return self

    def is_current(self, as_of: datetime) -> bool:
        _require_aware(as_of)
        return self.occurred_at <= as_of <= self.valid_until


class DecisionMakerEvidence(_StrictModel):
    decision_maker_id: UUID = Field(default_factory=uuid4)
    organization_id: UUID
    asserted_name: str | None = Field(default=None, max_length=300)
    asserted_title: str = Field(min_length=1, max_length=300)
    seniority: DecisionMakerSeniority
    responsibility: str | None = Field(default=None, max_length=500)
    authority_score: Decimal = Field(ge=Decimal("0"), le=Decimal("1"))
    confidence: Decimal = Field(ge=Decimal("0"), le=Decimal("1"))
    public_profile_url: str | None = Field(default=None, max_length=2_000)
    observed_at: datetime
    evidence_ids: tuple[UUID, ...] = Field(min_length=1, max_length=32)
    verification_state: VerificationState = VerificationState.UNVERIFIED
    verified_at: datetime | None = None

    @field_validator("asserted_name", "asserted_title", "responsibility")
    @classmethod
    def normalize_person_text(
        cls,
        value: str | None,
        info: ValidationInfo,
    ) -> str | None:
        if value is None:
            return None
        maximum = 500 if info.field_name == "responsibility" else 300
        normalized = _bounded_text(
            value,
            field_name=info.field_name,
            maximum=maximum,
        )
        if info.field_name == "responsibility":
            _reject_sensitive_text(normalized)
        return normalized

    @field_validator("public_profile_url")
    @classmethod
    def validate_profile_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return normalize_https_url(value)

    @field_validator("observed_at", "verified_at")
    @classmethod
    def validate_datetimes(cls, value: datetime | None) -> datetime | None:
        if value is not None:
            _require_aware(value)
        return value

    @field_validator("evidence_ids")
    @classmethod
    def unique_evidence_ids(cls, value: tuple[UUID, ...]) -> tuple[UUID, ...]:
        return _unique_uuids(value, field_name="decision-maker evidence_ids")

    @model_validator(mode="after")
    def validate_decision_maker(self) -> "DecisionMakerEvidence":
        _validate_verification_timestamp(
            self.verification_state,
            self.verified_at,
        )
        if (
            self.verification_state is VerificationState.VERIFIED
            and not self.asserted_name
        ):
            raise ValueError("verified decision-maker evidence requires a name")
        if self.verified_at is not None and self.verified_at < self.observed_at:
            raise ValueError("decision-maker verified_at cannot precede observed_at")
        return self


class PublicContact(_StrictModel):
    contact_id: UUID = Field(default_factory=uuid4)
    organization_id: UUID
    decision_maker_id: UUID | None = None
    channel: ContactChannel
    value: str = Field(min_length=1, max_length=2_000, repr=False)
    scope: ContactScope
    publicly_listed: Literal[True] = True
    observed_at: datetime
    evidence_ids: tuple[UUID, ...] = Field(min_length=1, max_length=32)
    verification_state: VerificationState = VerificationState.UNVERIFIED
    verified_at: datetime | None = None

    @field_validator("value")
    @classmethod
    def normalize_value(cls, value: str, info: ValidationInfo) -> str:
        channel = info.data.get("channel")
        if not isinstance(channel, ContactChannel):
            return value
        return normalize_public_contact(channel, value)

    @field_validator("evidence_ids")
    @classmethod
    def unique_evidence_ids(cls, value: tuple[UUID, ...]) -> tuple[UUID, ...]:
        return _unique_uuids(value, field_name="contact evidence_ids")

    @field_validator("observed_at", "verified_at")
    @classmethod
    def validate_verified_at(cls, value: datetime | None) -> datetime | None:
        if value is not None:
            _require_aware(value)
        return value

    @model_validator(mode="after")
    def validate_contact(self) -> "PublicContact":
        _validate_verification_timestamp(
            self.verification_state,
            self.verified_at,
        )
        if self.verified_at is not None and self.verified_at < self.observed_at:
            raise ValueError("contact verified_at cannot precede observed_at")
        if (
            self.scope is ContactScope.NAMED_BUSINESS_PERSON
            and self.decision_maker_id is None
        ):
            raise ValueError("named business contact requires decision_maker_id")
        if (
            self.scope is not ContactScope.NAMED_BUSINESS_PERSON
            and self.decision_maker_id is not None
        ):
            raise ValueError(
                "decision_maker_id is only valid for a named business contact"
            )
        return self


def calculate_facts_sha256(facts: tuple[EvidenceFact, ...]) -> str:
    payload = [
        {"name": fact.name, "value": fact.value}
        for fact in sorted(facts, key=lambda item: (item.name, item.value))
    ]
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def normalize_https_url(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).strip()
    if any(character.isspace() for character in normalized):
        raise ValueError("URL cannot contain whitespace")
    parsed = urlsplit(normalized)
    if parsed.scheme.casefold() != "https" or not parsed.hostname:
        raise ValueError("URL must be an absolute HTTPS URL")
    if parsed.username or parsed.password:
        raise ValueError("credential-bearing URLs are not allowed")
    if parsed.query:
        raise ValueError("canonical URLs cannot contain query parameters")
    if parsed.fragment:
        raise ValueError("canonical URLs cannot contain fragments")
    host = parsed.hostname.rstrip(".").casefold()
    try:
        address = ip_address(host)
    except ValueError:
        normalize_identifier(IdentifierKind.DOMAIN, host)
        if host == "localhost" or host.endswith(_NON_PUBLIC_HOST_SUFFIXES):
            raise ValueError("URL host must be publicly routable")
    else:
        if not address.is_global:
            raise ValueError("URL IP address must be globally routable")
    return normalized


def normalize_public_contact(channel: ContactChannel, value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).strip()
    if channel is ContactChannel.BUSINESS_EMAIL:
        if len(normalized) > 254 or normalized.count("@") != 1:
            raise ValueError("invalid public business email")
        local, domain = normalized.rsplit("@", 1)
        if (
            len(local) > 64
            or local.startswith(".")
            or local.endswith(".")
            or ".." in local
            or not re.fullmatch(r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+", local)
            or domain.endswith(".")
            or not re.fullmatch(r"[^\s/@:]+", domain)
        ):
            raise ValueError("invalid public business email")
        canonical_domain = normalize_identifier(IdentifierKind.DOMAIN, domain)
        if canonical_domain.endswith(_NON_PUBLIC_HOST_SUFFIXES):
            raise ValueError("business email domain must be publicly routable")
        return f"{local}@{canonical_domain}"
    if channel is ContactChannel.BUSINESS_PHONE:
        if not re.fullmatch(r"\+?[0-9()\s.-]+", normalized):
            raise ValueError("business phone contains unsupported characters")
        digits = re.sub(r"\D", "", normalized)
        if len(digits) == 10:
            digits = f"7{digits}"
        elif len(digits) == 11 and digits.startswith("8"):
            digits = f"7{digits[1:]}"
        elif len(digits) == 11 and digits.startswith("7"):
            pass
        elif normalized.startswith("+") and 11 <= len(digits) <= 15:
            pass
        else:
            raise ValueError(
                "business phone must be an unambiguous Russian number "
                "or an explicit international E.164 number"
            )
        if not re.fullmatch(r"[1-9][0-9]{10,14}", digits):
            raise ValueError("business phone must be a valid E.164 number")
        return f"+{digits}"
    if channel is ContactChannel.TELEGRAM:
        username = normalized.removeprefix("@").rstrip("/")
        if username.startswith("https://t.me/"):
            username = username.removeprefix("https://t.me/")
        if not re.fullmatch(r"[A-Za-z0-9_]{5,32}", username):
            raise ValueError("Telegram contact must be a public username")
        return f"https://t.me/{username}"
    return normalize_https_url(normalized)


def _bounded_text(value: str, *, field_name: str, maximum: int) -> str:
    normalized = " ".join(unicodedata.normalize("NFKC", value).split())
    if not normalized:
        raise ValueError(f"{field_name} cannot be blank")
    if len(normalized) > maximum:
        raise ValueError(f"{field_name} exceeds {maximum} characters")
    return normalized


def _require_aware(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime values must be timezone-aware")


def _unique_uuids(values: tuple[UUID, ...], *, field_name: str) -> tuple[UUID, ...]:
    if len(set(values)) != len(values):
        raise ValueError(f"{field_name} must not contain duplicates")
    return values


def _validate_verification_timestamp(
    state: VerificationState,
    verified_at: datetime | None,
) -> None:
    if state is VerificationState.VERIFIED and verified_at is None:
        raise ValueError("verified facts require verified_at")
    if state is not VerificationState.VERIFIED and verified_at is not None:
        raise ValueError("verified_at is only valid for verified facts")


def _reject_sensitive_text(value: str) -> None:
    lowered = value.casefold()
    prohibited_markers = (
        "date of birth",
        "ethnicity",
        "health condition",
        "home address",
        "medical",
        "passport",
        "personal address",
        "political view",
        "religion",
        "биометр",
        "дата рождения",
        "здоровь",
        "медицин",
        "паспорт",
        "политическ",
        "религи",
        "снилс",
        "sexual orientation",
        "social security",
    )
    if any(marker in lowered for marker in prohibited_markers):
        raise ValueError("sensitive personal information is not allowed")

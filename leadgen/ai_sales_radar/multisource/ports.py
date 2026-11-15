"""Transport-neutral ports for future external-source adapters.

This module defines contracts only.  It contains no Yandex, 2GIS, FNS,
Google Sheets, database, Telegram, or network implementation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Generic, Protocol, TypeVar, runtime_checkable
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .models import (
    BusinessVertical,
    DecisionMakerEvidence,
    DemandSignal,
    EvidenceEnvelope,
    Organization,
    PublicContact,
)
from .qualification import QualifiedLead


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class EvidenceBatch(Generic[T]):
    value: T
    evidence: tuple[EvidenceEnvelope, ...]


class DiscoveryQuery(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )

    vertical: BusinessVertical
    city: str = Field(min_length=1, max_length=200)
    region: str | None = Field(default=None, max_length=200)
    limit: int = Field(default=50, ge=1, le=200)

    @field_validator("city", "region")
    @classmethod
    def normalize_location(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = " ".join(value.split())
        if not normalized:
            raise ValueError("locations cannot be blank")
        return normalized


class RegistryPreviewOperation(str, Enum):
    CREATE = "create"
    UPDATE = "update"
    SKIP_DUPLICATE = "skip_duplicate"
    SKIP_UNCHANGED = "skip_unchanged"


class QualifiedLeadRegistryPreview(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )

    operation: RegistryPreviewOperation
    lead_id: UUID
    dedupe_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    changed_fields: tuple[str, ...] = Field(default=(), max_length=64)
    explanation: str = Field(min_length=1, max_length=1_000)


@runtime_checkable
class OrganizationDiscoveryPort(Protocol):
    source_family: str

    async def discover(
        self,
        query: DiscoveryQuery,
        *,
        as_of: datetime,
    ) -> EvidenceBatch[tuple[Organization, ...]]: ...


@runtime_checkable
class OrganizationRegistryPort(Protocol):
    source_family: str

    async def verify(
        self,
        organization: Organization,
        *,
        as_of: datetime,
    ) -> EvidenceBatch[Organization]: ...


@runtime_checkable
class DemandSignalResearchPort(Protocol):
    source_family: str

    async def collect(
        self,
        organization: Organization,
        *,
        as_of: datetime,
    ) -> EvidenceBatch[tuple[DemandSignal, ...]]: ...


@runtime_checkable
class DecisionMakerResearchPort(Protocol):
    source_family: str

    async def collect(
        self,
        organization: Organization,
        *,
        as_of: datetime,
    ) -> EvidenceBatch[tuple[DecisionMakerEvidence, ...]]: ...


@runtime_checkable
class PublicContactResearchPort(Protocol):
    source_family: str

    async def collect(
        self,
        organization: Organization,
        decision_makers: tuple[DecisionMakerEvidence, ...],
        *,
        as_of: datetime,
    ) -> EvidenceBatch[tuple[PublicContact, ...]]: ...


@runtime_checkable
class ContactVerificationPort(Protocol):
    source_family: str

    async def verify(
        self,
        organization: Organization,
        contacts: tuple[PublicContact, ...],
        *,
        as_of: datetime,
    ) -> EvidenceBatch[tuple[PublicContact, ...]]: ...


@runtime_checkable
class ExistingOrganizationIndexPort(Protocol):
    async def find_duplicates(
        self,
        organization: Organization,
    ) -> tuple[UUID, ...]: ...


@runtime_checkable
class QualifiedLeadRegistryPreviewPort(Protocol):
    async def preview_upsert(
        self,
        lead: QualifiedLead,
    ) -> QualifiedLeadRegistryPreview: ...

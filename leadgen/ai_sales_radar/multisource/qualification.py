"""Hard qualification gates for evidence-backed local business leads."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from hashlib import sha256
from typing import Literal
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .identity import IdentifierKind, normalize_identifier, select_dedupe_key
from .models import (
    ContactChannel,
    ContactScope,
    DecisionMakerEvidence,
    DecisionMakerSeniority,
    DemandSignal,
    EvidenceEnvelope,
    EvidenceSourceKind,
    Organization,
    OrganizationIdentifier,
    OrganizationLegalStatus,
    OrganizationLifecycle,
    PublicContact,
    VerificationState,
)


_SINGLE_VALUED_FACT_NAMES = frozenset(
    {
        "business.domain",
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
        "organization.ogrn",
        "role.name",
        "role.responsibility",
        "role.seniority",
        "role.title",
    }
)

_GENERIC_EMAIL_LOCAL_PARTS = frozenset(
    {
        "admin",
        "contact",
        "hello",
        "info",
        "mail",
        "manager",
        "office",
        "order",
        "reception",
        "sales",
        "support",
        "zakaz",
    }
)
from .scoring import (
    QualificationScoreInputV2,
    QualificationScoreV2,
    calculate_qualification_score_v2,
    score_factor_evidence_ids,
)


class QualificationDisposition(str, Enum):
    QUALIFIED = "qualified"
    REVIEW = "review"
    REJECTED = "rejected"


class LeadPriority(str, Enum):
    HOT = "hot"
    QUALIFIED = "qualified"


class SafeNextAction(str, Enum):
    MANUAL_CRM_REVIEW = "manual_crm_review"
    PREPARE_PERSONALIZED_DRAFT = "prepare_personalized_draft"


class _StrictModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )


class QualifiedLead(_StrictModel):
    """A fully checked lead that still requires an explicit human action."""

    lead_id: UUID
    organization: Organization
    demand_signals: tuple[DemandSignal, ...] = Field(min_length=1, max_length=32)
    decision_makers: tuple[DecisionMakerEvidence, ...] = Field(
        min_length=1,
        max_length=32,
    )
    contacts: tuple[PublicContact, ...] = Field(min_length=1, max_length=32)
    evidence: tuple[EvidenceEnvelope, ...] = Field(min_length=1, max_length=256)
    score_input: QualificationScoreInputV2
    score: QualificationScoreV2
    dedupe_key: str = Field(min_length=1, max_length=300)
    dedupe_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    priority: LeadPriority
    qualified_at: datetime
    safe_next_action: SafeNextAction
    manual_review_required: Literal[True] = True

    @field_validator("qualified_at")
    @classmethod
    def validate_qualified_at(cls, value: datetime) -> datetime:
        _require_aware(value)
        return value

    @model_validator(mode="after")
    def enforce_qualification_contract(self) -> "QualifiedLead":
        audit = _audit_qualification(
            organization=self.organization,
            demand_signals=self.demand_signals,
            decision_makers=self.decision_makers,
            contacts=self.contacts,
            evidence=self.evidence,
            score_input=self.score_input,
            score=self.score,
            as_of=self.qualified_at,
        )
        if audit.failures:
            raise ValueError(
                "qualified lead failed gates: " + ", ".join(audit.failures)
            )
        if tuple(self.demand_signals) != audit.current_signals:
            raise ValueError("qualified lead contains stale or unverified signals")
        if tuple(self.decision_makers) != audit.verified_decision_makers:
            raise ValueError("qualified lead contains unverified decision makers")
        if tuple(self.contacts) != audit.verified_contacts:
            raise ValueError("qualified lead contains unverified contacts")
        if self.dedupe_key != audit.dedupe_key:
            raise ValueError("qualified lead dedupe_key does not match identity")
        expected_hash = sha256(self.dedupe_key.encode("utf-8")).hexdigest()
        if self.dedupe_hash != expected_hash:
            raise ValueError("qualified lead dedupe_hash does not match dedupe_key")
        expected_priority = (
            LeadPriority.HOT
            if self.score.total >= 80 and audit.hot_eligible
            else LeadPriority.QUALIFIED
        )
        if self.priority is not expected_priority:
            raise ValueError("qualified lead priority does not match score")
        return self


class QualificationDecision(_StrictModel):
    disposition: QualificationDisposition
    gate_failures: tuple[str, ...]
    qualified_lead: QualifiedLead | None = None

    @model_validator(mode="after")
    def validate_decision_shape(self) -> "QualificationDecision":
        if self.disposition is QualificationDisposition.QUALIFIED:
            if self.qualified_lead is None or self.gate_failures:
                raise ValueError("qualified decision requires a clean qualified lead")
        elif self.qualified_lead is not None:
            raise ValueError("non-qualified decision cannot contain a qualified lead")
        return self


@dataclass(frozen=True, slots=True)
class _QualificationAudit:
    failures: tuple[str, ...]
    dedupe_key: str | None
    current_signals: tuple[DemandSignal, ...]
    verified_decision_makers: tuple[DecisionMakerEvidence, ...]
    verified_contacts: tuple[PublicContact, ...]
    direct_decision_maker_contact: bool
    hot_eligible: bool


def evaluate_qualification(
    *,
    lead_id: UUID,
    organization: Organization,
    demand_signals: tuple[DemandSignal, ...],
    decision_makers: tuple[DecisionMakerEvidence, ...],
    contacts: tuple[PublicContact, ...],
    evidence: tuple[EvidenceEnvelope, ...],
    score_input: QualificationScoreInputV2,
    score: QualificationScoreV2,
    as_of: datetime,
    safe_next_action: SafeNextAction = SafeNextAction.MANUAL_CRM_REVIEW,
) -> QualificationDecision:
    """Apply hard gates; a high numerical score alone is insufficient."""

    audit = _audit_qualification(
        organization=organization,
        demand_signals=demand_signals,
        decision_makers=decision_makers,
        contacts=contacts,
        evidence=evidence,
        score_input=score_input,
        score=score,
        as_of=as_of,
    )
    if audit.failures:
        permanent_rejection = (
            organization.legal_status
            in {
                OrganizationLegalStatus.INACTIVE,
                OrganizationLegalStatus.LIQUIDATING,
            }
            or score.total < 50
        )
        return QualificationDecision(
            disposition=(
                QualificationDisposition.REJECTED
                if permanent_rejection
                else QualificationDisposition.REVIEW
            ),
            gate_failures=audit.failures,
        )

    assert audit.dedupe_key is not None
    qualified = QualifiedLead(
        lead_id=lead_id,
        organization=organization,
        demand_signals=audit.current_signals,
        decision_makers=audit.verified_decision_makers,
        contacts=audit.verified_contacts,
        evidence=tuple(sorted(evidence, key=lambda item: str(item.evidence_id))),
        score_input=score_input,
        score=score,
        dedupe_key=audit.dedupe_key,
        dedupe_hash=sha256(audit.dedupe_key.encode("utf-8")).hexdigest(),
        priority=(
            LeadPriority.HOT
            if score.total >= 80 and audit.hot_eligible
            else LeadPriority.QUALIFIED
        ),
        qualified_at=as_of,
        safe_next_action=safe_next_action,
    )
    return QualificationDecision(
        disposition=QualificationDisposition.QUALIFIED,
        gate_failures=(),
        qualified_lead=qualified,
    )


def _audit_qualification(
    *,
    organization: Organization,
    demand_signals: tuple[DemandSignal, ...],
    decision_makers: tuple[DecisionMakerEvidence, ...],
    contacts: tuple[PublicContact, ...],
    evidence: tuple[EvidenceEnvelope, ...],
    score_input: QualificationScoreInputV2,
    score: QualificationScoreV2,
    as_of: datetime,
) -> _QualificationAudit:
    _require_aware(as_of)
    failures: set[str] = set()

    if organization.lifecycle is not OrganizationLifecycle.VERIFIED:
        failures.add("organization_not_verified")
    if organization.legal_status is not OrganizationLegalStatus.ACTIVE:
        failures.add("organization_not_active")

    if len({item.signal_id for item in demand_signals}) != len(demand_signals):
        failures.add("duplicate_demand_signal")
    if len({item.decision_maker_id for item in decision_makers}) != len(
        decision_makers
    ):
        failures.add("duplicate_decision_maker")
    if len({item.contact_id for item in contacts}) != len(contacts):
        failures.add("duplicate_contact")
    if len({(item.channel, item.value.casefold()) for item in contacts}) != len(
        contacts
    ):
        failures.add("duplicate_contact_value")

    verified_organization_identifiers = tuple(
        item
        for item in organization.identifiers
        if item.verification_state is VerificationState.VERIFIED
        and item.verified_at is not None
        and item.verified_at <= as_of
    )
    if len(verified_organization_identifiers) != len(organization.identifiers):
        failures.add("unverified_organization_identifier_present")

    verified_legal_identifiers = tuple(
        item
        for item in verified_organization_identifiers
        if item.kind in {IdentifierKind.INN, IdentifierKind.OGRN}
    )
    legal_identifiers = tuple(
        (item.kind, item.value) for item in verified_legal_identifiers
    )
    dedupe_key, durable = select_dedupe_key(
        legal_identifiers,
        allow_provider_fallback=False,
    )
    if not dedupe_key or not durable:
        failures.add("verified_fns_identity_missing")

    current_signals = tuple(
        item
        for item in demand_signals
        if item.organization_id == organization.organization_id
        and item.verification_state is VerificationState.VERIFIED
        and item.verified_at is not None
        and item.verified_at <= as_of
        and item.is_current(as_of)
        and item.strength >= Decimal("0.5")
    )
    if not current_signals:
        failures.add("current_verified_demand_signal_missing")

    verified_decision_makers = tuple(
        item
        for item in decision_makers
        if item.organization_id == organization.organization_id
        and item.verification_state is VerificationState.VERIFIED
        and item.verified_at is not None
        and item.verified_at <= as_of
        and item.observed_at <= as_of
        and item.seniority is not DecisionMakerSeniority.UNKNOWN
        and item.authority_score >= Decimal("0.5")
        and item.confidence >= Decimal("0.5")
    )
    if not verified_decision_makers:
        failures.add("verified_decision_maker_authority_missing")

    verified_contacts = tuple(
        item
        for item in contacts
        if item.organization_id == organization.organization_id
        and item.verification_state is VerificationState.VERIFIED
        and item.verified_at is not None
        and item.verified_at <= as_of
        and item.observed_at <= as_of
        and item.publicly_listed
    )
    if not verified_contacts:
        failures.add("verified_public_business_contact_missing")

    if (
        len(current_signals) != len(demand_signals)
        or len(verified_decision_makers) != len(decision_makers)
        or len(verified_contacts) != len(contacts)
    ):
        failures.add("stale_or_unverified_entities_present")

    verified_decision_maker_ids = {
        item.decision_maker_id for item in verified_decision_makers
    }
    if any(
        item.decision_maker_id is not None
        and item.decision_maker_id not in verified_decision_maker_ids
        for item in verified_contacts
    ):
        failures.add("contact_decision_maker_unverified")

    if any(
        item.verification_state is VerificationState.VERIFIED
        and (item.verified_at is None or item.verified_at > as_of)
        for item in organization.identifiers
    ):
        failures.add("future_verification_timestamp")

    evidence_by_id = {item.evidence_id: item for item in evidence}
    if len(evidence_by_id) != len(evidence):
        failures.add("duplicate_evidence_id")

    all_referenced_ids = _all_referenced_evidence_ids(
        organization,
        demand_signals,
        decision_makers,
        contacts,
        score_input,
    )
    if all_referenced_ids - evidence_by_id.keys():
        failures.add("evidence_closure_incomplete")
    if evidence_by_id.keys() - all_referenced_ids:
        failures.add("unreferenced_evidence_not_allowed")

    if _subject_binding_failures(
        organization,
        demand_signals,
        decision_makers,
        contacts,
        evidence_by_id,
    ):
        failures.add("evidence_subject_mismatch")

    gate_evidence_ids = _gate_evidence_ids(
        organization,
        current_signals,
        verified_decision_makers,
        verified_contacts,
        score_input,
    )
    verified_current_gate_evidence = tuple(
        evidence_by_id[evidence_id]
        for evidence_id in gate_evidence_ids
        if evidence_id in evidence_by_id
        and evidence_by_id[evidence_id].verification_state
        is VerificationState.VERIFIED
        and evidence_by_id[evidence_id].verified_at is not None
        and evidence_by_id[evidence_id].verified_at <= as_of
        and evidence_by_id[evidence_id].collected_at <= as_of
        and evidence_by_id[evidence_id].is_current(as_of)
        and evidence_by_id[evidence_id].confidence >= Decimal("0.5")
    )
    if len(verified_current_gate_evidence) != len(gate_evidence_ids):
        failures.add("verified_current_evidence_missing")

    current_gate_evidence_ids = {
        item.evidence_id for item in verified_current_gate_evidence
    }
    if _identifier_evidence_failures(
        verified_organization_identifiers,
        evidence_by_id,
        current_gate_evidence_ids,
    ):
        failures.add("organization_identifier_evidence_mismatch")

    if _single_valued_fact_conflicts(verified_current_gate_evidence):
        failures.add("conflicting_current_evidence_facts")

    if _fns_legal_evidence_conflicts(
        verified_legal_identifiers,
        verified_current_gate_evidence,
    ):
        failures.add("conflicting_fns_legal_evidence")

    verified_organization_domains = _verified_organization_domains(
        organization,
        verified_organization_identifiers,
        evidence_by_id,
        current_gate_evidence_ids,
    )
    if _organization_evidence_binding_failures(
        organization,
        verified_organization_identifiers,
        verified_organization_domains,
        evidence_by_id,
        current_gate_evidence_ids,
    ):
        failures.add("organization_evidence_not_bound_to_organization")
    if (
        not _factor_at_least(
            score_input.company_fit.value,
            Decimal("0.5"),
        )
        or not _organization_has_verified_company_fit(
            organization,
            verified_organization_identifiers,
            verified_organization_domains,
            evidence_by_id,
            current_gate_evidence_ids,
        )
    ):
        failures.add("verified_company_fit_missing")
    if _entity_source_binding_failures(
        current_signals,
        verified_decision_makers,
        verified_contacts,
        verified_organization_identifiers,
        verified_organization_domains,
        evidence_by_id,
        current_gate_evidence_ids,
    ):
        failures.add("entity_evidence_not_bound_to_organization")

    if any(
        not _decision_maker_has_business_authority_evidence(
            item,
            verified_organization_identifiers,
            verified_organization_domains,
            evidence_by_id,
            current_gate_evidence_ids,
        )
        for item in verified_decision_makers
    ):
        failures.add("decision_maker_business_authority_evidence_missing")

    if not _fns_evidence_supports_legal_identity_and_active_status(
        verified_legal_identifiers,
        evidence_by_id,
        verified_current_gate_evidence,
    ):
        failures.add("current_fns_legal_verification_missing")

    source_families = {
        _source_independence_key(item)
        for item in verified_current_gate_evidence
    }
    if len(source_families) < 2:
        failures.add("independent_source_families_missing")

    direct_decision_maker_ids = _direct_contact_decision_maker_ids(
        verified_contacts,
        verified_decision_makers,
        evidence_by_id,
        current_gate_evidence_ids,
    )
    direct_decision_maker_contact = bool(direct_decision_maker_ids)
    hot_eligible = direct_decision_maker_contact and all(
        _factor_at_least(getattr(score_input, field_name).value, Decimal("0.5"))
        for field_name in (
            "company_fit",
            "pain_strength",
            "timing_readiness",
            "decision_maker_access",
            "evidence_quality",
        )
    )

    if _score_factor_binding_failures(
        organization,
        current_signals,
        verified_decision_makers,
        verified_contacts,
        score_input,
        evidence_by_id,
    ):
        failures.add("score_factor_evidence_mismatch")

    if _score_factor_value_failures(
        current_signals,
        verified_decision_makers,
        verified_contacts,
        score_input,
        evidence_by_id,
        direct_decision_maker_ids,
    ):
        failures.add("score_factor_exceeds_verified_facts")

    if any(
        evidence_by_id.get(evidence_id) is not None
        and evidence_by_id[evidence_id].source_kind
        is EvidenceSourceKind.OFFICIAL_REGISTRY
        for contact in verified_contacts
        for evidence_id in contact.evidence_ids
    ):
        failures.add("registry_cannot_verify_contact")
    if any(
        evidence_by_id.get(evidence_id) is not None
        and evidence_by_id[evidence_id].source_kind
        is EvidenceSourceKind.OFFICIAL_REGISTRY
        for signal in current_signals
        for evidence_id in signal.evidence_ids
    ):
        failures.add("registry_cannot_prove_demand")

    if calculate_qualification_score_v2(score_input) != score:
        failures.add("score_input_mismatch")
    if score.total < 70:
        failures.add("score_below_qualification_threshold")

    return _QualificationAudit(
        failures=tuple(sorted(failures)),
        dedupe_key=dedupe_key,
        current_signals=current_signals,
        verified_decision_makers=verified_decision_makers,
        verified_contacts=verified_contacts,
        direct_decision_maker_contact=direct_decision_maker_contact,
        hot_eligible=hot_eligible,
    )


def _subject_binding_failures(
    organization: Organization,
    demand_signals: tuple[DemandSignal, ...],
    decision_makers: tuple[DecisionMakerEvidence, ...],
    contacts: tuple[PublicContact, ...],
    evidence_by_id: dict[UUID, EvidenceEnvelope],
) -> tuple[str, ...]:
    failures: list[str] = []
    organization_evidence_ids = set(organization.evidence_ids)
    for identifier in organization.identifiers:
        organization_evidence_ids.update(identifier.evidence_ids)
    failures.extend(
        _subject_mismatches(
            organization_evidence_ids,
            subject_type="organization",
            subject_id=organization.organization_id,
            allowed_claim_types={
                "business_profile",
                "legal_identity",
                "legal_status",
                "organization_profile",
            },
            allowed_source_kinds=None,
            evidence_by_id=evidence_by_id,
        )
    )
    for signal in demand_signals:
        failures.extend(
            _subject_mismatches(
                set(signal.evidence_ids),
                subject_type="demand_signal",
                subject_id=signal.signal_id,
                allowed_claim_types={
                    "demand_signal",
                    "operational_gap",
                    "project_request",
                },
                allowed_source_kinds={
                    EvidenceSourceKind.BUSINESS_DIRECTORY,
                    EvidenceSourceKind.OFFICIAL_WEBSITE,
                    EvidenceSourceKind.PUBLIC_BUSINESS_PROFILE,
                    EvidenceSourceKind.PUBLIC_JOB_POSTING,
                },
                evidence_by_id=evidence_by_id,
            )
        )
        if not _evidence_set_has_facts(
            signal.evidence_ids,
            evidence_by_id,
            {
                "demand.kind": signal.kind.value,
                "demand.strength": _canonical_decimal(signal.strength),
                "demand.summary_sha256": sha256(
                    signal.summary.encode("utf-8")
                ).hexdigest(),
                "demand.timing_readiness": _canonical_decimal(
                    signal.timing_readiness
                ),
            },
        ):
            failures.append(str(signal.signal_id))
    for decision_maker in decision_makers:
        failures.extend(
            _subject_mismatches(
                set(decision_maker.evidence_ids),
                subject_type="decision_maker",
                subject_id=decision_maker.decision_maker_id,
                allowed_claim_types={
                    "business_role",
                    "decision_maker",
                    "management_role",
                },
                allowed_source_kinds={
                    EvidenceSourceKind.OFFICIAL_REGISTRY,
                    EvidenceSourceKind.OFFICIAL_WEBSITE,
                    EvidenceSourceKind.PUBLIC_BUSINESS_PROFILE,
                },
                evidence_by_id=evidence_by_id,
            )
        )
        required_role_facts = {
            "role.name": decision_maker.asserted_name or "",
            "role.seniority": decision_maker.seniority.value,
            "role.title": decision_maker.asserted_title,
        }
        if decision_maker.responsibility:
            required_role_facts["role.responsibility"] = (
                decision_maker.responsibility
            )
        if not _evidence_set_has_facts(
            decision_maker.evidence_ids,
            evidence_by_id,
            required_role_facts,
        ):
            failures.append(str(decision_maker.decision_maker_id))
    for contact in contacts:
        failures.extend(
            _subject_mismatches(
                set(contact.evidence_ids),
                subject_type="public_contact",
                subject_id=contact.contact_id,
                allowed_claim_types={"business_contact", "public_contact"},
                allowed_source_kinds={
                    EvidenceSourceKind.BUSINESS_DIRECTORY,
                    EvidenceSourceKind.OFFICIAL_WEBSITE,
                    EvidenceSourceKind.PUBLIC_BUSINESS_PROFILE,
                    EvidenceSourceKind.PUBLIC_JOB_POSTING,
                },
                evidence_by_id=evidence_by_id,
            )
        )
        required_contact_facts = {
            "contact.channel": contact.channel.value,
            "contact.scope": contact.scope.value,
            "contact.value_sha256": sha256(
                contact.value.encode("utf-8")
            ).hexdigest(),
        }
        if contact.decision_maker_id is not None:
            decision_maker = next(
                (
                    item
                    for item in decision_makers
                    if item.decision_maker_id == contact.decision_maker_id
                ),
                None,
            )
            required_contact_facts["contact.decision_maker_id"] = str(
                contact.decision_maker_id
            )
            if decision_maker is not None and decision_maker.asserted_name:
                required_contact_facts["contact.decision_maker_name_sha256"] = (
                    _name_sha256(decision_maker.asserted_name)
                )
        if not _evidence_set_has_facts(
            contact.evidence_ids,
            evidence_by_id,
            required_contact_facts,
        ):
            failures.append(str(contact.contact_id))
    return tuple(failures)


def _subject_mismatches(
    evidence_ids: set[UUID],
    *,
    subject_type: str,
    subject_id: UUID,
    allowed_claim_types: set[str],
    allowed_source_kinds: set[EvidenceSourceKind] | None,
    evidence_by_id: dict[UUID, EvidenceEnvelope],
) -> tuple[str, ...]:
    return tuple(
        str(evidence_id)
        for evidence_id in evidence_ids
        if evidence_id in evidence_by_id
        and (
            evidence_by_id[evidence_id].subject_type != subject_type
            or evidence_by_id[evidence_id].subject_id != subject_id
            or evidence_by_id[evidence_id].claim_type not in allowed_claim_types
            or (
                allowed_source_kinds is not None
                and evidence_by_id[evidence_id].source_kind
                not in allowed_source_kinds
            )
        )
    )


def _score_factor_binding_failures(
    organization: Organization,
    demand_signals: tuple[DemandSignal, ...],
    decision_makers: tuple[DecisionMakerEvidence, ...],
    contacts: tuple[PublicContact, ...],
    score_input: QualificationScoreInputV2,
    evidence_by_id: dict[UUID, EvidenceEnvelope],
) -> tuple[str, ...]:
    organization_ids = set(organization.evidence_ids)
    organization_ids.update(
        evidence_id
        for identifier in organization.identifiers
        for evidence_id in identifier.evidence_ids
    )
    demand_ids = {
        evidence_id
        for item in demand_signals
        for evidence_id in item.evidence_ids
    }
    decision_access_ids = {
        evidence_id
        for item in (*decision_makers, *contacts)
        for evidence_id in item.evidence_ids
    }
    all_gate_ids = organization_ids | demand_ids | decision_access_ids
    decision_maker_ids = {
        evidence_id
        for item in decision_makers
        for evidence_id in item.evidence_ids
    }
    contact_ids = {
        evidence_id
        for item in contacts
        for evidence_id in item.evidence_ids
    }
    expected = {
        "company_fit": organization_ids,
        "pain_strength": demand_ids,
        "timing_readiness": demand_ids,
        "decision_maker_access": decision_access_ids,
        "evidence_quality": all_gate_ids,
    }
    failures: list[str] = []
    for field_name, allowed_ids in expected.items():
        factor = getattr(score_input, field_name)
        if (
            factor.value is not None
            and factor.value > 0
            and (
                not set(factor.evidence_ids).issubset(allowed_ids)
                or (
                    field_name == "company_fit"
                    and not any(
                        evidence_id in evidence_by_id
                        and evidence_by_id[evidence_id].claim_type
                        in {"business_profile", "organization_profile"}
                        and _has_fact(
                            evidence_by_id[evidence_id],
                            "business.vertical",
                            organization.vertical.value,
                        )
                        for evidence_id in factor.evidence_ids
                    )
                )
                or (
                    field_name == "decision_maker_access"
                    and (
                        not set(factor.evidence_ids) & decision_maker_ids
                        or not set(factor.evidence_ids) & contact_ids
                    )
                )
                or (
                    field_name == "evidence_quality"
                    and set(factor.evidence_ids) != all_gate_ids
                )
            )
        ):
            failures.append(field_name)
    return tuple(failures)


def _fns_evidence_supports_legal_identity_and_active_status(
    identifiers: tuple[OrganizationIdentifier, ...],
    evidence_by_id: dict[UUID, EvidenceEnvelope],
    current_gate_evidence: tuple[EvidenceEnvelope, ...],
) -> bool:
    if not identifiers:
        return False
    selected_identifier = next(
        (
            identifier
            for kind in (IdentifierKind.INN, IdentifierKind.OGRN)
            for identifier in identifiers
            if identifier.kind is kind
        ),
        None,
    )
    if selected_identifier is None:
        return False
    current_ids = {item.evidence_id for item in current_gate_evidence}
    if len(identifiers) > 1:
        common_evidence_ids = set(identifiers[0].evidence_ids)
        for identifier in identifiers[1:]:
            common_evidence_ids.intersection_update(identifier.evidence_ids)
        return any(
            evidence_id in current_ids
            and evidence_id in evidence_by_id
            and evidence_by_id[evidence_id].source_kind
            is EvidenceSourceKind.OFFICIAL_REGISTRY
            and evidence_by_id[evidence_id].source_family == "fns"
            and _is_fns_host(evidence_by_id[evidence_id].source_url)
            and evidence_by_id[evidence_id].claim_type
            in {"legal_identity", "legal_status"}
            and all(
                _has_fact(
                    evidence_by_id[evidence_id],
                    f"organization.{identifier.kind.value}",
                    identifier.value,
                )
                for identifier in identifiers
            )
            and _has_fact(
                evidence_by_id[evidence_id],
                "legal.status",
                "active",
            )
            for evidence_id in common_evidence_ids
        )
    return any(
        evidence_id in current_ids
        and evidence_id in evidence_by_id
        and evidence_by_id[evidence_id].source_kind
        is EvidenceSourceKind.OFFICIAL_REGISTRY
        and evidence_by_id[evidence_id].source_family == "fns"
        and _is_fns_host(evidence_by_id[evidence_id].source_url)
        and evidence_by_id[evidence_id].claim_type
        in {"legal_identity", "legal_status"}
        and _has_fact(
            evidence_by_id[evidence_id],
            f"organization.{selected_identifier.kind.value}",
            selected_identifier.value,
        )
        and _has_fact(evidence_by_id[evidence_id], "legal.status", "active")
        for evidence_id in selected_identifier.evidence_ids
    )


def _has_fact(evidence: EvidenceEnvelope, name: str, value: str) -> bool:
    expected = value.casefold()
    return any(
        fact.name == name and fact.value.casefold() == expected
        for fact in evidence.facts
    )


def _evidence_set_has_facts(
    evidence_ids: tuple[UUID, ...],
    evidence_by_id: dict[UUID, EvidenceEnvelope],
    expected_facts: dict[str, str],
) -> bool:
    return all(
        any(
            evidence_id in evidence_by_id
            and _has_fact(evidence_by_id[evidence_id], name, value)
            for evidence_id in evidence_ids
        )
        for name, value in expected_facts.items()
    )


def _canonical_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == 0:
        return "0"
    return format(normalized, "f")


def _identifier_fact_name(kind: IdentifierKind) -> str:
    return {
        IdentifierKind.INN: "organization.inn",
        IdentifierKind.OGRN: "organization.ogrn",
        IdentifierKind.KPP: "organization.kpp",
        IdentifierKind.DOMAIN: "business.domain",
        IdentifierKind.TWO_GIS_ID: "source.record_id",
        IdentifierKind.YANDEX_MAPS_ID: "source.record_id",
    }[kind]


def _identifier_evidence_failures(
    identifiers: tuple[OrganizationIdentifier, ...],
    evidence_by_id: dict[UUID, EvidenceEnvelope],
    current_gate_evidence_ids: set[UUID],
) -> tuple[str, ...]:
    failures: list[str] = []
    legal_identifiers = tuple(
        identifier
        for identifier in identifiers
        if identifier.kind in {IdentifierKind.INN, IdentifierKind.OGRN}
    )
    for identifier in identifiers:
        matching = tuple(
            evidence_by_id[evidence_id]
            for evidence_id in identifier.evidence_ids
            if evidence_id in current_gate_evidence_ids
            and evidence_id in evidence_by_id
            and _has_fact(
                evidence_by_id[evidence_id],
                _identifier_fact_name(identifier.kind),
                identifier.value,
            )
        )
        needs_legal_link = identifier.kind not in {
            IdentifierKind.INN,
            IdentifierKind.OGRN,
        }
        if (
            not matching
            or (
                identifier.kind in {IdentifierKind.INN, IdentifierKind.OGRN}
                and not any(
                    evidence.source_kind
                    is EvidenceSourceKind.OFFICIAL_REGISTRY
                    and evidence.source_family == "fns"
                    and _is_fns_host(evidence.source_url)
                    for evidence in matching
                )
            )
            or (
                identifier.kind
                in {IdentifierKind.TWO_GIS_ID, IdentifierKind.YANDEX_MAPS_ID}
                and not any(
                    _provider_evidence_matches_identifier(evidence, identifier)
                    for evidence in matching
                )
            )
            or (
                needs_legal_link
                and not any(
                    any(
                        _has_fact(
                            evidence,
                            _identifier_fact_name(legal_identifier.kind),
                            legal_identifier.value,
                        )
                        for legal_identifier in legal_identifiers
                    )
                    for evidence in matching
                )
            )
        ):
            failures.append(f"{identifier.kind.value}:{identifier.value}")
    return tuple(failures)


def _single_valued_fact_conflicts(
    evidence: tuple[EvidenceEnvelope, ...],
) -> tuple[str, ...]:
    values: dict[tuple[str, UUID, str], set[str]] = {}
    for envelope in evidence:
        for fact in envelope.facts:
            if fact.name not in _SINGLE_VALUED_FACT_NAMES:
                continue
            key = (envelope.subject_type, envelope.subject_id, fact.name)
            values.setdefault(key, set()).add(fact.value.casefold())
    return tuple(
        f"{subject_type}:{subject_id}:{fact_name}"
        for (subject_type, subject_id, fact_name), fact_values in values.items()
        if len(fact_values) > 1
    )


def _fns_legal_evidence_conflicts(
    identifiers: tuple[OrganizationIdentifier, ...],
    current_gate_evidence: tuple[EvidenceEnvelope, ...],
) -> tuple[str, ...]:
    expected = {
        _identifier_fact_name(identifier.kind): identifier.value.casefold()
        for identifier in identifiers
    }
    failures: list[str] = []
    for evidence in current_gate_evidence:
        if not (
            evidence.source_kind is EvidenceSourceKind.OFFICIAL_REGISTRY
            and _is_fns_host(evidence.source_url)
        ):
            continue
        for fact in evidence.facts:
            if fact.name == "legal.status" and fact.value.casefold() != "active":
                failures.append(f"{evidence.evidence_id}:legal.status")
            if fact.name in {"organization.inn", "organization.ogrn"} and (
                expected.get(fact.name) != fact.value.casefold()
            ):
                failures.append(f"{evidence.evidence_id}:{fact.name}")
    return tuple(failures)


def _verified_organization_domains(
    organization: Organization,
    identifiers: tuple[OrganizationIdentifier, ...],
    evidence_by_id: dict[UUID, EvidenceEnvelope],
    current_gate_evidence_ids: set[UUID],
) -> set[str]:
    domains = {
        identifier.value
        for identifier in identifiers
        if identifier.kind is IdentifierKind.DOMAIN
        and any(
            evidence_id in current_gate_evidence_ids
            and evidence_id in evidence_by_id
            and _has_fact(
                evidence_by_id[evidence_id],
                "business.domain",
                identifier.value,
            )
            for evidence_id in identifier.evidence_ids
        )
    }
    if organization.website_url is not None:
        website_host = urlsplit(organization.website_url).hostname or ""
        try:
            website_domain = normalize_identifier(
                IdentifierKind.DOMAIN,
                website_host,
            )
        except ValueError:
            return set()
        if website_domain not in domains:
            return set()
    return domains


def _organization_has_verified_company_fit(
    organization: Organization,
    identifiers: tuple[OrganizationIdentifier, ...],
    verified_domains: set[str],
    evidence_by_id: dict[UUID, EvidenceEnvelope],
    current_gate_evidence_ids: set[UUID],
) -> bool:
    return any(
        evidence_id in current_gate_evidence_ids
        and evidence_id in evidence_by_id
        and evidence_by_id[evidence_id].claim_type
        in {"business_profile", "organization_profile"}
        and evidence_by_id[evidence_id].source_kind
        in {
            EvidenceSourceKind.BUSINESS_DIRECTORY,
            EvidenceSourceKind.OFFICIAL_WEBSITE,
            EvidenceSourceKind.PUBLIC_BUSINESS_PROFILE,
        }
        and _has_fact(
            evidence_by_id[evidence_id],
            "business.vertical",
            organization.vertical.value,
        )
        and _evidence_is_bound_to_organization(
            evidence_by_id[evidence_id],
            identifiers,
            verified_domains,
        )
        for evidence_id in organization.evidence_ids
    )


def _organization_evidence_binding_failures(
    organization: Organization,
    identifiers: tuple[OrganizationIdentifier, ...],
    verified_domains: set[str],
    evidence_by_id: dict[UUID, EvidenceEnvelope],
    current_gate_evidence_ids: set[UUID],
) -> tuple[str, ...]:
    organization_evidence_ids = set(organization.evidence_ids)
    organization_evidence_ids.update(
        evidence_id
        for identifier in identifiers
        for evidence_id in identifier.evidence_ids
    )
    return tuple(
        str(evidence_id)
        for evidence_id in organization_evidence_ids
        if evidence_id in current_gate_evidence_ids
        and evidence_id in evidence_by_id
        and not _evidence_is_bound_to_organization(
            evidence_by_id[evidence_id],
            identifiers,
            verified_domains,
        )
    )


def _provider_evidence_matches_identifier(
    evidence: EvidenceEnvelope,
    identifier: OrganizationIdentifier,
) -> bool:
    host = _normalized_url_host(evidence.source_url)
    if identifier.kind is IdentifierKind.TWO_GIS_ID:
        return (
            host == "2gis.ru"
            or host.endswith(".2gis.ru")
            or host == "2gis.com"
            or host.endswith(".2gis.com")
        ) and evidence.external_record_id == identifier.value
    if identifier.kind is IdentifierKind.YANDEX_MAPS_ID:
        return (
            host in {"yandex.ru", "yandex.com"}
            or host.endswith(".yandex.ru")
            or host.endswith(".yandex.com")
        ) and evidence.external_record_id == identifier.value
    return False


def _evidence_is_bound_to_organization(
    evidence: EvidenceEnvelope,
    identifiers: tuple[OrganizationIdentifier, ...],
    verified_domains: set[str],
) -> bool:
    exact_identity_fact = any(
        _has_fact(
            evidence,
            _identifier_fact_name(identifier.kind),
            identifier.value,
        )
        for identifier in identifiers
        if identifier.kind
        in {
            IdentifierKind.INN,
            IdentifierKind.OGRN,
            IdentifierKind.DOMAIN,
        }
    )
    if exact_identity_fact:
        return True

    host = _normalized_url_host(evidence.source_url)
    if evidence.source_kind is EvidenceSourceKind.OFFICIAL_WEBSITE:
        return any(
            host == domain or host.endswith(f".{domain}")
            for domain in verified_domains
        )

    if evidence.source_kind in {
        EvidenceSourceKind.BUSINESS_DIRECTORY,
        EvidenceSourceKind.PUBLIC_BUSINESS_PROFILE,
    }:
        return any(
            _provider_evidence_matches_identifier(evidence, identifier)
            for identifier in identifiers
            if identifier.kind
            in {IdentifierKind.TWO_GIS_ID, IdentifierKind.YANDEX_MAPS_ID}
        )

    if evidence.source_kind is EvidenceSourceKind.PUBLIC_JOB_POSTING:
        return any(
            host == domain or host.endswith(f".{domain}")
            for domain in verified_domains
        )
    return False


def _entity_source_binding_failures(
    demand_signals: tuple[DemandSignal, ...],
    decision_makers: tuple[DecisionMakerEvidence, ...],
    contacts: tuple[PublicContact, ...],
    identifiers: tuple[OrganizationIdentifier, ...],
    verified_domains: set[str],
    evidence_by_id: dict[UUID, EvidenceEnvelope],
    current_gate_evidence_ids: set[UUID],
) -> tuple[str, ...]:
    failures: list[str] = []
    for item in (*demand_signals, *decision_makers, *contacts):
        bound_ids = tuple(
            evidence_id
            for evidence_id in item.evidence_ids
            if evidence_id in current_gate_evidence_ids
            and evidence_id in evidence_by_id
            and _evidence_is_bound_to_organization(
                evidence_by_id[evidence_id],
                identifiers,
                verified_domains,
            )
        )
        if isinstance(item, DemandSignal):
            required_facts = {
                "demand.kind": item.kind.value,
                "demand.strength": _canonical_decimal(item.strength),
                "demand.summary_sha256": sha256(
                    item.summary.encode("utf-8")
                ).hexdigest(),
                "demand.timing_readiness": _canonical_decimal(
                    item.timing_readiness
                ),
            }
        elif isinstance(item, DecisionMakerEvidence):
            required_facts = {
                "role.name": item.asserted_name or "",
                "role.seniority": item.seniority.value,
                "role.title": item.asserted_title,
            }
            if item.responsibility:
                required_facts["role.responsibility"] = item.responsibility
        else:
            required_facts = {
                "contact.channel": item.channel.value,
                "contact.scope": item.scope.value,
                "contact.value_sha256": sha256(
                    item.value.encode("utf-8")
                ).hexdigest(),
            }
            if item.decision_maker_id is not None:
                required_facts["contact.decision_maker_id"] = str(
                    item.decision_maker_id
                )
                decision_maker = next(
                    (
                        candidate
                        for candidate in decision_makers
                        if candidate.decision_maker_id
                        == item.decision_maker_id
                    ),
                    None,
                )
                if decision_maker is not None and decision_maker.asserted_name:
                    required_facts[
                        "contact.decision_maker_name_sha256"
                    ] = _name_sha256(decision_maker.asserted_name)
        if set(bound_ids) != set(item.evidence_ids) or not _evidence_set_has_facts(
            bound_ids,
            evidence_by_id,
            required_facts,
        ):
            failures.append(str(item))
    return tuple(failures)


def _decision_maker_has_business_authority_evidence(
    decision_maker: DecisionMakerEvidence,
    identifiers: tuple[OrganizationIdentifier, ...],
    verified_domains: set[str],
    evidence_by_id: dict[UUID, EvidenceEnvelope],
    current_gate_evidence_ids: set[UUID],
) -> bool:
    return any(
        evidence_id in current_gate_evidence_ids
        and evidence_id in evidence_by_id
        and evidence_by_id[evidence_id].source_kind
        in {
            EvidenceSourceKind.OFFICIAL_WEBSITE,
            EvidenceSourceKind.PUBLIC_BUSINESS_PROFILE,
        }
        and not _is_fns_host(evidence_by_id[evidence_id].source_url)
        and _evidence_is_bound_to_organization(
            evidence_by_id[evidence_id],
            identifiers,
            verified_domains,
        )
        for evidence_id in decision_maker.evidence_ids
    )


def _direct_contact_decision_maker_ids(
    contacts: tuple[PublicContact, ...],
    decision_makers: tuple[DecisionMakerEvidence, ...],
    evidence_by_id: dict[UUID, EvidenceEnvelope],
    current_gate_evidence_ids: set[UUID],
) -> set[UUID]:
    decision_maker_by_id = {
        item.decision_maker_id: item for item in decision_makers
    }
    direct_ids: set[UUID] = set()
    for contact in contacts:
        if (
            contact.scope is not ContactScope.NAMED_BUSINESS_PERSON
            or contact.decision_maker_id not in decision_maker_by_id
            or contact.channel is ContactChannel.WEBSITE_FORM
            or _is_generic_named_contact_value(contact)
        ):
            continue
        decision_maker = decision_maker_by_id[contact.decision_maker_id]
        assert decision_maker.asserted_name is not None
        required_facts = {
            "contact.channel": contact.channel.value,
            "contact.scope": contact.scope.value,
            "contact.value_sha256": sha256(
                contact.value.encode("utf-8")
            ).hexdigest(),
            "contact.decision_maker_id": str(contact.decision_maker_id),
            "contact.decision_maker_name_sha256": _name_sha256(
                decision_maker.asserted_name
            ),
        }
        if _evidence_set_has_facts(
            tuple(
                evidence_id
                for evidence_id in contact.evidence_ids
                if evidence_id in current_gate_evidence_ids
            ),
            evidence_by_id,
            required_facts,
        ):
            direct_ids.add(contact.decision_maker_id)
    return direct_ids


def _is_generic_named_contact_value(contact: PublicContact) -> bool:
    if contact.channel is not ContactChannel.BUSINESS_EMAIL:
        return False
    local_part = contact.value.split("@", 1)[0].casefold().split("+", 1)[0]
    return local_part in _GENERIC_EMAIL_LOCAL_PARTS


def _name_sha256(value: str) -> str:
    normalized = " ".join(value.casefold().split())
    return sha256(normalized.encode("utf-8")).hexdigest()


def _source_independence_key(evidence: EvidenceEnvelope) -> str:
    host = _normalized_url_host(evidence.source_url)
    if _is_fns_host(evidence.source_url):
        return "fns"
    if host == "2gis.ru" or host.endswith(".2gis.ru"):
        return "two_gis"
    if host == "2gis.com" or host.endswith(".2gis.com"):
        return "two_gis"
    if (
        host in {"yandex.ru", "yandex.com"}
        or host.endswith(".yandex.ru")
        or host.endswith(".yandex.com")
    ):
        return "yandex_maps"
    return f"web:{host}"


def _is_fns_host(source_url: str) -> bool:
    host = _normalized_url_host(source_url)
    return host == "nalog.ru" or host.endswith(".nalog.ru")


def _normalized_url_host(source_url: str) -> str:
    host = (urlsplit(source_url).hostname or "").casefold()
    try:
        return normalize_identifier(IdentifierKind.DOMAIN, host)
    except ValueError:
        return host


def _score_factor_value_failures(
    demand_signals: tuple[DemandSignal, ...],
    decision_makers: tuple[DecisionMakerEvidence, ...],
    contacts: tuple[PublicContact, ...],
    score_input: QualificationScoreInputV2,
    evidence_by_id: dict[UUID, EvidenceEnvelope],
    verified_direct_decision_maker_ids: set[UUID],
) -> tuple[str, ...]:
    failures: list[str] = []
    pain_evidence_ids = set(score_input.pain_strength.evidence_ids)
    timing_evidence_ids = set(score_input.timing_readiness.evidence_ids)
    decision_access_evidence_ids = set(
        score_input.decision_maker_access.evidence_ids
    )
    cited_pain_signals = tuple(
        item
        for item in demand_signals
        if set(item.evidence_ids) & pain_evidence_ids
    )
    cited_timing_signals = tuple(
        item
        for item in demand_signals
        if set(item.evidence_ids) & timing_evidence_ids
    )
    cited_contacts = tuple(
        item
        for item in contacts
        if set(item.evidence_ids) & decision_access_evidence_ids
    )
    cited_contact_decision_maker_ids = {
        item.decision_maker_id
        for item in cited_contacts
        if item.decision_maker_id is not None
    }
    cited_decision_makers = tuple(
        item
        for item in decision_makers
        if set(item.evidence_ids) & decision_access_evidence_ids
        or item.decision_maker_id in cited_contact_decision_maker_ids
    )
    maximum_pain = max(
        (item.strength for item in cited_pain_signals),
        default=Decimal("0"),
    )
    maximum_timing = max(
        (item.timing_readiness for item in cited_timing_signals),
        default=Decimal("0"),
    )
    cited_direct_decision_maker_ids = {
        item.decision_maker_id
        for item in cited_contacts
        if item.decision_maker_id in verified_direct_decision_maker_ids
    }
    maximum_access = max(
        (
            min(
                item.authority_score,
                item.confidence,
                (
                    Decimal("1")
                    if item.decision_maker_id
                    in cited_direct_decision_maker_ids
                    else Decimal("0.5")
                ),
            )
            for item in cited_decision_makers
        ),
        default=Decimal("0"),
    )
    factual_caps = {
        "pain_strength": maximum_pain,
        "timing_readiness": maximum_timing,
        "decision_maker_access": maximum_access,
    }
    for field_name in (
        "company_fit",
        "pain_strength",
        "timing_readiness",
        "decision_maker_access",
        "evidence_quality",
    ):
        factor = getattr(score_input, field_name)
        if factor.value is None or factor.value == 0:
            continue
        evidence_confidence = min(
            (
                evidence_by_id[evidence_id].confidence
                for evidence_id in factor.evidence_ids
                if evidence_id in evidence_by_id
            ),
            default=Decimal("0"),
        )
        allowed_value = min(
            evidence_confidence,
            factual_caps.get(field_name, Decimal("1")),
        )
        if factor.value > allowed_value:
            failures.append(field_name)
    return tuple(failures)


def _all_referenced_evidence_ids(
    organization: Organization,
    demand_signals: tuple[DemandSignal, ...],
    decision_makers: tuple[DecisionMakerEvidence, ...],
    contacts: tuple[PublicContact, ...],
    score_input: QualificationScoreInputV2,
) -> set[UUID]:
    referenced = set(organization.evidence_ids)
    for identifier in organization.identifiers:
        referenced.update(identifier.evidence_ids)
    for item in (*demand_signals, *decision_makers, *contacts):
        referenced.update(item.evidence_ids)
    referenced.update(score_factor_evidence_ids(score_input))
    return referenced


def _gate_evidence_ids(
    organization: Organization,
    demand_signals: tuple[DemandSignal, ...],
    decision_makers: tuple[DecisionMakerEvidence, ...],
    contacts: tuple[PublicContact, ...],
    score_input: QualificationScoreInputV2,
) -> set[UUID]:
    referenced = set(organization.evidence_ids)
    referenced.update(
        evidence_id
        for identifier in organization.identifiers
        if identifier.verification_state is VerificationState.VERIFIED
        for evidence_id in identifier.evidence_ids
    )
    for item in (*demand_signals, *decision_makers, *contacts):
        referenced.update(item.evidence_ids)
    referenced.update(score_factor_evidence_ids(score_input))
    return referenced


def _require_aware(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime values must be timezone-aware")


def _factor_at_least(value: Decimal | None, minimum: Decimal) -> bool:
    return value is not None and value >= minimum

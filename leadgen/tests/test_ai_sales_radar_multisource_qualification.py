from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
import unittest
from uuid import UUID

from pydantic import ValidationError

from ai_sales_radar.multisource.identity import IdentifierKind
from ai_sales_radar.multisource.models import (
    BusinessVertical,
    CollectionBasis,
    ContactChannel,
    ContactScope,
    DecisionMakerEvidence,
    DecisionMakerSeniority,
    DemandSignal,
    DemandSignalKind,
    EvidenceEnvelope,
    EvidenceFact,
    EvidenceSourceKind,
    EvidenceStoragePolicy,
    Organization,
    OrganizationIdentifier,
    OrganizationLegalStatus,
    OrganizationLifecycle,
    PublicContact,
    VerificationState,
    calculate_facts_sha256,
)
from ai_sales_radar.multisource.qualification import (
    LeadPriority,
    QualificationDisposition,
    QualifiedLead,
    evaluate_qualification,
)
from ai_sales_radar.multisource.scoring import (
    QualificationScoreInputV2,
    ScoreFactor,
    calculate_qualification_score_v2,
)


NOW = datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc)
ORG_ID = UUID("10000000-0000-0000-0000-000000000001")
LEAD_ID = UUID("30000000-0000-0000-0000-000000000001")
SIGNAL_ID = UUID("31000000-0000-0000-0000-000000000001")
DM_ID = UUID("40000000-0000-0000-0000-000000000001")
CONTACT_ID = UUID("41000000-0000-0000-0000-000000000001")
ORG_EVIDENCE_ID = UUID("20000000-0000-0000-0000-000000000001")
PROFILE_EVIDENCE_ID = UUID("20000000-0000-0000-0000-000000000002")
DEMAND_EVIDENCE_ID = UUID("20000000-0000-0000-0000-000000000003")
DM_EVIDENCE_ID = UUID("20000000-0000-0000-0000-000000000004")
CONTACT_EVIDENCE_ID = UUID("20000000-0000-0000-0000-000000000005")
EXTRA_EVIDENCE_ID = UUID("20000000-0000-0000-0000-000000000006")
WEAK_SIGNAL_ID = UUID("31000000-0000-0000-0000-000000000002")
WEAK_EVIDENCE_ID = UUID("20000000-0000-0000-0000-000000000007")


class MultisourceQualificationTest(unittest.TestCase):
    def test_complete_verified_lead_qualifies_for_manual_review(self):
        result = evaluate_qualification(**self._context())

        self.assertEqual(result.disposition, QualificationDisposition.QUALIFIED)
        self.assertEqual(result.gate_failures, ())
        self.assertIsNotNone(result.qualified_lead)
        assert result.qualified_lead is not None
        self.assertEqual(result.qualified_lead.priority, LeadPriority.HOT)
        self.assertTrue(result.qualified_lead.manual_review_required)
        self.assertEqual(result.qualified_lead.dedupe_key, "inn:7707083893")

    def test_high_score_without_decision_maker_stays_in_review(self):
        context = self._context()
        context["decision_makers"] = ()

        result = evaluate_qualification(**context)

        self.assertEqual(result.disposition, QualificationDisposition.REVIEW)
        self.assertIn(
            "verified_decision_maker_authority_missing",
            result.gate_failures,
        )

    def test_expired_demand_signal_cannot_qualify(self):
        context = self._context()
        context["as_of"] = NOW + timedelta(days=31)

        result = evaluate_qualification(**context)

        self.assertEqual(result.disposition, QualificationDisposition.REVIEW)
        self.assertIn(
            "current_verified_demand_signal_missing",
            result.gate_failures,
        )

    def test_missing_or_unreferenced_evidence_cannot_qualify(self):
        missing = self._context()
        missing["evidence"] = tuple(missing["evidence"])[1:]
        missing_result = evaluate_qualification(**missing)

        extra = self._context()
        extra_evidence = self._evidence(
            evidence_id=EXTRA_EVIDENCE_ID,
            subject_type="organization",
            subject_id=ORG_ID,
            source_kind=EvidenceSourceKind.OFFICIAL_WEBSITE,
            source_family="company_website",
            claim_type="business_profile",
        )
        extra["evidence"] = (*tuple(extra["evidence"]), extra_evidence)
        extra_result = evaluate_qualification(**extra)

        self.assertIn("evidence_closure_incomplete", missing_result.gate_failures)
        self.assertIn(
            "unreferenced_evidence_not_allowed",
            extra_result.gate_failures,
        )

    def test_cross_subject_or_wrong_claim_evidence_cannot_be_reused(self):
        context = self._context()
        evidence = list(context["evidence"])
        demand = evidence[2]
        evidence[2] = demand.model_copy(
            update={"subject_id": ORG_ID, "claim_type": "legal_status"}
        )
        context["evidence"] = tuple(evidence)

        result = evaluate_qualification(**context)

        self.assertEqual(result.disposition, QualificationDisposition.REVIEW)
        self.assertIn("evidence_subject_mismatch", result.gate_failures)

    def test_domain_only_identity_never_replaces_fns_verification(self):
        context = self._context()
        organization = context["organization"]
        domain_identifier = OrganizationIdentifier(
            kind=IdentifierKind.DOMAIN,
            value="example.ru",
            verification_state=VerificationState.VERIFIED,
            evidence_ids=(ORG_EVIDENCE_ID,),
            verified_at=NOW,
        )
        context["organization"] = organization.model_copy(
            update={"identifiers": (domain_identifier,)}
        )

        result = evaluate_qualification(**context)

        self.assertEqual(result.disposition, QualificationDisposition.REVIEW)
        self.assertIn("verified_fns_identity_missing", result.gate_failures)

    def test_future_evidence_or_one_source_family_cannot_qualify(self):
        future = self._context()
        evidence = list(future["evidence"])
        evidence[2] = evidence[2].model_copy(
            update={"verified_at": NOW + timedelta(days=1)}
        )
        future["evidence"] = tuple(evidence)
        future_result = evaluate_qualification(**future)

        one_family = self._context()
        one_family["evidence"] = tuple(
            item.model_copy(
                update={
                    "source_family": "fns",
                    "source_url": (
                        f"https://egrul.nalog.ru/evidence/{item.evidence_id}"
                    ),
                }
            )
            for item in one_family["evidence"]
        )
        one_family_result = evaluate_qualification(**one_family)

        self.assertIn(
            "verified_current_evidence_missing",
            future_result.gate_failures,
        )
        self.assertIn(
            "independent_source_families_missing",
            one_family_result.gate_failures,
        )

    def test_score_cannot_exceed_verified_demand_or_authority(self):
        weak_demand = self._context()
        signal = tuple(weak_demand["demand_signals"])[0]
        weak_demand["demand_signals"] = (
            signal.model_copy(update={"strength": Decimal("0.2")}),
        )
        weak_result = evaluate_qualification(**weak_demand)

        weak_lpr = self._context()
        decision_maker = tuple(weak_lpr["decision_makers"])[0]
        weak_lpr["decision_makers"] = (
            decision_maker.model_copy(update={"authority_score": Decimal("0.2")}),
        )
        weak_lpr_result = evaluate_qualification(**weak_lpr)

        self.assertIn(
            "score_factor_exceeds_verified_facts",
            weak_result.gate_failures,
        )
        self.assertIn(
            "verified_decision_maker_authority_missing",
            weak_lpr_result.gate_failures,
        )

    def test_score_cap_uses_the_signal_evidence_actually_cited(self):
        context = self._context()
        weak_summary = "Часть заявок обрабатывается вручную"
        weak_evidence = self._evidence(
            evidence_id=WEAK_EVIDENCE_ID,
            subject_type="demand_signal",
            subject_id=WEAK_SIGNAL_ID,
            source_kind=EvidenceSourceKind.OFFICIAL_WEBSITE,
            source_family="company_website",
            claim_type="operational_gap",
            facts=(
                EvidenceFact(name="demand.kind", value="lead_handling_gap"),
                EvidenceFact(name="demand.strength", value="0.5"),
                EvidenceFact(
                    name="demand.summary_sha256",
                    value=sha256(weak_summary.encode("utf-8")).hexdigest(),
                ),
                EvidenceFact(name="demand.timing_readiness", value="0.5"),
            ),
        )
        weak_signal = DemandSignal(
            signal_id=WEAK_SIGNAL_ID,
            organization_id=ORG_ID,
            kind=DemandSignalKind.LEAD_HANDLING_GAP,
            summary=weak_summary,
            strength=Decimal("0.5"),
            timing_readiness=Decimal("0.5"),
            occurred_at=NOW,
            valid_until=NOW + timedelta(days=30),
            evidence_ids=(WEAK_EVIDENCE_ID,),
            verification_state=VerificationState.VERIFIED,
            verified_at=NOW,
        )
        original_score_input = context["score_input"]
        score_input = original_score_input.model_copy(
            update={
                "pain_strength": self._factor((WEAK_EVIDENCE_ID,)),
                "timing_readiness": self._factor((WEAK_EVIDENCE_ID,)),
                "evidence_quality": self._factor(
                    (*original_score_input.evidence_quality.evidence_ids, WEAK_EVIDENCE_ID)
                ),
            }
        )
        context["demand_signals"] = (
            *tuple(context["demand_signals"]),
            weak_signal,
        )
        context["evidence"] = (*tuple(context["evidence"]), weak_evidence)
        context["score_input"] = score_input
        context["score"] = calculate_qualification_score_v2(score_input)

        result = evaluate_qualification(**context)

        self.assertIn(
            "score_factor_exceeds_verified_facts",
            result.gate_failures,
        )

    def test_critical_evidence_requires_minimum_confidence(self):
        context = self._context()
        evidence = list(context["evidence"])
        evidence[-1] = evidence[-1].model_copy(
            update={"confidence": Decimal("0.4")}
        )
        context["evidence"] = tuple(evidence)

        result = evaluate_qualification(**context)

        self.assertIn("verified_current_evidence_missing", result.gate_failures)

    def test_hot_priority_requires_a_direct_lpr_contact(self):
        context = self._context()
        contact = tuple(context["contacts"])[0]
        context["contacts"] = (
            contact.model_copy(
                update={
                    "decision_maker_id": None,
                    "scope": ContactScope.ORGANIZATION,
                }
            ),
        )
        evidence = list(context["evidence"])
        organization_contact_facts = (
            EvidenceFact(name="contact.channel", value="business_email"),
            EvidenceFact(name="contact.scope", value="organization"),
            EvidenceFact(
                name="contact.value_sha256",
                value=sha256(b"contact018@example.invalid").hexdigest(),
            ),
        )
        evidence[4] = evidence[4].model_copy(
            update={
                "facts": organization_contact_facts,
                "facts_sha256": calculate_facts_sha256(
                    organization_contact_facts
                ),
            }
        )
        context["evidence"] = tuple(evidence)
        score_input = context["score_input"].model_copy(
            update={
                "decision_maker_access": ScoreFactor(
                    value=Decimal("0.5"),
                    evidence_ids=(DM_EVIDENCE_ID, CONTACT_EVIDENCE_ID),
                    reason_codes=("generic_business_contact",),
                )
            }
        )
        context["score_input"] = score_input
        context["score"] = calculate_qualification_score_v2(score_input)

        result = evaluate_qualification(**context)

        self.assertEqual(result.disposition, QualificationDisposition.QUALIFIED)
        assert result.qualified_lead is not None
        self.assertEqual(result.qualified_lead.priority, LeadPriority.QUALIFIED)

    def test_generic_email_cannot_be_promoted_to_direct_lpr_access(self):
        context = self._context()
        contact = tuple(context["contacts"])[0]
        context["contacts"] = (
            contact.model_copy(update={"value": "contact017@example.invalid"}),
        )
        evidence = list(context["evidence"])
        generic_contact_facts = (
            EvidenceFact(name="contact.channel", value="business_email"),
            EvidenceFact(
                name="contact.scope",
                value="named_business_person",
            ),
            EvidenceFact(
                name="contact.value_sha256",
                value=sha256(b"contact017@example.invalid").hexdigest(),
            ),
            EvidenceFact(name="contact.decision_maker_id", value=str(DM_ID)),
            EvidenceFact(
                name="contact.decision_maker_name_sha256",
                value=sha256("иван иванов".encode("utf-8")).hexdigest(),
            ),
        )
        evidence[4] = evidence[4].model_copy(
            update={
                "facts": generic_contact_facts,
                "facts_sha256": calculate_facts_sha256(
                    generic_contact_facts
                ),
            }
        )
        context["evidence"] = tuple(evidence)
        score_input = context["score_input"].model_copy(
            update={
                "decision_maker_access": ScoreFactor(
                    value=Decimal("0.5"),
                    evidence_ids=(DM_EVIDENCE_ID, CONTACT_EVIDENCE_ID),
                    reason_codes=("generic_business_contact",),
                )
            }
        )
        context["score_input"] = score_input
        context["score"] = calculate_qualification_score_v2(score_input)

        result = evaluate_qualification(**context)

        self.assertEqual(result.disposition, QualificationDisposition.QUALIFIED)
        assert result.qualified_lead is not None
        self.assertEqual(result.qualified_lead.priority, LeadPriority.QUALIFIED)

    def test_conflicting_fns_statuses_are_fail_closed(self):
        context = self._context()
        evidence = list(context["evidence"])
        conflicting_id = EXTRA_EVIDENCE_ID
        conflicting = self._evidence(
            evidence_id=conflicting_id,
            subject_type="organization",
            subject_id=ORG_ID,
            source_kind=EvidenceSourceKind.OFFICIAL_REGISTRY,
            source_family="fns",
            claim_type="legal_status",
            source_url="https://egrul.nalog.ru/conflict",
            facts=(
                EvidenceFact(name="organization.inn", value="7707083893"),
                EvidenceFact(name="legal.status", value="inactive"),
            ),
        )
        evidence.append(conflicting)
        context["evidence"] = tuple(evidence)
        organization = context["organization"]
        context["organization"] = organization.model_copy(
            update={
                "evidence_ids": (*organization.evidence_ids, conflicting_id)
            }
        )
        score_input = context["score_input"]
        evidence_quality = score_input.evidence_quality.model_copy(
            update={
                "evidence_ids": (
                    *score_input.evidence_quality.evidence_ids,
                    conflicting_id,
                )
            }
        )
        score_input = score_input.model_copy(
            update={"evidence_quality": evidence_quality}
        )
        context["score_input"] = score_input
        context["score"] = calculate_qualification_score_v2(score_input)

        result = evaluate_qualification(**context)

        self.assertIn("conflicting_fns_legal_evidence", result.gate_failures)
        self.assertIn(
            "conflicting_current_evidence_facts",
            result.gate_failures,
        )

    def test_duplicate_entities_and_unverified_identifiers_are_rejected(self):
        duplicate = self._context()
        signal = tuple(duplicate["demand_signals"])[0]
        duplicate["demand_signals"] = (signal, signal)
        duplicate_result = evaluate_qualification(**duplicate)

        unverified = self._context()
        organization = unverified["organization"]
        unverified_ogrn = OrganizationIdentifier(
            kind=IdentifierKind.OGRN,
            value="1027700132195",
            evidence_ids=(PROFILE_EVIDENCE_ID,),
        )
        unverified["organization"] = organization.model_copy(
            update={
                "identifiers": (*organization.identifiers, unverified_ogrn)
            }
        )
        unverified_result = evaluate_qualification(**unverified)

        self.assertIn("duplicate_demand_signal", duplicate_result.gate_failures)
        self.assertIn(
            "unverified_organization_identifier_present",
            unverified_result.gate_failures,
        )

    def test_entity_evidence_must_belong_to_the_verified_company(self):
        context = self._context()
        evidence = list(context["evidence"])
        evidence[2] = evidence[2].model_copy(
            update={"source_url": "https://another-company.ru/request"}
        )
        context["evidence"] = tuple(evidence)

        result = evaluate_qualification(**context)

        self.assertIn(
            "entity_evidence_not_bound_to_organization",
            result.gate_failures,
        )

    def test_verified_company_fit_is_required_even_when_score_is_75(self):
        context = self._context()
        evidence = list(context["evidence"])
        profile_facts = tuple(
            fact
            for fact in evidence[1].facts
            if fact.name != "business.vertical"
        )
        evidence[1] = evidence[1].model_copy(
            update={
                "facts": profile_facts,
                "facts_sha256": calculate_facts_sha256(profile_facts),
            }
        )
        context["evidence"] = tuple(evidence)
        score_input = context["score_input"].model_copy(
            update={
                "company_fit": ScoreFactor(
                    value=Decimal("0"),
                    evidence_ids=(),
                    reason_codes=("not_verified",),
                )
            }
        )
        context["score_input"] = score_input
        context["score"] = calculate_qualification_score_v2(score_input)

        result = evaluate_qualification(**context)

        self.assertEqual(context["score"].total, 75)
        self.assertIn("verified_company_fit_missing", result.gate_failures)

    def test_every_verified_legal_identifier_requires_fns_evidence(self):
        context = self._context()
        evidence = list(context["evidence"])
        profile_facts = (
            *evidence[1].facts,
            EvidenceFact(name="organization.ogrn", value="1027700132195"),
        )
        evidence[1] = evidence[1].model_copy(
            update={
                "facts": profile_facts,
                "facts_sha256": calculate_facts_sha256(profile_facts),
            }
        )
        context["evidence"] = tuple(evidence)
        organization = context["organization"]
        profile_only_ogrn = OrganizationIdentifier(
            kind=IdentifierKind.OGRN,
            value="1027700132195",
            verification_state=VerificationState.VERIFIED,
            evidence_ids=(PROFILE_EVIDENCE_ID,),
            verified_at=NOW,
        )
        context["organization"] = organization.model_copy(
            update={
                "identifiers": (*organization.identifiers, profile_only_ogrn)
            }
        )

        result = evaluate_qualification(**context)

        self.assertIn(
            "organization_identifier_evidence_mismatch",
            result.gate_failures,
        )

    def test_inn_and_ogrn_require_one_fns_record_that_links_them(self):
        context = self._context()
        linked_ogrn = "1027739642281"
        separate_fns = self._evidence(
            evidence_id=EXTRA_EVIDENCE_ID,
            subject_type="organization",
            subject_id=ORG_ID,
            source_kind=EvidenceSourceKind.OFFICIAL_REGISTRY,
            source_family="fns",
            claim_type="legal_identity",
            source_url="https://egrul.nalog.ru/separate-record",
            facts=(
                EvidenceFact(name="organization.ogrn", value=linked_ogrn),
                EvidenceFact(name="legal.status", value="active"),
            ),
        )
        context["evidence"] = (*tuple(context["evidence"]), separate_fns)
        organization = context["organization"]
        separate_ogrn = OrganizationIdentifier(
            kind=IdentifierKind.OGRN,
            value=linked_ogrn,
            verification_state=VerificationState.VERIFIED,
            evidence_ids=(EXTRA_EVIDENCE_ID,),
            verified_at=NOW,
        )
        context["organization"] = organization.model_copy(
            update={
                "identifiers": (*organization.identifiers, separate_ogrn),
                "evidence_ids": (
                    *organization.evidence_ids,
                    EXTRA_EVIDENCE_ID,
                ),
            }
        )
        score_input = context["score_input"]
        evidence_quality = score_input.evidence_quality.model_copy(
            update={
                "evidence_ids": (
                    *score_input.evidence_quality.evidence_ids,
                    EXTRA_EVIDENCE_ID,
                )
            }
        )
        score_input = score_input.model_copy(
            update={"evidence_quality": evidence_quality}
        )
        context["score_input"] = score_input
        context["score"] = calculate_qualification_score_v2(score_input)

        result = evaluate_qualification(**context)

        self.assertIn(
            "current_fns_legal_verification_missing",
            result.gate_failures,
        )

    def test_one_fns_record_can_link_a_valid_inn_and_ogrn(self):
        context = self._context()
        linked_ogrn = "1027739642281"
        evidence = list(context["evidence"])
        linked_facts = (
            *evidence[0].facts,
            EvidenceFact(name="organization.ogrn", value=linked_ogrn),
        )
        evidence[0] = evidence[0].model_copy(
            update={
                "facts": linked_facts,
                "facts_sha256": calculate_facts_sha256(linked_facts),
            }
        )
        context["evidence"] = tuple(evidence)
        organization = context["organization"]
        linked_identifier = OrganizationIdentifier(
            kind=IdentifierKind.OGRN,
            value=linked_ogrn,
            verification_state=VerificationState.VERIFIED,
            evidence_ids=(ORG_EVIDENCE_ID,),
            verified_at=NOW,
        )
        context["organization"] = organization.model_copy(
            update={
                "identifiers": (*organization.identifiers, linked_identifier)
            }
        )

        result = evaluate_qualification(**context)

        self.assertEqual(result.disposition, QualificationDisposition.QUALIFIED)

    def test_unbound_company_fit_evidence_cannot_inflate_the_score(self):
        context = self._context()
        evidence = list(context["evidence"])
        evidence[1] = evidence[1].model_copy(
            update={"confidence": Decimal("0.5")}
        )
        unbound_fit = self._evidence(
            evidence_id=EXTRA_EVIDENCE_ID,
            subject_type="organization",
            subject_id=ORG_ID,
            source_kind=EvidenceSourceKind.BUSINESS_DIRECTORY,
            source_family="unrelated_directory",
            claim_type="business_profile",
            source_url="https://unrelated.example.com/profile",
            facts=(
                EvidenceFact(name="business.vertical", value="coffee_shop"),
            ),
        )
        context["evidence"] = (*tuple(evidence), unbound_fit)
        organization = context["organization"]
        context["organization"] = organization.model_copy(
            update={
                "evidence_ids": (
                    *organization.evidence_ids,
                    EXTRA_EVIDENCE_ID,
                )
            }
        )
        score_input = context["score_input"].model_copy(
            update={
                "company_fit": ScoreFactor(
                    value=Decimal("1"),
                    evidence_ids=(EXTRA_EVIDENCE_ID,),
                    reason_codes=("unbound_fit",),
                ),
                "evidence_quality": ScoreFactor(
                    value=Decimal("0.5"),
                    evidence_ids=(
                        ORG_EVIDENCE_ID,
                        PROFILE_EVIDENCE_ID,
                        DEMAND_EVIDENCE_ID,
                        DM_EVIDENCE_ID,
                        CONTACT_EVIDENCE_ID,
                        EXTRA_EVIDENCE_ID,
                    ),
                    reason_codes=("mixed_quality",),
                ),
            }
        )
        context["score_input"] = score_input
        context["score"] = calculate_qualification_score_v2(score_input)

        result = evaluate_qualification(**context)

        self.assertEqual(context["score"].total, 93)
        self.assertIn(
            "organization_evidence_not_bound_to_organization",
            result.gate_failures,
        )

    def test_fns_evidence_must_support_the_selected_dedupe_identity(self):
        context = self._context()
        organization = context["organization"]
        unsupported_inn = OrganizationIdentifier(
            kind=IdentifierKind.INN,
            value="7707083893",
            verification_state=VerificationState.VERIFIED,
            evidence_ids=(PROFILE_EVIDENCE_ID,),
            verified_at=NOW,
        )
        supported_ogrn = OrganizationIdentifier(
            kind=IdentifierKind.OGRN,
            value="1027700132195",
            verification_state=VerificationState.VERIFIED,
            evidence_ids=(ORG_EVIDENCE_ID,),
            verified_at=NOW,
        )
        context["organization"] = organization.model_copy(
            update={"identifiers": (unsupported_inn, supported_ogrn)}
        )
        evidence = list(context["evidence"])
        fns_evidence = evidence[0]
        facts = (
            EvidenceFact(name="organization.ogrn", value="1027700132195"),
            EvidenceFact(name="legal.status", value="active"),
        )
        evidence[0] = fns_evidence.model_copy(
            update={
                "facts": facts,
                "facts_sha256": calculate_facts_sha256(facts),
            }
        )
        context["evidence"] = tuple(evidence)

        result = evaluate_qualification(**context)

        self.assertIn(
            "current_fns_legal_verification_missing",
            result.gate_failures,
        )

    def test_direct_qualified_lead_construction_rechecks_all_gates(self):
        valid = evaluate_qualification(**self._context()).qualified_lead
        assert valid is not None
        payload = valid.model_dump()
        payload["dedupe_hash"] = "0" * 64

        with self.assertRaises(ValidationError):
            QualifiedLead.model_validate(payload)

    @classmethod
    def _context(cls) -> dict[str, object]:
        evidence = (
            cls._evidence(
                evidence_id=ORG_EVIDENCE_ID,
                subject_type="organization",
                subject_id=ORG_ID,
                source_kind=EvidenceSourceKind.OFFICIAL_REGISTRY,
                source_family="fns",
                claim_type="legal_status",
                source_url="https://egrul.nalog.ru/index.html",
                facts=(
                    EvidenceFact(name="organization.inn", value="7707083893"),
                    EvidenceFact(name="legal.status", value="active"),
                ),
            ),
            cls._evidence(
                evidence_id=PROFILE_EVIDENCE_ID,
                subject_type="organization",
                subject_id=ORG_ID,
                source_kind=EvidenceSourceKind.BUSINESS_DIRECTORY,
                source_family="two_gis",
                claim_type="business_profile",
                source_url="https://2gis.ru/spb/firm/123",
                facts=(
                    EvidenceFact(name="business.vertical", value="coffee_shop"),
                    EvidenceFact(name="business.category", value="Кофейня"),
                    EvidenceFact(name="business.domain", value="example.ru"),
                    EvidenceFact(name="organization.inn", value="7707083893"),
                ),
            ),
            cls._evidence(
                evidence_id=DEMAND_EVIDENCE_ID,
                subject_type="demand_signal",
                subject_id=SIGNAL_ID,
                source_kind=EvidenceSourceKind.OFFICIAL_WEBSITE,
                source_family="company_website",
                claim_type="operational_gap",
                facts=(
                    EvidenceFact(name="demand.kind", value="lead_handling_gap"),
                    EvidenceFact(name="demand.strength", value="1"),
                    EvidenceFact(
                        name="demand.summary_sha256",
                        value=sha256(
                            "Заявки обрабатываются вручную".encode("utf-8")
                        ).hexdigest(),
                    ),
                    EvidenceFact(name="demand.timing_readiness", value="1"),
                ),
            ),
            cls._evidence(
                evidence_id=DM_EVIDENCE_ID,
                subject_type="decision_maker",
                subject_id=DM_ID,
                source_kind=EvidenceSourceKind.OFFICIAL_WEBSITE,
                source_family="company_website",
                claim_type="management_role",
                facts=(
                    EvidenceFact(name="role.name", value="Иван Иванов"),
                    EvidenceFact(
                        name="role.responsibility",
                        value="Автоматизация клиентского сервиса",
                    ),
                    EvidenceFact(name="role.seniority", value="manager"),
                    EvidenceFact(name="role.title", value="Управляющий"),
                ),
            ),
            cls._evidence(
                evidence_id=CONTACT_EVIDENCE_ID,
                subject_type="public_contact",
                subject_id=CONTACT_ID,
                source_kind=EvidenceSourceKind.OFFICIAL_WEBSITE,
                source_family="company_website",
                claim_type="public_contact",
                facts=(
                    EvidenceFact(
                        name="contact.channel",
                        value="business_email",
                    ),
                    EvidenceFact(
                        name="contact.scope",
                        value="named_business_person",
                    ),
                    EvidenceFact(
                        name="contact.value_sha256",
                        value=sha256(b"contact018@example.invalid").hexdigest(),
                    ),
                    EvidenceFact(
                        name="contact.decision_maker_id",
                        value=str(DM_ID),
                    ),
                    EvidenceFact(
                        name="contact.decision_maker_name_sha256",
                        value=sha256(
                            "иван иванов".encode("utf-8")
                        ).hexdigest(),
                    ),
                ),
            ),
        )
        identifier = OrganizationIdentifier(
            kind=IdentifierKind.INN,
            value="7707083893",
            verification_state=VerificationState.VERIFIED,
            evidence_ids=(ORG_EVIDENCE_ID,),
            verified_at=NOW,
        )
        domain_identifier = OrganizationIdentifier(
            kind=IdentifierKind.DOMAIN,
            value="example.ru",
            verification_state=VerificationState.VERIFIED,
            evidence_ids=(PROFILE_EVIDENCE_ID,),
            verified_at=NOW,
        )
        organization = Organization(
            organization_id=ORG_ID,
            display_name="Тестовая кофейня",
            vertical=BusinessVertical.COFFEE_SHOP,
            category="Кофейня",
            city="Санкт-Петербург",
            website_url="https://example.ru",
            identifiers=(identifier, domain_identifier),
            legal_status=OrganizationLegalStatus.ACTIVE,
            lifecycle=OrganizationLifecycle.VERIFIED,
            evidence_ids=(ORG_EVIDENCE_ID, PROFILE_EVIDENCE_ID),
        )
        demand = DemandSignal(
            signal_id=SIGNAL_ID,
            organization_id=ORG_ID,
            kind=DemandSignalKind.LEAD_HANDLING_GAP,
            summary="Заявки обрабатываются вручную",
            strength=Decimal("1"),
            timing_readiness=Decimal("1"),
            occurred_at=NOW,
            valid_until=NOW + timedelta(days=30),
            evidence_ids=(DEMAND_EVIDENCE_ID,),
            verification_state=VerificationState.VERIFIED,
            verified_at=NOW,
        )
        decision_maker = DecisionMakerEvidence(
            decision_maker_id=DM_ID,
            organization_id=ORG_ID,
            asserted_name="Иван Иванов",
            asserted_title="Управляющий",
            seniority=DecisionMakerSeniority.MANAGER,
            responsibility="Автоматизация клиентского сервиса",
            authority_score=Decimal("1"),
            confidence=Decimal("1"),
            observed_at=NOW,
            evidence_ids=(DM_EVIDENCE_ID,),
            verification_state=VerificationState.VERIFIED,
            verified_at=NOW,
        )
        contact = PublicContact(
            contact_id=CONTACT_ID,
            organization_id=ORG_ID,
            decision_maker_id=DM_ID,
            channel=ContactChannel.BUSINESS_EMAIL,
            value="contact018@example.invalid",
            scope=ContactScope.NAMED_BUSINESS_PERSON,
            observed_at=NOW,
            evidence_ids=(CONTACT_EVIDENCE_ID,),
            verification_state=VerificationState.VERIFIED,
            verified_at=NOW,
        )
        score_input = QualificationScoreInputV2(
            company_fit=cls._factor((PROFILE_EVIDENCE_ID,)),
            pain_strength=cls._factor((DEMAND_EVIDENCE_ID,)),
            timing_readiness=cls._factor((DEMAND_EVIDENCE_ID,)),
            decision_maker_access=cls._factor(
                (DM_EVIDENCE_ID, CONTACT_EVIDENCE_ID)
            ),
            evidence_quality=cls._factor(
                (
                    ORG_EVIDENCE_ID,
                    PROFILE_EVIDENCE_ID,
                    DEMAND_EVIDENCE_ID,
                    DM_EVIDENCE_ID,
                    CONTACT_EVIDENCE_ID,
                )
            ),
        )
        return {
            "lead_id": LEAD_ID,
            "organization": organization,
            "demand_signals": (demand,),
            "decision_makers": (decision_maker,),
            "contacts": (contact,),
            "evidence": evidence,
            "score_input": score_input,
            "score": calculate_qualification_score_v2(score_input),
            "as_of": NOW,
        }

    @staticmethod
    def _factor(evidence_ids: tuple[UUID, ...]) -> ScoreFactor:
        return ScoreFactor(
            value=Decimal("1"),
            evidence_ids=evidence_ids,
            reason_codes=("verified",),
        )

    @staticmethod
    def _evidence(
        *,
        evidence_id: UUID,
        subject_type: str,
        subject_id: UUID,
        source_kind: EvidenceSourceKind,
        source_family: str,
        claim_type: str,
        facts: tuple[EvidenceFact, ...] | None = None,
        source_url: str | None = None,
    ) -> EvidenceEnvelope:
        normalized_facts = facts or (
            EvidenceFact(name="verified.claim", value=claim_type),
        )
        return EvidenceEnvelope(
            evidence_id=evidence_id,
            subject_type=subject_type,
            subject_id=subject_id,
            source_kind=source_kind,
            source_family=source_family,
            source_name=source_family,
            source_url=(
                source_url or f"https://example.ru/evidence/{evidence_id}"
            ),
            external_record_id=str(evidence_id),
            claim_type=claim_type,
            claim_summary="Проверенное публичное бизнес-утверждение",
            facts=normalized_facts,
            observed_at=NOW,
            collected_at=NOW,
            valid_until=NOW + timedelta(days=60),
            content_sha256="1" * 64,
            facts_sha256=calculate_facts_sha256(normalized_facts),
            collector_version="test.v1",
            collection_basis=CollectionBasis.PUBLIC_WEB,
            storage_policy=EvidenceStoragePolicy.FACTS_ONLY,
            confidence=Decimal("1"),
            verification_state=VerificationState.VERIFIED,
            verified_at=NOW,
        )


if __name__ == "__main__":
    unittest.main()

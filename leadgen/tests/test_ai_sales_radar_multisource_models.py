from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
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


NOW = datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc)
ORG_ID = UUID("10000000-0000-0000-0000-000000000001")
EVIDENCE_ID = UUID("20000000-0000-0000-0000-000000000001")


class MultisourceModelsTest(unittest.TestCase):
    def test_evidence_is_bounded_verified_and_has_no_raw_payload(self):
        fact = EvidenceFact(name="legal.status", value="active")
        evidence = self._evidence(facts=(fact,))

        self.assertEqual(
            evidence.facts_sha256,
            calculate_facts_sha256((fact,)),
        )
        with self.assertRaises(ValidationError):
            EvidenceEnvelope.model_validate(
                {**evidence.model_dump(), "raw_payload": {"provider": "dump"}}
            )
        with self.assertRaises(ValidationError):
            self._evidence(source_url="https://user:contact012@example.invalid/record")
        with self.assertRaises(ValidationError):
            self._evidence(source_url="https://example.ru/record?token=secret")
        with self.assertRaises(ValidationError):
            self._evidence(observed_at=datetime(2026, 8, 31, 12, 0))
        with self.assertRaises(ValidationError):
            self._evidence(facts_sha256="0" * 64)
        with self.assertRaises(ValidationError):
            EvidenceFact(name="passport", value="sensitive")
        with self.assertRaises(ValidationError):
            self._evidence(source_url="https://localhost/record")
        with self.assertRaises(ValidationError):
            self._evidence(source_url="https://127.0.0.1/record")

    def test_single_evidence_envelope_rejects_duplicate_fact_names(self):
        facts = (
            EvidenceFact(name="legal.status", value="active"),
            EvidenceFact(name="legal.status", value="inactive"),
        )

        with self.assertRaises(ValidationError):
            self._evidence(
                facts=facts,
                facts_sha256=calculate_facts_sha256(facts),
            )

    def test_typed_fact_values_cannot_hide_raw_contacts_or_bad_identifiers(self):
        invalid_facts = (
            ("source.record_id", "contact013@example.invalid"),
            ("contact.value_sha256", "not-a-sha256"),
            ("contact.decision_maker_id", "not-a-uuid"),
            ("organization.inn", "7707083894"),
            ("demand.strength", "1.1"),
        )

        for name, value in invalid_facts:
            with self.subTest(name=name):
                with self.assertRaises(ValidationError):
                    EvidenceFact(name=name, value=value)

    def test_verified_organization_requires_verified_identifier(self):
        unverified = OrganizationIdentifier(
            kind=IdentifierKind.INN,
            value="7707083893",
            evidence_ids=(EVIDENCE_ID,),
        )
        with self.assertRaises(ValidationError):
            self._organization(identifiers=(unverified,))

    def test_duplicate_normalized_identifiers_are_rejected(self):
        first = self._identifier()
        second = self._identifier(value="7707 083 893")

        with self.assertRaises(ValidationError):
            self._organization(identifiers=(first, second))

    def test_only_public_business_contacts_are_accepted(self):
        contact = PublicContact(
            organization_id=ORG_ID,
            channel=ContactChannel.BUSINESS_EMAIL,
            value="contact014@example.com",
            scope=ContactScope.ORGANIZATION,
            observed_at=NOW,
            evidence_ids=(EVIDENCE_ID,),
            verification_state=VerificationState.VERIFIED,
            verified_at=NOW,
        )

        self.assertEqual(contact.value, "contact014@example.com")
        with self.assertRaises(ValidationError):
            PublicContact.model_validate(
                {**contact.model_dump(), "publicly_listed": False}
            )

    def test_russian_business_phone_is_canonical_and_rejects_letters(self):
        values = {
            "organization_id": ORG_ID,
            "channel": ContactChannel.BUSINESS_PHONE,
            "scope": ContactScope.ORGANIZATION,
            "observed_at": NOW,
            "evidence_ids": (EVIDENCE_ID,),
        }
        domestic = PublicContact(value="8 (000) 000-00-01", **values)
        international = PublicContact(value="+7 (000) 000-00-01", **values)

        self.assertEqual(domestic.value, "+70000000001")
        self.assertEqual(domestic.value, international.value)
        with self.assertRaises(ValidationError):
            PublicContact(value="abc79991234567", **values)
        with self.assertRaises(ValidationError):
            PublicContact(value="+00000000000", **values)

    def test_business_email_requires_valid_local_and_public_domain_parts(self):
        values = {
            "organization_id": ORG_ID,
            "channel": ContactChannel.BUSINESS_EMAIL,
            "scope": ContactScope.ORGANIZATION,
            "observed_at": NOW,
            "evidence_ids": (EVIDENCE_ID,),
        }

        contact = PublicContact(value="Sales@пример.рф", **values)

        self.assertEqual(contact.value, "Sales@xn--e1afmkfd.xn--p1ai")
        with self.assertRaises(ValidationError):
            PublicContact(value="contact015@example.invalid", **values)
        with self.assertRaises(ValidationError):
            PublicContact(value="contact016@example.invalid", **values)

    def test_only_named_contact_can_reference_a_decision_maker(self):
        with self.assertRaises(ValidationError):
            PublicContact(
                organization_id=ORG_ID,
                decision_maker_id=UUID(
                    "40000000-0000-0000-0000-000000000001"
                ),
                channel=ContactChannel.BUSINESS_EMAIL,
                value="contact017@example.invalid",
                scope=ContactScope.ORGANIZATION,
                observed_at=NOW,
                evidence_ids=(EVIDENCE_ID,),
            )

    def test_verified_decision_maker_requires_name(self):
        with self.assertRaises(ValidationError):
            DecisionMakerEvidence(
                organization_id=ORG_ID,
                asserted_title="Управляющий",
                seniority=DecisionMakerSeniority.MANAGER,
                authority_score=Decimal("0.8"),
                confidence=Decimal("0.8"),
                observed_at=NOW,
                evidence_ids=(EVIDENCE_ID,),
                verification_state=VerificationState.VERIFIED,
                verified_at=NOW,
            )

    def test_impossible_verification_chronology_is_rejected(self):
        with self.assertRaises(ValidationError):
            self._evidence(
                verified_at=datetime(
                    2026,
                    8,
                    30,
                    12,
                    0,
                    tzinfo=timezone.utc,
                )
            )

    def test_link_only_evidence_retains_no_facts(self):
        evidence = self._evidence(
            facts=(),
            facts_sha256=calculate_facts_sha256(()),
            storage_policy=EvidenceStoragePolicy.LINK_ONLY,
        )

        self.assertEqual(evidence.facts, ())

    @staticmethod
    def _evidence(**overrides) -> EvidenceEnvelope:
        facts = overrides.pop(
            "facts",
            (EvidenceFact(name="legal.status", value="active"),),
        )
        values = {
            "evidence_id": EVIDENCE_ID,
            "subject_type": "organization",
            "subject_id": ORG_ID,
            "source_kind": EvidenceSourceKind.OFFICIAL_REGISTRY,
            "source_family": "fns",
            "source_name": "ФНС",
            "source_url": "https://egrul.nalog.ru/record",
            "external_record_id": "1027700132195",
            "claim_type": "legal_status",
            "claim_summary": "Организация действует",
            "facts": facts,
            "observed_at": NOW,
            "collected_at": NOW,
            "valid_until": datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc),
            "content_sha256": "1" * 64,
            "facts_sha256": calculate_facts_sha256(facts),
            "collector_version": "test.v1",
            "collection_basis": CollectionBasis.PUBLIC_WEB,
            "storage_policy": EvidenceStoragePolicy.FACTS_ONLY,
            "confidence": Decimal("1"),
            "verification_state": VerificationState.VERIFIED,
            "verified_at": NOW,
        }
        values.update(overrides)
        return EvidenceEnvelope(**values)

    @staticmethod
    def _identifier(**overrides) -> OrganizationIdentifier:
        values = {
            "kind": IdentifierKind.INN,
            "value": "7707083893",
            "verification_state": VerificationState.VERIFIED,
            "evidence_ids": (EVIDENCE_ID,),
            "verified_at": NOW,
        }
        values.update(overrides)
        return OrganizationIdentifier(**values)

    @classmethod
    def _organization(cls, **overrides) -> Organization:
        values = {
            "organization_id": ORG_ID,
            "display_name": "Тестовая кофейня",
            "vertical": BusinessVertical.COFFEE_SHOP,
            "category": "Кофейня",
            "city": "Санкт-Петербург",
            "identifiers": (cls._identifier(),),
            "legal_status": OrganizationLegalStatus.ACTIVE,
            "lifecycle": OrganizationLifecycle.VERIFIED,
            "evidence_ids": (EVIDENCE_ID,),
        }
        values.update(overrides)
        return Organization(**values)


if __name__ == "__main__":
    unittest.main()

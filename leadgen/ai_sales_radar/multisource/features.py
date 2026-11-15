"""Fail-closed feature flags for the isolated multisource contour."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, model_validator


class MultisourceFeatureFlags(BaseModel):
    """Explicit switches; importing this module never reads the environment.

    Child switches are rejected while the master switch is off.  Callers must
    use :meth:`effective` before constructing adapters or scheduling work.
    The first foundation version cannot enable live CRM writes or outreach.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    enabled: bool = False
    two_gis_discovery_enabled: bool = False
    yandex_maps_discovery_enabled: bool = False
    fns_verification_enabled: bool = False
    company_website_research_enabled: bool = False
    demand_signal_research_enabled: bool = False
    decision_maker_research_enabled: bool = False
    public_contact_verification_enabled: bool = False
    qualification_enabled: bool = False
    registry_preview_enabled: bool = False

    dry_run_only: Literal[True] = True
    persistence_enabled: Literal[False] = False
    sheets_write_enabled: Literal[False] = False
    automatic_outreach_enabled: Literal[False] = False

    @model_validator(mode="after")
    def require_master_for_child_flags(self) -> "MultisourceFeatureFlags":
        if not self.enabled and _enabled_children(self):
            raise ValueError("multisource child flags require enabled=True")
        return self

    def effective(self) -> "EffectiveMultisourceFeatures":
        """Return the only switches that runtime composition may consume."""

        if not self.enabled:
            return EffectiveMultisourceFeatures()
        return EffectiveMultisourceFeatures(
            enabled=True,
            two_gis_discovery_enabled=self.two_gis_discovery_enabled,
            yandex_maps_discovery_enabled=self.yandex_maps_discovery_enabled,
            fns_verification_enabled=self.fns_verification_enabled,
            company_website_research_enabled=(
                self.company_website_research_enabled
            ),
            demand_signal_research_enabled=self.demand_signal_research_enabled,
            decision_maker_research_enabled=(
                self.decision_maker_research_enabled
            ),
            public_contact_verification_enabled=(
                self.public_contact_verification_enabled
            ),
            qualification_enabled=self.qualification_enabled,
            registry_preview_enabled=self.registry_preview_enabled,
        )


class EffectiveMultisourceFeatures(BaseModel):
    """Resolved switches after applying the master kill switch."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    enabled: bool = False
    two_gis_discovery_enabled: bool = False
    yandex_maps_discovery_enabled: bool = False
    fns_verification_enabled: bool = False
    company_website_research_enabled: bool = False
    demand_signal_research_enabled: bool = False
    decision_maker_research_enabled: bool = False
    public_contact_verification_enabled: bool = False
    qualification_enabled: bool = False
    registry_preview_enabled: bool = False

    dry_run_only: Literal[True] = True
    persistence_enabled: Literal[False] = False
    sheets_write_enabled: Literal[False] = False
    automatic_outreach_enabled: Literal[False] = False

    @model_validator(mode="after")
    def require_master_for_child_flags(self) -> "EffectiveMultisourceFeatures":
        if not self.enabled and _enabled_children(self):
            raise ValueError("effective child flags require enabled=True")
        return self


def _enabled_children(
    flags: MultisourceFeatureFlags | EffectiveMultisourceFeatures,
) -> tuple[str, ...]:
    return tuple(
        field_name
        for field_name, value in flags.model_dump().items()
        if field_name != "enabled"
        and field_name.endswith("_enabled")
        and value is True
    )

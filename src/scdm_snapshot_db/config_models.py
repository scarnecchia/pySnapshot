# pattern: Functional Core
"""Frozen configuration dataclasses, domain graph, and output registry.

These types are pure data: no IO, no Spark, no environment access.
Validation lives in ``config_validation``; loading lives in ``config_loading``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Literal

from .models import Domain, OutputDescriptor, SubCohort

__all__ = [
    "ALL_OUTPUT_NAMES",
    "DEATH_OUTPUT_NAMES",
    "DEMOGRAPHIC_OUTPUT_NAMES",
    "DEPENDENT_DOMAINS",
    "DISPENSING_OUTPUT_NAMES",
    "DOMAIN_DEPENDENCIES",
    "DOMAIN_INPUT_REQUIRED",
    "ENCOUNTER_OUTPUT_NAMES",
    "ENROLLMENT_OUTPUT_NAMES",
    "LAB_OUTPUT_NAMES",
    "MIL_OUTPUT_NAMES",
    "OUTPUT_REGISTRY",
    "BenchmarkSettings",
    "BroadcastStrategy",
    "Config",
    "DomainDependency",
    "InputPaths",
    "RequestValues",
    "SparkSettings",
    "WriteMode",
    "WriteSettings",
]

WriteMode = Literal["errorifexists", "overwrite", "ignore"]
BroadcastStrategy = Literal["auto", "broadcast", "disabled"]


# ─── Spark settings ───────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class SparkSettings:
    """Local Spark session configuration."""

    master: str = "local[*]"
    app_name: str = "scdm_snapshot_db"
    driver_memory: str = "4g"
    shuffle_partitions: int = 200
    default_parallelism: int = 0
    adaptive_query_execution: bool = True
    session_timezone: str = "UTC"
    extra_settings: dict[str, str] = field(default_factory=dict)
    broadcast_strategy: BroadcastStrategy = "auto"
    storage_level: str = "MEMORY_AND_DISK"
    output_partitions: int = 0


# ─── Write settings ───────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class WriteSettings:
    """Output write behavior."""

    mode: WriteMode = "errorifexists"


# ─── Benchmark settings ───────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class BenchmarkSettings:
    """Fresh-process benchmark runner configuration."""

    repetitions: int = 5
    clean_output: bool = True
    metadata_path: str = "benchmark_result.json"
    warmup_repetitions: int = 0


# ─── Input paths ──────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class InputPaths:
    """Logical paths to the seven source domains.

    Only the paths for selected domains need to exist and be valid.
    """

    enrollment: str | None = None
    demographic: str | None = None
    dispensing: str | None = None
    encounter: str | None = None
    lab: str | None = None
    death: str | None = None
    mil: str | None = None

    def for_domain(self, domain: Domain) -> str | None:
        """Return the configured path for the given domain."""
        return getattr(self, domain.value)


# ─── Request values ───────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class RequestValues:
    """Per-request analytical parameters."""

    dpid: str = ""
    dp_max_date: date = date(2023, 6, 30)


# ─── Top-level config ─────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class Config:
    """Fully resolved configuration for a pipeline run."""

    request: RequestValues
    input_paths: InputPaths
    output_root: str
    selected_domains: frozenset[Domain]
    spark: SparkSettings
    write: WriteSettings
    benchmark: BenchmarkSettings


# ─── Domain dependency graph ──────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class DomainDependency:
    """Describes what a domain needs from enrollment."""

    requires_enrollment: bool
    sub_cohorts: frozenset[SubCohort]
    input_domain: Domain | None  # the source table domain (None = enrollment itself)

    @property
    def is_independent(self) -> bool:
        """True when the domain does not depend on enrollment."""
        return not self.requires_enrollment


# Domains that depend on enrollment sub-cohorts
DEPENDENT_DOMAINS: frozenset[Domain] = frozenset({
    Domain.DEMOGRAPHIC,
    Domain.DISPENSING,
    Domain.ENCOUNTER,
    Domain.LAB,
    Domain.DEATH,
})

DOMAIN_DEPENDENCIES: dict[Domain, DomainDependency] = {
    Domain.ENROLLMENT: DomainDependency(
        requires_enrollment=True,
        sub_cohorts=frozenset({
            SubCohort.MEDICAL_AND_DRUG,
            SubCohort.DRUG_ONLY,
            SubCohort.MEDICAL_ONLY,
        }),
        input_domain=None,
    ),
    Domain.DEMOGRAPHIC: DomainDependency(
        requires_enrollment=True,
        sub_cohorts=frozenset({SubCohort.MEDICAL_AND_DRUG}),
        input_domain=Domain.DEMOGRAPHIC,
    ),
    Domain.DISPENSING: DomainDependency(
        requires_enrollment=True,
        sub_cohorts=frozenset({SubCohort.MEDICAL_AND_DRUG, SubCohort.DRUG_ONLY}),
        input_domain=Domain.DISPENSING,
    ),
    Domain.ENCOUNTER: DomainDependency(
        requires_enrollment=True,
        sub_cohorts=frozenset({SubCohort.MEDICAL_AND_DRUG}),
        input_domain=Domain.ENCOUNTER,
    ),
    Domain.LAB: DomainDependency(
        requires_enrollment=True,
        sub_cohorts=frozenset({SubCohort.MEDICAL_AND_DRUG}),
        input_domain=Domain.LAB,
    ),
    Domain.DEATH: DomainDependency(
        requires_enrollment=True,
        sub_cohorts=frozenset({SubCohort.MEDICAL_AND_DRUG, SubCohort.MEDICAL_ONLY}),
        input_domain=Domain.DEATH,
    ),
    Domain.MIL: DomainDependency(
        requires_enrollment=False,
        sub_cohorts=frozenset(),
        input_domain=Domain.MIL,
    ),
}

# Maps each Domain to the source domain whose path is required
DOMAIN_INPUT_REQUIRED: dict[Domain, Domain] = {
    Domain.ENROLLMENT: Domain.ENROLLMENT,
    Domain.DEMOGRAPHIC: Domain.DEMOGRAPHIC,
    Domain.DISPENSING: Domain.DISPENSING,
    Domain.ENCOUNTER: Domain.ENCOUNTER,
    Domain.LAB: Domain.LAB,
    Domain.DEATH: Domain.DEATH,
    Domain.MIL: Domain.MIL,
}


# ─── Output registry ──────────────────────────────────────────────────────────

ENROLLMENT_OUTPUT_NAMES: list[str] = [
    "enr_pat_covlength_md",
    "enr_patid_ct_md",
    "enr_pat_covyears_md",
    "enr_pat_enrcount_md",
    "enr_active_patid_ct_md",
]

DEMOGRAPHIC_OUTPUT_NAMES: list[str] = [
    "dem_pat_lstagecount_md",
    "dem_pat_actagect_md",
    "dem_catvars_md",
]

DISPENSING_OUTPUT_NAMES: list[str] = [
    "dis_pat_rx_md",
    "dis_pat_rx_d",
]

ENCOUNTER_OUTPUT_NAMES: list[str] = [
    "enc_pat_enccount_md",
]

LAB_OUTPUT_NAMES: list[str] = [
    "lab_pat_testct_md",
]

DEATH_OUTPUT_NAMES: list[str] = [
    "dth_dthct_md",
    "dth_dthct_m",
]

MIL_OUTPUT_NAMES: list[str] = [
    "mil_linkage_rates",
]

ALL_OUTPUT_NAMES: list[str] = (
    ENROLLMENT_OUTPUT_NAMES
    + DEMOGRAPHIC_OUTPUT_NAMES
    + DISPENSING_OUTPUT_NAMES
    + ENCOUNTER_OUTPUT_NAMES
    + LAB_OUTPUT_NAMES
    + DEATH_OUTPUT_NAMES
    + MIL_OUTPUT_NAMES
)

OUTPUT_REGISTRY: list[OutputDescriptor] = [
    OutputDescriptor(
        name="enr_pat_covlength_md",
        domain=Domain.ENROLLMENT,
        sub_cohorts=frozenset({SubCohort.MEDICAL_AND_DRUG}),
        description="inclusive span length distribution per patient",
    ),
    OutputDescriptor(
        name="enr_patid_ct_md",
        domain=Domain.ENROLLMENT,
        sub_cohorts=frozenset({SubCohort.MEDICAL_AND_DRUG}),
        description="distinct medical-and-drug patient count",
    ),
    OutputDescriptor(
        name="enr_pat_covyears_md",
        domain=Domain.ENROLLMENT,
        sub_cohorts=frozenset({SubCohort.MEDICAL_AND_DRUG}),
        description="distinct patient-year coverage counts",
    ),
    OutputDescriptor(
        name="enr_pat_enrcount_md",
        domain=Domain.ENROLLMENT,
        sub_cohorts=frozenset({SubCohort.MEDICAL_AND_DRUG}),
        description="bridged enrollment span count distribution",
    ),
    OutputDescriptor(
        name="enr_active_patid_ct_md",
        domain=Domain.ENROLLMENT,
        sub_cohorts=frozenset({SubCohort.MEDICAL_AND_DRUG}),
        description="distinct active patient count at dp_max_date",
    ),
    OutputDescriptor(
        name="dem_pat_lstagecount_md",
        domain=Domain.DEMOGRAPHIC,
        sub_cohorts=frozenset({SubCohort.MEDICAL_AND_DRUG}),
        description="latest-stage age category counts",
    ),
    OutputDescriptor(
        name="dem_pat_actagect_md",
        domain=Domain.DEMOGRAPHIC,
        sub_cohorts=frozenset({SubCohort.MEDICAL_AND_DRUG}),
        description="active patient age category counts",
    ),
    OutputDescriptor(
        name="dem_catvars_md",
        domain=Domain.DEMOGRAPHIC,
        sub_cohorts=frozenset({SubCohort.MEDICAL_AND_DRUG}),
        description="categorical variable counts for sex, race, hispanic",
    ),
    OutputDescriptor(
        name="dis_pat_rx_md",
        domain=Domain.DISPENSING,
        sub_cohorts=frozenset({SubCohort.MEDICAL_AND_DRUG}),
        description="dispensing count distribution for medical-and-drug cohort",
    ),
    OutputDescriptor(
        name="dis_pat_rx_d",
        domain=Domain.DISPENSING,
        sub_cohorts=frozenset({SubCohort.DRUG_ONLY}),
        description="dispensing count distribution for drug-only cohort",
    ),
    OutputDescriptor(
        name="enc_pat_enccount_md",
        domain=Domain.ENCOUNTER,
        sub_cohorts=frozenset({SubCohort.MEDICAL_AND_DRUG}),
        description="encounter count distribution for medical-and-drug cohort",
    ),
    OutputDescriptor(
        name="lab_pat_testct_md",
        domain=Domain.LAB,
        sub_cohorts=frozenset({SubCohort.MEDICAL_AND_DRUG}),
        description="lab test count distribution for medical-and-drug cohort",
    ),
    OutputDescriptor(
        name="dth_dthct_md",
        domain=Domain.DEATH,
        sub_cohorts=frozenset({SubCohort.MEDICAL_AND_DRUG}),
        description="death count for medical-and-drug cohort",
    ),
    OutputDescriptor(
        name="dth_dthct_m",
        domain=Domain.DEATH,
        sub_cohorts=frozenset({SubCohort.MEDICAL_ONLY}),
        description="death count for medical-only cohort",
    ),
    OutputDescriptor(
        name="mil_linkage_rates",
        domain=Domain.MIL,
        sub_cohorts=frozenset(),
        description="maternal-infant linkage rates by overall and four dimensions",
    ),
]

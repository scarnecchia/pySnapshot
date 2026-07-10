# pattern: Functional Core
"""Immutable result, domain, output, and benchmark metadata types."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum

__all__ = [
    "BenchmarkSample",
    "BenchmarkSummary",
    "ComparisonDatasetResult",
    "ComparisonResult",
    "ComparisonStatus",
    "Domain",
    "OutputDescriptor",
    "PipelineResult",
    "SubCohort",
]


class Domain(StrEnum):
    """Analytical domains that can be selected for a run."""

    ENROLLMENT = "enrollment"
    DEMOGRAPHIC = "demographic"
    DISPENSING = "dispensing"
    ENCOUNTER = "encounter"
    LAB = "lab"
    DEATH = "death"
    MIL = "mil"


class SubCohort(StrEnum):
    """Enrollment sub-cohorts used by downstream transforms."""

    MEDICAL_ONLY = "m"
    MEDICAL_AND_DRUG = "md"
    DRUG_ONLY = "d"


@dataclass(frozen=True, slots=True)
class OutputDescriptor:
    """Describes one logical output dataset."""

    name: str
    domain: Domain
    sub_cohorts: frozenset[SubCohort]
    description: str


@dataclass(frozen=True, slots=True)
class PipelineResult:
    """Machine-readable result of a pipeline run."""

    success: bool
    elapsed_seconds: float
    selected_domains: list[str]
    selected_outputs: list[str]
    output_root: str
    error_type: str | None = None
    error_message: str | None = None
    python_version: str | None = None
    pyspark_version: str | None = None
    java_version: str | None = None
    spark_version: str | None = None
    effective_spark_settings: dict[str, str] = field(default_factory=dict)
    started_at: str | None = None
    finished_at: str | None = None


@dataclass(frozen=True, slots=True)
class BenchmarkSample:
    """One repetition of a benchmark run."""

    repetition: int
    success: bool
    child_elapsed_seconds: float | None
    outer_elapsed_seconds: float
    exit_code: int
    output_dir: str
    error_message: str | None = None


@dataclass(frozen=True, slots=True)
class BenchmarkSummary:
    """Aggregated benchmark results."""

    config_path: str
    repetitions: int
    samples: list[BenchmarkSample]
    successful_count: int
    failed_count: int
    median_seconds: float | None
    min_seconds: float | None
    max_seconds: float | None
    dispersion_seconds: float | None


class ComparisonStatus(StrEnum):
    """Status of a single dataset comparison."""

    EXACT_MATCH = "exact_match"
    NUMERIC_TOLERANCE_MATCH = "numeric_tolerance_match"
    SCHEMA_DIFFERENCE = "schema_difference"
    VALUE_DIFFERENCE = "value_difference"
    MISSING_REFERENCE = "missing_reference"
    MISSING_ACTUAL = "missing_actual"


@dataclass(frozen=True, slots=True)
class ComparisonDatasetResult:
    """Result of comparing one logical dataset."""

    name: str
    status: ComparisonStatus
    details: str


@dataclass(frozen=True, slots=True)
class ComparisonResult:
    """Overall result of comparing two output sets."""

    datasets: list[ComparisonDatasetResult]
    exact_match_count: int
    numeric_tolerance_match_count: int
    schema_difference_count: int
    value_difference_count: int
    missing_reference_count: int
    missing_actual_count: int
    overall_equivalent: bool

    @property
    def status_counts(self) -> Mapping[str, int]:
        """Return a mapping of status name to count."""
        return {
            ComparisonStatus.EXACT_MATCH.value: self.exact_match_count,
            ComparisonStatus.NUMERIC_TOLERANCE_MATCH.value: self.numeric_tolerance_match_count,
            ComparisonStatus.SCHEMA_DIFFERENCE.value: self.schema_difference_count,
            ComparisonStatus.VALUE_DIFFERENCE.value: self.value_difference_count,
            ComparisonStatus.MISSING_REFERENCE.value: self.missing_reference_count,
            ComparisonStatus.MISSING_ACTUAL.value: self.missing_actual_count,
        }

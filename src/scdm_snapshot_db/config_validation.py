# pattern: Functional Core
"""Pure validation and domain-resolution logic.

No IO, no Spark, no environment access. All functions take already-parsed
data and return validated results or raise ``ConfigError``.
"""

from __future__ import annotations

import re
from pathlib import PurePath

from .config_models import (
    DOMAIN_DEPENDENCIES,
    DOMAIN_INPUT_REQUIRED,
    Config,
    Domain,
    DomainDependency,
    InputPaths,
    RequestValues,
    SparkSettings,
    WriteMode,
)
from .error_classification import ConfigError

__all__ = [
    "resolve_domains",
    "resolve_outputs_for_domains",
    "resolve_required_inputs",
    "resolve_sub_cohorts",
    "validate_input_paths_for_domains",
    "validate_output_overlap",
    "validate_request",
    "validate_spark_settings",
    "validate_write_mode",
]

_VALID_DOMAIN_NAMES: frozenset[str] = frozenset(d.value for d in Domain)


# ─── Request validation ───────────────────────────────────────────────────────


def validate_request(request: RequestValues) -> RequestValues:
    """Validate request-level parameters. Raises ``ConfigError`` on failure."""
    if not request.dpid or not request.dpid.strip():
        raise ConfigError("dpid must not be empty")
    return request


# ─── Input path validation ────────────────────────────────────────────────────


def validate_input_paths_for_domains(
    input_paths: InputPaths,
    domains: frozenset[Domain],
) -> None:
    """Ensure each selected domain has a configured input path.

    Enrollment is required when any dependent domain is selected.
    """
    for domain in domains:
        dep = DOMAIN_DEPENDENCIES[domain]
        if dep.requires_enrollment and input_paths.enrollment is None:
            raise ConfigError(f"enrollment input path required because {domain.value} is selected")
        input_domain = DOMAIN_INPUT_REQUIRED[domain]
        path = input_paths.for_domain(input_domain)
        if path is None:
            raise ConfigError(f"input path for {input_domain.value} is required but not configured")


def validate_output_overlap(output_root: str, input_paths: InputPaths) -> None:
    """Reject when the output root is inside or contains any input path."""
    out = PurePath(output_root)
    for domain in Domain:
        path_str = input_paths.for_domain(domain)
        if path_str is None:
            continue
        inp = PurePath(path_str)
        # Check if output is inside input or input is inside output
        if _is_prefix(out, inp) or _is_prefix(inp, out):
            raise ConfigError(f"output root must not overlap with input path for {domain.value}")


def _is_prefix(a: PurePath, b: PurePath) -> bool:
    """Return True if ``a`` is a prefix of ``b``."""
    try:
        b.relative_to(a)
        return True
    except ValueError:
        return False


# ─── Spark settings validation ────────────────────────────────────────────────

_MEM_RE = re.compile(r"^\d+[gGmM]$")


def validate_spark_settings(spark: SparkSettings) -> SparkSettings:
    """Validate Spark configuration. Raises ``ConfigError`` on failure."""
    if not spark.master.startswith("local"):
        raise ConfigError(
            f"spark master '{spark.master}' is not local mode; "
            "this package targets local execution only"
        )
    if not _MEM_RE.match(spark.driver_memory):
        raise ConfigError(
            f"driver memory '{spark.driver_memory}' must match pattern like '4g' or '512m'"
        )
    if spark.shuffle_partitions < 1:
        raise ConfigError("shuffle partitions must be at least 1")
    if spark.default_parallelism < 0:
        raise ConfigError("default parallelism must be non-negative")
    if spark.output_partitions < 0:
        raise ConfigError("output partitions must be non-negative (0 means auto)")
    return spark


def validate_write_mode(mode: str) -> WriteMode:
    """Validate the write mode string."""
    valid: set[WriteMode] = {"errorifexists", "overwrite", "ignore"}
    if mode not in valid:
        raise ConfigError(f"write mode '{mode}' must be one of {', '.join(sorted(valid))}")
    return mode


# ─── Domain resolution ────────────────────────────────────────────────────────


def resolve_domains(selected: frozenset[Domain]) -> frozenset[Domain]:
    """Expand selected domains to include all required dependencies.

    If any dependent domain is selected, enrollment is automatically included.
    Returns the full set of domains that will be processed.
    """
    result: set[Domain] = set(selected)
    for domain in selected:
        dep = DOMAIN_DEPENDENCIES[domain]
        if dep.requires_enrollment:
            result.add(Domain.ENROLLMENT)
    return frozenset(result)


def resolve_required_inputs(domains: frozenset[Domain]) -> frozenset[Domain]:
    """Return the set of domains whose input paths are required."""
    needed: set[Domain] = set()
    for domain in domains:
        input_domain = DOMAIN_INPUT_REQUIRED[domain]
        needed.add(input_domain)
    return frozenset(needed)


def resolve_sub_cohorts(domains: frozenset[Domain]) -> frozenset[str]:
    """Return the set of sub-cohort identifiers required by the selected domains."""
    cohorts: set[str] = set()
    for domain in domains:
        dep: DomainDependency = DOMAIN_DEPENDENCIES[domain]
        for sc in dep.sub_cohorts:
            cohorts.add(sc.value)
    return frozenset(cohorts)


def resolve_outputs_for_domains(domains: frozenset[Domain]) -> list[str]:
    """Return the ordered list of output names for the selected domains."""
    from .config_models import OUTPUT_REGISTRY

    return [desc.name for desc in OUTPUT_REGISTRY if desc.domain in domains]


# ─── Full config validation ───────────────────────────────────────────────────


def validate_config(config: Config) -> Config:
    """Run all validation checks on a resolved Config. Raises ``ConfigError``."""
    validate_request(config.request)
    resolved = resolve_domains(config.selected_domains)
    validate_input_paths_for_domains(config.input_paths, resolved)
    validate_output_overlap(config.output_root, config.input_paths)
    validate_spark_settings(config.spark)
    validate_write_mode(config.write.mode)
    if config.benchmark.repetitions < 1:
        raise ConfigError("benchmark repetitions must be at least 1")
    if config.benchmark.warmup_repetitions < 0:
        raise ConfigError("benchmark warmup repetitions must be non-negative")
    return config


def parse_domain_names(names: list[str]) -> frozenset[Domain]:
    """Parse a list of domain name strings into a frozenset of Domain.

    Rejects duplicates and unknown names.
    """
    result: set[Domain] = set()
    for name in names:
        if name not in _VALID_DOMAIN_NAMES:
            raise ConfigError(
                f"unknown domain '{name}'; valid domains: {', '.join(sorted(_VALID_DOMAIN_NAMES))}"
            )
        domain = Domain(name)
        if domain in result:
            raise ConfigError(f"duplicate domain '{name}'")
        result.add(domain)
    return frozenset(result)

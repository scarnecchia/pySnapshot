"""Pure unit tests for configuration validation and domain resolution."""

from __future__ import annotations

from datetime import date

import pytest

from scdm_snapshot_db.config_models import (
    BenchmarkSettings,
    Config,
    Domain,
    InputPaths,
    RequestValues,
    SparkSettings,
    WriteSettings,
)
from scdm_snapshot_db.config_validation import (
    parse_domain_names,
    resolve_domains,
    resolve_outputs_for_domains,
    resolve_required_inputs,
    resolve_sub_cohorts,
    validate_config,
    validate_input_paths_for_domains,
    validate_output_overlap,
    validate_request,
)
from scdm_snapshot_db.error_classification import ConfigError

# ─── AC.2: Config precedence and validation ───────────────────────────────────


class TestConfigValidation:
    def test_validate_request_empty_dpid(self) -> None:
        with pytest.raises(ConfigError, match="dpid"):
            validate_request(RequestValues(dpid="", dp_max_date=date(2023, 1, 1)))

    def test_validate_request_whitespace_dpid(self) -> None:
        with pytest.raises(ConfigError, match="dpid"):
            validate_request(RequestValues(dpid="  ", dp_max_date=date(2023, 1, 1)))

    def test_validate_request_valid(self) -> None:
        req = RequestValues(dpid="MKSCNR", dp_max_date=date(2023, 6, 30))
        assert validate_request(req) == req

    def test_validate_output_overlap_output_inside_input(self) -> None:
        with pytest.raises(ConfigError, match="overlap"):
            validate_output_overlap(
                "/data/enrollment/output",
                InputPaths(enrollment="/data/enrollment"),
            )

    def test_validate_output_overlap_input_inside_output(self) -> None:
        with pytest.raises(ConfigError, match="overlap"):
            validate_output_overlap(
                "/data",
                InputPaths(enrollment="/data/enrollment"),
            )

    def test_validate_output_overlap_no_overlap(self) -> None:
        validate_output_overlap(
            "/output",
            InputPaths(enrollment="/input/enrollment"),
        )

    def test_parse_domain_names_unknown(self) -> None:
        with pytest.raises(ConfigError, match="unknown domain"):
            parse_domain_names(["foobar"])

    def test_parse_domain_names_duplicate(self) -> None:
        with pytest.raises(ConfigError, match="duplicate"):
            parse_domain_names(["mil", "mil"])

    def test_parse_domain_names_valid(self) -> None:
        result = parse_domain_names(["mil", "lab"])
        assert Domain.MIL in result
        assert Domain.LAB in result

    def test_validate_config_invalid_repetitions(self) -> None:
        config = Config(
            request=RequestValues(dpid="X"),
            input_paths=InputPaths(mil="/data/mil"),
            output_root="/output",
            selected_domains=frozenset({Domain.MIL}),
            spark=SparkSettings(),
            write=WriteSettings(),
            benchmark=BenchmarkSettings(repetitions=0),
        )
        with pytest.raises(ConfigError, match="repetitions"):
            validate_config(config)


# ─── AC.3: Domain dependency resolution ───────────────────────────────────────


class TestDomainResolution:
    def test_resolve_mil_only(self) -> None:
        """MIL can run alone without enrollment."""
        result = resolve_domains(frozenset({Domain.MIL}))
        assert Domain.ENROLLMENT not in result
        assert Domain.MIL in result

    def test_dependent_domain_adds_enrollment(self) -> None:
        """Selecting demographic includes enrollment."""
        result = resolve_domains(frozenset({Domain.DEMOGRAPHIC}))
        assert Domain.ENROLLMENT in result
        assert Domain.DEMOGRAPHIC in result

    def test_each_domain_resolves_exact_inputs(self) -> None:
        """Each domain resolves to its expected required inputs."""
        # MIL only needs MIL input
        mil_inputs = resolve_required_inputs(frozenset({Domain.MIL}))
        assert mil_inputs == frozenset({Domain.MIL})

        # Demographic needs enrollment + demographic
        dem_resolved = resolve_domains(frozenset({Domain.DEMOGRAPHIC}))
        dem_inputs = resolve_required_inputs(dem_resolved)
        assert Domain.ENROLLMENT in dem_inputs
        assert Domain.DEMOGRAPHIC in dem_inputs

        # Death needs enrollment + death
        death_resolved = resolve_domains(frozenset({Domain.DEATH}))
        death_inputs = resolve_required_inputs(death_resolved)
        assert Domain.ENROLLMENT in death_inputs
        assert Domain.DEATH in death_inputs

    def test_selected_inputs_only(self) -> None:
        """MIL-only does not require enrollment input path."""
        validate_input_paths_for_domains(
            InputPaths(mil="/data/mil"),
            frozenset({Domain.MIL}),
        )

    def test_demographic_requires_enrollment_input(self) -> None:
        with pytest.raises(ConfigError, match="enrollment input path required"):
            resolve = resolve_domains(frozenset({Domain.DEMOGRAPHIC}))
            validate_input_paths_for_domains(
                InputPaths(demographic="/data/dem"),
                resolve,
            )

    def test_dispensing_sub_cohorts(self) -> None:
        """Dispensing requires both md and d sub-cohorts."""
        resolved = resolve_domains(frozenset({Domain.DISPENSING}))
        cohorts = resolve_sub_cohorts(resolved)
        assert "md" in cohorts
        assert "d" in cohorts

    def test_death_sub_cohorts(self) -> None:
        """Death requires both md and m sub-cohorts."""
        resolved = resolve_domains(frozenset({Domain.DEATH}))
        cohorts = resolve_sub_cohorts(resolved)
        assert "md" in cohorts
        assert "m" in cohorts

    def test_mil_outputs(self) -> None:
        outputs = resolve_outputs_for_domains(frozenset({Domain.MIL}))
        assert outputs == ["mil_linkage_rates"]

    def test_all_domains_outputs_count(self) -> None:
        from scdm_snapshot_db.config_models import ALL_OUTPUT_NAMES

        all_domains = frozenset(d for d in Domain)
        resolved = resolve_domains(all_domains)
        outputs = resolve_outputs_for_domains(resolved)
        assert len(outputs) == len(ALL_OUTPUT_NAMES) == 15

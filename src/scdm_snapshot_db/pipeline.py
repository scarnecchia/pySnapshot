# pattern: Imperative Shell
"""Pipeline orchestration: sequences transforms and writes outputs.

This module contains no analytical or validation policy. It resolves
dependencies, creates Spark sessions, reads inputs, constructs shared
plans, persists at declared boundaries, writes outputs, and records
metadata.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from .config_models import Config, SubCohort
from .config_validation import resolve_domains, resolve_outputs_for_domains
from .error_classification import (
    ConfigError,
    DataValidationError,
    OutputError,
    SchemaError,
    classify_exception,
)
from .input_validation import validate_one_demographic_per_patient
from .inputs import InputManifest, read_inputs
from .models import Domain, PipelineResult
from .outputs import write_output
from .spark_session import get_effective_settings, spark_session_scope
from .transforms import (
    death,
    demographic,
    dispensing,
    encounter,
    enrollment,
    lab,
    mil,
)

logger = logging.getLogger(__name__)

__all__ = ["run_pipeline"]


def _safe_version(attr: str, module: str) -> str | None:
    """Get a version string from a module without triggering network access."""
    try:
        mod = __import__(module, fromlist=["__version__"])
        return getattr(mod, attr, None)
    except Exception:
        return None


def _java_version() -> str | None:
    """Get Java version without external network access."""
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        return f"JAVA_HOME={java_home}"
    return None


def _build_enrollment_plans(
    manifest: InputManifest,
    config: Config,
) -> dict[str, DataFrame]:
    """Build bridged enrollment plans for required sub-cohorts."""
    from .config_validation import resolve_sub_cohorts

    needed_cohorts = resolve_sub_cohorts(config.selected_domains)
    enrollment_df = manifest.get(Domain.ENROLLMENT.value)

    plans: dict[str, DataFrame] = {}

    if SubCohort.MEDICAL_ONLY.value in needed_cohorts:
        med_df = enrollment.filter_medical_only(enrollment_df)
        plans[SubCohort.MEDICAL_ONLY.value] = enrollment.bridge_intervals(med_df)

    if SubCohort.MEDICAL_AND_DRUG.value in needed_cohorts:
        md_df = enrollment.filter_medical_and_drug(enrollment_df)
        plans[SubCohort.MEDICAL_AND_DRUG.value] = enrollment.bridge_intervals(md_df)

    if SubCohort.DRUG_ONLY.value in needed_cohorts:
        drug_df = enrollment.filter_drug_only(enrollment_df)
        plans[SubCohort.DRUG_ONLY.value] = enrollment.bridge_intervals(drug_df)

    return plans


def _run_enrollment_outputs(
    bridged: dict[str, DataFrame],
    config: Config,
) -> list[tuple[str, DataFrame]]:
    """Build enrollment output plans."""
    dp = config.request.dpid
    dp_max = config.request.dp_max_date
    md = bridged.get(SubCohort.MEDICAL_AND_DRUG.value)
    outputs: list[tuple[str, DataFrame]] = []

    if md is None:
        return outputs

    outputs.append(
        (
            "enr_pat_covlength_md",
            enrollment.enr_pat_covlength_md(md, dp),
        )
    )
    outputs.append(
        (
            "enr_patid_ct_md",
            enrollment.enr_patid_ct_md(md, dp),
        )
    )
    outputs.append(
        (
            "enr_pat_covyears_md",
            enrollment.enr_pat_covyears_md(md, dp),
        )
    )
    outputs.append(
        (
            "enr_pat_enrcount_md",
            enrollment.enr_pat_enrcount_md(md, dp),
        )
    )
    outputs.append(
        (
            "enr_active_patid_ct_md",
            enrollment.enr_active_patid_ct_md(md, dp, dp_max),
        )
    )
    return outputs


def _run_demographic_outputs(
    manifest: InputManifest,
    bridged: dict[str, DataFrame],
    config: Config,
) -> list[tuple[str, DataFrame]]:
    """Build demographic output plans after validation."""
    dp = config.request.dpid
    dp_max = config.request.dp_max_date
    md = bridged[SubCohort.MEDICAL_AND_DRUG.value]
    dem_df = manifest.get(Domain.DEMOGRAPHIC.value)

    # Validate one demographic row per patient (the single named action)
    projected_dem = dem_df.select("patid", "birth_date", "sex", "race", "hispanic")
    validation_result = validate_one_demographic_per_patient(projected_dem)

    logger.info(
        "demographic validation completed in %.3fs",
        validation_result.elapsed_seconds,
    )

    # Build latest-span selection
    latest = demographic.select_latest_span(md)

    outputs: list[tuple[str, DataFrame]] = []
    outputs.append(
        (
            "dem_pat_lstagecount_md",
            demographic.dem_pat_lstagecount_md(latest, projected_dem, dp, dp_max),
        )
    )
    outputs.append(
        (
            "dem_pat_actagect_md",
            demographic.dem_pat_actagect_md(latest, projected_dem, dp, dp_max),
        )
    )

    # dem_catvars_md uses distinct md patient set
    distinct_md = md.select("patid").distinct()
    outputs.append(
        (
            "dem_catvars_md",
            demographic.dem_catvars_md(distinct_md, projected_dem, dp),
        )
    )
    return outputs


def _run_dispensing_outputs(
    manifest: InputManifest,
    bridged: dict[str, DataFrame],
    config: Config,
) -> list[tuple[str, DataFrame]]:
    """Build dispensing output plans."""
    dp = config.request.dpid
    dis_df = manifest.get(Domain.DISPENSING.value)
    md = bridged[SubCohort.MEDICAL_AND_DRUG.value]
    d = bridged.get(SubCohort.DRUG_ONLY.value)

    outputs: list[tuple[str, DataFrame]] = []
    outputs.append(("dis_pat_rx_md", dispensing.dis_pat_rx_md(dis_df, md, dp)))
    if d is not None:
        outputs.append(("dis_pat_rx_d", dispensing.dis_pat_rx_d(dis_df, d, dp)))
    return outputs


def _run_encounter_outputs(
    manifest: InputManifest,
    bridged: dict[str, DataFrame],
    config: Config,
) -> list[tuple[str, DataFrame]]:
    """Build encounter output plans."""
    dp = config.request.dpid
    enc_df = manifest.get(Domain.ENCOUNTER.value)
    md = bridged[SubCohort.MEDICAL_AND_DRUG.value]
    return [("enc_pat_enccount_md", encounter.enc_pat_enccount_md(enc_df, md, dp))]


def _run_lab_outputs(
    manifest: InputManifest,
    bridged: dict[str, DataFrame],
    config: Config,
) -> list[tuple[str, DataFrame]]:
    """Build lab output plans."""
    dp = config.request.dpid
    lab_df = manifest.get(Domain.LAB.value)
    md = bridged[SubCohort.MEDICAL_AND_DRUG.value]
    return [("lab_pat_testct_md", lab.lab_pat_testct_md(lab_df, md, dp))]


def _run_death_outputs(
    manifest: InputManifest,
    bridged: dict[str, DataFrame],
    config: Config,
) -> list[tuple[str, DataFrame]]:
    """Build death output plans."""
    dp = config.request.dpid
    death_df = manifest.get(Domain.DEATH.value)
    md = bridged[SubCohort.MEDICAL_AND_DRUG.value]
    m = bridged.get(SubCohort.MEDICAL_ONLY.value)

    distinct_md = md.select("patid").distinct()
    outputs: list[tuple[str, DataFrame]] = [
        ("dth_dthct_md", death.dth_dthct_md(death_df, distinct_md, dp))
    ]
    if m is not None:
        distinct_m = m.select("patid").distinct()
        outputs.append(("dth_dthct_m", death.dth_dthct_m(death_df, distinct_m, dp)))
    return outputs


def _run_mil_outputs(
    manifest: InputManifest,
    config: Config,
    session: SparkSession,
) -> list[tuple[str, DataFrame]]:
    """Build MIL output plans after conflict validation."""
    dp = config.request.dpid
    mil_df = manifest.get(Domain.MIL.value)

    # Validate delivery attribute conflicts
    conflict_df = mil.mil_build_conflict_check(mil_df)
    conflicts = conflict_df.collect()
    if conflicts:
        raise DataValidationError(
            f"conflicting delivery attributes found in mil data; "
            f"found {len(conflicts)} conflicting deliveries"
        )

    return [("mil_linkage_rates", mil.mil_linkage_rates(mil_df, dp))]


def run_pipeline(config: Config) -> PipelineResult:
    """Execute the full analytical pipeline.

    This is a thin shell: resolve domains, create Spark, read inputs,
    validate schemas, build shared plans, execute writes, stop Spark,
    record metadata.
    """
    resolved = resolve_domains(config.selected_domains)
    output_names = resolve_outputs_for_domains(resolved)
    logger.info(
        "resolved domains: %s, outputs: %d",
        sorted(d.value for d in resolved),
        len(output_names),
    )

    started_at = datetime.now(UTC).isoformat()
    timer_start = time.monotonic()
    error_type: str | None = None
    error_message: str | None = None
    success = False

    try:
        with spark_session_scope(config.spark) as session:
            effective = get_effective_settings(session)
            logger.info("spark session created with %d settings", len(effective))

            manifest = read_inputs(session, resolved, config.input_paths)

            # Build shared enrollment plans if needed
            bridged: dict[str, DataFrame] = {}
            if Domain.ENROLLMENT in resolved:
                bridged = _build_enrollment_plans(manifest, config)

            # Build output plans in domain order
            output_plans: list[tuple[str, DataFrame]] = []

            if Domain.ENROLLMENT in resolved:
                output_plans.extend(_run_enrollment_outputs(bridged, config))

            if Domain.DEMOGRAPHIC in resolved:
                output_plans.extend(_run_demographic_outputs(manifest, bridged, config))

            if Domain.DISPENSING in resolved:
                output_plans.extend(_run_dispensing_outputs(manifest, bridged, config))

            if Domain.ENCOUNTER in resolved:
                output_plans.extend(_run_encounter_outputs(manifest, bridged, config))

            if Domain.LAB in resolved:
                output_plans.extend(_run_lab_outputs(manifest, bridged, config))

            if Domain.DEATH in resolved:
                output_plans.extend(_run_death_outputs(manifest, bridged, config))

            if Domain.MIL in resolved:
                output_plans.extend(_run_mil_outputs(manifest, config, session))

            # Write outputs in deterministic sequence
            for name, df in output_plans:
                write_output(
                    df,
                    config.output_root,
                    name,
                    mode=config.write.mode,
                    num_partitions=config.spark.output_partitions,
                )

        success = True

    except (ConfigError, SchemaError, DataValidationError, OutputError) as exc:
        error_type = classify_exception(exc)
        error_message = str(exc)
        logger.error("pipeline failed: %s", error_message)
    except Exception as exc:
        error_type = classify_exception(exc)
        error_message = str(exc)
        logger.error("pipeline failed with unexpected error: %s", error_message)

    elapsed = time.monotonic() - timer_start
    finished_at = datetime.now(UTC).isoformat()

    return PipelineResult(
        success=success,
        elapsed_seconds=round(elapsed, 3),
        selected_domains=sorted(d.value for d in resolved),
        selected_outputs=output_names,
        output_root=config.output_root,
        error_type=error_type,
        error_message=error_message,
        python_version=sys.version.split()[0],
        pyspark_version=_safe_version("__version__", "pyspark"),
        java_version=_java_version(),
        spark_version=_safe_version("version", "pyspark"),
        effective_spark_settings={},  # populated by benchmark runner if available
        started_at=started_at,
        finished_at=finished_at,
    )


def write_run_result(result: PipelineResult, path: str | Path) -> None:
    """Write a run result as machine-readable JSON atomically."""
    import json

    data = {
        "success": result.success,
        "elapsed_seconds": result.elapsed_seconds,
        "selected_domains": result.selected_domains,
        "selected_outputs": result.selected_outputs,
        "output_root": result.output_root,
        "error_type": result.error_type,
        "error_message": result.error_message,
        "python_version": result.python_version,
        "pyspark_version": result.pyspark_version,
        "java_version": result.java_version,
        "spark_version": result.spark_version,
        "effective_spark_settings": result.effective_spark_settings,
        "started_at": result.started_at,
        "finished_at": result.finished_at,
    }
    p = Path(path)
    tmp = p.with_suffix(".tmp")
    with tmp.open("w") as f:
        json.dump(data, f, indent=2)
    tmp.replace(p)

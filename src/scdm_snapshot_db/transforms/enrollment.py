# pattern: Functional Core
"""Enrollment transformations with corrected running-maximum interval bridging.

These functions construct lazy DataFrame plans from input DataFrames and
scalar configuration. They do not create Spark sessions, trigger actions,
read/write files, or access the environment.

The corrected bridging uses a running maximum of ``enr_end`` over all
preceding rows (per patient, ordered by start/end) rather than the source
SQL's ``LAG(enr_end)`` which only considers the immediately preceding row.
This correctly handles nested intervals where a short interval appears
between longer ones.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pyspark.sql.types import LongType, StringType

__all__ = [
    "GAP_THRESHOLD_DAYS",
    "filter_medical_only",
    "filter_medical_and_drug",
    "filter_drug_only",
    "clean_intervals",
    "bridge_intervals",
    "enr_pat_covlength_md",
    "enr_patid_ct_md",
    "enr_pat_covyears_md",
    "enr_pat_enrcount_md",
    "enr_active_patid_ct_md",
]


GAP_THRESHOLD_DAYS = 46


# ─── Cohort filters ───────────────────────────────────────────────────────────


def filter_medical_only(enrollment_df: DataFrame) -> DataFrame:
    """Filter to medical-only cohort: ``medcov = 'y'``."""
    return enrollment_df.filter(F.lower(F.col("medcov")) == "y")


def filter_medical_and_drug(enrollment_df: DataFrame) -> DataFrame:
    """Filter to medical-and-drug cohort: ``drugcov = 'y' AND medcov = 'y'``."""
    return enrollment_df.filter(
        (F.lower(F.col("drugcov")) == "y")
        & (F.lower(F.col("medcov")) == "y")
    )


def filter_drug_only(enrollment_df: DataFrame) -> DataFrame:
    """Filter to drug-only cohort: ``drugcov = 'y'``."""
    return enrollment_df.filter(F.lower(F.col("drugcov")) == "y")


# ─── Interval cleaning ────────────────────────────────────────────────────────


def clean_intervals(df: DataFrame) -> DataFrame:
    """Normalize and deduplicate enrollment intervals.

    - Requires non-null patid, enr_start, enr_end
    - Rejects intervals where ``enr_end < enr_start``
    - Deduplicates by ``(patid, enr_start, enr_end)``
    """
    return (
        df
        .filter(
            F.col("patid").isNotNull()
            & F.col("enr_start").isNotNull()
            & F.col("enr_end").isNotNull()
            & (F.col("enr_end") >= F.col("enr_start"))
        )
        .select("patid", "enr_start", "enr_end")
        .distinct()
    )


# ─── Corrected interval bridging ──────────────────────────────────────────────


def bridge_intervals(df: DataFrame) -> DataFrame:
    """Bridge enrollment intervals using running-maximum gap logic.

    Replaces the source SQL's ``LAG(enr_end)`` with a running maximum of
    ``enr_end`` over all preceding rows per patient. A new span begins only
    when ``enr_start`` is more than ``GAP_THRESHOLD_DAYS`` after that
    running maximum.

    Returns columns: ``patid``, ``_enr_start``, ``_enr_end``, ``_span_id``.

    The result is ordered/non-overlapping per patient and is invariant to
    input row ordering because the window sorts deterministically.
    """
    cleaned = clean_intervals(df)

    w = Window.partitionBy("patid").orderBy("enr_start", "enr_end")

    # Running maximum of enr_end over ALL preceding rows (not just LAG)
    prior_max_end = F.max("enr_end").over(w.rowsBetween(Window.UNBOUNDED_PRECEDING, -1))

    # Flag rows that start a new span
    new_span = F.when(
        prior_max_end.isNull()
        | (F.col("enr_start") > F.date_add(prior_max_end, GAP_THRESHOLD_DAYS)),
        1,
    ).otherwise(0)

    # Assign span IDs via cumulative sum
    span_id = F.sum(new_span).over(
        w.rowsBetween(Window.UNBOUNDED_PRECEDING, Window.CURRENT_ROW)
    )

    with_spans = cleaned.withColumn("_span_id", span_id)

    # Aggregate each patient/span to min start / max end
    bridged = (
        with_spans
        .groupBy("patid", "_span_id")
        .agg(
            F.min("enr_start").alias("_enr_start"),
            F.max("enr_end").alias("_enr_end"),
        )
        .orderBy("patid", "_enr_start")
    )

    return bridged


# ─── Enrollment output plans ──────────────────────────────────────────────────


def enr_pat_covlength_md(bridged_md: DataFrame, dp: str) -> DataFrame:
    """Distribution of total inclusive enrollment length per patient.

    Corrected: inclusive span duration ``datediff(end, start) + 1``,
    summed by patient, then distributed by total length.
    """
    total_length = (
        bridged_md
        .withColumn("span_days", F.datediff(F.col("_enr_end"), F.col("_enr_start")) + 1)
        .groupBy("patid")
        .agg(F.sum("span_days").alias("total_length"))
    )
    return (
        total_length
        .groupBy(F.lit(dp).alias("dp"))
        .agg(
            F.col("total_length").cast("long").alias("total_length"),
            F.count(F.lit(1)).cast("long").alias("count"),
        )
        .select("dp", "total_length", "count")
        .orderBy("total_length")
    )


def enr_patid_ct_md(bridged_md: DataFrame, dp: str) -> DataFrame:
    """Distinct medical-and-drug patient count."""
    distinct_patids = bridged_md.select("patid").distinct()
    return (
        distinct_patids
        .groupBy(F.lit(dp).alias("dp"))
        .agg(F.count(F.lit(1)).cast("long").alias("count"))
        .select("dp", "count")
    )


def enr_pat_covyears_md(bridged_md: DataFrame, dp: str) -> DataFrame:
    """Distinct patient-year coverage counts.

    Corrected: explode inclusive calendar years covered by each bridged
    span, deduplicate ``(patid, year)``, and count each patient at most
    once per calendar year.
    """
    with_years = (
        bridged_md
        .withColumn("start_year", F.year("_enr_start"))
        .withColumn("end_year", F.year("_enr_end"))
        .withColumn(
            "year_seq",
            F.sequence(F.col("start_year"), F.col("end_year"), F.lit(1)),
        )
        .withColumn("year", F.explode("year_seq"))
        .select("patid", F.col("year").alias("year"))
        .distinct()
    )
    return (
        with_years
        .groupBy(F.lit(dp).alias("dp"), F.col("year").cast("int").alias("year"))
        .agg(F.count(F.lit(1)).cast("long").alias("count"))
        .select("dp", "year", "count")
        .orderBy("year")
    )


def enr_pat_enrcount_md(bridged_md: DataFrame, dp: str) -> DataFrame:
    """Distribution of distinct bridged enrollment spans per patient.

    Counts distinct ``_span_id`` per patient, then aggregates the
    patient distribution by span count.
    """
    span_counts = (
        bridged_md
        .groupBy("patid")
        .agg(F.countDistinct("_span_id").cast("long").alias("enr_count"))
    )
    return (
        span_counts
        .groupBy(
            F.lit(dp).alias("dp"),
            F.col("enr_count").cast("long").alias("enr_count"),
        )
        .agg(F.count(F.lit(1)).cast("long").alias("count"))
        .select("dp", "enr_count", "count")
        .orderBy("enr_count")
    )


def enr_active_patid_ct_md(
    bridged_md: DataFrame,
    dp: str,
    dp_max_date: date,
) -> DataFrame:
    """Count distinct medical-and-drug patients with any bridged span
    ending on or after ``dp_max_date``.
    """
    active = (
        bridged_md
        .filter(F.col("_enr_end") >= F.lit(dp_max_date))
        .select("patid")
        .distinct()
    )
    return (
        active
        .groupBy(F.lit(dp).alias("dp"))
        .agg(F.count(F.lit(1)).cast("long").alias("count"))
        .select("dp", "count")
    )

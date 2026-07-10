# pattern: Functional Core
"""Lab transformations with corrected date precedence.

Derives ``test_dt = COALESCE(to_date(lab_dt), to_date(result_dt), to_date(order_dt))``
so every operand has the same DateType. Timestamp conversion uses the
configured session timezone. Malformed non-null strings become null after
``to_date`` and are filtered only after derivation.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .event_in_window import count_distribution, events_in_window

__all__ = ["lab_pat_testct_md"]


def _derive_test_dt(lab_df: DataFrame) -> DataFrame:
    """Derive ``test_dt`` from candidate date columns via COALESCE.

    Each candidate is cast to date: ``DateType`` passes through,
    ``TimestampType`` converts using session timezone, and strings are
    parsed by ``to_date`` (malformed strings become null).
    """
    return lab_df.withColumn(
        "test_dt",
        F.coalesce(
            F.to_date(F.col("lab_dt")),
            F.to_date(F.col("result_dt")),
            F.to_date(F.col("order_dt")),
        ),
    )


def _pre_aggregate_lab(lab_df: DataFrame) -> DataFrame:
    """Derive test_dt, filter null, aggregate by patient and date."""
    with_test_dt = _derive_test_dt(lab_df)
    return (
        with_test_dt
        .filter(F.col("test_dt").isNotNull())
        .groupBy("patid", "test_dt")
        .agg(F.count(F.lit(1)).cast("long").alias("_count"))
    )


def lab_pat_testct_md(lab_df: DataFrame, bridged_md: DataFrame, dp: str) -> DataFrame:
    """Lab test count distribution for medical-and-drug cohort."""
    pre = _pre_aggregate_lab(lab_df)
    per_patient = events_in_window(pre, bridged_md, "test_dt")
    return count_distribution(per_patient, dp, dimension_col="lab_count").orderBy("lab_count")

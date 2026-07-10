# pattern: Functional Core
"""Dispensing transformations: count events within enrollment windows."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .event_in_window import count_distribution, events_in_window

__all__ = ["dis_pat_rx_md", "dis_pat_rx_d"]


def _pre_aggregate_dispensing(dispensing_df: DataFrame) -> DataFrame:
    """Aggregate dispensing rows by patient and date."""
    return (
        dispensing_df
        .filter(F.col("rxdate").isNotNull())
        .groupBy("patid", "rxdate")
        .agg(F.count(F.lit(1)).cast("long").alias("_count"))
    )


def dis_pat_rx_md(dispensing_df: DataFrame, bridged_md: DataFrame, dp: str) -> DataFrame:
    """Dispensing count distribution for medical-and-drug cohort."""
    pre = _pre_aggregate_dispensing(dispensing_df)
    per_patient = events_in_window(pre, bridged_md, "rxdate")
    return count_distribution(per_patient, dp, dimension_col="rx_count").orderBy("rx_count")


def dis_pat_rx_d(dispensing_df: DataFrame, bridged_d: DataFrame, dp: str) -> DataFrame:
    """Dispensing count distribution for drug-only cohort."""
    pre = _pre_aggregate_dispensing(dispensing_df)
    per_patient = events_in_window(pre, bridged_d, "rxdate")
    return count_distribution(per_patient, dp, dimension_col="rx_count").orderBy("rx_count")

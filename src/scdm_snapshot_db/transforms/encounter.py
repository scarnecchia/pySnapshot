# pattern: Functional Core
"""Encounter transformations: count events within enrollment windows."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .event_in_window import count_distribution, events_in_window

__all__ = ["enc_pat_enccount_md"]


def _pre_aggregate_encounter(encounter_df: DataFrame) -> DataFrame:
    """Aggregate encounter rows by patient and admission date."""
    return (
        encounter_df.filter(F.col("adate").isNotNull())
        .groupBy("patid", "adate")
        .agg(F.count(F.lit(1)).cast("long").alias("_count"))
    )


def enc_pat_enccount_md(encounter_df: DataFrame, bridged_md: DataFrame, dp: str) -> DataFrame:
    """Encounter count distribution for medical-and-drug cohort."""
    pre = _pre_aggregate_encounter(encounter_df)
    per_patient = events_in_window(pre, bridged_md, "adate")
    return count_distribution(per_patient, dp, dimension_col="enc_count").orderBy("enc_count")

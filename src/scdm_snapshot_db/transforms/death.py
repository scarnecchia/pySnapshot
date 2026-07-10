# pattern: Functional Core
"""Death transformations with distinct cohort-membership semantics.

Uses semi-join so duplicate enrollment membership rows cannot inflate
death records. Counts distinct death records for patients in each cohort.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

__all__ = ["dth_dthct_m", "dth_dthct_md"]


def _count_deaths(death_df: DataFrame, distinct_patids: DataFrame, dp: str) -> DataFrame:
    """Count death records for patients in the given distinct set.

    Uses a semi-join (``inner`` join on distinct patids) so that even if
    the enrollment source has duplicate membership rows, death records
    are counted exactly once per matching patient.
    """
    # distinct_patids is already distinct; join inflates only if death_df
    # has multiple rows per patient, which is the intended count
    return (
        death_df.join(distinct_patids, on="patid", how="inner")
        .groupBy(F.lit(dp).alias("dp"))
        .agg(F.count(F.lit(1)).cast("long").alias("count"))
        .select("dp", "count")
    )


def dth_dthct_md(death_df: DataFrame, distinct_md_patids: DataFrame, dp: str) -> DataFrame:
    """Death count for medical-and-drug cohort."""
    return _count_deaths(death_df, distinct_md_patids, dp)


def dth_dthct_m(death_df: DataFrame, distinct_m_patids: DataFrame, dp: str) -> DataFrame:
    """Death count for medical-only cohort."""
    return _count_deaths(death_df, distinct_m_patids, dp)

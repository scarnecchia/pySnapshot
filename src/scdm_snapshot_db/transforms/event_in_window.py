# pattern: Functional Core
"""Shared interval-join pattern for event-in-enrollment-window counting.

Narrowly scoped: takes pre-aggregated event counts and bridged enrollment
spans, performs the equi-join + date-containment, and returns per-patient
event totals.

Because bridged enrollment spans are non-overlapping per patient, each
event matches at most one span, preventing double counting.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

__all__ = ["count_distribution", "events_in_window"]


def events_in_window(
    pre_aggregated: DataFrame,
    bridged: DataFrame,
    event_date_col: str,
    event_count_col: str = "_count",
    patid_col: str = "patid",
) -> DataFrame:
    """Join pre-aggregated events to bridged enrollment spans.

    Performs equi-join on ``patid`` plus date containment
    (``event_date BETWEEN _enr_start AND _enr_end``).

    Returns per-patient total event counts with column ``_total``.
    """
    return (
        pre_aggregated.join(bridged, on=patid_col, how="inner")
        .filter(
            (F.col(event_date_col) >= F.col("_enr_start"))
            & (F.col(event_date_col) <= F.col("_enr_end"))
        )
        .groupBy(patid_col)
        .agg(F.sum(event_count_col).cast("long").alias("_total"))
    )


def count_distribution(
    per_patient_counts: DataFrame,
    dp: str,
    count_col: str = "_total",
    output_count_col: str = "count",
    dimension_col: str | None = None,
) -> DataFrame:
    """Aggregate per-patient counts into a distribution.

    Groups by the dimension column (the per-patient count value) and counts
    patients. Produces explicit ``dp``, dimension, and ``count`` columns.
    """
    dim_name = dimension_col or count_col
    group_cols = [
        F.lit(dp).alias("dp"),
        F.col(count_col).cast("long").alias(dim_name),
    ]
    result = per_patient_counts.groupBy(*group_cols).agg(
        F.count(F.lit(1)).cast("long").alias(output_count_col)
    )
    final_cols = ["dp", dimension_col or count_col, output_count_col]
    return result.select(*final_cols)

# pattern: Functional Core
"""Demographic transformations with corrected latest-span selection.

These functions construct lazy DataFrame plans. They do not trigger actions.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

__all__ = [
    "dem_age_category",
    "dem_catvars_md",
    "dem_pat_actagect_md",
    "dem_pat_lstagecount_md",
    "select_latest_span",
]


def select_latest_span(bridged_md: DataFrame) -> DataFrame:
    """For each patient, select the bridged span with max ``_enr_start``.

    Ties are broken by max ``_enr_end``. Returns one row per patient with
    columns: ``patid``, ``_enr_start``, ``_enr_end``.
    """
    w = Window.partitionBy("patid").orderBy(
        F.col("_enr_start").desc(),
        F.col("_enr_end").desc(),
    )
    return (
        bridged_md.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select("patid", "_enr_start", "_enr_end")
    )


def dem_age_category(
    latest_span: DataFrame,
    demographic_df: DataFrame,
) -> DataFrame:
    """Join latest spans to demographics and compute age category.

    Computes completed-year age using ``floor(months_between / 12)``.
    Null ``birth_date`` yields ``MISSING``. Negative ages yield ``NEGATIVE``.
    Age 80 and above yields ``80+ yrs`` (corrected: source missed exactly 80).

    Returns columns: ``patid``, ``age_category``, ``_dpMaxenroll``,
    ``_enr_start``, ``_enr_end``.
    """
    joined = latest_span.join(demographic_df, on="patid", how="inner")

    with_age = joined.withColumn(
        "age",
        F.floor(F.months_between(F.col("_enr_start"), F.col("birth_date")) / 12),
    )

    with_category = with_age.withColumn(
        "age_category",
        F.when(F.col("birth_date").isNull(), F.lit("MISSING"))
        .when(F.col("age") < 0, F.lit("NEGATIVE"))
        .when(F.col("age").between(0, 1), F.lit("0-1 yrs"))
        .when(F.col("age").between(2, 4), F.lit("2-4 yrs"))
        .when(F.col("age").between(5, 9), F.lit("5-9 yrs"))
        .when(F.col("age").between(10, 14), F.lit("10-14 yrs"))
        .when(F.col("age").between(15, 19), F.lit("15-19 yrs"))
        .when(F.col("age").between(20, 24), F.lit("20-24 yrs"))
        .when(F.col("age").between(25, 29), F.lit("25-29 yrs"))
        .when(F.col("age").between(30, 34), F.lit("30-34 yrs"))
        .when(F.col("age").between(35, 39), F.lit("35-39 yrs"))
        .when(F.col("age").between(40, 44), F.lit("40-44 yrs"))
        .when(F.col("age").between(45, 49), F.lit("45-49 yrs"))
        .when(F.col("age").between(50, 54), F.lit("50-54 yrs"))
        .when(F.col("age").between(55, 59), F.lit("55-59 yrs"))
        .when(F.col("age").between(60, 64), F.lit("60-64 yrs"))
        .when(F.col("age").between(65, 69), F.lit("65-69 yrs"))
        .when(F.col("age").between(70, 74), F.lit("70-74 yrs"))
        .when(F.col("age").between(75, 79), F.lit("75-79 yrs"))
        .when(F.col("age") >= 80, F.lit("80+ yrs"))
        .otherwise(F.lit("MISSING")),
    )

    return with_category.select(
        "patid",
        "age_category",
        "_enr_start",
        "_enr_end",
        "birth_date",
    )


def _mark_active(df: DataFrame, dp_max_date: date) -> DataFrame:
    """Add ``_dpMaxenroll`` column: 1 when ``_enr_end >= dp_max_date``."""
    return df.withColumn(
        "_dpMaxenroll",
        F.when(F.col("_enr_end") >= F.lit(dp_max_date), 1).otherwise(0),
    )


def dem_pat_lstagecount_md(
    latest_span: DataFrame,
    demographic_df: DataFrame,
    dp: str,
    dp_max_date: date,
) -> DataFrame:
    """Latest-stage age category counts.

    Every medical-and-drug patient with one valid demographic row participates.
    Null birth_date yields ``MISSING`` and is retained (intentional deviation
    from source which drops null birth dates).
    """
    aged = dem_age_category(latest_span, demographic_df)
    return (
        aged.groupBy(F.lit(dp).alias("dp"), F.col("age_category"))
        .agg(F.count(F.lit(1)).cast("long").alias("count"))
        .select("dp", "age_category", "count")
        .orderBy("age_category")
    )


def dem_pat_actagect_md(
    latest_span: DataFrame,
    demographic_df: DataFrame,
    dp: str,
    dp_max_date: date,
) -> DataFrame:
    """Active patient age category counts.

    Same as lstagecount but filtered to active patients (latest span end
    >= ``dp_max_date``). Null birth_date patients retained as ``MISSING``.
    """
    aged = dem_age_category(latest_span, demographic_df)
    active = _mark_active(aged, dp_max_date).filter(F.col("_dpMaxenroll") == 1)
    return (
        active.groupBy(F.lit(dp).alias("dp"), F.col("age_category"))
        .agg(F.count(F.lit(1)).cast("long").alias("count"))
        .select("dp", "age_category", "count")
        .orderBy("age_category")
    )


def dem_catvars_md(
    distinct_md_patids: DataFrame,
    demographic_df: DataFrame,
    dp: str,
) -> DataFrame:
    """Categorical variable counts for sex, race, hispanic.

    Joins categorical columns to the distinct medical-and-drug patient set,
    then reshapes into long form using native Spark ``stack`` expression
    (not Python UDFs).
    """
    joined = distinct_md_patids.join(demographic_df, on="patid", how="inner").select(
        "patid",
        F.col("sex").alias("sex"),
        F.col("race").alias("race"),
        F.col("hispanic").alias("hispanic"),
    )

    # Native stack to reshape from wide to long
    stacked = joined.select(
        "patid",
        F.expr("stack(3, 'sex', sex, 'race', race, 'hispanic', hispanic)").alias(
            "variable", "value"
        ),
    )

    return (
        stacked.filter(F.col("value").isNotNull())
        .groupBy(
            F.lit(dp).alias("dp"),
            F.col("variable"),
            F.col("value"),
        )
        .agg(F.count(F.lit(1)).cast("long").alias("count"))
        .select("dp", "variable", "value", "count")
        .orderBy("variable", "value")
    )

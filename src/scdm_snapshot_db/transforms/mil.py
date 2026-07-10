# pattern: Functional Core
"""MIL (maternal-infant linkage) transformations.

Delivery grain is ``(mpatid, encounterid)``. Per-delivery attribute
consistency is validated before output writes. Uses native Spark
aggregations with a single union-based normalized grouping plan rather
than five independent source scans.

Corrected MIL maternal-age contract: ``MISSING`` for null, ``NEGATIVE``
for < 0, bands ``0-1 yrs``, ``2-4 yrs``, five-year bands ``5-9`` through
``50-54``, and ``55+ yrs`` for age >= 55. This is exhaustive and extends
the source's 10-54-only categories.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

__all__ = [
    "mil_age_category",
    "mil_build_conflict_check",
    "mil_linkage_rates",
]


# ─── Age categories ───────────────────────────────────────────────────────────


def mil_age_category(age_col: str = "age") -> Column:
    """Return a Spark CASE expression for MIL maternal age category.

    Exhaustive corrected categories extending the source 10-54 range.
    """
    return (
        F.when(F.col(age_col).isNull(), F.lit("MISSING"))
        .when(F.col(age_col) < 0, F.lit("NEGATIVE"))
        .when(F.col(age_col).between(0, 1), F.lit("0-1 yrs"))
        .when(F.col(age_col).between(2, 4), F.lit("2-4 yrs"))
        .when(F.col(age_col).between(5, 9), F.lit("5-9 yrs"))
        .when(F.col(age_col).between(10, 14), F.lit("10-14 yrs"))
        .when(F.col(age_col).between(15, 19), F.lit("15-19 yrs"))
        .when(F.col(age_col).between(20, 24), F.lit("20-24 yrs"))
        .when(F.col(age_col).between(25, 29), F.lit("25-29 yrs"))
        .when(F.col(age_col).between(30, 34), F.lit("30-34 yrs"))
        .when(F.col(age_col).between(35, 39), F.lit("35-39 yrs"))
        .when(F.col(age_col).between(40, 44), F.lit("40-44 yrs"))
        .when(F.col(age_col).between(45, 49), F.lit("45-49 yrs"))
        .when(F.col(age_col).between(50, 54), F.lit("50-54 yrs"))
        .when(F.col(age_col) >= 55, F.lit("55+ yrs"))
        .otherwise(F.lit("MISSING"))
    )


# ─── Conflict detection ───────────────────────────────────────────────────────


def mil_build_conflict_check(mil_df: DataFrame) -> DataFrame:
    """Build a lazy DataFrame of delivery-level attribute conflicts.

    For each delivery and each attribute (``enctype``, ``birth_type``,
    ``age``, service year from ``adate``), requires the cardinality of the
    null-safe representation ``struct(is_null, value)`` to be exactly one.
    Returns rows where any attribute conflicts.

    This is a lazy plan; the caller (shell) must collect and raise.
    """

    def null_safe(col_name: str) -> Column:
        return F.struct(
            F.col(col_name).isNull().alias("is_null"),
            F.col(col_name).alias("value"),
        )

    per_delivery = (
        mil_df.filter(F.col("mpatid").isNotNull())
        .groupBy("mpatid", "encounterid")
        .agg(
            F.size(F.collect_set(null_safe("enctype"))).alias("_enctype_card"),
            F.size(F.collect_set(null_safe("birth_type"))).alias("_birth_type_card"),
            F.size(F.collect_set(null_safe("age"))).alias("_age_card"),
            F.size(F.collect_set(null_safe("adate"))).alias("_adate_card"),
        )
    )

    return per_delivery.filter(
        (F.col("_enctype_card") > 1)
        | (F.col("_birth_type_card") > 1)
        | (F.col("_age_card") > 1)
        | (F.col("_adate_card") > 1)
    )


# ─── Linkage rates output ─────────────────────────────────────────────────────


def mil_linkage_rates(mil_df: DataFrame, dp: str) -> DataFrame:
    """Build the MIL linkage rates output plan.

    Output schema:
    - ``dp: string``
    - ``variable: string`` (overall, age_category, enctype, year, birth_type)
    - ``value: string``
    - ``deliveries: long``
    - ``linked_deliveries: long``
    - ``distinct_infants_linked: long``
    - ``linkage_rate: decimal(9,6)``
    """
    # ── Per-delivery grain: attributes and linked flag ──────────────────
    delivery_grain = (
        mil_df.filter(F.col("mpatid").isNotNull())
        .groupBy("mpatid", "encounterid")
        .agg(
            F.first("enctype").alias("_enctype"),
            F.first("birth_type").alias("_birth_type"),
            F.first("age").alias("_age"),
            F.first("adate").alias("_adate"),
        )
        .withColumn("_age_category", mil_age_category("_age"))
        .withColumn(
            "_year",
            F.when(F.col("_adate").isNull(), F.lit(None)).otherwise(
                F.year("_adate").cast("string")
            ),
        )
    )

    # ── Per-delivery distinct infants ───────────────────────────────────
    delivery_infants = (
        mil_df.filter(F.col("mpatid").isNotNull() & F.col("cpatid").isNotNull())
        .select("mpatid", "encounterid", "cpatid")
        .distinct()
    )

    # ── Per-delivery is_linked flag ─────────────────────────────────────
    delivery_linked = (
        delivery_infants.groupBy("mpatid", "encounterid")
        .agg(F.countDistinct("cpatid").cast("long").alias("_infant_count"))
        .withColumn("_is_linked", F.lit(True))
    )

    delivery_full = delivery_grain.join(
        delivery_linked, on=["mpatid", "encounterid"], how="left"
    ).withColumn("_is_linked", F.coalesce(F.col("_is_linked"), F.lit(False)))

    # ── Build dimension rows via union ──────────────────────────────────
    base_cols = ["mpatid", "encounterid", "_is_linked"]

    def _null_to_missing(col_ref: Column) -> Column:
        return F.when(col_ref.isNull(), F.lit("MISSING")).otherwise(col_ref)

    overall_rows = delivery_full.select(
        F.lit("overall").alias("variable"),
        F.lit("overall").alias("value"),
        *base_cols,
    )
    age_rows = delivery_full.select(
        F.lit("age_category").alias("variable"),
        _null_to_missing(F.col("_age_category")).alias("value"),
        *base_cols,
    )
    enc_rows = delivery_full.select(
        F.lit("enctype").alias("variable"),
        _null_to_missing(F.col("_enctype")).alias("value"),
        *base_cols,
    )
    year_rows = delivery_full.select(
        F.lit("year").alias("variable"),
        _null_to_missing(F.col("_year")).alias("value"),
        *base_cols,
    )
    birth_rows = delivery_full.select(
        F.lit("birth_type").alias("variable"),
        _null_to_missing(F.col("_birth_type")).alias("value"),
        *base_cols,
    )

    dimensions = (
        overall_rows.unionByName(age_rows)
        .unionByName(enc_rows)
        .unionByName(year_rows)
        .unionByName(birth_rows)
    )

    # ── Aggregate deliveries and linked_deliveries ──────────────────────
    delivery_agg = dimensions.groupBy("variable", "value").agg(
        F.count(F.lit(1)).cast("long").alias("deliveries"),
        F.sum(F.when(F.col("_is_linked"), 1).otherwise(0)).cast("long").alias("linked_deliveries"),
    )

    # ── Aggregate distinct_infants_linked ───────────────────────────────
    dim_with_infants = dimensions.join(delivery_infants, on=["mpatid", "encounterid"], how="left")
    infant_agg = dim_with_infants.groupBy("variable", "value").agg(
        F.countDistinct("cpatid").cast("long").alias("distinct_infants_linked")
    )

    # ── Combine and compute linkage_rate ────────────────────────────────
    combined = delivery_agg.join(infant_agg, on=["variable", "value"], how="inner")

    return (
        combined.filter(F.col("deliveries") > 0)  # guard: groups emitted only when deliveries > 0
        .withColumn("dp", F.lit(dp))
        .withColumn(
            "linkage_rate",
            (F.col("linked_deliveries") / F.col("deliveries")).cast(DecimalType(9, 6)),
        )
        .select(
            "dp",
            "variable",
            "value",
            "deliveries",
            "linked_deliveries",
            "distinct_infants_linked",
            "linkage_rate",
        )
    )

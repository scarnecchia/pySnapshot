"""Synthetic Parquet fixture builder for tests.

Creates small Parquet datasets containing no real patient data.
Uses explicit Spark schemas for all identifiers, dates, counts, and decimals.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

if TYPE_CHECKING:
    pass

__all__ = [
    "write_enrollment_parquet",
    "write_demographic_parquet",
    "write_dispensing_parquet",
    "write_encounter_parquet",
    "write_lab_parquet",
    "write_death_parquet",
    "write_mil_parquet",
    "ENROLLMENT_SCHEMA",
    "DEMOGRAPHIC_SCHEMA",
    "DISPENSING_SCHEMA",
    "ENCOUNTER_SCHEMA",
    "LAB_SCHEMA",
    "DEATH_SCHEMA",
    "MIL_SCHEMA",
]

ENROLLMENT_SCHEMA = StructType([
    StructField("patid", StringType(), False),
    StructField("enr_start", DateType(), False),
    StructField("enr_end", DateType(), False),
    StructField("drugcov", StringType(), True),
    StructField("medcov", StringType(), True),
])

DEMOGRAPHIC_SCHEMA = StructType([
    StructField("patid", StringType(), False),
    StructField("birth_date", DateType(), True),
    StructField("sex", StringType(), True),
    StructField("race", StringType(), True),
    StructField("hispanic", StringType(), True),
])

DISPENSING_SCHEMA = StructType([
    StructField("patid", StringType(), False),
    StructField("rxdate", DateType(), True),
])

ENCOUNTER_SCHEMA = StructType([
    StructField("patid", StringType(), False),
    StructField("adate", DateType(), True),
])

LAB_SCHEMA = StructType([
    StructField("patid", StringType(), False),
    StructField("lab_dt", DateType(), True),
    StructField("result_dt", DateType(), True),
    StructField("order_dt", DateType(), True),
])

DEATH_SCHEMA = StructType([
    StructField("patid", StringType(), False),
    StructField("death_date", DateType(), True),
])

MIL_SCHEMA = StructType([
    StructField("mpatid", StringType(), True),
    StructField("encounter_id", StringType(), True),
    StructField("cpatid", StringType(), True),
    StructField("enc_type", StringType(), True),
    StructField("birth_type", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("adate", DateType(), True),
])


def _write_parquet(
    spark: SparkSession,
    path: str | Path,
    schema: StructType,
    rows: list[dict],
) -> str:
    """Write a list of dict rows as a Parquet dataset."""
    df = spark.createDataFrame(rows, schema=schema)
    out = str(path)
    df.coalesce(1).write.mode("overwrite").parquet(out)
    return out


def write_enrollment_parquet(
    spark: SparkSession,
    path: str | Path,
    rows: list[dict],
) -> str:
    """Write synthetic enrollment data."""
    return _write_parquet(spark, path, ENROLLMENT_SCHEMA, rows)


def write_demographic_parquet(
    spark: SparkSession,
    path: str | Path,
    rows: list[dict],
) -> str:
    """Write synthetic demographic data."""
    return _write_parquet(spark, path, DEMOGRAPHIC_SCHEMA, rows)


def write_dispensing_parquet(
    spark: SparkSession,
    path: str | Path,
    rows: list[dict],
) -> str:
    """Write synthetic dispensing data."""
    return _write_parquet(spark, path, DISPENSING_SCHEMA, rows)


def write_encounter_parquet(
    spark: SparkSession,
    path: str | Path,
    rows: list[dict],
) -> str:
    """Write synthetic encounter data."""
    return _write_parquet(spark, path, ENCOUNTER_SCHEMA, rows)


def write_lab_parquet(
    spark: SparkSession,
    path: str | Path,
    rows: list[dict],
) -> str:
    """Write synthetic lab data."""
    return _write_parquet(spark, path, LAB_SCHEMA, rows)


def write_death_parquet(
    spark: SparkSession,
    path: str | Path,
    rows: list[dict],
) -> str:
    """Write synthetic death data."""
    return _write_parquet(spark, path, DEATH_SCHEMA, rows)


def write_mil_parquet(
    spark: SparkSession,
    path: str | Path,
    rows: list[dict],
) -> str:
    """Write synthetic MIL data."""
    return _write_parquet(spark, path, MIL_SCHEMA, rows)

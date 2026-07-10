"""Tests for the output comparison utility (AC.14)."""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

from scdm_snapshot_db.comparison import compare_outputs
from scdm_snapshot_db.models import ComparisonStatus


def _write_simple_dataset(spark, path: str | Path, schema: StructType, rows: list[dict]) -> None:
    """Write a simple Parquet dataset."""
    df = spark.createDataFrame(rows, schema=schema)
    df.coalesce(1).write.mode("overwrite").parquet(str(path))


SCHEMA = StructType(
    [
        StructField("dp", StringType(), False),
        StructField("count", LongType(), False),
    ]
)


class TestComparison:
    def test_compare_exact_unordered_outputs(self, spark, tmp_path: Path) -> None:
        """Exact match is detected even with different row ordering."""
        actual = tmp_path / "actual" / "test_output"
        reference = tmp_path / "reference" / "test_output"
        _write_simple_dataset(
            spark,
            actual,
            SCHEMA,
            [
                {"dp": "X", "count": 10},
                {"dp": "X", "count": 20},
            ],
        )
        _write_simple_dataset(
            spark,
            reference,
            SCHEMA,
            [
                {"dp": "X", "count": 20},
                {"dp": "X", "count": 10},
            ],
        )
        result = compare_outputs(
            spark,
            str(tmp_path / "actual"),
            str(tmp_path / "reference"),
            output_names=["test_output"],
        )
        assert result.datasets[0].status == ComparisonStatus.EXACT_MATCH

    def test_compare_numeric_tolerance(self, spark, tmp_path: Path) -> None:
        """Numeric differences within tolerance are classified as tolerance match."""
        actual = tmp_path / "actual" / "test_output"
        reference = tmp_path / "reference" / "test_output"
        _write_simple_dataset(spark, actual, SCHEMA, [{"dp": "X", "count": 100}])
        _write_simple_dataset(spark, reference, SCHEMA, [{"dp": "X", "count": 101}])
        result = compare_outputs(
            spark,
            str(tmp_path / "actual"),
            str(tmp_path / "reference"),
            output_names=["test_output"],
            numeric_tolerance=2.0,
        )
        assert result.datasets[0].status == ComparisonStatus.NUMERIC_TOLERANCE_MATCH

    def test_compare_reports_value_deviations(self, spark, tmp_path: Path) -> None:
        """Substantive value differences are reported."""
        actual = tmp_path / "actual" / "test_output"
        reference = tmp_path / "reference" / "test_output"
        _write_simple_dataset(spark, actual, SCHEMA, [{"dp": "X", "count": 100}])
        _write_simple_dataset(spark, reference, SCHEMA, [{"dp": "X", "count": 200}])
        result = compare_outputs(
            spark,
            str(tmp_path / "actual"),
            str(tmp_path / "reference"),
            output_names=["test_output"],
        )
        assert result.datasets[0].status == ComparisonStatus.VALUE_DIFFERENCE
        assert not result.overall_equivalent

    def test_compare_reports_schema_deviations(self, spark, tmp_path: Path) -> None:
        """Schema differences are reported."""
        from pyspark.sql.types import DoubleType

        actual = tmp_path / "actual" / "test_output"
        reference = tmp_path / "reference" / "test_output"
        _write_simple_dataset(spark, actual, SCHEMA, [{"dp": "X", "count": 100}])
        diff_schema = StructType(
            [
                StructField("dp", StringType(), False),
                StructField("count", DoubleType(), False),
            ]
        )
        _write_simple_dataset(spark, reference, diff_schema, [{"dp": "X", "count": 100.0}])
        result = compare_outputs(
            spark,
            str(tmp_path / "actual"),
            str(tmp_path / "reference"),
            output_names=["test_output"],
        )
        assert result.datasets[0].status == ComparisonStatus.SCHEMA_DIFFERENCE

    def test_compare_missing_reference(self, spark, tmp_path: Path) -> None:
        """Missing reference is reported."""
        actual = tmp_path / "actual" / "test_output"
        _write_simple_dataset(spark, actual, SCHEMA, [{"dp": "X", "count": 100}])
        result = compare_outputs(
            spark,
            str(tmp_path / "actual"),
            str(tmp_path / "reference"),
            output_names=["test_output"],
        )
        assert result.datasets[0].status == ComparisonStatus.MISSING_REFERENCE

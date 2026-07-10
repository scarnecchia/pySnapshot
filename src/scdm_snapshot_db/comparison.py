# pattern: Imperative Shell
"""Parquet comparison utility for validating outputs against SAS results.

Compares Spark-native Parquet datasets against external SAS Parquet
outputs as unordered multisets with schema mapping and configured numeric
tolerance. Classifies exact matches, type-only differences, numeric-tolerance
matches, and substantive value differences.

Never declares overall equivalence when intentional corrected-contract
deviations differ.
"""

from __future__ import annotations

import logging
import math
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from .config_models import ALL_OUTPUT_NAMES
from .models import ComparisonDatasetResult, ComparisonResult, ComparisonStatus

logger = logging.getLogger(__name__)

__all__ = ["compare_outputs"]


def _read_parquet(spark: SparkSession, path: str | Path) -> DataFrame | None:
    """Read a Parquet dataset if it exists."""
    p = Path(path)
    if not p.exists():
        return None
    try:
        return spark.read.parquet(str(path))
    except Exception:
        return None


def _compare_column_types(
    actual_fields: dict[str, str],
    reference_fields: dict[str, str],
) -> list[str]:
    """Compare column types and return differences."""
    differences: list[str] = []
    all_cols = set(actual_fields.keys()) | set(reference_fields.keys())
    for col in sorted(all_cols):
        a_type = actual_fields.get(col)
        r_type = reference_fields.get(col)
        if col not in reference_fields:
            differences.append(f"column '{col}' in actual but not reference")
        elif col not in actual_fields:
            differences.append(f"column '{col}' in reference but not actual")
        elif a_type != r_type:
            differences.append(f"column '{col}' type differs: actual={a_type}, reference={r_type}")
    return differences


def _is_numeric_type(type_name: str) -> bool:
    """Check if a Spark type name is numeric."""
    return type_name in ("long", "integer", "double", "decimal", "float", "short")


def _rows_as_dicts(df: DataFrame) -> list[dict[str, object]]:
    """Collect DataFrame rows as dicts, converting values for comparison."""
    rows = df.collect()
    result: list[dict[str, object]] = []
    for row in rows:
        d = {}
        for field in df.schema.fields:
            val = row[field.name]
            d[field.name] = val
        result.append(d)
    return result


def _values_equal(a: object, b: object, numeric_tolerance: float) -> bool:
    """Compare two values with numeric tolerance."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if math.isnan(a) and math.isnan(b):
            return True
        return abs(a - b) <= numeric_tolerance
    return a == b


def _compare_datasets(
    actual: DataFrame,
    reference: DataFrame,
    numeric_tolerance: float,
) -> ComparisonStatus:
    """Compare two DataFrames as unordered multisets."""
    # Check schema
    actual_fields = {f.name: f.dataType.typeName() for f in actual.schema.fields}
    reference_fields = {f.name: f.dataType.typeName() for f in reference.schema.fields}

    type_diffs = _compare_column_types(actual_fields, reference_fields)
    if type_diffs:
        return ComparisonStatus.SCHEMA_DIFFERENCE

    # Collect as multisets for comparison
    actual_rows = _rows_as_dicts(actual)
    reference_rows = _rows_as_dicts(reference)

    if len(actual_rows) != len(reference_rows):
        return ComparisonStatus.VALUE_DIFFERENCE

    # Try exact match first
    actual_sorted = sorted(actual_rows, key=lambda r: tuple(sorted(r.items())))
    reference_sorted = sorted(reference_rows, key=lambda r: tuple(sorted(r.items())))

    exact = all(
        _values_equal(a, b, 0.0) for a, b in zip(actual_sorted, reference_sorted, strict=False)
    )
    if exact:
        return ComparisonStatus.EXACT_MATCH

    # Try numeric tolerance
    tolerance = all(
        _values_equal(a, b, numeric_tolerance)
        for a, b in zip(actual_sorted, reference_sorted, strict=False)
    )
    if tolerance:
        return ComparisonStatus.NUMERIC_TOLERANCE_MATCH

    return ComparisonStatus.VALUE_DIFFERENCE


def compare_outputs(
    spark: SparkSession,
    actual_root: str | Path,
    reference_root: str | Path,
    output_names: list[str] | None = None,
    numeric_tolerance: float = 0.0,
) -> ComparisonResult:
    """Compare Spark outputs against reference outputs.

    Each named output is compared as an unordered multiset. The result
    classifies each dataset and reports overall equivalence. Intentional
    corrected-contract deviations appear as value differences and do not
    contribute to equivalence.
    """
    names = output_names or ALL_OUTPUT_NAMES
    actual_root = Path(actual_root)
    reference_root = Path(reference_root)

    results: list[ComparisonDatasetResult] = []

    for name in names:
        actual_path = actual_root / name
        reference_path = reference_root / name

        actual_df = _read_parquet(spark, actual_path)
        reference_df = _read_parquet(spark, reference_path)

        if actual_df is None:
            results.append(
                ComparisonDatasetResult(
                    name=name,
                    status=ComparisonStatus.MISSING_ACTUAL,
                    details=f"actual output not found at {actual_path}",
                )
            )
            continue

        if reference_df is None:
            results.append(
                ComparisonDatasetResult(
                    name=name,
                    status=ComparisonStatus.MISSING_REFERENCE,
                    details=f"reference output not found at {reference_path}",
                )
            )
            continue

        status = _compare_datasets(actual_df, reference_df, numeric_tolerance)
        results.append(
            ComparisonDatasetResult(
                name=name,
                status=status,
                details=f"{status.value}: {name}",
            )
        )

    exact = sum(1 for r in results if r.status == ComparisonStatus.EXACT_MATCH)
    numeric = sum(1 for r in results if r.status == ComparisonStatus.NUMERIC_TOLERANCE_MATCH)
    schema_diff = sum(1 for r in results if r.status == ComparisonStatus.SCHEMA_DIFFERENCE)
    value_diff = sum(1 for r in results if r.status == ComparisonStatus.VALUE_DIFFERENCE)
    missing_ref = sum(1 for r in results if r.status == ComparisonStatus.MISSING_REFERENCE)
    missing_act = sum(1 for r in results if r.status == ComparisonStatus.MISSING_ACTUAL)

    # Overall equivalent only when all datasets are exact or numeric-tolerance matches
    overall = exact + numeric == len(results) and len(results) > 0

    return ComparisonResult(
        datasets=results,
        exact_match_count=exact,
        numeric_tolerance_match_count=numeric,
        schema_difference_count=schema_diff,
        value_difference_count=value_diff,
        missing_reference_count=missing_ref,
        missing_actual_count=missing_act,
        overall_equivalent=overall,
    )

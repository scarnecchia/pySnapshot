# pattern: Functional Core
"""Pure required-column and type-compatibility policy over Spark schemas.

All functions accept ``StructType`` metadata and return ``list[str]`` findings
or raise ``SchemaError``. No Spark actions, no IO.

Type rationale (informed by the ``scdm_convert`` polars-readstat pipeline):

- **Date columns** accept only ``{"date"}`` because polars-readstat resolves
  SAS date-formatted numerics (canonical ``Float64``) to ``Date`` in the
  parquet output. If a date arrives as ``DoubleType`` the SAS file lacked
  the date format and validation should catch it, not silently pass it.
- **Lab date columns** additionally accept ``{"string"}`` because the lab
  transform uses ``F.to_date()`` which parses ISO date strings. Malformed
  strings become null and are filtered downstream.
- **ID columns** accept ``{"int", "long"}`` matching the ``Int64`` polars
  type in the canonical ``scdm_schema.json`` registry.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .error_classification import SchemaError

if TYPE_CHECKING:
    from pyspark.sql.types import StructField, StructType

__all__ = [
    "REQUIRED_COLUMNS",
    "check_required_columns",
    "check_type_compatibility",
    "validate_schema",
]


# Required columns per domain
REQUIRED_COLUMNS: dict[str, list[tuple[str, set[str]]]] = {
    "enrollment": [
        ("patid", {"int", "long"}),
        ("enr_start", {"date"}),
        ("enr_end", {"date"}),
        ("drugcov", {"string"}),
        ("medcov", {"string"}),
    ],
    "demographic": [
        ("patid", {"int", "long"}),
        ("birth_date", {"date"}),
        ("sex", {"string"}),
        ("race", {"string"}),
        ("hispanic", {"string"}),
    ],
    "dispensing": [
        ("patid", {"int", "long"}),
        ("rxdate", {"date"}),
    ],
    "encounter": [
        ("patid", {"int", "long"}),
        ("adate", {"date"}),
    ],
    "lab": [
        ("patid", {"int", "long"}),
        ("lab_dt", {"date", "timestamp", "string"}),
        ("result_dt", {"date", "timestamp", "string"}),
        ("order_dt", {"date", "timestamp", "string"}),
    ],
    "death": [
        ("patid", {"int", "long"}),
        ("deathdt", {"date"}),
    ],
    "mil": [
        ("mpatid", {"int", "long"}),
        ("encounterid", {"int", "long"}),
        ("cpatid", {"int", "long"}),
        ("enctype", {"string"}),
        ("birth_type", {"int", "long"}),
        ("age", {"int", "long", "double", "decimal"}),
        ("adate", {"date"}),
    ],
}


def _field_by_name(schema: StructType) -> dict[str, StructField]:
    """Return a name→field mapping from a schema."""
    return {f.name: f for f in schema.fields}


def check_required_columns(domain: str, schema: StructType) -> list[str]:
    """Check that all required columns exist. Returns a list of missing column names."""
    required = REQUIRED_COLUMNS.get(domain, [])
    field_names = {f.name for f in schema.fields}
    return [name for name, _types in required if name not in field_names]


def check_type_compatibility(
    domain: str,
    schema: StructType,
) -> list[str]:
    """Check that required columns have compatible types.

    Returns a list of human-readable findings (empty if all OK).
    """
    required = REQUIRED_COLUMNS.get(domain, [])
    fields = _field_by_name(schema)
    findings: list[str] = []
    for col_name, acceptable_types in required:
        if col_name not in fields:
            continue  # missing columns handled by check_required_columns
        actual = fields[col_name].dataType.typeName()
        if acceptable_types and actual not in acceptable_types:
            findings.append(
                f"column '{col_name}' has type '{actual}', expected one of "
                f"{sorted(acceptable_types)}"
            )
    return findings


def validate_schema(domain: str, schema: StructType) -> None:
    """Raise ``SchemaError`` if required columns are missing or types are incompatible."""
    missing = check_required_columns(domain, schema)
    if missing:
        raise SchemaError(f"domain '{domain}' is missing required columns: {', '.join(missing)}")
    type_findings = check_type_compatibility(domain, schema)
    if type_findings:
        raise SchemaError(f"domain '{domain}' has type issues: {'; '.join(type_findings)}")

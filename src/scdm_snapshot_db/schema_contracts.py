# pattern: Functional Core
"""Pure required-column and type-compatibility policy over Spark schemas.

All functions accept ``StructType`` metadata and return ``list[str]`` findings
or raise ``SchemaError``. No Spark actions, no IO.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .error_classification import SchemaError

if TYPE_CHECKING:
    from pyspark.sql.types import StructField, StructType

__all__ = [
    "REQUIRED_COLUMNS",
    "TYPE_COMPATIBILITY",
    "check_required_columns",
    "check_type_compatibility",
    "validate_schema",
]


# Required columns per domain
REQUIRED_COLUMNS: dict[str, list[tuple[str, set[str]]]] = {
    "enrollment": [
        ("patid", {"string"}),
        ("enr_start", {"date"}),
        ("enr_end", {"date"}),
        ("drugcov", {"string"}),
        ("medcov", {"string"}),
    ],
    "demographic": [
        ("patid", {"string"}),
        ("birth_date", {"date"}),
        ("sex", {"string"}),
        ("race", {"string"}),
        ("hispanic", {"string"}),
    ],
    "dispensing": [
        ("patid", {"string"}),
        ("rxdate", {"date"}),
    ],
    "encounter": [
        ("patid", {"string"}),
        ("adate", {"date"}),
    ],
    "lab": [
        ("patid", {"string"}),
        ("lab_dt", {"date", "timestamp"}),
        ("result_dt", {"date", "timestamp"}),
        ("order_dt", {"date", "timestamp"}),
    ],
    "death": [
        ("patid", {"string"}),
    ],
    "mil": [
        ("mpatid", {"string"}),
        ("encounter_id", {"string"}),
        ("cpatid", {"string"}),
        ("enc_type", {"string"}),
        ("birth_type", {"string"}),
        ("age", {"int", "long", "double", "decimal"}),
        ("adate", {"date"}),
    ],
}

# Column → set of acceptable Spark type names. An empty set means any type is OK.
TYPE_COMPATIBILITY: dict[str, set[str]] = {
    "patid": {"string"},
    "enr_start": {"date"},
    "enr_end": {"date"},
    "drugcov": {"string"},
    "medcov": {"string"},
    "birth_date": {"date"},
    "sex": {"string"},
    "race": {"string"},
    "hispanic": {"string"},
    "rxdate": {"date"},
    "adate": {"date"},
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
        # For lab columns that accept timestamp or date, also accept string
        if domain == "lab" and col_name in ("lab_dt", "result_dt", "order_dt"):
            acceptable_types = acceptable_types | {"string"}
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
        raise SchemaError(
            f"domain '{domain}' is missing required columns: {', '.join(missing)}"
        )
    type_findings = check_type_compatibility(domain, schema)
    if type_findings:
        raise SchemaError(
            f"domain '{domain}' has type issues: {'; '.join(type_findings)}"
        )

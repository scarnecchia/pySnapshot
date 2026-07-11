"""Canonical-schema-driven contract tests for REQUIRED_COLUMNS.

Layer 1 (no Spark): verifies that every ``(domain, required column)`` in
``REQUIRED_COLUMNS`` matches the canonical ``scdm_schema.json`` registry
maintained by the ``scdm_convert`` pipeline.

Layer 2 (Spark, marked ``@pytest.mark.integration``): behavioral tests that
exercise ``validate_schema`` and the lab transform with canonical and
non-canonical schemas.

The canonical schema is vendored at ``tests/data/scdm_schema.json`` so the
tests are self-contained and do not depend on a sibling repository checkout.
When the canonical schema is updated in ``scdm_convert``, copy the new
version here and re-run the tests.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scdm_snapshot_db import schema_contracts
from scdm_snapshot_db.schema_contracts import REQUIRED_COLUMNS

# ---------------------------------------------------------------------------
# Canonical schema loading
# ---------------------------------------------------------------------------

_CANONICAL_SCHEMA_PATH = Path(__file__).resolve().parent / "data" / "scdm_schema.json"

# Domain name → canonical table name in scdm_schema.json
DOMAIN_TO_TABLE = {
    "enrollment": "enrollment",
    "demographic": "demographic",
    "dispensing": "dispensing",
    "encounter": "encounter",
    "lab": "laboratory_result",
    "death": "death",
    "mil": "mother_infant_linkage",
}

# ---------------------------------------------------------------------------
# Independent date-column registry
#
# (canonical_table, column) pairs known to resolve to Date in parquet output,
# derived from the conversion pipeline's behavior and the SCDM spec — NOT
# from REQUIRED_COLUMNS. This is the independent oracle the contract test
# checks against.
# ---------------------------------------------------------------------------

DATE_COLUMNS: set[tuple[str, str]] = {
    ("enrollment", "enr_start"),
    ("enrollment", "enr_end"),
    ("demographic", "birth_date"),
    ("demographic", "postalcode_date"),
    ("dispensing", "rxdate"),
    ("encounter", "adate"),
    ("encounter", "ddate"),
    ("death", "deathdt"),
    ("laboratory_result", "order_dt"),
    ("laboratory_result", "lab_dt"),
    ("laboratory_result", "result_dt"),
    # lab_tm and result_tm resolve to Time, not Date — excluded
    ("mother_infant_linkage", "mbirth_date"),
    ("mother_infant_linkage", "adate"),
    ("mother_infant_linkage", "ddate"),
    ("mother_infant_linkage", "cbirth_date"),
    ("mother_infant_linkage", "cenr_start"),
    ("inpatient_pharmacy", "rxadate"),
    ("inpatient_transfusion", "tdate_start"),
    ("inpatient_transfusion", "tdate_end"),
    ("prescribing", "orderdate"),
    ("prm_survey", "question_dt"),
    ("prm_response", "response_dt"),
    ("feature_engineering", "feature_dt"),
}

# Intentional supersets: contract type sets that are deliberately wider than
# the canonical type. Keyed by (domain, column) → set of extra Spark types.
# This must be EXHAUSTIVE — test_no_undocumented_supersets verifies that
# every extra type in every contract column is listed here.
INTENTIONAL_SUPERSETS: dict[tuple[str, str], set[str]] = {
    ("lab", "lab_dt"): {"string", "timestamp"},
    ("lab", "result_dt"): {"string", "timestamp"},
    ("lab", "order_dt"): {"string", "timestamp"},
    ("mil", "birth_type"): {"long"},  # canonical Int32 → {"int"}, contract adds {"long"}
    ("mil", "age"): {"double", "decimal"},  # canonical Int64 → {"int","long"}, contract adds wider numeric
}


def _load_canonical_schema() -> dict:
    """Load the canonical scdm_schema.json."""
    with open(_CANONICAL_SCHEMA_PATH) as f:
        return json.load(f)


def _canonical_columns(table: str) -> dict[str, str]:
    """Return {column_name: polars_type} for a canonical table."""
    schema = _load_canonical_schema()
    table_def = schema.get(table, {})
    return {col["name"]: col["polars_type"] for col in table_def.get("columns", [])}


def _polars_to_spark_types(table: str, col_name: str, polars_type: str) -> set[str]:
    """Map a canonical polars_type to the expected set of Spark type names.

    Float64 columns in DATE_COLUMNS resolve to Date in parquet output;
    all other Float64 columns resolve to Float64 (DoubleType in Spark).
    """
    if polars_type == "Int64":
        return {"int", "long"}
    if polars_type == "Int32":
        return {"int"}
    if polars_type == "String":
        return {"string"}
    if polars_type == "Float64":
        if (table, col_name) in DATE_COLUMNS:
            return {"date"}
        return {"double"}
    # Fallback for any future type
    return {polars_type.lower()}


# ---------------------------------------------------------------------------
# Layer 1: Canonical contract tests (no Spark required)
# ---------------------------------------------------------------------------

class TestCanonicalContract:
    """Verify REQUIRED_COLUMNS matches the canonical scdm_schema.json registry."""

    @pytest.mark.parametrize("domain", list(DOMAIN_TO_TABLE.keys()))
    def test_all_required_columns_match_canonical(self, domain: str) -> None:
        """Every (domain, required column) must exist in the canonical table
        and the contract's type set must contain the canonical Spark type."""
        table = DOMAIN_TO_TABLE[domain]
        canonical = _canonical_columns(table)
        required = REQUIRED_COLUMNS.get(domain, [])

        assert required, f"REQUIRED_COLUMNS['{domain}'] is empty"

        for col_name, contract_types in required:
            # Column must exist in canonical schema
            assert col_name in canonical, (
                f"Column '{col_name}' in domain '{domain}' not found in "
                f"canonical table '{table}'"
            )
            polars_type = canonical[col_name]
            expected_types = _polars_to_spark_types(table, col_name, polars_type)

            # The contract's type set must be a superset of the canonical types
            assert expected_types.issubset(contract_types), (
                f"Column '{col_name}' in domain '{domain}': canonical types "
                f"{expected_types} not subset of contract types {contract_types}"
            )

    def test_lab_date_contracts_include_string(self) -> None:
        """Lab date columns must include 'string' in their type set (direct
        structural assertion, not subset check)."""
        lab_cols = dict(REQUIRED_COLUMNS["lab"])
        for col_name in ("lab_dt", "result_dt", "order_dt"):
            assert col_name in lab_cols, f"'{col_name}' missing from REQUIRED_COLUMNS['lab']"
            assert "string" in lab_cols[col_name], (
                f"'{col_name}' type set {lab_cols[col_name]} does not include 'string'"
            )

    def test_death_includes_deathdt(self) -> None:
        """REQUIRED_COLUMNS['death'] must contain ('deathdt', {'date'})."""
        death_cols = REQUIRED_COLUMNS["death"]
        col_dict = dict(death_cols)
        assert "deathdt" in col_dict, "'deathdt' missing from REQUIRED_COLUMNS['death']"
        assert col_dict["deathdt"] == {"date"}, (
            f"'deathdt' type set is {col_dict['deathdt']}, expected {{'date'}}"
        )

    def test_all_id_columns_are_numeric(self) -> None:
        """Every ID column in REQUIRED_COLUMNS must have {'int', 'long'} in its
        type set.

        Scope: only the 7 domains currently in REQUIRED_COLUMNS. Canonical ID
        columns in domains not yet covered (diagnosis, procedure, facility,
        provider, etc.) are checked by ``test_canonical_id_columns_in_covered_domains``.
        """
        for domain, required in REQUIRED_COLUMNS.items():
            for col_name, contract_types in required:
                if col_name.endswith("id") or col_name == "patid" or col_name == "mpatid":
                    assert {"int", "long"}.issubset(contract_types), (
                        f"ID column '{col_name}' in domain '{domain}' has types "
                        f"{contract_types}, missing 'int' or 'long'"
                    )

    def test_canonical_id_columns_in_covered_domains(self) -> None:
        """Every canonical Int64/Int32 ID column in the 7 covered domains must
        either be present in REQUIRED_COLUMNS (with numeric types) or be
        listed in OPTIONAL_CANONICAL_IDS (explicitly acknowledged as not required).

        This prevents silent omission of ID columns from the contract — if a
        new ID column is added to the canonical schema, this test will fail
        until it is either added to REQUIRED_COLUMNS or explicitly allowlisted.
        """
        # Canonical ID columns intentionally not required in the contract.
        # These exist in the canonical schema but are not needed by any
        # current transform. New additions must be reviewed.
        OPTIONAL_CANONICAL_IDS: set[tuple[str, str]] = {
            ("dispensing", "providerid"),
            ("encounter", "encounterid"),
            ("encounter", "facilityid"),
            ("laboratory_result", "labid"),
            ("laboratory_result", "facilityid"),
        }

        for domain, table in DOMAIN_TO_TABLE.items():
            canonical = _canonical_columns(table)
            for col_name, polars_type in canonical.items():
                is_id = (
                    col_name.endswith("id")
                    or col_name == "patid"
                    or col_name == "mpatid"
                )
                if is_id and polars_type in ("Int64", "Int32"):
                    required = dict(REQUIRED_COLUMNS.get(domain, []))
                    if col_name in required:
                        assert {"int", "long"}.issubset(required[col_name]), (
                            f"ID column '{col_name}' in domain '{domain}' is in "
                            f"REQUIRED_COLUMNS with types {required[col_name]}, "
                            f"missing 'int' or 'long'"
                        )
                    else:
                        assert (table, col_name) in OPTIONAL_CANONICAL_IDS, (
                            f"Canonical ID column '{col_name}' in table '{table}' "
                            f"(domain '{domain}') is not in REQUIRED_COLUMNS and not "
                            f"in OPTIONAL_CANONICAL_IDS — either add it to the contract "
                            f"or explicitly allowlist it"
                        )

    def test_no_undocumented_supersets(self) -> None:
        """Every extra type in a contract column (beyond the canonical Spark
        types) must be documented in INTENTIONAL_SUPERSETS.

        This prevents silent relaxation — if someone adds 'string' or 'double'
        to a contract type set, this test fails until the superset is documented.
        """
        for domain, required in REQUIRED_COLUMNS.items():
            table = DOMAIN_TO_TABLE[domain]
            canonical = _canonical_columns(table)
            for col_name, contract_types in required:
                polars_type = canonical.get(col_name)
                if polars_type is None:
                    continue  # column not in canonical — other tests catch this
                expected_types = _polars_to_spark_types(table, col_name, polars_type)
                extras = contract_types - expected_types
                documented_extras = INTENTIONAL_SUPERSETS.get((domain, col_name), set())
                assert extras == documented_extras, (
                    f"Column '{col_name}' in domain '{domain}' has undocumented "
                    f"superset: contract types {sorted(contract_types)}, canonical "
                    f"{sorted(expected_types)}, extras {sorted(extras)}, documented "
                    f"extras {sorted(documented_extras)}"
                )

    def test_date_columns_registry_is_exhaustive_for_covered_domains(self) -> None:
        """DATE_COLUMNS must include every Float64 column in the 7 covered
        canonical tables whose name matches a date-like pattern.

        Note: this uses a naming heuristic (suffixes like _dt, _date, *date,
        _start, _end) to identify candidate date columns. It is not a semantic
        oracle — it catches newly added date-like columns but may miss dates
        with non-standard names. The manual DATE_COLUMNS set remains the
        authoritative source; this test guards against omission for the
        common naming patterns.
        """
        DATE_NAME_SUFFIXES = ("_dt", "_date", "date", "_start", "_end")

        for domain, table in DOMAIN_TO_TABLE.items():
            canonical = _canonical_columns(table)
            for col_name, polars_type in canonical.items():
                if polars_type != "Float64":
                    continue
                # Skip time columns (resolve to Time, not Date)
                if col_name.endswith("_tm"):
                    continue
                # Check if the column name suggests a date
                looks_like_date = any(
                    col_name.endswith(suffix) for suffix in DATE_NAME_SUFFIXES
                ) or col_name == "postalcode_date"
                if looks_like_date:
                    assert (table, col_name) in DATE_COLUMNS, (
                        f"Canonical Float64 column '{col_name}' in table '{table}' "
                        f"looks like a date but is not in DATE_COLUMNS — it would "
                        f"be mapped to 'double' instead of 'date'"
                    )

    def test_type_compatibility_removed(self) -> None:
        """TYPE_COMPATIBILITY dict must be removed from the module."""
        assert not hasattr(schema_contracts, "TYPE_COMPATIBILITY"), (
            "TYPE_COMPATIBILITY should have been removed from schema_contracts"
        )
        assert "TYPE_COMPATIBILITY" not in schema_contracts.__all__, (
            "TYPE_COMPATIBILITY should have been removed from __all__"
        )

    def test_intentional_supersets_exist_in_contract(self) -> None:
        """Every entry in INTENTIONAL_SUPERSETS must reference a column that
        actually exists in REQUIRED_COLUMNS."""
        for (domain, col_name), extra_types in INTENTIONAL_SUPERSETS.items():
            required = REQUIRED_COLUMNS.get(domain, [])
            col_dict = dict(required)
            assert col_name in col_dict, (
                f"INTENTIONAL_SUPERSETS references '{col_name}' in domain "
                f"'{domain}' but column not found in REQUIRED_COLUMNS"
            )


# ---------------------------------------------------------------------------
# Layer 2: Behavioral tests (require Spark)
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestValidateSchemaDeath:
    """Behavioral tests for validate_schema with death domain."""

    def test_validate_schema_accepts_canonical_death(self, spark) -> None:
        """validate_schema('death', ...) accepts patid LongType + deathdt DateType."""
        from pyspark.sql.types import (
            DateType,
            LongType,
            StructField,
            StructType,
        )

        schema = StructType([
            StructField("patid", LongType(), False),
            StructField("deathdt", DateType(), True),
        ])
        # Should not raise
        schema_contracts.validate_schema("death", schema)

    def test_validate_schema_rejects_legacy_death_date(self, spark) -> None:
        """validate_schema('death', ...) raises SchemaError when only death_date
        is present (no deathdt)."""
        from scdm_snapshot_db.error_classification import SchemaError
        from pyspark.sql.types import (
            DateType,
            LongType,
            StructField,
            StructType,
        )

        schema = StructType([
            StructField("patid", LongType(), False),
            StructField("death_date", DateType(), True),
        ])
        with pytest.raises(SchemaError, match="deathdt"):
            schema_contracts.validate_schema("death", schema)

    def test_validate_schema_rejects_wrong_deathdt_type(self, spark) -> None:
        """validate_schema('death', ...) raises SchemaError when deathdt is
        StringType instead of DateType."""
        from scdm_snapshot_db.error_classification import SchemaError
        from pyspark.sql.types import (
            LongType,
            StringType,
            StructField,
            StructType,
        )

        schema = StructType([
            StructField("patid", LongType(), False),
            StructField("deathdt", StringType(), True),
        ])
        with pytest.raises(SchemaError, match="deathdt"):
            schema_contracts.validate_schema("death", schema)


@pytest.mark.integration
class TestLabStringDates:
    """Behavioral tests for lab date columns accepting string type."""

    def test_lab_string_dates_pass_validation(self, spark) -> None:
        """check_type_compatibility('lab', ...) returns empty findings when lab
        date columns are StringType."""
        from pyspark.sql.types import (
            LongType,
            StringType,
            StructField,
            StructType,
        )

        schema = StructType([
            StructField("patid", LongType(), False),
            StructField("lab_dt", StringType(), True),
            StructField("result_dt", StringType(), True),
            StructField("order_dt", StringType(), True),
        ])
        findings = schema_contracts.check_type_compatibility("lab", schema)
        assert findings == [], f"Expected no findings, got: {findings}"

    def test_lab_string_dates_derive_test_dt(self, spark) -> None:
        """_derive_test_dt produces valid test_dt for ISO date strings and null
        for malformed strings."""
        from datetime import date

        from pyspark.sql.types import (
            LongType,
            StringType,
            StructField,
            StructType,
        )

        from scdm_snapshot_db.transforms.lab import _derive_test_dt

        schema = StructType([
            StructField("patid", LongType(), False),
            StructField("lab_dt", StringType(), True),
            StructField("result_dt", StringType(), True),
            StructField("order_dt", StringType(), True),
        ])
        df = spark.createDataFrame(
            [
                # Valid ISO string → parsed
                {"patid": 1, "lab_dt": "2020-03-15", "result_dt": None, "order_dt": None},
                # Malformed string → null
                {"patid": 2, "lab_dt": "not-a-date", "result_dt": None, "order_dt": None},
                # Fallback through coalesce: lab_dt malformed, result_dt valid
                {"patid": 3, "lab_dt": "bad", "result_dt": "2021-01-20", "order_dt": None},
                # All null → test_dt null
                {"patid": 4, "lab_dt": None, "result_dt": None, "order_dt": None},
            ],
            schema=schema,
        )

        result = _derive_test_dt(df).select("patid", "test_dt").orderBy("patid").collect()

        assert result[0]["test_dt"] == date(2020, 3, 15), (
            f"Expected 2020-03-15, got {result[0]['test_dt']}"
        )
        assert result[0]["patid"] == 1

        assert result[1]["test_dt"] is None, (
            f"Expected null for malformed string, got {result[1]['test_dt']}"
        )
        assert result[1]["patid"] == 2

        assert result[2]["test_dt"] == date(2021, 1, 20), (
            f"Expected fallback to result_dt 2021-01-20, got {result[2]['test_dt']}"
        )
        assert result[2]["patid"] == 3

        assert result[3]["test_dt"] is None, (
            f"Expected null when all dates null, got {result[3]['test_dt']}"
        )
        assert result[3]["patid"] == 4

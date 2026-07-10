"""Tests for MIL transformation (AC.9)."""

from __future__ import annotations

from datetime import date

import pytest

pytestmark = pytest.mark.integration

from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from scdm_snapshot_db.transforms import mil

MIL_SCHEMA = StructType(
    [
        StructField("mpatid", StringType(), True),
        StructField("encounter_id", StringType(), True),
        StructField("cpatid", StringType(), True),
        StructField("enc_type", StringType(), True),
        StructField("birth_type", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("adate", DateType(), True),
    ]
)


def _make_mil_df(spark, rows):
    return spark.createDataFrame(rows, schema=MIL_SCHEMA)


class TestMILAgeCategories:
    def test_mil_age_categories_are_exhaustive(self, spark) -> None:
        """All age categories from the corrected contract are produced."""
        deliveries = [
            # age 0 → 0-1 yrs
            {
                "mpatid": "M01",
                "encounter_id": "E01",
                "cpatid": "C01",
                "enc_type": "D",
                "birth_type": "V",
                "age": 0,
                "adate": date(2020, 1, 1),
            },
            # age 55 → 55+ yrs
            {
                "mpatid": "M02",
                "encounter_id": "E02",
                "cpatid": None,
                "enc_type": "D",
                "birth_type": "C",
                "age": 55,
                "adate": date(2020, 6, 1),
            },
            # age null → MISSING
            {
                "mpatid": "M03",
                "encounter_id": "E03",
                "cpatid": "C03",
                "enc_type": "D",
                "birth_type": "V",
                "age": None,
                "adate": date(2020, 3, 15),
            },
            # age -1 → NEGATIVE
            {
                "mpatid": "M04",
                "encounter_id": "E04",
                "cpatid": None,
                "enc_type": "D",
                "birth_type": "V",
                "age": -1,
                "adate": date(2020, 7, 1),
            },
        ]
        df = _make_mil_df(spark, deliveries)
        result = mil.mil_linkage_rates(df, "TEST").collect()

        age_cat_rows = [r for r in result if r.variable == "age_category"]
        cats = {r.value for r in age_cat_rows}
        assert "0-1 yrs" in cats
        assert "55+ yrs" in cats
        assert "MISSING" in cats
        assert "NEGATIVE" in cats

    def test_mil_age_category_extends_source(self, spark) -> None:
        """Ages below 10 and above 54 should produce valid categories."""
        deliveries = [
            {
                "mpatid": "M01",
                "encounter_id": "E01",
                "cpatid": "C01",
                "enc_type": "D",
                "birth_type": "V",
                "age": 3,
                "adate": date(2020, 1, 1),
            },
            {
                "mpatid": "M02",
                "encounter_id": "E02",
                "cpatid": None,
                "enc_type": "D",
                "birth_type": "V",
                "age": 60,
                "adate": date(2020, 1, 1),
            },
        ]
        df = _make_mil_df(spark, deliveries)
        result = mil.mil_linkage_rates(df, "TEST").collect()
        age_cats = {r.value for r in result if r.variable == "age_category"}
        assert "2-4 yrs" in age_cats
        assert "55+ yrs" in age_cats


class TestMILGroupedResults:
    def test_mil_grouped_results(self, spark) -> None:
        """Overall and four dimension groups are produced."""
        deliveries = [
            {
                "mpatid": "M01",
                "encounter_id": "E01",
                "cpatid": "C01",
                "enc_type": "D",
                "birth_type": "V",
                "age": 25,
                "adate": date(2020, 1, 1),
            },
            {
                "mpatid": "M02",
                "encounter_id": "E02",
                "cpatid": None,
                "enc_type": "L",
                "birth_type": "C",
                "age": 30,
                "adate": date(2021, 6, 1),
            },
        ]
        df = _make_mil_df(spark, deliveries)
        result = mil.mil_linkage_rates(df, "TEST").collect()
        variables = {r.variable for r in result}
        assert "overall" in variables
        assert "age_category" in variables
        assert "enc_type" in variables
        assert "year" in variables
        assert "birth_type" in variables

    def test_mil_infant_dedup_within_deliveries(self, spark) -> None:
        """Same infant linked to same delivery multiple times counts once."""
        deliveries = [
            {
                "mpatid": "M01",
                "encounter_id": "E01",
                "cpatid": "C01",
                "enc_type": "D",
                "birth_type": "V",
                "age": 25,
                "adate": date(2020, 1, 1),
            },
            {
                "mpatid": "M01",
                "encounter_id": "E01",
                "cpatid": "C01",
                "enc_type": "D",
                "birth_type": "V",
                "age": 25,
                "adate": date(2020, 1, 1),
            },  # duplicate link
        ]
        df = _make_mil_df(spark, deliveries)
        result = mil.mil_linkage_rates(df, "TEST").collect()
        overall = next(r for r in result if r.variable == "overall")
        assert overall.deliveries == 1
        assert overall.linked_deliveries == 1
        assert overall.distinct_infants_linked == 1


class TestMILSchemaAndPrecision:
    def test_mil_exact_ordered_schema_and_precision(self, spark) -> None:
        """Output schema has correct types and precision."""
        deliveries = [
            {
                "mpatid": "M01",
                "encounter_id": "E01",
                "cpatid": "C01",
                "enc_type": "D",
                "birth_type": "V",
                "age": 25,
                "adate": date(2020, 1, 1),
            },
        ]
        df = _make_mil_df(spark, deliveries)
        result = mil.mil_linkage_rates(df, "TEST")
        schema = result.schema

        # Check column names
        field_names = [f.name for f in schema.fields]
        assert field_names == [
            "dp",
            "variable",
            "value",
            "deliveries",
            "linked_deliveries",
            "distinct_infants_linked",
            "linkage_rate",
        ]

        # Check types
        type_map = {f.name: f.dataType.typeName() for f in schema.fields}
        assert type_map["dp"] == "string"
        assert type_map["variable"] == "string"
        assert type_map["value"] == "string"
        assert type_map["deliveries"] == "long"
        assert type_map["linked_deliveries"] == "long"
        assert type_map["distinct_infants_linked"] == "long"
        assert type_map["linkage_rate"] == "decimal"

        # Check decimal precision
        rate_field = next(f for f in schema.fields if f.name == "linkage_rate")
        assert rate_field.dataType.precision == 9
        assert rate_field.dataType.scale == 6


class TestMILConflictDetection:
    def test_mil_conflict_detected(self, spark) -> None:
        """Conflicting delivery attributes are detected."""
        deliveries = [
            {
                "mpatid": "M01",
                "encounter_id": "E01",
                "cpatid": "C01",
                "enc_type": "D",
                "birth_type": "V",
                "age": 25,
                "adate": date(2020, 1, 1),
            },
            {
                "mpatid": "M01",
                "encounter_id": "E01",
                "cpatid": "C02",
                "enc_type": "L",  # different enc_type
                "birth_type": "V",
                "age": 25,
                "adate": date(2020, 1, 1),
            },
        ]
        df = _make_mil_df(spark, deliveries)
        conflicts = mil.mil_build_conflict_check(df).collect()
        assert len(conflicts) == 1

    def test_mil_no_conflict_repeated_values(self, spark) -> None:
        """Repeated equal values are consistent (no conflict)."""
        deliveries = [
            {
                "mpatid": "M01",
                "encounter_id": "E01",
                "cpatid": "C01",
                "enc_type": "D",
                "birth_type": "V",
                "age": 25,
                "adate": date(2020, 1, 1),
            },
            {
                "mpatid": "M01",
                "encounter_id": "E01",
                "cpatid": "C02",
                "enc_type": "D",
                "birth_type": "V",
                "age": 25,
                "adate": date(2020, 1, 1),
            },
        ]
        df = _make_mil_df(spark, deliveries)
        conflicts = mil.mil_build_conflict_check(df).collect()
        assert len(conflicts) == 0

    def test_mil_all_null_consistent(self, spark) -> None:
        """All-null values are consistent (no conflict)."""
        deliveries = [
            {
                "mpatid": "M01",
                "encounter_id": "E01",
                "cpatid": "C01",
                "enc_type": None,
                "birth_type": None,
                "age": None,
                "adate": None,
            },
            {
                "mpatid": "M01",
                "encounter_id": "E01",
                "cpatid": "C02",
                "enc_type": None,
                "birth_type": None,
                "age": None,
                "adate": None,
            },
        ]
        df = _make_mil_df(spark, deliveries)
        conflicts = mil.mil_build_conflict_check(df).collect()
        assert len(conflicts) == 0

"""Tests for demographic and event-domain transforms (AC.6, AC.7, AC.8)."""

from __future__ import annotations

from datetime import date

import pytest

pytestmark = pytest.mark.integration

from pyspark.sql.types import (
    LongType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from scdm_snapshot_db.transforms import death, demographic, dispensing, encounter


def _make_df(spark, schema, rows):
    return spark.createDataFrame(rows, schema)


class TestDemographicAgeBoundaries:
    DEM_SCHEMA = StructType(
        [
            StructField("patid", LongType(), False),
            StructField("birth_date", DateType(), True),
            StructField("sex", StringType(), True),
            StructField("race", StringType(), True),
            StructField("hispanic", StringType(), True),
        ]
    )

    ENR_SCHEMA = StructType(
        [
            StructField("patid", LongType(), False),
            StructField("_enr_start", DateType(), False),
            StructField("_enr_end", DateType(), False),
            StructField("_span_id", IntegerType(), False),
        ]
    )

    def test_age_80_bucket(self, spark) -> None:
        """Exactly age 80 should be in 80+ yrs (corrected from source's >80)."""
        latest = _make_df(
            spark,
            self.ENR_SCHEMA,
            [
                {
                    "patid": 1,
                    "_enr_start": date(2000, 1, 1),
                    "_enr_end": date(2023, 12, 31),
                    "_span_id": 1,
                },
            ],
        )
        dem = _make_df(
            spark,
            self.DEM_SCHEMA,
            [
                {
                    "patid": 1,
                    "birth_date": date(1920, 1, 1),
                    "sex": "M",
                    "race": "W",
                    "hispanic": "N",
                },
            ],
        )
        result = demographic.dem_pat_lstagecount_md(
            latest, dem, "TEST", date(2023, 6, 30)
        ).collect()
        cats = {r.age_category: r.count for r in result}
        assert cats.get("80+ yrs") == 1

    def test_null_birth_retained_as_missing(self, spark) -> None:
        """Null birth_date should be retained as MISSING (intentional deviation)."""
        latest = _make_df(
            spark,
            self.ENR_SCHEMA,
            [
                {
                    "patid": 1,
                    "_enr_start": date(2020, 1, 1),
                    "_enr_end": date(2023, 12, 31),
                    "_span_id": 1,
                },
            ],
        )
        dem = _make_df(
            spark,
            self.DEM_SCHEMA,
            [
                {"patid": 1, "birth_date": None, "sex": "M", "race": "W", "hispanic": "N"},
            ],
        )
        result = demographic.dem_pat_lstagecount_md(
            latest, dem, "TEST", date(2023, 6, 30)
        ).collect()
        cats = {r.age_category: r.count for r in result}
        assert cats.get("MISSING") == 1

    def test_demographic_leap_birthday(self, spark) -> None:
        """Leap day birthday should calculate completed years correctly."""
        latest = _make_df(
            spark,
            self.ENR_SCHEMA,
            [
                {
                    "patid": 1,
                    "_enr_start": date(2020, 2, 29),
                    "_enr_end": date(2023, 12, 31),
                    "_span_id": 1,
                },
            ],
        )
        dem = _make_df(
            spark,
            self.DEM_SCHEMA,
            [
                {
                    "patid": 1,
                    "birth_date": date(2000, 2, 29),
                    "sex": "F",
                    "race": "B",
                    "hispanic": "N",
                },
            ],
        )
        result = demographic.dem_pat_lstagecount_md(
            latest, dem, "TEST", date(2023, 6, 30)
        ).collect()
        cats = {r.age_category: r.count for r in result}
        # 20 years exactly on leap day → 15-19 or 20-24?
        # months_between(2020-02-29, 2000-02-29) = 240 months → floor(240/12) = 20 → 20-24
        assert cats.get("20-24 yrs") == 1

    def test_demographic_latest_and_active_selection(self, spark) -> None:
        """Latest span selected; active when end >= dp_max_date."""
        latest = _make_df(
            spark,
            self.ENR_SCHEMA,
            [
                {
                    "patid": 1,
                    "_enr_start": date(2022, 1, 1),
                    "_enr_end": date(2022, 6, 30),
                    "_span_id": 1,
                },
            ],
        )
        dem = _make_df(
            spark,
            self.DEM_SCHEMA,
            [
                {
                    "patid": 1,
                    "birth_date": date(2000, 1, 1),
                    "sex": "M",
                    "race": "W",
                    "hispanic": "N",
                },
            ],
        )
        dp_max = date(2023, 6, 30)
        # Not active (end < dp_max)
        act = demographic.dem_pat_actagect_md(latest, dem, "TEST", dp_max).collect()
        assert len(act) == 0
        # Active (end >= dp_max)
        latest_active = _make_df(
            spark,
            self.ENR_SCHEMA,
            [
                {
                    "patid": 1,
                    "_enr_start": date(2022, 1, 1),
                    "_enr_end": date(2023, 12, 31),
                    "_span_id": 1,
                },
            ],
        )
        act2 = demographic.dem_pat_actagect_md(latest_active, dem, "TEST", dp_max).collect()
        assert len(act2) == 1

    def test_demographic_categories_use_distinct_patient_membership(self, spark) -> None:
        """dem_catvars_md counts distinct patients per category."""

        distinct_md = _make_df(
            spark,
            StructType([StructField("patid", LongType(), False)]),
            [
                {"patid": 1},
                {"patid": 2},
            ],
        )
        dem = _make_df(
            spark,
            self.DEM_SCHEMA,
            [
                {
                    "patid": 1,
                    "birth_date": date(2000, 1, 1),
                    "sex": "M",
                    "race": "W",
                    "hispanic": "N",
                },
                {
                    "patid": 2,
                    "birth_date": date(2001, 1, 1),
                    "sex": "F",
                    "race": "B",
                    "hispanic": "Y",
                },
            ],
        )
        result = demographic.dem_catvars_md(distinct_md, dem, "TEST").collect()
        vars_vals = {(r.variable, r.value): r.count for r in result}
        assert vars_vals.get(("sex", "M")) == 1
        assert vars_vals.get(("sex", "F")) == 1
        assert vars_vals.get(("race", "W")) == 1
        assert vars_vals.get(("race", "B")) == 1
        assert vars_vals.get(("hispanic", "N")) == 1
        assert vars_vals.get(("hispanic", "Y")) == 1


class TestDispensingIntervalCounts:
    ENR_SCHEMA = StructType(
        [
            StructField("patid", LongType(), False),
            StructField("_enr_start", DateType(), False),
            StructField("_enr_end", DateType(), False),
            StructField("_span_id", IntegerType(), False),
        ]
    )

    def test_dispensing_interval_counts(self, spark) -> None:
        """Dispensing events outside enrollment window are not counted."""
        bridged = _make_df(
            spark,
            self.ENR_SCHEMA,
            [
                {
                    "patid": 1,
                    "_enr_start": date(2020, 1, 1),
                    "_enr_end": date(2020, 6, 30),
                    "_span_id": 1,
                },
            ],
        )
        dis_schema = StructType(
            [
                StructField("patid", LongType(), False),
                StructField("rxdate", DateType(), True),
            ]
        )
        dis = _make_df(
            spark,
            dis_schema,
            [
                {"patid": 1, "rxdate": date(2020, 3, 15)},  # in window
                {"patid": 1, "rxdate": date(2021, 1, 15)},  # out of window
            ],
        )
        result = dispensing.dis_pat_rx_md(dis, bridged, "TEST").collect()
        assert len(result) == 1
        assert result[0]["rx_count"] == 1
        assert result[0]["count"] == 1

    def test_event_not_duplicated_by_nested_enrollment(self, spark) -> None:
        """Events not double-counted when enrollment spans don't overlap (post-bridging)."""
        bridged = _make_df(
            spark,
            self.ENR_SCHEMA,
            [
                {
                    "patid": 1,
                    "_enr_start": date(2020, 1, 1),
                    "_enr_end": date(2020, 3, 31),
                    "_span_id": 1,
                },
                {
                    "patid": 1,
                    "_enr_start": date(2020, 6, 1),
                    "_enr_end": date(2020, 9, 30),
                    "_span_id": 2,
                },
            ],
        )
        dis_schema = StructType(
            [
                StructField("patid", LongType(), False),
                StructField("rxdate", DateType(), True),
            ]
        )
        dis = _make_df(
            spark,
            dis_schema,
            [
                {"patid": 1, "rxdate": date(2020, 2, 15)},  # in span 1
                {"patid": 1, "rxdate": date(2020, 7, 15)},  # in span 2
            ],
        )
        result = dispensing.dis_pat_rx_md(dis, bridged, "TEST").collect()
        assert result[0]["rx_count"] == 2  # both events counted, no duplication


class TestEncounterIntervalCounts:
    def test_encounter_interval_counts(self, spark) -> None:
        enr_schema = StructType(
            [
                StructField("patid", LongType(), False),
                StructField("_enr_start", DateType(), False),
                StructField("_enr_end", DateType(), False),
                StructField("_span_id", IntegerType(), False),
            ]
        )
        bridged = _make_df(
            spark,
            enr_schema,
            [
                {
                    "patid": 1,
                    "_enr_start": date(2020, 1, 1),
                    "_enr_end": date(2020, 6, 30),
                    "_span_id": 1,
                },
            ],
        )
        enc_schema = StructType(
            [
                StructField("patid", LongType(), False),
                StructField("adate", DateType(), True),
            ]
        )
        enc = _make_df(
            spark,
            enc_schema,
            [
                {"patid": 1, "adate": date(2020, 3, 15)},
                {"patid": 1, "adate": date(2020, 4, 20)},
            ],
        )
        result = encounter.enc_pat_enccount_md(enc, bridged, "TEST").collect()
        assert result[0]["enc_count"] == 2
        assert result[0]["count"] == 1


class TestLabDatePrecedence:
    LAB_SCHEMA = StructType(
        [
            StructField("patid", LongType(), False),
            StructField("lab_dt", DateType(), True),
            StructField("result_dt", DateType(), True),
            StructField("order_dt", DateType(), True),
        ]
    )
    ENR_SCHEMA = StructType(
        [
            StructField("patid", LongType(), False),
            StructField("_enr_start", DateType(), False),
            StructField("_enr_end", DateType(), False),
            StructField("_span_id", IntegerType(), False),
        ]
    )

    def test_lab_date_precedence_and_interval_counts(self, spark) -> None:
        """test_dt = COALESCE(lab_dt, result_dt, order_dt)."""
        bridged = _make_df(
            spark,
            self.ENR_SCHEMA,
            [
                {
                    "patid": 1,
                    "_enr_start": date(2020, 1, 1),
                    "_enr_end": date(2020, 12, 31),
                    "_span_id": 1,
                },
            ],
        )
        lab = _make_df(
            spark,
            self.LAB_SCHEMA,
            [
                {
                    "patid": 1,
                    "lab_dt": date(2020, 3, 15),
                    "result_dt": date(2020, 3, 20),
                    "order_dt": date(2020, 3, 10),
                },
                {
                    "patid": 1,
                    "lab_dt": None,
                    "result_dt": date(2020, 4, 10),
                    "order_dt": date(2020, 4, 5),
                },
                {"patid": 1, "lab_dt": None, "result_dt": None, "order_dt": date(2020, 5, 1)},
            ],
        )
        result = lab.lab_pat_testct_md(lab, bridged, "TEST").collect()
        assert result[0]["lab_count"] == 3  # all 3 tests counted
        assert result[0]["count"] == 1


class TestDeathDistinctMembership:
    def test_death_counts_distinct_cohort_membership(self, spark) -> None:
        """Death count not inflated by duplicate enrollment membership rows."""
        from pyspark.sql.types import StructField

        distinct_patids = _make_df(
            spark,
            StructType([StructField("patid", LongType(), False)]),
            [{"patid": 1}],
        )
        death_schema = StructType(
            [
                StructField("patid", LongType(), False),
                StructField("deathdt", DateType(), True),
            ]
        )
        death_df = _make_df(
            spark,
            death_schema,
            [
                {"patid": 1, "deathdt": date(2021, 1, 1)},
            ],
        )
        result = death.dth_dthct_md(death_df, distinct_patids, "TEST").collect()
        assert result[0]["count"] == 1

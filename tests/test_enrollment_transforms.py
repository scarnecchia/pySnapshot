"""Tests for corrected enrollment interval bridging (AC.4, AC.5)."""

from __future__ import annotations

from datetime import date

import pytest

pytestmark = pytest.mark.integration

from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType

from scdm_snapshot_db.transforms import enrollment


def _make_enrollment_df(spark, rows):
    """Create an enrollment DataFrame from a list of dicts."""
    schema = StructType(
        [
            StructField("patid", LongType(), False),
            StructField("enr_start", DateType(), False),
            StructField("enr_end", DateType(), False),
            StructField("drugcov", StringType(), True),
            StructField("medcov", StringType(), True),
        ]
    )
    return spark.createDataFrame(rows, schema)


class TestIntervalBridging:
    def test_bridge_nested_and_overlapping_intervals(self, spark) -> None:
        """Nested intervals should be bridged correctly."""
        rows = [
            {
                "patid": 1,
                "enr_start": date(2020, 1, 1),
                "enr_end": date(2020, 12, 31),
                "drugcov": "y",
                "medcov": "y",
            },
            {
                "patid": 1,
                "enr_start": date(2020, 3, 1),
                "enr_end": date(2020, 6, 30),
                "drugcov": "y",
                "medcov": "y",
            },
            {
                "patid": 1,
                "enr_start": date(2020, 7, 1),
                "enr_end": date(2020, 9, 30),
                "drugcov": "y",
                "medcov": "y",
            },
        ]
        df = _make_enrollment_df(spark, rows)
        bridged = enrollment.bridge_intervals(df)
        result = bridged.collect()
        # All three intervals are within 46 days of each other → 1 span
        assert len(result) == 1
        assert result[0]["patid"] == 1
        assert result[0]["_enr_start"] == date(2020, 1, 1)
        assert result[0]["_enr_end"] == date(2020, 12, 31)

    def test_bridge_gap_boundaries(self, spark) -> None:
        """Gaps of 46 days bridge; gaps of 47 days separate."""
        # Gap of 46 days: end=Jan 1, start=Feb 16 → 46 days → bridge
        rows_bridge = [
            {
                "patid": 1,
                "enr_start": date(2020, 1, 1),
                "enr_end": date(2020, 1, 1),
                "drugcov": "y",
                "medcov": "y",
            },
            {
                "patid": 1,
                "enr_start": date(2020, 2, 16),
                "enr_end": date(2020, 3, 1),
                "drugcov": "y",
                "medcov": "y",
            },
        ]
        df = _make_enrollment_df(spark, rows_bridge)
        bridged = enrollment.bridge_intervals(df)
        result = bridged.collect()
        assert len(result) == 1, f"Expected 1 span for 46-day gap, got {len(result)}"

        # Gap of 47 days: end=Jan 1, start=Feb 17 → 47 days → separate
        rows_separate = [
            {
                "patid": 2,
                "enr_start": date(2020, 1, 1),
                "enr_end": date(2020, 1, 1),
                "drugcov": "y",
                "medcov": "y",
            },
            {
                "patid": 2,
                "enr_start": date(2020, 2, 17),
                "enr_end": date(2020, 3, 1),
                "drugcov": "y",
                "medcov": "y",
            },
        ]
        df = _make_enrollment_df(spark, rows_separate)
        bridged = enrollment.bridge_intervals(df)
        result = bridged.collect()
        assert len(result) == 2, f"Expected 2 spans for 47-day gap, got {len(result)}"

    def test_bridge_ties_and_duplicates(self, spark) -> None:
        """Duplicate intervals should be deduplicated; ties handled deterministically."""
        rows = [
            {
                "patid": 1,
                "enr_start": date(2020, 1, 1),
                "enr_end": date(2020, 1, 31),
                "drugcov": "y",
                "medcov": "y",
            },
            {
                "patid": 1,
                "enr_start": date(2020, 1, 1),
                "enr_end": date(2020, 1, 31),
                "drugcov": "y",
                "medcov": "y",
            },  # exact duplicate
        ]
        df = _make_enrollment_df(spark, rows)
        bridged = enrollment.bridge_intervals(df)
        result = bridged.collect()
        assert len(result) == 1

    def test_bridge_is_order_invariant(self, spark) -> None:
        """Bridging should produce the same result regardless of input row order."""
        base_rows = [
            {
                "patid": 1,
                "enr_start": date(2020, 3, 1),
                "enr_end": date(2020, 3, 31),
                "drugcov": "y",
                "medcov": "y",
            },
            {
                "patid": 1,
                "enr_start": date(2020, 1, 1),
                "enr_end": date(2020, 1, 31),
                "drugcov": "y",
                "medcov": "y",
            },
            {
                "patid": 1,
                "enr_start": date(2020, 2, 1),
                "enr_end": date(2020, 2, 28),
                "drugcov": "y",
                "medcov": "y",
            },
        ]
        # Forward order
        df1 = _make_enrollment_df(spark, base_rows)
        result1 = sorted(
            [
                (r.patid, r._enr_start, r._enr_end)
                for r in enrollment.bridge_intervals(df1).collect()
            ]
        )
        # Reverse order
        df2 = _make_enrollment_df(spark, list(reversed(base_rows)))
        result2 = sorted(
            [
                (r.patid, r._enr_start, r._enr_end)
                for r in enrollment.bridge_intervals(df2).collect()
            ]
        )
        assert result1 == result2

    def test_bridged_spans_cover_inputs(self, spark) -> None:
        """Every valid source interval must be covered by a bridged span."""
        rows = [
            {
                "patid": 1,
                "enr_start": date(2020, 1, 1),
                "enr_end": date(2020, 3, 31),
                "drugcov": "y",
                "medcov": "y",
            },
            {
                "patid": 1,
                "enr_start": date(2020, 6, 1),
                "enr_end": date(2020, 8, 31),
                "drugcov": "y",
                "medcov": "y",
            },
        ]
        df = _make_enrollment_df(spark, rows)
        bridged = enrollment.bridge_intervals(df)
        spans = bridged.collect()
        # Check that the min span start <= Jan 1 and max span end >= Aug 31
        min_start = min(s._enr_start for s in spans)
        max_end = max(s._enr_end for s in spans)
        assert min_start <= date(2020, 1, 1)
        assert max_end >= date(2020, 8, 31)

    def test_one_day_inclusive_duration(self, spark) -> None:
        """A single-day span should have inclusive duration of 1 day."""
        rows = [
            {
                "patid": 1,
                "enr_start": date(2020, 6, 15),
                "enr_end": date(2020, 6, 15),
                "drugcov": "y",
                "medcov": "y",
            },
        ]
        df = _make_enrollment_df(spark, rows)
        md_df = enrollment.filter_medical_and_drug(df)
        bridged = enrollment.bridge_intervals(md_df)
        output = enrollment.enr_pat_covlength_md(bridged, "TEST")
        result = output.collect()
        assert len(result) == 1
        # datediff(end, start) + 1 = 0 + 1 = 1
        assert result[0]["total_length"] == 1
        assert result[0]["count"] == 1

    def test_enrollment_outputs_corrected_contract(self, spark) -> None:
        """Five enrollment outputs produce correct corrected results."""
        rows = [
            {
                "patid": 1,
                "enr_start": date(2020, 1, 1),
                "enr_end": date(2020, 6, 30),
                "drugcov": "y",
                "medcov": "y",
            },
            {
                "patid": 1,
                "enr_start": date(2020, 8, 1),
                "enr_end": date(2020, 12, 31),
                "drugcov": "y",
                "medcov": "y",
            },
            {
                "patid": 2,
                "enr_start": date(2020, 3, 1),
                "enr_end": date(2023, 12, 31),
                "drugcov": "y",
                "medcov": "y",
            },
        ]
        df = _make_enrollment_df(spark, rows)
        md_df = enrollment.filter_medical_and_drug(df)
        bridged = enrollment.bridge_intervals(md_df)

        dp = "TEST"
        dp_max = date(2023, 6, 30)

        # enr_patid_ct_md: 2 distinct patients
        ct = enrollment.enr_patid_ct_md(bridged, dp).collect()
        assert len(ct) == 1
        assert ct[0]["count"] == 2

        # enr_pat_enrcount_md: P1 has 2 spans, P2 has 1 span
        enc = enrollment.enr_pat_enrcount_md(bridged, dp).collect()
        enc_by_count = {r.enr_count: r.count for r in enc}
        assert enc_by_count.get(1) == 1  # P2 has 1 span
        assert enc_by_count.get(2) == 1  # P1 has 2 spans

        # enr_active_patid_ct_md: both P1 and P2 have spans ending >= dp_max
        active = enrollment.enr_active_patid_ct_md(bridged, dp, dp_max).collect()
        assert len(active) == 1
        assert active[0]["count"] == 2

        # enr_pat_covyears_md: P1 covers 2020 only, P2 covers 2020-2023
        years = enrollment.enr_pat_covyears_md(bridged, dp).collect()
        year_counts = {r.year: r.count for r in years}
        assert year_counts[2020] == 2  # both patients in 2020
        assert year_counts[2021] == 1  # only P2
        assert year_counts[2023] == 1  # only P2

    def test_repeated_spans_one_year_dedup(self, spark) -> None:
        """Patient-year deduplication: multiple spans in same year count once."""
        rows = [
            {
                "patid": 1,
                "enr_start": date(2020, 1, 1),
                "enr_end": date(2020, 1, 31),
                "drugcov": "y",
                "medcov": "y",
            },
            {
                "patid": 1,
                "enr_start": date(2020, 6, 1),
                "enr_end": date(2020, 6, 30),
                "drugcov": "y",
                "medcov": "y",
            },
        ]
        df = _make_enrollment_df(spark, rows)
        bridged = enrollment.bridge_intervals(enrollment.filter_medical_and_drug(df))
        years = enrollment.enr_pat_covyears_md(bridged, "TEST").collect()
        # Both spans are in 2020, but patient counted once for 2020
        year_2020 = [r for r in years if r.year == 2020]
        assert len(year_2020) == 1
        assert year_2020[0].count == 1

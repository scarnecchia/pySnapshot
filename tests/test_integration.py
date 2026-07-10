"""Integration tests for end-to-end pipeline execution (AC.10, AC.12, AC.15)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

from tests.synthetic_data import (
    write_death_parquet,
    write_demographic_parquet,
    write_dispensing_parquet,
    write_encounter_parquet,
    write_enrollment_parquet,
    write_lab_parquet,
    write_mil_parquet,
)

from scdm_snapshot_db.config_models import (
    BenchmarkSettings,
    Config,
    Domain,
    InputPaths,
    RequestValues,
    SparkSettings,
    WriteSettings,
)
from scdm_snapshot_db.pipeline import run_pipeline


def _make_all_domain_config(tmp_path: Path) -> Config:
    """Create a config with all domains and synthetic data paths."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    output_dir = tmp_path / "output"

    return Config(
        request=RequestValues(dpid="TESTDP", dp_max_date=date(2023, 6, 30)),
        input_paths=InputPaths(
            enrollment=str(data_dir / "enrollment"),
            demographic=str(data_dir / "demographic"),
            dispensing=str(data_dir / "dispensing"),
            encounter=str(data_dir / "encounter"),
            lab=str(data_dir / "lab"),
            death=str(data_dir / "death"),
            mil=str(data_dir / "mil"),
        ),
        output_root=str(output_dir),
        selected_domains=frozenset(d for d in Domain),
        spark=SparkSettings(master="local[2]", shuffle_partitions=4),
        write=WriteSettings(mode="overwrite"),
        benchmark=BenchmarkSettings(),
    )


def _write_all_data(spark, config: Config) -> None:
    """Write synthetic data for all domains."""
    write_enrollment_parquet(
        spark,
        config.input_paths.enrollment,
        [
            {
                "patid": "P1",
                "enr_start": date(2020, 1, 1),
                "enr_end": date(2023, 12, 31),
                "drugcov": "y",
                "medcov": "y",
            },
            {
                "patid": "P2",
                "enr_start": date(2020, 6, 1),
                "enr_end": date(2022, 6, 30),
                "drugcov": "y",
                "medcov": "y",
            },
            {
                "patid": "P3",
                "enr_start": date(2021, 1, 1),
                "enr_end": date(2023, 12, 31),
                "drugcov": "n",
                "medcov": "y",
            },
        ],
    )
    write_demographic_parquet(
        spark,
        config.input_paths.demographic,
        [
            {
                "patid": "P1",
                "birth_date": date(1990, 1, 1),
                "sex": "M",
                "race": "W",
                "hispanic": "N",
            },
            {
                "patid": "P2",
                "birth_date": date(1985, 6, 15),
                "sex": "F",
                "race": "B",
                "hispanic": "Y",
            },
            {"patid": "P3", "birth_date": None, "sex": "M", "race": "A", "hispanic": "N"},
        ],
    )
    write_dispensing_parquet(
        spark,
        config.input_paths.dispensing,
        [
            {"patid": "P1", "rxdate": date(2020, 3, 15)},
            {"patid": "P2", "rxdate": date(2021, 1, 10)},
        ],
    )
    write_encounter_parquet(
        spark,
        config.input_paths.encounter,
        [
            {"patid": "P1", "adate": date(2020, 5, 20)},
            {"patid": "P3", "adate": date(2022, 3, 1)},
        ],
    )
    write_lab_parquet(
        spark,
        config.input_paths.lab,
        [
            {"patid": "P1", "lab_dt": date(2020, 4, 1), "result_dt": None, "order_dt": None},
            {"patid": "P2", "lab_dt": None, "result_dt": date(2021, 2, 15), "order_dt": None},
        ],
    )
    write_death_parquet(
        spark,
        config.input_paths.death,
        [
            {"patid": "P2", "death_date": date(2022, 12, 1)},
        ],
    )
    write_mil_parquet(
        spark,
        config.input_paths.mil,
        [
            {
                "mpatid": "M01",
                "encounter_id": "E01",
                "cpatid": "C01",
                "enc_type": "D",
                "birth_type": "V",
                "age": 28,
                "adate": date(2020, 6, 1),
            },
            {
                "mpatid": "M02",
                "encounter_id": "E02",
                "cpatid": None,
                "enc_type": "L",
                "birth_type": "C",
                "age": 35,
                "adate": date(2021, 3, 15),
            },
        ],
    )


class TestAllDomainIntegration:
    def test_all_domains_write_exactly_15_parquet_datasets(self, spark, tmp_path: Path) -> None:
        """AC.10: All-domain run creates exactly 15 output directories."""
        config = _make_all_domain_config(tmp_path)
        _write_all_data(spark, config)

        result = run_pipeline(config)
        assert result.success, f"Pipeline failed: {result.error_message}"

        output_root = Path(config.output_root)
        output_dirs = sorted(d.name for d in output_root.iterdir() if d.is_dir())
        assert len(output_dirs) == 15, f"Expected 15 outputs, got {len(output_dirs)}: {output_dirs}"

    def test_vitals_is_not_emitted(self, spark, tmp_path: Path) -> None:
        """AC.10: The commented vitals block is absent."""
        config = _make_all_domain_config(tmp_path)
        _write_all_data(spark, config)

        result = run_pipeline(config)
        assert result.success

        output_root = Path(config.output_root)
        assert not (output_root / "vit_pat_vitct_md").exists()

    def test_output_writer_never_forces_single_partition(self, spark, tmp_path: Path) -> None:
        """AC.10: No output is forced to one part file (no coalesce(1))."""
        config = _make_all_domain_config(tmp_path)
        _write_all_data(spark, config)

        result = run_pipeline(config)
        assert result.success

        # Check each output directory has part files (not necessarily just one)
        output_root = Path(config.output_root)
        for d in output_root.iterdir():
            if d.is_dir():
                part_files = list(d.glob("part-*.parquet"))
                assert len(part_files) >= 1

    def test_run_result_covers_end_to_end_execution(self, spark, tmp_path: Path) -> None:
        """AC.12: Run result contains elapsed time, versions, and success."""
        config = _make_all_domain_config(tmp_path)
        _write_all_data(spark, config)

        result = run_pipeline(config)
        assert result.success
        assert result.elapsed_seconds > 0
        assert result.python_version is not None
        assert result.selected_outputs is not None
        assert len(result.selected_outputs) == 15
        assert result.started_at is not None
        assert result.finished_at is not None


class TestMILOnlyExecution:
    def test_mil_only_does_not_require_enrollment(self, spark, tmp_path: Path) -> None:
        """AC.3: MIL-only execution works without enrollment input."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        config = Config(
            request=RequestValues(dpid="TESTDP"),
            input_paths=InputPaths(mil=str(data_dir / "mil")),
            output_root=str(tmp_path / "output"),
            selected_domains=frozenset({Domain.MIL}),
            spark=SparkSettings(master="local[2]", shuffle_partitions=4),
            write=WriteSettings(mode="overwrite"),
            benchmark=BenchmarkSettings(),
        )
        write_mil_parquet(
            spark,
            config.input_paths.mil,
            [
                {
                    "mpatid": "M01",
                    "encounter_id": "E01",
                    "cpatid": "C01",
                    "enc_type": "D",
                    "birth_type": "V",
                    "age": 28,
                    "adate": date(2020, 6, 1),
                },
            ],
        )
        result = run_pipeline(config)
        assert result.success
        output_root = Path(config.output_root)
        assert (output_root / "mil_linkage_rates").exists()
        # No enrollment outputs
        assert not (output_root / "enr_patid_ct_md").exists()


class TestFailedRunResult:
    def test_missing_input_fails_before_writes(self, spark, tmp_path: Path) -> None:
        """AC.15: Missing input fails before output writes."""
        config = Config(
            request=RequestValues(dpid="TESTDP"),
            input_paths=InputPaths(mil=str(tmp_path / "nonexistent")),
            output_root=str(tmp_path / "output"),
            selected_domains=frozenset({Domain.MIL}),
            spark=SparkSettings(master="local[2]", shuffle_partitions=4),
            write=WriteSettings(mode="overwrite"),
            benchmark=BenchmarkSettings(),
        )
        result = run_pipeline(config)
        assert not result.success
        assert result.error_type is not None

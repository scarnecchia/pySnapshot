"""Tests for benchmark summary statistics and runner logic."""

from __future__ import annotations

import pytest

from scdm_snapshot_db.benchmark import summarize_samples
from scdm_snapshot_db.error_classification import ConfigError
from scdm_snapshot_db.models import BenchmarkSample


class TestBenchmarkSummary:
    def _make_sample(
        self,
        rep: int,
        success: bool = True,
        child_seconds: float | None = 10.0,
        outer_seconds: float = 12.0,
    ) -> BenchmarkSample:
        return BenchmarkSample(
            repetition=rep,
            success=success,
            child_elapsed_seconds=child_seconds,
            outer_elapsed_seconds=outer_seconds,
            exit_code=0 if success else 1,
            output_dir=f"/tmp/rep_{rep}",
            error_message=None if success else "failed",
        )

    def test_benchmark_summary_statistics(self) -> None:
        samples = [
            self._make_sample(1, child_seconds=10.0),
            self._make_sample(2, child_seconds=12.0),
            self._make_sample(3, child_seconds=14.0),
        ]
        summary = summarize_samples(samples, "config.toml", 3)
        assert summary.successful_count == 3
        assert summary.failed_count == 0
        assert summary.median_seconds == 12.0
        assert summary.min_seconds == 10.0
        assert summary.max_seconds == 14.0
        assert summary.dispersion_seconds == 4.0

    def test_benchmark_reports_failed_samples(self) -> None:
        samples = [
            self._make_sample(1, child_seconds=10.0),
            self._make_sample(2, success=False, child_seconds=None),
        ]
        summary = summarize_samples(samples, "config.toml", 2)
        assert summary.successful_count == 1
        assert summary.failed_count == 1
        assert summary.median_seconds == 10.0

    def test_benchmark_all_failed(self) -> None:
        samples = [
            self._make_sample(1, success=False, child_seconds=None),
            self._make_sample(2, success=False, child_seconds=None),
        ]
        summary = summarize_samples(samples, "config.toml", 2)
        assert summary.successful_count == 0
        assert summary.failed_count == 2
        assert summary.median_seconds is None
        assert summary.min_seconds is None
        assert summary.max_seconds is None

    def test_benchmark_output_guardrail(self) -> None:
        """Benchmark runner must refuse to delete paths outside benchmark root."""
        from pathlib import Path

        from scdm_snapshot_db.benchmark import _safe_remove

        with pytest.raises(ConfigError, match="refusing to delete"):
            _safe_remove(Path("/etc"), Path("/tmp/scdm_benchmark"))

    def test_benchmark_even_samples(self) -> None:
        samples = [self._make_sample(i, child_seconds=10.0 + i) for i in range(1, 5)]
        summary = summarize_samples(samples, "config.toml", 4)
        # median of [11, 12, 13, 14] = 12.5
        assert summary.median_seconds == 12.5

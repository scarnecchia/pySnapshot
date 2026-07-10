# pattern: Imperative Shell
"""Fresh-process benchmark runner.

Executes the pipeline ``run`` command in a fresh Python/JVM subprocess for
each repetition to avoid cross-run session/cache contamination.
"""

from __future__ import annotations

import json
import logging
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from .config_loading import load_config
from .error_classification import ConfigError
from .models import BenchmarkSample, BenchmarkSummary

logger = logging.getLogger(__name__)

__all__ = ["run_benchmark", "summarize_samples"]


def _safe_remove(path: Path, benchmark_root: Path) -> None:
    """Remove a path only if it is inside the benchmark root."""
    try:
        path.resolve().relative_to(benchmark_root.resolve())
    except ValueError as exc:
        raise ConfigError(f"refusing to delete path outside benchmark root: {path}") from exc
    if path.exists():
        shutil.rmtree(path)


def run_benchmark(
    config_path: str | Path,
    python_executable: str | None = None,
    module_args: list[str] | None = None,
) -> BenchmarkSummary:
    """Run the pipeline N times in fresh subprocesses.

    Each repetition uses a unique output directory. Warm-up repetitions
    (if configured) are discarded and not included in summaries.

    The child process runs the same CLI with a unique output directory.
    """
    config = load_config(config_path)
    python_exe = python_executable or sys.executable
    args = module_args or [
        "-m",
        "scdm_snapshot_db.cli",
        "run",
        "--config",
        str(config_path),
    ]

    benchmark_root = Path(tempfile.gettempdir()) / "scdm_benchmark"
    benchmark_root.mkdir(parents=True, exist_ok=True)

    samples: list[BenchmarkSample] = []

    # Warm-up runs (discarded)
    for i in range(config.benchmark.warmup_repetitions):
        out_dir = benchmark_root / f"warmup_{i}"
        run_args = [*args, "--output-root", str(out_dir)]
        logger.info("warmup repetition %d starting", i + 1)
        subprocess.run(  # noqa: S603
            [python_exe, *run_args],
            capture_output=True,
            text=True,
            check=False,
        )
        _safe_remove(out_dir, benchmark_root)

    # Measured repetitions
    for i in range(config.benchmark.repetitions):
        out_dir = benchmark_root / f"rep_{i}"
        _safe_remove(out_dir, benchmark_root)
        run_args = [*args, "--output-root", str(out_dir)]

        logger.info("benchmark repetition %d/%d starting", i + 1, config.benchmark.repetitions)
        start = time.monotonic()
        proc = subprocess.run(  # noqa: S603
            [python_exe, *run_args],
            capture_output=True,
            text=True,
            check=False,
        )
        outer_elapsed = time.monotonic() - start

        child_elapsed: float | None = None
        error_message: str | None = None

        if proc.returncode == 0:
            # Try to read the run result JSON for child timing
            result_path = Path(config.output_root) / "run_result.json"
            if result_path.exists():
                try:
                    with result_path.open() as f:
                        result_data = json.load(f)
                    child_elapsed = result_data.get("elapsed_seconds")
                except (json.JSONDecodeError, OSError):
                    pass
        else:
            error_message = proc.stderr[-500:] if proc.stderr else "unknown error"

        sample = BenchmarkSample(
            repetition=i + 1,
            success=proc.returncode == 0,
            child_elapsed_seconds=child_elapsed,
            outer_elapsed_seconds=round(outer_elapsed, 3),
            exit_code=proc.returncode,
            output_dir=str(out_dir),
            error_message=error_message,
        )
        samples.append(sample)
        logger.info(
            "repetition %d: success=%s, child=%.3fs, outer=%.3fs",
            i + 1,
            sample.success,
            child_elapsed or 0.0,
            outer_elapsed,
        )

        # Clean up if configured
        if config.benchmark.clean_output:
            _safe_remove(out_dir, benchmark_root)

    return summarize_samples(samples, str(config_path), config.benchmark.repetitions)


def summarize_samples(
    samples: list[BenchmarkSample],
    config_path: str,
    repetitions: int,
) -> BenchmarkSummary:
    """Aggregate benchmark samples into a summary."""
    successful = [s for s in samples if s.success and s.child_elapsed_seconds is not None]
    failed = [s for s in samples if not s.success]

    if successful:
        times = [s.child_elapsed_seconds for s in successful if s.child_elapsed_seconds is not None]
        median = statistics.median(times)
        mn = min(times)
        mx = max(times)
        dispersion = mx - mn
    else:
        median = None
        mn = None
        mx = None
        dispersion = None

    return BenchmarkSummary(
        config_path=config_path,
        repetitions=repetitions,
        samples=samples,
        successful_count=len(successful),
        failed_count=len(failed),
        median_seconds=round(median, 3) if median is not None else None,
        min_seconds=round(mn, 3) if mn is not None else None,
        max_seconds=round(mx, 3) if mx is not None else None,
        dispersion_seconds=round(dispersion, 3) if dispersion is not None else None,
    )


def write_benchmark_summary(summary: BenchmarkSummary, path: str | Path) -> None:
    """Write benchmark summary as JSON."""
    data = {
        "config_path": summary.config_path,
        "repetitions": summary.repetitions,
        "successful_count": summary.successful_count,
        "failed_count": summary.failed_count,
        "median_seconds": summary.median_seconds,
        "min_seconds": summary.min_seconds,
        "max_seconds": summary.max_seconds,
        "dispersion_seconds": summary.dispersion_seconds,
        "samples": [
            {
                "repetition": s.repetition,
                "success": s.success,
                "child_elapsed_seconds": s.child_elapsed_seconds,
                "outer_elapsed_seconds": s.outer_elapsed_seconds,
                "exit_code": s.exit_code,
                "output_dir": s.output_dir,
                "error_message": s.error_message,
            }
            for s in summary.samples
        ],
    }
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w") as f:
        json.dump(data, f, indent=2)

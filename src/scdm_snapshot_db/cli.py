# pattern: Imperative Shell
"""Console CLI for the SCDM snapshot pipeline.

Provides ``run``, ``benchmark``, and ``compare`` subcommands.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .benchmark import run_benchmark, write_benchmark_summary
from .comparison import compare_outputs
from .config_loading import load_config
from .config_models import Config
from .error_classification import ConfigError
from .logging_setup import configure_logging
from .pipeline import run_pipeline, write_run_result

logger = logging.getLogger(__name__)

__all__ = ["main"]


def _build_parser() -> argparse.ArgumentParser:
    """Build the argument parser."""
    parser = argparse.ArgumentParser(
        prog="scdm-snapshot",
        description="SCDM snapshot analytical pipeline (PySpark)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="logging level",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # ── run ──────────────────────────────────────────────────────────────
    run_parser = subparsers.add_parser("run", help="run the analytical pipeline")
    run_parser.add_argument("--config", required=True, help="path to TOML config file")
    run_parser.add_argument(
        "--output-root",
        default=None,
        help="override output root directory",
    )
    run_parser.add_argument(
        "--dpid",
        default=None,
        help="override DPID",
    )
    run_parser.add_argument(
        "--result-path",
        default=None,
        help="path to write run result JSON",
    )

    # ── benchmark ────────────────────────────────────────────────────────
    bench_parser = subparsers.add_parser("benchmark", help="run fresh-process benchmark")
    bench_parser.add_argument("--config", required=True, help="path to TOML config file")
    bench_parser.add_argument(
        "--result-path",
        default=None,
        help="path to write benchmark summary JSON",
    )

    # ── compare ──────────────────────────────────────────────────────────
    cmp_parser = subparsers.add_parser("compare", help="compare outputs against reference")
    cmp_parser.add_argument("--config", required=True, help="path to TOML config file")
    cmp_parser.add_argument(
        "--actual-root",
        required=True,
        help="directory containing actual outputs",
    )
    cmp_parser.add_argument(
        "--reference-root",
        required=True,
        help="directory containing reference outputs",
    )
    cmp_parser.add_argument(
        "--numeric-tolerance",
        type=float,
        default=0.0,
        help="tolerance for numeric comparisons",
    )

    return parser


def _apply_cli_overrides(
    config: Config,
    *,
    output_root: str | None = None,
    dpid: str | None = None,
) -> Config:
    """Apply CLI overrides to a resolved Config."""
    from dataclasses import replace

    overrides: dict[str, object] = {}

    if output_root is not None:
        overrides["output_root"] = output_root
    if dpid is not None:
        overrides["request"] = replace(config.request, dpid=dpid)

    if overrides:
        return replace(config, **overrides)  # type: ignore[arg-type]
    return config


def _cmd_run(args: argparse.Namespace) -> int:
    """Execute the run subcommand."""
    try:
        cli_overrides: dict[str, object] = {}
        if args.dpid is not None:
            cli_overrides["dpid"] = args.dpid
        if args.output_root is not None:
            cli_overrides["output_root"] = args.output_root

        config = load_config(args.config, cli_overrides=cli_overrides)
    except ConfigError as exc:
        logger.error("configuration error: %s", exc)
        return 2

    result = run_pipeline(config)

    result_path = args.result_path or str(Path(config.output_root) / "run_result.json")
    write_run_result(result, result_path)

    if result.success:
        logger.info(
            "pipeline completed in %.3fs with %d outputs",
            result.elapsed_seconds,
            len(result.selected_outputs),
        )
        return 0

    logger.error(
        "pipeline failed: %s (%s)",
        result.error_message,
        result.error_type,
    )
    return 1


def _cmd_benchmark(args: argparse.Namespace) -> int:
    """Execute the benchmark subcommand."""
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        logger.error("configuration error: %s", exc)
        return 2

    summary = run_benchmark(args.config)

    result_path = args.result_path or config.benchmark.metadata_path
    write_benchmark_summary(summary, result_path)

    # Print human-readable summary
    print(f"Benchmark: {summary.repetitions} repetitions")
    print(f"  Successful: {summary.successful_count}")
    print(f"  Failed: {summary.failed_count}")
    if summary.median_seconds is not None:
        print(f"  Median: {summary.median_seconds:.3f}s")
        print(f"  Min: {summary.min_seconds:.3f}s")
        print(f"  Max: {summary.max_seconds:.3f}s")
        print(f"  Dispersion: {summary.dispersion_seconds:.3f}s")
    else:
        print("  No successful runs to summarize")

    return 0 if summary.failed_count == 0 else 1


def _cmd_compare(args: argparse.Namespace) -> int:
    """Execute the compare subcommand."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local[*]").appName("scdm-compare").getOrCreate()

    try:
        result = compare_outputs(
            spark,
            args.actual_root,
            args.reference_root,
            numeric_tolerance=args.numeric_tolerance,
        )

        print("Comparison results:")
        for ds in result.datasets:
            print(f"  {ds.name}: {ds.status.value}")

        print(f"\nOverall equivalent: {result.overall_equivalent}")
        print(f"  Exact matches: {result.exact_match_count}")
        print(f"  Numeric tolerance matches: {result.numeric_tolerance_match_count}")
        print(f"  Schema differences: {result.schema_difference_count}")
        print(f"  Value differences: {result.value_difference_count}")
        print(f"  Missing reference: {result.missing_reference_count}")
        print(f"  Missing actual: {result.missing_actual_count}")

        return 0
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    """Main CLI entry point."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    log_level = getattr(logging, args.log_level)
    configure_logging(level=log_level)

    if args.command == "run":
        return _cmd_run(args)
    if args.command == "benchmark":
        return _cmd_benchmark(args)
    if args.command == "compare":
        return _cmd_compare(args)
    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())

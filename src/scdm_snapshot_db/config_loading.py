# pattern: Imperative Shell
"""TOML loading, CLI override gathering, and path access.

This module reads files, parses TOML, and assembles a resolved ``Config``.
It is the bridge between the outside world and the pure config dataclasses.
"""

from __future__ import annotations

import tomllib
from datetime import date
from pathlib import Path
from typing import Any

from .config_models import (
    BenchmarkSettings,
    BroadcastStrategy,
    Config,
    InputPaths,
    RequestValues,
    SparkSettings,
    WriteMode,
    WriteSettings,
)
from .config_validation import parse_domain_names, validate_config
from .error_classification import ConfigError
from .models import Domain

__all__ = ["build_config", "load_config", "load_toml"]


def load_toml(path: str | Path) -> dict[str, Any]:
    """Read and parse a TOML file."""
    p = Path(path)
    if not p.is_file():
        raise ConfigError(f"configuration file not found: {p}")
    with p.open("rb") as f:
        return tomllib.load(f)


def _coerce_int(value: Any, key: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"invalid integer for {key}: {value!r}") from exc


def _coerce_date(value: Any, key: str) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ConfigError(f"invalid date for {key}: {value!r} (expected yyyy-mm-dd)") from exc
    raise ConfigError(f"invalid date for {key}: {value!r} (expected yyyy-mm-dd)")


def _check_unknown_keys(table: dict[str, Any], allowed: set[str], section: str) -> None:
    unknown = set(table.keys()) - allowed
    if unknown:
        raise ConfigError(f"unknown key(s) in [{section}]: {', '.join(sorted(unknown))}")


def build_config(
    toml_data: dict[str, Any],
    *,
    cli_overrides: dict[str, Any] | None = None,
) -> Config:
    """Build a validated ``Config`` from parsed TOML and optional CLI overrides.

    CLI overrides take precedence over TOML values.
    """
    overrides = cli_overrides or {}

    # ── [request] ─────────────────────────────────────────────────────────
    req_table = dict(toml_data.get("request", {}))
    _check_unknown_keys(req_table, {"dpid", "dp_max_date"}, "request")

    dpid = str(overrides.get("dpid", req_table.get("dpid", "")))
    dp_max_date = _coerce_date(
        overrides.get("dp_max_date", req_table.get("dp_max_date", "2023-06-30")),
        "request.dp_max_date",
    )
    request = RequestValues(dpid=dpid, dp_max_date=dp_max_date)

    # ── [inputs] ──────────────────────────────────────────────────────────
    inputs_table = dict(toml_data.get("inputs", {}))
    valid_input_keys = {d.value for d in Domain}
    _check_unknown_keys(inputs_table, valid_input_keys, "inputs")
    input_paths = InputPaths(
        enrollment=inputs_table.get("enrollment"),
        demographic=inputs_table.get("demographic"),
        dispensing=inputs_table.get("dispensing"),
        encounter=inputs_table.get("encounter"),
        lab=inputs_table.get("lab"),
        death=inputs_table.get("death"),
        mil=inputs_table.get("mil"),
    )

    # ── [output] ──────────────────────────────────────────────────────────
    output_table = dict(toml_data.get("output", {}))
    _check_unknown_keys(output_table, {"root", "domains"}, "output")

    output_root = str(output_table.get("root", "output"))
    domain_names_raw = output_table.get("domains", [])
    if not isinstance(domain_names_raw, list):
        raise ConfigError("[output] domains must be a list of strings")
    selected_domains = parse_domain_names(list(domain_names_raw))
    if not selected_domains:
        raise ConfigError("at least one domain must be selected in [output] domains")

    # ── [spark] ───────────────────────────────────────────────────────────
    spark_table = dict(toml_data.get("spark", {}))
    _check_unknown_keys(
        spark_table,
        {
            "master",
            "app_name",
            "driver_memory",
            "shuffle_partitions",
            "default_parallelism",
            "adaptive_query_execution",
            "session_timezone",
            "extra_settings",
            "broadcast_strategy",
            "storage_level",
            "output_partitions",
        },
        "spark",
    )
    spark = SparkSettings(
        master=str(spark_table.get("master", "local[*]")),
        app_name=str(spark_table.get("app_name", "scdm_snapshot_db")),
        driver_memory=str(spark_table.get("driver_memory", "4g")),
        shuffle_partitions=_coerce_int(
            spark_table.get("shuffle_partitions", 200), "spark.shuffle_partitions"
        ),
        default_parallelism=_coerce_int(
            spark_table.get("default_parallelism", 0), "spark.default_parallelism"
        ),
        adaptive_query_execution=bool(spark_table.get("adaptive_query_execution", True)),
        session_timezone=str(spark_table.get("session_timezone", "UTC")),
        extra_settings=dict(spark_table.get("extra_settings", {})),
        broadcast_strategy=_coerce_broadcast(spark_table.get("broadcast_strategy", "auto")),
        storage_level=str(spark_table.get("storage_level", "MEMORY_AND_DISK")),
        output_partitions=_coerce_int(
            spark_table.get("output_partitions", 0), "spark.output_partitions"
        ),
    )

    # ── [write] ───────────────────────────────────────────────────────────
    write_table = dict(toml_data.get("write", {}))
    _check_unknown_keys(write_table, {"mode"}, "write")
    write_mode_str = str(write_table.get("mode", "errorifexists"))
    valid_modes: set[WriteMode] = {"errorifexists", "overwrite", "ignore"}
    if write_mode_str not in valid_modes:
        raise ConfigError(f"invalid write mode: {write_mode_str}")
    write = WriteSettings(mode=write_mode_str)

    # ── [benchmark] ───────────────────────────────────────────────────────
    bench_table = dict(toml_data.get("benchmark", {}))
    _check_unknown_keys(
        bench_table,
        {"repetitions", "clean_output", "metadata_path", "warmup_repetitions"},
        "benchmark",
    )
    benchmark = BenchmarkSettings(
        repetitions=_coerce_int(bench_table.get("repetitions", 5), "benchmark.repetitions"),
        clean_output=bool(bench_table.get("clean_output", True)),
        metadata_path=str(bench_table.get("metadata_path", "benchmark_result.json")),
        warmup_repetitions=_coerce_int(
            bench_table.get("warmup_repetitions", 0), "benchmark.warmup_repetitions"
        ),
    )

    config = Config(
        request=request,
        input_paths=input_paths,
        output_root=output_root,
        selected_domains=selected_domains,
        spark=spark,
        write=write,
        benchmark=benchmark,
    )
    return validate_config(config)


def _coerce_broadcast(value: Any) -> BroadcastStrategy:
    valid: set[BroadcastStrategy] = {"auto", "broadcast", "disabled"}
    s = str(value)
    if s not in valid:
        raise ConfigError(f"invalid broadcast_strategy: {s}; must be auto, broadcast, or disabled")
    return s


def load_config(
    config_path: str | Path,
    *,
    cli_overrides: dict[str, Any] | None = None,
) -> Config:
    """Load TOML from disk and build a validated Config."""
    toml_data = load_toml(config_path)
    return build_config(toml_data, cli_overrides=cli_overrides)

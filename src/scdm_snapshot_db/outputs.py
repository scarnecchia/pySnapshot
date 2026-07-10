# pattern: Imperative Shell
"""Spark-native output writing.

Each logical output is written to ``<output_root>/<output_name>/`` as a
standard Parquet dataset. No coalesce(1), no file renaming, no forced
single-partition output.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame

if TYPE_CHECKING:
    pass

from .config_models import WriteMode

logger = logging.getLogger(__name__)

__all__ = ["ensure_output_dir", "output_path", "write_output"]


def output_path(output_root: str, name: str) -> str:
    """Return the directory path for a logical output."""
    return str(Path(output_root) / name)


def ensure_output_dir(output_root: str, name: str) -> str:
    """Ensure the parent output root exists. Returns the full output dir path."""
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    return output_path(output_root, name)


def write_output(
    df: DataFrame,
    output_root: str,
    name: str,
    mode: WriteMode = "errorifexists",
    num_partitions: int = 0,
) -> str:
    """Write a DataFrame as a Spark-native Parquet dataset.

    Does not coalesce to 1 or force ordering. An optional ``num_partitions``
    can repartition for output sizing (0 = let Spark decide).

    Returns the output directory path.
    """
    out_dir = ensure_output_dir(output_root, name)
    logger.info("writing output %s to %s (mode=%s)", name, out_dir, mode)

    df.write.mode(mode).parquet(out_dir)

    logger.info("output %s written successfully", name)
    return out_dir

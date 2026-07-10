# pattern: Imperative Shell
"""Input reading and manifest construction.

Reads each needed Parquet dataset once with column projection.
Builds an input manifest from the selected domain graph.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from .config_models import InputPaths
from .config_validation import resolve_required_inputs, resolve_sub_cohorts
from .error_classification import SchemaError
from .models import Domain
from .schema_contracts import validate_schema

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

__all__ = ["InputManifest", "read_domain", "read_inputs"]


class InputManifest:
    """Holds loaded DataFrames for each domain in the run."""

    def __init__(self) -> None:
        self._frames: dict[str, DataFrame] = {}

    def add(self, domain: str, df: DataFrame) -> None:
        self._frames[domain] = df

    def get(self, domain: str) -> DataFrame:
        if domain not in self._frames:
            raise KeyError(f"domain '{domain}' not loaded in manifest")
        return self._frames[domain]

    def has(self, domain: str) -> bool:
        return domain in self._frames

    @property
    def domains(self) -> list[str]:
        return sorted(self._frames.keys())


def _required_columns_for_domain(domain: Domain) -> list[str]:
    """Return the column projection for a domain."""
    from .schema_contracts import REQUIRED_COLUMNS

    return [name for name, _types in REQUIRED_COLUMNS.get(domain.value, [])]


def read_domain(
    spark: SparkSession,
    domain: Domain,
    path: str,
) -> DataFrame:
    """Read a single Parquet dataset with column projection.

    Validates the schema after reading (Spark infers from Parquet metadata,
    not a full data scan).
    """
    logger.info("reading domain %s from %s", domain.value, path)

    # Read first to get the schema
    df = spark.read.parquet(path)

    # Validate schema
    schema: StructType = df.schema
    validate_schema(domain.value, schema)

    # Project only required columns
    cols = _required_columns_for_domain(domain)
    return df.select(*cols)


def read_inputs(
    spark: SparkSession,
    selected_domains: frozenset[Domain],
    input_paths: InputPaths,
) -> InputManifest:
    """Build an input manifest by reading all required domains once.

    Only reads the domains whose inputs are needed for the selected outputs.
    """
    required = resolve_required_inputs(selected_domains)
    manifest = InputManifest()

    for domain in required:
        path = input_paths.for_domain(domain)
        if path is None:
            raise SchemaError(f"no input path configured for required domain {domain.value}")
        df = read_domain(spark, domain, path)
        manifest.add(domain.value, df)

    logger.info(
        "input manifest built with domains: %s, sub-cohorts: %s",
        manifest.domains,
        sorted(resolve_sub_cohorts(selected_domains)),
    )
    return manifest

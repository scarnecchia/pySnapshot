# pattern: Imperative Shell
"""Spark session creation and lifecycle management.

This module is the sole owner of SparkSession creation. It reads validated
``SparkSettings`` and produces a configured local-mode session.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Iterator
from typing import TYPE_CHECKING

from pyspark.sql import SparkSession

from .config_models import SparkSettings

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

__all__ = ["create_spark_session", "get_effective_settings", "spark_session_scope"]


def create_spark_session(settings: SparkSettings) -> SparkSession:
    """Create a local-mode Spark session from validated settings.

    Sets deterministic timezone, adaptive execution, and tuning parameters.
    """
    builder = (
        SparkSession.builder.master(settings.master)
        .appName(settings.app_name)
        .config("spark.driver.memory", settings.driver_memory)
        .config("spark.sql.shuffle.partitions", str(settings.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", str(settings.adaptive_query_execution).lower())
        .config("spark.sql.session.timeZone", settings.session_timezone)
    )

    if settings.default_parallelism > 0:
        builder = builder.config("spark.default.parallelism", str(settings.default_parallelism))

    if settings.broadcast_strategy == "disabled":
        builder = builder.config("spark.sql.autoBroadcastJoinThreshold", "-1")

    for key, value in settings.extra_settings.items():
        builder = builder.config(key, value)

    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session


def get_effective_settings(session: SparkSession) -> dict[str, str]:
    """Return all effective Spark configuration values as a flat dict.

    Does not trigger Spark jobs; reads the configuration map directly.
    """
    return dict(session.sparkContext.getConf().getAll())


@contextlib.contextmanager
def spark_session_scope(settings: SparkSettings) -> Iterator[SparkSession]:
    """Context manager that creates and reliably stops a Spark session.

    The session is stopped in a ``finally`` block, including on failure.
    """
    session = create_spark_session(settings)
    try:
        yield session
    finally:
        try:
            session.stop()
        except Exception:
            logger.warning("spark session stop failed during cleanup", exc_info=True)

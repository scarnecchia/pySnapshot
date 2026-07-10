# pattern: Imperative Shell
"""Spark validation actions and validation timing.

This module executes Spark actions to check data invariants and triggers
them at carefully selected shell boundaries. It is not part of the
functional core.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from .error_classification import DataValidationError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

__all__ = ["DemographicValidationResult", "validate_one_demographic_per_patient"]


class DemographicValidationResult:
    """Result of the demographic duplicate-patid validation action."""

    def __init__(self, elapsed_seconds: float, job_description: str) -> None:
        self.elapsed_seconds = elapsed_seconds
        self.job_description = job_description


def validate_one_demographic_per_patient(
    demographic_df: DataFrame,
) -> DemographicValidationResult:
    """Execute the single named duplicate-patid aggregation.

    This is the one full-data validation action for demographics. It must
    run before any demographic output write. Returns timing metadata.

    Raises ``DataValidationError`` if any patient has more than one
    demographic row.
    """
    job_desc = "demographic_duplicate_patid_validation"
    start = time.monotonic()

    # Count rows per patid, find any with count > 1
    dup_counts = demographic_df.groupBy("patid").count().filter("count > 1").collect()

    elapsed = time.monotonic() - start

    if dup_counts:
        raise DataValidationError(
            f"duplicate patid found in demographic data; "
            f"found {len(dup_counts)} patients with multiple rows"
        )

    logger.info(
        "demographic validation passed",
        extra={"job": job_desc, "elapsed_seconds": round(elapsed, 3)},
    )

    return DemographicValidationResult(elapsed_seconds=elapsed, job_description=job_desc)

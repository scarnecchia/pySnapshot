# pattern: Functional Core
"""Pure mapping of narrow exceptions to stable error classes.

All error messages are lowercase sentence fragments without trailing punctuation.
"""

from __future__ import annotations

from typing import Literal

__all__ = [
    "ConfigError",
    "DataValidationError",
    "ErrorCategory",
    "OutputError",
    "SchemaError",
    "SparkExecutionError",
    "classify_exception",
]

ErrorCategory = Literal[
    "config", "input_schema", "data_validation", "spark_execution", "output", "unknown"
]


class ConfigError(Exception):
    """Raised when configuration is invalid."""

    category: ErrorCategory = "config"


class SchemaError(Exception):
    """Raised when input schema does not meet required-column/type policy."""

    category: ErrorCategory = "input_schema"


class DataValidationError(Exception):
    """Raised when data violates analytical invariants (e.g. duplicate patid)."""

    category: ErrorCategory = "data_validation"


class SparkExecutionError(Exception):
    """Raised when Spark execution fails."""

    category: ErrorCategory = "spark_execution"


class OutputError(Exception):
    """Raised when output writing fails."""

    category: ErrorCategory = "output"


def classify_exception(exc: BaseException) -> ErrorCategory:
    """Map an exception to its stable error category.

    Checks the ``category`` attribute of domain exceptions, then falls back
    to ``unknown`` for anything unrecognized.
    """
    category: object = getattr(exc, "category", None)
    valid_categories = {
        "config",
        "input_schema",
        "data_validation",
        "spark_execution",
        "output",
        "unknown",
    }
    if isinstance(category, str) and category in valid_categories:
        return category  # type: ignore[return-value]
    return "unknown"

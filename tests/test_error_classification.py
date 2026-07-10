"""Tests for error classification and exit codes."""

from __future__ import annotations

import pytest

from scdm_snapshot_db.error_classification import (
    ConfigError,
    DataValidationError,
    OutputError,
    SchemaError,
    SparkExecutionError,
    classify_exception,
)


class TestErrorClassification:
    def test_config_error_category(self) -> None:
        exc = ConfigError("bad config")
        assert classify_exception(exc) == "config"

    def test_schema_error_category(self) -> None:
        exc = SchemaError("missing column")
        assert classify_exception(exc) == "input_schema"

    def test_data_validation_error_category(self) -> None:
        exc = DataValidationError("duplicate patid")
        assert classify_exception(exc) == "data_validation"

    def test_spark_execution_error_category(self) -> None:
        exc = SparkExecutionError("spark failed")
        assert classify_exception(exc) == "spark_execution"

    def test_output_error_category(self) -> None:
        exc = OutputError("write failed")
        assert classify_exception(exc) == "output"

    def test_unknown_error_category(self) -> None:
        exc = ValueError("not a domain error")
        assert classify_exception(exc) == "unknown"

    @pytest.mark.parametrize(
        "exc_class,expected",
        [
            (ConfigError, "config"),
            (SchemaError, "input_schema"),
            (DataValidationError, "data_validation"),
            (SparkExecutionError, "spark_execution"),
            (OutputError, "output"),
        ],
    )
    def test_error_classes_return_correct_category(
        self, exc_class: type[Exception], expected: str
    ) -> None:
        exc = exc_class("test")
        assert classify_exception(exc) == expected

    def test_error_messages_are_lowercase_fragments(self) -> None:
        """Error messages should be lowercase sentence fragments."""
        exc = ConfigError("dpid must not be empty")
        assert str(exc) == str(exc).lower()
        assert not str(exc).endswith(".")

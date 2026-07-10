# pattern: Imperative Shell
"""Logging configuration using stdlib structured logging.

Configured at the CLI entry point. Logs domain names, paths, stage status,
elapsed durations, and output names. Never logs row-level values or patient
identifiers.
"""

from __future__ import annotations

import logging
import sys
from typing import TextIO

__all__ = ["configure_logging", "get_logger"]


_LOG_FORMAT = (
    '{"time": "%(asctime)s", "level": "%(levelname)s", '
    '"logger": "%(name)s", "message": "%(message)s"}'
)
_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


def configure_logging(
    level: int = logging.INFO,
    stream: TextIO | None = None,
) -> None:
    """Configure root logger with JSON-ish structured output."""
    root = logging.getLogger()
    root.setLevel(level)
    # Remove any existing handlers to avoid duplicate output
    for handler in list(root.handlers):
        root.removeHandler(handler)
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))
    root.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """Get a named logger."""
    return logging.getLogger(name)

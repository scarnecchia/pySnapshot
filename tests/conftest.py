"""Shared test fixtures."""
from __future__ import annotations

import pytest


@pytest.fixture
def dp() -> str:
    """Standard test DPID."""
    return "TESTDP"


@pytest.fixture
def dp_max_date():
    """Standard test DPMaxDate."""
    from datetime import date

    return date(2023, 6, 30)

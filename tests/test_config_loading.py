"""Tests for TOML configuration loading and CLI override merging."""

from __future__ import annotations

import textwrap
from datetime import date
from pathlib import Path

import pytest

from scdm_snapshot_db.config_loading import build_config, load_toml
from scdm_snapshot_db.error_classification import ConfigError


def _write_toml(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "test_config.toml"
    p.write_text(textwrap.dedent(content))
    return p


class TestConfigLoading:
    def test_config_precedence(self, tmp_path: Path) -> None:
        """CLI overrides take precedence over TOML values."""
        toml_path = _write_toml(
            tmp_path,
            """
            [request]
            dpid = "FROM_TOML"
            dp_max_date = "2023-06-30"

            [inputs]
            mil = "/data/mil"

            [output]
            root = "output"
            domains = ["mil"]
        """,
        )
        toml_data = load_toml(toml_path)
        config = build_config(toml_data, cli_overrides={"dpid": "FROM_CLI"})
        assert config.request.dpid == "FROM_CLI"

    def test_config_rejects_unknown_keys(self, tmp_path: Path) -> None:
        toml_path = _write_toml(
            tmp_path,
            """
            [request]
            dpid = "X"
            dp_max_date = "2023-06-30"
            bogus_key = true

            [inputs]
            mil = "/data/mil"

            [output]
            root = "output"
            domains = ["mil"]
        """,
        )
        toml_data = load_toml(toml_path)
        with pytest.raises(ConfigError, match="unknown key"):
            build_config(toml_data)

    def test_config_rejects_unsafe_paths(self, tmp_path: Path) -> None:
        toml_path = _write_toml(
            tmp_path,
            """
            [request]
            dpid = "X"
            dp_max_date = "2023-06-30"

            [inputs]
            mil = "/data/output/mil"

            [output]
            root = "/data/output"
            domains = ["mil"]
        """,
        )
        toml_data = load_toml(toml_path)
        with pytest.raises(ConfigError, match="overlap"):
            build_config(toml_data)

    def test_config_rejects_empty_dpid(self, tmp_path: Path) -> None:
        toml_path = _write_toml(
            tmp_path,
            """
            [request]
            dpid = ""
            dp_max_date = "2023-06-30"

            [inputs]
            mil = "/data/mil"

            [output]
            root = "output"
            domains = ["mil"]
        """,
        )
        toml_data = load_toml(toml_path)
        with pytest.raises(ConfigError, match="dpid"):
            build_config(toml_data)

    def test_config_rejects_unknown_domain(self, tmp_path: Path) -> None:
        toml_path = _write_toml(
            tmp_path,
            """
            [request]
            dpid = "X"
            dp_max_date = "2023-06-30"

            [inputs]
            mil = "/data/mil"

            [output]
            root = "output"
            domains = ["foobar"]
        """,
        )
        toml_data = load_toml(toml_path)
        with pytest.raises(ConfigError, match="unknown domain"):
            build_config(toml_data)

    def test_config_rejects_invalid_date(self, tmp_path: Path) -> None:
        toml_path = _write_toml(
            tmp_path,
            """
            [request]
            dpid = "X"
            dp_max_date = "not-a-date"

            [inputs]
            mil = "/data/mil"

            [output]
            root = "output"
            domains = ["mil"]
        """,
        )
        toml_data = load_toml(toml_path)
        with pytest.raises(ConfigError, match="invalid date"):
            build_config(toml_data)

    def test_config_rejects_non_local_master(self, tmp_path: Path) -> None:
        toml_path = _write_toml(
            tmp_path,
            """
            [request]
            dpid = "X"

            [inputs]
            mil = "/data/mil"

            [output]
            root = "output"
            domains = ["mil"]

            [spark]
            master = "yarn"
        """,
        )
        toml_data = load_toml(toml_path)
        with pytest.raises(ConfigError, match="local mode"):
            build_config(toml_data)

    def test_example_config_parses(self) -> None:
        """The example TOML in the repo root should parse successfully."""
        example_path = Path(__file__).parent.parent / "example_config.toml"
        toml_data = load_toml(example_path)
        config = build_config(toml_data)
        assert config.request.dpid == "MKSCNR"
        assert config.request.dp_max_date == date(2023, 6, 30)
        assert len(config.selected_domains) == 7

    def test_config_file_not_found(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError, match="not found"):
            load_toml(tmp_path / "nonexistent.toml")

    def test_config_no_domains_selected(self, tmp_path: Path) -> None:
        toml_path = _write_toml(
            tmp_path,
            """
            [request]
            dpid = "X"

            [inputs]
            mil = "/data/mil"

            [output]
            root = "output"
            domains = []
        """,
        )
        toml_data = load_toml(toml_path)
        with pytest.raises(ConfigError, match="at least one domain"):
            build_config(toml_data)

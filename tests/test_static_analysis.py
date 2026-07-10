"""Static analysis tests for AC.16: pattern headers, no catch-all modules, function annotations."""

from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import ClassVar

SRC_DIR = Path(__file__).parent.parent / "src" / "scdm_snapshot_db"

# Modules that are allowed to be pure structural (no runtime behavior)
_NON_RUNTIME_MODULES = {"__init__.py", "transforms/__init__.py"}

# Pattern classification header
_PATTERN_RE = re.compile(r"^# pattern: (Functional Core|Imperative Shell)", re.MULTILINE)

# Catch-all module names that must not exist
_FORBIDDEN_MODULES = {"utils.py", "helpers.py", "common.py"}


def _all_py_files() -> list[Path]:
    """Get all Python source files."""
    return sorted(SRC_DIR.rglob("*.py"))


class TestPatternHeaders:
    def test_runtime_files_have_pattern_header(self) -> None:
        """Every application source file with runtime behavior must have a pattern header."""
        missing = []
        for f in _all_py_files():
            rel = f.relative_to(SRC_DIR)
            if str(rel) in _NON_RUNTIME_MODULES:
                continue
            content = f.read_text()
            if not _PATTERN_RE.search(content):
                missing.append(str(rel))
        assert not missing, f"Files missing pattern header: {missing}"


class TestNoCatchAllModules:
    def test_no_catch_all_modules(self) -> None:
        """No utils.py, helpers.py, or common.py modules."""
        found = []
        for f in _all_py_files():
            if f.name in _FORBIDDEN_MODULES:
                found.append(str(f.relative_to(SRC_DIR)))
        assert not found, f"Forbidden catch-all modules found: {found}"


class TestFunctionAnnotations:
    def test_all_function_signatures_annotated(self) -> None:
        """All function signatures must be annotated (params and return)."""
        unannotated: list[str] = []

        for f in _all_py_files():
            rel = f.relative_to(SRC_DIR)
            if str(rel) in _NON_RUNTIME_MODULES:
                continue
            try:
                tree = ast.parse(f.read_text(), filename=str(f))
            except SyntaxError:
                continue

            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    # Skip dunder methods and private helpers with no args
                    if node.name.startswith("_") and node.name != "__init__":
                        continue
                    # Check return annotation
                    if node.returns is None:
                        unannotated.append(
                            f"{rel}:{node.lineno}:{node.name} (missing return annotation)"
                        )
                    # Check parameter annotations (skip self)
                    for arg in node.args.args:
                        if arg.arg == "self":
                            continue
                        if arg.annotation is None:
                            unannotated.append(
                                f"{rel}:{node.lineno}:{node.name} (param '{arg.arg}' unannotated)"
                            )

        # Allow __init__ methods in dataclasses to lack return annotation
        unannotated = [u for u in unannotated if "__init__" not in u]
        assert not unannotated, f"Unannotated signatures:\n{chr(10).join(unannotated)}"


class TestNoDisallowedAPIs:
    """AC.11: No UDFs, RDD transformations, coalesce(1), or unconditional actions."""

    DISALLOWED_PATTERNS: ClassVar[list[tuple[str, str]]] = [
        (r"\.coalesce\s*\(\s*1\s*\)", "coalesce(1)"),
        (r"udf\s*\(", "Python UDF"),
        (r"pandas_udf\s*\(", "pandas UDF"),
        (r"\.rdd\b", "RDD transformation"),
        (r"\.foreach\b", "foreach (row-wise processing)"),
        (r"\.toPandas\s*\(", "toPandas (row-wise processing)"),
    ]

    def test_no_disallowed_spark_apis(self) -> None:
        """Source must not use disallowed Spark APIs."""
        violations: list[str] = []
        for f in _all_py_files():
            rel = f.relative_to(SRC_DIR)
            content = f.read_text()
            for pattern, name in self.DISALLOWED_PATTERNS:
                for match in re.finditer(pattern, content):
                    line_num = content[: match.start()].count("\n") + 1
                    violations.append(f"{rel}:{line_num}: {name}")
        assert not violations, f"Disallowed APIs found:\n{chr(10).join(violations)}"


class TestCoreModulesExcludeIO:
    """AC.16: Functional core modules must not import IO or Spark actions."""

    CORE_MODULES: ClassVar[list[str]] = [
        "config_models.py",
        "config_validation.py",
        "schema_contracts.py",
        "error_classification.py",
        "models.py",
    ]

    FORBIDDEN_CORE_IMPORTS: ClassVar[list[str]] = [
        "spark_session",
        "from pyspark.sql import SparkSession",
        "subprocess",
        "pathlib.Path",
        "os.environ",
        "open(",
    ]

    def test_core_modules_exclude_io_and_spark_actions(self) -> None:
        """Core modules must not import or use IO, Spark sessions, or environment access."""
        violations: list[str] = []
        for mod_name in self.CORE_MODULES:
            mod_path = SRC_DIR / mod_name
            if not mod_path.exists():
                continue
            content = mod_path.read_text()
            for forbidden in self.FORBIDDEN_CORE_IMPORTS:
                if forbidden in content:
                    violations.append(f"{mod_name}: contains '{forbidden}'")
        assert not violations, f"Core modules with forbidden IO:\n{chr(10).join(violations)}"

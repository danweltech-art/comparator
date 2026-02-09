"""Simple test runner that takes a YAML schema file and test folder."""

from __future__ import annotations

import yaml
from pathlib import Path
from typing import Optional

from .test_runner import TestRunner, GlobalReport, run_tests as _run_tests
from .models import EngineConfig


class ShadowDiffRunner:
    """
    Test runner that loads schema from YAML file and runs tests from a folder.

    Usage:
        runner = ShadowDiffRunner("schema.yaml", "test/datasets")
        report = runner.run()
        report.print_summary()

    Or as a one-liner:
        report = ShadowDiffRunner.run_tests("schema.yaml", "test/datasets")
    """

    def __init__(
        self,
        schema_path: str,
        test_folder: str,
        engine_config: Optional[EngineConfig] = None
    ):
        """
        Initialize the runner.

        Args:
            schema_path: Path to YAML/JSON schema file with x-migration-* extensions
            test_folder: Path to folder containing dataset JSON files
            engine_config: Optional engine configuration
        """
        self.schema_path = Path(schema_path)
        self.test_folder = Path(test_folder)
        self.engine_config = engine_config or EngineConfig()
        self._schema: Optional[dict] = None

    @property
    def schema(self) -> dict:
        """Load and cache the schema from file."""
        if self._schema is None:
            self._schema = self._load_schema()
        return self._schema

    def _load_schema(self) -> dict:
        """Load schema from YAML or JSON file."""
        if not self.schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {self.schema_path}")

        with open(self.schema_path, 'r') as f:
            content = f.read()

        # Try YAML first (also handles JSON since JSON is valid YAML)
        try:
            return yaml.safe_load(content)
        except yaml.YAMLError as e:
            raise ValueError(f"Failed to parse schema file: {e}")

    def run(self, print_report: bool = True) -> GlobalReport:
        """
        Run all tests in the test folder.

        Args:
            print_report: Whether to print the summary report

        Returns:
            GlobalReport with all results
        """
        if not self.test_folder.exists():
            raise FileNotFoundError(f"Test folder not found: {self.test_folder}")

        return _run_tests(
            test_dir=str(self.test_folder),
            schema=self.schema,
            engine_config=self.engine_config,
            print_report=print_report
        )

    @classmethod
    def run_tests(
        cls,
        schema_path: str,
        test_folder: str,
        print_report: bool = True,
        engine_config: Optional[EngineConfig] = None
    ) -> GlobalReport:
        """
        Convenience class method to run tests in one call.

        Args:
            schema_path: Path to YAML/JSON schema file
            test_folder: Path to folder containing dataset JSON files
            print_report: Whether to print the summary report
            engine_config: Optional engine configuration

        Returns:
            GlobalReport with all results

        Example:
            report = ShadowDiffRunner.run_tests("schema.yaml", "tests/")
        """
        runner = cls(schema_path, test_folder, engine_config)
        return runner.run(print_report=print_report)


def run_tests(
    schema_path: str,
    test_folder: str,
    print_report: bool = True
) -> GlobalReport:
    """
    Run tests from schema file and test folder.

    This is the simplest way to run tests:

        from shadowdiff.runner import run_tests
        report = run_tests("schema.yaml", "test/datasets")

    Args:
        schema_path: Path to YAML/JSON schema file with x-migration-* extensions
        test_folder: Path to folder containing dataset JSON files
        print_report: Whether to print the summary report

    Returns:
        GlobalReport with all results
    """
    return ShadowDiffRunner.run_tests(schema_path, test_folder, print_report)

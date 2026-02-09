"""Test runner for ShadowDiff datasets."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .engine import ShadowDiffEngine
from .models import EngineConfig, DiffReport


@dataclass
class ScenarioResult:
    """Result of a single test scenario."""
    name: str
    passed: bool
    expected_match: bool
    actual_match: bool
    diff_report: Optional[DiffReport] = None
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "passed": self.passed,
            "expected_match": self.expected_match,
            "actual_match": self.actual_match,
            "error": self.error
        }


@dataclass
class GlobalReport:
    """Global test report across all scenarios."""
    total: int = 0
    passed: int = 0
    failed: int = 0
    errors: int = 0
    results: list[ScenarioResult] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "summary": {
                "total": self.total,
                "passed": self.passed,
                "failed": self.failed,
                "errors": self.errors
            },
            "results": [r.to_dict() for r in self.results]
        }

    def print_summary(self):
        print(f"\nTest Results: {self.passed}/{self.total} passed")
        if self.failed > 0:
            print(f"  Failed: {self.failed}")
        if self.errors > 0:
            print(f"  Errors: {self.errors}")


class TestRunner:
    """Runs test datasets against a schema."""

    def __init__(self, schema: dict, engine_config: Optional[EngineConfig] = None):
        self.schema = schema
        self.engine = ShadowDiffEngine(engine_config or EngineConfig())

    def run_dataset(self, dataset: dict, name: str = "unnamed") -> ScenarioResult:
        """Run a single dataset test."""
        before = dataset.get("before", {})
        after = dataset.get("after", {})
        expected_match = dataset.get("expected_match", True)

        try:
            result = self.engine.compare(before, after, self.schema)

            if hasattr(result, 'is_match'):
                actual_match = result.is_match
                passed = actual_match == expected_match
                return ScenarioResult(
                    name=name,
                    passed=passed,
                    expected_match=expected_match,
                    actual_match=actual_match,
                    diff_report=result
                )
            else:
                return ScenarioResult(
                    name=name,
                    passed=False,
                    expected_match=expected_match,
                    actual_match=False,
                    error=str(result.error)
                )
        except Exception as e:
            return ScenarioResult(
                name=name,
                passed=False,
                expected_match=expected_match,
                actual_match=False,
                error=str(e)
            )

    def run_folder(self, folder: str, print_report: bool = True) -> GlobalReport:
        """Run all dataset files in a folder."""
        report = GlobalReport()
        folder_path = Path(folder)

        for dataset_file in sorted(folder_path.glob("*.json")):
            with open(dataset_file) as f:
                dataset = json.load(f)

            name = dataset.get("name", dataset_file.stem)
            result = self.run_dataset(dataset, name)

            report.results.append(result)
            report.total += 1

            if result.error:
                report.errors += 1
                if print_report:
                    print(f"ERROR: {name} - {result.error}")
            elif result.passed:
                report.passed += 1
                if print_report:
                    print(f"PASS: {name}")
            else:
                report.failed += 1
                if print_report:
                    print(f"FAIL: {name} (expected={result.expected_match}, got={result.actual_match})")

        if print_report:
            report.print_summary()

        return report


def run_tests(
    test_dir: str,
    schema: dict,
    engine_config: Optional[EngineConfig] = None,
    print_report: bool = True
) -> GlobalReport:
    """Run all tests in a directory."""
    runner = TestRunner(schema, engine_config)
    return runner.run_folder(test_dir, print_report)

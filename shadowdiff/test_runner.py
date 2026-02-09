"""Test runner for ShadowDiff datasets."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from .engine import ShadowDiffEngine
from .models import EngineConfig, DiffReport
from .extractor import DataExtractor, ExtractConfig, ExtractedData, DataAggregator
from .parent_validator import ParentValidator, ParentConfig, ParentValidationReport


@dataclass
class ScenarioResult:
    """Result of a single test scenario."""
    name: str
    dataset_path: str
    passed: bool
    diff_report: Optional[dict] = None
    parent_validation: Optional[dict] = None
    extracted_data: Optional[dict] = None

    def to_dict(self) -> dict:
        result = {
            "name": self.name,
            "dataset_path": self.dataset_path,
            "passed": self.passed,
        }
        if self.diff_report:
            result["diff_report"] = self.diff_report
        if self.parent_validation:
            result["parent_validation"] = self.parent_validation
        if self.extracted_data:
            result["extracted_data"] = self.extracted_data
        return result


@dataclass
class GlobalReport:
    """Global test report across all scenarios."""
    total: int = 0
    passed: int = 0
    failed: int = 0
    scenarios: list[ScenarioResult] = field(default_factory=list)
    aggregations: dict[str, Any] = field(default_factory=dict)
    breakdown: dict[str, list[str]] = field(default_factory=dict)
    timestamp: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        if not self.breakdown:
            self.breakdown = {
                "no_changes": [],
                "with_changes": [],
                "orphan_parents": [],
                "entries_removed": [],
                "entries_added": []
            }

    def to_dict(self) -> dict:
        pass_rate = f"{(self.passed / self.total * 100):.1f}%" if self.total > 0 else "0.0%"
        return {
            "timestamp": self.timestamp,
            "summary": {
                "total_scenarios": self.total,
                "passed": self.passed,
                "failed": self.failed,
                "pass_rate": pass_rate
            },
            "breakdown": self.breakdown,
            "aggregations": self.aggregations,
            "scenarios": [s.to_dict() for s in self.scenarios]
        }

    def print_summary(self):
        pass_rate = f"{(self.passed / self.total * 100):.1f}%" if self.total > 0 else "0.0%"
        print(f"\nTest Results: {self.passed}/{self.total} passed ({pass_rate})")
        if self.failed > 0:
            print(f"  Failed: {self.failed}")

        # Print breakdown
        if self.breakdown.get("no_changes"):
            print(f"  No changes: {len(self.breakdown['no_changes'])} datasets")
        if self.breakdown.get("with_changes"):
            print(f"  With changes: {len(self.breakdown['with_changes'])} datasets")
        if self.breakdown.get("orphan_parents"):
            print(f"  Orphan parents: {len(self.breakdown['orphan_parents'])} datasets")
        if self.breakdown.get("entries_removed"):
            print(f"  Entries removed: {len(self.breakdown['entries_removed'])} datasets")
        if self.breakdown.get("entries_added"):
            print(f"  Entries added: {len(self.breakdown['entries_added'])} datasets")

        if self.aggregations:
            print(f"\nAggregations:")
            for field_name, values in self.aggregations.items():
                print(f"  {field_name}:")
                for value, info in values.items():
                    print(f"    {value}: {info['count']} datasets")


class TestRunner:
    """Runs test datasets against a schema."""

    def __init__(self, schema: dict, engine_config: Optional[EngineConfig] = None):
        self.schema = schema
        self.engine = ShadowDiffEngine(engine_config or EngineConfig())

        # Setup extraction if configured
        self.extract_config = ExtractConfig.from_schema(schema)
        self.extractor = DataExtractor(self.extract_config) if self.extract_config else None

        # Setup parent validation if configured
        self.parent_config = ParentConfig.from_schema(schema)
        self.parent_validator = ParentValidator(self.parent_config) if self.parent_config else None

        # Setup aggregator based on extraction paths
        aggregate_paths = []
        if self.extract_config:
            aggregate_paths.extend(self.extract_config.before_paths)
        self.aggregator = DataAggregator(aggregate_paths) if aggregate_paths else None

    def _diff_report_to_dict(self, result: DiffReport) -> dict:
        """Convert DiffReport to dictionary format matching original output."""
        diffs = []
        for diff in result.diffs:
            diffs.append({
                "path": diff.path,
                "type": diff.type.value,
                "severity": diff.severity.value,
                "old_value": diff.old_value,
                "new_value": diff.new_value,
                "message": diff.message,
                "rule_applied": diff.rule_applied
            })

        return {
            "is_match": result.is_match,
            "summary": {
                "total_fields_checked": result.summary.total_fields_checked,
                "mismatches_found": result.summary.mismatches_found,
                "warnings_count": result.summary.warnings_count,
                "fields_ignored": result.summary.fields_ignored
            },
            "diffs_count": len(diffs),
            "diffs": diffs
        }

    def _analyze_diffs(self, diffs: list[dict]) -> dict:
        """Analyze diffs to categorize changes."""
        has_changes = len(diffs) > 0
        entries_removed = False
        entries_added = False

        for diff in diffs:
            diff_type = diff.get("type", "")
            if diff_type == "ARRAY_ITEM_MISSING":
                entries_removed = True
            elif diff_type in ("ARRAY_ITEM_EXTRA", "EXTRA_IN_NEW"):
                entries_added = True

        return {
            "has_changes": has_changes,
            "entries_removed": entries_removed,
            "entries_added": entries_added
        }

    def run_dataset(self, dataset: dict, name: str, dataset_path: str) -> ScenarioResult:
        """Run a single dataset test."""
        before = dataset.get("before", {})
        after = dataset.get("after", {})

        extracted_data = None
        parent_validation = None
        diff_report_dict = None

        try:
            # Run comparison
            result = self.engine.compare(before, after, self.schema)

            # Extract data if configured
            if self.extractor:
                extracted = self.extractor.extract(dataset)
                extracted_data = extracted.to_dict()
                if self.aggregator:
                    self.aggregator.add_dataset(name, extracted)

            # Validate parent relationships if configured
            if self.parent_validator:
                parent_result = self.parent_validator.validate(before, after)
                parent_validation = parent_result.to_dict()

            if hasattr(result, 'is_match'):
                diff_report_dict = self._diff_report_to_dict(result)
                passed = result.is_match
                return ScenarioResult(
                    name=name,
                    dataset_path=dataset_path,
                    passed=passed,
                    diff_report=diff_report_dict,
                    parent_validation=parent_validation,
                    extracted_data=extracted_data
                )
            else:
                return ScenarioResult(
                    name=name,
                    dataset_path=dataset_path,
                    passed=False,
                    diff_report={"error": str(result.error)},
                    parent_validation=parent_validation,
                    extracted_data=extracted_data
                )
        except Exception as e:
            return ScenarioResult(
                name=name,
                dataset_path=dataset_path,
                passed=False,
                diff_report={"error": str(e)},
                parent_validation=parent_validation,
                extracted_data=extracted_data
            )

    def run_folder(self, folder: str, print_report: bool = True) -> GlobalReport:
        """Run all dataset files in a folder."""
        report = GlobalReport()
        folder_path = Path(folder)

        for dataset_file in sorted(folder_path.glob("*.json")):
            with open(dataset_file) as f:
                dataset = json.load(f)

            name = dataset.get("name", dataset_file.stem)
            dataset_path = str(dataset_file)
            result = self.run_dataset(dataset, name, dataset_path)

            report.scenarios.append(result)
            report.total += 1

            if result.passed:
                report.passed += 1
                report.breakdown["no_changes"].append(name)
                if print_report:
                    print(f"PASS: {name}")
            else:
                report.failed += 1
                report.breakdown["with_changes"].append(name)
                if print_report:
                    print(f"FAIL: {name}")

                # Analyze diffs for breakdown
                if result.diff_report and "diffs" in result.diff_report:
                    analysis = self._analyze_diffs(result.diff_report["diffs"])
                    if analysis["entries_removed"]:
                        report.breakdown["entries_removed"].append(name)
                    if analysis["entries_added"]:
                        report.breakdown["entries_added"].append(name)

                # Check for orphan parents
                if result.parent_validation:
                    if result.parent_validation.get("summary", {}).get("orphans", 0) > 0:
                        report.breakdown["orphan_parents"].append(name)

        # Add aggregation results
        if self.aggregator:
            report.aggregations = self.aggregator.get_report()

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

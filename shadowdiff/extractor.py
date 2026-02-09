"""Data extraction utilities for ShadowDiff test reports."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class ExtractConfig:
    """Configuration for data extraction from test datasets."""
    input_paths: list[str] = field(default_factory=list)
    before_paths: list[str] = field(default_factory=list)
    after_paths: list[str] = field(default_factory=list)

    @classmethod
    def from_schema(cls, schema: dict) -> Optional['ExtractConfig']:
        """Extract config from schema x-migration-extract."""
        config_dict = schema.get('x-migration-extract')
        if not config_dict:
            return None

        return cls(
            input_paths=config_dict.get('input', []),
            before_paths=config_dict.get('before', []),
            after_paths=config_dict.get('after', [])
        )


@dataclass
class ExtractedData:
    """Extracted data from a dataset."""
    input_data: dict[str, Any] = field(default_factory=dict)
    before_data: dict[str, Any] = field(default_factory=dict)
    after_data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "input": self.input_data,
            "before": self.before_data,
            "after": self.after_data
        }


class DataExtractor:
    """
    Extracts data from JSON objects using path expressions.

    Supported path syntax:
    - Simple path: "label", "condition.type"
    - Array wildcard: "candidates[*].label"
    - Nested array wildcard: "candidates[*].coverages[*].code"
    - Root reference: "root.label" (same as "label")
    """

    def __init__(self, config: ExtractConfig):
        self.config = config

    def extract(self, dataset: dict) -> ExtractedData:
        """Extract data from a dataset based on configured paths."""
        result = ExtractedData()

        input_data = dataset.get('input', {})
        before_data = dataset.get('before', {})
        after_data = dataset.get('after', {})

        # Extract from input
        for path in self.config.input_paths:
            key = self._path_to_key(path)
            result.input_data[key] = self._extract_path(input_data, path)

        # Extract from before
        for path in self.config.before_paths:
            key = self._path_to_key(path)
            result.before_data[key] = self._extract_path(before_data, path)

        # Extract from after
        for path in self.config.after_paths:
            key = self._path_to_key(path)
            result.after_data[key] = self._extract_path(after_data, path)

        return result

    def _path_to_key(self, path: str) -> str:
        """Convert a path to a display key."""
        # Remove 'root.' prefix if present
        if path.startswith('root.'):
            path = path[5:]
        # Replace [*] with empty string for cleaner key
        return path.replace('[*]', '')

    def _extract_path(self, data: Any, path: str) -> Any:
        """
        Extract value(s) from data using path expression.

        Supports:
        - Simple paths: "field.subfield"
        - Array wildcards: "array[*].field"
        - Multiple wildcards: "array[*].nested[*].field"
        """
        # Remove 'root.' prefix if present
        if path.startswith('root.'):
            path = path[5:]

        return self._extract_recursive(data, path)

    def _extract_recursive(self, data: Any, path: str) -> Any:
        """Recursively extract values following the path."""
        if not path:
            return data

        if data is None:
            return None

        # Check for array wildcard
        wildcard_match = re.match(r'^([^.\[]+)\[\*\](.*)$', path)
        if wildcard_match:
            field_name = wildcard_match.group(1)
            remaining_path = wildcard_match.group(2)

            # Remove leading dot from remaining path
            if remaining_path.startswith('.'):
                remaining_path = remaining_path[1:]

            # Get the array
            if isinstance(data, dict) and field_name in data:
                array_data = data[field_name]
                if isinstance(array_data, list):
                    results = []
                    for item in array_data:
                        if remaining_path:
                            extracted = self._extract_recursive(item, remaining_path)
                            if isinstance(extracted, list):
                                results.extend(extracted)
                            elif extracted is not None:
                                results.append(extracted)
                        else:
                            results.append(item)
                    return results
                return None
            return None

        # Simple field access
        dot_match = re.match(r'^([^.\[]+)(?:\.(.+))?$', path)
        if dot_match:
            field_name = dot_match.group(1)
            remaining_path = dot_match.group(2) or ''

            if isinstance(data, dict) and field_name in data:
                if remaining_path:
                    return self._extract_recursive(data[field_name], remaining_path)
                return data[field_name]
            return None

        return None

    @staticmethod
    def extract_single_path(data: Any, path: str) -> Any:
        """Convenience method to extract a single path from data."""
        extractor = DataExtractor(ExtractConfig())
        return extractor._extract_path(data, path)


class DataAggregator:
    """Aggregates extracted data across multiple datasets."""

    def __init__(self, aggregate_by: list[str]):
        """
        Args:
            aggregate_by: List of paths to aggregate by (e.g., ["condition.type", "status"])
        """
        self.aggregate_by = aggregate_by
        self.aggregations: dict[str, dict[Any, list[str]]] = {}

    def add_dataset(self, dataset_name: str, extracted: ExtractedData):
        """Add a dataset's extracted data to the aggregation."""
        # Aggregate by before data
        for agg_field in self.aggregate_by:
            if agg_field not in self.aggregations:
                self.aggregations[agg_field] = {}

            # Try to find the field in before_data
            key = agg_field.replace('[*]', '')
            values = extracted.before_data.get(key)

            if values is None:
                # Try extracting directly
                continue

            if isinstance(values, list):
                for value in values:
                    self._add_to_aggregation(agg_field, value, dataset_name)
            else:
                self._add_to_aggregation(agg_field, values, dataset_name)

    def _add_to_aggregation(self, field: str, value: Any, dataset_name: str):
        """Add a value to the aggregation."""
        if value is None:
            return

        # Convert to hashable
        if isinstance(value, dict):
            value = str(value)
        elif isinstance(value, list):
            value = tuple(value) if all(not isinstance(v, (dict, list)) for v in value) else str(value)

        if value not in self.aggregations[field]:
            self.aggregations[field][value] = []

        if dataset_name not in self.aggregations[field][value]:
            self.aggregations[field][value].append(dataset_name)

    def get_report(self) -> dict:
        """Get the aggregation report."""
        report = {}
        for field, values in self.aggregations.items():
            report[field] = {
                str(value): {
                    "count": len(datasets),
                    "datasets": datasets
                }
                for value, datasets in sorted(values.items(), key=lambda x: str(x[0]))
            }
        return report

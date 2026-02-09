#!/usr/bin/env python
"""Simple script to test datasets with ShadowDiff engine."""

import json
import yaml
from pathlib import Path

# Fix: temporarily patch the __init__.py to avoid the missing test_runner import
init_path = Path("shadowdiff/__init__.py")
original_init = init_path.read_text()

# Create a patched version without test_runner imports
patched_init = """
from .engine import ShadowDiffEngine
from .models import EngineConfig, DiffReport, DiffEntry, DiffType, Severity, WarningEntry
"""

init_path.write_text(patched_init)

try:
    from shadowdiff import ShadowDiffEngine

    # Load schema
    with open("schema.yaml") as f:
        schema = yaml.safe_load(f)

    print("Schema loaded successfully\n")

    # Load and test each dataset
    datasets_dir = Path("datasets")
    for dataset_file in datasets_dir.glob("*.json"):
        print(f"Testing: {dataset_file.name}")
        print("-" * 40)

        with open(dataset_file) as f:
            dataset = json.load(f)

        before = dataset.get("before", {})
        after = dataset.get("after", {})
        expected = dataset.get("expected_match", True)

        # Run comparison
        engine = ShadowDiffEngine()
        result = engine.compare(before, after, schema)

        if hasattr(result, 'is_match'):
            print(f"Match: {result.is_match}")
            print(f"Expected: {expected}")
            print(f"Status: {'PASS' if result.is_match == expected else 'FAIL'}")
            print(f"Fields checked: {result.summary.total_fields_checked}")
            print(f"Mismatches: {result.summary.mismatches_found}")

            if result.diffs:
                print("\nDifferences:")
                for diff in result.diffs:
                    print(f"  - [{diff.type.value}] {diff.path}: {diff.message}")

            if result.warnings:
                print("\nWarnings:")
                for w in result.warnings:
                    print(f"  - {w.path}: {w.message}")
        else:
            print(f"Error: {result.error}")

        print("\n")

finally:
    # Restore original __init__.py
    init_path.write_text(original_init)

print("Done! (Original __init__.py restored)")

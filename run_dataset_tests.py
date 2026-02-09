#!/usr/bin/env python
"""Run ShadowDiff tests from command line."""

import argparse
import json
import sys
from pathlib import Path

from shadowdiff import run_tests


def main():
    parser = argparse.ArgumentParser(
        description="Run ShadowDiff dataset tests",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_dataset_tests.py schema.yaml report.json datasets/
  python run_dataset_tests.py -s schema.yaml -r report.json -d datasets/
  python run_dataset_tests.py --schema schema.yaml --report report.json --datasets datasets/
        """
    )

    parser.add_argument(
        "schema",
        nargs="?",
        help="Path to YAML/JSON schema file"
    )
    parser.add_argument(
        "report",
        nargs="?",
        help="Path to output JSON report file"
    )
    parser.add_argument(
        "datasets",
        nargs="?",
        help="Path to folder containing dataset JSON files"
    )

    # Also support named arguments
    parser.add_argument("-s", "--schema", dest="schema_named", help="Path to schema file")
    parser.add_argument("-r", "--report", dest="report_named", help="Path to output report")
    parser.add_argument("-d", "--datasets", dest="datasets_named", help="Path to datasets folder")
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress console output")

    args = parser.parse_args()

    # Use named args if positional not provided
    schema_path = args.schema or args.schema_named
    report_path = args.report or args.report_named
    datasets_path = args.datasets or args.datasets_named

    # Validate required arguments
    if not schema_path:
        parser.error("Schema path is required")
    if not report_path:
        parser.error("Report path is required")
    if not datasets_path:
        parser.error("Datasets path is required")

    # Validate paths exist
    if not Path(schema_path).exists():
        print(f"Error: Schema file not found: {schema_path}", file=sys.stderr)
        return 1

    if not Path(datasets_path).exists():
        print(f"Error: Datasets folder not found: {datasets_path}", file=sys.stderr)
        return 1

    # Run tests
    if not args.quiet:
        print(f"Schema: {schema_path}")
        print(f"Datasets: {datasets_path}")
        print(f"Report: {report_path}\n")

    report = run_tests(
        schema_path=schema_path,
        test_folder=datasets_path,
        print_report=not args.quiet
    )

    # Save report
    with open(report_path, 'w') as f:
        json.dump(report.to_dict(), indent=2, fp=f)

    if not args.quiet:
        print(f"\nReport saved to: {report_path}")

    # Return exit code
    return 0 if report.failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

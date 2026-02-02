"""Example usage of ShadowDiff comparison engine."""

import json
from shadowdiff import ShadowDiffEngine, EngineConfig

# Sample schema with migration extensions
schema = {
    "type": "object",
    "x-migration-global-ignores": ["$..updatedAt", "$..metadata"],
    "x-migration-allow-null-as-missing": True,
    "properties": {
        "id": {
            "type": "string",
            "x-migration-strategy": "strict"
        },
        "legacyId": {
            "type": "string",
            "x-migration-alias": "old_invoice_id"
        },
        "total": {
            "type": "number",
            "x-migration-precision": 0.01
        },
        "status": {
            "type": "string",
            "x-migration-enum-map": {
                "PAID": "paid",
                "PENDING": "pending"
            }
        },
        "description": {
            "type": "string",
            "x-migration-trim-whitespace": True,
            "x-migration-case-insensitive": True
        },
        "createdAt": {
            "type": "string",
            "x-migration-datetime-format": "ISO8601",
            "x-migration-datetime-tolerance": "5s"
        },
        "lineItems": {
            "type": "array",
            "x-migration-array-mode": "keyed",
            "x-migration-array-key": "sku",
            "x-migration-ignore-extra-items": True,
            "items": {
                "type": "object",
                "properties": {
                    "sku": {"type": "string"},
                    "quantity": {"type": "integer"},
                    "unitPrice": {
                        "type": "number",
                        "x-migration-precision": 0.001
                    }
                }
            }
        }
    }
}

# Old API response (legacy system)
old_response = {
    "id": "INV-001",
    "old_invoice_id": "LEGACY-123",  # Uses old field name
    "total": 100.00,
    "status": "PAID",  # Uses old enum value
    "description": "  Test Invoice  ",  # Has whitespace
    "createdAt": "2025-02-02T10:30:00Z",
    "updatedAt": "2025-02-02T11:00:00Z",  # Will be ignored
    "metadata": {"traceId": "abc123"},  # Will be ignored
    "lineItems": [
        {"sku": "WIDGET-001", "quantity": 5, "unitPrice": 10.00},
        {"sku": "GADGET-002", "quantity": 2, "unitPrice": 25.50}
    ]
}

# New API response (new system)
new_response = {
    "id": "INV-001",
    "legacyId": "LEGACY-123",  # Uses new field name
    "total": 100.00,  # Slightly different (within precision)
    "status": "paid",  # Uses new enum value
    "description": "test invoice",  # Different whitespace/case
    "createdAt": "2025-02-02T10:30:02Z",  # 2 seconds different (within tolerance)
    "lineItems": [
        {"sku": "WIDGET-001", "quantity": 5, "unitPrice": 10.00},
        {"sku": "GADGET-002", "quantity": 2, "unitPrice": 25.50} # Extra item (allowed)
    ]
}


def main():
    print("=" * 60)
    print("ShadowDiff Comparison Engine - Example")
    print("=" * 60)

    # Create engine with default config
    engine = ShadowDiffEngine()

    # Compare payloads
    result = engine.compare(old_response, new_response, schema)
	json.dumps(result.to_dict(), indent=2)
    # Check result type
    if hasattr(result, 'is_match'):
        # Success - DiffReport
        print(f"\nMatch: {result.is_match}")
        print(f"\nExecution:")
        print(f"  Duration: {result.execution.duration_ms}ms")
        print(f"  Engine Version: {result.execution.engine_version}")

        print(f"\nSummary:")
        print(f"  Fields Checked: {result.summary.total_fields_checked}")
        print(f"  Mismatches: {result.summary.mismatches_found}")
        print(f"  Warnings: {result.summary.warnings_count}")
        print(f"  Ignored: {result.summary.fields_ignored}")

        if result.diffs:
            print(f"\nDifferences:")
            for diff in result.diffs:
                print(f"  - [{diff.type.value}] {diff.path}")
                print(f"    Old: {diff.old_value}")
                print(f"    New: {diff.new_value}")
                print(f"    Message: {diff.message}")

        if result.warnings:
            print(f"\nWarnings:")
            for warning in result.warnings:
                print(f"  - [{warning.type.value}] {warning.path}")
                print(f"    {warning.message}")

        print("\n" + "-" * 60)
        print("Full JSON Report:")
        print(json.dumps(result.to_dict(), indent=2))

    else:
        # Error - ErrorResponse
        print(f"\nError: {result.error['code']}")
        print(f"Message: {result.error['message']}")
        print(f"Details: {result.error.get('details', {})}")


def example_with_mismatch():
    """Example that demonstrates a mismatch."""
    print("\n" + "=" * 60)
    print("Example with Mismatch")
    print("=" * 60)

    # Modify new response to have a mismatch
    mismatched_new = {
        "id": "INV-001",
        "legacyId": "LEGACY-123",
        "total": 100.00,  # Exceeds precision tolerance
        "status": "paid",
        "description": "test invoice",
        "createdAt": "2025-02-02T10:30:00Z",
        "lineItems": [
            {"sku": "WIDGET-001", "quantity": 5, "unitPrice": 10.00},  # Quantity changed
            {"sku": "GADGET-002", "quantity": 2, "unitPrice": 25.50}
        ]
    }

    engine = ShadowDiffEngine()
    result = engine.compare(old_response, mismatched_new, schema)

    if hasattr(result, 'is_match'):
        print(f"\nMatch: {result.is_match}")
        print(f"Mismatches found: {result.summary.mismatches_found}")

        if result.diffs:
            print(f"\nDifferences:")
            for diff in result.diffs:
                print(f"  - [{diff.type.value}] {diff.path}")
                print(f"    {diff.message}")
                if diff.rule_applied:
                    print(f"    Rule: {diff.rule_applied}")


def example_with_tracing():
    """Example with rule tracing enabled."""
    print("\n" + "=" * 60)
    print("Example with Rule Tracing")
    print("=" * 60)

    config = EngineConfig(trace_rule_application=True)
    engine = ShadowDiffEngine(config)

    result = engine.compare(old_response, new_response, schema)

    if hasattr(result, 'is_match') and result.trace:
        print(f"\nRule Traces:")
        for trace in result.trace:
            print(f"  - {trace.path}: {trace.rule} -> {trace.action}")
            if trace.details:
                print(f"    Details: {trace.details}")


if __name__ == "__main__":
    main()
    example_with_mismatch()
    example_with_tracing()
